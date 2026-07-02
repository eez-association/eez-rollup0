#!/usr/bin/env bash
# Fetch a Kurtosis reth's enode so the embedded L1 reth can dial it over RLPx
# (EEZ_L1_TRUSTED_PEERS) and backfill history — the follower CL only feeds it
# HEAD payloads, not blocks 1..N, and discv5 bans the private enclave IPs.
# Writes el-bootnode.env, sourced by run-eez-node.sh.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
ENDPOINTS="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"
DEST="${EEZ_L1_DATA_DIR:-$REPO/infra/kurtosis/eez-l1-data}"
OUT="$DEST/el-bootnode.env"

for t in cast jq; do command -v "$t" >/dev/null || { echo "$t not found in PATH" >&2; exit 1; }; done
[[ -f "$ENDPOINTS" ]] || { echo "missing $ENDPOINTS — run parse-endpoints.sh first" >&2; exit 1; }
# shellcheck disable=SC1090
source "$ENDPOINTS"
: "${EEZ_L1_RPC_URL:?run parse-endpoints.sh}"

echo "==> querying admin_nodeInfo on $EEZ_L1_RPC_URL"
info="$(cast rpc admin_nodeInfo --rpc-url "$EEZ_L1_RPC_URL" 2>/dev/null || true)"
enode="$(printf '%s' "$info" | jq -r '.enode // empty' 2>/dev/null || true)"

if [[ -z "$enode" ]]; then
    cat >&2 <<EOF
could not read an enode via admin_nodeInfo. The Kurtosis reth likely doesn't
expose the 'admin' RPC namespace. Options:
  - get it from the node logs:
      kurtosis service logs $ENCLAVE el-1-reth-lighthouse 2>&1 | grep -i enode
  - then set it by hand:
      echo 'EEZ_L1_TRUSTED_PEERS=enode://<pubkey>@<enclave-ip>:30303' > $OUT
EOF
    exit 1
fi

# admin_nodeInfo sometimes reports the RLPx addr as 127.0.0.1/0.0.0.0. On the
# host that won't reach the enclave node — substitute the container's enclave
# IP. VERIFY the IP is enclave-reachable from the host (Linux: it is).
host_part="${enode#enode://*@}"; ip="${host_part%%:*}"
if [[ "$ip" == "127.0.0.1" || "$ip" == "0.0.0.0" ]]; then
    echo "note: enode advertises $ip; trying to resolve the reth container's enclave IP" >&2
    cid="$(docker ps --filter "network=kt-${ENCLAVE}" --format '{{.Names}}' | grep -E 'el-[0-9]+-reth' | grep -v builder | head -1 || true)"
    if [[ -n "$cid" ]]; then
        real_ip="$(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "$cid" 2>/dev/null || true)"
        [[ -n "$real_ip" ]] && enode="${enode/@$ip:/@$real_ip:}"
    fi
fi

mkdir -p "$DEST"
printf 'EEZ_L1_TRUSTED_PEERS=%s\n' "$enode" > "$OUT"
echo "wrote $OUT"
cat "$OUT"
