#!/usr/bin/env bash
# Read a Kurtosis beacon's ENR + libp2p multiaddr (GET /eth/v1/node/identity) so
# the follower can peer into the enclave CL network. Writes cl-bootnode.env,
# consumed by docker-compose.eez-l1.yml. Needs port_publisher.cl enabled.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
DEST="${EEZ_L1_DATA_DIR:-$REPO/infra/kurtosis/eez-l1-data}"
OUT="$DEST/cl-bootnode.env"

command -v kurtosis >/dev/null || { echo "kurtosis not found in PATH" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl not found in PATH" >&2; exit 1; }

INSPECT="$(kurtosis enclave inspect "$ENCLAVE")" || {
    echo "enclave '$ENCLAVE' not running — run kurtosis-up.sh first" >&2
    exit 1
}

# Discover the first CL (lighthouse) beacon service. Names are cl-<idx>-<cl>-<el>
# e.g. cl-1-lighthouse-reth. Skip the builder CL (…-builder…) — we want a
# validator-backed follower to sync from.
discover_cl_service() {
    echo "$INSPECT" | awk '
        /^[=]* User Services/ { in_services=1; next }
        in_services && /^[=]/ { exit }
        in_services && /cl-[0-9]+-lighthouse/ && !/builder/ {
            match($0, /cl-[0-9]+-lighthouse[a-z0-9-]*/);
            if (RSTART > 0) { print substr($0, RSTART, RLENGTH); exit }
        }
    '
}

CL_SVC="${KURTOSIS_CL_SERVICE:-$(discover_cl_service)}"
[[ -n "$CL_SVC" ]] || { echo "could not find a cl-*-lighthouse service; set KURTOSIS_CL_SERVICE" >&2; exit 1; }

# Beacon HTTP port id is "http" (4000) in ethereum-package.
CL_HTTP="$(kurtosis port print "$ENCLAVE" "$CL_SVC" http 2>/dev/null || true)"
[[ -n "$CL_HTTP" ]] || {
    echo "could not read $CL_SVC http port; check port_publisher.cl in network_params.yaml" >&2
    echo "try: kurtosis port print $ENCLAVE $CL_SVC http" >&2
    exit 1
}
[[ "$CL_HTTP" =~ ^https?:// ]] || CL_HTTP="http://$CL_HTTP"

echo "==> reading ENR from $CL_SVC ($CL_HTTP)"
identity="$(curl -fsS "$CL_HTTP/eth/v1/node/identity")" || {
    echo "beacon API call failed at $CL_HTTP/eth/v1/node/identity" >&2
    exit 1
}

if command -v jq >/dev/null 2>&1; then
    enr="$(printf '%s' "$identity" | jq -r '.data.enr')"
    # A directly-dialable libp2p multiaddr on the enclave network. Prefer a
    # non-loopback ip4 tcp address (the CL's enclave container IP:9000).
    libp2p="$(printf '%s' "$identity" | jq -r '.data.p2p_addresses[]' \
        | grep -E '/ip4/' | grep '/tcp/' | grep -v '127.0.0.1' | head -1)"
else
    # jq-free extraction.
    enr="$(printf '%s' "$identity" | grep -o '"enr"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed -E 's/.*"(enr:[^"]*)".*/\1/')"
    libp2p="$(printf '%s' "$identity" | grep -oE '/ip4/[0-9.]+/tcp/[0-9]+/p2p/[A-Za-z0-9]+' | grep -v '/ip4/127\.0\.0\.1/' | head -1)"
fi
[[ "$enr" == enr:* ]] || { echo "did not get a valid ENR (got: ${enr:-empty})" >&2; exit 1; }
[[ "$libp2p" == /ip4/* ]] || { echo "did not get a dialable libp2p multiaddr (got: ${libp2p:-empty})" >&2; exit 1; }

mkdir -p "$DEST"
# discv5 bans private-IP ENRs from the routing table, so on a private enclave
# the follower must DIAL peers directly by multiaddr (--libp2p-addresses); the
# ENR is kept for reference / in case discovery is ever usable.
{
    printf 'EEZ_L1_CL_BOOTNODE=%s\n' "$enr"
    printf 'EEZ_L1_CL_LIBP2P=%s\n' "$libp2p"
} > "$OUT"

echo "wrote $OUT"
cat "$OUT"
