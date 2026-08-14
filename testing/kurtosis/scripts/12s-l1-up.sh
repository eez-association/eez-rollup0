#!/usr/bin/env bash
# Machine 1: (re)launch the Kurtosis L1-only devnet in a consistent state and
# expose it to Machine 2 + the world via socat. Emits machine2.env (enode / CL
# multiaddr / endpoints) and prints the browser UI URLs. Idempotent — safe to
# re-run; it tears down the old enclave + forwarders first.
#
#   bash scripts/12s-l1-up.sh.sh
#
# Fixed external ports (world-open): EL 8545, builder 8645, beacon 5052,
# EL-P2P 30303, CL-P2P 9010, dora 8080, mev-relay 9060, blockscout 4000/api 4001.
set -uo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"; cd "$REPO"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-l1-12s}"
ARGS="${KURTOSIS_ARGS_FILE:-$REPO/infra/kurtosis/l1-only.args.yaml}"
IP1="${IP1:-$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.*src \([0-9.]*\).*/\1/p')}"
NET="kt-$ENCLAVE"
for t in kurtosis docker cast jq curl socat openssl; do command -v "$t" >/dev/null || { echo "✗ $t missing"; exit 1; }; done

echo "==> [1/7] teardown old state (enclave + forwarders + public blockscout)"
pkill -f 'socat TCP-LISTEN' 2>/dev/null || true
docker rm -f bs-frontend-public 2>/dev/null || true
kurtosis enclave rm -f "$ENCLAVE" 2>/dev/null || true

echo "==> [2/7] launch L1-only enclave ($ENCLAVE)"
kurtosis run github.com/ethpandaops/ethereum-package --enclave "$ENCLAVE" --args-file "$ARGS"

# host-port resolver: "kurtosis port print" → strip scheme, keep :PORT
kp() { kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null | sed -E 's#^https?://##; s#/$##'; }
hp() { kp "$1" "$2" | sed -E 's#.*:([0-9]+)$#\1#'; }   # just the port number

echo "==> [3/7] resolve services"
EL_RPC=$(hp el-1-reth-lighthouse rpc)
EL_P2P=$(hp el-1-reth-lighthouse tcp-discovery)
CL_HTTP=$(hp cl-1-lighthouse-reth http)
CL_P2P=$(hp cl-1-lighthouse-reth tcp-discovery)
BUILDER=$(hp el-2-reth-builder-lighthouse rbuilder-rpc)
DORA=$(hp dora http)
MEVWEB=$(hp mev-relay-website http)
BS_API=$(hp blockscout http)
echo "    EL_RPC=127.0.0.1:$EL_RPC EL_P2P=$EL_P2P CL_HTTP=$CL_HTTP CL_P2P=$CL_P2P BUILDER=$BUILDER"
echo "    DORA=$DORA MEVWEB=$MEVWEB BS_API=$BS_API"

echo "==> [4/7] socat forwarders (0.0.0.0, world-open)"
fwd() { setsid bash -c "exec socat TCP-LISTEN:$1,fork,reuseaddr TCP:127.0.0.1:$2" >/dev/null 2>&1 </dev/null & disown 2>/dev/null || true; }
fwd 8545 "$EL_RPC"; fwd 8645 "$BUILDER"; fwd 5052 "$CL_HTTP"
fwd 30303 "$EL_P2P"; fwd 9010 "$CL_P2P"
fwd 8080 "$DORA"; fwd 9060 "$MEVWEB"; fwd 4001 "$BS_API"
sleep 2

echo "==> [5/7] resolve enode + CL peer-id"
for i in $(seq 1 30); do PUBENODE=$(cast rpc admin_nodeInfo --rpc-url "http://127.0.0.1:$EL_RPC" 2>/dev/null | jq -r '.enode // empty'); [[ -n "$PUBENODE" ]] && break; sleep 2; done
PUBKEY=$(sed -E 's#enode://([0-9a-f]+)@.*#\1#' <<<"$PUBENODE")
for i in $(seq 1 30); do PEERID=$(curl -s "http://127.0.0.1:$CL_HTTP/eth/v1/node/identity" 2>/dev/null | jq -r '.data.peer_id // empty'); [[ -n "$PEERID" ]] && break; sleep 2; done
echo "    enode pubkey=${PUBKEY:0:16}…  peer-id=$PEERID"

echo "==> [6/7] public blockscout frontend (separate container; Kurtosis's stays intact)"
FE=$(docker ps --format '{{.Names}}' | grep -i blockscout-frontend | grep -v public | head -1)
if [[ -n "$FE" ]]; then
  docker inspect "$FE" --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep -vE '^(HOSTNAME|PATH|NODE_VERSION|YARN_VERSION|HOME|TERM|NEXT_PUBLIC_API_HOST|NEXT_PUBLIC_APP_HOST|NEXT_PUBLIC_APP_PORT|NEXT_PUBLIC_NETWORK_RPC_URL)=' > /tmp/bs-fe.env
  docker run -d --name bs-frontend-public --network "$NET" -p 4000:3000 --env-file /tmp/bs-fe.env \
    -e NEXT_PUBLIC_API_HOST="$IP1:4001" -e NEXT_PUBLIC_APP_HOST="$IP1" -e NEXT_PUBLIC_APP_PORT=4000 \
    -e NEXT_PUBLIC_NETWORK_RPC_URL="http://$IP1:8545" \
    --entrypoint ./entrypoint.sh ghcr.io/blockscout/frontend:latest node server.js >/dev/null && echo "    bs-frontend-public up on :4000"
fi

echo "==> [7/7] download L1 genesis + write machine2.env"
rm -rf "$REPO/l1-genesis"; kurtosis files download "$ENCLAVE" el_cl_genesis_data "$REPO/l1-genesis" >/dev/null 2>&1
# Plain KEY=VAL (no `export`) so it works as a docker-compose --env-file AND is
# sourceable with `set -a; source machine2.env; set +a`.
cat > "$REPO/machine2.env" <<EOF
# ── Machine 1 Kurtosis L1 ($IP1) → Machine 2 eez stack. Regenerated $(date -u +%FT%TZ 2>/dev/null || echo now). ──
# scp -r <M1>:$REPO/l1-genesis  ->  Machine 2 ; ports below are socat-forwarded OPEN.
EEZ_L1_CHAIN_ID=7331
EEZ_L1_BLOCK_TIME_MS=12000
EEZ_L1_TARGET_RPC_URL=http://$IP1:8545
EEZ_L1_BUILDER_RPC_URL=http://$IP1:8645
L1_BEACON_URL=http://$IP1:5052
EEZ_L1_TRUSTED_PEERS=enode://$PUBKEY@$IP1:30303
CL_MULTIADDR=/ip4/$IP1/tcp/9010/p2p/$PEERID
CL_PEERID=$PEERID
EOF
echo
echo "════════ Machine 1 L1 up ════════"
echo " machine2.env  -> $REPO/machine2.env   (scp to Machine 2 + the l1-genesis dir)"
echo " Browser UIs:   blockscout http://$IP1:4000   dora http://$IP1:8080   mev-relay http://$IP1:9060"
echo " EL RPC http://$IP1:8545 (7331)   builder http://$IP1:8645   beacon http://$IP1:5052"
echo "═════════════════════════════════"
