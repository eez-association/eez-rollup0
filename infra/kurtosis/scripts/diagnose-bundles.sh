#!/usr/bin/env bash
# Snapshot the EEZ Kurtosis bundle path:
#   eez-node submitter -> rbuilder eth_sendBundle -> relay/mev-boost -> L1 block.
# This is intentionally read-only. It helps distinguish:
#   - no bundle submission
#   - bundle accepted by RPC but dropped before inclusion
#   - rbuilder simulation/timestamp rejection
#   - builder/relay not winning proposer slots
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

E="${KURTOSIS_ENCLAVE:-eez-devnet}"
RPC="${EEZ_L1_RPC_URL:?set EEZ_L1_RPC_URL}"
BUILDER="${EEZ_L1_BUILDER_RPC_URL:?set EEZ_L1_BUILDER_RPC_URL}"

need() {
    command -v "$1" >/dev/null || {
        echo "diagnose-bundles: $1 not found" >&2
        exit 1
    }
}
need kurtosis
need cast
need curl

section() { printf '\n== %s ==\n' "$1"; }

section "endpoints"
echo "enclave=$E"
echo "l1_rpc=$RPC"
echo "builder_rpc=$BUILDER"

section "heads"
head_1="$(cast block-number --rpc-url "$RPC" 2>/dev/null || true)"
echo "l1_head=${head_1:-unreachable}"
sleep 2
head_2="$(cast block-number --rpc-url "$RPC" 2>/dev/null || true)"
echo "l1_head_after_2s=${head_2:-unreachable}"
if [[ -n "$head_1" && -n "$head_2" && "$head_1" == "$head_2" ]]; then
    echo "note=L1 may still be fine with 12s slots; re-run if this stays unchanged across >15s"
fi

section "builder rpc probe"
probe='{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":[],"blockNumber":"0x1"}]}'
curl -sS "$BUILDER" -H 'Content-Type: application/json' -d "$probe" || true
echo

section "recent eez-node bundle lines"
node_log="$(kurtosis service logs "$E" eez-node 2>/dev/null || true)"
printf '%s\n' "$node_log" \
    | grep -iE 'eth_sendBundle response received|bundle outcome observed|bundle dropped|bundle observation exceeding|advanced L2 safe head' \
    | tail -80 || true

last_hash="$(printf '%s\n' "$node_log" \
    | grep -i 'bundle outcome observed' \
    | tail -1 \
    | sed -nE 's/.*tx_hash=([^ ]+).*/\1/p')"
last_target="$(printf '%s\n' "$node_log" \
    | grep -i 'bundle outcome observed' \
    | tail -1 \
    | sed -nE 's/.*target_block=([0-9]+).*/\1/p')"

if [[ -n "$last_hash" ]]; then
    section "last observed bundle"
    echo "tx_hash=$last_hash"
    [[ -n "$last_target" ]] && echo "target_block=$last_target"
    echo "receipt:"
    cast receipt "$last_hash" --rpc-url "$RPC" 2>/dev/null || echo "no receipt on canonical L1"
fi

section "rbuilder logs"
kurtosis service logs "$E" "${KURTOSIS_BUILDER_SERVICE:-el-5-reth-builder-lighthouse}" 2>/dev/null \
    | grep -iE 'bundle|sendBundle|simulation|simulate|revert|reject|error|timestamp|IncorrectTimestamp|payload|bid' \
    | tail -160 || true

section "likely mev relay / boost logs"
for svc in mev-boost-relay mev-boost cl-5-lighthouse-builder; do
    if kurtosis service logs "$E" "$svc" >/tmp/eez-diag-svc.log 2>/dev/null; then
        echo "-- $svc --"
        grep -iE 'builder|bid|payload|relay|registration|validator|error|warn' /tmp/eez-diag-svc.log \
            | tail -80 || true
    fi
done
