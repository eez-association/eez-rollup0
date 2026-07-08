#!/usr/bin/env bash
# Snapshot the EEZ Kurtosis bundle path:
#   eez-node submitter -> rbuilder eth_sendBundle -> relay/mev-boost -> L1 block.
# By default this is read-only. Set EEZ_DIAG_SEND_PROBE=1 to submit harmless
# 0-value control bundles from the poster key to itself. It helps
# distinguish:
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
POSTER_KEY="${EEZ_L1_POSTER_KEY:-}"
KEY="${EEZ_DIAG_PROBE_KEY:-${POSTER_KEY:-}}"
SLOT_SECONDS="${EEZ_L1_SLOT_SECONDS:-12}"
PROBE_SLACK="${EEZ_DIAG_PROBE_SLACK:-3}"

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
if [[ -n "$POSTER_KEY" ]]; then
    echo "poster=$(cast wallet address --private-key "$POSTER_KEY" 2>/dev/null || echo unknown)"
else
    echo "poster=unset"
fi
if [[ -n "$KEY" ]]; then
    echo "probe_sender=$(cast wallet address --private-key "$KEY" 2>/dev/null || echo unknown)"
    if [[ -n "$POSTER_KEY" && "$KEY" == "$POSTER_KEY" ]]; then
        echo "warning=active probe is using the EEZ poster key; this can consume the postBatch nonce and perturb eez-node"
        echo "warning=set EEZ_DIAG_PROBE_KEY to a separate funded dev key for clean active probes"
    fi
else
    echo "probe_sender=unset (active probe disabled unless EEZ_DIAG_PROBE_KEY or EEZ_L1_POSTER_KEY is set)"
fi

section "service hints"
kurtosis enclave inspect "$E" 2>/dev/null \
    | grep -iE 'builder|relay|boost|lighthouse|reth|eez-node' \
    | head -120 || true

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

if [[ "${EEZ_DIAG_SEND_PROBE:-0}" == "1" ]]; then
    section "active control bundle"
    if [[ -z "$KEY" ]]; then
        echo "EEZ_DIAG_SEND_PROBE=1 requires EEZ_L1_POSTER_KEY/poster_key in args.yaml"
    else
        addr="$(cast wallet address --private-key "$KEY")"
        bal="$(cast balance "$addr" --rpc-url "$RPC" 2>/dev/null || echo 0)"
        echo "from=$addr balance=$bal"
        if [[ "$bal" == "0" ]]; then
            echo "poster has zero balance; cannot send active probe"
        else
            send_control_bundle() {
                local label="$1" target="$2" pin_ts="${3:-}"
                local raw hash body waited max_wait
                raw="$(cast mktx "$addr" --value 0 --private-key "$KEY" --rpc-url "$RPC")"
                hash="$(cast keccak "$raw")"
                if [[ -n "$pin_ts" ]]; then
                    body="$(printf '{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":["%s"],"blockNumber":"0x%x","minTimestamp":%s,"maxTimestamp":%s}]}' "$raw" "$target" "$pin_ts" "$pin_ts")"
                    echo "$label tx_hash=$hash target_block=$target pin_timestamp=$pin_ts slack=$PROBE_SLACK"
                else
                    body="$(printf '{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":["%s"],"blockNumber":"0x%x"}]}' "$raw" "$target")"
                    echo "$label tx_hash=$hash target_block=$target slack=$PROBE_SLACK"
                fi
                echo "$label send_response=$(curl -sS "$BUILDER" -H 'Content-Type: application/json' -d "$body" || true)"
                waited=0
                max_wait=$(( SLOT_SECONDS * (PROBE_SLACK + 6) ))
                while (( $(cast block-number --rpc-url "$RPC") <= target + 1 )); do
                    sleep 2
                    waited=$(( waited + 2 ))
                    if (( waited > max_wait )); then
                        echo "$label timed out waiting for L1 to pass target+1"
                        break
                    fi
                done
                if cast receipt "$hash" --rpc-url "$RPC" >/dev/null 2>&1; then
                    echo "$label=LANDED"
                    cast receipt "$hash" --rpc-url "$RPC" | sed -n '1,40p'
                else
                    echo "$label=DID_NOT_LAND"
                    echo "$label target_block_summary:"
                    cast block "$target" --rpc-url "$RPC" 2>/dev/null | sed -n '1,80p' || true
                fi
            }

            probe_head="$(cast block-number --rpc-url "$RPC")"
            target=$(( probe_head + PROBE_SLACK ))
            send_control_bundle "control_unpinned" "$target"

            pinned_head="$(cast block-number --rpc-url "$RPC")"
            pinned_target=$(( pinned_head + PROBE_SLACK ))
            head_ts="$(cast block "$pinned_head" --field timestamp --rpc-url "$RPC")"
            pinned_ts=$(( head_ts + PROBE_SLACK * SLOT_SECONDS ))
            send_control_bundle "control_pinned" "$pinned_target" "$pinned_ts"
        fi
    fi
fi

section "recent eez-node bundle lines"
node_log="$(kurtosis service logs "$E" eez-node 2>/dev/null || true)"
printf '%s\n' "$node_log" \
    | grep -iE 'compose_sync_slot invoked|dispatching bundle to builder|eth_sendBundle response received|bundle outcome observed|bundle dropped|bundle observation exceeding|advanced L2 safe head' \
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

builder_log="$(kurtosis service logs "$E" "${KURTOSIS_BUILDER_SERVICE:-el-5-reth-builder-lighthouse}" 2>/dev/null || true)"

if [[ -n "${last_target:-}" ]]; then
    section "rbuilder target-block lines"
    printf '%s\n' "$builder_log" \
        | grep -E "block=${last_target}|slot=${last_target}|block ${last_target}|slot ${last_target}|target.?block.?${last_target}" \
        | tail -120 || true
fi

section "rbuilder logs"
printf '%s\n' "$builder_log" \
    | grep -iE 'bundle|sendBundle|simulation|simulate|revert|reject|error|timestamp|IncorrectTimestamp|payload|bid' \
    | tail -160 || true

section "likely mev relay / boost logs"
for svc in \
    mev-relay-api \
    mev-relay-housekeeper \
    mev-boost-1-lighthouse-reth \
    mev-boost-2-lighthouse-reth \
    mev-boost-3-lighthouse-reth \
    mev-boost-4-lighthouse-reth \
    cl-1-lighthouse-reth \
    cl-2-lighthouse-reth \
    cl-3-lighthouse-reth \
    cl-4-lighthouse-reth \
    cl-5-lighthouse-reth-builder
do
    if kurtosis service logs "$E" "$svc" >/tmp/eez-diag-svc.log 2>/dev/null; then
        echo "-- $svc --"
        grep -iE 'builder|bid|payload|relay|registration|validator|error|warn|mev|boost|header|getPayload' /tmp/eez-diag-svc.log \
            | tail -80 || true
    fi
done
