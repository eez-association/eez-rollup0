#!/usr/bin/env bash
# Production-shaped load and block-hash commitment checks for a running enclave.
set -euo pipefail

K="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-production}"
SCENARIO_DIR="$RESULT_DIR/production"
SAMPLE_FILE="$SCENARIO_DIR/commitment-samples.jsonl"
SUMMARY_FILE="$SCENARIO_DIR/summary.json"
mkdir -p "$SCENARIO_DIR"
: >"$SAMPLE_FILE"
rm -f "$SCENARIO_DIR/observer.failed" "$SCENARIO_DIR/observer.stop" "$SUMMARY_FILE"

for tool in bash cast curl jq kurtosis openssl; do
    command -v "$tool" >/dev/null || { echo "$tool not in PATH" >&2; exit 1; }
done

# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
# shellcheck disable=SC1091
source "$K/scripts/lib.sh"
L1="${L1:-$EEZ_DEVNET_L1_RPC}"
L2="${L2:-$EEZ_DEVNET_L2_RPC}"
L1F="${L1F:-$EEZ_DEVNET_L1_FRONT}"
L2F="${L2F:-$EEZ_DEVNET_L2_FRONT}"

DEPLOY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/eez-production-$ENCLAVE.XXXXXX")
observer_pid=""
cleanup() {
    local status=$?
    [[ -z "$observer_pid" ]] || kill "$observer_pid" 2>/dev/null || true
    [[ -z "$observer_pid" ]] || wait "$observer_pid" 2>/dev/null || true
    rm -rf "$DEPLOY_DIR"
    return "$status"
}
trap cleanup EXIT

kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null
set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/deployments.env"
set +a
: "${EEZ_REGISTRY_ADDRESS:?missing from deployment artifact}"
: "${EEZ_ROLLUP_ID:?missing from deployment artifact}"

echo "==> production scenario: ordered state chains, poison isolation, and mixed-direction drains"
bash "$K/scripts/verify-state-chaining.sh" 2>&1 | tee "$SCENARIO_DIR/state-and-poison.log"

rpc() { # <url> <method> <params-json>
    local response
    response=$(curl -fsS --max-time 10 -H 'content-type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" "$1")
    jq -e 'if .error then error(.error.message) else .result end' <<<"$response"
}

assert_rpc_rejection() { # <label> <url> <method> <params>
    local label="$1" url="$2" method="$3" params="$4" response
    response=$(curl -sS --max-time 10 -H 'content-type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}" "$url")
    jq -e '.error != null and .result == null' <<<"$response" >/dev/null || {
        echo "$label was not rejected: $response" >&2
        return 1
    }
    echo "    ✓ $label rejected without a transaction result"
}

sample_commitment() {
    local before after safe block number state_root safe_number status=ok reason=""
    before=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
        "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
    safe=$(retry cast block safe --rpc-url "$L2" --json)
    # Query by hash: a state root is not a block identifier, so this directly
    # guards the state-root -> block-hash migration.
    if ! block=$(rpc "$L2" eth_getBlockByHash "[\"$before\",false]" 2>/dev/null); then
        block=null
    fi
    after=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
        "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
    safe_number=$(jq -r '.number' <<<"$safe" | xargs cast to-dec)
    number=$(jq -r '.number // empty' <<<"$block" 2>/dev/null || true)
    state_root=$(jq -r '.stateRoot // empty' <<<"$block" 2>/dev/null || true)

    if [[ "${before,,}" != "${after,,}" ]]; then
        status=raced; reason="L1 commitment advanced during sample"
    elif [[ "$block" == null || -z "$number" ]]; then
        status=fail; reason="stable L1 commitment is not an L2 block hash"
    else
        number=$(cast to-dec "$number")
        if (( number > safe_number )); then
            status=fail; reason="committed block is above L2 safe head"
        elif [[ "${before,,}" == "${state_root,,}" ]]; then
            status=fail; reason="L1 commitment equals state root instead of block hash"
        fi
    fi
    jq -nc --arg status "$status" --arg reason "$reason" --arg commitment "$before" \
        --arg block_number "${number:-}" --arg safe_number "$safe_number" \
        '{time:(now|todateiso8601),status:$status,reason:$reason,commitment:$commitment,
          committed_block:(if ($block_number|length)>0 then ($block_number|tonumber) else null end),
          safe_block:($safe_number|tonumber)}' \
        >>"$SAMPLE_FILE"
    [[ "$status" != fail ]] || { echo "commitment observer: $reason ($before)" >&2; return 1; }
}

observe_commitments() {
    local end=$((SECONDS + ${EEZ_PRODUCTION_OBSERVER_SECS:-900}))
    while (( SECONDS < end )) && [[ ! -f "$SCENARIO_DIR/observer.stop" ]]; do
        sample_commitment || return 1
        sleep "${EEZ_PRODUCTION_SAMPLE_INTERVAL_SECS:-2}"
    done
}

echo "==> production scenario: RPC boundary abuse"
assert_rpc_rejection "malformed L1-front transaction" "$L1F" eth_sendRawTransaction '["0xdeadbeef"]'
assert_rpc_rejection "malformed L2-front transaction" "$L2F" eth_sendRawTransaction '["0xdeadbeef"]'
assert_rpc_rejection "unknown L2 method" "$L2" eez_methodThatMustNotExist '[]'

echo "==> production scenario: mixed bidirectional load with poison calls and pure-L2 contention"
workload_l1_start=$(cast block-number --rpc-url "$L1")
workload_l2_start=$(cast block-number --rpc-url "$L2")
system_address=0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076
system_nonce_before=$(rpc "$L2" eth_getTransactionCount \
    "[\"$system_address\",$(printf '\"0x%x\"' "$workload_l2_start")]" | jq -r '.' | xargs cast to-dec)
( observe_commitments || touch "$SCENARIO_DIR/observer.failed" ) >"$SCENARIO_DIR/observer.log" 2>&1 &
observer_pid=$!
EEZ_WAVE_MODE=mixed-pure \
EEZ_WAVE_COUNT="${EEZ_PRODUCTION_WAVES:-3}" \
EEZ_INCLUDE_REVERTS=1 \
EEZ_FILLER_PER_GAP="${EEZ_PRODUCTION_FILLER_PER_GAP:-4}" \
EEZ_NODE_LOG="$SCENARIO_DIR/eez-node-wave.log" \
EEZ_PROOF_SIGNER_LOG="$SCENARIO_DIR/eez-proof-signer-wave.log" \
    bash "$K/scripts/cross-chain-wave.sh" 2>&1 | tee "$SCENARIO_DIR/mixed-load.log"

touch "$SCENARIO_DIR/observer.stop"
wait "$observer_pid" || true
observer_pid=""
if [[ -f "$SCENARIO_DIR/observer.failed" ]]; then
    cat "$SCENARIO_DIR/observer.log" >&2
    exit 1
fi

sample_commitment
valid_samples=$(jq -s '[.[] | select(.status == "ok")] | length' "$SAMPLE_FILE")
raced_samples=$(jq -s '[.[] | select(.status == "raced")] | length' "$SAMPLE_FILE")
(( valid_samples > 0 )) || { echo "no stable block-hash commitment sample was observed" >&2; exit 1; }

echo "==> production scenario: native system-transaction security and accounting"
safe_block=$(cast block safe --rpc-url "$L2" --json)
safe_number=$(jq -r '.number' <<<"$safe_block" | xargs cast to-dec)
call_tuple='(uint16,bool,uint64,address,uint64,address,uint256,bytes)'
expected_tuple="(bytes32,$call_tuple[],bytes32,bool,bytes)"
entry_tuple="(bytes32,$call_tuple[],$expected_tuple[],bytes32,bool,bytes)"
static_tuple="(bytes32,$call_tuple[],bytes32,bool,bytes)"
incoming_selector=$(cast sig "executeIncomingCrossChainCall(address,uint256,bytes,address,uint64,$entry_tuple[],$static_tuple[])")
load_selector=$(cast sig "loadExecutionTable($entry_tuple[],$static_tuple[])")
native_count=0
incoming_count=0
load_count=0
first_native_raw=""
expected_nonce="$system_nonce_before"
l2_chain_id=$(cast chain-id --rpc-url "$L2")

for ((height = workload_l2_start + 1; height <= safe_number; height++)); do
    block=$(rpc "$L2" eth_getBlockByNumber "[$(printf '\"0x%x\"' "$height"),true]")
    tx_count=$(jq '.transactions | length' <<<"$block")
    for ((index = 0; index < tx_count; index++)); do
        tx=$(jq -c ".transactions[$index]" <<<"$block")
        [[ $(jq -r '.type' <<<"$tx") == 0x76 ]] || continue
        native_count=$((native_count + 1))
        tx_hash=$(jq -r '.hash' <<<"$tx")
        tx_nonce=$(jq -r '.nonce' <<<"$tx" | xargs cast to-dec)
        tx_input=$(jq -r '.input' <<<"$tx")
        [[ $(jq -r '.from | ascii_downcase' <<<"$tx") == "$system_address" ]] \
            || { echo "native tx $tx_hash has a spoofed sender" >&2; exit 1; }
        [[ $(jq -r '.to | ascii_downcase' <<<"$tx") == "${EEZL2_ADDRESS,,}" ]] \
            || { echo "native tx $tx_hash targets a non-EEZL2 address" >&2; exit 1; }
        [[ $(jq -r '.gasPrice' <<<"$tx") == 0x0 ]] \
            || { echo "native tx $tx_hash charges a gas price" >&2; exit 1; }
        [[ $(jq -r '.gas' <<<"$tx" | xargs cast to-dec) == 2000000 ]] \
            || { echo "native tx $tx_hash does not expose the fixed protocol gas budget" >&2; exit 1; }
        [[ $(jq -r '.chainId' <<<"$tx" | xargs cast to-dec) == "$l2_chain_id" ]] \
            || { echo "native tx $tx_hash is not replay-bound to this L2 chain" >&2; exit 1; }
        [[ $(jq -r '[.r,.s,.v] | all(. == "0x0")' <<<"$tx") == true ]] \
            || { echo "native tx $tx_hash exposes a forged signature" >&2; exit 1; }
        (( tx_nonce == expected_nonce )) \
            || { echo "native nonce gap: got $tx_nonce, expected $expected_nonce" >&2; exit 1; }
        expected_nonce=$((expected_nonce + 1))

        receipt=$(rpc "$L2" eth_getTransactionReceipt "[\"$tx_hash\"]")
        [[ $(jq -r '.type' <<<"$receipt") == 0x76 \
            && $(jq -r '.status' <<<"$receipt") == 0x1 \
            && $(jq -r '.effectiveGasPrice' <<<"$receipt") == 0x0 \
            && $(jq -r '.gasUsed' <<<"$receipt") != 0x0 ]] \
            || { echo "native receipt invariant failed: $receipt" >&2; exit 1; }

        case "${tx_input:0:10}" in
            "$incoming_selector") incoming_count=$((incoming_count + 1)) ;;
            "$load_selector")
                load_count=$((load_count + 1))
                (( index + 1 < tx_count )) \
                    || { echo "outbound load $tx_hash is terminal in its Sync block" >&2; exit 1; }
                [[ $(jq -r ".transactions[$((index + 1))].type" <<<"$block") != 0x76 ]] \
                    || { echo "outbound load $tx_hash is not immediately paired with its user tx" >&2; exit 1; }
                ;;
            *) echo "unknown privileged system selector in $tx_hash" >&2; exit 1 ;;
        esac
        if [[ -z "$first_native_raw" ]]; then
            first_native_raw=$(rpc "$L2" eth_getRawTransactionByHash "[\"$tx_hash\"]" | jq -r '.')
        fi
    done
done
system_nonce_after=$(rpc "$L2" eth_getTransactionCount \
    "[\"$system_address\",$(printf '\"0x%x\"' "$safe_number")]" | jq -r '.' | xargs cast to-dec)
(( native_count > 0 && incoming_count > 0 && load_count > 0 )) \
    || { echo "native direction coverage missing: total=$native_count inbound=$incoming_count outbound=$load_count" >&2; exit 1; }
(( system_nonce_after - system_nonce_before == native_count )) \
    || { echo "system nonce delta does not equal canonical native tx count" >&2; exit 1; }
[[ "$first_native_raw" == 0x76* ]] || { echo "native transaction did not retain its 0x76 envelope" >&2; exit 1; }
assert_rpc_rejection "replayed privileged native envelope" "$L2" eth_sendRawTransaction "[\"$first_native_raw\"]"

echo "==> production scenario: landed DA payload canonicality and workload binding"
batch_logs=$(cast logs --address "$EEZ_REGISTRY_ADDRESS" --from-block "$workload_l1_start" \
    --to-block latest 'BatchPosted(uint256)' --rpc-url "$L1" --json)
batch_count=$(jq 'length' <<<"$batch_logs")
(( batch_count > 0 )) || { echo "workload produced no BatchPosted events" >&2; exit 1; }
calldata_files=()
for ((index = 0; index < batch_count; index++)); do
    batch_hash=$(jq -r ".[$index].transactionHash" <<<"$batch_logs")
    batch_input=$(rpc "$L1" eth_getTransactionByHash "[\"$batch_hash\"]" | jq -r '.input')
    calldata_file="$SCENARIO_DIR/post-batch-$index.calldata"
    printf '%s\n' "$batch_input" >"$calldata_file"
    calldata_files+=("$calldata_file")
done
(cd "$REPO" && cargo run --quiet --locked -p eez-protocol --example inspect_post_batch -- \
    "${calldata_files[@]}") >"$SCENARIO_DIR/da-payloads.jsonl"
jq -se --argjson rollup "$EEZ_ROLLUP_ID" '
    length > 0
    and all(.rollup_id == $rollup)
    and all(.block_count > 0 and .call_data_bytes > 0 and .proof_count > 0)
    and (map(.action_count) | add > 0)
    and (map(.pure_transaction_count) | add > 0)
' "$SCENARIO_DIR/da-payloads.jsonl" >/dev/null || {
    echo "landed DA payloads did not bind the expected rollup, actions, proofs, and pure traffic" >&2
    exit 1
}

echo "==> production scenario: post-load liveness and head monotonicity"
l1_before=$(cast block-number --rpc-url "$L1")
l2_before=$(cast block-number --rpc-url "$L2")
sleep "${EEZ_PRODUCTION_LIVENESS_WINDOW_SECS:-15}"
l1_after=$(cast block-number --rpc-url "$L1")
l2_after=$(cast block-number --rpc-url "$L2")
(( l1_after > l1_before )) || { echo "L1 stopped advancing ($l1_before -> $l1_after)" >&2; exit 1; }
(( l2_after > l2_before )) || { echo "L2 stopped advancing ($l2_before -> $l2_after)" >&2; exit 1; }

jq -n --argjson valid_samples "$valid_samples" --argjson raced_samples "$raced_samples" \
    --argjson waves "${EEZ_PRODUCTION_WAVES:-3}" --argjson l1_before "$l1_before" \
    --argjson l1_after "$l1_after" --argjson l2_before "$l2_before" --argjson l2_after "$l2_after" \
    --argjson native_count "$native_count" --argjson incoming_count "$incoming_count" \
    --argjson load_count "$load_count" --argjson batch_count "$batch_count" \
    '{result:"pass",workload:{mode:"mixed-pure",waves:$waves,poison_calls:true},
      commitment_observer:{valid_samples:$valid_samples,raced_samples:$raced_samples},
      native_system_transactions:{total:$native_count,inbound:$incoming_count,outbound_loads:$load_count},
      da_payloads:{landed_and_decoded:$batch_count},
      liveness:{l1:{before:$l1_before,after:$l1_after},l2:{before:$l2_before,after:$l2_after}}}' \
    >"$SUMMARY_FILE"

echo "production scenarios PASS ($valid_samples stable block-hash samples, $raced_samples settlement races)"
