#!/usr/bin/env bash
# Verify that separate cross-chain transactions composed into one Sync block
# observe state left by their accepted predecessors on both participating chains.

set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
LOG_DIR="$REPO/datadir/smoke-logs"
mkdir -p "$LOG_DIR"

for tool in cast forge jq curl kurtosis openssl; do
    command -v "$tool" >/dev/null || { echo "$tool not in PATH"; exit 1; }
done

# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
: "${L1:=$EEZ_DEVNET_L1_RPC}"
: "${L2:=$EEZ_DEVNET_L2_RPC}"
: "${L1F:=$EEZ_DEVNET_L1_FRONT}"
: "${L2F:=$EEZ_DEVNET_L2_FRONT}"

NODE_LOG="${EEZ_NODE_LOG:-$LOG_DIR/state-chaining-node.log}"
SIGNER_LOG="${EEZ_PROOF_SIGNER_LOG:-$LOG_DIR/state-chaining-proof-signer.log}"
DEPLOY_DIR=$(mktemp -d "${TMPDIR:-/tmp}/eez-deployments-$ENCLAVE-state-chaining.XXXXXX")
trap 'rm -rf "$DEPLOY_DIR"' EXIT

if [[ "${EEZ_USE_LOCAL_DEPLOYMENTS:-0}" == "1" && -f "$REPO/deployments.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/deployments.env"
    set +a
else
    kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null 2>&1 \
        || { echo "could not download the eez-deployments artifact"; exit 1; }
    set -a
    # shellcheck disable=SC1091
    source "$DEPLOY_DIR/deployments.env"
    set +a
fi
[[ -n "${EEZ_REGISTRY_ADDRESS:-}" && -n "${EEZ_ROLLUP_ID:-}" ]] \
    || { echo "deployments.env is missing the registry address or rollup id"; exit 1; }

L2_DEPLOY_KEY=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
L2_DEPLOYER=$(cast wallet address --private-key "$L2_DEPLOY_KEY")
INBOUND_KEY="${EEZ_STATE_CHAINING_IN_KEY:-0x$(openssl rand -hex 32)}"
INBOUND_SENDER=$(cast wallet address --private-key "$INBOUND_KEY")
OUTBOUND_KEY="${EEZ_STATE_CHAINING_OUT_KEY:-0x$(openssl rand -hex 32)}"
OUTBOUND_SENDER=$(cast wallet address --private-key "$OUTBOUND_KEY")
EEZL2_ADDRESS="${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}"
L1_ROLLUP_ID="${EEZ_L1_ROLLUP_ID:-0}"
PRIORITY_GAS_PRICE="${EEZ_TEST_PRIORITY_GAS_PRICE_WEI:-1}"
RECEIPT_WAIT_SECS="${EEZ_STATE_CHAINING_WAIT_SECS:-300}"
WRAPPED_TOPIC=$(cast keccak 'Wrapped(uint256,bool,bool,uint256)')

yaml_value() {
    grep -E "^[[:space:]]*$1:" "${KURTOSIS_ARGS_FILE:-$K/args.yaml}" 2>/dev/null | head -1 \
        | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^"//; s/"$//'
}
FUNDING_KEY="${EEZ_FUND_FROM_KEY:-${EEZ_PROOF_SIGNER_KEY:-$(yaml_value proof_signer_key)}}"
[[ -n "$FUNDING_KEY" ]] || { echo "could not resolve the L1 funding key"; exit 1; }
L1_DEPLOY_KEY="${EEZ_L1_SETUP_KEY:-$FUNDING_KEY}"

retry() {
    local attempts=0 max="${RETRY_MAX:-6}" delay="${RETRY_DELAY:-3}" output rc
    while :; do
        if output=$("$@" 2>&1); then
            printf '%s' "$output"
            return 0
        else
            rc=$?
        fi
        (( ++attempts >= max )) && {
            echo "retry: '$*' failed after $attempts attempts: $output" >&2
            return "$rc"
        }
        sleep "$delay"
    done
}

gas_price_for() {
    local rpc="$1" gas_price base_hex base minimum
    gas_price=$(cast gas-price --rpc-url "$rpc" 2>/dev/null || echo 1000000000)
    gas_price="${EEZ_TEST_GAS_PRICE_WEI:-$gas_price}"
    base_hex=$(cast block latest --field baseFeePerGas --rpc-url "$rpc" 2>/dev/null || echo 0)
    base=$(cast to-dec "$base_hex" 2>/dev/null || echo 0)
    minimum=$((2 * base + PRIORITY_GAS_PRICE))
    (( gas_price < minimum )) && gas_price="$minimum"
    echo "$gas_price"
}

fund() {
    local rpc="$1" key="$2" from="$3" to="$4" nonce
    nonce=$(retry cast nonce "$from" --rpc-url "$rpc")
    cast send "$to" --value 10ether --private-key "$key" --nonce "$nonce" \
        --gas-price "$(gas_price_for "$rpc")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        --rpc-url "$rpc" >/dev/null
}

forge_deploy() {
    local rpc="$1" key="$2" script="$3" signature="$4" gas_price output
    shift 4
    gas_price=$(gas_price_for "$rpc")
    if ! output=$(cd "$REPO/contracts" && forge script "script/$script" --sig "$signature" "$@" \
        --rpc-url "$rpc" --broadcast --private-key "$key" --gas-price "$gas_price" \
        --skip-simulation 2>&1); then
        printf '%s\n' "$output" >&2
        return 1
    fi
    printf '%s\n' "$output"
}

grab_address() { grep -oE "$1=0x[0-9a-fA-F]{40}" | head -1 | cut -d= -f2; }
refresh_node_log() { kurtosis service logs -a "$ENCLAVE" eez-node >"$NODE_LOG" 2>&1 || true; }
refresh_signer_log() { kurtosis service logs -a "$ENCLAVE" eez-proof-signer >"$SIGNER_LOG" 2>&1 || true; }
strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

create_l2_proxy() {
    local target="$1" proxy code nonce raw chain_id response
    chain_id=$(cast chain-id --rpc-url "$L2")
    proxy=$(cast call "$EEZL2_ADDRESS" 'computeCrossChainProxyAddress(address,uint64)(address)' \
        "$target" "$L1_ROLLUP_ID" --rpc-url "$L2" | tr -d '[:space:]')
    code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
    if [[ "$code" == "0x" || -z "$code" ]]; then
        nonce=$(retry cast nonce "$L2_DEPLOYER" --rpc-url "$L2")
        raw=$(cast mktx --rpc-url "$L2" --chain-id "$chain_id" --private-key "$L2_DEPLOY_KEY" \
            --nonce "$nonce" --gas-limit 1500000 --gas-price "$(gas_price_for "$L2")" \
            --priority-gas-price "$PRIORITY_GAS_PRICE" \
            "$EEZL2_ADDRESS" 'createCrossChainProxy(address,uint64)' "$target" "$L1_ROLLUP_ID")
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] \
            || { echo "could not build the L2 proxy creation transaction" >&2; return 1; }
        response=$(curl -s -X POST "$L2" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}")
        jq -e '.result != null' <<<"$response" >/dev/null \
            || { echo "L2 proxy creation was rejected: $response" >&2; return 1; }
        for _ in $(seq 1 30); do
            code=$(cast code "$proxy" --rpc-url "$L2" 2>/dev/null || echo 0x)
            [[ "$code" != "0x" && -n "$code" ]] && break
            sleep 1
        done
    fi
    [[ "$code" != "0x" && -n "$code" ]] || return 1
    echo "$proxy"
}

wait_for_sync_boundary() {
    local baseline count=0 deadline
    refresh_node_log
    baseline=$(strip_ansi <"$NODE_LOG" | grep -Fc 'compose_sync_slot invoked' || true)
    deadline=$((SECONDS + ${EEZ_SYNC_BOUNDARY_WAIT_SECS:-60}))
    while (( SECONDS < deadline )); do
        refresh_node_log
        count=$(strip_ansi <"$NODE_LOG" | grep -Fc 'compose_sync_slot invoked' || true)
        (( count > baseline )) && return 0
        sleep 1
    done
    echo "timed out waiting for a fresh Sync-slot boundary" >&2
    return 1
}

send_front() {
    local front="$1" raw="$2" expected_hash="$3" response returned_hash
    response=$(curl -s -X POST "$front" -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}")
    returned_hash=$(jq -er '.result // error("missing transaction hash")' <<<"$response" 2>/dev/null || true)
    [[ "${returned_hash,,}" == "${expected_hash,,}" ]] \
        || { echo "cross-chain front rejected or changed the transaction: $response" >&2; return 1; }
}

receipt_json() {
    curl --max-time 3 -s -X POST -H 'Content-Type: application/json' \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$1\"],\"id\":1}" \
        "$2" 2>/dev/null
}

wait_for_receipts() {
    local rpc="$1"
    shift
    local hashes=("$@") deadline hash receipt status confirmed
    deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    while (( SECONDS < deadline )); do
        confirmed=0
        for hash in "${hashes[@]}"; do
            receipt=$(receipt_json "$hash" "$rpc" || true)
            status=$(jq -r '.result.status // "missing"' <<<"$receipt" 2>/dev/null || echo missing)
            [[ "$status" == "0x1" ]] && confirmed=$((confirmed + 1))
            [[ "$status" != "0x0" ]] || { echo "transaction $hash reverted" >&2; return 1; }
        done
        (( confirmed == ${#hashes[@]} )) && return 0
        sleep 5
    done
    echo "only $confirmed/${#hashes[@]} transactions landed within ${RECEIPT_WAIT_SECS}s" >&2
    return 1
}

wrapped_result() {
    local hash="$1" rpc="$2" wrapper="$3" receipt data
    receipt=$(receipt_json "$hash" "$rpc")
    data=$(jq -er --arg address "${wrapper,,}" --arg topic "${WRAPPED_TOPIC,,}" \
        '[.result.logs[] | select((.address | ascii_downcase) == $address)
          | select((.topics[0] | ascii_downcase) == $topic)]
         | if length == 1 then .[0].data else error("expected exactly one Wrapped event") end' \
        <<<"$receipt")
    cast decode-abi --json 'wrapped()(uint256,bool,bool,uint256)' "$data" | jq -c .
}

unique_receipt_block() {
    local rpc="$1"
    shift
    local hash receipt blocks=()
    for hash in "$@"; do
        receipt=$(receipt_json "$hash" "$rpc")
        blocks+=("$(jq -er '.result.blockNumber' <<<"$receipt")")
    done
    printf '%s\n' "${blocks[@]}" | sort -u
}

assert_root_convergence() {
    local sync_height="$1" deadline l1_root l1_recheck safe_block safe_height l2_root
    deadline=$((SECONDS + ${EEZ_STATE_ROOT_WAIT_SECS:-60}))
    while (( SECONDS < deadline )); do
        l1_root=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
        safe_block=$(retry cast block safe --rpc-url "$L2" --json)
        safe_height=$(jq -r '.number' <<<"$safe_block" | xargs cast to-dec)
        l2_root=$(jq -r '.stateRoot' <<<"$safe_block")
        l1_recheck=$(retry cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" | sed -n '2p' | tr -d '[:space:]')
        if [[ "${l1_root,,}" == "${l1_recheck,,}" && "${l1_recheck,,}" == "${l2_root,,}" ]] \
            && (( safe_height >= sync_height )); then
            echo "    ✓ L1 tracked root matches L2 safe root at height $safe_height"
            return 0
        fi
        sleep 1
    done
    echo "L1/L2 roots did not converge through Sync height $sync_height" >&2
    return 1
}

assert_proof_for_height() {
    local sync_height="$1" node_baseline="$2" signer_baseline="$3"
    local deadline node_evidence signer_evidence settled attested_hash signed
    deadline=$((SECONDS + ${EEZ_STATE_CHAINING_PROOF_WAIT_SECS:-180}))
    while (( SECONDS < deadline )); do
        refresh_node_log
        refresh_signer_log
        node_evidence=$(sed -n "$((node_baseline + 1)),\$p" "$NODE_LOG" | strip_ansi)
        signer_evidence=$(sed -n "$((signer_baseline + 1)),\$p" "$SIGNER_LOG" | strip_ansi)
        settled=$(grep -F 'event_name="eez.composer.bundle.observed"' <<<"$node_evidence" \
            | grep -E "sync_height=$sync_height([^0-9]|\$)" | grep -F 'settled=true' | tail -1 || true)
        attested_hash=$(grep -F 'event_name="eez.prover_client.attested"' <<<"$node_evidence" \
            | grep -E "to=$sync_height([^0-9]|$)" | grep -oE 'hash=0x[0-9a-fA-F]{64}' \
            | tail -1 | cut -d= -f2 || true)
        signed=""
        if [[ -n "$attested_hash" ]]; then
            signed=$(grep -F 'event_name="eez.proof_signer.window_signed"' <<<"$signer_evidence" \
                | grep -E "validated_to_block=$sync_height([^0-9]|$)" \
                | grep -F "recomputed_public_inputs_hash=$attested_hash" | tail -1 || true)
        fi
        [[ -n "$settled" && -n "$signed" ]] && {
            echo "    ✓ bundle settled and proof signer validated Sync height $sync_height"
            return 0
        }
        sleep 3
    done
    echo "no matching settled bundle and proof-signer validation for Sync height $sync_height" >&2
    return 1
}

run_scenario() {
    local direction="$1" scenario="$2" source_rpc="$3" front="$4" key="$5" sender="$6"
    local wrapper="$7" target="$8" target_rpc="$9"
    local chain_id nonce gas_price raw hash node_baseline signer_baseline
    local source_blocks all_target_logs target_logs target_blocks sync_height final_value result
    local completed_before target_before target_event_count target_event_count_before target_event_deadline
    local expected_final
    local hashes=() actual_results=()
    local expected_results=()

    echo
    echo "==> $direction $scenario-state chaining: waiting for a fresh drain window"
    wait_for_sync_boundary
    refresh_signer_log
    node_baseline=$(wc -l <"$NODE_LOG")
    signer_baseline=$(wc -l <"$SIGNER_LOG")
    chain_id=$(cast chain-id --rpc-url "$source_rpc")
    nonce=$(retry cast nonce "$sender" --rpc-url "$source_rpc")
    gas_price=$(gas_price_for "$source_rpc")
    completed_before=$(retry cast call "$wrapper" 'completedProxyCalls()(uint256)' --rpc-url "$source_rpc")
    target_before=$(retry cast call "$target" 'value()(uint256)' --rpc-url "$target_rpc")
    all_target_logs=$(retry cast logs --address "$target" --from-block 0 --to-block latest \
        'ValueSet(address,uint256)' --rpc-url "$target_rpc" --json)
    target_event_count_before=$(jq -r 'length' <<<"$all_target_logs")
    target_event_count="$target_event_count_before"

    if [[ "$scenario" == "destination" ]]; then
        [[ "$completed_before" == "3" && "$target_before" == "3" ]] || {
            echo "$direction destination scenario requires wrapper count=3 and target value=3; got $completed_before and $target_before" >&2
            return 1
        }
        expected_results=('[1,true,true,1]' '[1,true,false,1]' '[1,true,false,1]')
        expected_final=1
    else
        [[ "$completed_before" == "0" && "$target_before" == "0" ]] || {
            echo "$direction source scenario requires fresh wrapper and target state; got $completed_before and $target_before" >&2
            return 1
        }
        expected_results=('[1,true,true,1]' '[2,true,true,2]' '[3,true,true,3]')
        expected_final=3
    fi

    echo "==> $direction $scenario-state chaining: submitting three separate transactions"
    for _ in 1 2 3; do
        if [[ "$scenario" == "destination" ]]; then
            raw=$(cast mktx --chain-id "$chain_id" --private-key "$key" --nonce "$nonce" \
                --gas-limit 800000 --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                --rpc-url "$source_rpc" \
                "$wrapper" 'setViaProxy(uint256)' 1)
        else
            raw=$(cast mktx --chain-id "$chain_id" --private-key "$key" --nonce "$nonce" \
                --gas-limit 800000 --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                --rpc-url "$source_rpc" \
                "$wrapper" 'setNextValueViaProxy()')
        fi
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "could not build $direction transaction"; return 1; }
        hash=$(cast keccak "$raw")
        send_front "$front" "$raw" "$hash"
        hashes+=("$hash")
        nonce=$((nonce + 1))
    done

    wait_for_receipts "$source_rpc" "${hashes[@]}"
    source_blocks=$(unique_receipt_block "$source_rpc" "${hashes[@]}")
    [[ $(wc -l <<<"$source_blocks" | tr -d ' ') == 1 ]] \
        || { echo "$direction source transactions landed in different blocks: $source_blocks" >&2; return 1; }

    for hash in "${hashes[@]}"; do
        actual_results+=("$(wrapped_result "$hash" "$source_rpc" "$wrapper")")
    done
    for index in 0 1 2; do
        result="${actual_results[$index]}"
        [[ "$result" == "${expected_results[$index]}" ]] || {
            echo "$direction $scenario transaction $((index + 1)) returned $result; expected ${expected_results[$index]}" >&2
            return 1
        }
    done
    echo "    ✓ ordered returns exactly match ${expected_results[*]}"

    target_event_deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    while (( SECONDS < target_event_deadline )); do
        all_target_logs=$(retry cast logs --address "$target" --from-block 0 --to-block latest \
            'ValueSet(address,uint256)' --rpc-url "$target_rpc" --json)
        target_event_count=$(jq -r 'length' <<<"$all_target_logs")
        (( target_event_count >= target_event_count_before + 3 )) && break
        sleep 3
    done
    (( target_event_count == target_event_count_before + 3 )) \
        || { echo "$direction $scenario target did not emit exactly three new ValueSet events" >&2; return 1; }
    target_logs=$(jq -c --argjson before "$target_event_count_before" '.[$before:]' <<<"$all_target_logs")
    target_blocks=$(jq -r '[.[].blockNumber] | unique | .[]' <<<"$target_logs")
    [[ $(wc -l <<<"$target_blocks" | tr -d ' ') == 1 ]] \
        || { echo "$direction destination calls landed in different blocks: $target_blocks" >&2; return 1; }

    if [[ "$direction" == "inbound" ]]; then
        sync_height=$(cast to-dec "$target_blocks")
    else
        sync_height=$(cast to-dec "$source_blocks")
    fi
    echo "    ✓ all source transactions and destination effects preserve one ordered Sync block"

    final_value=$(retry cast call "$target" 'value()(uint256)' --rpc-url "$target_rpc")
    [[ "$final_value" == "$expected_final" ]] \
        || { echo "$direction $scenario final Value.value=$final_value, expected $expected_final" >&2; return 1; }
    assert_proof_for_height "$sync_height" "$node_baseline" "$signer_baseline"
    assert_root_convergence "$sync_height"
}

echo "════════════════════════════════════════════════════════════════"
echo " STATE CHAINING TEST (kurtosis)"
echo "════════════════════════════════════════════════════════════════"
echo "    L1=$L1"
echo "    L2=$L2"
echo "    inbound sender=$INBOUND_SENDER"
echo "    outbound sender=$OUTBOUND_SENDER"

cast block-number --rpc-url "$L1" >/dev/null || { echo "L1 RPC is not reachable"; exit 1; }
cast block-number --rpc-url "$L2" >/dev/null || { echo "L2 RPC is not reachable"; exit 1; }

echo "==> funding fresh source accounts"
fund "$L1" "$FUNDING_KEY" "$(cast wallet address --private-key "$FUNDING_KEY")" "$INBOUND_SENDER"
fund "$L2" "$L2_DEPLOY_KEY" "$L2_DEPLOYER" "$OUTBOUND_SENDER"

echo "==> deploying stateful targets and source-side wrappers"
L2_VALUE=$(forge_deploy "$L2" "$L2_DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 \
    | grab_address EEZ_VALUE_ADDRESS)
L1_VALUE=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 \
    | grab_address EEZ_VALUE_ADDRESS)
[[ -n "$L2_VALUE" && -n "$L1_VALUE" ]] || { echo "Value deployment failed"; exit 1; }

INBOUND_PROXY=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" CreateValueProxy.s.sol:CreateValueProxy \
    'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE" "$EEZ_ROLLUP_ID" \
    | grab_address EEZ_VALUE_PROXY)
OUTBOUND_PROXY=$(create_l2_proxy "$L1_VALUE")
[[ -n "$INBOUND_PROXY" && -n "$OUTBOUND_PROXY" ]] || { echo "proxy creation failed"; exit 1; }

INBOUND_WRAPPER=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 \
    'run(address)' "$INBOUND_PROXY" | grab_address EEZ_SETTER_WRAPPER)
OUTBOUND_WRAPPER=$(forge_deploy "$L2" "$L2_DEPLOY_KEY" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 \
    'run(address)' "$OUTBOUND_PROXY" | grab_address EEZ_SETTER_WRAPPER)
[[ -n "$INBOUND_WRAPPER" && -n "$OUTBOUND_WRAPPER" ]] || { echo "wrapper deployment failed"; exit 1; }

run_scenario inbound source "$L1" "$L1F" "$INBOUND_KEY" "$INBOUND_SENDER" \
    "$INBOUND_WRAPPER" "$L2_VALUE" "$L2"
run_scenario inbound destination "$L1" "$L1F" "$INBOUND_KEY" "$INBOUND_SENDER" \
    "$INBOUND_WRAPPER" "$L2_VALUE" "$L2"
run_scenario outbound source "$L2" "$L2F" "$OUTBOUND_KEY" "$OUTBOUND_SENDER" \
    "$OUTBOUND_WRAPPER" "$L1_VALUE" "$L1"
run_scenario outbound destination "$L2" "$L2F" "$OUTBOUND_KEY" "$OUTBOUND_SENDER" \
    "$OUTBOUND_WRAPPER" "$L1_VALUE" "$L1"

echo
echo "==> STATE CHAINING TEST PASSED (source + destination, inbound + outbound)"
