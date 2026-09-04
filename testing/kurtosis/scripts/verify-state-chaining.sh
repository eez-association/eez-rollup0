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
# shellcheck disable=SC1091
source "$K/scripts/lib.sh"
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
INBOUND_KEY="${EEZ_STATE_CHAINING_IN_KEY:-0x$(openssl rand -hex 32)}"
INBOUND_SENDER=$(cast wallet address --private-key "$INBOUND_KEY")
OUTBOUND_KEY="${EEZ_STATE_CHAINING_OUT_KEY:-0x$(openssl rand -hex 32)}"
OUTBOUND_SENDER=$(cast wallet address --private-key "$OUTBOUND_KEY")
POISON_KEY="${EEZ_STATE_CHAINING_POISON_KEY:-0x$(openssl rand -hex 32)}"
POISON_SENDER=$(cast wallet address --private-key "$POISON_KEY")
EEZL2_ADDRESS="${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}"
L1_ROLLUP_ID="${EEZ_L1_ROLLUP_ID:-0}"
RECEIPT_WAIT_SECS="${EEZ_STATE_CHAINING_WAIT_SECS:-300}"
WRAPPED_TOPIC=$(cast keccak 'Wrapped(uint256,bool,bool,uint256)')

FUNDING_KEY="${EEZ_FUND_FROM_KEY:-$L2_DEPLOY_KEY}"
L1_DEPLOY_KEY="${EEZ_L1_SETUP_KEY:-$FUNDING_KEY}"

refresh_node_log() { kurtosis service logs -a "$ENCLAVE" eez-node >"$NODE_LOG" 2>&1 || true; }
refresh_signer_log() { kurtosis service logs -a "$ENCLAVE" eez-proof-signer >"$SIGNER_LOG" 2>&1 || true; }

wait_for_sync_boundary() {
    # These scenarios assert that all prepared calls share one composed Sync
    # slot, so submit them immediately after the previous pool snapshot.
    local baseline count=0 deadline
    refresh_node_log
    baseline=$(strip_ansi <"$NODE_LOG" \
        | grep -Fc '"event_name":"eez.composer.sync_slot.drain"' || true)
    deadline=$((SECONDS + ${EEZ_SYNC_BOUNDARY_WAIT_SECS:-60}))
    while (( SECONDS < deadline )); do
        refresh_node_log
        count=$(strip_ansi <"$NODE_LOG" \
            | grep -Fc '"event_name":"eez.composer.sync_slot.drain"' || true)
        (( count > baseline )) && return 0
        sleep 1
    done
    echo "timed out waiting for a fresh Sync-slot boundary" >&2
    return 1
}

wait_for_receipts() {
    local rpc="$1"
    shift
    local hashes=("$@") deadline hash status confirmed
    deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    while (( SECONDS < deadline )); do
        confirmed=0
        for hash in "${hashes[@]}"; do
            status=$(receipt_status "$hash" "$rpc")
            [[ "$status" == "1" ]] && confirmed=$((confirmed + 1))
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
        settled=$(grep -F '"event_name":"eez.composer.bundle.observed"' <<<"$node_evidence" \
            | grep -F "\"sync_height\":$sync_height," \
            | grep -F '"settled":true' | tail -1 || true)
        attested_hash=$(grep -F '"event_name":"eez.prover_client.attested"' <<<"$node_evidence" \
            | grep -F "\"to\":$sync_height," \
            | grep -oE '"hash":"0x[0-9a-fA-F]{64}"' \
            | tail -1 | cut -d'"' -f4 || true)
        signed=""
        if [[ -n "$attested_hash" ]]; then
            signed=$(grep -F '"event_name":"eez.proof_signer.window_signed"' <<<"$signer_evidence" \
                | grep -F "\"validated_to_block\":$sync_height," \
                | grep -F "\"recomputed_public_inputs_hash\":\"$attested_hash\"" \
                | tail -1 || true)
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
    local chain_id nonce gas_price raw hash value node_baseline signer_baseline
    local source_blocks all_target_logs target_logs target_blocks sync_height final_value result
    local completed_before target_before target_event_count target_event_count_before target_event_deadline
    local expected_final
    local hashes=() raw_transactions=() actual_results=()
    local expected_results=() index step

    echo
    chain_id=$(cast chain-id --rpc-url "$source_rpc")
    nonce=$(retry cast nonce "$sender" --rpc-url "$source_rpc")
    gas_price=$(gas_price_for "$source_rpc")
    completed_before=$(retry cast call "$wrapper" 'completedProxyCalls()(uint256)' --rpc-url "$source_rpc")
    target_before=$(retry cast call "$target" 'value()(uint256)' --rpc-url "$target_rpc")
    all_target_logs=$(retry cast logs --address "$target" --from-block 0 --to-block latest \
        'ValueSet(address,uint256)' --rpc-url "$target_rpc" --json)
    target_event_count_before=$(jq -r 'length' <<<"$all_target_logs")
    target_event_count="$target_event_count_before"

    case "$scenario" in
        source)
            [[ "$completed_before" == "0" && "$target_before" == "0" ]] || {
                echo "$direction source scenario requires fresh wrapper and target state; got $completed_before and $target_before" >&2
                return 1
            }
            expected_results=('[1,true,true,1]' '[2,true,true,2]' '[3,true,true,3]')
            expected_final=3
            ;;
        destination)
            [[ "$completed_before" == "3" && "$target_before" == "3" ]] || {
                echo "$direction destination scenario requires wrapper count=3 and target value=3; got $completed_before and $target_before" >&2
                return 1
            }
            expected_results=('[1,true,true,1]' '[1,true,false,1]' '[1,true,false,1]')
            expected_final=1
            ;;
        mixed)
            [[ "$completed_before" == "6" && "$target_before" == "1" ]] || {
                echo "$direction mixed scenario requires wrapper count=6 and target value=1; got $completed_before and $target_before" >&2
                return 1
            }
            # The fixed middle call sees the first write; the final derived call sees both frames.
            expected_results=('[7,true,true,7]' '[7,true,false,7]' '[9,true,true,9]')
            expected_final=9
            ;;
        *)
            echo "unknown state-chaining scenario: $scenario" >&2
            return 1
            ;;
    esac

    echo "==> $direction $scenario-state chaining: preparing three separate transactions"
    for step in 1 2 3; do
        if [[ "$scenario" == "destination" || ( "$scenario" == "mixed" && "$step" == "2" ) ]]; then
            value=1
            [[ "$scenario" == "mixed" ]] && value=7
            raw=$(cast mktx --chain-id "$chain_id" --private-key "$key" --nonce "$nonce" \
                --gas-limit 800000 --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                --rpc-url "$source_rpc" \
                "$wrapper" 'setViaProxy(uint256)' "$value")
        else
            raw=$(cast mktx --chain-id "$chain_id" --private-key "$key" --nonce "$nonce" \
                --gas-limit 800000 --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
                --rpc-url "$source_rpc" \
                "$wrapper" 'setNextValueViaProxy()')
        fi
        [[ "$raw" =~ ^0x[0-9a-fA-F]+$ ]] || { echo "could not build $direction transaction"; return 1; }
        hash=$(cast keccak "$raw")
        raw_transactions+=("$raw")
        hashes+=("$hash")
        nonce=$((nonce + 1))
    done

    echo "==> $direction $scenario-state chaining: waiting for a fresh drain window"
    wait_for_sync_boundary
    refresh_signer_log
    node_baseline=$(wc -l <"$NODE_LOG")
    signer_baseline=$(wc -l <"$SIGNER_LOG")

    echo "==> $direction $scenario-state chaining: submitting prepared transactions"
    for index in 0 1 2; do
        send_front "$front" "${raw_transactions[$index]}" "${hashes[$index]}"
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

run_mixed_direction_scenario() {
    local node_baseline signer_baseline inbound_nonce outbound_nonce l1_gas_price l2_gas_price
    local inbound_raw outbound_raw inbound_hash outbound_hash inbound_result outbound_result
    local l2_events_before l2_events event_deadline inbound_block outbound_block sync_height

    echo
    echo "==> mixed-direction state chaining: preparing transactions"
    # Build on the final state of the six directional scenarios above.
    [[ $(retry cast call "$INBOUND_WRAPPER" 'completedProxyCalls()(uint256)' --rpc-url "$L1") == "9" ]]
    [[ $(retry cast call "$OUTBOUND_WRAPPER" 'completedProxyCalls()(uint256)' --rpc-url "$L2") == "9" ]]
    [[ $(retry cast call "$L2_VALUE" 'value()(uint256)' --rpc-url "$L2") == "9" ]]
    [[ $(retry cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1") == "9" ]]

    inbound_nonce=$(retry cast nonce "$INBOUND_SENDER" --rpc-url "$L1")
    outbound_nonce=$(retry cast nonce "$OUTBOUND_SENDER" --rpc-url "$L2")
    l1_gas_price=$(gas_price_for "$L1")
    l2_gas_price=$(gas_price_for "$L2")
    l2_events=$(retry cast logs --address "$L2_VALUE" --from-block 0 --to-block latest \
        'ValueSet(address,uint256)' --rpc-url "$L2" --json)
    l2_events_before=$(jq -r 'length' <<<"$l2_events")

    inbound_raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$L1")" \
        --private-key "$INBOUND_KEY" --nonce "$inbound_nonce" --gas-limit 800000 \
        --gas-price "$l1_gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" --rpc-url "$L1" \
        "$INBOUND_WRAPPER" 'setViaProxy(uint256)' 11)
    outbound_raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$L2")" \
        --private-key "$OUTBOUND_KEY" --nonce "$outbound_nonce" --gas-limit 800000 \
        --gas-price "$l2_gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" --rpc-url "$L2" \
        "$OUTBOUND_WRAPPER" 'setViaProxy(uint256)' 13)
    [[ "$inbound_raw" =~ ^0x[0-9a-fA-F]+$ && "$outbound_raw" =~ ^0x[0-9a-fA-F]+$ ]] \
        || { echo "could not build mixed-direction transactions" >&2; return 1; }
    inbound_hash=$(cast keccak "$inbound_raw")
    outbound_hash=$(cast keccak "$outbound_raw")

    echo "==> mixed-direction state chaining: waiting for a fresh drain window"
    wait_for_sync_boundary
    refresh_signer_log
    node_baseline=$(wc -l <"$NODE_LOG")
    signer_baseline=$(wc -l <"$SIGNER_LOG")

    echo "==> mixed-direction state chaining: submitting prepared transactions"
    send_front "$L1F" "$inbound_raw" "$inbound_hash"
    send_front "$L2F" "$outbound_raw" "$outbound_hash"

    wait_for_receipts "$L1" "$inbound_hash"
    wait_for_receipts "$L2" "$outbound_hash"
    inbound_result=$(wrapped_result "$inbound_hash" "$L1" "$INBOUND_WRAPPER")
    outbound_result=$(wrapped_result "$outbound_hash" "$L2" "$OUTBOUND_WRAPPER")
    [[ "$inbound_result" == '[11,true,true,11]' ]] \
        || { echo "mixed inbound returned $inbound_result" >&2; return 1; }
    [[ "$outbound_result" == '[13,true,true,13]' ]] \
        || { echo "mixed outbound returned $outbound_result" >&2; return 1; }

    event_deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    while (( SECONDS < event_deadline )); do
        l2_events=$(retry cast logs --address "$L2_VALUE" --from-block 0 --to-block latest \
            'ValueSet(address,uint256)' --rpc-url "$L2" --json)
        (( $(jq -r 'length' <<<"$l2_events") >= l2_events_before + 1 )) && break
        sleep 3
    done
    [[ $(jq -r 'length' <<<"$l2_events") == "$((l2_events_before + 1))" ]] \
        || { echo "mixed inbound destination effect was not unique" >&2; return 1; }
    inbound_block=$(jq -er '.[-1].blockNumber' <<<"$l2_events")
    outbound_block=$(receipt_json "$outbound_hash" "$L2" | jq -er '.result.blockNumber')
    # Pin the canonical mixed-direction order from the Rust regression.
    [[ "${inbound_block,,}" == "${outbound_block,,}" ]] \
        || { echo "mixed-direction calls split across L2 blocks: inbound=$inbound_block outbound=$outbound_block" >&2; return 1; }

    [[ $(retry cast call "$L2_VALUE" 'value()(uint256)' --rpc-url "$L2") == "11" ]]
    [[ $(retry cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1") == "13" ]]
    sync_height=$(cast to-dec "$inbound_block")
    echo "    ✓ inbound delivery and outbound source transaction share Sync height $sync_height"
    assert_proof_for_height "$sync_height" "$node_baseline" "$signer_baseline"
    assert_root_convergence "$sync_height"
}

run_poison_mid_bundle_scenario() {
    local target proxy wrapper survivor_nonce poison_nonce gas_price
    local first_raw poison_raw second_raw first_hash poison_hash second_hash
    local node_baseline signer_baseline source_blocks target_logs target_blocks sync_height
    local poison_target_before deadline evidence first_result second_result

    echo
    echo "==> inbound poison-mid-bundle recovery: deploying an isolated state chain"
    target=$(forge_deploy "$L2" "$L2_DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 \
        'run(uint256)' 0 | grab_address EEZ_VALUE_ADDRESS)
    proxy=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" CreateValueProxy.s.sol:CreateValueProxy \
        'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$target" "$EEZ_ROLLUP_ID" \
        | grab_address EEZ_VALUE_PROXY)
    wrapper=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" DeploySetterWrapperL1.s.sol:DeploySetterWrapperL1 \
        'run(address)' "$proxy" | grab_address EEZ_SETTER_WRAPPER)
    [[ -n "$target" && -n "$proxy" && -n "$wrapper" ]] \
        || { echo "poison recovery deployment failed" >&2; return 1; }

    survivor_nonce=$(retry cast nonce "$INBOUND_SENDER" --rpc-url "$L1")
    poison_nonce=$(retry cast nonce "$POISON_SENDER" --rpc-url "$L1")
    gas_price=$(gas_price_for "$L1")
    poison_target_before=$(retry cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1")

    first_raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$L1")" \
        --private-key "$INBOUND_KEY" --nonce "$survivor_nonce" --gas-limit 800000 \
        --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        "$wrapper" 'setNextValueViaProxy()')
    poison_raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$L1")" \
        --private-key "$POISON_KEY" --nonce "$poison_nonce" --gas-limit 600000 \
        --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        "$L1_VALUE" 'setValue(uint256)' 999)
    second_raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$L1")" \
        --private-key "$INBOUND_KEY" --nonce "$((survivor_nonce + 1))" --gas-limit 800000 \
        --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        "$wrapper" 'setNextValueViaProxy()')
    [[ "$first_raw" =~ ^0x[0-9a-fA-F]+$ && "$poison_raw" =~ ^0x[0-9a-fA-F]+$ \
        && "$second_raw" =~ ^0x[0-9a-fA-F]+$ ]] \
        || { echo "could not build poison recovery transactions" >&2; return 1; }
    first_hash=$(cast keccak "$first_raw")
    poison_hash=$(cast keccak "$poison_raw")
    second_hash=$(cast keccak "$second_raw")

    # The placement is the assertion: valid, deployed non-proxy, valid in one
    # drain. A separate poison sender avoids intentionally evicting a survivor.
    wait_for_sync_boundary
    refresh_node_log
    refresh_signer_log
    node_baseline=$(wc -l <"$NODE_LOG")
    signer_baseline=$(wc -l <"$SIGNER_LOG")
    send_front "$L1F" "$first_raw" "$first_hash"
    send_front "$L1F" "$poison_raw" "$poison_hash"
    send_front "$L1F" "$second_raw" "$second_hash"

    wait_for_receipts "$L1" "$first_hash" "$second_hash"
    first_result=$(wrapped_result "$first_hash" "$L1" "$wrapper")
    second_result=$(wrapped_result "$second_hash" "$L1" "$wrapper")
    [[ "$first_result" == '[1,true,true,1]' && "$second_result" == '[2,true,true,2]' ]] \
        || { echo "poison survivors returned $first_result and $second_result" >&2; return 1; }
    source_blocks=$(unique_receipt_block "$L1" "$first_hash" "$second_hash")
    [[ $(wc -l <<<"$source_blocks" | tr -d ' ') == 1 ]] \
        || { echo "poison survivors landed in different source blocks" >&2; return 1; }

    deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    evidence=""
    while (( SECONDS < deadline )); do
        refresh_node_log
        evidence=$(sed -n "$((node_baseline + 1)),\$p" "$NODE_LOG" | strip_ansi \
            | grep -F '"event_name":"eez.composer.cc_compose.poison_eviction_completed"' \
            | grep -F "\"tx_hash\":\"$poison_hash\"" \
            | grep -F '"direction":"Inbound"' | tail -1 || true)
        [[ -n "$evidence" ]] && break
        sleep 2
    done
    [[ -n "$evidence" && "$(receipt_status "$poison_hash" "$L1")" == "missing" ]] \
        || { echo "deployed non-proxy was not poison-evicted" >&2; return 1; }
    [[ $(retry cast call "$L1_VALUE" 'value()(uint256)' --rpc-url "$L1") == "$poison_target_before" ]] \
        || { echo "poison transaction changed its source target" >&2; return 1; }

    deadline=$((SECONDS + RECEIPT_WAIT_SECS))
    target_logs='[]'
    while (( SECONDS < deadline )); do
        target_logs=$(retry cast logs --address "$target" --from-block 0 --to-block latest \
            'ValueSet(address,uint256)' --rpc-url "$L2" --json)
        (( $(jq -r 'length' <<<"$target_logs") >= 2 )) && break
        sleep 3
    done
    [[ $(jq -r 'length' <<<"$target_logs") == 2 ]] \
        || { echo "poison survivors did not produce exactly two destination effects" >&2; return 1; }
    target_blocks=$(jq -r '[.[].blockNumber] | unique | .[]' <<<"$target_logs")
    [[ $(wc -l <<<"$target_blocks" | tr -d ' ') == 1 ]] \
        || { echo "poison survivors split across Sync blocks" >&2; return 1; }
    [[ $(retry cast call "$target" 'value()(uint256)' --rpc-url "$L2") == "2" ]] \
        || { echo "poison survivor chain did not converge to value 2" >&2; return 1; }

    sync_height=$(cast to-dec "$target_blocks")
    echo "    ✓ poison evicted; both ordered survivors settled in Sync height $sync_height"
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
fund "$L1" "$FUNDING_KEY" "$INBOUND_SENDER"
fund "$L2" "$L2_DEPLOY_KEY" "$OUTBOUND_SENDER"
fund "$L1" "$FUNDING_KEY" "$POISON_SENDER"

echo "==> deploying stateful targets and source-side wrappers"
L2_VALUE=$(forge_deploy "$L2" "$L2_DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 \
    | grab_address EEZ_VALUE_ADDRESS)
L1_VALUE=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 'run(uint256)' 0 \
    | grab_address EEZ_VALUE_ADDRESS)
[[ -n "$L2_VALUE" && -n "$L1_VALUE" ]] || { echo "Value deployment failed"; exit 1; }

INBOUND_PROXY=$(forge_deploy "$L1" "$L1_DEPLOY_KEY" CreateValueProxy.s.sol:CreateValueProxy \
    'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$L2_VALUE" "$EEZ_ROLLUP_ID" \
    | grab_address EEZ_VALUE_PROXY)
OUTBOUND_PROXY=$(create_l2_proxy "$L1_VALUE" "$L2_DEPLOY_KEY" "$L1_ROLLUP_ID")
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
run_scenario inbound mixed "$L1" "$L1F" "$INBOUND_KEY" "$INBOUND_SENDER" \
    "$INBOUND_WRAPPER" "$L2_VALUE" "$L2"
run_scenario outbound source "$L2" "$L2F" "$OUTBOUND_KEY" "$OUTBOUND_SENDER" \
    "$OUTBOUND_WRAPPER" "$L1_VALUE" "$L1"
run_scenario outbound destination "$L2" "$L2F" "$OUTBOUND_KEY" "$OUTBOUND_SENDER" \
    "$OUTBOUND_WRAPPER" "$L1_VALUE" "$L1"
run_scenario outbound mixed "$L2" "$L2F" "$OUTBOUND_KEY" "$OUTBOUND_SENDER" \
    "$OUTBOUND_WRAPPER" "$L1_VALUE" "$L1"
run_poison_mid_bundle_scenario
run_mixed_direction_scenario

echo
echo "==> STATE CHAINING TEST PASSED"
