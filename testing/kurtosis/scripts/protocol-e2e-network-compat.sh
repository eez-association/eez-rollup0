# eez-core-protocol@6fcc90b no longer correlates batches through L2 block refs.
# Use the expected execution hashes to correlate L1 and L2 activity instead.
verify_l2_settlement() {
    local l1_rpc="$1"
    local rollups="$2"
    local target_block="${L2_BLOCK:-}"
    local deadline=$((SECONDS + ${EEZ_PROTOCOL_SETTLEMENT_TIMEOUT_SECS:-180}))
    local safe_block safe_number l1_root l1_recheck l2_root

    if [[ -z "$target_block" ]]; then
        VERIFY_OUT="FAIL: no L2 trigger block available for settlement verification"
        return 1
    fi

    while ((SECONDS < deadline)); do
        safe_block="$(cast block safe --rpc-url "$L2_RPC" --json)"
        safe_number="$(jq -r '.number' <<<"$safe_block" | xargs cast to-dec)"
        if ((safe_number >= target_block)); then
            l1_root="$(cast call "$rollups" 'rollups(uint64)(address,bytes32,uint256)' \
                "${L2_ROLLUP_ID:-1}" --rpc-url "$l1_rpc" \
                | sed -n '2p' | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
            l2_root="$(jq -r '.stateRoot' <<<"$safe_block" | tr '[:upper:]' '[:lower:]')"
            l1_recheck="$(cast call "$rollups" 'rollups(uint64)(address,bytes32,uint256)' \
                "${L2_ROLLUP_ID:-1}" --rpc-url "$l1_rpc" \
                | sed -n '2p' | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')"
            if [[ "$l1_root" == "$l1_recheck" && "$l1_recheck" == "$l2_root" ]]; then
                VERIFY_OUT="PASS: L2 block $target_block settled at safe block $safe_number"
                return 0
            fi
        fi
        sleep 2
    done

    VERIFY_OUT="FAIL: L2 block $target_block did not reach matching L1/L2 settled state"
    return 1
}

verify_l1_batch() {
    if [[ -z "${4:-}" || "$4" == "[]" ]]; then
        verify_l2_settlement "$1" "$3"
        return
    fi
    _run_verifier \
        VerifyL1Batch "$1" "run(uint256,address,bytes32[])" \
        "$2" "$3" "$4"
}

find_batch_block_by_l2_ref() {
    local _l2_block="$1"
    local start_block="$2"
    local end_block="$3"
    local rollups="$4"
    local rpc="$5"
    local expected_hashes="${EXPECTED_L1_CALL_HASHES:-[]}"
    local block batch_tx

    : "$_l2_block"
    if [[ "$expected_hashes" == "[]" ]]; then
        verify_l2_settlement "$rpc" "$rollups" || return 1
        end_block="$(cast block-number --rpc-url "$rpc")"
    fi

    for ((block = end_block; block >= start_block; block--)); do
        if [[ "$expected_hashes" == "[]" ]]; then
            _run_verifier VerifyL1Batch "$rpc" \
                "run(uint256,address,bytes32[])" "$block" "$rollups" "[]" || continue
        elif ! verify_l1_batch "$rpc" "$block" "$rollups" "$expected_hashes"; then
            continue
        fi
        if [[ -n "$VERIFY_OUT" ]]; then
            batch_tx="$(extract "$VERIFY_OUT" "L1_BATCH_TX")"
            if [[ -n "$batch_tx" ]]; then
                FOUND_L1_BLOCK="$block"
                FOUND_BATCH_TX="$batch_tx"
                return 0
            fi
        fi
    done
    return 1
}

extract_l2_blocks_from_tx() {
    local _batch_tx="$1"
    local _l1_rpc="$2"
    local expected_hashes="${EXPECTED_L2_HASHES:-[]}"
    local next_block="${EEZ_PROTOCOL_L2_SEARCH_START:-0}"
    local deadline=$((SECONDS + ${EEZ_PROTOCOL_L2_SEARCH_TIMEOUT_SECS:-120}))
    local current_block block

    : "$_batch_tx" "$_l1_rpc"
    if [[ -z "$expected_hashes" || "$expected_hashes" == "[]" ]]; then
        echo "[]"
        return 0
    fi

    while ((SECONDS < deadline)); do
        current_block="$(cast block-number --rpc-url "$L2_RPC")"
        for ((block = next_block; block <= current_block; block++)); do
            if verify_l2_table \
                "$L2_RPC" "[$block]" "$MANAGER_L2" "$expected_hashes"; then
                echo "[$block]"
                return 0
            fi
        done
        next_block=$((current_block + 1))
        sleep 2
    done

    echo "[]"
}

# The upstream helper captures failed Forge output without printing it.
deploy_contracts() {
    local sol="$1" l1_rpc="$2" l2_rpc="$3" pk="$4"
    local contracts contract rpc label out

    contracts="$(grep -oE 'contract Deploy[A-Za-z0-9_]* ' "$sol" | awk '{print $2}')"
    [[ -n "$contracts" ]] || { echo "No Deploy* contracts found"; return 1; }

    while IFS= read -r contract; do
        if [[ "$contract" == *L2* ]]; then
            rpc="$l2_rpc"
            label="L2"
        else
            rpc="$l1_rpc"
            label="L1"
        fi
        echo "--- $contract ($label) ---"
        if ! out="$(forge script "$sol:$contract" \
            --rpc-url "$rpc" --broadcast --private-key "$pk" 2>&1)"; then
            echo "$out" >&2
            return 1
        fi
        echo "$out" | sed 's/^[[:space:]]*//' | grep -E '^[A-Z0-9_]+=' | grep -v '^==' || true
        _export_outputs "$out"
    done <<<"$contracts"
}
