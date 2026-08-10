# eez-core-protocol@6fcc90b no longer correlates batches through L2 block refs.
# Locate the L1 batch by its expected ExecutionConsumed hashes instead.
find_batch_block_by_l2_ref() {
    local _l2_block="$1"
    local start_block="$2"
    local end_block="$3"
    local rollups="$4"
    local rpc="$5"
    local block batch_tx

    : "$_l2_block"
    for ((block = end_block; block >= start_block; block--)); do
        if verify_l1_batch \
            "$rpc" "$block" "$rollups" "${EXPECTED_L1_CALL_HASHES:-[]}"; then
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
