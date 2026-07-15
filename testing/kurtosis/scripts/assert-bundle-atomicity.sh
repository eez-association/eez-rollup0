#!/usr/bin/env bash
# Verify all-or-none inclusion and transaction ordering for composer bundles.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

RPC="${EEZ_L1_RPC_URL:?set EEZ_L1_RPC_URL}"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
LOG_FILE="${EEZ_ATOMIC_LOG:-$(mktemp /tmp/eez-atomic-log.XXXXXX)}"

for tool in cast jq kurtosis; do
    command -v "$tool" >/dev/null || { echo "atomic assertion: $tool not found" >&2; exit 1; }
done

kurtosis service logs "$ENCLAVE" eez-node 2>&1 \
    | sed 's/\x1b\[[0-9;]*m//g' >"$LOG_FILE"

if grep -q 'relay has no eth_sendBundle; submitting txs via mempool' "$LOG_FILE"; then
    echo "atomic assertion: composer used mempool fallback instead of rbuilder" >&2
    exit 1
fi

mapfile -t dispatches < <(
    grep 'bundle_tx_hashes=' "$LOG_FILE" \
        | while IFS= read -r line; do
            target="$(sed -E 's/.*target_block=([0-9]+).*/\1/' <<<"$line")"
            hashes="$(sed -E 's/.*bundle_tx_hashes=([^ ]+).*/\1/' <<<"$line")"
            printf '%s|%s\n' "$target" "$hashes"
          done \
        | sort -u
)

if (( ${#dispatches[@]} == 0 )); then
    echo "atomic assertion: no composer bundle hash groups found in eez-node logs" >&2
    exit 1
fi

checked=0
included=0
dropped=0
for dispatch in "${dispatches[@]}"; do
    target="${dispatch%%|*}"
    group="${dispatch#*|}"
    IFS=',' read -r -a hashes <<<"$group"
    (( ${#hashes[@]} > 0 )) || continue

    receipt_count=0
    block=""
    previous_index=-1
    for hash in "${hashes[@]}"; do
        receipt="$(cast receipt "$hash" --rpc-url "$RPC" --json 2>/dev/null || true)"
        [[ -n "$receipt" ]] || continue

        tx_block_hex="$(jq -r '.blockNumber' <<<"$receipt")"
        tx_block_number="$(cast to-dec "$tx_block_hex")"
        # A transaction may be requeued in a later bundle after this target
        # was missed. Only inclusion in this dispatch's target block counts;
        # otherwise a healthy retry would look like partial inclusion here.
        (( tx_block_number == target )) || continue
        receipt_count=$((receipt_count + 1))

        status="$(jq -r '.status' <<<"$receipt")"
        [[ "$status" == "0x1" || "$status" == "1" ]] || {
            echo "atomic assertion: included transaction reverted: $hash status=$status" >&2
            exit 1
        }
        tx_block="$tx_block_hex"
        tx_index_hex="$(jq -r '.transactionIndex' <<<"$receipt")"
        tx_index="$(cast to-dec "$tx_index_hex")"

        if [[ -z "$block" ]]; then
            block="$tx_block"
        elif [[ "$tx_block" != "$block" ]]; then
            echo "atomic assertion: bundle split across blocks ($block and $tx_block): $group" >&2
            exit 1
        fi
        if (( previous_index >= 0 && tx_index != previous_index + 1 )); then
            echo "atomic assertion: bundle order is not contiguous at $hash (index=$tx_index previous=$previous_index)" >&2
            exit 1
        fi
        previous_index=$tx_index
    done

    if (( receipt_count == 0 )); then
        dropped=$((dropped + 1))
    elif (( receipt_count != ${#hashes[@]} )); then
        echo "atomic assertion: PARTIAL INCLUSION in target $target: $receipt_count/${#hashes[@]} transactions landed: $group" >&2
        exit 1
    else
        included=$((included + 1))
    fi
    checked=$((checked + 1))
done

(( included > 0 )) || {
    echo "atomic assertion: observed $checked dispatches but no fully included composer bundle" >&2
    exit 1
}

echo "atomic assertion PASS: checked=$checked fully_included=$included fully_dropped=$dropped partial=0"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Atomic bundle receipts"
        echo
        echo "- Dispatches checked: $checked"
        echo "- Fully included: $included"
        echo "- Fully dropped: $dropped"
        echo "- Partial inclusions: 0"
    } >>"$GITHUB_STEP_SUMMARY"
fi
