#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${EEZ_CI_RESULT_DIR:?set EEZ_CI_RESULT_DIR}"
mkdir -p "$RESULT_DIR/checks"

write_failure_result() {
    local status=$?
    if (( status != 0 )) && [[ ! -f "$RESULT_DIR/result.json" ]]; then
        jq -n \
            --arg result fail \
            --arg candidate_image "${EEZ_NODE_IMAGE:-unknown}" \
            --argjson exit_code "$status" \
            '{result: $result, candidate_image: $candidate_image, exit_code: $exit_code}' \
            >"$RESULT_DIR/result.json" || true
    fi
}
trap write_failure_result EXIT

run_check() {
    local name="$1"
    shift
    "$@" 2>&1 | tee "$RESULT_DIR/checks/$name.log"
}

run_check builder-timestamp bash "$HERE/assert-builder-timestamp.sh"
run_check builder-atomic-rejection bash "$HERE/assert-builder-atomic-rejection.sh"
run_check cross-chain-wave env EEZ_WAVE_MODE=mixed EEZ_WAVE_COUNT=1 \
    bash "$HERE/cross-chain-wave.sh"
run_check bundle-atomicity bash "$HERE/assert-bundle-atomicity.sh"

atomic_log="$RESULT_DIR/checks/bundle-atomicity.log"
checked="$(sed -nE 's/.*checked=([0-9]+).*/\1/p' "$atomic_log" | tail -1)"
included="$(sed -nE 's/.*fully_included=([0-9]+).*/\1/p' "$atomic_log" | tail -1)"
dropped="$(sed -nE 's/.*fully_dropped=([0-9]+).*/\1/p' "$atomic_log" | tail -1)"
[[ -n "$checked" && -n "$included" && -n "$dropped" ]] || {
    echo "production-path verification did not produce atomicity counts" >&2
    exit 1
}

jq -n \
    --arg result pass \
    --arg candidate_image "${EEZ_NODE_IMAGE:-unknown}" \
    --argjson bundles_attempted "$checked" \
    --argjson fully_included "$included" \
    --argjson fully_dropped "$dropped" \
    '{
        result: $result,
        candidate_image: $candidate_image,
        bundles_attempted: $bundles_attempted,
        fully_included: $fully_included,
        fully_dropped: $fully_dropped,
        partial_inclusions: 0,
        timestamp_bounds: "pass",
        builder_atomic_rejection: "pass",
        healthy_recovery_after_rejection: "pass",
        cross_chain_convergence: "pass",
        l1_l2_root_divergence: 0,
        safe_head_convergence: "pass"
    }' >"$RESULT_DIR/result.json"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Production-path result"
        echo
        echo "- Candidate: \`${EEZ_NODE_IMAGE:-unknown}\`"
        echo "- Timestamp bounds: pass"
        echo "- Conflicting-nonce bundle: fully dropped"
        echo "- Healthy mixed wave after rejection: converged"
        echo "- Composer bundles: $included included, $dropped dropped, 0 partial"
        echo "- L1/L2 root divergence: 0"
        echo "- L2 safe head: converged"
    } >>"$GITHUB_STEP_SUMMARY"
fi
