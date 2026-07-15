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

for mode in inbound outbound mixed; do
    run_check "cross-chain-wave-$mode" \
        env EEZ_WAVE_MODE="$mode" EEZ_WAVE_COUNT=1 \
        bash "$HERE/cross-chain-wave.sh"
done

jq -n \
    --arg result pass \
    --arg candidate_image "${EEZ_NODE_IMAGE:-unknown}" \
    '{
        result: $result,
        candidate_image: $candidate_image,
        modes: ["inbound", "outbound", "mixed"],
        cross_chain_convergence: "pass",
        l1_l2_root_divergence: 0,
        safe_head_convergence: "pass"
    }' >"$RESULT_DIR/result.json"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Production-path result"
        echo
        echo "- Candidate: \`${EEZ_NODE_IMAGE:-unknown}\`"
        echo "- Inbound wave: pass"
        echo "- Outbound wave: pass"
        echo "- Mixed wave: pass"
        echo "- L1/L2 root divergence: 0"
        echo "- L2 safe head: converged"
    } >>"$GITHUB_STEP_SUMMARY"
fi
