#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${EEZ_CI_RESULT_DIR:?set EEZ_CI_RESULT_DIR}"
mkdir -p "$RESULT_DIR/checks"
if [[ -n "${EEZ_SETTLEMENT_SCENARIOS:-}" ]]; then
    printf '[]\n' >"$EEZ_SETTLEMENT_SCENARIOS"
fi

run_check() {
    local name="$1"
    shift
    "$@" 2>&1 | tee "$RESULT_DIR/checks/$name.log"
}

run_wave() {
    local mode="$1" count="$2" include_reverts="${3:-1}" first last
    local manifest="${EEZ_SETTLEMENT_SCENARIOS:-}"
    local transactions="$RESULT_DIR/checks/transactions-$mode.txt"
    if [[ -n "$manifest" ]]; then
        first="$(cast block-number --rpc-url "${L1:?set L1 to the canonical RPC}")"
        first=$((first + 1))
        : >"$transactions"
    fi
    run_check "cross-chain-wave-$mode" \
        env EEZ_WAVE_MODE="$mode" EEZ_WAVE_COUNT="$count" EEZ_INCLUDE_REVERTS="$include_reverts" \
            EEZ_WAVE_TX_MANIFEST="${manifest:+$transactions}" \
        bash "$HERE/cross-chain-wave.sh"
    if [[ -n "$manifest" ]]; then
        last="$(cast block-number --rpc-url "$L1")"
        jq --arg mode "$mode" --arg transactions "$transactions" \
            --argjson first "$first" --argjson last "$last" \
            '. + [{mode: $mode, from: $first, to: $last, transactions: $transactions}]' \
            "$manifest" >"$manifest.tmp"
        mv "$manifest.tmp" "$manifest"
    fi
}

for mode in inbound outbound mixed; do
    run_wave "$mode" 1
done
run_wave mixed-pure "${EEZ_MIXED_PURE_WAVE_COUNT:-3}" "${EEZ_INCLUDE_REVERTS:-0}"

run_check "state-chaining" bash "$HERE/verify-state-chaining.sh"
