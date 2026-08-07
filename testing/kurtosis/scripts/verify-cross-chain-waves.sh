#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${EEZ_CI_RESULT_DIR:?set EEZ_CI_RESULT_DIR}"
mkdir -p "$RESULT_DIR/checks"

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

run_check "cross-chain-wave-mixed-pure" \
    env EEZ_WAVE_MODE=mixed-pure \
        EEZ_WAVE_COUNT="${EEZ_MIXED_PURE_WAVE_COUNT:-3}" \
    bash "$HERE/cross-chain-wave.sh"
