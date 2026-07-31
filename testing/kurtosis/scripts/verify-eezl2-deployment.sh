#!/usr/bin/env bash
# Verify the live EEZL2 predeploy against the public deployment bindings.

set -euo pipefail

K="$(cd "$(dirname "$0")/.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"

for tool in cast jq kurtosis; do
    command -v "$tool" >/dev/null || { echo "$tool not in PATH" >&2; exit 1; }
done

http_url() {
    case "$1" in
        http://*|https://*) printf '%s\n' "$1" ;;
        "") printf '\n' ;;
        *) printf 'http://%s\n' "$1" ;;
    esac
}

L2="${L2:-$(http_url "$(kurtosis port print "$ENCLAVE" eez-node l2-rpc 2>/dev/null || true)")}"
[[ -n "$L2" ]] || { echo "could not resolve the enclave L2 RPC" >&2; exit 1; }

deploy_dir="$(mktemp -d /tmp/eez-deployment-check.XXXXXX)"
trap 'rm -rf "$deploy_dir"' EXIT
kurtosis files download "$ENCLAVE" eez-deployments "$deploy_dir" >/dev/null

set -a
# shellcheck disable=SC1091
source "$deploy_dir/deployments.env"
set +a

for name in \
    EEZ_CCM_L2_ADDRESS \
    EEZ_L2_SYSTEM_ADDRESS \
    EEZ_L2_EEZL2_CODE_HASH \
    EEZ_ROLLUP_ID \
    EEZ_INITIAL_STATE_ROOT
do
    [[ -n "${!name:-}" ]] || { echo "$name missing from deployments.env" >&2; exit 1; }
done

actual_system="$(cast call "$EEZ_CCM_L2_ADDRESS" 'SYSTEM_ADDRESS()(address)' --rpc-url "$L2")"
actual_rollup_id="$(cast call "$EEZ_CCM_L2_ADDRESS" 'ROLLUP_ID()(uint64)' --rpc-url "$L2" | awk '{print $1}')"
actual_use_gas_left="$(cast call "$EEZ_CCM_L2_ADDRESS" 'USE_GAS_LEFT()(bool)' --rpc-url "$L2")"
runtime="$(cast code "$EEZ_CCM_L2_ADDRESS" --rpc-url "$L2")"
actual_runtime_hash="$(cast keccak "$runtime")"
actual_state_root="$(cast block 0 --rpc-url "$L2" --json | jq -er '.stateRoot')"

[[ "${actual_system,,}" == "${EEZ_L2_SYSTEM_ADDRESS,,}" ]] || {
    echo "live EEZL2 SYSTEM_ADDRESS mismatch: $actual_system != $EEZ_L2_SYSTEM_ADDRESS" >&2
    exit 1
}
[[ "$actual_rollup_id" == "$EEZ_ROLLUP_ID" ]] || {
    echo "live EEZL2 ROLLUP_ID mismatch: $actual_rollup_id != $EEZ_ROLLUP_ID" >&2
    exit 1
}
[[ "$actual_use_gas_left" == "false" ]] || {
    echo "live EEZL2 USE_GAS_LEFT is $actual_use_gas_left; expected false" >&2
    exit 1
}
[[ "${actual_runtime_hash,,}" == "${EEZ_L2_EEZL2_CODE_HASH,,}" ]] || {
    echo "live EEZL2 runtime hash mismatch: $actual_runtime_hash != $EEZ_L2_EEZL2_CODE_HASH" >&2
    exit 1
}
[[ "${actual_state_root,,}" == "${EEZ_INITIAL_STATE_ROOT,,}" ]] || {
    echo "live L2 genesis state root mismatch: $actual_state_root != $EEZ_INITIAL_STATE_ROOT" >&2
    exit 1
}

echo "EEZL2 deployment bindings verified (rollupId=$EEZ_ROLLUP_ID, system=$EEZ_L2_SYSTEM_ADDRESS)"
