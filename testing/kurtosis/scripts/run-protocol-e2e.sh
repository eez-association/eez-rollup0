#!/usr/bin/env bash
# Run supported eez-core-protocol scenarios against the CI enclave.
# The full scenario suite runs separately against Anvil in the normal CI.
set -euo pipefail

KURTOSIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$KURTOSIS_DIR/../.." && pwd)"
PROTOCOL="$REPO/eez-core-protocol"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-e2e}/protocol-e2e"

SUPPORTED_TARGETS=(
    one_way                           # bridge, counter, and counterL2
    revert/L1_to_L2/revertCounter     # one-hop forced-revert scenario
)

UNSUPPORTED_TARGETS=(
    multi_call                         # nested replay
    multi_tx                           # chained state
    nested                             # nested replay
    reentrant                          # reentrancy
    revert/L1_to_L2/nestedCallRevert   # nested replay
    revert/L1_to_L2/revertContinue     # nested replay
    revert/L2_to_L1/revertContinueL2   # nested replay
    revert/L2_to_L1/revertCounterL2    # L2-to-L1 forced revert
)

for tool in bash bc cast forge git jq kurtosis; do
    command -v "$tool" >/dev/null || { echo "$tool not found in PATH" >&2; exit 1; }
done

expected_protocol="$(git -C "$REPO" ls-files -s eez-core-protocol | awk '{print $2}')"
actual_protocol="$(git -C "$PROTOCOL" rev-parse HEAD)"
if [[ -z "$expected_protocol" || "$actual_protocol" != "$expected_protocol" ]]; then
    echo "eez-core-protocol is not at the commit pinned by this checkout" >&2
    echo "expected: ${expected_protocol:-missing}; found: $actual_protocol" >&2
    echo "Run: git submodule update --init --recursive eez-core-protocol" >&2
    exit 1
fi

source "$KURTOSIS_DIR/ports.sh" >/dev/null
L1_RPC="${L1_RPC:-$EEZ_DEVNET_L1_RPC}"
L2_RPC="${L2_RPC:-$EEZ_DEVNET_L2_RPC}"
L1_FRONT="${L1_FRONT:-$EEZ_DEVNET_L1_FRONT}"
L2_FRONT="${L2_FRONT:-$EEZ_DEVNET_L2_FRONT}"

mkdir -p "$RESULT_DIR"
deploy_dir="$(mktemp -d /tmp/eez-protocol-deployments.XXXXXX)"
trap 'rm -rf "$deploy_dir"' EXIT

kurtosis files download "$ENCLAVE" eez-deployments "$deploy_dir" >/dev/null
set -a
source "$deploy_dir/deployments.env"
set +a

ROLLUPS="${ROLLUPS:-$EEZ_REGISTRY_ADDRESS}"
MANAGER_L2="${MANAGER_L2:-${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}}"

# Hardhat account #2, shared with the earlier cross-chain wave.
PK="${PK:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
SENDER="$(cast wallet address --private-key "$PK")"

echo "==> protocol E2E endpoints"
echo "    protocol:     $actual_protocol"
echo "    L1 RPC/front: $L1_RPC / $L1_FRONT"
echo "    L2 RPC/front: $L2_RPC / $L2_FRONT"
echo "    registry:     $ROLLUPS"

(
    cd "$PROTOCOL"
    bash script/e2e/run/prepare-network.sh \
        --l1-rpc "$L1_RPC" \
        --l2-rpc "$L2_RPC" \
        --pk "$PK" \
        --rollups "$ROLLUPS"
)

# prepare-network.sh only warns if its L2 funding bridge fails.
min_l2_balance=10000000000000000
l2_balance="$(cast balance "$SENDER" --rpc-url "$L2_RPC")"
if (( $(echo "$l2_balance < $min_l2_balance" | bc) )); then
    echo "L2 balance for $SENDER is $l2_balance wei, below the $min_l2_balance wei minimum" >&2
    exit 1
fi

env_file="$deploy_dir/chain.env"
cat >"$env_file" <<ENV
L1_RPC=$L1_RPC
L1_FRONT=$L1_FRONT
L2_RPC=$L2_RPC
L2_FRONT=$L2_FRONT
ROLLUPS=$ROLLUPS
MANAGER_L2=$MANAGER_L2
PK=$PK
ENV

if [[ -n "${EEZ_PROTOCOL_SCENARIOS:-}" ]]; then
    read -r -a targets <<<"$EEZ_PROTOCOL_SCENARIOS"
else
    targets=("${SUPPORTED_TARGETS[@]}")
fi

run_log="$RESULT_DIR/network-sequential.log"
rm -rf "$PROTOCOL/tmp/e2e-network"
status=0
(
    cd "$PROTOCOL"
    DEVNET_ENV="$env_file" \
        bash script/e2e/run/network-sequential.sh "${targets[@]}"
) >"$run_log" 2>&1 || status=$?
cat "$run_log"
if [[ -d "$PROTOCOL/tmp/e2e-network" ]]; then
    cp -R "$PROTOCOL/tmp/e2e-network/." "$RESULT_DIR/"
fi

passed=()
failed=()
skipped=()
while read -r name verdict; do
    case "$verdict" in
        PASS) passed+=("$name") ;;
        FAIL) failed+=("$name") ;;
        SKIP) skipped+=("$name") ;;
    esac
done < <(sed -n 's/^RESULT \([^:]*\): \([A-Z]*\).*/\1 \2/p' "$run_log")

json_array() {
    jq -cn --args '$ARGS.positional' "$@"
}

jq -n \
    --arg protocol_commit "$actual_protocol" \
    --argjson passed "$(json_array ${passed[@]+"${passed[@]}"})" \
    --argjson failed "$(json_array ${failed[@]+"${failed[@]}"})" \
    --argjson skipped "$(json_array ${skipped[@]+"${skipped[@]}"})" \
    --argjson targets "$(json_array ${targets[@]+"${targets[@]}"})" \
    --argjson unsupported "$(json_array "${UNSUPPORTED_TARGETS[@]}")" \
    '{protocol_commit: $protocol_commit, targets: $targets, passed: $passed,
      failed: $failed, skipped: $skipped, unsupported: $unsupported}' \
    >"$RESULT_DIR/summary.json"

echo "==> protocol E2E result: ${#passed[@]} passed, ${#failed[@]} failed, ${#skipped[@]} skipped"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Protocol network E2E"
        echo
        echo "- Targets run: ${targets[*]}"
        echo "- Passed: ${#passed[@]}"
        echo "- Failed: ${#failed[@]}"
        echo "- Skipped (local-only): ${#skipped[@]}"
        echo "- Unsupported on this node: ${UNSUPPORTED_TARGETS[*]}"
    } >>"$GITHUB_STEP_SUMMARY"
fi

if (( status != 0 && ${#failed[@]} == 0 )); then
    echo "network-sequential.sh exited $status without reporting a failed scenario" >&2
    exit "$status"
fi
(( ${#failed[@]} == 0 ))
