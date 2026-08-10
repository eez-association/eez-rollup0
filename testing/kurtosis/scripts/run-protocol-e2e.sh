#!/usr/bin/env bash
# Run supported eez-core-protocol scenarios against the CI enclave.
# The full scenario suite runs separately against Anvil in the normal CI.
set -euo pipefail

KURTOSIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$KURTOSIS_DIR/../.." && pwd)"
PROTOCOL="$REPO/eez-core-protocol"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-e2e}/protocol-e2e"

SUPPORTED_SCENARIOS=(
    bridge
    counter
    counterL2
    helloWorld
    revertCounter
)

# These table shapes are not yet supported by the Kurtosis network path.
UNSUPPORTED_SCENARIOS=(
    deepNested
    multi-call-nested
    multi-call-nestedL2
    multi-call-twice
    multi-call-two-diff
    nestedCallRevert
    nestedCounter
    nestedCounterL2
    reentrant
    revertContinue
    revertContinueL2
    revertCounterL2
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

port_url() {
    local value
    value="$(kurtosis port print "$ENCLAVE" "$1" "$2" 2>/dev/null || true)"
    case "$value" in
        http://* | https://*) printf '%s\n' "$value" ;;
        "") printf '\n' ;;
        *) printf 'http://%s\n' "$value" ;;
    esac
}

export EEZ_PROTOCOL_L1_RPC="${L1_RPC:-$(port_url el-1-reth-lighthouse rpc)}"
export EEZ_PROTOCOL_L2_RPC="${L2_RPC:-$(port_url eez-node l2-rpc)}"
export EEZ_PROTOCOL_L1_FRONT="${L1_FRONT:-$(port_url eez-node l1-xchain)}"
export EEZ_PROTOCOL_L2_FRONT="${L2_FRONT:-$(port_url eez-node l2-xchain)}"

for endpoint in EEZ_PROTOCOL_L1_RPC EEZ_PROTOCOL_L2_RPC EEZ_PROTOCOL_L1_FRONT EEZ_PROTOCOL_L2_FRONT; do
    [[ -n "${!endpoint}" ]] || { echo "could not resolve $endpoint from enclave $ENCLAVE" >&2; exit 1; }
done

mkdir -p "$RESULT_DIR"
deploy_dir="$(mktemp -d /tmp/eez-protocol-deployments.XXXXXX)"
router_dir="$(mktemp -d /tmp/eez-protocol-cast.XXXXXX)"
e2e_base="$PROTOCOL/script/e2e/shared/E2EBase.sh"
e2e_base_backup="$deploy_dir/E2EBase.sh"
cp "$e2e_base" "$e2e_base_backup"
cleanup() {
    cp "$e2e_base_backup" "$e2e_base"
    rm -rf "$deploy_dir" "$router_dir"
}
trap cleanup EXIT

cat "$KURTOSIS_DIR/scripts/protocol-e2e-network-compat.sh" >>"$e2e_base"

kurtosis files download "$ENCLAVE" eez-deployments "$deploy_dir" >/dev/null
set -a
# shellcheck disable=SC1091
source "$deploy_dir/deployments.env"
set +a

ROLLUPS="${ROLLUPS:-$EEZ_REGISTRY_ADDRESS}"
MANAGER_L2="${MANAGER_L2:-${EEZL2_ADDRESS:-0x4200000000000000000000000000000000000007}}"
# Hardhat account #2 is prefunded and unused by the CI services.
PK="${PK:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"

echo "==> protocol E2E endpoints"
echo "    protocol:     $actual_protocol"
echo "    L1 RPC/front: $EEZ_PROTOCOL_L1_RPC / $EEZ_PROTOCOL_L1_FRONT"
echo "    L2 RPC/front: $EEZ_PROTOCOL_L2_RPC / $EEZ_PROTOCOL_L2_FRONT"
echo "    registry:     $ROLLUPS"

(
    cd "$PROTOCOL"
    bash script/e2e/shared/prepare-network.sh \
        --l1-rpc "$EEZ_PROTOCOL_L1_RPC" \
        --l2-rpc "$EEZ_PROTOCOL_L2_RPC" \
        --pk "$PK" \
        --rollups "$ROLLUPS"
)

EEZ_REAL_CAST="$(command -v cast)"
export EEZ_REAL_CAST
ln -s "$KURTOSIS_DIR/scripts/protocol-e2e-cast" "$router_dir/cast"

if [[ -n "${EEZ_PROTOCOL_SCENARIOS:-}" ]]; then
    read -r -a scenarios <<<"$EEZ_PROTOCOL_SCENARIOS"
else
    scenarios=("${SUPPORTED_SCENARIOS[@]}")
fi

passed=()
failed=()
for scenario in "${scenarios[@]}"; do
    sol="script/e2e/$scenario/E2E.s.sol"
    log="$RESULT_DIR/$scenario.log"
    [[ -f "$PROTOCOL/$sol" ]] || { echo "missing protocol scenario: $sol" >&2; exit 1; }

    echo "════════════ RUNNING $scenario ════════════"
    export EEZ_PROTOCOL_L2_SEARCH_START
    EEZ_PROTOCOL_L2_SEARCH_START="$(cast block-number --rpc-url "$EEZ_PROTOCOL_L2_RPC")"
    if (
        cd "$PROTOCOL"
        PATH="$router_dir:$PATH" bash script/e2e/shared/run-network.sh \
            "$sol" \
            --l1-rpc "$EEZ_PROTOCOL_L1_RPC" \
            --l2-rpc "$EEZ_PROTOCOL_L2_RPC" \
            --pk "$PK" \
            --rollups "$ROLLUPS" \
            --manager-l2 "$MANAGER_L2" \
            --l2-rollup-id "${EEZ_ROLLUP_ID:-1}"
    ) >"$log" 2>&1; then
        echo "RESULT $scenario: PASS"
        passed+=("$scenario")
    else
        echo "RESULT $scenario: FAIL"
        tail -n 120 "$log"
        failed+=("$scenario")
    fi
done

json_array() {
    jq -cn --args '$ARGS.positional' "$@"
}

jq -n \
    --arg protocol_commit "$actual_protocol" \
    --argjson passed "$(json_array "${passed[@]}")" \
    --argjson failed "$(json_array "${failed[@]}")" \
    --argjson unsupported "$(json_array "${UNSUPPORTED_SCENARIOS[@]}")" \
    '{protocol_commit: $protocol_commit, passed: $passed, failed: $failed, unsupported: $unsupported}' \
    >"$RESULT_DIR/summary.json"

echo "==> protocol E2E result: ${#passed[@]} passed, ${#failed[@]} failed"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
        echo "### Protocol network E2E"
        echo
        echo "- Passed: ${#passed[@]}"
        echo "- Failed: ${#failed[@]}"
        echo "- Unsupported network scenarios: ${UNSUPPORTED_SCENARIOS[*]}"
    } >>"$GITHUB_STEP_SUMMARY"
fi

(( ${#failed[@]} == 0 ))
