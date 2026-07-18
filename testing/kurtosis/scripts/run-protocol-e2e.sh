#!/usr/bin/env bash
# Run the node-compatible counter scenario against an existing Kurtosis enclave.
set -euo pipefail

KURTOSIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$KURTOSIS_DIR/../.." && pwd)"
PROTOCOL="$REPO/sync-rollups-protocol"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/production-path-ci}/protocol-e2e"
EXPECTED_PROTOCOL="5c51e02b0f965ee8c94e9ed2c7e0e9f924d41fba"

for tool in bash bc cast forge git jq kurtosis; do
    command -v "$tool" >/dev/null || { echo "$tool not found in PATH" >&2; exit 1; }
done

actual_protocol="$(git -C "$PROTOCOL" rev-parse HEAD)"
if [[ "$actual_protocol" != "$EXPECTED_PROTOCOL" ]]; then
    echo "protocol E2E requires $EXPECTED_PROTOCOL; found $actual_protocol" >&2
    echo "Run: git submodule update --init --recursive sync-rollups-protocol" >&2
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
cleanup() {
    rm -rf "$deploy_dir" "$router_dir"
}
trap cleanup EXIT

kurtosis files download "$ENCLAVE" eez-deployments "$deploy_dir" >/dev/null
set -a
source "$deploy_dir/deployments.env"
set +a

ROLLUPS="${ROLLUPS:-$EEZ_REGISTRY_ADDRESS}"
MANAGER_L2="${MANAGER_L2:-${EEZ_CCM_L2_ADDRESS:-0x4200000000000000000000000000000000000007}}"
# Hardhat account #2 is prefunded on both Kurtosis chains and is not used by
# the poster/prover services.
PK="${PK:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"

echo "==> protocol E2E endpoints"
echo "    L1 RPC/front: $EEZ_PROTOCOL_L1_RPC / $EEZ_PROTOCOL_L1_FRONT"
echo "    L2 RPC/front: $EEZ_PROTOCOL_L2_RPC / $EEZ_PROTOCOL_L2_FRONT"
echo "    registry:     $ROLLUPS"

# CREATE2 factories are idempotent; account #2 is already funded on both
# chains by the Kurtosis genesis configuration.
(
    cd "$PROTOCOL"
    bash script/e2e/shared/prepare-network.sh \
        --l1-rpc "$EEZ_PROTOCOL_L1_RPC" \
        --l2-rpc "$EEZ_PROTOCOL_L2_RPC" \
        --pk "$PK" \
        --rollups "$ROLLUPS"
)

EEZ_REAL_CAST="$(command -v cast)"
EEZ_REAL_FORGE="$(command -v forge)"
EEZ_PROTOCOL_VERIFY_FILE="$KURTOSIS_DIR/contracts/VerifyProtocol5c.s.sol"
export EEZ_REAL_CAST EEZ_REAL_FORGE EEZ_PROTOCOL_VERIFY_FILE
ln -s "$KURTOSIS_DIR/scripts/protocol-e2e-cast" "$router_dir/cast"
ln -s "$KURTOSIS_DIR/scripts/protocol-e2e-forge" "$router_dir/forge"

log="$RESULT_DIR/counter.log"
echo "════════════ RUNNING counter ════════════"
if (
    cd "$PROTOCOL"
    PATH="$router_dir:$PATH" bash script/e2e/shared/run-network.sh \
        script/e2e/counter/E2E.s.sol \
        --l1-rpc "$EEZ_PROTOCOL_L1_RPC" \
        --l2-rpc "$EEZ_PROTOCOL_L2_RPC" \
        --pk "$PK" \
        --rollups "$ROLLUPS" \
        --manager-l2 "$MANAGER_L2"
) >"$log" 2>&1; then
    echo "RESULT counter: PASS"
else
    echo "RESULT counter: FAIL"
    tail -n 80 "$log"
    exit 1
fi
