#!/usr/bin/env bash
# Prove the live L1 cross-chain front rejects a nonce gap, then settles the
# contiguous pair that fills it. Destination state must not move on the reject.
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-e2e}"
DEPLOY_DIR="$(mktemp -d "${TMPDIR:-/tmp}/eez-ingress-admission.XXXXXX")"

cleanup() {
    local status=$?
    trap - EXIT
    rm -rf "$DEPLOY_DIR"
    exit "$status"
}
trap cleanup EXIT

for tool in cast curl forge jq kurtosis openssl; do
    command -v "$tool" >/dev/null || { echo "$tool not found in PATH" >&2; exit 1; }
done

# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
# shellcheck disable=SC1091
source "$K/scripts/lib.sh"
L1="$EEZ_DEVNET_L1_RPC"
L2="$EEZ_DEVNET_L2_RPC"
L1F="$EEZ_DEVNET_L1_FRONT"

kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null
set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/deployments.env"
set +a
: "${EEZ_REGISTRY_ADDRESS:?deployments.env is missing EEZ_REGISTRY_ADDRESS}"
: "${EEZ_ROLLUP_ID:?deployments.env is missing EEZ_ROLLUP_ID}"

DEPLOY_KEY="${EEZ_INGRESS_DEPLOY_KEY:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
USER_KEY="${EEZ_INGRESS_USER_KEY:-0x$(openssl rand -hex 32)}"
USER="$(cast wallet address --private-key "$USER_KEY")"
WAIT_SECS="${EEZ_INGRESS_WAIT_SECS:-300}"

wait_receipt_ok() {
    local hash="$1" deadline status
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        status=$(receipt_status "$hash" "$L1")
        [[ "$status" == "1" ]] && return 0
        [[ "$status" != "0x0" ]] || { echo "transaction $hash reverted" >&2; return 1; }
        sleep 3
    done
    echo "transaction $hash did not settle" >&2
    return 1
}

wait_value() {
    local expected="$1" deadline value
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        value=$(cast call "$TARGET" 'value()(uint256)' --rpc-url "$L2" 2>/dev/null || true)
        [[ "$value" == "$expected" ]] && return 0
        sleep 3
    done
    echo "destination value did not reach $expected (last=$value)" >&2
    return 1
}

wait_root_convergence() {
    local deadline l1_root l1_recheck safe_json l2_root
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        l1_root=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
        safe_json=$(cast block safe --rpc-url "$L2" --json 2>/dev/null || true)
        l2_root=$(jq -r '.stateRoot // empty' <<<"$safe_json")
        l1_recheck=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
        if [[ -n "$l1_root" \
            && "${l1_root,,}" == "${l1_recheck,,}" \
            && "${l1_recheck,,}" == "${l2_root,,}" ]]; then
            return 0
        fi
        sleep 3
    done
    echo "L1 committed root and L2 safe root did not converge" >&2
    return 1
}

build_inbound() {
    local nonce="$1" value="$2" gas_price
    gas_price=$(gas_price_for "$L1")
    cast mktx --chain-id "$(cast chain-id --rpc-url "$L1")" \
        --private-key "$USER_KEY" --nonce "$nonce" --gas-limit 700000 \
        --gas-price "$gas_price" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        "$PROXY" 'setValue(uint256)' "$value"
}

# Like send_front, but the expected outcome is an invalid-nonce rejection.
reject_gap_nonce() {
    local front="$1" raw="$2" resp rc i
    for ((i = 0; i < 120; i++)); do
        resp=$(curl -sS --max-time 10 -X POST "$front" -H 'Content-Type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"],\"id\":1}" 2>/dev/null); rc=$?
        (( rc == 0 )) && [[ -n "$resp" ]] \
            || { echo "    ✗ gap-nonce submit failed (curl rc=$rc, ${#resp} byte body)" >&2; return 1; }
        if grep -q '"error"' <<<"$resp"; then
            grep -q 'starting up' <<<"$resp" && { sleep 1; continue; }
            grep -qi 'invalid nonce' <<<"$resp" && return 0
            echo "    ✗ front rejected the gap nonce for an unexpected reason: $resp" >&2
            return 1
        fi
        echo "    ✗ front admitted nonce N+1 before N: $resp" >&2
        return 1
    done
    echo "    ✗ front still starting up after 120s" >&2
    return 1
}

mkdir -p "$RESULT_DIR/checks"
echo "==> preparing ingress-admission fixture"
fund "$L1" "$DEPLOY_KEY" "$USER"
TARGET=$(forge_deploy "$L2" "$DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 \
    'run(uint256)' 0 | grab_address EEZ_VALUE_ADDRESS)
PROXY=$(forge_deploy "$L1" "$DEPLOY_KEY" CreateValueProxy.s.sol:CreateValueProxy \
    'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$TARGET" "$EEZ_ROLLUP_ID" \
    | grab_address EEZ_VALUE_PROXY)
[[ -n "$TARGET" && -n "$PROXY" ]] || { echo "ingress fixture deployment failed" >&2; exit 1; }

nonce=$(cast nonce "$USER" --rpc-url "$L1")
gap_raw=$(build_inbound "$((nonce + 1))" 91)
gap_hash=$(cast keccak "$gap_raw")

echo "==> rejecting inbound nonce $((nonce + 1)) before $nonce"
reject_gap_nonce "$L1F" "$gap_raw"
[[ "$(receipt_status "$gap_hash" "$L1")" == "missing" ]] \
    || { echo "rejected gap nonce still produced an L1 receipt" >&2; exit 1; }
[[ "$(cast call "$TARGET" 'value()(uint256)' --rpc-url "$L2")" == "0" ]] \
    || { echo "rejected gap nonce changed destination state" >&2; exit 1; }
[[ "$(cast nonce "$USER" --rpc-url "$L1")" == "$nonce" ]] \
    || { echo "rejected gap nonce consumed the source-chain nonce" >&2; exit 1; }

echo "==> settling contiguous nonces $nonce and $((nonce + 1))"
first_raw=$(build_inbound "$nonce" 91)
first_hash=$(cast keccak "$first_raw")
send_front "$L1F" "$first_raw" "$first_hash"
second_raw=$(build_inbound "$((nonce + 1))" 92)
second_hash=$(cast keccak "$second_raw")
send_front "$L1F" "$second_raw" "$second_hash"
wait_receipt_ok "$first_hash"
wait_receipt_ok "$second_hash"
wait_value 92
wait_root_convergence

jq -n \
    --arg gap_tx "$gap_hash" \
    --arg first_tx "$first_hash" \
    --arg second_tx "$second_hash" \
    --argjson nonce "$nonce" \
    '{
        rejected_gap_tx: $gap_tx,
        settled_nonce: $nonce,
        settled_next_nonce_tx: $second_tx,
        first_contiguous_tx: $first_tx
    }' >"$RESULT_DIR/checks/ingress-admission.json"

echo "    ✓ L1 front rejected nonce N+1 before N without destination mutation"
echo "    ✓ contiguous N and N+1 settled and L1/L2 roots converged"
