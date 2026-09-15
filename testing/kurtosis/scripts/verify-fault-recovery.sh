#!/usr/bin/env bash
# Exercise real service outages while binding recovery to transaction effects,
# L1/L2 root convergence, and exact safe-chain replay.
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:-$REPO/artifacts/kurtosis-e2e}"
DEPLOY_DIR="$(mktemp -d "${TMPDIR:-/tmp}/eez-fault-recovery.XXXXXX")"
SIGNER_STOPPED=0
NODE_STOPPED=0

cleanup() {
    local status=$?
    trap - EXIT
    if (( NODE_STOPPED )); then
        kurtosis service start "$ENCLAVE" eez-node >/dev/null 2>&1 || true
    fi
    if (( SIGNER_STOPPED )); then
        kurtosis service start "$ENCLAVE" eez-proof-signer >/dev/null 2>&1 || true
    fi
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

DEPLOY_KEY="${EEZ_FAULT_DEPLOY_KEY:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
USER_KEY="${EEZ_FAULT_USER_KEY:-0x$(openssl rand -hex 32)}"
USER="$(cast wallet address --private-key "$USER_KEY")"
WAIT_SECS="${EEZ_FAULT_WAIT_SECS:-300}"

wait_l1_blocks() {
    local count="$1" start deadline current
    start=$(cast block-number --rpc-url "$L1")
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        current=$(cast block-number --rpc-url "$L1" 2>/dev/null || echo "$start")
        (( current >= start + count )) && return 0
        sleep 2
    done
    echo "L1 did not advance by $count blocks" >&2
    return 1
}

wait_receipt_ok() {
    local hash="$1" deadline status
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        status=$(receipt_status "$hash" "$L1")
        [[ "$status" == "1" ]] && return 0
        [[ "$status" != "0x0" ]] || { echo "transaction $hash reverted" >&2; return 1; }
        sleep 3
    done
    echo "transaction $hash did not settle after recovery" >&2
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
    local deadline l1_root l1_recheck safe_json safe_height l2_root
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        l1_root=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
        safe_json=$(cast block safe --rpc-url "$L2" --json 2>/dev/null || true)
        l2_root=$(jq -r '.stateRoot // empty' <<<"$safe_json")
        safe_height=$(jq -r '.number // empty' <<<"$safe_json")
        l1_recheck=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --rpc-url "$L1" 2>/dev/null | sed -n '2p' | tr -d '[:space:]')
        if [[ -n "$l1_root" && -n "$safe_height" \
            && "${l1_root,,}" == "${l1_recheck,,}" \
            && "${l1_recheck,,}" == "${l2_root,,}" ]]; then
            SAFE_HEIGHT=$(cast to-dec "$safe_height")
            SAFE_HASH=$(jq -r '.hash' <<<"$safe_json")
            return 0
        fi
        sleep 3
    done
    echo "L1 committed root and L2 safe root did not converge" >&2
    return 1
}

wait_l2_rpc() {
    local deadline
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        cast block-number --rpc-url "$L2" >/dev/null 2>&1 && return 0
        sleep 3
    done
    echo "L2 RPC did not recover after eez-node restart" >&2
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

mkdir -p "$RESULT_DIR/checks"
echo "==> preparing signer and node fault-recovery fixture"
fund "$L1" "$DEPLOY_KEY" "$USER"
TARGET=$(forge_deploy "$L2" "$DEPLOY_KEY" DeployValueL2.s.sol:DeployValueL2 \
    'run(uint256)' 0 | grab_address EEZ_VALUE_ADDRESS)
PROXY=$(forge_deploy "$L1" "$DEPLOY_KEY" CreateValueProxy.s.sol:CreateValueProxy \
    'run(address,address,uint64)' "$EEZ_REGISTRY_ADDRESS" "$TARGET" "$EEZ_ROLLUP_ID" \
    | grab_address EEZ_VALUE_PROXY)
[[ -n "$TARGET" && -n "$PROXY" ]] || { echo "fault fixture deployment failed" >&2; exit 1; }

nonce=$(cast nonce "$USER" --rpc-url "$L1")
raw=$(build_inbound "$nonce" 41)
hash=$(cast keccak "$raw")

echo "==> stopping proof signer with an inbound transaction pending"
kurtosis service stop "$ENCLAVE" eez-proof-signer
SIGNER_STOPPED=1
send_front "$L1F" "$raw" "$hash"
wait_l1_blocks 2
[[ "$(receipt_status "$hash" "$L1")" == "missing" ]] \
    || { echo "transaction settled without an available proof signer" >&2; exit 1; }
[[ "$(cast call "$TARGET" 'value()(uint256)' --rpc-url "$L2")" == "0" ]] \
    || { echo "unattested transaction changed destination state" >&2; exit 1; }

echo "==> restarting proof signer and requiring pending plus fresh progress"
kurtosis service start "$ENCLAVE" eez-proof-signer
SIGNER_STOPPED=0
wait_receipt_ok "$hash"
wait_value 41
wait_root_convergence
pre_restart_height="$SAFE_HEIGHT"
pre_restart_hash="$SAFE_HASH"

echo "==> restarting eez-node and checking exact safe-chain persistence"
l1_before_restart=$(cast block-number --rpc-url "$L1")
kurtosis service stop "$ENCLAVE" eez-node
NODE_STOPPED=1
wait_l1_blocks 1
(( $(cast block-number --rpc-url "$L1") > l1_before_restart )) \
    || { echo "canonical L1 did not remain live during the node outage" >&2; exit 1; }
kurtosis service start "$ENCLAVE" eez-node
NODE_STOPPED=0
# Service restarts normally retain published ports; refresh them instead of
# making that an implicit harness assumption.
# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
L2="$EEZ_DEVNET_L2_RPC"
L1F="$EEZ_DEVNET_L1_FRONT"
wait_l2_rpc
replayed=$(cast block "$pre_restart_height" --rpc-url "$L2" --json | jq -r '.hash')
[[ "${replayed,,}" == "${pre_restart_hash,,}" ]] \
    || { echo "restarted node changed safe-prefix block $pre_restart_height" >&2; exit 1; }
wait_root_convergence
(( SAFE_HEIGHT >= pre_restart_height )) \
    || { echo "safe head retreated after node restart" >&2; exit 1; }

nonce=$(cast nonce "$USER" --rpc-url "$L1")
fresh_raw=$(build_inbound "$nonce" 43)
fresh_hash=$(cast keccak "$fresh_raw")
send_front "$L1F" "$fresh_raw" "$fresh_hash"
wait_receipt_ok "$fresh_hash"
wait_value 43
wait_root_convergence
(( SAFE_HEIGHT > pre_restart_height )) \
    || { echo "fresh transaction did not advance the safe chain after restart" >&2; exit 1; }

jq -n \
    --arg pending_tx "$hash" \
    --arg fresh_tx "$fresh_hash" \
    --argjson replayed_height "$pre_restart_height" \
    --arg replayed_hash "$pre_restart_hash" \
    --argjson recovered_safe_height "$SAFE_HEIGHT" \
    '{
        signer_outage_pending_tx: $pending_tx,
        post_restart_fresh_tx: $fresh_tx,
        replayed_safe_block: {height: $replayed_height, hash: $replayed_hash},
        recovered_safe_height: $recovered_safe_height
    }' >"$RESULT_DIR/checks/fault-recovery.json"

echo "    ✓ signer outage preserved safety and recovered the pending transaction"
echo "    ✓ node restart preserved the exact safe prefix and settled fresh traffic"
