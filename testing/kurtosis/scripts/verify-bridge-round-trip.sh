#!/usr/bin/env bash
# Deposit through the reference bridge, then withdraw the same amount after
# both the composer and an independent L1-only follower have derived it.
set -euo pipefail
export FOUNDRY_DISABLE_NIGHTLY_WARNING=1

K="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$K/../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-ci}"
RESULT_DIR="${EEZ_CI_RESULT_DIR:?set EEZ_CI_RESULT_DIR}/checks"
mkdir -p "$RESULT_DIR"
RESULT_DIR="$(cd "$RESULT_DIR" && pwd)"
DEPLOY_DIR="$(mktemp -d "${TMPDIR:-/tmp}/eez-bridge.XXXXXX")"
FOLLOWER=""
cleanup() {
    local status=$?
    trap - EXIT
    if [[ -n "$FOLLOWER" ]]; then
        docker logs "$FOLLOWER" >"$RESULT_DIR/bridge-follower.log" 2>&1 || true
        docker rm -f "$FOLLOWER" >/dev/null 2>&1 || true
    fi
    rm -rf "$DEPLOY_DIR"
    exit "$status"
}
trap cleanup EXIT
for tool in cast forge jq curl kurtosis openssl docker python3; do
    command -v "$tool" >/dev/null || { echo "$tool not found" >&2; exit 1; }
done
# shellcheck disable=SC1091
source "$K/ports.sh" >/dev/null
# shellcheck disable=SC1091
source "$K/scripts/lib.sh"
L1="$EEZ_DEVNET_L1_RPC"
L2="$EEZ_DEVNET_L2_RPC"
kurtosis files download "$ENCLAVE" eez-deployments "$DEPLOY_DIR" >/dev/null
set -a
# shellcheck disable=SC1091
source "$DEPLOY_DIR/deployments.env"
set +a
: "${EEZ_L1_BRIDGE_ADDRESS:?deployment is missing its reference bridge}"
DEPLOY_KEY="${EEZ_TEST_DEPLOY_KEY:-0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a}"
ADMIN="$(cast wallet address --private-key "$DEPLOY_KEY")"
L2_BRIDGE=$(forge_deploy "$L2" "$DEPLOY_KEY" DeployBridge.s.sol:DeployBridge \
    'run(address,uint64,address)' "$EEZL2_ADDRESS" "$EEZ_ROLLUP_ID" "$ADMIN" | grab_address BRIDGE)
[[ -n "$L2_BRIDGE" ]] || { echo "L2 bridge deployment failed" >&2; exit 1; }

L1_KEY="0x$(openssl rand -hex 32)"
L2_KEY="0x$(openssl rand -hex 32)"
L1_USER="$(cast wallet address --private-key "$L1_KEY")"
L2_USER="$(cast wallet address --private-key "$L2_KEY")"
AMOUNT=1000000000000000
WAIT_SECS="${EEZ_BRIDGE_WAIT_SECS:-300}"
fund "$L1" "$DEPLOY_KEY" "$L1_USER"
# Reserve source-chain gas separately, so the deposited principal can return
# in full. All value assertions below account for the actual receipt fees.
reserve="$(python3 - "$(gas_price_for "$L2")" <<'PY'
import sys
print(2 * 600000 * int(sys.argv[1]))
PY
)"
cast send "$L2_USER" --value "$reserve" --private-key "$DEPLOY_KEY" \
    --gas-price "$(gas_price_for "$L2")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
    --rpc-url "$L2" >/dev/null
L1_BEFORE="$(cast balance "$L1_USER" --rpc-url "$L1")"
L2_BEFORE="$(cast balance "$L2_USER" --rpc-url "$L2")"

# This follower has no sequencer RPC or shared datadir. On the Linux CI
# runner, host networking lets it reach Kurtosis's published L1 endpoints.
read -r follower_port auth_port < <(python3 - <<'PY'
import socket
with socket.socket() as http, socket.socket() as auth:
    http.bind(('127.0.0.1', 0))
    auth.bind(('127.0.0.1', 0))
    print(http.getsockname()[1], auth.getsockname()[1])
PY
)
FOLLOWER_RPC="http://127.0.0.1:$follower_port"
FOLLOWER=$(docker run -d --network host \
    --mount "type=bind,src=$DEPLOY_DIR,dst=/genesis,readonly" \
    -e EEZ_L1_RPC_URL="$L1" -e EEZ_L1_CHAIN_ID="$(cast chain-id --rpc-url "$L1")" \
    -e EEZ_REGISTRY_ADDRESS -e EEZ_REGISTRY_DEPLOY_BLOCK -e EEZ_ROLLUP_ID -e EEZL2_ADDRESS \
    -e EEZ_L1_BLOCK_TIME_MS="$(yaml_value l1_block_time_ms)" \
    -e EEZ_L2_BLOCK_TIME_MS="$(yaml_value l2_block_time_ms)" \
    -e EEZ_PROOF_TIME_MS="$(yaml_value proof_time_ms)" \
    -e EEZ_SUBMISSION_SLACK_MS="$(yaml_value submission_slack_ms)" \
    --entrypoint eez-follower "${EEZ_NODE_IMAGE:?set EEZ_NODE_IMAGE}" node \
    --chain=/genesis/l2-genesis.json --datadir=/data/l2 \
    --http --http.addr=127.0.0.1 --http.port="$follower_port" --http.api=eth \
    --authrpc.port="$auth_port" --port=0 --discovery.port=0 --disable-discovery --ipcdisable)

assert_value() {
    [[ "$2" == "$3" ]] || { echo "$1: actual=$2 expected=$3" >&2; exit 1; }
}
add_wei() {
    python3 - "$1" "$2" <<'PY'
import sys
print(int(sys.argv[1]) + int(sys.argv[2]))
PY
}
escrow_at() {
    cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
        "$EEZ_ROLLUP_ID" --block "$1" --rpc-url "$L1" | sed -n '3s/ .*//p'
}

# Bind the state assertions to one L1 commitment and its exact L2 block. The
# follower must have derived at least that safe height, not merely imported it.
wait_checkpoint() {
    local expected_balance="$1" expected_escrow="$2" deadline l1_block roots commitment safe height hash follower_safe
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        l1_block=$(cast block-number --rpc-url "$L1")
        roots=$(cast call "$EEZ_REGISTRY_ADDRESS" 'rollups(uint64)(address,bytes32,uint256)' \
            "$EEZ_ROLLUP_ID" --block "$l1_block" --rpc-url "$L1")
        commitment=$(sed -n '2p' <<<"$roots" | tr -d '[:space:]')
        safe=$(cast block safe --rpc-url "$L2" --json)
        height=$(cast to-dec "$(jq -r '.number' <<<"$safe")")
        hash=$(jq -r '.hash' <<<"$safe")
        follower_safe=$(cast block safe --rpc-url "$FOLLOWER_RPC" --json 2>/dev/null || true)
        if [[ "${hash,,}" == "${commitment,,}" && -n "$follower_safe" ]] \
            && (( $(cast to-dec "$(jq -r '.number // "0x0"' <<<"$follower_safe")") >= height )) \
            && [[ "$(cast balance "$L2_USER" --block "$height" --rpc-url "$L2")" == "$expected_balance" \
               && "$(escrow_at "$l1_block")" == "$expected_escrow" ]]; then
            assert_value "follower block hash" \
                "$(cast block "$height" --field hash --rpc-url "$FOLLOWER_RPC")" "$hash"
            assert_value "follower recipient balance" \
                "$(cast balance "$L2_USER" --block "$height" --rpc-url "$FOLLOWER_RPC")" "$expected_balance"
            CHECKPOINT_HEIGHT="$height"
            CHECKPOINT_HASH="$hash"
            return 0
        fi
        sleep 2
    done
    echo "bridge state, escrow and follower did not converge" >&2
    return 1
}
wait_receipt() {
    local hash="$1" rpc="$2" deadline status
    deadline=$((SECONDS + WAIT_SECS))
    while (( SECONDS < deadline )); do
        status=$(receipt_status "$hash" "$rpc")
        [[ "$status" == 1 ]] && return 0
        [[ "$status" != 0x0 ]] || { echo "bridge transaction reverted: $hash" >&2; return 1; }
        sleep 2
    done
    echo "bridge transaction did not settle: $hash" >&2
    return 1
}
receipt_fee() {
    receipt_json "$1" "$2" | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(int(r["gasUsed"],16)*int(r["effectiveGasPrice"],16))'
}
submit_bridge() {
    local rpc="$1" front="$2" key="$3" bridge="$4" target="$5" recipient="$6" sender raw
    sender=$(cast wallet address --private-key "$key")
    raw=$(cast mktx --chain-id "$(cast chain-id --rpc-url "$rpc")" --private-key "$key" \
        --nonce "$(cast nonce "$sender" --rpc-url "$rpc")" --gas-limit 600000 \
        --gas-price "$(gas_price_for "$rpc")" --priority-gas-price "$PRIORITY_GAS_PRICE" \
        --value "$AMOUNT" "$bridge" 'bridgeEther(uint64,address)' "$target" "$recipient")
    TX_HASH=$(cast keccak "$raw")
    send_front "$front" "$raw" "$TX_HASH"
    wait_receipt "$TX_HASH" "$rpc"
}

ESCROW_BEFORE="$(escrow_at latest)"
wait_checkpoint "$L2_BEFORE" "$ESCROW_BEFORE"
submit_bridge "$L1" "$EEZ_DEVNET_L1_FRONT" "$L1_KEY" "$EEZ_L1_BRIDGE_ADDRESS" "$EEZ_ROLLUP_ID" "$L2_USER"
DEPOSIT_HASH="$TX_HASH"
DEPOSIT_FEE="$(receipt_fee "$DEPOSIT_HASH" "$L1")"
wait_checkpoint "$(add_wei "$L2_BEFORE" "$AMOUNT")" "$(add_wei "$ESCROW_BEFORE" "$AMOUNT")"
DEPOSIT_HEIGHT="$CHECKPOINT_HEIGHT"
DEPOSIT_BLOCK="$CHECKPOINT_HASH"
assert_value "deposit sender balance" "$(cast balance "$L1_USER" --rpc-url "$L1")" \
    "$(add_wei "$L1_BEFORE" "$((-AMOUNT - DEPOSIT_FEE))")"

# The withdrawal starts only after the deposit is safe on both nodes.
submit_bridge "$L2" "$EEZ_DEVNET_L2_FRONT" "$L2_KEY" "$L2_BRIDGE" 0 "$L1_USER"
WITHDRAWAL_HASH="$TX_HASH"
WITHDRAWAL_FEE="$(receipt_fee "$WITHDRAWAL_HASH" "$L2")"
wait_checkpoint "$(add_wei "$L2_BEFORE" "$((-WITHDRAWAL_FEE))")" "$ESCROW_BEFORE"
assert_value "deposit remains in the safe chain" \
    "$(cast block "$DEPOSIT_HEIGHT" --field hash --rpc-url "$L2")" "$DEPOSIT_BLOCK"
assert_value "follower retains the deposit checkpoint" \
    "$(cast block "$DEPOSIT_HEIGHT" --field hash --rpc-url "$FOLLOWER_RPC")" "$DEPOSIT_BLOCK"
assert_value "returned L1 principal" "$(cast balance "$L1_USER" --rpc-url "$L1")" \
    "$(add_wei "$L1_BEFORE" "$((-DEPOSIT_FEE))")"
assert_value "L1 bridge retains no ETH" "$(cast balance "$EEZ_L1_BRIDGE_ADDRESS" --rpc-url "$L1")" 0
assert_value "L2 bridge retains no ETH" "$(cast balance "$L2_BRIDGE" --block "$CHECKPOINT_HEIGHT" --rpc-url "$L2")" 0
jq -n --arg deposit "$DEPOSIT_HASH" --arg withdrawal "$WITHDRAWAL_HASH" \
    --arg deposit_hash "$DEPOSIT_BLOCK" --arg withdrawal_hash "$CHECKPOINT_HASH" \
    --argjson deposit_height "$DEPOSIT_HEIGHT" --argjson withdrawal_height "$CHECKPOINT_HEIGHT" \
    --arg amount "$AMOUNT" --arg escrow "$ESCROW_BEFORE" \
    '{deposit: $deposit, withdrawal: $withdrawal, amount: $amount, escrow_before_and_after: $escrow,
      deposit_checkpoint: {height: $deposit_height, hash: $deposit_hash},
      withdrawal_checkpoint: {height: $withdrawal_height, hash: $withdrawal_hash}}' \
    >"$RESULT_DIR/bridge-round-trip.json"
echo "reference bridge ETH round-trip passed, including L1-only follower replay"
