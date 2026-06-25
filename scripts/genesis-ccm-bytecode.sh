#!/usr/bin/env bash
#
# Extract the runtime bytecode of `EEZL2` with constructor immutables
# (`ROLLUP_ID`, `SYSTEM_ADDRESS`) baked in, for paste-in to `genesis.json`
# under alloc."0x4200000000000000000000000000000000000007".code (the L2 CCM
# predeploy).
#
# Why: `EEZL2` has two `immutable` constructor params baked into the deployed
# bytecode at construction time, so `forge inspect deployedBytecode` (which
# leaves immutable slots zeroed) is NOT sufficient. We deploy once against an
# ephemeral anvil and read the resulting runtime bytecode via `cast code`.
#
# Ported from based-rollup scripts/genesis-ccm-bytecode.sh (decision B4),
# adapted for eez-rollup0: submodule path `sync-rollups-protocol`, predeploy
# address `0x4200…07`.
#
# Usage:
#   ./scripts/genesis-ccm-bytecode.sh > /tmp/eezl2-runtime.hex
#
# Env overrides:
#   ROLLUP_ID       — uint256 baked into EEZL2.ROLLUP_ID, the REGISTRY id
#                     (registerRollup → 1), NOT the chain id (default: 1)
#   SYSTEM_ADDRESS  — address baked into EEZL2.SYSTEM_ADDRESS (default: anvil #0)
#   ANVIL_PORT      — local port for ephemeral anvil          (default: 8545)
#   PK              — deploy private key                      (default: anvil #0)

set -euo pipefail

ROLLUP_ID=${ROLLUP_ID:-1}
SYSTEM_ADDRESS=${SYSTEM_ADDRESS:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}
ANVIL_PORT=${ANVIL_PORT:-8545}
PK=${PK:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SUBMODULE="$REPO_ROOT/sync-rollups-protocol"

for cmd in anvil forge cast jq; do
    command -v "$cmd" >/dev/null || {
        echo "error: $cmd not found (install foundry: https://getfoundry.sh)" >&2
        exit 1
    }
done

[[ -f "$SUBMODULE/foundry.toml" ]] || {
    echo "error: submodule not initialised at $SUBMODULE" >&2
    echo "       run: git submodule update --init --recursive" >&2
    exit 1
}

cd "$SUBMODULE"

>&2 echo "[1/4] forge build (cached after first run)…"
forge build --silent

ANVIL_LOG=$(mktemp -t eezl2-anvil.XXXXXX)
RPC_URL="http://127.0.0.1:$ANVIL_PORT"

>&2 echo "[2/4] booting ephemeral anvil on $RPC_URL …"
anvil --port "$ANVIL_PORT" --silent >"$ANVIL_LOG" 2>&1 &
ANVIL_PID=$!
trap 'kill $ANVIL_PID 2>/dev/null || true; rm -f "$ANVIL_LOG"' EXIT

for _ in {1..40}; do
    if cast block-number --rpc-url "$RPC_URL" >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
cast block-number --rpc-url "$RPC_URL" >/dev/null || {
    echo "error: anvil never became ready" >&2
    cat "$ANVIL_LOG" >&2
    exit 1
}

>&2 echo "[3/4] deploying EEZL2(ROLLUP_ID=$ROLLUP_ID, SYSTEM_ADDRESS=$SYSTEM_ADDRESS) …"
DEPLOY_JSON=$(forge create \
    src/L2/EEZL2.sol:EEZL2 \
    --rpc-url "$RPC_URL" \
    --private-key "$PK" \
    --broadcast \
    --json \
    --constructor-args "$ROLLUP_ID" "$SYSTEM_ADDRESS")

DEPLOYED_ADDR=$(echo "$DEPLOY_JSON" | jq -r '.deployedTo')
[[ "$DEPLOYED_ADDR" =~ ^0x[0-9a-fA-F]{40}$ ]] || {
    echo "error: could not parse deployed address from forge create output:" >&2
    echo "$DEPLOY_JSON" >&2
    exit 1
}

>&2 echo "       deployed at $DEPLOYED_ADDR"

>&2 echo "[4/4] reading runtime bytecode via cast code …"
RUNTIME_HEX=$(cast code "$DEPLOYED_ADDR" --rpc-url "$RPC_URL")
[[ "$RUNTIME_HEX" =~ ^0x[0-9a-fA-F]+$ ]] || {
    echo "error: cast code returned no bytecode" >&2
    exit 1
}

>&2 echo "       runtime length: ${#RUNTIME_HEX} chars"
>&2 echo
>&2 echo "Paste the following into genesis.json under"
>&2 echo "  alloc.\"0x4200000000000000000000000000000000000007\".code"
>&2 echo

echo "$RUNTIME_HEX"
