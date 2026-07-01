#!/usr/bin/env bash
# Bootstraps the EEZ protocol contracts onto the private devnet L1.
#
# Why this exists (chicken-and-egg specific to the EMBEDDED L1 model): the
# contracts must be deployed against eez-node's own embedded L1 RPC (there is
# no external, pre-existing chain to deploy against ahead of time, unlike
# Chiado — see README "Why this exists"). But eez-node's composer-mode
# startup reads EEZ_REGISTRY_ADDRESS / EEZ_ROLLUP_ID / etc from env via a
# hard `?` early in `main()` (crates/eez-node/src/main.rs) and exits
# immediately if they're missing — so eez-node can't even boot long enough
# for a deploy script to run against it, and the deploy script can't run
# before eez-node boots. This script breaks the cycle with a disposable
# bootstrap pass:
#
#   1. Launch eez-node with harmless PLACEHOLDER deploy-vars (any
#      syntactically valid address/u64) just to bring the embedded L1 up and
#      let the CL start producing blocks. The bootstrap pass's own Sync-slot
#      postBatches target a nonexistent contract — they land as harmless
#      zero-effect calls (no code at that address), which is fine; we throw
#      this pass's L2 history away entirely.
#   2. Once the L1 RPC is live and producing blocks, run the REAL
#      scripts/deploy.sh against it, capturing the real addresses into
#      infra/devnet-l1/deployments.env.
#   3. Kill the bootstrap eez-node, wipe its L2 datadir (its history used
#      the placeholder addresses and must not be replayed as real), and
#      hand off to run-devnet-l1.sh for the real run.
#
# Usage:  bash infra/devnet-l1/scripts/deploy-eez.sh
#
# Requires the L1 CL to already be up (docker-compose.cl.yml) and past
# GENESIS_DELAY so it's producing blocks — see README step ordering.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
cd "$REPO"

ENV_FILE="${DEVNET_ENV_FILE:-infra/devnet-l1/.env}"
[[ -f "$ENV_FILE" ]] || { echo "no $ENV_FILE — copy infra/devnet-l1/.env.example and edit it" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
# shellcheck disable=SC1090
source "$HERE/devnet-paths.sh"
devnet_resolve_paths
devnet_verify_cl_testnet_dir
: "${EEZ_L1_CHAIN:?}"
: "${EEZ_L1_CHAIN_PATH:?}"
: "${EEZ_L1_JWT_SECRET:?}"
: "${EEZ_L1_RPC_URL:?set EEZ_L1_RPC_URL in .env (self-referential, http://127.0.0.1:$EEZ_L1_HTTP_PORT)}"
: "${EEZ_L1_POSTER_KEY:?}"
: "${EEZ_PROOF_SIGNER_KEY:?}"
if [ "$EEZ_L1_CHAIN" != "devnet" ]; then
    echo "EEZ_L1_CHAIN must be 'devnet' (got '$EEZ_L1_CHAIN')" >&2
    exit 1
fi
[[ -f "$EEZ_L1_CHAIN_PATH" ]] || { echo "EL genesis not found — run gen-genesis.sh first" >&2; exit 1; }

BOOTSTRAP_L2_DATADIR="${EEZ_L2_DATADIR:-/tmp/eez-devnet-l2}-bootstrap"
: "${EEZ_L2_GENESIS_PATH:=./datadir/genesis.json}"
L2_P2P_PORT="${EEZ_L2_P2P_PORT:-30640}"
BOOTSTRAP_LOG="$(mktemp -t eez-devnet-bootstrap.XXXXXX.log)"

cleanup() {
    if [[ -n "${BOOTSTRAP_PID:-}" ]] && kill -0 "$BOOTSTRAP_PID" 2>/dev/null; then
        echo "==> stopping bootstrap eez-node (pid $BOOTSTRAP_PID)"
        kill "$BOOTSTRAP_PID" 2>/dev/null || true
        wait "$BOOTSTRAP_PID" 2>/dev/null || true
    fi
    rm -rf "$BOOTSTRAP_L2_DATADIR"
}
trap cleanup EXIT

echo "==> [1/3] launching disposable bootstrap eez-node (placeholder deploy-vars)"
EEZ_L1_EMBEDDED=1 \
EEZ_L1_CHAIN="$EEZ_L1_CHAIN" \
EEZ_L1_CHAIN_PATH="$EEZ_L1_CHAIN_PATH" \
EEZ_L1_JWT_SECRET="$EEZ_L1_JWT_SECRET" \
EEZ_L1_HTTP_PORT="${EEZ_L1_HTTP_PORT:-18545}" \
EEZ_L1_AUTH_PORT="${EEZ_L1_AUTH_PORT:-18546}" \
EEZ_L1_P2P_PORT="${EEZ_L1_P2P_PORT:-30444}" \
EEZ_L1_DATADIR="${EEZ_L1_DATADIR:?}" \
EEZ_L1_RPC_URL="$EEZ_L1_RPC_URL" \
EEZ_L1_BUILDER_RPC_URL="${EEZ_L1_BUILDER_RPC_URL:-$EEZ_L1_RPC_URL}" \
EEZ_L1_CHAIN_ID="${EEZ_L1_CHAIN_ID:-7331}" \
EEZ_L1_POSTER_KEY="$EEZ_L1_POSTER_KEY" \
EEZ_PROOF_SIGNER_KEY="$EEZ_PROOF_SIGNER_KEY" \
EEZ_L2_SYSTEM_KEY="${EEZ_L2_SYSTEM_KEY:?}" \
EEZ_L2_SYSTEM_ADDRESS="${EEZ_L2_SYSTEM_ADDRESS:?}" \
EEZ_CCM_L2_ADDRESS="${EEZ_CCM_L2_ADDRESS:?}" \
EEZ_L1_BLOCK_TIME_MS="${EEZ_L1_BLOCK_TIME_MS:-12000}" \
EEZ_L2_BLOCK_TIME_MS="${EEZ_L2_BLOCK_TIME_MS:-2000}" \
EEZ_PROOF_TIME_MS="${EEZ_PROOF_TIME_MS:-4000}" \
EEZ_SUBMISSION_SLACK_MS="${EEZ_SUBMISSION_SLACK_MS:-1500}" \
EEZ_REGISTRY_ADDRESS=0x000000000000000000000000000000000000dEaD \
EEZ_MOCK_PROOF_SYSTEM_ADDRESS=0x000000000000000000000000000000000000dEaD \
EEZ_ROLLUP_ID=0 \
EEZ_REGISTRY_DEPLOY_BLOCK=0 \
    cargo run -p eez-node -- node \
        --chain="$EEZ_L2_GENESIS_PATH" \
        --datadir="$BOOTSTRAP_L2_DATADIR" \
        --http --http.addr=0.0.0.0 --http.port=18689 \
        --http.api=eth,net,web3 \
        --authrpc.addr=127.0.0.1 --authrpc.port=18685 \
        --port="$((L2_P2P_PORT + 1))" \
        --ipcdisable --disable-discovery \
        >"$BOOTSTRAP_LOG" 2>&1 &
BOOTSTRAP_PID=$!
echo "    pid=$BOOTSTRAP_PID  log=$BOOTSTRAP_LOG"

echo "==> [1/3] waiting for embedded L1 RPC ($EEZ_L1_RPC_URL) and block production"
for i in $(seq 1 90); do
    if ! kill -0 "$BOOTSTRAP_PID" 2>/dev/null; then
        echo "bootstrap eez-node exited early — see $BOOTSTRAP_LOG" >&2
        tail -n 60 "$BOOTSTRAP_LOG" >&2
        exit 1
    fi
    head="$(cast block-number --rpc-url "$EEZ_L1_RPC_URL" 2>/dev/null || true)"
    if [[ -n "$head" && "$head" != "0" ]]; then
        echo "    L1 at block $head — CL is driving the embedded EL"
        break
    fi
    (( i == 90 )) && { echo "L1 never advanced past block 0 after 90 tries — check $BOOTSTRAP_LOG and the CL logs" >&2; exit 1; }
    sleep 3
done

echo "==> [2/3] deploying EEZ protocol contracts against $EEZ_L1_RPC_URL"
DEPLOY_ENV="$(mktemp -t eez-devnet-deploy-env.XXXXXX)"
{
    echo "EEZ_L1_RPC_URL=$EEZ_L1_RPC_URL"
    echo "EEZ_L1_POSTER_KEY=$EEZ_L1_POSTER_KEY"
    echo "EEZ_PROOF_SIGNER_KEY=$EEZ_PROOF_SIGNER_KEY"
} >"$DEPLOY_ENV"
trap 'rm -f "$DEPLOY_ENV"' EXIT

OUT_FILE="$REPO/infra/devnet-l1/deployments.env"
EEZ_ENV_FILE="$DEPLOY_ENV" \
EEZ_DEPLOYMENTS_FILE="$OUT_FILE" \
EEZ_DEPLOY_SKIP_SIMULATION=1 \
EEZ_GENESIS_OUT="$REPO/$EEZ_L2_GENESIS_PATH" \
    "$REPO/scripts/deploy.sh"

echo "==> [3/3] stopping bootstrap eez-node + discarding its (placeholder) L2 history"
kill "$BOOTSTRAP_PID" 2>/dev/null || true
wait "$BOOTSTRAP_PID" 2>/dev/null || true
unset BOOTSTRAP_PID
rm -rf "$BOOTSTRAP_L2_DATADIR"

echo
echo "deployed. wrote $OUT_FILE"
echo "next: rm -rf \"\${EEZ_L2_DATADIR:-/tmp/eez-devnet-l2}\" (fresh L2 for the real run), then:"
echo "  bash infra/devnet-l1/scripts/run-devnet-l1.sh"
