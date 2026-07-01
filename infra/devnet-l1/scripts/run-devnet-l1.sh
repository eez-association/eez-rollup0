#!/usr/bin/env bash
# Launch eez-node in composer mode with the embedded L1 reth running as a
# CL-driven EthereumNode on the private devnet genesis (EEZ_L1_CHAIN=devnet).
#
# Order of operations for a fresh Phase 1 run:
#   1. bash infra/devnet-l1/scripts/gen-genesis.sh
#   2. docker compose --env-file infra/devnet-l1/.env \
#        -f infra/devnet-l1/docker-compose.cl.yml up -d
#   3. bash infra/devnet-l1/scripts/deploy-eez.sh      (writes deployments.env)
#   4. bash infra/devnet-l1/scripts/run-devnet-l1.sh   (this script)
#
# The CL will sit at slot 0 until GENESIS_DELAY elapses, then start driving
# the embedded EL via engine API. eez-node's embedded reth produces NO blocks
# on its own (no auto-mine) — every L1 block arrives via newPayload from
# lighthouse. That is the whole point of Phase 1.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
cd "$REPO"

ENV_FILE="${DEVNET_ENV_FILE:-infra/devnet-l1/.env}"
if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
else
    echo "no $ENV_FILE — copy infra/devnet-l1/.env.example and edit it" >&2
    exit 1
fi

: "${EEZ_L1_CHAIN:?}"
: "${EEZ_L1_CHAIN_PATH:?}"
: "${EEZ_L1_JWT_SECRET:?}"
if [ "$EEZ_L1_CHAIN" != "devnet" ]; then
    echo "EEZ_L1_CHAIN must be 'devnet' for this script (got '$EEZ_L1_CHAIN')" >&2
    exit 1
fi
if [ ! -f "$EEZ_L1_CHAIN_PATH" ]; then
    echo "EL genesis not found at $EEZ_L1_CHAIN_PATH — run gen-genesis.sh first" >&2
    exit 1
fi
if [ ! -f "$EEZ_L1_JWT_SECRET" ]; then
    echo "JWT not found at $EEZ_L1_JWT_SECRET — run gen-genesis.sh first" >&2
    exit 1
fi

# deploy-eez.sh writes here (registry/proof-system/rollupId/deploy-block).
# eez-node's own dotenvy::from_filename("deployments.env") only looks in
# CWD ($REPO), not infra/devnet-l1/ — source explicitly so composer mode
# has EEZ_REGISTRY_ADDRESS etc without colliding with a root-level
# deployments.env from the Chiado/Dev workflow.
DEPLOYMENTS_FILE="${DEVNET_DEPLOYMENTS_FILE:-infra/devnet-l1/deployments.env}"
if [ -f "$DEPLOYMENTS_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$DEPLOYMENTS_FILE"
    set +a
else
    echo "no $DEPLOYMENTS_FILE — run infra/devnet-l1/scripts/deploy-eez.sh first" >&2
    exit 1
fi
: "${EEZ_REGISTRY_ADDRESS:?}"
: "${EEZ_ROLLUP_ID:?}"

: "${EEZ_L2_GENESIS_PATH:=./datadir/genesis.json}"
L2_P2P_PORT="${EEZ_L2_P2P_PORT:-30640}"

echo "==> launching eez-node (embedded devnet L1, CL-driven)"
echo "    EL genesis : $EEZ_L1_CHAIN_PATH"
echo "    L1 authrpc : 127.0.0.1:${EEZ_L1_AUTH_PORT:-18546} (lighthouse dials here)"
echo "    L1 RPC     : 127.0.0.1:${EEZ_L1_HTTP_PORT:-18545}"

exec cargo run -p eez-node -- node \
    --chain="$EEZ_L2_GENESIS_PATH" \
    --datadir="$EEZ_L2_DATADIR" \
    --http --http.addr=0.0.0.0 --http.port="${EEZ_L2_HTTP_PORT:-18688}" \
    --http.api=eth,net,web3 \
    --authrpc.addr=127.0.0.1 --authrpc.port="${EEZ_L2_AUTH_PORT:-18684}" \
    --port="$L2_P2P_PORT" \
    --ipcdisable --disable-discovery
