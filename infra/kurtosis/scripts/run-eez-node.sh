#!/usr/bin/env bash
# Launch eez-node with its embedded L1 reth on the extracted Kurtosis genesis,
# so it joins Pair B's chain and the composer reads L1 state in-process. The
# reth is CL-driven — the follower beacon feeds it blocks over the engine API.
# Invoked by eez-up.sh; run l1-up.sh first.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENV_FILE="${KURTOSIS_ENV_FILE:-$REPO/infra/kurtosis/.env}"
ENDPOINTS="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"
DATA_DIR="${EEZ_L1_DATA_DIR:-$REPO/infra/kurtosis/eez-l1-data}"

command -v cargo >/dev/null || { echo "cargo not found in PATH" >&2; exit 1; }
[[ -f "$ENV_FILE" ]] || { echo "missing $ENV_FILE (cp infra/kurtosis/eez.env.example)" >&2; exit 1; }
[[ -f "$REPO/deployments.env" ]] || { echo "run deploy-eez.sh first" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
# endpoints.env supplies EEZ_L1_BUILDER_RPC_URL (rbuilder); el-bootnode.env
# supplies EEZ_L1_TRUSTED_PEERS for embedded-reth backfill over RLPx.
[[ -f "$ENDPOINTS" ]] && source "$ENDPOINTS"
[[ -f "$DATA_DIR/el-bootnode.env" ]] && source "$DATA_DIR/el-bootnode.env"
source "$REPO/deployments.env"
set +a

export EEZ_L1_EMBEDDED=1
export EEZ_L1_CHAIN=devnet
export EEZ_L1_CHAIN_PATH="$DATA_DIR/genesis.json"
export EEZ_L1_JWT_SECRET="$DATA_DIR/jwt/jwtsecret"
export EEZ_L1_HTTP_PORT="${EEZ_L1_HTTP_PORT:-18545}"
# Engine API port the follower beacon dials. NOT 18546 (reth WS = http_port+1).
export EEZ_L1_AUTH_PORT="${EEZ_L1_AUTH_PORT:-18551}"
# Dial our OWN embedded L1, not the Kurtosis EL endpoints.env pointed at.
export EEZ_L1_RPC_URL="http://127.0.0.1:${EEZ_L1_HTTP_PORT}"
export EEZ_L1_TARGET_RPC_URL="$EEZ_L1_RPC_URL"
export EEZ_L1_CHAIN_ID="${EEZ_L1_CHAIN_ID:-7331}"

: "${EEZ_L1_BUILDER_RPC_URL:?run parse-endpoints.sh (rbuilder RPC) or set in .env}"
: "${EEZ_L2_DATADIR:?}"
: "${EEZ_L2_GENESIS_PATH:?}"
: "${EEZ_REGISTRY_ADDRESS:?}"
: "${EEZ_ROLLUP_ID:?}"
[[ -f "$EEZ_L1_CHAIN_PATH" ]] || { echo "missing extracted EL genesis: $EEZ_L1_CHAIN_PATH — run extract-genesis.sh" >&2; exit 1; }
[[ -f "$EEZ_L1_JWT_SECRET" ]] || { echo "missing local JWT: $EEZ_L1_JWT_SECRET — run extract-genesis.sh" >&2; exit 1; }
[[ -f "$EEZ_L2_GENESIS_PATH" ]] || { echo "missing L2 genesis: $EEZ_L2_GENESIS_PATH (run deploy-eez.sh)" >&2; exit 1; }

mkdir -p "$EEZ_L2_DATADIR"

L2_P2P_PORT="${EEZ_L2_P2P_PORT:-30640}"
L2_DISCOVERY_PORT="${EEZ_L2_DISCOVERY_PORT:-$L2_P2P_PORT}"
L2_DISCOVERY_V5_PORT="${EEZ_L2_DISCOVERY_V5_PORT:-$((L2_P2P_PORT + 1))}"

echo "==> launching eez-node (EMBEDDED devnet L1, CL-driven, unified harness)"
echo "    EL genesis  : $EEZ_L1_CHAIN_PATH"
echo "    L1 RPC      : 127.0.0.1:${EEZ_L1_HTTP_PORT} (embedded reth)"
echo "    L1 engine   : 127.0.0.1:${EEZ_L1_AUTH_PORT} (follower beacon dials here)"
echo "    builder RPC : $EEZ_L1_BUILDER_RPC_URL (Kurtosis rbuilder)"

exec cargo run -p eez-node -- node \
    --chain="$EEZ_L2_GENESIS_PATH" \
    --datadir="$EEZ_L2_DATADIR" \
    --http --http.addr=0.0.0.0 --http.port="${EEZ_L2_HTTP_PORT:-18688}" \
    --http.api=eth,net,web3 \
    --authrpc.addr=127.0.0.1 --authrpc.port="${EEZ_L2_AUTH_PORT:-18684}" \
    --port="$L2_P2P_PORT" \
    --discovery.port="$L2_DISCOVERY_PORT" \
    --discovery.v5.port="$L2_DISCOVERY_V5_PORT" \
    --ipcdisable --disable-discovery
