#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENV_FILE="${KURTOSIS_ENV_FILE:-$REPO/infra/kurtosis/.env}"
ENDPOINTS="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"

command -v cargo >/dev/null || { echo "cargo not found in PATH" >&2; exit 1; }

[[ -f "$ENV_FILE" ]] || { echo "missing $ENV_FILE" >&2; exit 1; }
[[ -f "$REPO/deployments.env" ]] || { echo "run deploy-eez.sh first" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
[[ -f "$ENDPOINTS" ]] && source "$ENDPOINTS"
source "$REPO/deployments.env"
set +a

: "${EEZ_L1_RPC_URL:?}"
: "${EEZ_L1_BUILDER_RPC_URL:?}"
: "${EEZ_L2_DATADIR:?}"
: "${EEZ_L2_GENESIS_PATH:?}"
[[ -f "$EEZ_L2_GENESIS_PATH" ]] || { echo "missing L2 genesis: $EEZ_L2_GENESIS_PATH" >&2; exit 1; }

mkdir -p "$EEZ_L2_DATADIR"
EEZ_L1_EMBEDDED="${EEZ_L1_EMBEDDED:-0}"

L2_P2P_PORT="${EEZ_L2_P2P_PORT:-30640}"
L2_DISCOVERY_PORT="${EEZ_L2_DISCOVERY_PORT:-$L2_P2P_PORT}"
L2_DISCOVERY_V5_PORT="${EEZ_L2_DISCOVERY_V5_PORT:-$((L2_P2P_PORT + 1))}"

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
