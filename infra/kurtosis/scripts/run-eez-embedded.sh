#!/usr/bin/env bash
# Launch eez-node with its EMBEDDED L1 reth (EEZ_L1_EMBEDDED=1,
# EEZ_L1_CHAIN=devnet) on the genesis extracted from the Kurtosis enclave, so it
# joins the same chain as Pair B. This is the unified-harness counterpart of
# run-eez-node.sh (which runs EEZ_L1_EMBEDDED=0, settlement-only, no cross-chain).
#
# The embedded reth is CL-driven: it produces no blocks itself. The follower
# beacon (docker-compose.eez-l1.yml) dials its engine API (EEZ_L1_AUTH_PORT) and
# feeds it every block the Kurtosis validators propose — including rbuilder's.
#
# Order:
#   1. kurtosis-up.sh            2. parse-endpoints.sh
#   3. extract-genesis.sh        4. get-cl-bootnode.sh
#   5. cp eez-embedded.env.example .env  (set poster/proof keys)
#   6. deploy-eez.sh             7. docker compose ... eez-l1.yml up -d
#   8. run-eez-embedded.sh       (this script)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENV_FILE="${KURTOSIS_ENV_FILE:-$REPO/infra/kurtosis/.env}"
ENDPOINTS="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"
DATA_DIR="${EEZ_L1_DATA_DIR:-$REPO/infra/kurtosis/eez-l1-data}"

command -v cargo >/dev/null || { echo "cargo not found in PATH" >&2; exit 1; }
[[ -f "$ENV_FILE" ]] || { echo "missing $ENV_FILE (cp infra/kurtosis/eez-embedded.env.example)" >&2; exit 1; }
[[ -f "$REPO/deployments.env" ]] || { echo "run deploy-eez.sh first" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
# endpoints.env supplies EEZ_L1_BUILDER_RPC_URL (rbuilder) — keep that; we
# override the plain L1 RPC below to point at eez-node's OWN embedded reth.
[[ -f "$ENDPOINTS" ]] && source "$ENDPOINTS"
# el-bootnode.env supplies EEZ_L1_TRUSTED_PEERS (a Kurtosis reth enode) so the
# embedded L1 reth can backfill history over RLPx — see get-el-bootnode.sh.
[[ -f "$DATA_DIR/el-bootnode.env" ]] && source "$DATA_DIR/el-bootnode.env"
source "$REPO/deployments.env"
set +a

# ── Embedded L1 config (this is what makes it composer/cross-chain mode) ─────
export EEZ_L1_EMBEDDED=1
export EEZ_L1_CHAIN=devnet
export EEZ_L1_CHAIN_PATH="$DATA_DIR/genesis.json"
export EEZ_L1_JWT_SECRET="$DATA_DIR/jwt/jwtsecret"
export EEZ_L1_HTTP_PORT="${EEZ_L1_HTTP_PORT:-18545}"
# Engine API port the follower beacon dials. NOT 18546 (reth WS = http_port+1).
export EEZ_L1_AUTH_PORT="${EEZ_L1_AUTH_PORT:-18551}"
# eez-node dials its OWN embedded L1 for tip/mode gating — override any Kurtosis
# EL URL that endpoints.env set (same chain, but self is authoritative here).
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
