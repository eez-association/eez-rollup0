#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${EEZ_CONFIG_FILE:-/config/.env}"
DEPLOYMENTS_ENV_FILE="${EEZ_DEPLOYMENTS_ENV_FILE:-${EEZ_DEPLOYMENTS_ENV:-/shared/deployments.env}}"
NODE_NAME="${EEZ_NODE_NAME:-sequencer}"

[[ -f "$CONFIG_FILE" ]] || {
    echo "start-sequencer: config file $CONFIG_FILE not found" >&2
    exit 1
}
[[ -f "$DEPLOYMENTS_ENV_FILE" ]] || {
    echo "start-sequencer: deployments env $DEPLOYMENTS_ENV_FILE not found" >&2
    exit 1
}

set -a
# shellcheck disable=SC1090
source "$CONFIG_FILE"
# shellcheck disable=SC1090
source "$DEPLOYMENTS_ENV_FILE"
set +a

# Per-node overrides for multi-sequencer deployments. They are applied after
# sourcing the shared files so each service can keep a distinct L1 poster.
[[ -n "${EEZ_NODE_L1_POSTER_KEY:-}" ]] && export EEZ_L1_POSTER_KEY="$EEZ_NODE_L1_POSTER_KEY"
[[ -n "${EEZ_NODE_COMPOSER_INTERVAL_SECS:-}" ]] && export EEZ_COMPOSER_INTERVAL_SECS="$EEZ_NODE_COMPOSER_INTERVAL_SECS"
[[ -n "${EEZ_NODE_COMPOSER_EXPECT_EXTERNAL_BATCHES:-}" ]] && export EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES="$EEZ_NODE_COMPOSER_EXPECT_EXTERNAL_BATCHES"
[[ -n "${EEZ_NODE_COMPOSER_DISABLED:-}" ]] && export EEZ_COMPOSER_DISABLED="$EEZ_NODE_COMPOSER_DISABLED"
[[ -n "${EEZ_NODE_SEQUENCER_DISABLED:-}" ]] && export EEZ_SEQUENCER_DISABLED="$EEZ_NODE_SEQUENCER_DISABLED"

# Local devnets do not run a separate builder relay. Use the L1 RPC endpoint
# unless a deployment explicitly supplies a builder URL.
: "${EEZ_L1_BUILDER_RPC_URL:=${EEZ_L1_RPC_URL:-}}"
[[ -n "$EEZ_L1_BUILDER_RPC_URL" ]] || {
    echo "start-sequencer: EEZ_L1_RPC_URL or EEZ_L1_BUILDER_RPC_URL must be set" >&2
    exit 1
}
export EEZ_L1_BUILDER_RPC_URL

GENESIS_PATH="${EEZ_NODE_L2_GENESIS_PATH:-${EEZ_L2_GENESIS_PATH:-/shared/genesis-l2.json}}"
DATADIR="${EEZ_NODE_DATADIR:-/data}"

[[ -f "$GENESIS_PATH" ]] || {
    echo "start-sequencer: L2 genesis $GENESIS_PATH not found" >&2
    exit 1
}

echo "start-sequencer: ${NODE_NAME}"
echo "  genesis     = ${GENESIS_PATH}"
echo "  datadir     = ${DATADIR}"
echo "  l1 rpc      = ${EEZ_L1_RPC_URL:-unset}"
echo "  builder rpc = ${EEZ_L1_BUILDER_RPC_URL:-unset}"
echo "  poster      = $(cast wallet address --private-key "${EEZ_L1_POSTER_KEY:-}" 2>/dev/null || echo unknown)"
echo "  external    = ${EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES:-false}"
echo "  interval    = ${EEZ_COMPOSER_INTERVAL_SECS:-60}s"

exec eez-node node \
    --chain "$GENESIS_PATH" \
    --datadir "$DATADIR" \
    --http \
    --http.addr=0.0.0.0 \
    --http.port=8545 \
    --http.api=debug,trace,txpool,eth,net,web3 \
    --http.corsdomain=* \
    --ws \
    --ws.addr=0.0.0.0 \
    --ws.port=8546 \
    --ws.api=debug,trace,txpool,eth,net,web3 \
    --engine.persistence-threshold 256 \
    --engine.memory-block-buffer-target 128
