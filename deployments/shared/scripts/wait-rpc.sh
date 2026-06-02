#!/usr/bin/env bash
set -euo pipefail

RPC_URL="${1:?usage: wait-rpc.sh <rpc-url> [label]}"
LABEL="${2:-$RPC_URL}"
TIMEOUT_SECS="${WAIT_RPC_TIMEOUT_SECS:-120}"

deadline=$((SECONDS + TIMEOUT_SECS))
echo "wait-rpc: waiting for ${LABEL} at ${RPC_URL}"

while true; do
    if curl -sf --max-time 2 \
        -X POST \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$RPC_URL" >/dev/null; then
        echo "wait-rpc: ${LABEL} is ready"
        exit 0
    fi

    if (( SECONDS >= deadline )); then
        echo "wait-rpc: timed out waiting for ${LABEL} after ${TIMEOUT_SECS}s" >&2
        exit 1
    fi

    sleep 2
done
