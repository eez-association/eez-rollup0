#!/usr/bin/env bash
set -euo pipefail

L1_RPC="${L1_RPC:-http://127.0.0.1:9555}"
L2_RPC="${L2_RPC:-http://127.0.0.1:9545}"
SLEEP_SECS="${CHECK_SLEEP_SECS:-6}"
SEND_TX="${SEND_TX:-1}"
SENDER_KEY="${EEZ_TX_SENDER_KEY:-0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba}"
RECIPIENT="${CHECK_RECIPIENT:-0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC}"

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "check-devnet: $1 is required" >&2
        exit 1
    }
}

require cast

echo "check-devnet: L1 $L1_RPC"
l1_before="$(cast block-number --rpc-url "$L1_RPC")"
echo "  L1 block: $l1_before"

echo "check-devnet: L2 $L2_RPC"
l2_before="$(cast block-number --rpc-url "$L2_RPC")"
echo "  L2 block: $l2_before"

sleep "$SLEEP_SECS"

l1_after="$(cast block-number --rpc-url "$L1_RPC")"
l2_after="$(cast block-number --rpc-url "$L2_RPC")"
echo "  L1 block after ${SLEEP_SECS}s: $l1_after"
echo "  L2 block after ${SLEEP_SECS}s: $l2_after"

if (( l2_after <= l2_before )); then
    echo "check-devnet: L2 block number did not advance" >&2
    exit 1
fi

if [[ "$SEND_TX" == "1" || "$SEND_TX" == "true" ]]; then
    echo "check-devnet: sending one L2 transfer"
    cast send \
        --rpc-url "$L2_RPC" \
        --private-key "$SENDER_KEY" \
        --value 0.0001ether \
        "$RECIPIENT" >/dev/null
    echo "check-devnet: transfer accepted"
fi

echo "check-devnet: ok"
