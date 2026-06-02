#!/usr/bin/env bash
set -euo pipefail

trap 'echo "tx-sender: shutting down"; exit 0' SIGTERM SIGINT

RPC_LIST="${EEZ_TX_SENDER_RPCS:-${1:-http://sequencer:8545}}"
INTERVAL_SECS="${EEZ_TX_SENDER_INTERVAL_SECS:-12}"
VALUE="${EEZ_TX_SENDER_VALUE:-0.001ether}"

# Foundry/anvil dev account #5. This avoids the deploy/proof key and the
# multi-sequencer L1 poster keys (#0-#3).
SENDER_KEY="${EEZ_TX_SENDER_KEY:-0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba}"

RECIPIENTS=(
    "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
    "0x90F79bf6EB2c4f870365E785982E1f101E93b906"
    "0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65"
)

IFS=',' read -r -a RAW_RPCS <<< "$RPC_LIST"
RPCS=()
for raw in "${RAW_RPCS[@]}"; do
    rpc="${raw//[[:space:]]/}"
    [[ -n "$rpc" ]] && RPCS+=("$rpc")
done

if (( ${#RPCS[@]} == 0 )); then
    echo "tx-sender: no RPCs configured" >&2
    exit 1
fi

echo "tx-sender: targets:"
for rpc in "${RPCS[@]}"; do
    echo "  - $rpc"
done

for rpc in "${RPCS[@]}"; do
    /app/deployments/shared/scripts/wait-rpc.sh "$rpc" "$rpc"
done

round=0
while true; do
    round=$((round + 1))
    echo "tx-sender: round ${round}"

    for i in "${!RPCS[@]}"; do
        rpc="${RPCS[$i]}"
        recipient="${RECIPIENTS[$((i % ${#RECIPIENTS[@]}))]}"
        echo "tx-sender: ${rpc} -> ${recipient} (${VALUE})"
        if ! cast send \
            --rpc-url "$rpc" \
            --private-key "$SENDER_KEY" \
            --value "$VALUE" \
            "$recipient"; then
            echo "tx-sender: send failed for $rpc; continuing" >&2
        fi
    done

    sleep "$INTERVAL_SECS"
done
