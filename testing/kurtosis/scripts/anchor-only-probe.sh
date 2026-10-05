#!/usr/bin/env bash
set -euo pipefail

K="$(cd "$(dirname "$0")/.." && pwd)"
: "${L2:=http://127.0.0.1:18688}"
: "${EEZ_NODE_CONTAINER:=eez-node-kurtosis}"
: "${EEZ_ANCHOR_ONLY_WAIT_SECS:=120}"

EEZ_PARITY_MODE=anchor-only \
EEZ_PARITY_ROUNDS="${EEZ_PARITY_ROUNDS:-10}" \
    "$K/scripts/parity-gate-host.sh"

deadline=$((SECONDS + EEZ_ANCHOR_ONLY_WAIT_SECS))
partial=""
record=""
while (( SECONDS < deadline )); do
    partial=$(docker logs "$EEZ_NODE_CONTAINER" 2>&1 | jq -Rrc \
        'fromjson? | select(
            .fields.event_name == "eez.deriver.reconcile.partial_consumption"
            and (((.fields.outbound | tonumber) + (.fields.inbound | tonumber)) > 0)
            and (.fields.outbound_applied | tonumber) == 0
            and (.fields.inbound_applied | tonumber) == 0
        ) | .fields' \
        | tail -1)
    if [[ -n "$partial" ]]; then
        tx_hash=$(jq -r '.tx_hash' <<<"$partial")
        record=$(docker logs "$EEZ_NODE_CONTAINER" 2>&1 | jq -Rrc --arg tx_hash "$tx_hash" \
            'fromjson? | select(
                .fields.event_name == "eez.deriver.safe.advanced"
                and (.fields.applied_entries | tonumber) == 1
                and .fields.tx_hash == $tx_hash
            ) | .fields' | tail -1)
    fi
    [[ -n "$record" ]] && break
    sleep 3
done

[[ -n "$record" ]] || {
    echo "no anchor-only settlement observed within ${EEZ_ANCHOR_ONLY_WAIT_SECS}s" >&2
    exit 1
}

safe_height=$(jq -r '.to_block' <<<"$record")
safe_hash=$(jq -r '.new_safe_hash' <<<"$record")
commitment=$(jq -r '.l1_settled_commitment' <<<"$record")
canonical=$(cast block "$safe_height" --rpc-url "$L2" --json | jq -r '.hash')

[[ "${safe_hash,,}" == "${commitment,,}" ]] || {
    echo "anchor-only safe hash does not match the L1 commitment" >&2
    exit 1
}
[[ "${safe_hash,,}" == "${canonical,,}" ]] || {
    echo "anchor-only safe hash is not canonical at height $safe_height" >&2
    exit 1
}

docker logs "$EEZ_NODE_CONTAINER" 2>&1 \
    | "$K/scripts/settle-height-check.py"
echo "anchor-only settlement verified at Sync height $safe_height ($safe_hash)"
