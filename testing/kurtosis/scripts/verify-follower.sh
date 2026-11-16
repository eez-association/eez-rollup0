#!/usr/bin/env bash
set -euo pipefail

# A fixed safe block commits to the full derived chain, including its state.
# The follower has no L2 peers, so matching it proves reconstruction from L1 DA.
follower="$(kurtosis port print "$KURTOSIS_ENCLAVE" eez-l2-follower l2-rpc)"
case "$follower" in http://*|https://*) ;; *) follower="http://$follower" ;; esac
checkpoint="$(cast block safe --rpc-url "$EEZ_DEVNET_L2_RPC" --json)"
height="$(jq -er '.number' <<<"$checkpoint")"
expected_hash="$(jq -er '.hash' <<<"$checkpoint")"
checkpoint_number="$(cast to-dec "$height")"
(( checkpoint_number > 0 )) || { echo "no settled L2 checkpoint" >&2; exit 1; }
deadline=$((SECONDS + ${EEZ_FOLLOWER_WAIT_SECS:-180}))
while true; do
    safe="$(cast block safe --rpc-url "$follower" --json 2>/dev/null || echo '{}')"
    safe_height="$(jq -r '.number // "0x0"' <<<"$safe")"
    safe_number="$(cast to-dec "$safe_height")"
    if (( safe_number >= checkpoint_number )); then
        derived_hash="$(cast block "$height" --field hash --rpc-url "$follower")"
        canonical_hash="$(cast block "$height" --field hash --rpc-url "$EEZ_DEVNET_L2_RPC")"
        [[ "$derived_hash" == "$expected_hash" && "$canonical_hash" == "$expected_hash" ]] || {
            echo "follower chain mismatch or checkpoint reorg at $height" >&2; exit 1;
        }
        echo "L1-derived follower matches settled L2 block $height ($expected_hash)"
        break
    fi
    (( SECONDS < deadline )) || { echo "follower did not derive settled L2 block $height" >&2; exit 1; }
    sleep 2
done
