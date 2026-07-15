#!/usr/bin/env bash
# Verify that a conflicting-nonce bundle is rejected atomically.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

RPC="${EEZ_L1_RPC_URL:?set EEZ_L1_RPC_URL}"
BUILDER="${EEZ_L1_BUILDER_RPC_URL:?set EEZ_L1_BUILDER_RPC_URL}"
KEY="${EEZ_BUNDLE_PROBE_KEY:?set EEZ_BUNDLE_PROBE_KEY}"
SLOT="${EEZ_L1_SLOT_SECONDS:-4}"
SLACK="${EEZ_BUILDER_PROBE_SLACK:-3}"
ADDR="$(cast wallet address --private-key "$KEY")"

head="$(cast block-number --rpc-url "$RPC")"
head_ts="$(cast block "$head" --field timestamp --rpc-url "$RPC")"
target=$((head + SLACK))
target_ts=$((head_ts + SLACK * SLOT))
nonce="$(cast nonce "$ADDR" --block pending --rpc-url "$RPC")"

# Both transactions are individually valid at the current state, but they
# cannot execute sequentially because they deliberately use the same nonce.
raw_first="$(cast mktx "$ADDR" --value 0 --nonce "$nonce" --private-key "$KEY" --rpc-url "$RPC")"
raw_conflict="$(cast mktx "$ADDR" --value 1 --nonce "$nonce" --private-key "$KEY" --rpc-url "$RPC")"
hash_first="$(cast keccak "$raw_first")"
hash_conflict="$(cast keccak "$raw_conflict")"

body="$(printf '{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":["%s","%s"],"blockNumber":"0x%x","minTimestamp":%s,"maxTimestamp":%s}]}' \
    "$raw_first" "$raw_conflict" "$target" "$target_ts" "$target_ts")"
response="$(curl -sS "$BUILDER" -H 'Content-Type: application/json' -d "$body")"
echo "builder atomic rejection: first=$hash_first conflict=$hash_conflict target=$target response=$response"

deadline=$((SECONDS + SLOT * (SLACK + 8)))
while (( $(cast block-number --rpc-url "$RPC") <= target + 1 )); do
    (( SECONDS < deadline )) || { echo "builder atomic rejection: L1 stalled" >&2; exit 2; }
    sleep 2
done

first_landed=0
conflict_landed=0
cast receipt "$hash_first" --rpc-url "$RPC" >/dev/null 2>&1 && first_landed=1
cast receipt "$hash_conflict" --rpc-url "$RPC" >/dev/null 2>&1 && conflict_landed=1

if (( first_landed != 0 || conflict_landed != 0 )); then
    echo "builder atomic rejection FAIL: first=$first_landed conflict=$conflict_landed" >&2
    exit 1
fi

echo "builder atomic rejection PASS"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "- Conflicting-nonce negative bundle: fully dropped" >>"$GITHUB_STEP_SUMMARY"
fi
