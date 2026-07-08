#!/usr/bin/env bash
# Live Kurtosis probe: submit an exact-timestamp bundle to rbuilder and pass
# only if the bundled transaction is included on L1.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for t in cast curl kurtosis; do
    command -v "$t" >/dev/null || { echo "bundle-inclusion: $t not found" >&2; exit 1; }
done

# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

DEV_MNEMONIC="giant issue aisle success illegal bike spike question tent bar rely arctic volcano long crawl hungry vocal artwork sniff fantasy very lucky have athlete"

RPC="${EEZ_L1_RPC_URL:-${L1_RPC:-}}"
BUILDER="${EEZ_L1_BUILDER_RPC_URL:-${BUILDER_RPC:-}}"
POSTER_KEY="${EEZ_L1_POSTER_KEY:-}"
SLOT_SECONDS="${EEZ_L1_SLOT_SECONDS:-$(( ${EEZ_L1_BLOCK_TIME_MS:-12000} / 1000 ))}"
SLACK="${EEZ_BUNDLE_TEST_SLACK:-3}"
ATTEMPTS="${EEZ_BUNDLE_TEST_ATTEMPTS:-3}"
WARMUP_BLOCK="${EEZ_MEV_WARMUP_BLOCK:-132}"

require_value() {
    local name="$1" value="$2" hint="$3"
    if [[ -n "$value" ]]; then
        return
    fi

    echo "bundle-inclusion: could not resolve $name" >&2
    echo "  enclave: ${ENCLAVE:-eez-devnet}" >&2
    echo "  $hint" >&2
    echo "  inspect: kurtosis enclave inspect ${ENCLAVE:-eez-devnet}" >&2
    exit 1
}

require_value "EEZ_L1_RPC_URL" "$RPC" \
    "tried configured service plus discovered non-builder el-* services with port ${KURTOSIS_L1_RPC_PORT:-rpc}"
require_value "EEZ_L1_BUILDER_RPC_URL" "$BUILDER" \
    "tried configured service plus discovered builder/rbuilder services with ports ${KURTOSIS_BUILDER_RPC_PORT:-rbuilder-rpc}, rpc, http"
require_value "EEZ_L1_POSTER_KEY" "$POSTER_KEY" \
    "tried: poster_key in ${KURTOSIS_ARGS_FILE:-$HERE/../args.yaml}; override with EEZ_L1_POSTER_KEY=0x..."

TEST_KEY="${EEZ_BUNDLE_TEST_KEY:-$(cast wallet private-key \
    --mnemonic "$DEV_MNEMONIC" \
    --mnemonic-index "${EEZ_BUNDLE_TEST_MNEMONIC_INDEX:-14}")}"
TEST_ADDR="$(cast wallet address --private-key "$TEST_KEY")"
POSTER_ADDR="$(cast wallet address --private-key "$POSTER_KEY")"

wait_for_l1() {
    local target="$1"
    while (( $(cast block-number --rpc-url "$RPC") < target )); do
        sleep "$SLOT_SECONDS"
    done
}

wait_past_target() {
    local target="$1" waited=0 max_wait
    max_wait=$(( SLOT_SECONDS * (SLACK + 8) ))
    while (( $(cast block-number --rpc-url "$RPC") <= target + 1 )); do
        sleep 2
        waited=$(( waited + 2 ))
        if (( waited > max_wait )); then
            echo "bundle-inclusion: timed out waiting for L1 to pass target $target" >&2
            return 1
        fi
    done
}

ensure_funded() {
    local bal
    bal="$(cast balance "$TEST_ADDR" --rpc-url "$RPC")"
    if [[ "$bal" != "0" ]]; then
        return
    fi

    echo "==> funding bundle test key $TEST_ADDR from $POSTER_ADDR"
    cast send "$TEST_ADDR" \
        --value "${EEZ_BUNDLE_TEST_FUND_VALUE:-1ether}" \
        --private-key "$POSTER_KEY" \
        --rpc-url "$RPC" >/dev/null

    for _ in {1..30}; do
        bal="$(cast balance "$TEST_ADDR" --rpc-url "$RPC" 2>/dev/null || echo 0)"
        [[ "$bal" != "0" ]] && return
        sleep 2
    done

    echo "bundle-inclusion: test key was not funded" >&2
    exit 1
}

send_bundle() {
    local attempt="$1" head head_ts target target_ts raw hash body resp

    head="$(cast block-number --rpc-url "$RPC")"
    head_ts="$(cast block "$head" --field timestamp --rpc-url "$RPC")"
    target=$(( head + SLACK ))
    target_ts=$(( head_ts + SLACK * SLOT_SECONDS ))

    raw="$(cast mktx "$TEST_ADDR" --value 0 --private-key "$TEST_KEY" --rpc-url "$RPC")"
    hash="$(cast keccak "$raw")"
    body="$(printf '{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":["%s"],"blockNumber":"0x%x","minTimestamp":%s,"maxTimestamp":%s}]}' \
        "$raw" "$target" "$target_ts" "$target_ts")"

    echo "==> attempt $attempt/$ATTEMPTS: tx=$hash target=$target exact_ts=$target_ts slack=$SLACK"
    resp="$(curl -sS "$BUILDER" -H 'Content-Type: application/json' -d "$body")" || {
        echo "bundle-inclusion: eth_sendBundle POST failed" >&2
        return 1
    }
    if grep -q '"error"' <<<"$resp"; then
        echo "bundle-inclusion: rbuilder rejected bundle: $resp" >&2
        return 1
    fi

    wait_past_target "$target" || return 1
    if cast receipt "$hash" --rpc-url "$RPC" >/dev/null 2>&1; then
        echo "PASS: bundle tx landed: $hash"
        return 0
    fi

    echo "bundle-inclusion: bundle tx did not land for target $target"
    return 1
}

echo "== rbuilder bundle inclusion test =="
echo "rpc=$RPC"
echo "builder=$BUILDER"
echo "test_addr=$TEST_ADDR"

ensure_funded

head="$(cast block-number --rpc-url "$RPC")"
if (( WARMUP_BLOCK > 0 && head < WARMUP_BLOCK )); then
    echo "==> waiting for MEV warmup: L1 head $head < $WARMUP_BLOCK"
    wait_for_l1 "$WARMUP_BLOCK"
fi

for attempt in $(seq 1 "$ATTEMPTS"); do
    if send_bundle "$attempt"; then
        exit 0
    fi
done

echo "FAIL: no bundle inclusion after $ATTEMPTS attempts" >&2
echo "Hint: check rbuilder logs for orders_received/orders_simulated_ok/bundle_count." >&2
exit 1
