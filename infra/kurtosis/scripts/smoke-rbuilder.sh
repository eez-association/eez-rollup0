#!/usr/bin/env bash
# rbuilder timestamp-pin smoke test for a running Kurtosis devnet.
#
# Sends one valid and one invalid eth_sendBundle timestamp-pinned control bundle
# from the poster key. Run after MEV warmup.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/enclave-env.sh"

: "${EEZ_L1_SLOT_SECONDS:=$(( ${EEZ_L1_BLOCK_TIME_MS:-12000} / 1000 ))}"

RPC="${EEZ_L1_RPC_URL:?set EEZ_L1_RPC_URL}"
BUILDER="${EEZ_L1_BUILDER_RPC_URL:?set EEZ_L1_BUILDER_RPC_URL}"
KEY="${EEZ_L1_POSTER_KEY:?set EEZ_L1_POSTER_KEY (a funded prefunded account)}"
SLOT="${EEZ_L1_SLOT_SECONDS:-12}"
SLACK="${RBUILDER_SMOKE_SLACK:-3}"

for t in cast curl; do
    command -v "$t" >/dev/null || { echo "smoke-rbuilder: $t not found" >&2; exit 1; }
done

ADDR="$(cast wallet address --private-key "$KEY")"
bal="$(cast balance "$ADDR" --rpc-url "$RPC")"
[[ "$bal" != "0" ]] || { echo "smoke-rbuilder: poster $ADDR has 0 balance — fund it" >&2; exit 1; }

head="$(cast block-number --rpc-url "$RPC")"
if (( head < 130 )); then
    echo "smoke-rbuilder: WARN L1 at block $head (< ~4 epochs). The flashbots relay"
    echo "  proposes builder blocks only after ~epoch 4; the POSITIVE probe may fail"
    echo "  for warmup reasons. Re-run once the chain is past ~block 130."
fi

# Send one tx pinned to [min,max]=ts, targeting head+SLACK. cast keccak(raw) is
# the tx hash. Return codes: 0 landed, 1 confirmed not landed, 2 inconclusive
# (network/L1 failure — no evidence about the pin either way).
probe() {
    local label="$1" min_ts="$2" max_ts="$3"
    local h target raw hash body resp landed=1 waited=0 max_wait
    h="$(cast block-number --rpc-url "$RPC")"
    target=$(( h + SLACK ))
    raw="$(cast mktx "$ADDR" --value 0 --private-key "$KEY" --rpc-url "$RPC")"
    hash="$(cast keccak "$raw")"
    body="$(printf '{"jsonrpc":"2.0","id":1,"method":"eth_sendBundle","params":[{"txs":["%s"],"blockNumber":"0x%x","minTimestamp":%s,"maxTimestamp":%s}]}' \
        "$raw" "$target" "$min_ts" "$max_ts")"
    echo "  $label: bundle tx=$hash target=$target pin=[$min_ts,$max_ts]"
    resp="$(curl -sS "$BUILDER" -H 'Content-Type: application/json' -d "$body")" \
        || { echo "  $label: eth_sendBundle POST failed (network error)" >&2; return 2; }
    if grep -q '"error"' <<<"$resp"; then
        echo "  $label: relay rejected the bundle outright: $resp"
        return 1   # explicit rejection — a clean "did not land" for either probe
    fi
    # Wait until the chain is two blocks past the target, then check inclusion.
    # Bounded so a stalled L1 fails loud instead of hanging forever.
    max_wait=$(( SLOT * (SLACK + 6) ))
    while (( $(cast block-number --rpc-url "$RPC") <= target + 1 )); do
        sleep 2
        waited=$(( waited + 2 ))
        if (( waited > max_wait )); then
            echo "  $label: TIMED OUT after ${waited}s waiting for L1 to pass target+1 — is L1 producing blocks?" >&2
            return 2
        fi
    done
    if cast receipt "$hash" --rpc-url "$RPC" >/dev/null 2>&1; then landed=0; fi
    (( landed == 0 )) && echo "  $label: tx LANDED" || echo "  $label: tx did NOT land"
    return $landed
}

echo "== rbuilder timestamp-pin smoke =="
echo "poster=$ADDR rpc=$RPC builder=$BUILDER"

# Predicted timestamp of the target slot on a healthy chain: ts(head)+slack*slot.
head_ts="$(cast block "$head" --field timestamp --rpc-url "$RPC")"
correct_ts=$(( head_ts + SLACK * SLOT ))
impossible_ts=$(( head_ts - 2 * SLOT ))   # in the past → no future block matches

echo
echo "[1/2] POSITIVE — correct slot timestamp, must land"
pos=0; probe "positive" "$correct_ts" "$correct_ts" || pos=$?

echo
echo "[2/2] NEGATIVE — impossible (past) timestamp, must NOT land"
neg=0; probe "negative" "$impossible_ts" "$impossible_ts" || neg=$?

echo
echo "== verdict =="
if (( pos == 2 )); then
    echo "INCONCLUSIVE: positive probe hit an infra failure (POST error or L1 stalled)."
    echo "  → not evidence about timestamp enforcement either way. Check RPC/builder URLs and retry."
    exit 2
fi
if (( pos != 0 )); then
    echo "INCONCLUSIVE: positive probe did not land."
    echo "  → relay not warmed up (wait ~epoch 4), rbuilder not winning slots"
    echo "    (raise mev_builder_subsidy), or EEZ_L1_BUILDER_RPC_URL is wrong."
    echo "  → could also be a skipped slot at this specific target block (small"
    echo "    validator set) rather than a real problem — safe to just retry once."
    exit 2
fi
if (( neg == 2 )); then
    echo "INCONCLUSIVE: negative probe hit an infra failure — cannot confirm enforcement either way."
    exit 2
fi
if (( neg == 0 )); then
    echo "FAIL: builder IGNORES the timestamp pin (negative tx landed despite an"
    echo "  impossible timestamp). EEZ BundleTarget::Exact would settle in the"
    echo "  WRONG slot. Do not trust timestamp pinning on this rbuilder image."
    exit 1
fi

# Direct proof: rbuilder rejects an out-of-range bundle with IncorrectTimestamp
# (crates/rbuilder/.../order_commit.rs — it checks min<=block_ts<=max, where
# block_ts is the CL-supplied slot timestamp). Best-effort; the non-inclusion
# above is the real verdict, this just surfaces the reason if logs are reachable.
if command -v kurtosis >/dev/null 2>&1; then
    blogs="$(kurtosis service logs "${KURTOSIS_ENCLAVE:-eez-devnet}" \
        "${KURTOSIS_BUILDER_SERVICE:-el-5-reth-builder-lighthouse}" 2>/dev/null || true)"
    if grep -qi "IncorrectTimestamp" <<<"$blogs"; then
        echo "  confirmed: rbuilder logged IncorrectTimestamp (block_ts outside [min,max])."
    fi
fi

echo "PASS: bundles land (positive) AND the timestamp pin is enforced (negative"
echo "  dropped). rbuilder takes block_ts from the slot's payload attributes and"
echo "  drops bundles whose [min,max] excludes it — so min==max lands only in the"
echo "  block at that exact timestamp. EEZ BundleTarget::Exact is safe here."
