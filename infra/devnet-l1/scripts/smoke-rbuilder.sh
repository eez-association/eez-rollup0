#!/usr/bin/env bash
#
# Proves the rbuilder honors eth_sendBundle timestamp pins (minTimestamp/
# maxTimestamp) — the one behavior the Python builder-stub can't verify and
# that EEZ's BundleTarget::Exact depends on for correct slot settlement (see
# crates/eez-l1/src/submitter.rs::post_bundle).
#
# Two probes, both a value-0 self-transfer from the funded poster key:
#   POSITIVE — pin the CORRECT target-slot timestamp → the tx MUST land by
#              the target block. Proves PBS is live and bundles reach chain.
#   NEGATIVE — pin an IMPOSSIBLE (past) timestamp → the tx must NOT land.
#              A builder that ignores the pin would include it anyway; if it
#              lands, EEZ would silently settle in the wrong slot. This probe
#              is the actual enforcement test.
#
# Requires: cast, curl, a warmed-up relay. In flashbots MEV the relay only
# proposes builder blocks after ~epoch 4 (~25 min post-genesis) — before that
# the POSITIVE probe fails for warmup reasons, not a real bug (script warns).
#
# Env (sourced from .env by default, override via SMOKE_ENV_FILES):
#   EEZ_L1_RPC_URL, EEZ_L1_BUILDER_RPC_URL, EEZ_L1_POSTER_KEY
#   EEZ_L1_SLOT_SECONDS   seconds per slot        (default 12)
#   RBUILDER_SMOKE_SLACK  target = head + slack    (default 3)
#
# Run after the MEV stack is up and past warmup:
#   bash infra/devnet-l1/scripts/smoke-rbuilder.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
ENV_FILE="${DEVNET_ENV_FILE:-$REPO/infra/devnet-l1/.env}"
SMOKE_ENV_FILES="${SMOKE_ENV_FILES-$ENV_FILE}"
for f in $SMOKE_ENV_FILES; do
    # shellcheck disable=SC1090
    [[ -f "$f" ]] && { set -a; source "$f"; set +a; }
done

: "${EEZ_L1_RPC_URL:=http://127.0.0.1:${EEZ_L1_HTTP_PORT:-18545}}"
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

# Send one bundle (single tx) pinned to [min,max]=ts, targeting head+SLACK.
# Uses cast keccak(raw) = the typed tx hash (keccak256 of the raw signed
# 2718-encoded tx bytes IS the tx hash for both legacy and typed txs;
# verify this holds on your foundry version if cast's hex-vs-string input
# handling for `keccak` ever changes).
#
# Return codes:
#   0 = tx landed
#   1 = confirmed NOT landed (relay explicitly rejected it, or it never
#       appeared after polling past the target) — a clean negative result
#   2 = inconclusive (POST/network failure, or polling timed out because
#       L1 itself stalled) — NOT evidence about the timestamp pin either way
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
echo "PASS: bundles land (positive) AND the timestamp pin is enforced (negative"
echo "  dropped or explicitly rejected). EEZ BundleTarget::Exact is safe on this rbuilder."
