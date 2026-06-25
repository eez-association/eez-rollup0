#!/usr/bin/env bash
#
# Phase-3 ship-gate — composer-driven dispatch end-to-end.
#
# Validates the DRIVEN prover path (EEZ_COMPOSER_DRIVEN=1): the prover takes its
# verify range from the composer's ProverDispatch stream (NOT a self-picked
# from_block), re-executes that posted window under native-validate, and attests
# — and the composer's ProofSink advances the verified frontier on the verified
# attestation. Driven over the embedded block-13 settling fixture via the
# `feed_fixture` harness (ControlFeed + the REAL ProofSink + ProverDispatch, all
# wired to ONE shared PostedWindows ledger) and the host native-validate — no
# Chiado, no L1, no composer node, NO backfill (the directive's window is
# self-contained from from_block).
#
# Soundness parity with self-pick: clean witness MUST attest; a one-nibble
# tamper MUST be refused (native-validate rejects → no attestation).
#
# Binds a FREE port (EEZ_FIXTURE_ADDR), so it coexists with a live node on :50051.
# SKIPS cleanly (exit 0) if native-validate is absent (host-only ZisK build).
#
# Usage: bash scripts/driven-dispatch-e2e.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
NV="${EEZ_VALIDATOR_BIN:-/home/ubuntu/zisk-eth-client/target/release/native-validate}"
CFG="${EEZ_CHAIN_CONFIG:-$REPO/configs/l2-chainconfig.json}"
FIX="$REPO/crates/eez-proverd/tests/fixtures"          # block-13 settling fixture
# hardhat #0 — the fixture's authorizedSigner; its address is the registered
# vkey AND the ProofSink's attester, so the recomputed publicInputsHash matches
# the captured PostBatch and the CLEAN case reaches "ATTESTED". (Dev key.)
SIGNER="${EEZ_PROOF_SIGNER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
ADDR="${EEZ_FIXTURE_ADDR:-127.0.0.1:50071}"            # free port (dodge live :50051)
PORT="${ADDR##*:}"; HOST="${ADDR%%:*}"
WAIT_S=40

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

# ── prerequisites ────────────────────────────────────────────────────
if [[ ! -x "$NV" ]]; then
    echo "SKIP: native-validate not found at $NV (separate host-only ZisK build)."
    exit 0
fi
for f in block-13.rlp witness-13.json postbatch-13.json; do
    [[ -f "$FIX/$f" ]] || { red "FAIL: missing fixture $FIX/$f"; exit 1; }
done
[[ -f "$CFG" ]] || { red "FAIL: missing chain-config $CFG"; exit 1; }

echo "native-validate : $NV"
echo "fixture         : $FIX (block 13, settling)"
echo "dispatch addr   : $ADDR (free port; driven directive [13..13])"
echo

cd "$REPO"
echo "[build] feed_fixture + eez-proverd …"
cargo build --quiet -p eez-node --example feed_fixture -p eez-proverd

FEED_BIN="$REPO/target/debug/examples/feed_fixture"
PROVERD_BIN="$REPO/target/debug/eez-proverd"

# Tampered fixture: one nibble flipped in the longest state[] MPT node.
TAMPER_DIR="$(mktemp -d -t eez-driven-tamper.XXXXXX)"
cp "$FIX/block-13.rlp" "$FIX/postbatch-13.json" "$TAMPER_DIR/"
python3 - "$FIX/witness-13.json" "$TAMPER_DIR/witness-13.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
w = json.load(open(src)); state = w["state"]
i = max(range(len(state)), key=lambda k: len(state[k]))
s = state[i]; body = s[2:] if s.startswith("0x") else s
pos = len(body) // 2; ch = body[pos]; new = "0" if ch != "0" else "f"
state[i] = ("0x" if s.startswith("0x") else "") + body[:pos] + new + body[pos+1:]
w["state"] = state; json.dump(w, open(dst, "w"))
print(f"tampered state[{i}] nibble@{pos}: '{ch}'->'{new}'", file=sys.stderr)
PY

cleanup() { pkill -x feed_fixture 2>/dev/null || true; pkill -x eez-proverd 2>/dev/null || true; rm -rf "$TAMPER_DIR"; }
trap cleanup EXIT

# run_case <fixture-dir> <proverd-log> <feed-log> — serve the fixture, run the
# DRIVEN prover one window, leave both logs for assertions.
run_case() {
    local dir="$1" plog="$2" flog="$3"
    pkill -x feed_fixture 2>/dev/null || true
    pkill -x eez-proverd 2>/dev/null || true
    sleep 1
    EEZ_FIXTURE_ADDR="$ADDR" RUST_LOG=info "$FEED_BIN" "$dir" 13 > "$flog" 2>&1 &
    for _ in $(seq 1 20); do (exec 3<>/dev/tcp/"$HOST"/"$PORT") 2>/dev/null && break; sleep 0.5; done
    # EEZ_COMPOSER_DRIVEN=1 → the driven path. --l2-rpc-url "" disables backfill
    # (the directive's window is self-contained from from_block).
    EEZ_COMPOSER_DRIVEN=1 RUST_LOG=info,eez_proverd=info "$PROVERD_BIN" \
        --control-addr "http://$ADDR" \
        --validator-bin "$NV" --chain-config "$CFG" \
        --signer-key "$SIGNER" --max-window 1 --l2-rpc-url "" > "$plog" 2>&1 &
    local pv=$!
    for _ in $(seq 1 $((WAIT_S * 2))); do
        if grep -qE "ATTESTED|window validation FAILED|rejected window|native-validate rejected|HARD reject" "$plog" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
    sleep 1   # let the ProofSink verify + advance the frontier
    pkill -x feed_fixture 2>/dev/null || true
    kill "$pv" 2>/dev/null || true
    wait "$pv" 2>/dev/null || true
}

fail=0

# ── CASE 1: clean witness MUST be driven-dispatched + attested ───────
echo "[case 1] clean witness, DRIVEN → expect directive received + native-validate accepted + ATTESTED + frontier advanced"
CLEAN_P="$(mktemp)"; CLEAN_F="$(mktemp)"
run_case "$FIX" "$CLEAN_P" "$CLEAN_F"
ok=1
grep -q "driven: received verify directive" "$CLEAN_P" || { red "  - no directive received"; ok=0; }
grep -q "native-validate accepted" "$CLEAN_P"          || { red "  - native-validate did not accept"; ok=0; }
grep -q "ATTESTED" "$CLEAN_P"                           || { red "  - did not attest"; ok=0; }
grep -q "driven: directive settled" "$CLEAN_P"         || { red "  - cursor did not advance on the directive"; ok=0; }
grep -q "verified frontier" "$CLEAN_F"                 || { red "  - composer frontier did not advance"; ok=0; }
if [[ "$ok" == 1 ]]; then
    green "  PASS: driven dispatch → re-execute → attest → frontier advance"
else
    red "  FAIL"; echo "  -- proverd --"; tail -10 "$CLEAN_P" | sed 's/^/    /'; echo "  -- feed --"; tail -6 "$CLEAN_F" | sed 's/^/    /'; fail=1
fi

# ── CASE 2: tampered witness MUST be refused (driven) ────────────────
echo "[case 2] tampered witness, DRIVEN → expect refusal (native-validate rejects), NO attestation"
TAMP_P="$(mktemp)"; TAMP_F="$(mktemp)"
run_case "$TAMPER_DIR" "$TAMP_P" "$TAMP_F"
if grep -qE "window validation FAILED|rejected window|native-validate rejected|HARD reject|driven: window rejected" "$TAMP_P" && ! grep -q "ATTESTED" "$TAMP_P"; then
    green "  PASS: driven prover REFUSED the tampered witness (no attestation)"
else
    red "  FAIL: tampered fixture was not refused (or still attested!)"; tail -10 "$TAMP_P" | sed 's/^/    /'; fail=1
fi

echo
if [[ "$fail" == 0 ]]; then
    green "Phase-3 ship-gate PASS — composer-driven dispatch attests the faithful window, refuses the tampered one."
    exit 0
else
    red "Phase-3 ship-gate FAIL."
    exit 1
fi
