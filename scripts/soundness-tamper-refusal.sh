#!/usr/bin/env bash
#
# C2 ship-gate — validator-mode tamper-refusal (the "prover done" criterion).
#
# Soundness claim under test: the out-of-process prover (eez-proverd) only
# attests a settling window whose witness FAITHFULLY re-executes under ZisK's
# `native-validate`. A faithfully-captured witness must ATTEST; a witness with
# even one nibble flipped in an MPT state node must be REFUSED (the node's
# hash-link breaks → native-validate rejects → the prover refuses to advance).
#
# This scripts the manual procedure that validated C2 once, as a repeatable
# regression check. It drives the real loop with the embedded settling fixture
# (crates/eez-proverd/tests/fixtures, block 13) via the `feed_fixture` harness
# (ControlFeed + the real ProofSink on 127.0.0.1:50051) and the host
# `native-validate` binary — no Chiado, no L1, no composer needed.
#
# SKIPS cleanly (exit 0) if `native-validate` is absent (it's a separate,
# host-only ZisK build), so CI without ZisK is green; run it where ZisK is built.
#
# Usage: bash scripts/soundness-tamper-refusal.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
NV="${EEZ_VALIDATOR_BIN:-/home/ubuntu/zisk-eth-client/target/release/native-validate}"
CFG="${EEZ_CHAIN_CONFIG:-$REPO/configs/l2-chainconfig.json}"
FIX="$REPO/crates/eez-proverd/tests/fixtures"          # block-13 settling fixture
# hardhat #0 — the fixture's authorizedSigner; its address is the registered
# vkey, so the recomputed publicInputsHash matches the captured PostBatch and
# the CLEAN case reaches "ATTESTED". (Dev key, fixture-only.)
SIGNER="${EEZ_PROOF_SIGNER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
PORT=50051
WAIT_S=40

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }

# ── prerequisites ────────────────────────────────────────────────────
if [[ ! -x "$NV" ]]; then
    echo "SKIP: native-validate not found at $NV (separate host-only ZisK build)."
    echo "      build it (zisk-eth-client: cargo build --release -p native-validate)"
    echo "      or set EEZ_VALIDATOR_BIN, then re-run."
    exit 0
fi
for f in block-13.rlp witness-13.json postbatch-13.json; do
    [[ -f "$FIX/$f" ]] || { red "FAIL: missing fixture $FIX/$f"; exit 1; }
done
[[ -f "$CFG" ]] || { red "FAIL: missing chain-config $CFG"; exit 1; }

echo "native-validate : $NV"
echo "chain-config    : $CFG"
echo "fixture         : $FIX (block 13, settling)"
echo

cd "$REPO"
echo "[build] feed_fixture + eez-proverd …"
cargo build --quiet -p eez-node --example feed_fixture -p eez-proverd

FEED_BIN="$REPO/target/debug/examples/feed_fixture"
PROVERD_BIN="$REPO/target/debug/eez-proverd"

# Tampered fixture dir: same block + postbatch, but ONE nibble flipped in the
# longest `state[]` MPT node of the witness (breaks that node's hash-link).
TAMPER_DIR="$(mktemp -d -t eez-tamper.XXXXXX)"
cp "$FIX/block-13.rlp" "$FIX/postbatch-13.json" "$TAMPER_DIR/"
python3 - "$FIX/witness-13.json" "$TAMPER_DIR/witness-13.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
w = json.load(open(src))
state = w["state"]
# Pick the longest MPT node (most structural — surest to break a hash-link).
i = max(range(len(state)), key=lambda k: len(state[k]))
s = state[i]
body = s[2:] if s.startswith("0x") else s
# Flip one nibble in the middle to a definitely-different value.
pos = len(body) // 2
ch = body[pos]
new = "0" if ch != "0" else "f"
body = body[:pos] + new + body[pos+1:]
state[i] = ("0x" if s.startswith("0x") else "") + body
w["state"] = state
json.dump(w, open(dst, "w"))
print(f"tampered state[{i}] nibble@{pos}: '{ch}'->'{new}' (node len {len(s)})", file=sys.stderr)
PY

cleanup() { pkill -x feed_fixture 2>/dev/null || true; pkill -x eez-proverd 2>/dev/null || true; rm -rf "$TAMPER_DIR"; }
trap cleanup EXIT

# run_case <fixture-dir> <logfile> — serve the fixture, run the prover one
# window, leave the log for assertions. Sequential (both bind :50051).
run_case() {
    local dir="$1" log="$2"
    pkill -x feed_fixture 2>/dev/null || true
    pkill -x eez-proverd 2>/dev/null || true
    sleep 1
    RUST_LOG=info "$FEED_BIN" "$dir" 13 >/dev/null 2>&1 &
    # wait for :50051
    for _ in $(seq 1 20); do (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.5; done
    RUST_LOG=info,eez_proverd=info "$PROVERD_BIN" \
        --control-addr "http://127.0.0.1:$PORT" \
        --validator-bin "$NV" --chain-config "$CFG" \
        --signer-key "$SIGNER" --max-window 1 > "$log" 2>&1 &
    local pv=$!
    # poll until a terminal marker appears or timeout
    for _ in $(seq 1 $((WAIT_S * 2))); do
        if grep -qE "ATTESTED|window validation FAILED|rejected window|native-validate rejected" "$log" 2>/dev/null; then
            break
        fi
        sleep 0.5
    done
    pkill -x feed_fixture 2>/dev/null || true
    kill "$pv" 2>/dev/null || true
    wait "$pv" 2>/dev/null || true
}

fail=0

# ── CASE 1: clean fixture MUST attest ────────────────────────────────
echo "[case 1] clean witness  → expect ✓ native-validate accepted + ✓ ATTESTED"
CLEAN_LOG="$(mktemp)"
run_case "$FIX" "$CLEAN_LOG"
if grep -q "ATTESTED" "$CLEAN_LOG" && grep -q "native-validate accepted" "$CLEAN_LOG"; then
    green "  PASS: prover re-executed the faithful witness and attested"
else
    red "  FAIL: clean fixture did not attest"; sed -n 's/^/    /p' "$CLEAN_LOG" | tail -8; fail=1
fi

# ── CASE 2: tampered witness MUST be refused ─────────────────────────
echo "[case 2] tampered witness → expect refusal (native-validate rejects), NO attestation"
TAMP_LOG="$(mktemp)"
run_case "$TAMPER_DIR" "$TAMP_LOG"
if grep -qE "window validation FAILED|rejected window|native-validate rejected" "$TAMP_LOG" && ! grep -q "ATTESTED" "$TAMP_LOG"; then
    green "  PASS: prover REFUSED the tampered witness (no attestation)"
else
    red "  FAIL: tampered fixture was not refused (or it still attested!)"; sed -n 's/^/    /p' "$TAMP_LOG" | tail -8; fail=1
fi

echo
if [[ "$fail" == 0 ]]; then
    green "C2 ship-gate PASS — faithful witness attests, tampered witness refused."
    exit 0
else
    red "C2 ship-gate FAIL."
    exit 1
fi
