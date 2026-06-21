#!/usr/bin/env python3
"""Mutation-testing harness for the eez-fuzz oracle.

Injects one realistic composer bug at a time, runs the corpus-replay regression
(the committed corpus through the shared run-path), and classifies:

  CAUGHT  — the oracle panics (the harness validates that logic)
  MISSED  — the regression stays green (a blind spot: the corpus/oracle can't
            see this bug class — enrich corpus / add an oracle / fix the path)
  SKIP    — the anchor text didn't match (catalog drifted from the code)

Mutation score = CAUGHT / (CAUGHT+MISSED). The recursive campaign drives it up
by closing MISSED gaps and expanding the catalog.

Usage: CARGO_TARGET_DIR=... python3 crates/eez-fuzz/mutants.py
Run from the repo root (or anywhere; it cd's to the repo root).
"""
import os, subprocess, sys, time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
os.chdir(ROOT)

# (name, subsystem, file, old, new) — old must match the current code exactly.
MUTATIONS = [
    ("overlay-skip-sstore", "overlay diff-apply",
     "crates/eez-evm-inspector/src/overlay.rs",
     """ctx.journal_mut()
                .sstore(*addr, *key, *after_val)
                .map_err(|e| OverlayError::SourceSstore {
                    address: *addr,
                    key: *key,
                    message: format!("{e:?}"),
                })?;""",
     "let _ = (key, after_val); // MUTANT"),

    ("rollinghash-drop-retdata", "rolling hash",
     "crates/eez-protocol/src/rolling_hash.rs",
     "        h.update([u8::from(success)]);\n        h.update(ret_data);",
     "        h.update([u8::from(success)]);\n        let _ = ret_data; // MUTANT"),

    ("rollinghash-flip-success", "rolling hash",
     "crates/eez-protocol/src/rolling_hash.rs",
     "        h.update([CALL_END]);\n        h.update(uint256_be(call_number));\n        h.update([u8::from(success)]);",
     "        h.update([CALL_END]);\n        h.update(uint256_be(call_number));\n        h.update([u8::from(!success)]); // MUTANT"),

    ("rollinghash-callbegin-off", "rolling hash",
     "crates/eez-protocol/src/rolling_hash.rs",
     "        h.update([CALL_BEGIN]);\n        h.update(uint256_be(call_number));",
     "        h.update([CALL_BEGIN]);\n        h.update(uint256_be(call_number + 1)); // MUTANT"),

    ("rollinghash-callend-off", "rolling hash",
     "crates/eez-protocol/src/rolling_hash.rs",
     "        h.update([CALL_END]);\n        h.update(uint256_be(call_number));",
     "        h.update([CALL_END]);\n        h.update(uint256_be(call_number + 1)); // MUTANT"),

    # Inspector dispatch — the L1→L2 value-settling path. These SHOULD be caught
    # (they corrupt what the destination contract receives), validating coverage.
    ("inspector-corrupt-calldata", "inspector dispatch",
     "crates/eez-evm-inspector/src/inspector.rs",
     "            calldata: calldata.clone(),",
     "            calldata: { let mut __c = calldata.clone().to_vec(); if let Some(__b) = __c.last_mut() { *__b = __b.wrapping_add(1); } __c.into() }, // MUTANT"),

    ("inspector-bump-value", "inspector dispatch",
     "crates/eez-evm-inspector/src/inspector.rs",
     "            value: call_value,",
     "            value: call_value.wrapping_add(alloy_primitives::U256::from(1u8)), // MUTANT"),

    ("inspector-wrong-destination", "inspector dispatch",
     "crates/eez-evm-inspector/src/inspector.rs",
     "            destination: info.original_address,",
     "            destination: alloy_primitives::Address::ZERO, // MUTANT"),

    # Composer batch encoding — the synthesized returnData L1 re-executes.
    ("entries-corrupt-returndata", "batch returnData",
     "crates/eez-evm/src/entries/mod.rs",
     "            callCount: U256::ZERO,\n            returnData: return_data,",
     "            callCount: U256::ZERO,\n            returnData: { let mut __r = return_data.to_vec(); if let Some(__b) = __r.last_mut() { *__b = __b.wrapping_add(1); } __r.into() }, // MUTANT"),
]

# Score against the COMMITTED deterministic regression (curated e2e Program
# cases + lib world tests) — not the untracked local corpus. This is what CI
# guarantees, so the mutation score reflects the real, reproducible coverage.
TEST = ["cargo", "test", "-p", "eez-fuzz", "--lib", "--test", "e2e_cases"]


def classify(name, file, old, new):
    src = open(file).read()
    if old not in src:
        return "SKIP"
    open(file, "w").write(src.replace(old, new, 1))
    try:
        r = subprocess.run(TEST, capture_output=True, text=True, timeout=600)
        # Non-zero exit = a test failed = the oracle caught the bug.
        return "CAUGHT" if r.returncode != 0 else "MISSED"
    finally:
        subprocess.run(["git", "checkout", "-q", file])


def main():
    results = []
    for name, sub, file, old, new in MUTATIONS:
        t = time.time()
        verdict = classify(name, file, old, new)
        results.append((verdict, name, sub))
        print(f"  {verdict:6}  {name:28} [{sub}]  ({time.time()-t:.0f}s)", flush=True)
    caught = sum(1 for v, *_ in results if v == "CAUGHT")
    scored = sum(1 for v, *_ in results if v in ("CAUGHT", "MISSED"))
    missed = [n for v, n, _ in results if v == "MISSED"]
    print(f"\nmutation score: {caught}/{scored}" + (f" = {caught/scored:.0%}" if scored else ""))
    print("MISSED (blind spots):", missed or "none")

    # Append to the recursive scoreboard — the score over time. Watch it climb
    # as blind spots close (e.g. once the reentrant path ratifies).
    import datetime
    log = os.path.join(os.path.dirname(__file__), "fuzz", ".mutation-score.log")
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    with open(log, "a") as f:
        f.write(f"{stamp}  {caught}/{scored}  MISSED={missed}\n")


if __name__ == "__main__":
    main()
