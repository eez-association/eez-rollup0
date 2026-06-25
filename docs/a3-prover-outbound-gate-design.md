# A3 — prover outbound gate (zk-sound L2-source validation) — design

Status: **scoped + designed, not implemented**. Authoritative design for A3
(the prover-side soundness gate for outbound L2->L1 cross-chain calls), from the
understand-workflow `wf_e70ff7ba-5f0` (2026-06-22). Companion to
`l2-to-l1-extension-plan.md` (A3 / R10 / CRITICAL-2). Mechanism chosen by Edu:
**B1 — the real zk-sound gate** (validate inside the proven computation), NOT the
orchestrator-side soft gate.

---

## 0. The soundness gap (why A3 exists)

An outbound L2->L1 entry on L1 carries `l2ToL1Calls[]` and a `proxyEntryHash`.
The L1 `executeCrossChainCall` only checks the entry is INTERNALLY consistent:
`proxyEntryHash == H_L1 = keccak256(abi.encode(targetRollupId, target, value,
data, source, MAINNET_ROLLUP_ID=0))` (EEZ.sol ~880/893). **Nothing cross-checks
that the L2 ever authorized the call** — i.e. that the L2 block actually emitted
`CrossChainCallExecuted(H_L2)` (EEZL2.sol ~197-200, `H_L2` = same params but
`sourceRollupId = L2_ROLLUP_ID`). A malicious/buggy composer could emit an
outbound entry with a self-consistent `proxyEntryHash` for a call the L2 never
made, and L1 would execute a **phantom withdrawal** (paying out the rollup's
escrowed `etherBalance`).

The prover today is **ether-agnostic and log-blind**: it attests only the L2
state root (via stateless re-execution) + the `publicInputsHash` (which binds
the entry bytes, incl. `l2ToL1Calls`, but only as a HASH — it never checks the
calls happened). The witness (`ExecutionWitness`: state/codes/keys/headers) has
**no receipts/logs**. So the prover cannot gate outbound — CRITICAL-2 / R10.

**No based-rollup precedent:** based ALSO has no prover-side outbound gate (it
relies on L1 state-root binding + `ExecutionConsumed` event observation + §4f
filtering). A3 is **new R&D beyond based**, not a port.

---

## 1. The soundness property A3 must enforce

For every outbound entry `E` (proxyEntryHash != 0... — wait, outbound IMMEDIATES
have proxyEntryHash==0 the lean way; the OUTBOUND-with-`l2ToL1Calls` entry is the
settlement entry) in the settled batch, A3 must prove:

> there EXISTS a `CrossChainCallExecuted(H_L2)` log in the re-executed L2 block,
> emitted by the canonical EEZL2 (`0x42..07`), whose recomputed
> `H_L1 = keccak256(abi.encode(targetRollupId, target, value, data, source, 0))`
> equals `E`'s `proxyEntryHash` (or, for an N-call entry, each `l2ToL1Calls[i]`
> matches a distinct log).

Recompute `H_L1` from the LOG's params (the log carries `H_L2` as topic0 +
proxy/source/data/value in data); the only difference between `H_L2` and `H_L1`
is `sourceRollupId` (L2 vs MAINNET=0), so the gate recomputes with `0` and
compares to `proxyEntryHash`. **Note** the `L2ToL1CallSol` struct omits
`targetRollupId`; thread it (the entry's `destinationRollupId` is the source
rollup, NOT the call target — see R11; the call target rollup is implicit =
MAINNET for an L2->L1, or read from the log).

**zk-sound** means this check runs INSIDE the proven computation (the ZisK
guest), OR the data it checks against is COMMITTED by the guest (bound into the
public inputs) and the orchestrator checks against that committed data. We take
the latter: the guest commits the per-block `CrossChainCallExecuted` logs digest;
the proverd cross-checks the entries against the (proven) logs.

---

## 2. Architecture — where the gate lives (3 repos)

The prover proves L2 re-execution via ZisK. The chain:

```
zisk-patch-stateless/crates/stateless   <- re-exec CORE (receipts/logs produced here)
  └─ stateless_validation_recovered_with_pair_roots(...) -> (hash, pair_roots, tx_statuses)
zisk-eth-client/crates/guest-reth/src/validation.rs
  └─ validate_block_pair_roots(...) -> (hash, pair_roots, tx_statuses)   [thin wrapper]
zisk-eth-client/crates/eez-public-inputs/src/lib.rs
  └─ guest_outputs_commitment(pair_roots, tx_statuses, batch_commitment) -> B256  [GUEST commits this]
zisk-eth-client/bin/guests/stateless-validator-reth/src/main.rs          [the zkVM GUEST]
zisk-eth-client/bin/native-validate/src/main.rs                          [native runner -> JSON]
eez-rollup0/crates/eez-proverd/src/main.rs                               [orchestrator gate]
```

The guest commits `block_hash` + `guest_outputs_commitment(...)` as its public
outputs; native-validate recomputes the identical commitment (native↔guest
parity is a byte compare); proverd verifies the proof's outputs == native's.

---

## 3. The change, component by component

### 3.1 `zisk-patch-stateless/crates/stateless` (the re-exec core) — EXPOSE LOGS
`stateless_validation_recovered_with_pair_roots` executes the block (producing
receipts internally) and returns `(hash, pair_roots, tx_statuses)`. **Extend it
to also return the receipts' logs** (or, leaner, ONLY the
`CrossChainCallExecuted` logs filtered by address == canonical EEZL2 + topic0 ==
the event signature). This is the deepest + most sensitive edit (forked
reth-stateless). Prefer a NARROW filter computed inside the executor loop to
avoid hauling all logs across the zkVM boundary (cost). Output: `Vec<CrossChain-
CallLog { h_l2: B256, target, value, data, source }>` per block (or per tx).

### 3.2 `guest-reth/src/validation.rs` — THREAD LOGS
`validate_block_pair_roots` returns `(hash, pair_roots, tx_statuses, cc_logs)`.
Thin pass-through of 3.1's new output.

### 3.3 `eez-public-inputs/src/lib.rs` — BIND LOGS IN THE COMMITMENT
`guest_outputs_commitment(pair_roots, tx_statuses, batch_commitment, cc_logs)`
appends the `cc_logs` digest (length-prefixed, each log's `(h_l2, target, value,
data, source)`) into the keccak buffer. This is what makes the logs PROVEN — the
guest commits this exact value, so the proverd's check against native's logs is
sound. **Versioned tag bump** (`GUEST_OUTPUTS_TAG`) so old proofs are rejected.

### 3.4 `bin/guests/stateless-validator-reth/src/main.rs` — GUEST COMMIT
The zkVM guest calls the updated `validate_block_pair_roots` + the updated
`guest_outputs_commitment`. No logic change beyond threading; the commitment now
binds the logs. (The guest's committed outputs are what the SNARK attests.)

### 3.5 `bin/native-validate/src/main.rs` — EMIT LOGS IN JSON
Add `cc_logs` to the per-block JSON (`blocks[].cc_logs[]`) + recompute the
`outputs_commitment` with the logs (already mirrors the guest). The proverd reads
`cc_logs` from here.

### 3.6 `eez-rollup0/crates/eez-proverd/src/main.rs` — THE GATE
After `verify_settlement_chain` (which proves the root) + the
outputs_commitment match (which proves `cc_logs` are real), add
`verify_outbound_authorized(batch, cc_logs)`:
- For each outbound entry's `l2ToL1Calls[i]`, recompute `H_L1` and find a
  matching `cc_logs` entry (recomputed `H_L1` from the log's params). Consume
  each log at most once (multiplicity). `bail!` if any outbound call has no
  backing log -> phantom withdrawal rejected.
- Mirror the existing inbound gate (`extract_inbounds` ~788-844) in shape.

---

## 4. Build + validation requirements (why this is multi-session)

- Builds across 3 repos: `zisk-patch-stateless` (forked reth-stateless core),
  `zisk-eth-client` (guest-reth, eez-public-inputs, native-validate, the zkVM
  guest), `eez-rollup0` (eez-proverd). Local edits only — **NO git pull/push on
  the zisk repos** (standing constraint; Edu lifted only the LOCAL-EDIT bar for
  A3).
- The zkVM guest must be REBUILT (the ZisK guest ELF) for the commitment change
  to take effect in real proofs; native↔guest parity must be re-verified.
- A test must prove BOTH: (a) a real outbound (with a genuine
  CrossChainCallExecuted log) PASSES, and (b) a FABRICATED outbound entry (no
  backing log) is REJECTED. The negative test needs a crafted batch/log set —
  feasible at the proverd level with synthetic `cc_logs` (unit), and at the
  validator level by re-executing a block with vs without the real call.
- Risk: the commitment-tag bump invalidates all existing proofs/vkeys — coordinate
  with any live deployment.

---

## 5. Recommended implementation order (a focused session)

1. **3.1 + 3.2** — expose `cc_logs` from the re-exec core + thread through
   guest-reth. Build `native-validate`; confirm logs appear (add to JSON 3.5
   first, BEFORE the commitment, so it's observable + non-breaking).
2. **3.6 (proverd gate) reading 3.5's JSON** — implement + UNIT-test the gate
   against synthetic `cc_logs` (real passes, fabricated rejected). At this point
   the gate is SOFT (reads native's JSON, not yet commitment-bound) but
   functional + tested.
3. **3.3 + 3.4 (commitment binding + guest)** — bind `cc_logs` into
   `guest_outputs_commitment` (tag bump) + rebuild the guest. NOW the gate is
   zk-sound (proverd checks against PROVEN logs). Re-verify native↔guest parity.
4. **E2E** — run the prover over a real outbound batch (value-bearing withdrawal
   from `e2e_value_outbound`) end-to-end; confirm it attests + the gate passes;
   confirm a fabricated entry is rejected.

This staging keeps each step independently verifiable and defers the
circuit-invalidating commitment bump to last.

---

## 6. Out of scope / constraints

- NO git pull/push on any zisk repo (local edits only).
- The gate is for OUTBOUND. Inbound is already gated (`extract_inbounds`).
- N>=2 multi-call outbound interacts (multiple logs per entry) — see
  `multicall-design.md`; A3 must iterate `l2ToL1Calls[]`, not assume 1.

---

## 7. Implementation progress

### Phase 1 — EXPOSE `cc_logs` — DONE (2026-06-22, compiles; NOT committed in
the zisk repos per the no-git-push constraint — LOCAL working changes there).

The `CrossChainCallExecuted` logs from the canonical EEZL2 are now threaded out
of the re-execution and emitted in native-validate's JSON. **Non-breaking**: the
guest commitment is UNCHANGED (logs are threaded but `_cc_logs`-ignored in the
guest), so existing proofs/vkeys stay valid. Files (local edits):

- `zisk-patch-stateless/crates/stateless/src/validation.rs` —
  `stateless_validation_recovered_with_pair_roots` return type
  `(B256, Vec<B256>, Vec<bool>)` -> `+ Vec<Log>`; collects every tx's logs RAW
  (`result.receipts.iter().flat_map(|r| r.logs...)`), generic (no eez coupling
  in the core). Both return paths (incl. the no-op-guard early return) carry it.
  Builds (`cargo build -p stateless`, 53s).
- `zisk-eth-client/crates/guest-reth/src/validation.rs` —
  `validate_block_pair_roots` threads the raw `Vec<Log>` (thin pass-through).
- `zisk-eth-client/bin/native-validate/src/main.rs` — filters to
  `address == 0x42..07 && topic0 == CrossChainCallExecuted sig
  (0xad427580..)`, adds `cc_logs: Vec<Log>` to `Validated`, emits per-block
  `"cc_logs": [{topics, data}]` in the STDOUT JSON. Builds
  (`cargo build --bin native-validate`, 10s).
- `zisk-eth-client/bin/guests/stateless-validator-reth/src/main.rs` (crate
  `zec-reth`, built via the ZisK toolchain, OUT of the cargo workspace) —
  destructures `_cc_logs` (ignored; commitment unchanged). Validated on the next
  ZisK guest rebuild (Phase 3).

Smoke test pending: needs a staged `block-<n>.rlp`+`witness-<n>.json` from a real
outbound batch (the e2e/prover flow generates them at runtime) to see a non-empty
`cc_logs` in native-validate's JSON. Empty `cc_logs: []` on a non-outbound block
already confirms the field is emitted.

### Phase 2 — the proverd gate — DONE (2026-06-22, commit `26dc7bd`, SOFT)

`verify_outbound_authorized(batch, cc_logs)` implemented in
`eez-proverd/src/main.rs` (mirrors the existing `inbound_outcome_gate`) + wired
into the settlement flow + unit-tested (real passes; phantom/wrong-hash/tampered
rejected; multiplicity enforced; anchor + inbound skipped). `cc_logs` parsed from
Phase 1's native-validate JSON into `VerifiedWindow.sync_cc_logs` (Option; None
=> skip). **SOFT** (warn, not `window_ok=false`): the logs are re-executed by
native-validate but NOT bound into the guest commitment yet (Phase 3), so a
failure is logged. Flip to hard-reject after Phase 3 + an E2E-prover run. The
hash semantics were RESOLVED (verified byte-for-byte on both sides):

`H_L2 = computeCrossChainCallHash(targetRollupId, target, value, data, source,
sourceRollupId=L2)` (EEZL2.sol:197) — the log's topic1. The gate recomputes via
`eez_evm::cross_chain_call_hash(RollupId(0)/*MAINNET*/, target, value, &data,
source, RollupId(call.sourceRollupId))` and matches against the log topic1s. The
original semantics notes (kept for reference):

- The log gives `H_L2` (topic1), `proxy` (topic2), and `(source, callData,
  value)` (data). The outbound ENTRY gives `l2ToL1Calls[i]` =
  `(target, value, data, source, sourceRollupId, ...)` + `proxyEntryHash`.
- `H_L2 = keccak256(abi.encode(targetRollupId, target, value, data, source,
  L2_ROLLUP_ID))`; the L1 `proxyEntryHash` uses `MAINNET=0` for the source
  rollup — so a DIRECT `H_L2 == proxyEntryHash` compare FAILS.
- The clean match is likely: recompute `H_L2'` from the ENTRY's
  `l2ToL1Calls[i]` fields (with `sourceRollupId = L2`) and compare to the log's
  topic1 `H_L2`. Verify the EXACT preimage on both sides (EEZL2.sol H computation
  vs `cross_chain_call_hash` in eez-evm) before coding — `targetRollupId` is NOT
  in `L2ToL1CallSol` (R11: the entry's `destinationRollupId` is the SOURCE
  rollup, not the call target), so thread it correctly.
- Multiplicity: consume each log at most once (N identical calls -> N logs).

### Phase 3 — bind into the commitment (zk-sound) — DONE (2026-06-22, code)

`guest_outputs_commitment` now takes `cc_logs: &[B256]` and binds them
(length-prefixed); `GUEST_OUTPUTS_TAG` bumped `V1`->`V2` (invalidates existing
proofs/vkeys). guest-reth filters the raw block logs to the canonical EEZL2
`CrossChainCallExecuted` and returns just each call's H (topic1); native-validate
emits them as a flat `cc_logs` list AND recomputes the commitment with them;
the ZisK guest commits the same. native↔guest parity holds by construction (the
SAME `guest_outputs_commitment`). proverd parses the flat list + the gate checks
against it — now zk-sound (the logs are PROVEN). native-validate builds,
eez-public-inputs commitment test green, eez-proverd 16/16. (ZisK edits LOCAL —
no git push.)

**The ONLY thing left to fully close A3 (operational, needs the ZisK toolchain):**
1. Rebuild the ZisK guest ELF (`zec-reth`) so real proofs commit the V2
   value, and regenerate the vkey.
2. Run the prover over a real outbound batch (an `e2e_value_outbound`-style
   withdrawal) to confirm the cc_logs pipeline end-to-end (guest-reth filter ->
   native JSON -> proverd parse -> gate PASSES the real call).
3. Flip the proverd gate from SOFT (`warn!`) to HARD (`window_ok = false`) — the
   one-line change at the `(2d) Outbound authorization gate` site — so a phantom
   withdrawal is REJECTED. Kept soft until (2) so a pipeline bug can't
   false-reject a valid withdrawal.
