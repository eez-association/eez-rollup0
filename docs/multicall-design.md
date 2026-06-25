# Multi-call cross-chain (N>=2) — design (PARKED for later implementation)

Status: **scoped, not implemented**. Authoritative scope doc for adding N>=2
cross-chain calls in ONE transaction (the flash-loan use case). Produced by the
understand-workflow `wf_15689911-e7c` (2026-06-22). Companion to
`l2-to-l1-extension-plan.md` (A5 multi-call). Single-call cross-chain (both
directions, value-bearing) is DONE; this doc covers the N>=2 extension.

---

## 0. The decisive fact: NO contract changes needed

The EEZ/EEZL2 contracts (submodule, out of scope to change) **already support
N>=2**. The work is **purely off-chain** (composer + deriver + `eez-evm`
entries). Evidence:

- **`_processNCalls(count)`** (`EEZ.sol:945-992`) loops `count` times over the
  active `l2ToL1Calls[]`, advancing a monotonic `_currentL2ToL1Call` cursor and
  folding the rolling hash per call. No `count==1` restriction.
- **Continuation / reentrant frames** (`EEZ.sol:801-817`,
  `_consumeNestedAction`): a reentrant `executeCrossChainCall` runs
  `_processNCalls(nested.callCount)` on the SAME `l2ToL1Calls[]` array,
  continuing the cursor from the outer frame. This is the flash-loan pattern
  (call A borrow -> reentrant call B repay -> RESULT).
- **Per-call success in the rolling hash** (`EEZBase.sol:253-260`):
  `_rollingHashCallEnd(callNumber, success, retData)` tags every call. The
  entry's final `rollingHash` is the fold of `CALL_BEGIN(i) ++ CALL_END(i,
  success_i, retData_i)` for `i in 1..=N`.
- **Multiple `stateDeltas` per entry** are supported by `_applyStateDeltas`
  (loops the array), and **interior `currentState` matching** drives ordered
  consumption (see §3).

So the gap is: the off-chain side must (a) DETECT N calls, (b) BUILD an N-call
entry (or a continuation chain), (c) extract per-call success/value from the
folded rolling hash, (d) reconstruct it deterministically in the deriver, and
(e) seal per-prefix interior roots so L1 consumption is ordered.

---

## 1. What eez0 does today (the single-call assumptions)

| Site | File:line | Assumption |
|---|---|---|
| Sync-block seal | `eez-composer/src/local/session.rs:489-562` (esp. 550-552) | Seals the L2 root **ONCE** at the end: `final_state_root = *per_tx_roots.last()`. `per_tx_roots` accumulates per-tx roots but the intermediate ones are **discarded**. No per-prefix interior-root sealing. |
| Outbound value | `eez-evm/src/entries/mod.rs:351-376` (esp. 362) | `outbound_ether_out` returns `None` if `callCount != 1 \|\| l2ToL1Calls.len() != 1` — multi-call value is **rejected**. |
| L2 entry build | `eez-evm/src/entries/mod.rs:512-551` (esp. 547) | `build_l2_outbound_entry` hardcodes `callCount = 1`, rolling hash `CALL_BEGIN(1) ++ CALL_END(1, success, retData)`. |
| Entry build | `eez-evm/src/entries/mod.rs:75-223` | `build_batch` opens ONE `ExecutionEntrySol` per TOP-LEVEL call; nested success calls append to `expectedL1ToL2Calls`, not a new entry. |
| Sync pairs | `eez-evm/src/system_tx.rs:265-312` (esp. 277 `.first()`) | `build_cross_chain_sync_pairs` builds ONE `loadExecutionTable` + ONE trigger per entry, reading `entry.l2ToL1Calls.first()` only. |
| Inbound value map | `eez-composer/src/composer.rs:~2086` | `e.l2ToL1Calls.first()?.value` — only call[0]'s value is mapped to `etherDelta`; calls 1..N are silently dropped. |
| Deriver | `eez-deriver/src/deriver.rs:~1034-1043` + `system_tx.rs:98/277` | Reads `l2ToL1Calls[0]` only -> calls 1..N silently TRUNCATED -> Sync-block root diverges (composer ran N, deriver ran 1). |
| Detection | `eez-protocol/src/composition.rs` | A user tx that makes N internally-invoked cross-chain calls records only the **single outer outcome** — the N nested calls are NOT separate `ExecutedAction`s. No trace-walking. |

**Note** — N SEPARATE top-level cross-chain calls each in its OWN user tx
already works (this is A2b's mixed batch: `MAX_USER_TXS_PER_BUNDLE=3`, one entry
per tx, `build_cross_chain_sync_pairs` iterates them). The gap is N calls
**within ONE tx**.

---

## 2. The based-rollup reference (the proven pattern to mirror)

based-rollup HAS multi-call / flash loans. Mirror:

- **Per-prefix interior-root sealing** (`based-rollup driver.rs:3686-3774`): for
  N calls it computes N+1 unique roots by re-running `compute_state_root_with_-
  entries` over the prefix `filter_block_entries(.., keep_k, ..)` for each
  `k in 0..N`. Each entry gets a unique `currentState` (R(0,0) -> R(1,0) ->
  ... -> R(N)).
- **Continuation table** (`based-rollup table_builder.rs:1230-1400`,
  `docs/DERIVATION.md`): the canonical N=2 flash-loan layout —
  - L2: Entry 0 `CALL{user->bridge_l2} -> callReturn{scope=[0]}`, Entry 1
    `callReturn RESULT{L2, returned_tokens}`, Entry 2 `scope-exit RESULT{L1}`.
  - L1 mirror (5 entries): trigger, delivery `RESULT{scope=[0]}`, execution
    call, reentrant bridge-return CALL, scope-exit RESULT — each with a unique
    interior `currentState` S0->S1->S2->S3.
- **Scope navigation** for depth>1: each reentrant `executeCrossChainCall` starts
  its own scope tree; children get `scope=[0]`, `scope=[1]`, ...
- **Value per interior step**: deposits `etherDelta=+call_value` per call;
  withdrawals `etherDelta=0` on CALL, `-amount` on RESULT. Per-interior-step,
  not per-tx.

### Why single-seal BREAKS for N>=2 (the crux)

L1's `_findAndApplyExecution` checks `delta.currentState == rollups[rid].state-
Root` before consuming an entry. With a SINGLE final seal, every entry would
carry `currentState = R_final`:

1. postBatch sets on-chain `stateRoot = R(0,0)`.
2. Entry 1 has `currentState = R_final != R(0,0)` -> **`ExecutionNotFound`**.

Per-prefix sealing gives entry `k` a unique `currentState = R(k)` chained to the
prior entry's `newState`, so each consumes in order. (eez0's existing A2b
"stitch" at `composer.rs:~2052-2068` chains `currentState` to the prior
`newState`, but every `newState = sync_block_state_root` — fine for separate
settlement-marker entries where the real state change rides the user tx, but
INSUFFICIENT for N interior steps within one tx, which need distinct interior
roots.)

---

## 3. The gaps in eez0 (what to build), by phase

### Phase A — N sequential calls in ONE entry (no reentrancy)

A user tx makes N sequential L2->L1 cross-chain calls (not nested). The contract
runs all N via `_processNCalls(N)` in ONE entry consumption. Off-chain work:

1. **Detection** (composer / `composition.rs`): trace the tx and record EACH
   cross-chain call as a separate `ExecutedAction` (mirror based-rollup
   `l1_proxy.rs:walk_trace_tree`). Today nested calls merge into one outcome.
2. **N-call entry** (`entries/mod.rs`): `build_l2_outbound_entry` /
   `build_l1_postbatch` build `l2ToL1Calls = [c_0..c_{N-1}]`, `callCount = N`,
   rolling hash folding `CALL_BEGIN(i) ++ CALL_END(i, success_i, retData_i)` for
   all N. Generalize the hardcoded `callCount=1`.
3. **Per-call value/success** (`entries/mod.rs:outbound_ether_out` + the inbound
   value map): iterate all N `l2ToL1Calls`, recover each call's `success` by
   replaying the rolling-hash fold incrementally (the contract does
   `CALL_END(i, success, ret)`; the off-chain side tries `success in {true,
   false}` per call against the next fold state). Sum `etherOut` over successful
   value calls; sum inbound `etherDelta` over all N. Remove the `callCount==1`
   rejection.
4. **Sync pairs** (`system_tx.rs:build_cross_chain_sync_pairs`): for an N-call
   entry, build ONE `loadExecutionTable` carrying the entry (the contract's
   single consume runs all N) — NOT N separate loads. Verify the `.first()` ->
   full-array change.
5. **Deriver** (`deriver.rs` + `system_tx.rs`): iterate all N `l2ToL1Calls` in
   reconstruction (remove the `.first()` truncation). Stays purely L1-derived
   (the entry carries all N calls).
6. **Interior roots** (`local/session.rs`): if N sequential calls produce N
   interior state steps that L1 consumes separately, seal per-prefix (mirror
   based `driver.rs:3686-3774`); if the contract consumes all N in ONE entry
   (one `currentState`/`newState`), the existing single seal + stitch may
   suffice — VERIFY against `_processNCalls` semantics first.

### Phase B — continuation / flash-loan (reentrant, scope navigation)

The full based-rollup pattern. Port `table_builder.rs`:

1. **Continuation entry construction**: build the L1+L2 entry chains with scope
   navigation (`callReturn{scope=[0]}`, RESULT entries, scope-exit). Mirror
   based `table_builder.rs` (`build_l2_to_l1_continuation_entries`,
   `push_reentrant_child_entries`, the return-call address-direction rules).
2. **Per-prefix interior-root sealing** (`local/session.rs` + the splice): seal
   a unique `currentState` per interior step (S0->S1->S2->S3), chained — the
   based `attach_unified_chained_state_deltas` analog.
3. **Reentrant detection** (composer): iterative `debug_traceCallMany` expansion
   to detect reentrant cross-chain calls (mirror based `proxy.rs` /
   `l1_proxy.rs:walk_trace_tree`, `MAX_RECURSIVE_DEPTH`).
4. **Deriver continuation reconstruction**: reconstruct the full chain
   (`loadExecutionTable` with N entries + one `executeIncomingCrossChainCall`
   triggering the chain via CCM `newScope`). Mirror based `convert_l1_entries_-
   to_l2_pairs` + the `has_continuations` rules (skip the RESULT entry when
   continuations present; classify continuation entries by `hash(next_action)
   == action_hash`).

### Value-carrying folds into both

Per-call `etherDelta` (deposit `+v_i`, withdrawal `-v_i` on the call's RESULT),
already proven for single-call. The N-call generalization sums per call. The
flash-loan forward calls carry `etherDelta=0` (value returns within the tx).

---

## 4. Recommended order + risks

- **Phase A first** (N sequential, no reentrancy): meaningful, validatable,
  smaller. Test: one tx with 2 L2->L1 calls (e.g. `CallTwice`-style), assert
  both land + follower re-derives.
- **Phase B** (continuation/flash-loan): the big port; mirror based-rollup's
  table_builder + per-prefix sealing 1:1. Follow the based-rollup
  flash-loan rules (continuation entry construction, return-call direction,
  scope-exit RESULT, swap-and-pop reorder, state-delta assignment after
  reorder).

**Top risks:**
- Silent truncation: today the deriver drops calls 1..N with NO error (just a
  downstream root divergence). Any N>=2 work MUST add a guard that the entry's
  `callCount` == reconstructed call count.
- Per-call success recovery from the folded rolling hash is the subtle bit —
  the fold is one value; recovering each `success_i` requires replaying
  `CALL_END(i, success, ret)` incrementally and matching (feasible; based does
  it). `outbound_ether_out` only does N=1 today.
- Interior-root sealing determinism: the deriver must compute the SAME interior
  roots as the composer purely from L1 — no L2 simulation in derivation.
- Hash collisions (identical calls): per-prefix `currentState` + swap-and-pop
  disambiguate (based handles via prefix counting).

---

## 5. Out of scope (do NOT touch)

- Contracts (submodule) — already support N>=2.
- `eez-public-inputs` / native-validate — prover is ether-agnostic and
  root-driven; multi-call changes the entries, not the prover gates.
