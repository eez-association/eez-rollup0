# Crosschain upgrade: eez0 → based modular completeness

**Status:** ANALYSIS (plan, not started) · **Last updated:** 2026-06-20
**Scope note (Edu, 2026-06-20):** deliver-as-failed (`success=false`, Gap 2 / Phase 2) is
**DEFERRED** — eez0 keeps discarding reverting inbounds for now. Blueprint preserved in Appendix A.

Goal: bring eez0's crosschain implementation up to the most complete version —
the modular reference at `/home/ubuntu/based-rollup`. This is a **diff-and-upgrade**,
not a greenfield build. (Supersedes the earlier `inbound-phase-p-plan.md`, which
was mis-framed: it concluded "inbound non-functional / port based's orchestration"
from a based-shaped lens — WRONG. eez0 has a native, working inbound delivery path;
the real gaps are narrower. See §2.)

## 1. Verdict — where eez0 actually stands

eez0 is **NOT meaningfully behind on the protocol core.** The gap is **concentrated**
(inbound failure *delivery* + prover *soundness gates*), not diffuse, and is **mostly
wiring** — the primitives already exist in eez0.

| Layer | Status | One-line |
|---|---|---|
| `eez-protocol` / `eez-evm` (core crates) | **At parity / AHEAD** | eez0 FIXED a storage-slot bug based still carries (`ROLLUPS_MAPPING_SLOT=2` vs based's wrong `1`); added deriver decode canaries based never needed. |
| Deriver (`eez-deriver`) | **Complete / sound** | Inbound system-tx reconstruction + partial-consumption truncation matches based's intent. |
| Composer inbound failure path | **Behind (1 gen)** | Primitives exist; composer *poison-evicts* reverts instead of *delivering* them. |
| Prover (`eez-proverd`) | **Behind — SOUNDNESS HOLE** | The inbound outcome gate (X==Y) is explicitly NOT PORTED (`main.rs:21`). |
| Settlement lifecycle | **Behind (robustness)** | No `SettlementTracker`; optimistic Pending/Settled/Failed only. |
| L1-originated inbound | **Missing (capability)** | eez0 detects cross-chain only at L2 ingress; cannot accept an L1 user `counterL1`-style call. |

## 2. The three real gaps (with the architecture caveat)

**Caveat:** eez0 is **producer/follower + out-of-process prover**; based is **monolithic
with embedded driver**. Adopt based's *logic*, not its *modules* — anything that assumes
the driver+composer share an address space must be re-homed across eez0's process boundary.

**Gap 1 — Inbound outcome gate in the prover (X==Y) — SOUNDNESS, top priority.**
`eez-proverd/src/main.rs:21` literally: "NOT YET PORTED — the fail-closed settlement gates."
Without it, a malicious composer can feed divergent `returnData` to L1 vs L2 and the prover
passes *vacuously*. Based ref: `bin/eez-prover:344-406`. **Cleanest port** — read-only over
data the feed already carries (no composer/contract change). Its data source (the validator
re-exec producing `pair_roots`/`tx_statuses`) is `native-validate`, already wired + validated
live in the Chiado Phase 3/4 work.

**Gap 2 — Deliver-as-failed routing in the composer. ⛔ DEFERRED (Edu, 2026-06-20).**
**Decision: for now eez0 keeps DISCARDING reverting inbounds (poison-evict); `success=false`
delivery is NOT needed yet — tackle it later.** eez0's current behavior is COHERENT (successes
delivered; a reverting inbound is dropped at compose and the user resubmits) — it just lacks
based's *atomic* failure delivery. The detailed blueprint + the in-tree findings (so we don't
re-derive them) are in **Appendix A** below. Current behavior, verified in-tree:
`compose_via_evm_composer` uses the `composition` (discards the `recorded` outcome); a reverting
top-level inbound makes `build_batch` return an EMPTY batch (`entries/mod.rs:120-128`, "no
top-level success → empty"), which the loop treats as poison and evicts (`composer.rs:1125`,
comment "or revert"). `system_tx.rs:110` hardcodes `success: true` — SAFE today precisely
because only successful inbounds ever reach it (failures evicted upstream).

**Gap 3 — L1-originated inbound interception (capability).**
eez0 detects cross-chain only at L2 ingress; it cannot accept an L1 user tx and orchestrate
the L1→L2 delivery. Based has `l1_interceptor` + `orchestrate_inbound_l1`. Net-new surface +
a deployment SPOF question (the interceptor fronts L1 RPC). Lowest urgency — gated on product need.

*(Secondary, robustness not soundness: no `SettlementTracker` → no divergence-halt, no bounded
per-content retry, no inbound requeue.)*

## 3. KEEP — eez0 is right or equal (do NOT adopt based)

- **`ROLLUPS_MAPPING_SLOT=2`** (`eez-evm/action.rs:78`) — eez0 AHEAD; based's `1` is a latent
  storage-layout bug. **File upstream against based.**
- **Deriver decode canaries** (`entries/mod.rs:1073-1191`) — eez0 AHEAD (based has no deriver).
- **`system_tx.rs`, types.rs events, batch.rs `call_data()`, public_inputs `pub`** — required
  producer/follower adaptations.
- **Separate driver/deriver/composer + out-of-process prover** — intentional split.
- The **already-complete** eez0 prover gates (publicInputsHash recompute, settlement-chain,
  reverted-system-tx #10) — do NOT re-port.

## 4. Phased upgrade plan (smallest-meaningful-first; [CC] = consensus-critical)

- **Phase 0** — file the based `ROLLUPS_MAPPING_SLOT=1` bug upstream (eez0 correct; no eez0 change).
- **Phase 1 [CC]** — **inbound outcome gate (X==Y) + multiplicity (≤1) + witness-metadata
  cross-check** in `eez-proverd`. The soundness close; cleanest port; acceptance oracle for Phase 2.
  Exit: prover refuses a window whose sealed L2 inbound `returnData` diverges from the L1 entry,
  refuses >1-inbound windows, refuses a witness whose RLP header contradicts the ControlEvent.
- **Phase 2 [CC] — ⛔ DEFERRED (Edu, 2026-06-20): keep discarding reverts for now.** deliver-as-failed
  routing in `compose_via_evm_composer`. See **Appendix A** for the full blueprint when we tackle it.
- **Phase 3 [CC]** — `SettlementTracker` (re-homed composer-side, fed by `L1Watcher`) + inbound
  requeue + ProofSink coupling. Robustness: divergence-halt, bounded retry, abandoned-cycle recovery.
- **Phase 4 [CC], gated on product need** — L1 inbound interception (`l1_interceptor` +
  `orchestrate_inbound_l1` + CapturingDispatcher). Decide the SPOF/HA story first.

**First slice: Phase 1, the inbound outcome gate.** Files: `crates/eez-proverd/src/main.rs`
(new gate fn, model on based `eez-prover:344-406`), reusing `eez_evm::entries::decode_inbound`
+ `decode_postbatch` + `public_inputs_hashes` (already imported). `core-worker → auditor → test-writer`.

## 5. Open questions for Edu

1. **Prover validator wiring** — the gate's data source is `native-validate` (already wired in
   Phase 3/4). Confirm the gate consumes the window's post_batch + sealed block from the feed
   (it does) — so Phase 1 needs no new plumbing. ✅ likely.
2. **Do you need L1-originated inbound (Phase 4) this cycle?** If no L1 user calls `counterL1`
   directly, Phase 4 parks and the whole effort is Phases 1-3 (medium/small).
3. **`SettlementTracker` ownership across the split** — composer-side, fed by `L1Watcher` (the one
   place copying based's *structure* would fight eez0's architecture). Confirm before Phase 3.
4. **Upstream relationship** — re-merge with based, or hard-fork? If re-merging, the slot bug +
   canaries must go upstream or every sync re-introduces the divergence.

---

## Appendix A — deliver-as-failed (`success=false`) blueprint — DEFERRED

**Decision (Edu, 2026-06-20):** keep eez0's poison-evict (discard reverting inbounds, user
resubmits); do NOT build atomic failure delivery yet. This appendix preserves the full analysis
(verified in-tree, 2026-06-20) so it isn't re-derived when we tackle it. **It is a real
consensus-critical, contract-coupled feature — not a one-liner.**

### A.1 How eez0's inbound works TODAY (verified)
- ingress classifies a cross-chain tx by proxy address (`authorizedProxies`) → `held_pool` →
  `compose_via_evm_composer` (`composer.rs:~1084`): `simulate_and_resolve(raw_tx)` →
  `build_inbound_system_txs(composition.targets[].batch.entries())` → L2 delivery
  (`executeIncomingCrossChainCall`) + the L1 batch from `composition.source.batch`
  (`prepare_post_batch_raw`).
- **Success** → delivered. **Revert** → `build_batch` returns an EMPTY batch
  (`entries/mod.rs:120-128`, "no top-level success → empty"); the loop sees EmptyCalls →
  `sim_error_is_poison` true (`composer.rs:157`, comment "or revert") → **evicted**. User resubmits.

### A.2 Findings that corrected my wrong assumptions (DON'T re-trip on these)
- `simulate_and_resolve` **discards** the recorded outcome (`eez-protocol/composer.rs:543`:
  `.map(|(c,_recorded)| c)`); `simulate_and_resolve_recorded` (`:541`) returns `(composition,
  recorded)` and the `recorded` (`Vec<ExecutedAction>`) carries the REAL `outcome` (success +
  return_data) even for a revert. **That is the success source — NOT a rolling-hash recovery.**
- `EntryBuilder.append_call` (`entries/mod.rs:932`) — the fn that would fold the TOP-LEVEL
  `call_begin/call_end(success,returnData)` into the entry rolling hash — is **DEAD CODE**
  (`#[allow(dead_code)]`). So a top-level inbound entry's `rollingHash` does NOT encode its own
  success the recoverable way; do NOT try to recover `success` from a single entry's rollingHash
  for the top-level inbound. (The recoverable fold exists for OUTBOUND `build_l1_postbatch` and for
  the L2 mirror `build_l2_incoming_entry` / `decode_inbound`, not the build_batch top-level entry.)
- `build_return` / `build_l1_inbound_failed` (`eez-evm/lib.rs:286-307`) and `encode_delivery`
  (`lib.rs:256-277`) are DORMANT (no live callers). The note at `lib.rs:279-285` is authoritative:
  the live SUCCESS path takes the L1 batch from `Composition.source` (`build_batch`); the FAILURE
  shape — "settlement-only entry + failed `LookupCall`" — is a shape **`build_batch` does NOT
  emit**, only `build_return`'s failure branch does.
- The contract (`EEZBase.sol`) handles a failed inbound via dedicated **reverted-lookup**
  machinery (`ExpectedLookup`, `ContextResult`, `RollingHashMismatch`, `_revertedLookup*`). So the
  failure delivery is NOT a normal entry with `success=false`; it is the failed-`LookupCall`
  reverted-lookup path. **Verify against the .sol before implementing.**

### A.3 The blueprint (port based `composer-lib/src/inbound.rs::orchestrate_inbound_l1`, ADAPTED)
based reads the REAL outcome from the `recorded` action and branches:
```rust
// action = the single inbound delivery (select_inbound_action over `recorded`)
if !action.value.is_zero() && !action.outcome.is_success() { /* reject value-carrying revert */ }
let l2 = EvmProtocol.encode_delivery(&msg, &Delivery{ success: action.outcome.is_success(), return_data: Y });
let l1 = if action.outcome.is_success() { Composition.source.batch }
         else { EvmProtocol.build_return(&msg, &Delivery{ success:false, return_data:Y }) }; // failed LookupCall
// enqueue InboundDelivery { l2 syncpair_calldata, l1 inbound_batch, ... }
```
**eez0 adaptation (when we do it):**
1. `compose_via_evm_composer`: switch the inbound path to `simulate_and_resolve_recorded`; read
   `action.outcome`; STOP poison-evicting a failed top-level inbound.
2. L2 delivery: build with the REAL success (`encode_delivery(success: real)` — activates the
   dormant trait method — or thread the real success into `build_inbound_system_txs`).
3. L1 batch: success → `source.batch`; failure → `build_return` failure branch
   (`build_l1_inbound_failed` → the failed-`LookupCall` reverted-lookup shape).
4. **Deriver** (`deriver.rs:~972`): rebuild the failed delivery from the L1 failed-`LookupCall`
   entry SHAPE (it has no recorded outcome — it must derive `success=false` from the entry shape),
   so composer-emit == deriver-rebuild (the Phase-C byte-identity invariant).
5. Reject value-carrying reverts (based `inbound.rs:252-264`).
6. Soundness close: the prover inbound outcome gate (X==Y) — Gap 1 / Phase 1 — should land FIRST
   (it's the acceptance oracle for this).
**Consensus-critical:** touches `compose_via_evm_composer` (just bug-fixed), the deriver, and must
mirror the contract's reverted-lookup protocol exactly. `core-worker → auditor → test-writer → qa`.
