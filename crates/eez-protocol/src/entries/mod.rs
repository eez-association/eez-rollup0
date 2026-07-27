//! Unified flat-emitter entry building for the multi-prover protocol.
//!
//! [`build_batch`] walks the preorder `recorded[..]` slice once,
//! classifies each call (top-level / nested-success / nested-failed /
//! lookup), folds per-entry rolling hashes, and emits an [`EvmBatch`]
//! carrying the deferred-execution table + lookup queue + transient-
//! prefix metadata that
//! `EEZ.postAndVerifyBatch` /
//! `EEZL2.loadExecutionTable` consume.
//!
//! The proof-system fields on the batch produced here stay empty
//! (`proofSystems = []`, `proofs = []`, …) — the submit path fills them
//! downstream (`prepare_post_batch` fills the carriers; the proof sink
//! fills `proofs[]` with the prover's signature).

use crate::{ExecutedAction, ProtocolResult, RollupId, rolling_hash::EntryRollingHash};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolCall;

use tracing::{debug, trace};

use crate::abi::{
    CrossChainCallSol, ExecutionEntrySol, ExpectedL1ToL2CallSol, L2ExecutionEntrySol,
    L2ToL1CallSol, LookupCallSol, StateDeltaSol, postAndVerifyBatchCall,
};
use crate::action::cross_chain_call_hash;
use crate::batch::EvmBatch;
use crate::dialect::ChainDialect;

/// Classification of a single [`ExecutedAction`] within an entry's
/// flat call window. Drives [`build_batch`]'s emission decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    /// Top-level call dispatched directly from `source_rollup_id` —
    /// becomes one entry's outer call (entry's `proxyEntryHash`
    /// matches this call's `crossChainCallHash`; entry's
    /// `L2ToL1Calls[]` carries reentrant children, not this call
    /// itself).
    TopLevel,
    /// Reentrant cross-chain call dispatched from inside a top-level
    /// call's execution and which succeeded — routes to the entry's
    /// `expectedL1ToL2Calls[]` table.
    NestedSuccess,
    /// Reentrant cross-chain call which reverted (caught by try/catch
    /// in the caller) — routes to `lookupCalls[]` with `failed = true`.
    NestedFailed,
}

impl CallKind {
    /// Classify a recorded call relative to `source_id` (the rollup
    /// whose batch is being built).
    fn classify(call: &ExecutedAction, source_id: RollupId) -> Self {
        if call.source_rollup_id == source_id {
            return Self::TopLevel;
        }
        if call.outcome.is_success() {
            Self::NestedSuccess
        } else {
            Self::NestedFailed
        }
    }
}

/// Build the chain-shaped [`EvmBatch`] for the rollup whose dialect
/// is supplied.
///
/// Walks `recorded` in preorder. Each [`CallKind::TopLevel`] call
/// opens a new [`ExecutionEntrySol`] with that call as the outer.
/// Subsequent classified calls land in the entry's `L2ToL1Calls` /
/// `expectedL1ToL2Calls` arrays or in the batch-level lookup queue,
/// per their kind.
///
/// `source_rollup_id` is the rollup THIS batch targets.
///
/// # Errors
///
/// Returns [`crate::ProtocolErrorKind::InvalidEncoding`]
/// if a call's outcome is `Pending` (composition lifecycle bug — every
/// call should be resolved before entering finalize).
#[tracing::instrument(level = "debug", name = "build_batch", skip_all, fields(source = %source_rollup_id), err)]
pub fn build_batch(
    recorded: &[ExecutedAction],
    dialect: &ChainDialect,
    source_rollup_id: RollupId,
) -> ProtocolResult<EvmBatch> {
    let group: Vec<&ExecutedAction> = recorded
        .iter()
        .filter(|c| {
            c.source_rollup_id == source_rollup_id || c.target_rollup_id == source_rollup_id
        })
        .collect();

    let any_top_level_success = group.iter().any(|c| {
        CallKind::classify(c, source_rollup_id) == CallKind::TopLevel && c.outcome.is_success()
    });
    if !any_top_level_success && !group.is_empty() {
        debug!(
            target: "eez::entries",
            %source_rollup_id,
            group = group.len(),
            "build_batch: no top-level success in group → empty batch (nested/static only)",
        );
        return Ok(EvmBatch::default());
    }

    let mut entries: Vec<ExecutionEntrySol> = Vec::new();
    let mut current_entry: Option<EntryBuilder> = None;
    let mut entry_nested_number: u64 = 0;

    for call in &group {
        let kind = CallKind::classify(call, source_rollup_id);
        match kind {
            CallKind::TopLevel => {
                if let Some(prev) = current_entry.take() {
                    entries.push(prev.finish());
                }
                // Entry-rollup / source==this batch: the outer is NEVER folded
                // into `L2ToL1Calls` (callCount=0, rollingHash=0). On consume,
                // `executeCrossChainCall` recomputes `rollingHash` by
                // re-executing `L2ToL1Calls`, which holds only reentrant L2→L1
                // children — not the top-level call, whose effect rides
                // `stateDeltas` and whose return rides `returnData`. Folding it
                // lets L1 re-execute the outer against a codeless target,
                // dropping the return data; any return-bearing call then reverts
                // `RollingHashMismatch` (origin/main fix, sync-rollups-protocol
                // @fe7bf66 / `DEPOSIT_SPEC.md §8`). This is structural here: only
                // calls SOURCED from this rollup reach `build_batch`'s TopLevel
                // (`CallKind::classify` keys top-level on the source), so the
                // outer is always an entry-rollup originating call. The TARGET
                // batch's "keep the outer so `executeIncomingCrossChainCall`
                // forwards on arrival" case is handled separately by
                // `build_l1_inbound_sidecar` (see `composition.rs` has_incoming
                // short-circuit) — an incoming call is never top-level here.
                let builder = EntryBuilder::new(call, *dialect);
                entry_nested_number = 0;
                current_entry = Some(builder);
            }
            CallKind::NestedSuccess => {
                let Some(builder) = current_entry.as_mut() else {
                    continue;
                };
                entry_nested_number += 1;
                builder.append_nested(call, entry_nested_number);
            }
            // D3 (5c51e02): nested failed / static lookups now live ENTRY-SCOPED
            // in `expectedLookups`, keyed by execution-context cursors — a
            // top-level `l1ToL2lookupCalls` entry would mis-consume on the new
            // contract. The entry-scoped emission is the deferred feature; until
            // it exists, REFUSE such a composition loudly (fail-closed) rather
            // than emit a batch the new semantics handle wrong. Unreachable in
            // production today (Static needs `static_meta`, never set; NestedFailed
            // needs a depth-2 outbound try/catch path no harness exercises) — but
            // gated so a future trigger errors instead of silently corrupting.
            CallKind::NestedFailed => {
                return Err(crate::ProtocolErrorKind::Unsupported(
                    "nested failed cross-chain lookup: entry-scoped emission (5c51e02) not built",
                )
                .into());
            }
        }
    }
    if let Some(last) = current_entry.take() {
        entries.push(last.finish());
    }

    debug!(
        target: "eez::entries",
        %source_rollup_id,
        recorded = recorded.len(),
        group = group.len(),
        entries = entries.len(),
        "build_batch: deferred L2 table built",
    );
    for (i, e) in entries.iter().enumerate() {
        trace!(
            target: "eez::entries",
            idx = i,
            proxy_entry_hash = %e.proxyEntryHash,
            rolling_hash = %e.rollingHash,
            dest_rollup = %e.destinationRollupId,
            "build_batch entry",
        );
    }

    Ok(EvmBatch {
        entries,
        // Empty by construction: NestedFailed/Static (the only top-level
        // lookup producers) are refused above (D3); the entry batch carries
        // no lookups.
        l1ToL2lookupCalls: Vec::new(),
        transientExecutionEntryCount: U256::ZERO,
        transientLookupCallCount: U256::ZERO,
        // Filled downstream at submit time: `prepare_post_batch`
        // wires the carriers; the proof sink fills `proofs[]` with
        // the prover's signature. Empty at build time.
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        // Future blob-carrier wiring. Today ships calldata-only +
        // empty proofs[]. The on-chain `_verifyProofSystemBatch`
        // reverts if a caller forgets to populate these before
        // submission, so missed wiring surfaces loudly.
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    })
}

/// Build the L1 `postAndVerifyBatch` batch for L2→L1 cross-chain calls.
///
/// This is the **executing** mirror of [`build_batch`]'s deferred L2 table.
/// Per the spec (`script/e2e/counterL2/E2E.s.sol::_l1Entries`), an L2→L1 call
/// on the L1 side is an IMMEDIATE entry — `proxyEntryHash = 0`, covered by
/// `transientExecutionEntryCount` — that actually RUNS the inbound call on L1
/// via `_processNCalls` (`L2ToL1Calls[i]` forwarded through the lazily-created
/// source proxy), as opposed to the L2 side which only loads the deferred
/// entry (`proxyEntryHash`=callHash, no calls, callCount 0) and returns the
/// precomputed `returnData`.
///
/// `calls` are the recorded calls targeting the L1 (i.e. `group_calls_for(L1)`
/// in `finalize`). `destination_rollup_id` is the settled rollup the entry
/// routes to. It does NOT affect the inline drain's routing (immediates run via
/// `attemptApplyImmediate`, not the queue), BUT it is load-bearing: `EEZ`'s
/// `_validateStructure` requires every entry's `destinationRollupId ∈ batch`
/// (and `MAINNET(0)` is rejected), so a wrong id reverts the whole batch — the
/// composer rewrites it to `rid` and `assert_batch_registry_native` enforces it.
///
/// `stateDeltas` (settling the L2 root) are attached downstream by
/// `prepare_post_batch` — one chained `R_{k-1}→R_k` delta per entry; this
/// builder emits them empty.
#[must_use]
#[tracing::instrument(level = "debug", name = "build_l1_postbatch", skip_all, fields(dest = %destination_rollup_id, calls = calls.len()))]
pub fn build_l1_postbatch(calls: &[ExecutedAction], destination_rollup_id: RollupId) -> EvmBatch {
    let mut entries: Vec<ExecutionEntrySol> = Vec::with_capacity(calls.len());

    for call in calls {
        // Only SUCCESSFUL top-level L2→L1 calls become executing L1 entries —
        // symmetric with `build_batch`'s "no top-level success → empty" rule
        // (mod.rs:120) and the D3 guard. A reverted L2→L1 call has no L1
        // executing effect to settle; emitting one would settle an entry the
        // (empty) L2 table doesn't back, and under 5c51e02's entry-scoped
        // nested-lookup semantics a reverted call hosting a nested failure
        // would mis-consume on-chain. If every call reverted the batch is
        // empty → the zk-poster leg `continue`s (composition.rs:606), settling
        // nothing — consistent with the empty L2 entry table. (Unreachable in
        // every current harness: counterL2/multitx/replay L2→L1 calls all
        // succeed; no outbound-failure flow exists.)
        if !call.outcome.is_success() {
            trace!(
                target: "eez::entries",
                target_addr = %call.target_address,
                "build_l1_postbatch: skipping reverted top-level L2→L1 call (no L1 executing entry)",
            );
            continue;
        }
        let success = true;
        let return_bytes: Vec<u8> = call
            .outcome
            .return_data()
            .map(<[u8]>::to_vec)
            .unwrap_or_default();

        // One top-level call per immediate entry → callCount = 1. The rolling
        // hash folds CALL_BEGIN(1) ++ CALL_END(1, success, returnData), exactly
        // as `EEZ._processNCalls` recomputes it on-chain.
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, success, &return_bytes);

        trace!(
            target: "eez::entries",
            target_addr = %call.target_address,
            source_rollup = %call.source_rollup_id,
            success,
            return_len = return_bytes.len(),
            rolling_hash = %B256::from(rolling.current()),
            "build_l1_postbatch: immediate executing entry",
        );

        entries.push(ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(destination_rollup_id.0),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: call.target_address,
                value: call.value,
                data: call.data.clone(),
                sourceAddress: call.source_address,
                sourceRollupId: U256::from(call.source_rollup_id.0),
                revertSpan: U256::from(call.revert_span.unwrap_or(0)),
            }],
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::from(1u8),
            returnData: Bytes::from(return_bytes),
            rollingHash: B256::from(rolling.current()),
        });
    }

    let immediate_count = entries.len();
    debug!(
        target: "eez::entries",
        %destination_rollup_id,
        immediate_count,
        "build_l1_postbatch: executing L1 batch built",
    );
    EvmBatch {
        entries,
        l1ToL2lookupCalls: Vec::new(),
        // All entries are immediate (executed inline in postAndVerifyBatch).
        transientExecutionEntryCount: U256::from(immediate_count),
        transientLookupCallCount: U256::ZERO,
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    }
}

/// The ETH value an entry's successful `L2ToL1Calls` RELEASE on L1 — the
/// `etherOut` `EEZ._processNCalls` accumulates (`cc.value > 0 && success`).
///
/// The L1 settlement books `etherDelta = etherIn − etherOut`: for a value-carrying
/// OUTBOUND (L2→L1) delivery `etherIn = 0` so `etherDelta = −etherOut` (the L2's
/// registry `etherBalance` shrinks by the withdrawn V — the mirror of the inbound
/// `+V` deposit). Per-call `success` is recovered from the entry's `rollingHash`
/// (the same fold `build_l1_postbatch` wrote: `call_begin(1) ++ call_end(1, s, returnData)`).
///
/// Returns `Some(0)` for entries with no value calls (inbound deferred entries with
/// `callCount = 0`, heartbeat settlement-only entries — they hit this and net to 0).
/// Returns `None` when value is present but the shape isn't the supported single
/// top-level call (`callCount == 1`, one `L2ToL1Call`): multi-call value outbound
/// isn't supported yet, so the caller MUST reject rather than mis-book the delta.
pub fn outbound_ether_out(entry: &ExecutionEntrySol) -> Option<U256> {
    let total: U256 = entry
        .l2ToL1Calls
        .iter()
        .fold(U256::ZERO, |acc, c| acc.saturating_add(c.value));
    if total.is_zero() {
        return Some(U256::ZERO);
    }
    // Value present → we need the per-call success. Only the single top-level
    // call shape (counterL2) is supported; multi-call value would need per-call
    // success extraction from the folded rolling hash.
    if entry.callCount != U256::from(1u8) || entry.l2ToL1Calls.len() != 1 {
        return None;
    }
    let value = entry.l2ToL1Calls[0].value;
    // Recover success: build_l1_postbatch folded call_begin(1) ++ call_end(1, s, returnData).
    for s in [true, false] {
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, s, &entry.returnData);
        if B256::from(rolling.current()) == entry.rollingHash {
            return Some(if s { value } else { U256::ZERO });
        }
    }
    None // rolling hash matched neither flag — malformed entry
}

/// Inputs to [`build_l2_incoming_entry`], named to make transposition
/// impossible. The two `Address` (`target`/`source`) and the two `RollupId`
/// (`source_rollup_id`/`l2_rollup_id`) are same-typed and so silently swappable
/// as positional args — and a swap changes the order-sensitive
/// `proxyEntryHash`, which then fails as an opaque on-chain hash mismatch.
/// Naming each field at the call site removes that class of bug.
#[derive(Clone, Debug)]
pub struct IncomingEntry {
    /// The inbound call's target (the L2 contract being called).
    pub target: Address,
    /// The caller on the source chain.
    pub source: Address,
    /// `msg.value` carried by the inbound call.
    pub value: U256,
    /// The inbound call's calldata.
    pub data: Bytes,
    /// The source chain's rollup id.
    pub source_rollup_id: RollupId,
    /// The L2's own rollup id (MUST equal the deployed `EEZL2.ROLLUP_ID`).
    pub l2_rollup_id: RollupId,
    /// The inbound call's result, folded into the rolling hash and returned.
    pub return_data: Bytes,
    /// Whether the inbound call succeeded.
    pub success: bool,
}

/// Build the single L2 mirror entry for an inbound L1→L2 cross-chain call
/// (counterL1), executed on the L2 via `EEZL2.executeIncomingCrossChainCall`.
///
/// This is the inverse of [`build_l1_postbatch`]: there the L1 *executes* an
/// L2→L1 call; here the L2 executes an L1→L2 call. Per the spec
/// (`script/e2e/counter/E2E.s.sol::_l2Entries`) the L2 mirror entry carries the
/// inbound call in `L2ToL1Calls[0]` (`callCount = 1`), runs it through
/// `_processNCalls` (delivered via the lazily-created source proxy for
/// `(source, source_rollup_id)`), and its `proxyEntryHash` binds the call —
/// `crossChainCallHash(l2_rollup_id, target, value, data, source, source_rollup_id)`
/// — to the entry, the SAME preimage `executeIncomingCrossChainCall` recomputes
/// on-chain (it hashes with the L2's own `ROLLUP_ID` as the target rollup, so
/// `l2_rollup_id` MUST equal the deployed `EEZL2.ROLLUP_ID`).
///
/// `return_data` is the inbound call's result (e.g. `abi.encode(1)` for
/// `Counter.increment()`), folded into the rolling hash and returned by the
/// entry. The L2 carries no `stateDeltas`.
#[must_use]
pub fn build_l2_incoming_entry(entry: IncomingEntry) -> L2ExecutionEntrySol {
    let IncomingEntry {
        target,
        source,
        value,
        data,
        source_rollup_id,
        l2_rollup_id,
        return_data,
        success,
    } = entry;
    let proxy_entry_hash =
        cross_chain_call_hash(l2_rollup_id, target, value, &data, source, source_rollup_id);

    // One inbound top-level call → callCount = 1; rolling hash folds
    // CALL_BEGIN(1) ++ CALL_END(1, success, returnData), exactly as
    // `EEZL2._processNCalls` recomputes it on-chain.
    let mut rolling = EntryRollingHash::new();
    rolling.call_begin(1);
    rolling.call_end(1, success, &return_data);

    // L2 (lean IEEZL2) shape: no stateDeltas / destinationRollupId; the inbound
    // call is `incomingCalls[0]`.
    L2ExecutionEntrySol {
        proxyEntryHash: proxy_entry_hash,
        incomingCalls: vec![CrossChainCallSol {
            targetAddress: target,
            value,
            data,
            sourceAddress: source,
            sourceRollupId: U256::from(source_rollup_id.0),
            revertSpan: U256::ZERO,
        }],
        expectedOutgoingCalls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::from(1u8),
        returnData: return_data,
        rollingHash: B256::from(rolling.current()),
    }
}

/// Fields for an OUTBOUND L2→L1 deferred entry (A2.1) — the mirror of
/// [`IncomingEntry`] with the cross-chain hash direction INVERTED.
///
/// There is deliberately NO `target_rollup_id` field: an L2→L1 call
/// targets L1 (`RollupId::MAINNET`) by definition, hardcoded inside
/// [`build_l2_outbound_entry`], so the two silently-swappable `RollupId`
/// args of [`cross_chain_call_hash`] cannot be mis-ordered by a caller
/// (a swap yields a different `proxyEntryHash` → `ExecutionNotFound` on
/// L2). The deployed L2 source proxy MUST be created with
/// `originalRollupId == 0` for the on-chain recompute
/// (`EEZL2.executeCrossChainCall`, `EEZL2.sol:197-199`) to match.
#[derive(Clone, Debug)]
pub struct OutboundEntry {
    /// The L1 target contract the L2→L1 call invokes.
    pub target: Address,
    /// The L2 caller the proxy passes as `sourceAddress` (the user EOA).
    pub source: Address,
    /// `msg.value` of the L2→L1 call (0 for the value-free first cut).
    pub value: U256,
    /// The L2→L1 call's calldata.
    pub data: Bytes,
    /// The L2's own rollup id (MUST equal the deployed `EEZL2.ROLLUP_ID`).
    pub l2_rollup_id: RollupId,
    /// The L1 call's result, folded into the rolling hash. Empty for a
    /// void target.
    pub return_data: Bytes,
    /// Whether the L1 call succeeded.
    pub success: bool,
}

/// Build the L2 DEFERRED entry for an OUTBOUND L2→L1 cross-chain call —
/// loaded via `EEZL2.loadExecutionTable` and consumed by the user tx's
/// `EEZL2.executeCrossChainCall`.
///
/// `proxyEntryHash` uses `targetRollupId = MAINNET(0)` (the L1 target's
/// home rollup = the L2 proxy's `originalRollupId`) and `sourceRollupId =
/// l2_rollup_id` (the L2's own id = `ROLLUP_ID` on-chain) — the exact
/// preimage `EEZL2.executeCrossChainCall` recomputes (`EEZL2.sol:197-199`)
/// and the ONLY field the on-chain consume compares to select the entry
/// (`_consumeAndExecute`, `EEZL2.sol:405`).
///
/// LEAN settlement-record shape: `callCount = 0`, empty `incomingCalls`,
/// `rollingHash = 0` — the mirror of [`build_l1_inbound_entry`] (the L1
/// deferred side). The entry MUST NOT re-deliver the call on L2: its
/// `targetAddress` is an L1 contract with NO code on L2, so a populated
/// (`callCount = 1`) entry makes `_processNCalls(1)` re-execute the call
/// against a codeless address (yielding `success = true, returnData = 0x`)
/// and re-fold `CALL_END(1, true, "")`, which never matches the entry's
/// `CALL_END(1, success, returnData_L1)` (the L1 leg's non-empty return)
/// → `RollingHashMismatch` at `EEZL2.sol:422` (proven on the live failed
/// outbound tx `0x0968ed79…`, revert selector `0xf3a3b67c`). With the lean
/// shape `_processNCalls(0)` is a no-op, leaving `_rollingHash = 0 ==
/// entry.rollingHash`, so the consume passes its post-checks, returns
/// `returnData`, and the user tx SUCCEEDS. The actual L1 delivery + ether
/// accounting are carried by the SEPARATE L1 immediate entry
/// (`build_l1_postbatch`) + settlement `StateDelta`, never by this L2
/// re-execution. DEFERRED (non-zero `proxyEntryHash`).
#[must_use]
pub fn build_l2_outbound_entry(entry: OutboundEntry) -> L2ExecutionEntrySol {
    let OutboundEntry {
        target,
        source,
        value,
        data,
        l2_rollup_id,
        return_data,
        // Unused in the lean settlement-record shape: the L2 entry no longer
        // re-delivers the call, so there is no rolling-hash fold to carry
        // `success`. The L1 leg's success is recovered downstream from the L1
        // immediate entry's rolling hash (`outbound_ether_out`), not here.
        success: _,
    } = entry;
    let proxy_entry_hash = cross_chain_call_hash(
        RollupId::MAINNET, // targetRollupId — L1 target's home rollup (proxy.originalRollupId)
        target,
        value,
        &data,
        source,
        l2_rollup_id, // sourceRollupId — the L2's own id (ROLLUP_ID on-chain)
    );

    L2ExecutionEntrySol {
        proxyEntryHash: proxy_entry_hash,
        // No re-delivery on L2 (target is a codeless L1 address) → no incoming
        // call, no fold. _processNCalls(0) is a no-op on-chain.
        incomingCalls: Vec::new(),
        expectedOutgoingCalls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: return_data,
        rollingHash: B256::ZERO,
    }
}

/// Build the L1 `postAndVerifyBatch` batch carrying a single DEFERRED entry for
/// the L1 entry for an inbound L1→L2 call (counterL1, L1 side).
///
/// Per the spec (`script/e2e/counter/E2E.s.sol::_l1Entries`), the L1 side of an
/// L1→L2 call does NOT execute the call — it queues a deferred entry whose
/// consumption (via `EEZ.executeCrossChainCall`, same-block) just returns the
/// precomputed `return_data` to the caller (CAP@L1) and applies the settlement
/// `StateDelta` (added later by `prepare_post_batch`). So: `callCount = 0`, no
/// `L2ToL1Calls`, `rollingHash = 0`, and a NON-zero `proxyEntryHash` =
/// `crossChainCallHash(dest_rollup_id, target, value, data, source, MAINNET=0)`
/// — the preimage `executeCrossChainCall` recomputes on-chain (it uses the L1
/// proxy's `originalRollupId` = `dest_rollup_id` as the target rollup, and
/// `MAINNET_ROLLUP_ID` as the source). The entry is DEFERRED (queued), so
/// `transientExecutionEntryCount = 0`.
#[must_use]
pub fn build_l1_inbound_entry(
    target: Address,
    value: U256,
    data: Bytes,
    source: Address,
    dest_rollup_id: RollupId,
    return_data: Bytes,
) -> EvmBatch {
    let proxy_entry_hash =
        cross_chain_call_hash(dest_rollup_id, target, value, &data, source, RollupId(0));

    let entry = ExecutionEntrySol {
        stateDeltas: Vec::new(), // the settlement delta is attached downstream
        proxyEntryHash: proxy_entry_hash,
        destinationRollupId: U256::from(dest_rollup_id.0),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: return_data,
        rollingHash: B256::ZERO,
    };

    EvmBatch {
        entries: vec![entry],
        l1ToL2lookupCalls: Vec::new(),
        // Deferred (queued, NOT immediate) → no transient prefix.
        transientExecutionEntryCount: U256::ZERO,
        transientLookupCallCount: U256::ZERO,
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    }
}

/// Build the follower-only INBOUND (L1→L2) DA-sidecar batch — the entry that
/// carries each incoming cross-chain call's params so the deriver can re-lower
/// it into `executeIncomingCrossChainCall`.
///
/// This is the inbound mirror of [`build_l1_postbatch`] (which builds the
/// OUTBOUND L2→L1 immediate entries) and the POPULATED counterpart of
/// [`build_l1_inbound_entry`] (the LEAN on-chain entry: empty `l2ToL1Calls`,
/// `callCount = 0`). The sidecar entry MUST carry the call in `l2ToL1Calls[0]`
/// (`callCount = 1`): the deriver's `build_inbound_system_txs` reads the inbound
/// call EXCLUSIVELY from `l2ToL1Calls[0]`, and the deriver's emptiness filter
/// drops entries with no `l2ToL1Calls`. It is shipped OFF-CHAIN only (serialized
/// into the opaque `callData` DA channel), so this populated shape never reaches
/// `EEZ._processNCalls` (where it would revert — `UnconsumedL2ToL1Calls` /
/// `RollingHashMismatch`); only the LEAN on-chain entry is consumed by the
/// bundled L1 user tx.
///
/// One entry per INCOMING call in `calls` (`target_rollup_id == this`, source on
/// another rollup). Every per-entry field is taken from the SAME recorded call
/// the on-chain `EntryBuilder` uses (`target_rollup_id`, `source_rollup_id`,
/// addresses, value, data, return data) so the sidecar's `proxyEntryHash` binds
/// the SAME cross-chain call as the on-chain entry; only `l2ToL1Calls` /
/// `callCount` differ. The `build_inbound_system_txs` consumer reuses
/// `l2ToL1Calls[0].sourceRollupId` for BOTH the L2 hash recompute and the
/// `executeIncomingCrossChainCall` `sourceRollup` arg, so the value is
/// self-consistent on delivery.
#[must_use]
pub fn build_l1_inbound_sidecar(calls: &[ExecutedAction], target_rollup_id: RollupId) -> EvmBatch {
    let mut entries: Vec<ExecutionEntrySol> = Vec::new();

    for call in calls {
        // Only INCOMING calls (originated on another rollup) become inbound
        // delivery entries. Intra-rollup / nested calls are re-executed as part
        // of the delivery itself, not as separate entries.
        if call.source_rollup_id == target_rollup_id {
            continue;
        }
        // Symmetric with `build_l1_postbatch` / `build_batch`'s "no top-level
        // success → empty" rule: a reverted inbound call has no delivery to
        // reconstruct (the failure path uses `build_l1_inbound_failed`).
        if !call.outcome.is_success() {
            continue;
        }
        let success = true;
        let return_bytes: Vec<u8> = call
            .outcome
            .return_data()
            .map(<[u8]>::to_vec)
            .unwrap_or_default();

        // SAME preimage as the on-chain `EntryBuilder::new` (6 fields, same
        // order) so the sidecar and the lean on-chain entry bind the identical
        // cross-chain call.
        let proxy_entry_hash = cross_chain_call_hash(
            call.target_rollup_id,
            call.target_address,
            call.value,
            &call.data,
            call.source_address,
            call.source_rollup_id,
        );

        // One incoming top-level call → callCount = 1; rolling hash folds
        // CALL_BEGIN(1) ++ CALL_END(1, success, returnData), exactly as
        // `EEZL2._processNCalls` recomputes on delivery.
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, success, &return_bytes);

        entries.push(ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: proxy_entry_hash,
            destinationRollupId: U256::from(call.target_rollup_id.0),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: call.target_address,
                value: call.value,
                data: call.data.clone(),
                sourceAddress: call.source_address,
                sourceRollupId: U256::from(call.source_rollup_id.0),
                revertSpan: U256::from(call.revert_span.unwrap_or(0)),
            }],
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::from(1u8),
            returnData: Bytes::from(return_bytes),
            rollingHash: B256::from(rolling.current()),
        });
    }

    let inbound_count = entries.len();
    debug!(
        target: "eez::entries",
        %target_rollup_id,
        inbound_count,
        "build_l1_inbound_sidecar: follower-only inbound DA-sidecar batch built",
    );
    EvmBatch {
        entries,
        l1ToL2lookupCalls: Vec::new(),
        // Off-chain DA sidecar entries — never executed on-chain, so no
        // transient (immediate) prefix (mirrors the deferred lean entry).
        transientExecutionEntryCount: U256::ZERO,
        transientLookupCallCount: U256::ZERO,
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    }
}

/// Build an L1 `postAndVerifyBatch` batch carrying a single SETTLEMENT-ONLY
/// immediate entry — a "pure state commitment" with no cross-chain call.
///
/// Used by the inbound (L1→L2) PROVEN settlement (counterL1 Layer 2): after the
/// L2 executes `executeIncomingCrossChainCall` and seals a block with root `R`,
/// the composer settles `R` on the L1 with this batch, routed through the prover
/// (which validates `newState == R == the L2 block root` and signs) — NOT the
/// composer co-signing. The entry is IMMEDIATE (`proxyEntryHash == 0`,
/// `transientExecutionEntryCount = 1`) so `EEZ.postAndVerifyBatch` applies it
/// INLINE via `attemptApplyImmediate` → `_applyStateDeltas`, with no calls
/// (`callCount = 0`, `rollingHash = 0`) and no user consume required. The
/// settlement `StateDelta` itself is attached by `prepare_post_batch`.
///
/// (The L1 user's proven precomputed result of the call — the deferred-entry consume — is a
/// separate concern; see [`build_l1_inbound_entry`].)
#[must_use]
pub fn build_l1_settlement_only(rollup_id: RollupId) -> EvmBatch {
    let entry = ExecutionEntrySol {
        stateDeltas: Vec::new(),    // the settlement delta is attached downstream
        proxyEntryHash: B256::ZERO, // immediate / inline
        destinationRollupId: U256::from(rollup_id.0),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: Bytes::new(),
        rollingHash: B256::ZERO,
    };
    EvmBatch {
        entries: vec![entry],
        l1ToL2lookupCalls: Vec::new(),
        // Immediate (applied inline by postAndVerifyBatch).
        transientExecutionEntryCount: U256::from(1u8),
        transientLookupCallCount: U256::ZERO,
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    }
}

/// Build an L1 `postAndVerifyBatch` batch for a FAILED inbound L1→L2 call.
///
/// When the inbound call REVERTS on the L2 target (a `require`/revert, or a `value>0`
/// the target rejects), the L2 still SEALS — the revert is CAPTURED into the entry's
/// rolling hash (`CALL_END(.., success=false, retData)`), the system tx succeeds, and the
/// root advances to R — so R must still settle. But the L1 user's proxy call must observe
/// `(false, revertData)`, NOT a success returning Y. The protocol expresses a failed
/// top-level call as a `LookupCall { failed: true }` (`EEZ._tryRevertedTopLevelLookup`
/// reverts with its raw `returnData`), NOT an `ExecutionEntry`. So this batch carries BOTH,
/// processed via INDEPENDENT on-chain paths:
///
/// - `entries[0]`: an IMMEDIATE settlement-only entry (`proxyEntryHash == 0`); the
///   settlement `StateDelta` R is attached downstream by `prepare_post_batch`; drained
///   inline by `postAndVerifyBatch`, advancing the root.
/// - `l1ToL2lookupCalls[0]`: the failed lookup `{crossChainCallHash:H, returnData:revert_data,
///   failed:true, callNumber:0, lastNestedActionConsumed:0}` — PUBLISHED
///   (`transientLookupCallCount = 0`) into `verificationByRollup[dest].lookupQueue`. The
///   user's proxy call (same block) misses the empty entry queue and falls into
///   `_tryRevertedTopLevelLookup`, reverting with `revert_data`.
///
/// `H = cross_chain_call_hash(dest_rollup_id, target, value, data, source, MAINNET=0)` — the
/// exact hash `executeCrossChainCall` recomputes (target rollup = dest, source = MAINNET).
/// At pin 5c51e02 the failed lookup runs as a degenerate reverted-lookup mini-entry
/// (`callCount = 0`, empty tables, empty `expectedStateRoots` → vacuous pin match), which
/// `_executeRevertedLookup` reverts with `returnData` — observably identical to the prior
/// direct revert. Proven on-chain by `scripts/test-counterl1-fail.sh` (the live e2e) and the
/// submodule's `test_RevertedLookup_TopLevel_Reverts`.
#[must_use]
pub fn build_l1_inbound_failed(
    target: Address,
    value: U256,
    data: Bytes,
    source: Address,
    dest_rollup_id: RollupId,
    revert_data: Bytes,
) -> EvmBatch {
    let h = cross_chain_call_hash(dest_rollup_id, target, value, &data, source, RollupId(0));

    // entries[0]: the immediate settlement-only entry (the R delta is attached by
    // `prepare_post_batch` via `chain_settlement_deltas` — single entry, one
    // R0→R delta). Same shape as build_l1_settlement_only.
    let settlement = ExecutionEntrySol {
        stateDeltas: Vec::new(),
        proxyEntryHash: B256::ZERO,
        destinationRollupId: U256::from(dest_rollup_id.0),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: Bytes::new(),
        rollingHash: B256::ZERO,
    };

    // l1ToL2lookupCalls[0]: the failed top-level lookup that supplies the user's
    // revert. New (5c51e02) shape: no cursor key; matched by hash + state-root
    // pins. We pin NOTHING (`expectedStateRoots: []`, vacuous match) — the
    // consume is already gated same-block + queue-wipe-on-next-verify (D4); and
    // `callCount: 0` + empty tables + `rollingHash: 0` make the reverted-lookup
    // mini-entry a degenerate `_processNCalls(0)` that reverts with `returnData`,
    // observably identical to the old direct revert.
    let failed = LookupCallSol {
        crossChainCallHash: h,
        destinationRollupId: U256::from(dest_rollup_id.0),
        returnData: revert_data,
        failed: true,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        rollingHash: B256::ZERO,
        expectedStateRoots: Vec::new(),
    };

    EvmBatch {
        entries: vec![settlement],
        l1ToL2lookupCalls: vec![failed],
        // Settlement drained inline; the failed lookup PUBLISHED to lookupQueue.
        transientExecutionEntryCount: U256::from(1u8),
        transientLookupCallCount: U256::ZERO,
        proofSystems: Vec::new(),
        rollupIdsWithProofSystems: Vec::new(),
        crossProofSystemInteractions: B256::ZERO,
        blobIndices: Vec::new(),
        callData: Bytes::new(),
        proofs: Vec::new(),
        blockNumber: 0,
    }
}

/// Encode `EEZL2.executeIncomingCrossChainCall` calldata for the inbound L1→L2
/// call `(destination, value, data, source, source_rollup_id)`, loading `entry`
/// (built by [`build_l2_incoming_entry`]). System-only on-chain. Returns the
/// 4-byte-selector-prefixed calldata for a SYSTEM tx to the L2 CCM.
#[must_use]
pub fn encode_execute_incoming(
    destination: Address,
    value: U256,
    data: Bytes,
    source: Address,
    source_rollup_id: RollupId,
    entry: L2ExecutionEntrySol,
) -> Vec<u8> {
    use crate::abi::executeIncomingCrossChainCallCall;
    executeIncomingCrossChainCallCall {
        destination,
        value,
        data,
        sourceAddress: source,
        sourceRollup: U256::from(source_rollup_id.0),
        entries: vec![entry],
        lookupCalls: Vec::new(),
    }
    .abi_encode()
}

/// The `executeIncomingCrossChainCall` 4-byte selector, DERIVED from the ABI
/// (`SolCall::SELECTOR`) — never hardcoded. Marks an inbound (L1→L2) system tx.
pub const EXECUTE_INCOMING_SELECTOR: [u8; 4] =
    crate::abi::executeIncomingCrossChainCallCall::SELECTOR;

/// Decode the inbound return value `Y` (the entry's `returnData`) from an
/// `executeIncomingCrossChainCall` calldata. `None` if the calldata isn't that
/// call or carries no entry. The prover uses this to re-derive `Y` from a
/// sealed block and gate the L1 inbound entry's `returnData` against it — the
/// X==Y soundness close (the inbound entry is committed in the signed
/// `publicInputsHash`).
#[must_use]
pub fn decode_inbound_return_data(calldata: &[u8]) -> Option<Bytes> {
    crate::abi::executeIncomingCrossChainCallCall::abi_decode(calldata)
        .ok()?
        .entries
        .into_iter()
        .next()
        .map(|e| e.returnData)
}

/// The inbound L1→L2 call + outcome re-derived from a sealed block's
/// `executeIncomingCrossChainCall`. Every field is REAL (not a composer claim): the L2
/// block sealed only because `EEZL2` bound the call args into the entry's `proxyEntryHash`
/// (`EEZL2.sol:209`) AND checked `entry.rollingHash` against the real `(success, returnData)`
/// (`EEZL2.sol:216`). So the prover can trust `(target, value, data, source, return_data,
/// success)` decoded here to gate the L1 batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedInbound {
    /// The L2 target the inbound call hit (`executeIncomingCrossChainCall.destination`).
    pub target: Address,
    /// The call's ETH value.
    pub value: U256,
    /// The call's calldata.
    pub data: Bytes,
    /// The L1 source address that originated the call (`sourceAddress`).
    pub source: Address,
    /// The call's return bytes — the real return on success, the revert payload on failure.
    pub return_data: Bytes,
    /// Whether the inbound call succeeded (re-derived from the entry rolling hash).
    pub success: bool,
}

/// Decode + re-derive [`DecodedInbound`] from inbound `executeIncomingCrossChainCall`
/// calldata. The `success` flag is recovered by recomputing the entry rolling hash for
/// `success ∈ {true,false}` and matching `entry.rollingHash` — a malicious composer can't
/// claim success on a call that reverted (the block wouldn't have sealed). Returns `None`
/// if the calldata isn't an inbound delivery or the rolling hash matches neither flag.
///
/// The prover uses ALL fields to gate the L1 batch against the real outcome: the SHAPE
/// (success ⇒ returning deferred entry; failure ⇒ failed `LookupCall`) AND the call HASH
/// (the entry's `proxyEntryHash` / the lookup's `crossChainCallHash` must equal
/// `cross_chain_call_hash(settled_rollup, target, value, data, source, MAINNET=0)` — the H
/// the user computes on-chain — so the composer can't ship a delivery keyed on a hash the
/// user will never consume, which would grief them into `ExecutionNotFound`).
#[must_use]
pub fn decode_inbound(calldata: &[u8]) -> Option<DecodedInbound> {
    let call = crate::abi::executeIncomingCrossChainCallCall::abi_decode(calldata).ok()?;
    let target = call.destination;
    let value = call.value;
    let data = call.data;
    let source = call.sourceAddress;
    let entry = call.entries.into_iter().next()?;
    let return_data = entry.returnData;
    // build_l2_incoming_entry folds exactly: call_begin(1) then call_end(1, success, return_data).
    let mut success = None;
    for s in [true, false] {
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, s, &return_data);
        if B256::from(rolling.current()) == entry.rollingHash {
            success = Some(s);
            break;
        }
    }
    Some(DecodedInbound {
        target,
        value,
        data,
        source,
        return_data,
        success: success?,
    })
}

/// Encode `batch` as
/// `EEZ.postAndVerifyBatch` calldata.
///
/// Under the multi-prover ABI, proofs live inside the batch struct
/// (`batch.proofs[]`). Callers populate `proofs[]` before
/// encoding — see `composer-lib::post_batch_submitter`
/// (`prepare_post_batch` + the proof sink) for the canonical
/// fill+encode+submit pipeline.
#[must_use]
pub fn encode_postbatch(batch: &EvmBatch) -> Vec<u8> {
    postAndVerifyBatchCall {
        batch: batch.clone(),
    }
    .abi_encode()
}

/// Decode `EEZ.postAndVerifyBatch` calldata back into an [`EvmBatch`] — the
/// inverse of [`encode_postbatch`]. The prover uses this to reconstruct the
/// exact batch the composer finalized (shipped over the control feed) so it can
/// recompute the `publicInputsHash` byte-for-byte and check the settlement
/// `StateDelta` before attesting.
pub fn decode_postbatch(calldata: &[u8]) -> alloy_sol_types::Result<EvmBatch> {
    let call = postAndVerifyBatchCall::abi_decode(calldata)?;
    Ok(call.batch)
}

// ── Internal helpers ───────────────────────────────────────────

struct EntryBuilder {
    /// Outer top-level call's cross-chain-call hash — pinned at
    /// construction so the entry's `proxyEntryHash` field is stable
    /// as nested children land.
    proxy_entry_hash: B256,
    /// Top-level call's pre-computed return data.
    return_data: Bytes,
    /// Routing target for the entry's L1 queue
    /// (`verificationByRollup[destinationRollupId].queue`). For
    /// proxy-backed deferred entries this is the target chain of
    /// the outer call (`outer.target_rollup_id`).
    destination_rollup_id: U256,
    /// Per-entry stateDeltas — populated by the outer call's
    /// affected rollup(s); empty for non-state-mutating fixtures.
    state_deltas: Vec<StateDeltaSol>,
    /// Flat call list — reentrant children of the outer call in
    /// execution order.
    l2_to_l1_calls: Vec<L2ToL1CallSol>,
    /// Reentrant-success descendants → consumed sequentially by the
    /// outer call's execution.
    expected_l1_to_l2_calls: Vec<ExpectedL1ToL2CallSol>,
    /// Tagged rolling-hash accumulator. Mirrors `_rollingHash` in
    /// `EEZ._processNCalls` / `_consumeNestedAction`.
    rolling: EntryRollingHash,
}

impl EntryBuilder {
    fn new(outer: &ExecutedAction, _dialect: ChainDialect) -> Self {
        let proxy_entry_hash = cross_chain_call_hash(
            outer.target_rollup_id,
            outer.target_address,
            outer.value,
            &outer.data,
            outer.source_address,
            outer.source_rollup_id,
        );
        let return_data: Bytes = match &outer.outcome {
            crate::ExecutionOutcome::Resolved { return_data, .. } => {
                Bytes::from(return_data.clone())
            }
            crate::ExecutionOutcome::Pending => Bytes::new(),
        };
        Self {
            proxy_entry_hash,
            return_data,
            destination_rollup_id: U256::from(outer.target_rollup_id.0),
            state_deltas: Vec::new(),
            l2_to_l1_calls: Vec::new(),
            expected_l1_to_l2_calls: Vec::new(),
            rolling: EntryRollingHash::new(),
        }
    }

    fn append_nested(&mut self, call: &ExecutedAction, nested_number: u64) {
        let hash = cross_chain_call_hash(
            call.target_rollup_id,
            call.target_address,
            call.value,
            &call.data,
            call.source_address,
            call.source_rollup_id,
        );
        let return_data: Bytes = call
            .outcome
            .return_data()
            .map(<[u8]>::to_vec)
            .unwrap_or_default()
            .into();
        self.rolling.nested_begin(nested_number);
        self.rolling.nested_end(nested_number);
        self.expected_l1_to_l2_calls.push(ExpectedL1ToL2CallSol {
            crossChainCallHash: hash,
            callCount: U256::ZERO,
            returnData: return_data,
        });
    }

    fn finish(self) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: self.state_deltas,
            proxyEntryHash: self.proxy_entry_hash,
            destinationRollupId: self.destination_rollup_id,
            l2ToL1Calls: self.l2_to_l1_calls,
            expectedL1ToL2Calls: self.expected_l1_to_l2_calls,
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: self.return_data,
            rollingHash: B256::from(self.rolling.current()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionOutcome;
    use crate::abi::ExpectedLookupSol;
    use alloy_primitives::address;
    use alloy_sol_types::SolValue;

    /// A2.1a: the OUTBOUND L2 deferred entry's `proxyEntryHash` must byte-match
    /// the preimage `EEZL2.executeCrossChainCall` recomputes on-chain
    /// (`targetRollupId=MAINNET(0)`, `sourceRollupId=L2`). The two RollupId args
    /// are silently swappable — backwards = `ExecutionNotFound` — so this pins
    /// the direction, the shape gates, and the rolling-hash fold (success +
    /// returnData).
    #[test]
    fn outbound_l2_entry_hash_direction_and_lean_shape() {
        let c = address!("00000000000000000000000000000000000000cc"); // L1 target
        let d = address!("00000000000000000000000000000000000000dd"); // L2 user EOA
        let l2 = RollupId(42069);
        let data = Bytes::from(vec![0x55, 0x24, 0x10, 0x77, 0x07]);
        let value = U256::ZERO;
        // A NON-EMPTY L1 return — the exact asymmetry that broke the old
        // populated shape: on L2 the re-delivery to a codeless L1 target
        // returns empty, so CALL_END(1,true,"") never matched the entry's
        // CALL_END(1,true,<non-empty>) → RollingHashMismatch (EEZL2.sol:422).
        let ret = Bytes::from(vec![0xAB, 0xCD]);

        let entry = build_l2_outbound_entry(OutboundEntry {
            target: c,
            source: d,
            value,
            data: data.clone(),
            l2_rollup_id: l2,
            return_data: ret.clone(),
            success: true,
        });

        // proxyEntryHash == cross_chain_call_hash(MAINNET(0), C, 0, data, D, L2)
        // — the ONLY field the on-chain consume compares to select the entry
        // (`_consumeAndExecute`, EEZL2.sol:405).
        assert_eq!(
            entry.proxyEntryHash,
            cross_chain_call_hash(RollupId::MAINNET, c, value, &data, d, l2),
            "outbound proxyEntryHash must match the (MAINNET, …, L2) preimage",
        );
        assert_ne!(
            entry.proxyEntryHash,
            B256::ZERO,
            "deferred entry → non-zero proxyEntryHash"
        );

        // Direction is load-bearing: the INBOUND hash (rollup args swapped) MUST differ.
        assert_ne!(
            entry.proxyEntryHash,
            cross_chain_call_hash(l2, c, value, &data, d, RollupId::MAINNET),
            "outbound (MAINNET,…,L2) must differ from inbound (L2,…,MAINNET) — the swap is the #1 risk",
        );

        // LEAN settlement-record shape (mirrors build_l1_inbound_entry): callCount=0
        // → _processNCalls(0) is a no-op on-chain → _rollingHash stays 0 ==
        // entry.rollingHash at EEZL2.sol:422, so the consume passes and the user
        // tx SUCCEEDS (instead of reverting RollingHashMismatch).
        assert_eq!(entry.callCount, U256::ZERO, "lean: no re-delivery on L2");
        assert!(
            entry.incomingCalls.is_empty(),
            "lean: no incoming call to re-execute against a codeless L1 target",
        );
        assert_eq!(
            entry.rollingHash,
            B256::ZERO,
            "lean: empty fold matches the on-chain no-op _processNCalls(0)",
        );
        assert!(entry.expectedOutgoingCalls.is_empty());
        assert!(entry.expectedLookups.is_empty());

        // returnData is still carried through to the caller (the user's proxy
        // call returns it) but is NOT folded into the rolling hash, so a
        // non-empty L1 return no longer forces a mismatch.
        assert_eq!(entry.returnData, ret, "returnData passes through unchanged");

        // The lean rolling hash is independent of `success`/`return_data` — both
        // always leave rollingHash == 0 (no fold). The L1 leg's success/return
        // are recovered downstream from the L1 immediate entry, not this one.
        let failed = build_l2_outbound_entry(OutboundEntry {
            target: c,
            source: d,
            value,
            data,
            l2_rollup_id: l2,
            return_data: Bytes::new(),
            success: false,
        });
        assert_eq!(
            failed.rollingHash,
            B256::ZERO,
            "lean rollingHash is always 0, regardless of success/returnData",
        );
        assert_eq!(failed.callCount, U256::ZERO);
    }

    fn record(target: RollupId, caller_rollup: RollupId, success: bool) -> ExecutedAction {
        ExecutedAction {
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: target,
            source_rollup_id: caller_rollup,
            source_address: address!("00000000000000000000000000000000000000bb"),
            data: Bytes::from_static(&[0x12, 0x34]),
            value: U256::ZERO,
            outcome: ExecutionOutcome::Resolved {
                return_data: Vec::new(),
                pre_state_root: [0u8; 32],
                post_state_root: [0u8; 32],
                gas_used: 21_000,
                success,
            },
            revert_span: None,
        }
    }


    /// A #28 execution entry whose `expectedLookups` (the PR#28-only field,
    /// absent from the #27 `ExecutionEntry`) is NON-empty, so a decode canary
    /// built on it actually exercises the new wire field.
    fn lookup_bearing_entry() -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::repeat_byte(0xaa),
            destinationRollupId: U256::from(1u64),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: address!("00000000000000000000000000000000000000aa"),
                value: U256::from(42u64),
                data: Bytes::from_static(&[0xde, 0xad]),
                sourceAddress: address!("00000000000000000000000000000000000000bb"),
                sourceRollupId: U256::from(1u64),
                revertSpan: U256::ZERO,
            }],
            expectedL1ToL2Calls: vec![ExpectedL1ToL2CallSol {
                crossChainCallHash: B256::repeat_byte(0xc1),
                callCount: U256::from(1u64),
                returnData: Bytes::from_static(&[0x01]),
            }],
            expectedLookups: vec![ExpectedLookupSol {
                crossChainCallHash: B256::repeat_byte(0x77),
                returnData: Bytes::from_static(&[0xca, 0xfe]),
                failed: true,
                l2ToL1CallNumber: 3,
                lastL1ToL2CallConsumed: 2,
                executingLookupIndex: 1,
                l2ToL1Calls: Vec::new(),
                expectedL1ToL2Calls: Vec::new(),
                callCount: U256::from(9u64),
                rollingHash: B256::repeat_byte(0x99),
            }],
            callCount: U256::from(7u64),
            returnData: Bytes::from_static(&[0xfe, 0xed]),
            rollingHash: B256::repeat_byte(0x33),
        }
    }

    /// Phase-A decode canary (entry) — guards `deriver.rs:929`
    /// `ExecutionEntrySol::abi_decode`. based-rollup has no deriver, so this
    /// decode path was NEVER exercised upstream. Reverting the decode struct to
    /// the #27 shape (no `expectedLookups`) fails to compile against this test;
    /// any wire-layout drift fails the round-trip below.
    #[test]
    fn decode_canary_entry_roundtrip_preserves_pr28_lookups() {
        let entry = lookup_bearing_entry();
        let encoded = entry.abi_encode();
        let decoded = ExecutionEntrySol::abi_decode(&encoded)
            .expect("a freshly-encoded #28 entry must decode");

        assert_eq!(decoded.proxyEntryHash, entry.proxyEntryHash);
        assert_eq!(decoded.destinationRollupId, entry.destinationRollupId);
        assert_eq!(decoded.callCount, entry.callCount);
        assert_eq!(decoded.returnData, entry.returnData);
        assert_eq!(decoded.rollingHash, entry.rollingHash);
        assert_eq!(decoded.l2ToL1Calls.len(), 1);
        assert_eq!(decoded.l2ToL1Calls[0].value, U256::from(42u64));
        assert_eq!(
            decoded.l2ToL1Calls[0].data,
            Bytes::from_static(&[0xde, 0xad])
        );
        assert_eq!(decoded.l2ToL1Calls[0].revertSpan, U256::ZERO);

        // The PR#28-only field must survive byte-for-byte.
        assert_eq!(
            decoded.expectedLookups.len(),
            1,
            "expectedLookups dropped on decode — decode struct is not #28",
        );
        let lk = &decoded.expectedLookups[0];
        assert_eq!(lk.crossChainCallHash, B256::repeat_byte(0x77));
        assert!(lk.failed);
        assert_eq!(lk.l2ToL1CallNumber, 3);
        assert_eq!(lk.lastL1ToL2CallConsumed, 2);
        assert_eq!(lk.executingLookupIndex, 1);
        assert_eq!(lk.callCount, U256::from(9u64));
        assert_eq!(lk.returnData, Bytes::from_static(&[0xca, 0xfe]));
    }

    /// Phase-A decode canary (batch) — guards `deriver.rs:1067`
    /// `postAndVerifyBatchCall::abi_decode` via the same `decode_postbatch`
    /// helper the deriver uses. The lookup-bearing entry rides the FULL
    /// postAndVerifyBatch calldata, so `expectedLookups` must survive the whole
    /// L1-batch tuple, not just a standalone entry.
    #[test]
    fn decode_canary_postbatch_roundtrip_via_deriver_helper() {
        let mut batch = build_l1_postbatch(&[record(RollupId(0), RollupId(1), true)], RollupId(1));
        batch.entries.push(lookup_bearing_entry());

        let calldata = postAndVerifyBatchCall {
            batch: batch.clone(),
        }
        .abi_encode();
        let decoded = decode_postbatch(&calldata).expect("deriver decode of our own postBatch");

        assert_eq!(decoded.entries.len(), batch.entries.len());
        let last = decoded.entries.last().expect("entry present");
        assert_eq!(
            last.expectedLookups.len(),
            1,
            "PR#28 lookups lost across the postAndVerifyBatch round-trip",
        );
        assert_eq!(last.expectedLookups[0].l2ToL1CallNumber, 3);
        assert_eq!(last.callCount, U256::from(7u64));
    }

    /// Phase-A decode canary (negative) — the deriver decode must REJECT
    /// malformed / short calldata, never silently accept partial garbage (the
    /// failure mode an arity skew would produce on the wire).
    #[test]
    fn decode_canary_rejects_truncated_postbatch() {
        let batch = build_l1_postbatch(&[record(RollupId(0), RollupId(1), true)], RollupId(1));
        let calldata = postAndVerifyBatchCall {
            batch: batch.clone(),
        }
        .abi_encode();
        let truncated = &calldata[..calldata.len() / 2];
        assert!(
            decode_postbatch(truncated).is_err(),
            "deriver decode must reject malformed/short postBatch calldata",
        );
    }

    #[test]
    fn empty_recorded_yields_empty_batch() {
        let batch = build_batch(&[], &ChainDialect::EvmL1Style, RollupId(1))
            .expect("build_batch ok");
        assert!(batch.entries.is_empty());
        assert!(batch.l1ToL2lookupCalls.is_empty());
        assert!(batch.is_empty());
    }

    #[test]
    fn originating_call_omits_outer_from_l2_to_l1_calls() {
        // L1's batch for an originating L1→L2 call (the entry consumed on
        // the source chain via `executeCrossChainCall`): the top-level call
        // is NOT folded — `l2ToL1Calls` empty, callCount=0, rollingHash=0.
        // Only reentrant L2→L1 children fold; the effect rides the
        // stateDelta and the return value (if any) is carried separately in
        // `returnData`. (sync-rollups-protocol@fe7bf66 — a folded outer here
        // makes `executeCrossChainCall` revert `RollingHashMismatch`.)
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &ChainDialect::EvmL1Style, RollupId(0))
            .expect("build_batch ok");
        assert_eq!(batch.entries.len(), 1);
        // The TopLevel call is described by the entry's
        // `proxyEntryHash` + `returnData`; reentrant children land
        // in `l2ToL1Calls`. No children here, so both arrays empty.
        assert!(batch.entries[0].l2ToL1Calls.is_empty());
        // origin/main: the outer is omitted → callCount stays 0.
        assert_eq!(batch.entries[0].callCount, U256::ZERO);
        assert!(batch.entries[0].expectedL1ToL2Calls.is_empty());
        assert!(batch.l1ToL2lookupCalls.is_empty());
        assert_ne!(batch.entries[0].proxyEntryHash, B256::ZERO);
        assert_eq!(batch.entries[0].rollingHash, B256::ZERO);
        assert_eq!(
            batch.entries[0].destinationRollupId,
            U256::from(1),
            "destinationRollupId is the target chain of the outer call",
        );
        // NOTE: in this branch's architecture `build_batch` leaves
        // `stateDeltas` empty — the settling `R_{k-1}→R_k` delta is attached
        // downstream by `prepare_post_batch`, not by `EntryBuilder` (origin/main
        // populated it via `build_outer_state_deltas`, which our type model does
        // not use), so we do not assert a stateDelta here.
    }

    #[test]
    fn l1_postbatch_emits_immediate_executing_entry() {
        // An L2→L1 call: target = L1 (RollupId 0), caller = L2 (RollupId 1).
        // Mirrors `script/e2e/counterL2/E2E.s.sol::_l1Entries`.
        let calls = vec![record(RollupId(0), RollupId(1), true)];
        let batch = build_l1_postbatch(&calls, RollupId(1));

        assert_eq!(batch.entries.len(), 1);
        assert_eq!(
            batch.transientExecutionEntryCount,
            U256::from(1),
            "immediate: transientExecutionEntryCount covers the entry",
        );

        let e = &batch.entries[0];
        // Immediate/system-driven → proxyEntryHash == 0 (the L2 DEFERRED mirror
        // carries the non-zero call hash instead — see single_top_level above).
        assert_eq!(e.proxyEntryHash, B256::ZERO);
        // The inbound call lands flat in L2ToL1Calls with callCount = 1.
        assert_eq!(e.callCount, U256::from(1));
        assert_eq!(e.l2ToL1Calls.len(), 1);
        let c = &e.l2ToL1Calls[0];
        assert_eq!(c.targetAddress, calls[0].target_address);
        assert_eq!(c.sourceAddress, calls[0].source_address);
        assert_eq!(
            c.sourceRollupId,
            U256::from(1),
            "sourceRollupId = caller's L2 id"
        );
        assert_eq!(c.data, calls[0].data);
        // Executing entry folds CALL_BEGIN/CALL_END → non-zero rolling hash
        // (the deferred L2 entry's rollingHash is 0).
        assert_ne!(e.rollingHash, B256::ZERO);
        assert!(
            e.stateDeltas.is_empty(),
            "settlement deltas wired in a later step"
        );
        assert!(e.expectedL1ToL2Calls.is_empty());
        assert!(!batch.is_empty());
    }

    #[test]
    fn outbound_ether_out_recovers_released_value_on_success() {
        // A SUCCESSFUL value-carrying L2→L1 call releases its value (etherOut = V),
        // so the settlement books etherDelta = -V.
        let mut a = record(RollupId(0), RollupId(1), true);
        a.value = U256::from(777u64);
        let batch = build_l1_postbatch(&[a], RollupId(1));
        assert_eq!(
            outbound_ether_out(&batch.entries[0]),
            Some(U256::from(777u64))
        );
    }

    #[test]
    fn outbound_ether_out_zero_for_failed_call() {
        // A FAILED value call releases nothing — etherOut excludes it
        // (recovered as success=false from the rolling hash). build_l1_postbatch
        // no longer emits such an entry (it skips reverted calls), so construct
        // the failed-shape entry directly to keep covering outbound_ether_out's
        // false branch.
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, false, &Bytes::new());
        let entry = ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(1u64),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: address!("00000000000000000000000000000000000000aa"),
                value: U256::from(777u64),
                data: Bytes::new(),
                sourceAddress: address!("00000000000000000000000000000000000000bb"),
                sourceRollupId: U256::from(1u64),
                revertSpan: U256::ZERO,
            }],
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::from(1u8),
            returnData: Bytes::new(),
            rollingHash: B256::from(rolling.current()),
        };
        assert_eq!(outbound_ether_out(&entry), Some(U256::ZERO));
    }

    #[test]
    fn outbound_ether_out_zero_for_valueless() {
        // No value (inbound deferred / heartbeat / value-0 outbound) → 0.
        let batch = build_l1_postbatch(&[record(RollupId(0), RollupId(1), true)], RollupId(1));
        assert_eq!(outbound_ether_out(&batch.entries[0]), Some(U256::ZERO));
    }

    #[test]
    fn outbound_ether_out_rejects_multicall_value() {
        // Multi-call value (callCount>1) isn't supported → None (caller must reject).
        let mut a = record(RollupId(0), RollupId(1), true);
        a.value = U256::from(5u64);
        let mut entry = build_l1_postbatch(&[a], RollupId(1)).entries.pop().unwrap();
        entry.l2ToL1Calls.push(entry.l2ToL1Calls[0].clone());
        entry.callCount = U256::from(2u8);
        assert_eq!(outbound_ether_out(&entry), None);
    }

    #[test]
    fn l2_incoming_entry_mirrors_the_inbound_call() {
        // Inbound L1→L2 (counterL1): the L2 mirror entry for a call to
        // Counter@L2 from CAP@L1. Mirrors `script/e2e/counter/E2E.s.sol::_l2Entries`.
        let counter = address!("00000000000000000000000000000000000000aa");
        let cap = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[0xd0, 0x9d, 0xe0, 0x8a]); // increment()
        let ret = Bytes::from(alloy_primitives::U256::from(1u8).abi_encode()); // abi.encode(1)
        let l2_id = RollupId(33333); // arbitrary fixture id (registry-native: eez-dev's EEZL2.ROLLUP_ID is now 1)
        let src_id = RollupId(0); // MAINNET (L1 origin)

        let entry = build_l2_incoming_entry(IncomingEntry {
            target: counter,
            source: cap,
            value: U256::ZERO,
            data: data.clone(),
            source_rollup_id: src_id,
            l2_rollup_id: l2_id,
            return_data: ret.clone(),
            success: true,
        });

        // proxyEntryHash binds the call under the L2's OWN rollup id — the same
        // preimage `executeIncomingCrossChainCall` recomputes with ROLLUP_ID.
        assert_eq!(
            entry.proxyEntryHash,
            cross_chain_call_hash(l2_id, counter, U256::ZERO, &data, cap, src_id),
        );
        assert_eq!(entry.callCount, U256::from(1));
        assert_eq!(entry.incomingCalls.len(), 1);
        let c = &entry.incomingCalls[0];
        assert_eq!(c.targetAddress, counter);
        assert_eq!(c.sourceAddress, cap);
        assert_eq!(
            c.sourceRollupId,
            U256::ZERO,
            "inbound source = MAINNET (L1)"
        );
        assert_eq!(c.data, data);
        assert_eq!(entry.returnData, ret);
        assert_ne!(entry.rollingHash, B256::ZERO);
        // L2 (lean IEEZL2) carries no state deltas / destinationRollupId — the
        // absence is now type-enforced; the outgoing/lookup tables are empty.
        assert!(entry.expectedOutgoingCalls.is_empty() && entry.expectedLookups.is_empty());

        // The calldata round-trips through the executeIncomingCrossChainCall ABI.
        let calldata = encode_execute_incoming(
            counter,
            U256::ZERO,
            data.clone(),
            cap,
            src_id,
            entry.clone(),
        );
        use crate::abi::executeIncomingCrossChainCallCall;
        let decoded = executeIncomingCrossChainCallCall::abi_decode(&calldata)
            .expect("decode executeIncomingCrossChainCall");
        assert_eq!(decoded.destination, counter);
        assert_eq!(decoded.sourceAddress, cap);
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].proxyEntryHash, entry.proxyEntryHash);
    }

    #[test]
    fn decode_inbound_return_data_recovers_y() {
        // The prover's X==Y gate re-derives Y from a sealed
        // executeIncomingCrossChainCall exactly this way. Round-trip: encode an
        // inbound entry with returnData=Y, decode it back, assert Y — so an inbound entry
        // that cached X != Y would fail the prover's equality gate.
        let counter = address!("00000000000000000000000000000000000000aa");
        let cap = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[0xd0, 0x9d, 0xe0, 0x8a]); // increment()
        let y = Bytes::from(alloy_primitives::U256::from(7u8).abi_encode());
        let entry = build_l2_incoming_entry(IncomingEntry {
            target: counter,
            source: cap,
            value: U256::ZERO,
            data: data.clone(),
            source_rollup_id: RollupId(0),
            l2_rollup_id: RollupId(33333),
            return_data: y.clone(),
            success: true,
        });
        let calldata =
            encode_execute_incoming(counter, U256::ZERO, data.clone(), cap, RollupId(0), entry);

        // The selector is DERIVED from the ABI (not hardcoded) and matches.
        assert_eq!(calldata.get(0..4), Some(&EXECUTE_INCOMING_SELECTOR[..]));
        // The decode recovers Y exactly.
        assert_eq!(decode_inbound_return_data(&calldata), Some(y));
        // Non-matching calldata (wrong selector) → None: the gate fires ONLY on
        // a real inbound delivery, so outbound / other txs are skipped (no false
        // refusal of the outbound settlement path).
        assert_eq!(decode_inbound_return_data(&[0xde, 0xad, 0xbe, 0xef]), None);
    }

    #[test]
    fn decode_inbound_recovers_call_and_outcome() {
        // The prover's outcome gate re-derives the call fields + (returnData, success) from
        // a sealed executeIncomingCrossChainCall — the call args (for the H re-derivation)
        // and the success flag (by recomputing the entry rolling hash for each flag).
        let counter = address!("00000000000000000000000000000000000000aa");
        let cap = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[0xd0, 0x9d, 0xe0, 0x8a]); // increment()
        for (success, y) in [
            (
                true,
                Bytes::from(alloy_primitives::U256::from(7u8).abi_encode()),
            ),
            (false, Bytes::from_static(&[0xde, 0xad])), // a revert payload
        ] {
            let entry = build_l2_incoming_entry(IncomingEntry {
                target: counter,
                source: cap,
                value: U256::ZERO,
                data: data.clone(),
                source_rollup_id: RollupId(0),
                l2_rollup_id: RollupId(33333),
                return_data: y.clone(),
                success,
            });
            let calldata =
                encode_execute_incoming(counter, U256::ZERO, data.clone(), cap, RollupId(0), entry);
            assert_eq!(
                decode_inbound(&calldata),
                Some(DecodedInbound {
                    target: counter,
                    value: U256::ZERO,
                    data: data.clone(),
                    source: cap,
                    return_data: y,
                    success,
                }),
                "decode must recover the call args + returnData + success",
            );
        }
        // Wrong selector → None (the gate skips non-inbound txs).
        assert_eq!(decode_inbound(&[0xde, 0xad, 0xbe, 0xef]), None);
    }

    #[test]
    fn build_l1_inbound_failed_carries_settlement_and_failed_lookup() {
        let target = address!("00000000000000000000000000000000000000aa");
        let source = address!("00000000000000000000000000000000000000bb");
        let data = Bytes::from_static(&[0xd0, 0x9d, 0xe0, 0x8a]);
        let revert_data = Bytes::from_static(&[0xde, 0xad]);
        let dest = RollupId(1);
        let batch = build_l1_inbound_failed(
            target,
            U256::ZERO,
            data.clone(),
            source,
            dest,
            revert_data.clone(),
        );

        // entries[0] = immediate settlement-only (proxyEntryHash 0, empty returnData), drained inline.
        assert_eq!(batch.entries.len(), 1);
        let e = &batch.entries[0];
        assert_eq!(e.proxyEntryHash, B256::ZERO);
        assert!(e.returnData.is_empty());
        assert_eq!(batch.transientExecutionEntryCount, U256::from(1u8));

        // l1ToL2lookupCalls[0] = failed lookup with H + the revert data, top-level
        // (5c51e02: hash-keyed, empty pins, degenerate mini-entry), deferred.
        assert_eq!(batch.l1ToL2lookupCalls.len(), 1);
        let l = &batch.l1ToL2lookupCalls[0];
        assert!(l.failed);
        assert_eq!(l.returnData, revert_data);
        assert_eq!(l.callCount, U256::ZERO);
        assert_eq!(l.rollingHash, B256::ZERO);
        assert!(l.expectedStateRoots.is_empty());
        assert!(l.l2ToL1Calls.is_empty() && l.expectedL1ToL2Calls.is_empty());
        assert!(l.expectedLookups.is_empty());
        assert_eq!(
            l.crossChainCallHash,
            cross_chain_call_hash(dest, target, U256::ZERO, &data, source, RollupId(0)),
            "H must match what executeCrossChainCall recomputes (target=dest, source=MAINNET)",
        );
        assert_eq!(batch.transientLookupCallCount, U256::ZERO);
    }

    #[test]
    fn outgoing_to_other_rollup_yields_empty_batch_for_target() {
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &ChainDialect::EvmL2Style, RollupId(1))
            .expect("build_batch ok");
        assert!(batch.entries.is_empty());
        assert!(batch.l1ToL2lookupCalls.is_empty());
    }

    #[test]
    fn nested_reentrant_call_lands_in_expected_l1_to_l2_calls() {
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), true),
        ];
        let batch = build_batch(&calls, &ChainDialect::EvmL1Style, RollupId(0))
            .expect("build_batch ok");
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(
            batch.entries[0].expectedL1ToL2Calls.len(),
            1,
            "nested-success child must land in expectedL1ToL2Calls",
        );
        assert!(batch.l1ToL2lookupCalls.is_empty());
    }

    #[test]
    fn nested_failure_is_refused_d3_guard() {
        // 5c51e02 D3: a nested-failed call would need entry-scoped
        // `expectedLookups` emission (unbuilt) — build_batch refuses loudly
        // instead of mis-emitting a top-level lookup the new contract
        // mis-consumes.
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), false),
        ];
        let err = build_batch(&calls, &ChainDialect::EvmL1Style, RollupId(0))
            .expect_err("nested-failed lookup must be refused");
        assert!(
            format!("{err}").contains("entry-scoped emission"),
            "got: {err}"
        );
    }

    #[test]
    fn build_l1_postbatch_skips_reverted_calls() {
        // Review fix (2nd adaptation review): a reverted top-level L2→L1 call
        // produces NO executing L1 entry — symmetric with build_batch's empty
        // return, so an outbound failure settles nothing inconsistent.
        let ok = build_l1_postbatch(&[record(RollupId(0), RollupId(1), true)], RollupId(1));
        assert_eq!(
            ok.entries.len(),
            1,
            "successful L2→L1 call → one immediate entry"
        );

        let reverted = build_l1_postbatch(&[record(RollupId(0), RollupId(1), false)], RollupId(1));
        assert!(
            reverted.is_empty(),
            "fully-reverted outbound → empty L1 batch (zk-poster leg continues)"
        );

        // Mixed: only the successful call survives.
        let mixed = build_l1_postbatch(
            &[
                record(RollupId(0), RollupId(1), true),
                record(RollupId(0), RollupId(1), false),
            ],
            RollupId(1),
        );
        assert_eq!(
            mixed.entries.len(),
            1,
            "mixed batch keeps only the successful entry"
        );
    }

    #[test]
    fn terminal_revert_yields_empty_batch() {
        let calls = vec![record(RollupId(1), RollupId(0), false)];
        let batch = build_batch(&calls, &ChainDialect::EvmL1Style, RollupId(0))
            .expect("build_batch ok");
        assert!(batch.is_empty());
    }

    #[test]
    fn encode_postbatch_selector() {
        let batch = EvmBatch::default();
        let data = encode_postbatch(&batch);
        assert_eq!(&data[..4], &postAndVerifyBatchCall::SELECTOR);
    }
}
