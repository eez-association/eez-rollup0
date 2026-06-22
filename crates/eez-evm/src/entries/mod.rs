//! Unified flat-emitter entry building for the multi-prover protocol.
//!
//! [`build_batch`] walks the preorder `recorded[..]` slice once,
//! classifies each call (top-level / nested-success / nested-failed /
//! lookup), folds per-entry rolling hashes, and emits an [`EvmBatch`]
//! carrying the deferred-execution table + lookup queue + transient-
//! prefix metadata that `EEZ.postAndVerifyBatch` /
//! `CrossChainManagerL2.loadExecutionTable` consume. Per-fixture
//! byte-identity against upstream Foundry goldens is verified
//! end-to-end in `scripts/protocol-e2e.sh`.
//!
//! The proof-system fields on the produced batch stay empty
//! (`proofSystems = []`, `proofs = []`, …) on the calldata-only path.

use alloy_primitives::{B256, Bytes, I256, U256};
use alloy_sol_types::SolCall;
use eez_protocol::{
    ProtocolResult, RecordedCall, RollupId, SourceAttribution, rolling_hash::EntryRollingHash,
};

use crate::EvmProtocol;
use crate::action::cross_chain_call_hash;
use crate::batch::EvmBatch;
use crate::dialect::ChainDialect;
use crate::types::{
    ExecutionEntrySol, ExpectedL1ToL2CallSol, L2ToL1CallSol, LookupCallSol,
    ProofSystemBatchPerVerificationEntriesSol, StateDeltaSol, loadExecutionTableCall,
    postAndVerifyBatchCall,
};

/// Classification of a single [`RecordedCall`] within an entry's
/// flat call window. Drives [`build_batch`]'s emission decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    /// Outer call of an entry on `source_rollup_id`'s batch — either
    /// originating here (`caller_rollup_id == source_id`) or arriving
    /// from another rollup (`original_rollup_id == source_id`). The
    /// entry's `proxyEntryHash` matches this call's `crossChainCallHash`;
    /// `L2ToL1Calls[]` carries reentrant children, not this call itself.
    TopLevel,
    /// Reentrant cross-chain call dispatched from inside a top-level
    /// call's execution and which succeeded — routes to the entry's
    /// `expectedL1ToL2Calls[]` table.
    NestedSuccess,
    /// Reentrant cross-chain call which reverted (caught by try/catch
    /// in the caller) — routes to `lookupCalls[]` with `failed = true`.
    NestedFailed,
    /// Cross-chain call observed inside a `STATICCALL` frame
    /// (read-only) — routes to `lookupCalls[]` with `failed = false`.
    Static,
}

impl CallKind {
    /// Classify a recorded call relative to `source_id` (the rollup
    /// whose batch is being built).
    fn classify(call: &RecordedCall<EvmProtocol>, source_id: RollupId) -> Self {
        if call.static_meta.is_some() {
            return Self::Static;
        }
        // Both directions are TopLevel: originating here (caller ==
        // source) or arriving from another rollup (original == source).
        if call.caller_rollup_id == source_id || call.original_rollup_id == source_id {
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
/// `attribution` drives `StateDelta.currentState` chaining; `raw_tx`
/// is reserved for L1-style raw-tx routing. `source_rollup_id` is the
/// rollup THIS batch targets.
///
/// # Errors
///
/// Returns [`ProtocolErrorKind::InvalidEncoding`] if a call's
/// outcome is `Pending` (composition lifecycle bug — every call
/// should be resolved before entering finalize).
///
/// # Panics
///
/// Panics when `source_rollup_id` both originates AND receives
/// cross-chain traffic in one composition (nested re-entry, e.g.
/// L1→L2→L1) — unsupported while batch-shaping handles only flat
/// L1↔L2 transfers.
pub fn build_batch(
    recorded: &[RecordedCall<EvmProtocol>],
    attribution: &SourceAttribution<'_>,
    dialect: &ChainDialect,
    source_rollup_id: RollupId,
    raw_tx: &[u8],
) -> ProtocolResult<EvmBatch> {
    let _ = raw_tx; // L1-style raw-tx routing wired per fixture in the E2E suite

    let group: Vec<&RecordedCall<EvmProtocol>> = recorded
        .iter()
        .filter(|c| {
            c.caller_rollup_id == source_rollup_id || c.original_rollup_id == source_rollup_id
        })
        .collect();

    // Flat L1↔L2 only: a chain that BOTH originates and receives
    // cross-chain traffic in one composition (nested re-entry, e.g.
    // L1→L2→L1) needs a state-aware rolling-hash loop we don't have —
    // panic loudly (invariant 7) rather than emit a wrong batch shape.
    let has_originated = group.iter().any(|c| c.caller_rollup_id == source_rollup_id);
    let has_arrived = group.iter().any(|c| {
        c.original_rollup_id == source_rollup_id && c.caller_rollup_id != source_rollup_id
    });
    if has_originated && has_arrived {
        unimplemented!(
            "nested cross-chain re-entry not yet supported \
             (source_rollup_id={source_rollup_id} both originates and receives \
             cross-chain calls in this composition)"
        );
    }

    let any_top_level_success = group.iter().any(|c| {
        CallKind::classify(c, source_rollup_id) == CallKind::TopLevel && c.outcome.is_success()
    });
    if !any_top_level_success && !group.is_empty() {
        return Ok(EvmBatch::empty());
    }

    let mut entries: Vec<ExecutionEntrySol> = Vec::new();
    let mut lookup_calls: Vec<LookupCallSol> = Vec::new();
    let mut current_entry: Option<EntryBuilder> = None;
    let mut entry_nested_number: u64 = 0;

    for call in &group {
        let kind = CallKind::classify(call, source_rollup_id);
        match kind {
            CallKind::TopLevel => {
                if let Some(prev) = current_entry.take() {
                    entries.push(prev.finish());
                }
                let mut builder = EntryBuilder::new(call, *dialect, source_rollup_id, attribution);
                // Entry-rollup batch: omit the outer (callCount=0,
                // rollingHash=0). On consume, `executeCrossChainCall`
                // recomputes `rollingHash` by re-executing `L2ToL1Calls`,
                // which holds only reentrant L2→L1 children — not the
                // top-level call, whose effect rides `stateDeltas` and whose
                // return rides `returnData`. Folding it lets L1 re-execute
                // the outer against a codeless target, dropping the return
                // data; any return-bearing call then reverts
                // `RollingHashMismatch`. The target batch (`!is_entry_rollup`)
                // keeps the outer so `executeIncomingCrossChainCall` forwards
                // the call on arrival. (`DEPOSIT_SPEC.md §8`.)
                let is_entry_rollup = source_rollup_id == attribution.entry_rollup_id;
                if !is_entry_rollup {
                    builder.append_call(call, 1);
                }
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
            CallKind::NestedFailed => {
                lookup_calls.push(lookup_call_sol(call, /* failed= */ true));
            }
            CallKind::Static => {
                lookup_calls.push(lookup_call_sol(call, /* failed= */ false));
            }
        }
    }
    if let Some(last) = current_entry.take() {
        entries.push(last.finish());
    }

    Ok(EvmBatch {
        inner: ProofSystemBatchPerVerificationEntriesSol {
            entries,
            l1ToL2lookupCalls: lookup_calls,
            transientExecutionEntryCount: U256::ZERO,
            transientLookupCallCount: U256::ZERO,
            // Empty on the calldata-only path.
            proofSystems: Vec::new(),
            rollupIdsWithProofSystems: Vec::new(),
            crossProofSystemInteractions: B256::ZERO,
            // Calldata-only: no blob carriers, empty proofs[]. The
            // on-chain `_verifyProofSystemBatch` reverts loudly if a
            // caller submits without populating these.
            blobIndices: Vec::new(),
            callData: Bytes::new(),
            proofs: Vec::new(),
            // 0 = no block context; the composer sets the real L1 block
            // when posting (the simulator path doesn't bind one).
            blockNumber: 0,
        },
    })
}

/// Encode `batch` as `EEZ.postAndVerifyBatch` calldata.
///
/// Under the multi-prover ABI, proofs live inside the batch struct
/// (`batch.inner.proofs[]`). Callers populate `proofs[]` before
/// encoding — see `eez_evm_inspector::post_batch_submitter`
/// for the canonical fill+encode+submit pipeline.
#[must_use]
pub fn encode_postbatch(batch: &EvmBatch) -> Vec<u8> {
    postAndVerifyBatchCall {
        batch: batch.inner.clone(),
    }
    .abi_encode()
}

/// Encode `batch` as `CrossChainManagerL2.loadExecutionTable` calldata.
#[must_use]
pub fn encode_load_table(batch: &EvmBatch) -> Vec<u8> {
    loadExecutionTableCall {
        entries: batch.inner.entries.clone(),
        _lookupCalls: batch.inner.l1ToL2lookupCalls.clone(),
    }
    .abi_encode()
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
    /// the outer call (`outer.original_rollup_id`).
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
    fn new(
        outer: &RecordedCall<EvmProtocol>,
        _dialect: ChainDialect,
        source_rollup_id: RollupId,
        attribution: &SourceAttribution<'_>,
    ) -> Self {
        let proxy_entry_hash = cross_chain_call_hash(
            outer.original_rollup_id,
            outer.original_address,
            outer.value,
            &outer.calldata,
            outer.caller,
            outer.caller_rollup_id,
        );
        let return_data: Bytes = match &outer.outcome {
            eez_protocol::ExecutionOutcome::Resolved { return_data, .. } => {
                Bytes::from(return_data.clone())
            }
            eez_protocol::ExecutionOutcome::Pending => Bytes::new(),
        };
        let state_deltas = build_outer_state_deltas(outer, source_rollup_id, attribution);
        Self {
            proxy_entry_hash,
            return_data,
            destination_rollup_id: U256::from(outer.original_rollup_id.0),
            state_deltas,
            l2_to_l1_calls: Vec::new(),
            expected_l1_to_l2_calls: Vec::new(),
            rolling: EntryRollingHash::new(),
        }
    }

    fn append_call(&mut self, call: &RecordedCall<EvmProtocol>, call_number: u64) {
        let success = call.outcome.is_success();
        let return_data: &[u8] = call.outcome.return_data().unwrap_or(&[]);
        self.rolling.call_begin(call_number);
        self.rolling.call_end(call_number, success, return_data);
        self.l2_to_l1_calls.push(L2ToL1CallSol {
            targetAddress: call.original_address,
            value: call.value,
            data: call.calldata.clone(),
            sourceAddress: call.caller,
            sourceRollupId: U256::from(call.caller_rollup_id.0),
            revertSpan: U256::from(call.revert_span.unwrap_or(0)),
        });
    }

    fn append_nested(&mut self, call: &RecordedCall<EvmProtocol>, nested_number: u64) {
        let hash = cross_chain_call_hash(
            call.original_rollup_id,
            call.original_address,
            call.value,
            &call.calldata,
            call.caller,
            call.caller_rollup_id,
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
        let call_count = U256::from(self.l2_to_l1_calls.len() as u64);
        ExecutionEntrySol {
            stateDeltas: self.state_deltas,
            proxyEntryHash: self.proxy_entry_hash,
            destinationRollupId: self.destination_rollup_id,
            L2ToL1Calls: self.l2_to_l1_calls,
            expectedL1ToL2Calls: self.expected_l1_to_l2_calls,
            callCount: call_count,
            returnData: self.return_data,
            rollingHash: B256::from(self.rolling.current()),
        }
    }
}

/// Compute the per-entry `stateDeltas[]` for an outer cross-chain call.
///
/// Only the entry rollup's batch carries deltas — its `_applyStateDeltas`
/// updates `rollups[id].stateRoot`. Follower batches emit none
/// (`executeIncomingCrossChainCall` doesn't apply them).
///
/// `etherDelta` sign: `+value` when the entry rollup originates the call
/// (target's tracked balance grows), `-value` when the call arrives at
/// the entry rollup (caller's balance shrinks).
fn build_outer_state_deltas(
    outer: &RecordedCall<EvmProtocol>,
    source_rollup_id: RollupId,
    attribution: &SourceAttribution<'_>,
) -> Vec<StateDeltaSol> {
    if source_rollup_id != attribution.entry_rollup_id {
        return Vec::new();
    }

    let entry_rollup = attribution.entry_rollup_id;
    let (target_rollup, ether_delta_sign): (RollupId, i8) =
        if outer.caller_rollup_id == entry_rollup {
            (outer.original_rollup_id, 1)
        } else if outer.original_rollup_id == entry_rollup {
            (outer.caller_rollup_id, -1)
        } else {
            return Vec::new();
        };

    let current_state = attribution
        .initial_roots
        .get(&target_rollup)
        .copied()
        .unwrap_or([0u8; 32]);
    let new_state = attribution
        .per_tx_roots_by_rollup
        .get(&target_rollup)
        .and_then(|v| v.last().copied())
        .unwrap_or(current_state);

    let value_i256 = I256::try_from(outer.value).unwrap_or(I256::ZERO);
    let ether_delta = if ether_delta_sign >= 0 {
        value_i256
    } else {
        value_i256.wrapping_neg()
    };

    vec![StateDeltaSol {
        rollupId: U256::from(target_rollup.0),
        currentState: B256::from(current_state),
        newState: B256::from(new_state),
        etherDelta: ether_delta,
    }]
}

fn lookup_call_sol(call: &RecordedCall<EvmProtocol>, failed: bool) -> LookupCallSol {
    let hash = cross_chain_call_hash(
        call.original_rollup_id,
        call.original_address,
        call.value,
        &call.calldata,
        call.caller,
        call.caller_rollup_id,
    );
    let return_data: Bytes = call
        .outcome
        .return_data()
        .map(<[u8]>::to_vec)
        .unwrap_or_default()
        .into();
    LookupCallSol {
        crossChainCallHash: hash,
        destinationRollupId: U256::from(call.original_rollup_id.0),
        returnData: return_data,
        failed,
        callNumber: 0,
        lastNestedActionConsumed: 0,
        calls: Vec::new(),
        rollingHash: B256::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;
    use eez_protocol::ExecutionOutcome;
    use std::collections::HashMap;

    fn record(
        target: RollupId,
        caller_rollup: RollupId,
        success: bool,
    ) -> RecordedCall<EvmProtocol> {
        record_with_value(target, caller_rollup, success, U256::ZERO)
    }

    fn record_with_value(
        target: RollupId,
        caller_rollup: RollupId,
        success: bool,
        value: U256,
    ) -> RecordedCall<EvmProtocol> {
        RecordedCall {
            original_address: address!("00000000000000000000000000000000000000aa"),
            original_rollup_id: target,
            caller_rollup_id: caller_rollup,
            caller: address!("00000000000000000000000000000000000000bb"),
            calldata: Bytes::from_static(&[0x12, 0x34]),
            value,
            outcome: ExecutionOutcome::Resolved {
                return_data: Vec::new(),
                pre_state_root: [0u8; 32],
                post_state_root: [0u8; 32],
                gas_used: 21_000,
                success,
            },
            revert_span: None,
            static_meta: None,
        }
    }

    #[allow(
        clippy::type_complexity,
        reason = "test helper; the explicit tuple is local"
    )]
    fn empty_attribution() -> (
        HashMap<RollupId, [u8; 32]>,
        HashMap<RollupId, Vec<[u8; 32]>>,
    ) {
        (HashMap::new(), HashMap::new())
    }

    #[test]
    fn empty_recorded_yields_empty_batch() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let batch = build_batch(&[], &attr, &ChainDialect::EvmL1Style, RollupId(1), &[])
            .expect("build_batch ok");
        assert!(batch.entries().is_empty());
        assert!(batch.lookup_calls().is_empty());
        assert!(batch.is_empty());
    }

    #[test]
    fn originating_call_omits_outer_from_l2_to_l1_calls() {
        // L1's batch for an originating L1→L2 call (the entry consumed on
        // the source chain via `executeCrossChainCall`): the top-level call
        // is NOT folded — `L2ToL1Calls` empty, callCount=0, rollingHash=0.
        // Only reentrant L2→L1 children fold; the effect rides the
        // stateDelta and the return value (if any) is carried separately in
        // `returnData`. (sync-rollups-protocol@fe7bf66 — a folded outer here
        // makes `executeCrossChainCall` revert `RollingHashMismatch`.)
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        assert!(batch.entries()[0].L2ToL1Calls.is_empty());
        assert_eq!(batch.entries()[0].callCount, U256::ZERO);
        assert!(batch.entries()[0].expectedL1ToL2Calls.is_empty());
        assert!(batch.lookup_calls().is_empty());
        assert_ne!(batch.entries()[0].proxyEntryHash, B256::ZERO);
        assert_eq!(batch.entries()[0].rollingHash, B256::ZERO);
        assert_eq!(batch.entries()[0].destinationRollupId, U256::from(1));
        assert_eq!(batch.entries()[0].stateDeltas.len(), 1);
        assert_eq!(batch.entries()[0].stateDeltas[0].rollupId, U256::from(1));
    }

    #[test]
    fn arriving_call_yields_one_entry_on_target_batch() {
        // L2's batch for an arriving L1→L2 call opens one entry (so
        // `executeIncomingCrossChainCall` matches a `proxyEntryHash`),
        // outer in `L2ToL1Calls[0]`, no stateDeltas.
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL2Style, RollupId(1), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        assert_eq!(batch.entries()[0].L2ToL1Calls.len(), 1);
        assert_eq!(batch.entries()[0].callCount, U256::from(1));
        assert!(batch.entries()[0].expectedL1ToL2Calls.is_empty());
        assert!(batch.lookup_calls().is_empty());
        assert!(
            batch.entries()[0].stateDeltas.is_empty(),
            "follower batch carries no state deltas",
        );
        assert_eq!(batch.entries()[0].destinationRollupId, U256::from(1));
    }

    #[test]
    #[should_panic(expected = "nested cross-chain re-entry not yet supported")]
    fn nested_reentry_panics_unimplemented_on_entry_batch() {
        // L1→L2→L1 nested re-entry: unsupported by the flat-only gate.
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), true),
        ];
        let _ = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[]);
    }

    #[test]
    #[should_panic(expected = "nested cross-chain re-entry not yet supported")]
    fn nested_reentry_panics_unimplemented_even_on_failure() {
        // Same topology as above but the inner call fails. The gate
        // fires before classification, so failure mode is irrelevant.
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), false),
        ];
        let _ = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[]);
    }

    #[test]
    fn terminal_revert_yields_empty_batch() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![record(RollupId(1), RollupId(0), false)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert!(batch.is_empty());
    }

    #[test]
    fn value_bearing_outer_yields_deposit_shape_on_entry_batch() {
        // Deposit shape on L1's batch (entry rollup, value-bearing
        // outer): callCount=0, empty L2ToL1Calls, rollingHash=0, one
        // stateDelta with etherDelta=+value.
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let value = U256::from(1_000_000_000_000_000_000u128);
        let calls = vec![record_with_value(RollupId(1), RollupId(0), true, value)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        let entry = &batch.entries()[0];
        assert!(entry.L2ToL1Calls.is_empty());
        assert_eq!(entry.callCount, U256::ZERO);
        assert_eq!(entry.rollingHash, B256::ZERO);
        assert_eq!(entry.stateDeltas.len(), 1);
        assert_eq!(entry.stateDeltas[0].rollupId, U256::from(1));
        assert_eq!(
            entry.stateDeltas[0].etherDelta,
            I256::try_from(value).expect("value fits in i256"),
        );
        assert_ne!(entry.proxyEntryHash, B256::ZERO);
        assert_eq!(entry.destinationRollupId, U256::from(1));
    }

    #[test]
    fn value_bearing_outer_keeps_full_shape_on_target_batch() {
        // L2's batch for the same deposit keeps the full shape so
        // `executeIncomingCrossChainCall` forwards the value; the
        // inbound call's value must equal `outer.value` (strict on-chain
        // equality). No stateDeltas on the follower batch.
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let value = U256::from(1_000_000_000_000_000_000u128);
        let calls = vec![record_with_value(RollupId(1), RollupId(0), true, value)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL2Style, RollupId(1), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        let entry = &batch.entries()[0];
        assert_eq!(entry.L2ToL1Calls.len(), 1);
        assert_eq!(entry.L2ToL1Calls[0].value, value);
        assert_eq!(entry.callCount, U256::from(1));
        assert_ne!(entry.rollingHash, B256::ZERO);
        assert!(entry.stateDeltas.is_empty());
    }

    #[test]
    fn value_zero_setter_omits_outer_on_origin_keeps_on_target() {
        // Value-free outer (setter): the L1 origin batch (consumed via
        // `executeCrossChainCall`) OMITS the outer — callCount=0,
        // rollingHash=0 (only reentrant L2→L1 children fold). The L2
        // target batch KEEPS it so `executeIncomingCrossChainCall`
        // forwards the call on arrival. Both still share the
        // `proxyEntryHash` binding. (sync-rollups-protocol@fe7bf66 —
        // folding the outer on the origin batch reverts
        // `RollingHashMismatch`.)
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
            entry_rollup_id: RollupId(0),
        };
        let calls = vec![record_with_value(
            RollupId(1),
            RollupId(0),
            true,
            U256::ZERO,
        )];
        let l1_batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("L1 build_batch ok");
        let l2_batch = build_batch(&calls, &attr, &ChainDialect::EvmL2Style, RollupId(1), &[])
            .expect("L2 build_batch ok");
        // L1 origin batch: outer omitted, rolling hash zero.
        let l1 = &l1_batch.entries()[0];
        assert!(l1.L2ToL1Calls.is_empty(), "L1: outer omitted");
        assert_eq!(l1.callCount, U256::ZERO, "L1: callCount=0");
        assert_eq!(l1.rollingHash, B256::ZERO, "L1: rollingHash=0");
        // L2 target batch: outer kept so the call forwards on arrival.
        let l2 = &l2_batch.entries()[0];
        assert_eq!(l2.L2ToL1Calls.len(), 1, "L2: outer kept");
        assert_eq!(l2.callCount, U256::from(1), "L2: callCount=1");
        assert_ne!(l2.rollingHash, B256::ZERO, "L2: rolling hash set");
        // Shared proxyEntryHash binding across both batches.
        assert_eq!(l1.proxyEntryHash, l2.proxyEntryHash);
    }

    #[test]
    fn encode_postbatch_selector() {
        let batch = EvmBatch::empty();
        let data = encode_postbatch(&batch);
        assert_eq!(&data[..4], &postAndVerifyBatchCall::SELECTOR);
    }

    #[test]
    fn encode_load_table_selector() {
        let batch = EvmBatch::empty();
        let data = encode_load_table(&batch);
        assert_eq!(&data[..4], &loadExecutionTableCall::SELECTOR);
    }
}
