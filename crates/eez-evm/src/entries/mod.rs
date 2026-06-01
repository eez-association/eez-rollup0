//! Unified flat-emitter entry building for the multi-prover protocol.
//!
//! [`build_batch`] walks the preorder `recorded[..]` slice once,
//! classifies each call (top-level / nested-success / nested-failed /
//! lookup), folds per-entry rolling hashes, and emits an [`EvmBatch`]
//! carrying the deferred-execution table + lookup queue + transient-
//! prefix metadata that
//! `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch` /
//! `CrossChainManagerL2.loadExecutionTable` consume. Per-fixture
//! byte-identity against upstream Foundry goldens is verified
//! end-to-end in `scripts/protocol-e2e.sh`.
//!
//! For Phase 08 the proof-system fields on the produced batch stay
//! empty (`proofSystems = []`, `proofs = []`, …) — Phase 09 wires
//! them from the prover registry. See `docs/plans/08-protocol-
//! alignment.md` §D for the staging.

use alloy_primitives::{Bytes, B256, U256};
use alloy_sol_types::SolCall;
use crosschain_protocol::{rolling_hash::EntryRollingHash, ProtocolResult, RecordedCall, RollupId};

use crate::action::cross_chain_call_hash;
use crate::batch::EvmBatch;
use crate::dialect::ChainDialect;
use crate::types::{
    loadExecutionTableCall, postVerifyAndExecuteOrSaveExecutionsFromBatchCall, ExecutionEntrySol,
    ExpectedL1ToL2CallSol, L2ToL1CallSol, LookupCallSol, ProofSystemBatchPerVerificationEntriesSol,
    StateDeltaSol,
};
use crate::EvmProtocol;

/// Classification of a single [`RecordedCall`] within an entry's
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
        if call.caller_rollup_id == source_id {
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
/// `attribution` is plumbed for `StateDelta.currentState` chaining
/// (Phase 09 fully wires per-fixture); `raw_tx` is plumbed for
/// L1-style `executeL2TX` raw-tx routing wired per fixture in E2E.
/// `source_rollup_id` is the rollup THIS batch targets.
///
/// # Errors
///
/// Returns [`ProtocolErrorKind::InvalidEncoding`] if a call's
/// outcome is `Pending` (composition lifecycle bug — every call
/// should be resolved before entering finalize).
pub fn build_batch(
    recorded: &[RecordedCall<EvmProtocol>],
    attribution: &crosschain_protocol::SourceAttribution<'_>,
    dialect: &ChainDialect,
    source_rollup_id: RollupId,
    raw_tx: &[u8],
) -> ProtocolResult<EvmBatch> {
    let _ = (attribution, raw_tx); // wired per fixture in the E2E suite

    let group: Vec<&RecordedCall<EvmProtocol>> = recorded
        .iter()
        .filter(|c| {
            c.caller_rollup_id == source_rollup_id || c.original_rollup_id == source_rollup_id
        })
        .collect();

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
            // Phase 09 wires these four from the prover registry.
            proofSystems: Vec::new(),
            rollupIdsWithProofSystems: Vec::new(),
            crossProofSystemInteractions: B256::ZERO,
            // Phase 09 wires blob carriers; D1 ships calldata-only +
            // empty proofs[]. Composer surfaces the on-chain
            // `_verifyProofSystemBatch` revert if a caller forgets
            // to populate these before submission.
            blobIndices: Vec::new(),
            callData: Bytes::new(),
            proofs: Vec::new(),
        },
    })
}

/// Encode `batch` as
/// `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch` calldata.
///
/// Under the multi-prover ABI, proofs live inside the batch struct
/// (`batch.inner.proofs[]`). Callers populate `proofs[]` before
/// encoding — see `crosschain_evm_composer::post_batch_submitter`
/// for the canonical fill+encode+submit pipeline.
#[must_use]
pub fn encode_postbatch(batch: &EvmBatch) -> Vec<u8> {
    postVerifyAndExecuteOrSaveExecutionsFromBatchCall {
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
    fn new(outer: &RecordedCall<EvmProtocol>, _dialect: ChainDialect) -> Self {
        let proxy_entry_hash = cross_chain_call_hash(
            outer.original_rollup_id,
            outer.original_address,
            outer.value,
            &outer.calldata,
            outer.caller,
            outer.caller_rollup_id,
        );
        let return_data: Bytes = match &outer.outcome {
            crosschain_protocol::ExecutionOutcome::Resolved { return_data, .. } => {
                Bytes::from(return_data.clone())
            }
            crosschain_protocol::ExecutionOutcome::Pending => Bytes::new(),
        };
        Self {
            proxy_entry_hash,
            return_data,
            destination_rollup_id: U256::from(outer.original_rollup_id.0),
            state_deltas: Vec::new(),
            l2_to_l1_calls: Vec::new(),
            expected_l1_to_l2_calls: Vec::new(),
            rolling: EntryRollingHash::new(),
        }
    }

    #[allow(dead_code, reason = "kept for Phase 09 entry-builder reuse")]
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
        ExecutionEntrySol {
            stateDeltas: self.state_deltas,
            proxyEntryHash: self.proxy_entry_hash,
            destinationRollupId: self.destination_rollup_id,
            L2ToL1Calls: self.l2_to_l1_calls,
            expectedL1ToL2Calls: self.expected_l1_to_l2_calls,
            callCount: U256::ZERO,
            returnData: self.return_data,
            rollingHash: B256::from(self.rolling.current()),
        }
    }
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
    use crosschain_protocol::{ExecutionOutcome, SourceAttribution};
    use std::collections::HashMap;

    fn record(
        target: RollupId,
        caller_rollup: RollupId,
        success: bool,
    ) -> RecordedCall<EvmProtocol> {
        RecordedCall {
            original_address: address!("00000000000000000000000000000000000000aa"),
            original_rollup_id: target,
            caller_rollup_id: caller_rollup,
            caller: address!("00000000000000000000000000000000000000bb"),
            calldata: Bytes::from_static(&[0x12, 0x34]),
            value: U256::ZERO,
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
        };
        let batch = build_batch(&[], &attr, &ChainDialect::EvmL1Style, RollupId(1), &[])
            .expect("build_batch ok");
        assert!(batch.entries().is_empty());
        assert!(batch.lookup_calls().is_empty());
        assert!(batch.is_empty());
    }

    #[test]
    fn single_top_level_call_yields_one_entry() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
        };
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        // The TopLevel call is described by the entry's
        // `proxyEntryHash` + `returnData`; reentrant children land
        // in `L2ToL1Calls`. No children here, so both arrays empty.
        assert!(batch.entries()[0].L2ToL1Calls.is_empty());
        assert!(batch.entries()[0].expectedL1ToL2Calls.is_empty());
        assert!(batch.lookup_calls().is_empty());
        assert_ne!(batch.entries()[0].proxyEntryHash, B256::ZERO);
        assert_eq!(batch.entries()[0].rollingHash, B256::ZERO);
        assert_eq!(
            batch.entries()[0].destinationRollupId,
            U256::from(1),
            "destinationRollupId is the target chain of the outer call",
        );
    }

    #[test]
    fn outgoing_to_other_rollup_yields_empty_batch_for_target() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
        };
        let calls = vec![record(RollupId(1), RollupId(0), true)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL2Style, RollupId(1), &[])
            .expect("build_batch ok");
        assert!(batch.entries().is_empty());
        assert!(batch.lookup_calls().is_empty());
    }

    #[test]
    fn nested_reentrant_call_lands_in_expected_l1_to_l2_calls() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
        };
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), true),
        ];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        assert_eq!(
            batch.entries()[0].expectedL1ToL2Calls.len(),
            1,
            "nested-success child must land in expectedL1ToL2Calls",
        );
        assert!(batch.lookup_calls().is_empty());
    }

    #[test]
    fn nested_failure_routes_to_lookup_calls() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
        };
        let calls = vec![
            record(RollupId(1), RollupId(0), true),
            record(RollupId(0), RollupId(1), false),
        ];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert_eq!(batch.entries().len(), 1);
        assert!(batch.entries()[0].expectedL1ToL2Calls.is_empty());
        assert_eq!(batch.lookup_calls().len(), 1);
        assert!(batch.lookup_calls()[0].failed);
    }

    #[test]
    fn terminal_revert_yields_empty_batch() {
        let (init, ptx) = empty_attribution();
        let attr = SourceAttribution {
            initial_roots: &init,
            per_tx_roots_by_rollup: &ptx,
        };
        let calls = vec![record(RollupId(1), RollupId(0), false)];
        let batch = build_batch(&calls, &attr, &ChainDialect::EvmL1Style, RollupId(0), &[])
            .expect("build_batch ok");
        assert!(batch.is_empty());
    }

    #[test]
    fn encode_postbatch_selector() {
        let batch = EvmBatch::empty();
        let data = encode_postbatch(&batch);
        assert_eq!(
            &data[..4],
            &postVerifyAndExecuteOrSaveExecutionsFromBatchCall::SELECTOR
        );
    }

    #[test]
    fn encode_load_table_selector() {
        let batch = EvmBatch::empty();
        let data = encode_load_table(&batch);
        assert_eq!(&data[..4], &loadExecutionTableCall::SELECTOR);
    }
}
