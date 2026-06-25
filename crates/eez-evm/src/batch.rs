//! `EvmBatch` — the EVM realization of `ChainProtocol::Batch`.
//!
//! Thin wrapper over the on-chain
//! [`ProofSystemBatchPerVerificationEntriesSol`] struct so the
//! [`crate::EvmProtocol`] surface mirrors the actual L1 ABI. The
//! wrapper carries no duplicated state — accessors read directly from
//! `inner` — and is populated by the entry builder and, at submit
//! time, by the proof-system carrier population layer.
//!
//! Out of `build_batch` the proof-system fields are empty:
//! `proofSystems = []`, `rollupIdsWithProofSystems = []`,
//! `crossProofSystemInteractions = 0`, `blobIndices = []`,
//! `callData = b""`, `proofs = []`. The submit path populates them
//! (`prepare_post_batch` fills the carriers, the proof sink fills
//! `proofs[]` with the prover's signature). The deferred-execution
//! table (`entries`) and lookup queue (`l1ToL2lookupCalls`) are
//! populated by [`crate::entries::build_batch`].

use alloy_primitives::{Bytes, B256, U256};

use crate::types::{
    ExecutionEntrySol, LookupCallSol, ProofSystemBatchPerVerificationEntriesSol,
};

/// EVM realization of `ChainProtocol::Batch` — a thin wrapper around
/// the on-chain `ProofSystemBatchPerVerificationEntriesSol`.
///
/// `Default` is hand-rolled because `sol!`-generated structs do not
/// derive `Default`.
#[derive(Clone)]
pub struct EvmBatch {
    /// The on-chain batch struct — the **canonical surface** for both reading
    /// and mutating an `EvmBatch` (populate `proofs[]`, attach a settlement
    /// `StateDelta`, …). Populated field-by-field by the entry builder and, at
    /// submit time, by `prepare_post_batch` (carriers) + the proof sink
    /// (`proofs[]`). The remaining
    /// `entries()` / `lookup_calls()` / `transient_*_count()` methods are
    /// convenience read views over it.
    pub inner: ProofSystemBatchPerVerificationEntriesSol,
}

impl Default for EvmBatch {
    fn default() -> Self {
        Self {
            inner: ProofSystemBatchPerVerificationEntriesSol {
                entries: Vec::new(),
                l1ToL2lookupCalls: Vec::new(),
                transientExecutionEntryCount: U256::ZERO,
                transientLookupCallCount: U256::ZERO,
                proofSystems: Vec::new(),
                rollupIdsWithProofSystems: Vec::new(),
                crossProofSystemInteractions: B256::ZERO,
                blobIndices: Vec::new(),
                callData: Bytes::new(),
                proofs: Vec::new(),
                blockNumber: 0, // builder placeholder; prepare_post_batch binds the real N (0 = refused sentinel)
            },
        }
    }
}

// Manual `Debug` because the `sol!`-generated inner struct doesn't
// derive `Debug` (its dynamic-bytes fields are awkward to format).
impl std::fmt::Debug for EvmBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmBatch")
            .field("entries", &self.inner.entries.len())
            .field("l1ToL2lookupCalls", &self.inner.l1ToL2lookupCalls.len())
            .field(
                "transientExecutionEntryCount",
                &self.inner.transientExecutionEntryCount,
            )
            .field(
                "transientLookupCallCount",
                &self.inner.transientLookupCallCount,
            )
            .field("proofSystems", &self.inner.proofSystems.len())
            .field(
                "rollupIdsWithProofSystems",
                &self.inner.rollupIdsWithProofSystems.len(),
            )
            .field(
                "crossProofSystemInteractions",
                &self.inner.crossProofSystemInteractions,
            )
            .field("blobIndices", &self.inner.blobIndices.len())
            .field("callData", &self.inner.callData.len())
            .field("proofs", &self.inner.proofs.len())
            .finish()
    }
}

impl EvmBatch {
    /// Construct an empty batch — zero entries, zero lookup calls,
    /// zero transient counts, empty proof-system carriers.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// `true` if the batch carries no entries and no lookup calls.
    /// Used by the composer's terminal-revert short-circuit to skip
    /// CCM-verify and target-composition emission for a batch that
    /// was fully reverted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty() && self.inner.l1ToL2lookupCalls.is_empty()
    }

    /// Fold `other`'s entries + lookup calls onto this batch, summing the
    /// transient prefix counts. Used to MERGE the per-tx outbound batches
    /// of one Sync slot into a single multi-entry `postAndVerifyBatch`
    /// (Position B: one entry per L2 tx, drained in slot order, each
    /// chained `R_{k-1} -> R_k`). The on-chain transient drain consumes
    /// the concatenated entries in order (verified:
    /// `contracts/smoke/test/ChainedDeltaOracle.t.sol`).
    ///
    /// Proof-system carriers are NOT merged — they are filled later by
    /// `prepare_post_batch` over the merged whole. `self` should be the
    /// EARLIER tx's batch (slot order is load-bearing for the chain).
    pub fn merge(&mut self, mut other: EvmBatch) {
        let entries_added = U256::from(other.inner.entries.len());
        let lookups_added = U256::from(other.inner.l1ToL2lookupCalls.len());
        self.inner.entries.append(&mut other.inner.entries);
        self.inner
            .l1ToL2lookupCalls
            .append(&mut other.inner.l1ToL2lookupCalls);
        // Both per-tx batches are all-immediate (build_l1_postbatch), so the
        // transient prefix grows by the appended counts.
        self.inner.transientExecutionEntryCount += entries_added;
        self.inner.transientLookupCallCount += lookups_added;
    }

    // ── Accessors (read-only views into the inner struct) ──────────

    /// Execution entries — the deferred-execution table.
    #[must_use]
    pub fn entries(&self) -> &[ExecutionEntrySol] {
        &self.inner.entries
    }

    /// Lookup calls — content-addressed cached results.
    #[must_use]
    pub fn lookup_calls(&self) -> &[LookupCallSol] {
        &self.inner.l1ToL2lookupCalls
    }

    /// Transient-prefix count for `entries[]`.
    #[must_use]
    pub fn transient_execution_entry_count(&self) -> U256 {
        self.inner.transientExecutionEntryCount
    }

    /// The encoded postBatch callData (L2 block bytes) — consumed by the
    /// inspector's per-proof-system public-inputs hashing.
    #[must_use]
    pub fn call_data(&self) -> &alloy_primitives::Bytes {
        &self.inner.callData
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(rid: u64) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(rid),
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::from(1u8),
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    fn one_entry_batch(rid: u64) -> EvmBatch {
        let mut b = EvmBatch::empty();
        b.inner.entries.push(entry(rid));
        b.inner.transientExecutionEntryCount = U256::from(1u8);
        b
    }

    #[test]
    fn merge_concats_entries_in_order_and_sums_transient_counts() {
        let mut a = one_entry_batch(1);
        let b = one_entry_batch(2);
        a.merge(b);
        // Entries concatenated in slot order (a's tx first, then b's).
        assert_eq!(a.inner.entries.len(), 2);
        assert_eq!(a.inner.entries[0].destinationRollupId, U256::from(1u64));
        assert_eq!(a.inner.entries[1].destinationRollupId, U256::from(2u64));
        // Transient prefix grows by the appended count (both all-immediate).
        assert_eq!(a.inner.transientExecutionEntryCount, U256::from(2u8));
    }

    #[test]
    fn merge_onto_empty_is_identity() {
        let mut acc = EvmBatch::empty();
        acc.merge(one_entry_batch(7));
        assert_eq!(acc.inner.entries.len(), 1);
        assert_eq!(acc.inner.transientExecutionEntryCount, U256::from(1u8));
    }
}
