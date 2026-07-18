//! `EvmBatch` — the table-loading batch.
//!
//! Thin wrapper over the on-chain
//! [`ProofSystemBatchPerVerificationEntriesSol`] struct so the
//! batch surface mirrors the actual L1 ABI. The
//! wrapper carries no duplicated state
//! and is populated by the entry builder and, at submit
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

use alloy_primitives::{B256, Bytes, U256};

use crate::abi::ProofSystemBatchPerVerificationEntriesSol;

/// The table-loading batch — a thin wrapper around
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
    /// (`proofs[]`).
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
    /// target-composition emission for a batch that
    /// was fully reverted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.entries.is_empty() && self.inner.l1ToL2lookupCalls.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::ExecutionEntrySol;

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
    fn empty_batch_is_empty() {
        assert!(EvmBatch::empty().is_empty());
        assert!(!one_entry_batch(7).is_empty());
    }
}
