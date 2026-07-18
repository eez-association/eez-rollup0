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

use crate::abi::ProofSystemBatchPerVerificationEntriesSol;

/// The table-loading batch — the on-chain
/// `ProofSystemBatchPerVerificationEntriesSol`, aliased for brevity.
/// Populated field-by-field by the entry builder and, at submit time,
/// by `prepare_post_batch` (carriers) + the proof sink (`proofs[]`).
pub type EvmBatch = ProofSystemBatchPerVerificationEntriesSol;

impl ProofSystemBatchPerVerificationEntriesSol {
    /// `true` if the batch carries no entries and no lookup calls.
    /// Used by the composer's terminal-revert short-circuit to skip
    /// target-composition emission for a batch that was fully
    /// reverted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.l1ToL2lookupCalls.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::ExecutionEntrySol;
    use alloy_primitives::U256;

    fn one_entry_batch(rid: u64) -> EvmBatch {
        let mut b = EvmBatch::default();
        b.entries.push(ExecutionEntrySol {
            destinationRollupId: U256::from(rid),
            callCount: U256::from(1u8),
            ..Default::default()
        });
        b.transientExecutionEntryCount = U256::from(1u8);
        b
    }

    #[test]
    fn empty_batch_is_empty() {
        assert!(EvmBatch::default().is_empty());
        assert!(!one_entry_batch(7).is_empty());
    }
}
