//! `EvmBatch` — the L1 protocol batch.
//!
//! Thin wrapper over the on-chain
//! [`ProofSystemBatchPerVerificationEntriesSol`] struct so the
//! batch surface mirrors the actual L1 ABI. The
//! wrapper carries no duplicated state
//! and is populated by the entry builder and, at submit
//! time, by the proof-system carrier population layer.
//!
//! Entry construction populates the mutable and static execution tables;
//! submission fills the proof-system carriers and proofs.

use crate::abi::ProofSystemBatchPerVerificationEntriesSol;

/// The table-loading batch — the on-chain
/// `ProofSystemBatchPerVerificationEntriesSol`, aliased for brevity.
/// Populated field-by-field by the entry builder and, at submit time,
/// by `prepare_post_batch` (carriers) + the proof sink (`proofs[]`).
pub type EvmBatch = ProofSystemBatchPerVerificationEntriesSol;

impl ProofSystemBatchPerVerificationEntriesSol {
    /// `true` if the batch carries no mutable or static entries.
    /// Used by the composer's terminal-revert short-circuit to skip
    /// target-composition emission for a batch that was fully
    /// reverted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.staticEntries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{ExecutionEntrySol, StaticExecutionEntrySol};

    fn one_entry_batch(rid: u64) -> EvmBatch {
        let mut b = EvmBatch::default();
        b.entries.push(ExecutionEntrySol {
            destinationRollupId: rid,
            ..Default::default()
        });
        b
    }

    #[test]
    fn empty_batch_is_empty() {
        assert!(EvmBatch::default().is_empty());
        assert!(!one_entry_batch(7).is_empty());

        let batch = EvmBatch {
            staticEntries: vec![StaticExecutionEntrySol::default()],
            ..Default::default()
        };
        assert!(!batch.is_empty());
    }
}
