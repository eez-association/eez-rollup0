//! Shared EVM batch representation.
//!
//! [`EvmBatch`] aliases the pinned
//! [`ProofSystemBatchPerVerificationEntriesSol`] ABI type. Composition builds
//! partial batches containing source-side or target-side entries. Downstream
//! settlement may merge them and attach state updates, proof-system metadata,
//! and proofs.

use crate::abi::ProofSystemBatchPerVerificationEntriesSol;

/// Alias for the batch accepted by the protocol's `postAndVerifyBatch` ABI.
pub type EvmBatch = ProofSystemBatchPerVerificationEntriesSol;

impl ProofSystemBatchPerVerificationEntriesSol {
    /// `true` if the batch carries no mutable or static entries.
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
