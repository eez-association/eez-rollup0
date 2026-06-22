//! `EvmBatch` — the EVM realization of `ChainProtocol::Batch`.
//!
//! Thin wrapper over the on-chain
//! [`ProofSystemBatchPerVerificationEntriesSol`] struct so the
//! [`crate::EvmProtocol`] surface mirrors the actual L1 ABI. The
//! wrapper carries no duplicated state — accessors read directly from
//! `inner` — and is what the Phase 09 prover wiring will populate.
//!
//! For Phase 08 fixtures the proof-system fields stay empty:
//! `proofSystems = []`, `rollupIdsWithProofSystems = []`,
//! `crossProofSystemInteractions = 0`, `blobIndices = []`,
//! `callData = b""`, `proofs = []`. The deferred-execution table
//! (`entries`) and lookup queue (`l1ToL2lookupCalls`) are populated
//! by [`crate::entries::build_batch`]. Phase 09 populates the
//! proof-system fields from the prover registry.

use alloy_primitives::{Address, B256, Bytes, U256};

use crate::types::{
    ExecutionEntrySol, LookupCallSol, ProofSystemBatchPerVerificationEntriesSol,
    RollupIdWithProofSystemsSol,
};

/// EVM realization of `ChainProtocol::Batch` — a thin wrapper around
/// the on-chain `ProofSystemBatchPerVerificationEntriesSol`.
///
/// `Default` is hand-rolled because `sol!`-generated structs do not
/// derive `Default`.
#[derive(Clone)]
pub struct EvmBatch {
    /// The on-chain batch struct, populated field-by-field by the
    /// entry builder + (Phase 09) prover wiring.
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
                blockNumber: 0,
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

    // ── Accessors (read-only views into the inner struct) ──────────

    #[must_use]
    pub fn entries(&self) -> &[ExecutionEntrySol] {
        &self.inner.entries
    }

    #[must_use]
    pub fn lookup_calls(&self) -> &[LookupCallSol] {
        &self.inner.l1ToL2lookupCalls
    }

    #[must_use]
    pub fn transient_execution_entry_count(&self) -> U256 {
        self.inner.transientExecutionEntryCount
    }

    #[must_use]
    pub fn transient_lookup_call_count(&self) -> U256 {
        self.inner.transientLookupCallCount
    }

    #[must_use]
    pub fn proof_systems(&self) -> &[Address] {
        &self.inner.proofSystems
    }

    #[must_use]
    pub fn rollup_ids_with_proof_systems(&self) -> &[RollupIdWithProofSystemsSol] {
        &self.inner.rollupIdsWithProofSystems
    }

    #[must_use]
    pub fn cross_proof_system_interactions(&self) -> B256 {
        self.inner.crossProofSystemInteractions
    }

    #[must_use]
    pub fn blob_indices(&self) -> &[U256] {
        &self.inner.blobIndices
    }

    #[must_use]
    pub fn call_data(&self) -> &Bytes {
        &self.inner.callData
    }

    #[must_use]
    pub fn proofs(&self) -> &[Bytes] {
        &self.inner.proofs
    }
}
