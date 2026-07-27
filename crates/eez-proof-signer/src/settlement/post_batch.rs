//! Canonical post-batch decoding, public-input admission, and hash computation.

use std::num::NonZeroU64;

use alloy_primitives::{Address, B256, U256};
use eez_evm::EvmBatch;
use eez_evm::entries::decode_postbatch;
use eez_evm::public_inputs::public_inputs_hashes;
use eez_evm::types::ProofSystemBatchPerVerificationEntriesSol;
use thiserror::Error;

use crate::attest::NonZeroProofSystemVkey;

#[derive(Debug, Error)]
pub(crate) enum PostBatchDecodeError {
    #[error("invalid postAndVerifyBatch calldata: {reason}")]
    InvalidAbi { reason: String },
    #[error("postAndVerifyBatch calldata is not its canonical complete encoding")]
    NonCanonical,
}

/// Canonically decoded Composer input.
///
/// This type guarantees only a complete, canonical ABI encoding. Its state,
/// effect, rollup, DA, and public-input claims remain unverified.
#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct CanonicalPostBatch(EvmBatch);

impl CanonicalPostBatch {
    pub(super) const fn as_batch(&self) -> &EvmBatch {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn from_decoded_for_test(batch: EvmBatch) -> Self {
        Self(batch)
    }
}

#[cfg(test)]
impl std::ops::Deref for CanonicalPostBatch {
    type Target = EvmBatch;

    fn deref(&self) -> &Self::Target {
        self.as_batch()
    }
}

#[cfg(test)]
impl std::ops::DerefMut for CanonicalPostBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Decode a complete `postAndVerifyBatch` call and reject non-canonical encodings.
pub(crate) fn decode_canonical_post_batch(
    post_batch_calldata: Vec<u8>,
) -> Result<CanonicalPostBatch, PostBatchDecodeError> {
    let batch = decode_postbatch(&post_batch_calldata).map_err(|error| {
        PostBatchDecodeError::InvalidAbi {
            reason: error.to_string(),
        }
    })?;
    // Tokenize the decoded struct by reference. The shared convenience
    // encoder clones the complete batch, which needlessly amplifies memory for
    // adversarially large calldata. `decode_postbatch` already checked the
    // selector, so canonicality only needs an exact argument-byte comparison.
    let batch_token =
        <ProofSystemBatchPerVerificationEntriesSol as alloy_sol_types::SolType>::tokenize(
            &batch.inner,
        );
    let canonical_arguments = alloy_sol_types::abi::encode_sequence(&(batch_token,));
    if post_batch_calldata.get(4..) != Some(canonical_arguments.as_slice()) {
        return Err(PostBatchDecodeError::NonCanonical);
    }
    Ok(CanonicalPostBatch(batch))
}

#[derive(Debug, Error)]
pub(crate) enum PublicInputError {
    #[error("invalid public-input structure: {0}")]
    InvalidStructure(String),
    #[error("public-input hash computation failed: {0}")]
    Computation(String),
    #[error("expected one public-input hash, got {actual}")]
    HashCount { actual: usize },
}

/// A canonical batch whose configured public-input profile has been checked.
///
/// Constructing this value validates the rollup and proof-system bindings plus
/// the fields pinned by the active profile. Hashing remains separate so the
/// service can reject cheaper settlement failures first.
pub(crate) struct CheckedPublicInputProfile<'a> {
    canonical_batch: &'a CanonicalPostBatch,
}

impl<'a> CheckedPublicInputProfile<'a> {
    /// Validate the expected rollup and single-proof-system profile.
    pub(crate) fn new(
        canonical_batch: &'a CanonicalPostBatch,
        expected_rollup_id: NonZeroU64,
        expected_proof_system: Address,
    ) -> Result<Self, PublicInputError> {
        validate_public_input_structure(
            canonical_batch.as_batch(),
            expected_rollup_id,
            expected_proof_system,
        )?;
        Ok(Self { canonical_batch })
    }

    /// Compute the sole canonical hash from this profile-checked batch.
    ///
    /// This type does not prove that state, effects, or DA passed their gates;
    /// the complete settlement job grants that stronger attestation boundary.
    pub(crate) fn recompute_public_inputs_hash(
        &self,
        proof_system_vkey: NonZeroProofSystemVkey,
    ) -> Result<RecomputedPublicInputsHash, PublicInputError> {
        let hashes = public_inputs_hashes(
            self.canonical_batch.as_batch(),
            proof_system_vkey.get(),
            None,
        )
        .map_err(|error| PublicInputError::Computation(error.to_string()))?;
        let [hash] = hashes.as_slice() else {
            return Err(PublicInputError::HashCount {
                actual: hashes.len(),
            });
        };
        Ok(RecomputedPublicInputsHash(*hash))
    }
}

/// Public-input hash recomputed locally from a checked canonical batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecomputedPublicInputsHash(B256);

impl RecomputedPublicInputsHash {
    pub(crate) const fn into_inner(self) -> B256 {
        self.0
    }
}

/// Validate and recompute a hash in one call for callers that do not run the
/// settlement gates separately. The composer-supplied hash is never consulted.
#[cfg(test)]
pub(crate) fn recompute_public_input_hash(
    batch: &EvmBatch,
    proof_system_vkey: NonZeroProofSystemVkey,
    expected_rollup_id: NonZeroU64,
    expected_proof_system: Address,
) -> Result<B256, PublicInputError> {
    let canonical = CanonicalPostBatch(batch.clone());
    CheckedPublicInputProfile::new(&canonical, expected_rollup_id, expected_proof_system)?
        .recompute_public_inputs_hash(proof_system_vkey)
        .map(RecomputedPublicInputsHash::into_inner)
}

/// Validate every signer-specific structural rule before calling
/// `public_inputs_hashes`, including the one-to-one binding between the
/// expected rollup and the sole expected proof system.
fn validate_public_input_structure(
    batch: &EvmBatch,
    expected_rollup_id: NonZeroU64,
    expected_proof_system: Address,
) -> Result<(), PublicInputError> {
    let inner = &batch.inner;
    let [proof_system] = inner.proofSystems.as_slice() else {
        return Err(invalid_structure(format!(
            "expected exactly one proof system, got {}",
            inner.proofSystems.len()
        )));
    };
    if *proof_system != expected_proof_system {
        return Err(invalid_structure(format!(
            "proof system address {proof_system} does not match expected proof system {expected_proof_system}"
        )));
    }
    let [rollup_assignment] = inner.rollupIdsWithProofSystems.as_slice() else {
        return Err(invalid_structure(format!(
            "expected exactly one rollup assignment, got {}",
            inner.rollupIdsWithProofSystems.len()
        )));
    };
    let expected_rollup = U256::from(expected_rollup_id.get());
    if rollup_assignment.rollupId != expected_rollup {
        return Err(invalid_structure(format!(
            "rollup assignment id {} does not match expected rollup id {expected_rollup_id}",
            rollup_assignment.rollupId
        )));
    }
    if rollup_assignment.proofSystemIndex.as_slice() != [0] {
        return Err(invalid_structure(
            "rollup assignment proof-system indices must be exactly [0]",
        ));
    }
    if inner.crossProofSystemInteractions != B256::ZERO {
        return Err(invalid_structure(
            "crossProofSystemInteractions must be zero in the single-proof-system profile",
        ));
    }

    let is_expected_rollup = |rollup_id: &U256| *rollup_id == expected_rollup;
    for (entry_index, entry) in inner.entries.iter().enumerate() {
        if !is_expected_rollup(&entry.destinationRollupId) {
            return Err(invalid_structure(format!(
                "entry {entry_index} destination rollup {} does not match expected rollup id {expected_rollup_id}",
                entry.destinationRollupId,
            )));
        }
    }
    for (lookup_index, lookup) in inner.l1ToL2lookupCalls.iter().enumerate() {
        if !is_expected_rollup(&lookup.destinationRollupId) {
            return Err(invalid_structure(format!(
                "lookup {lookup_index} destination rollup {} does not match expected rollup id {expected_rollup_id}",
                lookup.destinationRollupId,
            )));
        }
    }

    if inner.blockNumber != 0 {
        return Err(invalid_structure(format!(
            "blockNumber must be zero, got {}",
            inner.blockNumber
        )));
    }
    if !inner.blobIndices.is_empty() {
        return Err(invalid_structure(format!(
            "blobIndices must be empty, got {}",
            inner.blobIndices.len()
        )));
    }
    // No pin for the remaining fields: transient counts (L1 scheduling
    // inputs) and `proofs` are outside the signed public-input preimage, and
    // `callData` is bound by both the DA gate and the recomputed hash.
    Ok(())
}

fn invalid_structure(reason: impl Into<String>) -> PublicInputError {
    PublicInputError::InvalidStructure(reason.into())
}
