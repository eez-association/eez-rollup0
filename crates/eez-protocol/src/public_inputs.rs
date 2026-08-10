//! Exact Rust mirror of `EEZ._verifyProofSystemBatch` at the pinned protocol
//! revision. Dynamic arrays are encoded independently and concatenated using
//! the contract's outer `abi.encodePacked` layout.

use crate::{ProofPlan, ProofPlanInvariantError, RollupId, RollupProofAssignment};
use alloy_primitives::{Address, B256, Bytes, keccak256};
use alloy_sol_types::{SolValue, sol};

use tracing::debug;

use crate::abi::{EvmBatch, ExecutionEntrySol, StaticExecutionEntrySol};

// ── Per-element atomic hashes ─────────────────────────────────────

/// `keccak256(abi.encode(entry))` for a mutable execution entry.
#[must_use]
pub fn entry_hash(entry: &ExecutionEntrySol) -> B256 {
    keccak256(SolValue::abi_encode(entry))
}

/// `keccak256(abi.encode(entry))` for a static execution entry.
#[must_use]
pub fn static_entry_hash(entry: &StaticExecutionEntrySol) -> B256 {
    keccak256(SolValue::abi_encode(entry))
}

// ── Shared public input ───────────────────────────────────────────

/// Shared portion of every proof system's public-input hash.
///
/// The sender is appended as a packed Solidity `address` (20 bytes), while
/// every hash array is first wrapped with its own `abi.encode`.
#[must_use]
pub fn shared_public_input(
    entry_hashes: &[B256],
    static_entry_hashes: &[B256],
    blob_hashes: &[B256],
    call_data: &Bytes,
    custom_data_hashes: &[B256],
    bound_sender: Address,
) -> B256 {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&entry_hashes.to_vec().abi_encode());
    buf.extend_from_slice(&static_entry_hashes.to_vec().abi_encode());
    buf.extend_from_slice(&blob_hashes.to_vec().abi_encode());
    buf.extend_from_slice(keccak256(call_data).as_slice());
    buf.extend_from_slice(&custom_data_hashes.to_vec().abi_encode());
    buf.extend_from_slice(bound_sender.as_slice());
    keccak256(&buf)
}

// ── Per-PS accumulator ────────────────────────────────────────────

sol! {
    struct CustomDataHashInput {
        uint64 rollupId;
        bytes customData;
    }

    struct PerPsFoldStep {
        bytes32 acc;
        uint64 rollupId;
        bytes32 vkey;
    }
}

fn custom_data_hash(rollup_id: RollupId, custom_data: &Bytes) -> B256 {
    let input = CustomDataHashInput {
        rollupId: rollup_id.0,
        customData: custom_data.clone(),
    };
    keccak256(CustomDataHashInput::abi_encode_params(&input))
}

/// Find the position of `k` in a strictly-increasing
/// `Vec<u64>`. Returns `None` if absent.
///
/// Mirrors `EEZ.sol::_findIndexPosition` (binary search safe
/// thanks to the strict-increasing invariant enforced by
/// `_validateStructure`).
fn position_of(k: u64, indices: &[u64]) -> Option<usize> {
    indices.binary_search(&k).ok()
}

/// Fold the rollups assigned to one proof system, then bind the shared hash.
#[must_use]
pub fn per_ps_public_inputs_hash(shared: B256, plan: &ProofPlan, ps_index_in_global: u64) -> B256 {
    let mut acc = B256::ZERO;
    for (r, assignment) in plan.rollup_assignments.iter().enumerate() {
        let Some(j) = position_of(ps_index_in_global, &assignment.proof_system_indexes) else {
            continue;
        };
        let step = PerPsFoldStep {
            acc,
            rollupId: assignment.rollup_id.0,
            vkey: B256::from(plan.vk_matrix[r][j]),
        };
        acc = keccak256(PerPsFoldStep::abi_encode_params(&step));
    }
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(shared.as_slice());
    buf[32..].copy_from_slice(acc.as_slice());
    keccak256(buf)
}

// ── Top-level helper ──────────────────────────────────────────────

/// Convenience: compute every per-PS public-inputs hash for a
/// batch.
///
/// Returns `vec[k] = publicInputsHash[k]` parallel to
/// `plan.proof_systems` (same length, same ordering). The
/// submitter consumes this directly and feeds each hash to the
/// corresponding signer.
///
/// # Errors
///
/// Returns [`ProofPlanInvariantError`] if `plan` is malformed.
/// Callers SHOULD have run `plan.check_invariants()` already;
/// this is belt-and-suspenders.
pub fn all_per_ps_hashes(
    plan: &ProofPlan,
    entry_hashes: &[B256],
    static_entry_hashes: &[B256],
    blob_hashes: &[B256],
    call_data: &Bytes,
    bound_sender: Address,
) -> Result<Vec<B256>, ProofPlanInvariantError> {
    plan.check_invariants()?;
    let custom_data_hashes: Vec<_> = plan
        .rollup_assignments
        .iter()
        .zip(&plan.custom_data)
        .map(|(assignment, data)| custom_data_hash(assignment.rollup_id, data))
        .collect();
    let shared = shared_public_input(
        entry_hashes,
        static_entry_hashes,
        blob_hashes,
        call_data,
        &custom_data_hashes,
        bound_sender,
    );
    let out: Vec<B256> = (0..plan.proof_systems.len() as u64)
        .map(|k| per_ps_public_inputs_hash(shared, plan, k))
        .collect();
    // This is the ONE helper both composer and prover call, so logging here
    // gives the publicInputsHash an audit trail on BOTH sides — an on-chain
    // `InvalidProof`/hash-mismatch is then diagnosable from the logs.
    debug!(
        target: "eez::public_inputs",
        shared = %shared,
        entry_hashes = entry_hashes.len(),
        static_entry_hashes = static_entry_hashes.len(),
        proof_systems = plan.proof_systems.len(),
        first_hash = ?out.first(),
        "all_per_ps_hashes: computed publicInputsHash(es)",
    );
    Ok(out)
}

/// Errors produced while reconstructing public inputs from the initial
/// supported batch profile.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicInputsError {
    /// The proof plan reconstructed from the batch is malformed.
    #[error(transparent)]
    InvalidPlan(#[from] ProofPlanInvariantError),
    /// Manager-defined custom data for nonzero block numbers is not wired yet.
    #[error("public-input hashing currently requires blockNumber == 0, got {block_number}")]
    UnsupportedBlockNumber {
        /// Unsupported `batch.blockNumber` value.
        block_number: u64,
    },
    /// Sender-bound public inputs require the actual transaction sender.
    #[error("public-input hashing currently requires bindMsgSenderInPublicInput == false")]
    UnsupportedSenderBinding,
    /// Blob hashes cannot be reconstructed from blob indices alone.
    #[error("public-input hashing currently requires empty blobIndices, got {count}")]
    UnsupportedBlobIndices {
        /// Number of unsupported blob indices.
        count: usize,
    },
}

/// Compute every per-proof-system hash for the currently supported batch
/// profile: block number zero, no sender binding, and no blobs.
///
/// The reference manager returns empty `customData` for block zero, so this
/// helper supplies one empty custom-data value per rollup and binds `address(0)`.
///
/// # Errors
///
/// Returns [`PublicInputsError`] for unsupported batch features or malformed
/// proof-system carriers.
#[tracing::instrument(level = "debug", name = "public_inputs_hashes", skip_all, err)]
pub fn public_inputs_hashes(batch: &EvmBatch, vkey: B256) -> Result<Vec<B256>, PublicInputsError> {
    if batch.blockNumber != 0 {
        return Err(PublicInputsError::UnsupportedBlockNumber {
            block_number: batch.blockNumber,
        });
    }
    if batch.bindMsgSenderInPublicInput {
        return Err(PublicInputsError::UnsupportedSenderBinding);
    }
    if !batch.blobIndices.is_empty() {
        return Err(PublicInputsError::UnsupportedBlobIndices {
            count: batch.blobIndices.len(),
        });
    }

    let rollup_assignments: Vec<RollupProofAssignment> = batch
        .rollupIdsWithProofSystems
        .iter()
        .map(|r| RollupProofAssignment {
            rollup_id: RollupId(r.rollupId),
            proof_system_indexes: r.proofSystemIndexes.clone(),
        })
        .collect();
    let vk_matrix: Vec<Vec<[u8; 32]>> = rollup_assignments
        .iter()
        .map(|a| vec![vkey.0; a.proof_system_indexes.len()])
        .collect();
    let plan = ProofPlan {
        proof_systems: batch.proofSystems.clone(),
        custom_data: vec![Bytes::new(); rollup_assignments.len()],
        rollup_assignments,
        vk_matrix,
    };
    let entry_hashes: Vec<B256> = batch.entries.iter().map(entry_hash).collect();
    let static_entry_hashes: Vec<B256> =
        batch.staticEntries.iter().map(static_entry_hash).collect();
    all_per_ps_hashes(
        &plan,
        &entry_hashes,
        &static_entry_hashes,
        &[],
        &batch.callData,
        Address::ZERO,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn position_of_finds_existing() {
        assert_eq!(position_of(0, &[0, 1, 2]), Some(0));
        assert_eq!(position_of(1, &[0, 1, 2]), Some(1));
        assert_eq!(position_of(2, &[0, 1, 2]), Some(2));
        assert_eq!(position_of(3, &[0, 1, 2]), None);
        assert_eq!(position_of(0, &[5]), None);
    }

    #[test]
    fn shared_hash_deterministic() {
        let s = shared_public_input(&[], &[], &[], &Bytes::new(), &[], Address::ZERO);
        let s2 = shared_public_input(&[], &[], &[], &Bytes::new(), &[], Address::ZERO);
        assert_eq!(s, s2);
    }

    #[test]
    fn shared_hash_changes_with_each_field() {
        let empty = Bytes::new();
        let base = shared_public_input(&[], &[], &[], &empty, &[], Address::ZERO);
        let h1 = shared_public_input(
            &[B256::repeat_byte(1)],
            &[],
            &[],
            &empty,
            &[],
            Address::ZERO,
        );
        let h2 = shared_public_input(
            &[],
            &[B256::repeat_byte(1)],
            &[],
            &empty,
            &[],
            Address::ZERO,
        );
        let h3 = shared_public_input(
            &[],
            &[],
            &[B256::repeat_byte(1)],
            &empty,
            &[],
            Address::ZERO,
        );
        let h4 = shared_public_input(
            &[],
            &[],
            &[],
            &Bytes::from_static(&[0x01]),
            &[],
            Address::ZERO,
        );
        let h5 = shared_public_input(
            &[],
            &[],
            &[],
            &empty,
            &[B256::repeat_byte(1)],
            Address::ZERO,
        );
        let h6 = shared_public_input(&[], &[], &[], &empty, &[], Address::repeat_byte(1));
        for h in [h1, h2, h3, h4, h5, h6] {
            assert_ne!(base, h);
        }
    }

    fn singleton_plan() -> ProofPlan {
        ProofPlan {
            proof_systems: vec![address!("00000000000000000000000000000000000000aa")],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![0],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0x42; 32]]],
        }
    }

    #[test]
    fn per_ps_hash_with_zero_attesters_uses_zero_acc() {
        // Build a plan where PS 0 attests nothing.
        let plan = ProofPlan {
            proof_systems: vec![
                address!("00000000000000000000000000000000000000aa"),
                address!("00000000000000000000000000000000000000bb"),
            ],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![1], // only PS index 1 attests
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0x42; 32]]],
        };
        plan.check_invariants().unwrap();
        let shared = B256::ZERO;
        let h0 = per_ps_public_inputs_hash(shared, &plan, 0);
        let h1 = per_ps_public_inputs_hash(shared, &plan, 1);

        // PS 0 has no attesters ⇒ acc = bytes32(0) ⇒ hash =
        // keccak256(shared || bytes32(0))
        let expected_h0 = {
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(shared.as_slice());
            // acc bytes stay zero
            keccak256(buf)
        };
        assert_eq!(h0, expected_h0);

        // PS 1 has one attester ⇒ acc ≠ 0 ⇒ different result.
        assert_ne!(h1, h0);
    }

    #[test]
    fn per_ps_hash_deterministic() {
        let plan = singleton_plan();
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), &[], Address::ZERO);
        let h1 = per_ps_public_inputs_hash(shared, &plan, 0);
        let h2 = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn per_ps_hash_changes_with_vkey() {
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), &[], Address::ZERO);
        let mut plan = singleton_plan();
        let h_base = per_ps_public_inputs_hash(shared, &plan, 0);
        plan.vk_matrix[0][0] = [0x99; 32];
        let h_changed = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_ne!(h_base, h_changed);
    }

    #[test]
    fn custom_data_hash_binds_rollup_and_bytes() {
        let base = custom_data_hash(RollupId(1), &Bytes::new());
        assert_ne!(base, custom_data_hash(RollupId(2), &Bytes::new()));
        assert_ne!(
            base,
            custom_data_hash(RollupId(1), &Bytes::from_static(b"data"))
        );
    }

    #[test]
    fn all_per_ps_hashes_length_matches_proof_systems() {
        let plan = singleton_plan();
        let hashes = all_per_ps_hashes(&plan, &[], &[], &[], &Bytes::new(), Address::ZERO).unwrap();
        assert_eq!(hashes.len(), plan.proof_systems.len());
    }

    #[test]
    fn all_per_ps_hashes_rejects_malformed_plan() {
        let mut plan = singleton_plan();
        plan.vk_matrix = vec![]; // outer-length mismatch
        let err =
            all_per_ps_hashes(&plan, &[], &[], &[], &Bytes::new(), Address::ZERO).unwrap_err();
        assert!(matches!(
            err,
            ProofPlanInvariantError::VkMatrixOuterLength { .. }
        ));
    }

    fn carrier_batch() -> crate::abi::EvmBatch {
        use crate::abi::RollupIdWithProofSystemsSol;
        let mut batch = crate::abi::EvmBatch {
            proofSystems: vec![address!("00000000000000000000000000000000000000aa")],
            ..Default::default()
        };
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: 1,
            proofSystemIndexes: vec![0],
        }];
        batch
    }

    #[test]
    fn per_ps_fold_matches_three_word_encoding() {
        let vkey = B256::repeat_byte(0x42);
        let got = public_inputs_hashes(&carrier_batch(), vkey).unwrap()[0];
        let custom_hash = custom_data_hash(RollupId(1), &Bytes::new());
        let shared =
            shared_public_input(&[], &[], &[], &Bytes::new(), &[custom_hash], Address::ZERO);

        let mut step = [0u8; 96];
        step[63] = 1;
        step[64..].copy_from_slice(vkey.as_slice());
        let acc = keccak256(step);
        let mut packed = [0u8; 64];
        packed[..32].copy_from_slice(shared.as_slice());
        packed[32..].copy_from_slice(acc.as_slice());
        assert_eq!(got, keccak256(packed));
    }

    #[test]
    fn unsupported_initial_profile_features_are_typed_errors() {
        let vkey = B256::repeat_byte(0x42);

        let mut batch = carrier_batch();
        batch.blockNumber = 7;
        assert!(matches!(
            public_inputs_hashes(&batch, vkey),
            Err(PublicInputsError::UnsupportedBlockNumber { block_number: 7 })
        ));

        let mut batch = carrier_batch();
        batch.bindMsgSenderInPublicInput = true;
        assert!(matches!(
            public_inputs_hashes(&batch, vkey),
            Err(PublicInputsError::UnsupportedSenderBinding)
        ));

        let mut batch = carrier_batch();
        batch.blobIndices.push(alloy_primitives::U256::ZERO);
        assert!(matches!(
            public_inputs_hashes(&batch, vkey),
            Err(PublicInputsError::UnsupportedBlobIndices { count: 1 })
        ));
    }
}
