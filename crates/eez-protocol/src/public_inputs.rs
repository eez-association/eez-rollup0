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
    Ok(per_ps_hashes(plan, shared))
}

/// Every per-PS hash of a checked `plan` over `shared`.
fn per_ps_hashes(plan: &ProofPlan, shared: B256) -> Vec<B256> {
    let out: Vec<B256> = (0..plan.proof_systems.len() as u64)
        .map(|k| per_ps_public_inputs_hash(shared, plan, k))
        .collect();
    // Every publicInputsHash passes through here, so an on-chain
    // `InvalidProof` is diagnosable from the logs.
    debug!(
        target: "eez::public_inputs",
        shared = %shared,
        proof_systems = plan.proof_systems.len(),
        first_hash = ?out.first(),
        "per_ps_hashes: computed publicInputsHash(es)",
    );
    out
}

/// Errors produced while reconstructing public inputs from the initial
/// supported batch profile.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicInputsError {
    /// The proof plan reconstructed from the batch is malformed.
    #[error(transparent)]
    InvalidPlan(#[from] ProofPlanInvariantError),
    /// The calldata profile binds no L1 context.
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
    /// Independently derived hashes must cover the selected indices exactly.
    #[error("blob hash count {actual} does not match blobIndices count {expected}")]
    BlobHashCount {
        /// Number selected by the canonical batch.
        expected: usize,
        /// Number derived from the submitted physical blobs.
        actual: usize,
    },
    /// Manager custom data is supplied once for every rollup assignment.
    #[error("custom-data count {actual} does not match rollup assignment count {expected}")]
    CustomDataCount {
        /// Number of rollup assignments in the canonical batch.
        expected: usize,
        /// Number of authenticated manager values supplied.
        actual: usize,
    },
}

/// What the manager and the DA carrier add to `sharedPublicInput`: the
/// selected blobs' versioned hashes and `getCustomData(blockNumber)` per listed
/// rollup, in listing order. Callers derive both independently of the batch.
#[derive(Debug, Clone, Copy)]
pub struct SettlementContext<'a> {
    /// Versioned hashes selected by the batch's blob indices.
    pub blob_hashes: &'a [B256],
    /// Per-rollup manager data in `rollupIds` order.
    pub custom_data: &'a [Bytes],
}

/// `sharedPublicInput` as `EEZ._verifyProofSystemBatch` computes it for the
/// calldata profile: block zero binds empty `customData` per rollup, and the
/// sender is bound only when the batch asks.
pub fn batch_shared_public_input(
    batch: &EvmBatch,
    sender: Address,
) -> Result<B256, PublicInputsError> {
    if batch.blockNumber != 0 {
        return Err(PublicInputsError::UnsupportedBlockNumber {
            block_number: batch.blockNumber,
        });
    }
    if !batch.blobIndices.is_empty() {
        return Err(PublicInputsError::UnsupportedBlobIndices {
            count: batch.blobIndices.len(),
        });
    }
    let custom_data = vec![Bytes::new(); batch.rollupIdsWithProofSystems.len()];
    batch_shared_public_input_with(
        batch,
        sender,
        SettlementContext {
            blob_hashes: &[],
            custom_data: &custom_data,
        },
    )
}

/// `sharedPublicInput` for `batch` under an independently derived `context`.
/// The scan and the signer both build on it, so they check what is signed.
///
/// # Errors
///
/// [`PublicInputsError`] when `context` does not cover the batch's blobs and rollups.
pub fn batch_shared_public_input_with(
    batch: &EvmBatch,
    sender: Address,
    context: SettlementContext<'_>,
) -> Result<B256, PublicInputsError> {
    if context.blob_hashes.len() != batch.blobIndices.len() {
        return Err(PublicInputsError::BlobHashCount {
            expected: batch.blobIndices.len(),
            actual: context.blob_hashes.len(),
        });
    }
    if context.custom_data.len() != batch.rollupIdsWithProofSystems.len() {
        return Err(PublicInputsError::CustomDataCount {
            expected: batch.rollupIdsWithProofSystems.len(),
            actual: context.custom_data.len(),
        });
    }
    let entry_hashes: Vec<B256> = batch.entries.iter().map(entry_hash).collect();
    let static_entry_hashes: Vec<B256> =
        batch.staticEntries.iter().map(static_entry_hash).collect();
    let custom_data_hashes: Vec<B256> = batch
        .rollupIdsWithProofSystems
        .iter()
        .zip(context.custom_data)
        .map(|(rollup, data)| custom_data_hash(RollupId(rollup.rollupId), data))
        .collect();
    let bound_sender = if batch.bindMsgSenderInPublicInput {
        sender
    } else {
        Address::ZERO
    };
    Ok(shared_public_input(
        &entry_hashes,
        &static_entry_hashes,
        context.blob_hashes,
        &batch.callData,
        &custom_data_hashes,
        bound_sender,
    ))
}

/// Compute every per-proof-system hash for the calldata profile: block number
/// zero, no sender binding, and no blobs.
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
    if batch.bindMsgSenderInPublicInput {
        return Err(PublicInputsError::UnsupportedSenderBinding);
    }
    let shared = batch_shared_public_input(batch, Address::ZERO)?;
    let custom_data = vec![Bytes::new(); batch.rollupIdsWithProofSystems.len()];
    per_ps_hashes_for(batch, vkey, custom_data, shared)
}

/// Compute blob-settlement hashes from independently derived blob hashes and
/// manager data. This profile lists only Rollup0 and never binds the sender.
///
/// # Errors
///
/// Returns [`PublicInputsError`] for sender binding, another listed rollup, or
/// blob hashes that do not cover `blobIndices`.
pub fn public_inputs_hashes_with_context(
    batch: &EvmBatch,
    vkey: B256,
    blob_hashes: &[B256],
    custom_data: Bytes,
) -> Result<Vec<B256>, PublicInputsError> {
    if batch.blockNumber != u64::MAX {
        return Err(PublicInputsError::UnsupportedBlockNumber {
            block_number: batch.blockNumber,
        });
    }
    if batch.bindMsgSenderInPublicInput {
        return Err(PublicInputsError::UnsupportedSenderBinding);
    }
    let custom_data = vec![custom_data];
    let shared = batch_shared_public_input_with(
        batch,
        Address::ZERO,
        SettlementContext {
            blob_hashes,
            custom_data: &custom_data,
        },
    )?;
    per_ps_hashes_for(batch, vkey, custom_data, shared)
}

/// Fold `shared` for every proof system, one uniform `vkey` per assignment.
fn per_ps_hashes_for(
    batch: &EvmBatch,
    vkey: B256,
    custom_data: Vec<Bytes>,
    shared: B256,
) -> Result<Vec<B256>, PublicInputsError> {
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
        custom_data,
        rollup_assignments,
        vk_matrix,
    };
    plan.check_invariants()?;
    Ok(per_ps_hashes(&plan, shared))
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

    /// No in-profile Solidity vector binds a sender or holds a static entry.
    #[test]
    fn batch_shared_public_input_follows_the_supported_profile() {
        use PublicInputsError::{UnsupportedBlobIndices, UnsupportedBlockNumber};
        let sender = Address::repeat_byte(0xbe);
        let base = crate::abi::EvmBatch {
            staticEntries: vec![StaticExecutionEntrySol::default()],
            ..Default::default()
        };
        let statics = [static_entry_hash(&base.staticEntries[0])];
        let shared = |bound| shared_public_input(&[], &statics, &[], &Bytes::new(), &[], bound);
        let (mut bound, mut block, mut blob) = (base.clone(), base.clone(), base.clone());
        bound.bindMsgSenderInPublicInput = true;
        block.blockNumber = 7;
        blob.blobIndices.push(alloy_primitives::U256::ZERO);
        for (batch, want) in [
            (&base, Ok(shared(Address::ZERO))),
            (&bound, Ok(shared(sender))),
            (&block, Err(UnsupportedBlockNumber { block_number: 7 })),
            (&blob, Err(UnsupportedBlobIndices { count: 1 })),
        ] {
            assert_eq!(batch_shared_public_input(batch, sender), want);
        }
    }

    /// Blob settlement binds its derived blob hashes and manager data exactly
    /// where the calldata profile binds empty values, through one composition.
    #[test]
    fn candidate_v1_binds_blob_hashes_and_custom_data() {
        let vkey = B256::repeat_byte(0x42);
        let mut batch = carrier_batch();
        batch.blockNumber = u64::MAX;
        batch.blobIndices = vec![alloy_primitives::U256::ZERO];
        let blob_hashes = [B256::repeat_byte(0x01)];
        let custom_data = Bytes::from_static(&[0xd2; 288]);

        let got =
            public_inputs_hashes_with_context(&batch, vkey, &blob_hashes, custom_data.clone());
        let custom_hash = custom_data_hash(RollupId(1), &custom_data);
        let shared = shared_public_input(
            &[],
            &[],
            &blob_hashes,
            &Bytes::new(),
            &[custom_hash],
            Address::ZERO,
        );
        let plan = singleton_plan();
        assert_eq!(got.unwrap(), [per_ps_public_inputs_hash(shared, &plan, 0)]);

        assert_eq!(
            public_inputs_hashes_with_context(&batch, vkey, &[], custom_data.clone()),
            Err(PublicInputsError::BlobHashCount {
                expected: 1,
                actual: 0
            }),
        );
        let mut wrong_block = batch.clone();
        wrong_block.blockNumber = 0;
        assert_eq!(
            public_inputs_hashes_with_context(
                &wrong_block,
                vkey,
                &blob_hashes,
                custom_data.clone()
            ),
            Err(PublicInputsError::UnsupportedBlockNumber { block_number: 0 }),
        );
        let mut bound = batch.clone();
        bound.bindMsgSenderInPublicInput = true;
        assert_eq!(
            public_inputs_hashes_with_context(&bound, vkey, &blob_hashes, custom_data.clone()),
            Err(PublicInputsError::UnsupportedSenderBinding),
        );
        let context = SettlementContext {
            blob_hashes: &blob_hashes,
            custom_data: &[],
        };
        assert_eq!(
            batch_shared_public_input_with(&batch, Address::ZERO, context),
            Err(PublicInputsError::CustomDataCount {
                expected: 1,
                actual: 0
            }),
        );
    }
}
