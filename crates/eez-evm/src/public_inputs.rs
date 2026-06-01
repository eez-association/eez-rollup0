//! Two-stage `publicInputsHash` construction in Rust.
//!
//! Byte-for-byte mirror of the on-chain construction in
//! `EEZ.sol:606-668` (`_verifyProofSystemBatch`). The composer
//! produces these bytes off-chain, signs each per-PS digest with
//! the proof system's authorized key, populates
//! `ProofSystemBatchPerVerificationEntries.proofs[]`, and submits
//! the batch. The on-chain rebuild then produces the SAME bytes —
//! every proof verifies if and only if both sides agreed.
//!
//! The byte-equality lock against the Solidity oracle lives at
//! `crates/eez-evm/tests/public_inputs_hash_vectors.rs`,
//! mirroring the `cross_chain_call_hash_vectors.rs` shape from D2.
//! The Foundry generator script is
//! `contracts/script/GenPublicInputsHashVectors.s.sol`.
//!
//! # Construction (read off `EEZ.sol:606-668`)
//!
//! ```text
//! // Step 1: per-entry / per-lookup-call atomic hashes.
//! entryHashes[i]      = keccak256(abi.encode(batch.entries[i]))
//! lookupCallHashes[i] = keccak256(abi.encode(batch.l1ToL2lookupCalls[i]))
//!
//! // Step 2: blob hashes — `blobhash(batch.blobIndices[i])`
//! // resolved off-chain by the composer; the on-chain code reads
//! // them from the tx-level blob set. Both produce the SAME
//! // bytes32 values, so this Rust impl accepts them as inputs.
//! blobHashes[i] = blobhash(batch.blobIndices[i])
//!
//! // Step 3: shared hash (everything except per-rollup
//! // attestation context). Note the abi.encodePacked OUTER
//! // wrapper over four abi.encode-wrapped arrays + a keccak256 +
//! // a bytes32 — preserves each dynamic array's length prefix
//! // while concatenating without re-padding.
//! sharedPublicInput = keccak256(abi.encodePacked(
//!     abi.encode(entryHashes),
//!     abi.encode(lookupCallHashes),
//!     abi.encode(blobHashes),
//!     keccak256(batch.callData),
//!     batch.crossProofSystemInteractions,
//! ))
//!
//! // Step 4: per-PS accumulator. For each PS k in
//! // proofSystems[], walk attesting rollups in canonical
//! // (rollupId-ascending) order; for each rollup that lists k
//! // in its proofSystemIndex[], fold (rid, vkey, blockHash,
//! // timestamp) — note the INCREMENTAL keccak256(abi.encode)
//! // shape, NOT a flat concat. `j = position_of(k,
//! // proofSystemIndex[r])` indexes into the JAGGED vkMatrix
//! // row.
//! acc_k = bytes32(0)
//! for each r with k ∈ proofSystemIndex[r]:
//!     let j = position_of(k, proofSystemIndex[r])
//!     acc_k = keccak256(abi.encode(
//!         acc_k, rid_r, vkMatrix[r][j], blockHashes[r], timestamps[r],
//!     ))
//!
//! // Step 5: final per-PS hash. abi.encodePacked: shared + acc
//! // concatenated as two raw bytes32 — no length prefix.
//! publicInputsHash[k] = keccak256(abi.encodePacked(sharedPublicInput, acc_k))
//! ```
//!
//! Off-chain provers MUST mirror this incremental scheme —
//! including the canonical rollupId-ascending order AND the
//! position-of-k-in-local-subset indexing — so the on-chain
//! rebuild produces a byte-identical `publicInputsHash_k`. A
//! naive `vkMatrix[r][k]` global index silently produces the
//! wrong fold (caught in §A1 spec audit; see DERIVATION.md §6e).

use alloy_primitives::{keccak256, Bytes, B256, U256};
use alloy_sol_types::{sol, SolValue};
use eez_protocol::{ProofPlan, ProofPlanInvariantError};

use crate::types::{ExecutionEntrySol, LookupCallSol};
use crate::EvmProtocol;

// ── Per-element atomic hashes ─────────────────────────────────────

/// `keccak256(abi.encode(entry))` — binds the full
/// `ExecutionEntry` content (stateDeltas, proxyEntryHash,
/// destinationRollupId, L2ToL1Calls, expectedL1ToL2Calls,
/// callCount, returnData, rollingHash). Matches
/// `EEZ.sol:619-621`.
///
/// Uses `SolValue::abi_encode` (standalone encoding — the
/// dynamic-struct offset that `abi.encode(structValue)`
/// emits on a single value is preserved).
#[must_use]
pub fn entry_hash(entry: &ExecutionEntrySol) -> B256 {
    keccak256(SolValue::abi_encode(entry))
}

/// `keccak256(abi.encode(lookupCall))` — same shape as
/// [`entry_hash`] for `LookupCall` structs. Matches
/// `EEZ.sol:624-626`.
#[must_use]
pub fn lookup_call_hash(lookup_call: &LookupCallSol) -> B256 {
    keccak256(SolValue::abi_encode(lookup_call))
}

// ── Shared public input ───────────────────────────────────────────

/// Shared portion of the public-inputs hash — everything in the
/// batch except per-rollup attestation context. Identical bytes
/// across all PSes verifying this batch.
///
/// `entry_hashes`, `lookup_call_hashes`, `blob_hashes` are the
/// per-element atomic-hash arrays (produced by [`entry_hash`] /
/// [`lookup_call_hash`] / `blobhash(batch.blobIndices[i])`).
/// `call_data` is `batch.callData` raw bytes. `cross_ps` is the
/// batch's `crossProofSystemInteractions` field (`bytes32(0)`
/// for the single-PS phase).
///
/// Note the `abi.encodePacked`-of-`abi.encode` wrapper pattern:
/// each dynamic array is independently `abi.encode`-wrapped
/// (preserving its length prefix), then the four blobs are
/// concatenated without re-padding. Diverging from this layout
/// silently breaks every proof.
#[must_use]
pub fn shared_public_input(
    entry_hashes: &[B256],
    lookup_call_hashes: &[B256],
    blob_hashes: &[B256],
    call_data: &Bytes,
    cross_ps: B256,
) -> B256 {
    // abi.encodePacked of: 4 abi.encode-wrapped arrays + 32-byte
    // keccak256(callData) + 32-byte cross_ps.
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&entry_hashes.to_vec().abi_encode());
    buf.extend_from_slice(&lookup_call_hashes.to_vec().abi_encode());
    buf.extend_from_slice(&blob_hashes.to_vec().abi_encode());
    let callback_hash = keccak256(call_data.as_ref());
    buf.extend_from_slice(callback_hash.as_slice());
    buf.extend_from_slice(cross_ps.as_slice());
    keccak256(&buf)
}

// ── Per-PS accumulator ────────────────────────────────────────────

sol! {
    /// Internal helper for the per-PS fold's `abi.encode` shape.
    /// Mirrors the positional encoding
    /// `abi.encode(acc, rollupId, vkey, blockHash, timestamp)`
    /// EEZ.sol does at line 666.
    struct PerPsFoldStep {
        bytes32 acc;
        uint256 rollupId;
        bytes32 vkey;
        bytes32 blockHash;
        uint256 timestamp;
    }
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

/// Per-PS public-inputs hash. Folds the attesting rollups for
/// proof system `ps_index_in_global` into a rolling accumulator,
/// then hashes the result with `shared_public_input`.
///
/// `ps_index_in_global` is the index into
/// [`ProofPlan::proof_systems`] (the batch-wide ordering, NOT
/// the per-rollup local index). The function walks
/// `plan.rollup_assignments` in canonical order, looks up
/// `position_of(ps_index_in_global,
/// rollup_assignments[r].proof_system_index)` to find each
/// attesting rollup's local index `j`, and folds
/// `(acc, rid, vk_matrix[r][j], blockHash, timestamp)`.
///
/// **Zero-attesters fall-through.** A global PS with zero
/// attesting rollups is permitted by `EEZ.sol`'s
/// `_validateStructure` (only `proofSystems[]` non-emptiness
/// + each rollup's `proofSystemIndex[]` non-emptiness are
/// enforced — `_validateStructure` does NOT require every
/// global PS to be referenced by some rollup). The on-chain
/// code still computes `publicInputsHash[k] = keccak(shared,
/// bytes32(0))` for such a slot. This function mirrors that
/// behavior: `acc` stays `bytes32(0)` if no rollup attests,
/// and the function returns the resulting hash.
#[must_use]
pub fn per_ps_public_inputs_hash(
    shared: B256,
    plan: &ProofPlan<EvmProtocol>,
    ps_index_in_global: u64,
) -> B256 {
    let mut acc = B256::ZERO;
    for (r, assignment) in plan.rollup_assignments.iter().enumerate() {
        let Some(j) = position_of(ps_index_in_global, &assignment.proof_system_index) else {
            continue;
        };
        let ctx = &plan.per_rollup_context[r];
        let step = PerPsFoldStep {
            acc,
            rollupId: U256::from(assignment.rollup_id.0),
            vkey: B256::from(plan.vk_matrix[r][j]),
            blockHash: B256::from(ctx.block_hash),
            timestamp: U256::from_be_bytes(ctx.timestamp),
        };
        // Step 4: incremental keccak256(abi.encode(...)). Use
        // `abi_encode_params` to get the positional form
        // (matches Solidity's `abi.encode(field1, field2, ...)`
        // exactly — same trick as `cross_chain_call_hash`).
        acc = keccak256(PerPsFoldStep::abi_encode_params(&step));
    }
    // Step 5: abi.encodePacked of shared + acc → two raw
    // bytes32 concatenated. No length prefix; just 64 bytes.
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
/// `plan.proof_systems` (same length, same ordering). The §E
/// submitter consumes this directly and feeds each hash to
/// the corresponding signer.
///
/// # Errors
///
/// Returns [`ProofPlanInvariantError`] if `plan` is malformed.
/// Callers SHOULD have run `plan.check_invariants()` already;
/// this is belt-and-suspenders.
pub fn all_per_ps_hashes(
    plan: &ProofPlan<EvmProtocol>,
    entry_hashes: &[B256],
    lookup_call_hashes: &[B256],
    blob_hashes: &[B256],
    call_data: &Bytes,
) -> Result<Vec<B256>, ProofPlanInvariantError> {
    plan.check_invariants()?;
    let shared = shared_public_input(
        entry_hashes,
        lookup_call_hashes,
        blob_hashes,
        call_data,
        B256::from(plan.cross_proof_system_interactions),
    );
    let out: Vec<B256> = (0..plan.proof_systems.len() as u64)
        .map(|k| per_ps_public_inputs_hash(shared, plan, k))
        .collect();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;
    use eez_protocol::{RollupId, RollupProofAssignment, TimestampAndBlockHash};

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
        let s = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let s2 = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        assert_eq!(s, s2);
    }

    #[test]
    fn shared_hash_changes_with_each_field() {
        let base = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let h1 = shared_public_input(&[B256::repeat_byte(1)], &[], &[], &Bytes::new(), B256::ZERO);
        let h2 = shared_public_input(&[], &[B256::repeat_byte(1)], &[], &Bytes::new(), B256::ZERO);
        let h3 = shared_public_input(&[], &[], &[B256::repeat_byte(1)], &Bytes::new(), B256::ZERO);
        let h4 = shared_public_input(&[], &[], &[], &Bytes::from_static(&[0x01]), B256::ZERO);
        let h5 = shared_public_input(&[], &[], &[], &Bytes::new(), B256::repeat_byte(1));
        for h in [h1, h2, h3, h4, h5] {
            assert_ne!(base, h);
        }
    }

    fn singleton_plan() -> ProofPlan<EvmProtocol> {
        ProofPlan {
            proof_systems: vec![address!("00000000000000000000000000000000000000aa")],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![TimestampAndBlockHash::default()],
            vk_matrix: vec![vec![[0x42; 32]]],
            cross_proof_system_interactions: [0u8; 32],
        }
    }

    #[test]
    fn per_ps_hash_with_zero_attesters_uses_zero_acc() {
        // Build a plan where PS 0 attests nothing.
        let plan = ProofPlan::<EvmProtocol> {
            proof_systems: vec![
                address!("00000000000000000000000000000000000000aa"),
                address!("00000000000000000000000000000000000000bb"),
            ],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![1], // only PS index 1 attests
            }],
            per_rollup_context: vec![TimestampAndBlockHash::default()],
            vk_matrix: vec![vec![[0x42; 32]]],
            cross_proof_system_interactions: [0u8; 32],
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
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let h1 = per_ps_public_inputs_hash(shared, &plan, 0);
        let h2 = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn per_ps_hash_changes_with_vkey() {
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let mut plan = singleton_plan();
        let h_base = per_ps_public_inputs_hash(shared, &plan, 0);
        plan.vk_matrix[0][0] = [0x99; 32];
        let h_changed = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_ne!(h_base, h_changed);
    }

    #[test]
    fn per_ps_hash_changes_with_timestamp() {
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let mut plan = singleton_plan();
        let h_base = per_ps_public_inputs_hash(shared, &plan, 0);
        plan.per_rollup_context[0].timestamp = [0xff; 32];
        let h_changed = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_ne!(h_base, h_changed);
    }

    #[test]
    fn per_ps_hash_changes_with_block_hash() {
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        let mut plan = singleton_plan();
        let h_base = per_ps_public_inputs_hash(shared, &plan, 0);
        plan.per_rollup_context[0].block_hash = [0xee; 32];
        let h_changed = per_ps_public_inputs_hash(shared, &plan, 0);
        assert_ne!(h_base, h_changed);
    }

    #[test]
    fn all_per_ps_hashes_length_matches_proof_systems() {
        let plan = singleton_plan();
        let hashes = all_per_ps_hashes(&plan, &[], &[], &[], &Bytes::new()).unwrap();
        assert_eq!(hashes.len(), plan.proof_systems.len());
    }

    #[test]
    fn all_per_ps_hashes_rejects_malformed_plan() {
        let mut plan = singleton_plan();
        plan.vk_matrix = vec![]; // outer-length mismatch
        let err = all_per_ps_hashes(&plan, &[], &[], &[], &Bytes::new()).unwrap_err();
        assert!(matches!(
            err,
            ProofPlanInvariantError::VkMatrixOuterLength { .. }
        ));
    }
}
