//! Two-stage `publicInputsHash` construction in Rust.
//!
//! Byte-for-byte mirror of the on-chain construction in upstream
//! `EEZ.sol`'s `_verifyProofSystemBatch`. The composer finalizes the
//! batch and computes these bytes; the PROVER independently recomputes
//! and signs each per-PS digest (the composer holds no attester key);
//! the composer's proof sink fills
//! `ProofSystemBatchPerVerificationEntries.proofs[]` with the prover's
//! signature and submits. The on-chain rebuild then produces the SAME
//! bytes — every proof verifies if and only if both sides agreed.
//!
//! A future byte-equality lock against a Solidity oracle (analogous to
//! the call-hash vectors in `action.rs`) belongs at
//! `crates/eez-protocol/tests/public_inputs_hash_vectors.rs`.
//!
//! # Construction
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
//! wrong fold.

use crate::{
    ProofPlan, ProofPlanInvariantError, RollupId, RollupProofAssignment, TimestampAndBlockHash,
};
use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_sol_types::{SolValue, sol};

use tracing::debug;

use crate::abi::{ExecutionEntrySol, LookupCallSol};
use crate::batch::EvmBatch;

// ── Per-element atomic hashes ─────────────────────────────────────

/// `keccak256(abi.encode(entry))` — binds the full
/// `ExecutionEntry` content (stateDeltas, proxyEntryHash,
/// destinationRollupId, L2ToL1Calls, expectedL1ToL2Calls,
/// callCount, returnData, rollingHash). Matches `EEZ.sol`'s
/// per-entry hashing.
///
/// Uses `SolValue::abi_encode` (standalone encoding — the
/// dynamic-struct offset that `abi.encode(structValue)`
/// emits on a single value is preserved).
#[must_use]
pub fn entry_hash(entry: &ExecutionEntrySol) -> B256 {
    keccak256(SolValue::abi_encode(entry))
}

/// `keccak256(abi.encode(lookupCall))` — same shape as
/// [`entry_hash`] for `LookupCall` structs. Matches `EEZ.sol`'s
/// per-lookup hashing.
///
/// Internal fold step — integrators call [`public_inputs_hashes`].
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
/// `lookup_call_hash` / `blobhash(batch.blobIndices[i])`).
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
    /// that `EEZ.sol`'s `_verifyProofSystemBatch` performs in its
    /// per-PS inner loop.
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
/// plus each rollup's `proofSystemIndex[]` non-emptiness are
/// enforced — `_validateStructure` does NOT require every
/// global PS to be referenced by some rollup). The on-chain
/// code still computes `publicInputsHash[k] = keccak(shared,
/// bytes32(0))` for such a slot. This function mirrors that
/// behavior: `acc` stays `bytes32(0)` if no rollup attests,
/// and the function returns the resulting hash.
///
/// Internal fold step — integrators call [`public_inputs_hashes`] (or
/// [`all_per_ps_hashes`]), which run `ProofPlan::check_invariants` first.
/// This function assumes a validated plan: it indexes `per_rollup_context` /
/// `vk_matrix` directly and would panic on a length mismatch.
#[must_use]
pub fn per_ps_public_inputs_hash(shared: B256, plan: &ProofPlan, ps_index_in_global: u64) -> B256 {
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
        // exactly — same trick as `common_cross_chain_call_hash`).
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
    // This is the ONE helper both composer and prover call, so logging here
    // gives the publicInputsHash an audit trail on BOTH sides — an on-chain
    // `InvalidProof`/hash-mismatch is then diagnosable from the logs.
    debug!(
        target: "eez::public_inputs",
        shared = %shared,
        entry_hashes = entry_hashes.len(),
        lookup_call_hashes = lookup_call_hashes.len(),
        proof_systems = plan.proof_systems.len(),
        first_hash = ?out.first(),
        "all_per_ps_hashes: computed publicInputsHash(es)",
    );
    Ok(out)
}

/// Compute every per-PS `publicInputsHash` for a FINALIZED batch (proof-system
/// carriers populated — `proofSystems`, `rollupIdsWithProofSystems`) given the
/// single registered verification `vkey`.
///
/// This is the ONE place the `ProofPlan` is reconstructed from a batch, so the
/// composer (which finalizes + submits) and the prover (which independently
/// recomputes the hash from the batch it receives on the control feed) produce
/// **byte-identical** hashes — the soundness invariant the on-chain
/// `_verifyProofSystemBatch` enforces.
///
/// `per_rollup_context` mirrors the reference `Rollup.getTimestampAndBlockHash`
/// for every rollup, keyed on `batch.blockNumber`:
/// - `blockNumber == 0` (timeless): `(0, 0)` — `l1_block_hash` must be `None`.
/// - `blockNumber == N != 0` (bound): `(0, blockhash(N))` — the TIMESTAMP stays
///   ZERO (`Rollup.sol` returns `(0, blockHash)` for a specific past block; a
///   past timestamp is not recoverable on-chain — only the `.max` sentinel
///   carries one, and we never operate it). `l1_block_hash` must be `Some`.
///
/// Any other combination is a wiring bug and errors out (fail-closed).
/// Lookup-call and blob hashes are empty (the current L2→L1 scenario carries
/// neither); when they appear they must be folded in on BOTH sides through this
/// same helper.
///
/// # Errors
/// Returns [`ProofPlanInvariantError`] if the reconstructed plan is malformed
/// or the `(blockNumber, l1_block_hash)` pair is inconsistent.
#[tracing::instrument(level = "debug", name = "public_inputs_hashes", skip_all, err)]
pub fn public_inputs_hashes(
    batch: &EvmBatch,
    vkey: B256,
    l1_block_hash: Option<B256>,
) -> Result<Vec<B256>, ProofPlanInvariantError> {
    let context = match (batch.blockNumber, l1_block_hash) {
        (0, None) => TimestampAndBlockHash {
            timestamp: [0u8; 32],
            block_hash: [0u8; 32],
        },
        (n, Some(hash)) if n != 0 => TimestampAndBlockHash {
            timestamp: [0u8; 32],
            block_hash: hash.0,
        },
        (n, h) => {
            return Err(ProofPlanInvariantError::BlockContextMismatch {
                block_number: n,
                hash_supplied: h.is_some(),
            });
        }
    };
    let rollup_assignments: Vec<RollupProofAssignment> = batch
        .rollupIdsWithProofSystems
        .iter()
        .map(|r| RollupProofAssignment {
            rollup_id: RollupId(r.rollupId.to::<u64>()),
            proof_system_index: r.proofSystemIndex.clone(),
        })
        .collect();
    let per_rollup_context = vec![context; rollup_assignments.len()];
    let vk_matrix: Vec<Vec<[u8; 32]>> = rollup_assignments
        .iter()
        .map(|a| vec![vkey.0; a.proof_system_index.len()])
        .collect();
    let plan = ProofPlan {
        proof_systems: batch.proofSystems.clone(),
        rollup_assignments,
        per_rollup_context,
        vk_matrix,
        cross_proof_system_interactions: batch.crossProofSystemInteractions.0,
    };
    let entry_hashes: Vec<B256> = batch.entries.iter().map(entry_hash).collect();
    // The lookup calls MUST be folded in too: `EEZ._verifyProofSystemBatch` hashes
    // `l1ToL2lookupCalls[]` into `sharedPublicInput` (EEZ.sol:584-587). Omitting them
    // (passing `&[]`) is byte-identical for batches WITHOUT lookup calls (the success +
    // settlement paths), but for a FAILED inbound — which carries a `LookupCall{failed}`
    // that supplies the user's revert — the composer hash would diverge from the on-chain
    // recompute and the prover's signature would fail `InvalidProof`. So derive them here.
    let lookup_call_hashes: Vec<B256> = batch
        .l1ToL2lookupCalls
        .iter()
        .map(lookup_call_hash)
        .collect();
    all_per_ps_hashes(
        &plan,
        &entry_hashes,
        &lookup_call_hashes,
        &[],
        &batch.callData,
    )
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

    fn singleton_plan() -> ProofPlan {
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
        let plan = ProofPlan {
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

    /// Minimal finalized batch (carriers filled) for `public_inputs_hashes`
    /// context tests — one PS, one attesting rollup, no entries.
    fn carrier_batch(block_number: u64) -> crate::batch::EvmBatch {
        use crate::abi::RollupIdWithProofSystemsSol;
        let mut batch = crate::batch::EvmBatch {
            blockNumber: block_number,
            proofSystems: vec![address!("00000000000000000000000000000000000000aa")],
            ..Default::default()
        };
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: U256::from(1),
            proofSystemIndex: vec![0],
        }];
        batch
    }

    /// Bound-context fold cross-checked against the UPSTREAM test's own mirror
    /// (`test/BlockNumberBinding.t.sol:_expectedPublicInputsHash`): for one PS
    /// and one attesting rollup,
    ///   `acc  = keccak256(abi.encode(bytes32(0), rid, vkey, blockHash, timestamp))`
    ///   `hash = keccak256(abi.encodePacked(shared, acc))`
    /// with timestamp == 0 for a specific bound block (`Rollup.sol` returns
    /// `(0, blockhash(N))`). The expectation is HAND-ENCODED (five static
    /// 32-byte words) — independent of the `PerPsFoldStep` sol! struct, so a
    /// refactor that drifts the fold encoding fails here.
    #[test]
    fn bound_context_matches_upstream_fold_construction() {
        let vkey = B256::repeat_byte(0x42);
        let blockhash_n = B256::repeat_byte(0xab);
        let batch = carrier_batch(1234);

        let got = public_inputs_hashes(&batch, vkey, Some(blockhash_n)).unwrap()[0];

        // shared over empty entries/lookups/blobs + empty callData + zero cross-PS.
        let shared = shared_public_input(&[], &[], &[], &Bytes::new(), B256::ZERO);
        // acc = keccak256(abi.encode(0, rid=1, vkey, blockhash(N), timestamp=0))
        let mut step = [0u8; 160];
        step[63] = 0x01; // uint256 rid = 1 (word 2, big-endian)
        step[64..96].copy_from_slice(vkey.as_slice()); // word 3
        step[96..128].copy_from_slice(blockhash_n.as_slice()); // word 4
        // words 1 (acc) and 5 (timestamp) stay zero.
        let acc = keccak256(step);
        let mut packed = [0u8; 64];
        packed[..32].copy_from_slice(shared.as_slice());
        packed[32..].copy_from_slice(acc.as_slice());
        assert_eq!(
            got,
            keccak256(packed),
            "fold must match the on-chain construction"
        );
    }

    #[test]
    fn timeless_and_bound_contexts_diverge() {
        let vkey = B256::repeat_byte(0x42);
        let timeless = public_inputs_hashes(&carrier_batch(0), vkey, None).unwrap()[0];
        let bound = public_inputs_hashes(&carrier_batch(7), vkey, Some(B256::repeat_byte(0xab)))
            .unwrap()[0];
        let bound_other =
            public_inputs_hashes(&carrier_batch(7), vkey, Some(B256::repeat_byte(0xcd))).unwrap()
                [0];
        assert_ne!(timeless, bound);
        assert_ne!(
            bound, bound_other,
            "the hash must bind the BLOCK HASH value"
        );
    }

    #[test]
    fn inconsistent_block_context_is_refused() {
        let vkey = B256::repeat_byte(0x42);
        // Bound batch without the hash → fail-closed.
        let err = public_inputs_hashes(&carrier_batch(7), vkey, None).unwrap_err();
        assert!(matches!(
            err,
            ProofPlanInvariantError::BlockContextMismatch {
                block_number: 7,
                hash_supplied: false
            }
        ));
        // Timeless batch WITH a hash → also refused (wiring bug, not ignored).
        let err = public_inputs_hashes(&carrier_batch(0), vkey, Some(B256::repeat_byte(0x01)))
            .unwrap_err();
        assert!(matches!(
            err,
            ProofPlanInvariantError::BlockContextMismatch {
                block_number: 0,
                hash_supplied: true
            }
        ));
    }
}
