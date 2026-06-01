//! Foundry-grounded byte-equality lock for `publicInputsHash`.
//!
//! Reads the JSON fixture at
//! `tests/fixtures/public_inputs_hash_vectors.json`
//! (regenerated via `scripts/regen-public-inputs-hash-vectors.sh`,
//! which runs the Foundry script
//! `contracts/script/GenPublicInputsHashVectors.s.sol`) and
//! asserts that the Rust mirror in
//! [`eez_evm::public_inputs`] produces byte-identical
//! shared + per-PS hashes for every vector.
//!
//! Vectors are flattened — they carry per-element hashes
//! (`entry_hashes`, `lookup_call_hashes`, `blob_hashes`) directly
//! rather than full `ExecutionEntry` / `LookupCall` structs. This
//! decouples the byte-equality lock from the entry struct shape
//! (which evolved in Phase 08 D1 and may evolve again) and isolates
//! the high-value lock to the multi-step
//! `abi.encodePacked`-of-`abi.encode` shared-hash construction +
//! per-PS incremental-keccak fold.
//!
//! The per-element `keccak256(abi.encode(entry))` byte-equality
//! against alloy's `SolValue::abi_encode` is a separate concern,
//! covered by the `cross_chain_call_hash_vectors.rs` byte-equality
//! lock for the analogous 6-field action-hash struct.

use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::{hex, Bytes, B256, U256};
use eez_evm::public_inputs::{per_ps_public_inputs_hash, shared_public_input};
use eez_evm::EvmProtocol;
use eez_protocol::{ProofPlan, RollupId, RollupProofAssignment, TimestampAndBlockHash};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Vector {
    name: String,
    #[serde(rename = "entryHashes")]
    entry_hashes: Vec<String>,
    #[serde(rename = "lookupCallHashes")]
    lookup_call_hashes: Vec<String>,
    #[serde(rename = "blobHashes")]
    blob_hashes: Vec<String>,
    #[serde(rename = "callData")]
    call_data: String,
    #[serde(rename = "crossProofSystemInteractions")]
    cross_proof_system_interactions: String,
    #[serde(rename = "proofSystemCount")]
    proof_system_count: u64,
    #[serde(rename = "rollupAssignments")]
    rollup_assignments: Vec<AssignmentJson>,
    #[serde(rename = "expectedSharedPublicInput")]
    expected_shared_public_input: String,
    #[serde(rename = "expectedPublicInputsHashes")]
    expected_public_inputs_hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AssignmentJson {
    #[serde(rename = "rollupId")]
    rollup_id: u64,
    #[serde(rename = "proofSystemIndex")]
    proof_system_index: Vec<u64>,
    vkeys: Vec<String>,
    #[serde(rename = "blockHash")]
    block_hash: String,
    timestamp: String,
}

fn load_vectors() -> Vec<Vector> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/public_inputs_hash_vectors.json");
    let raw =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    serde_json::from_str(&raw).expect("valid public_inputs_hash_vectors.json")
}

fn parse_b256(s: &str) -> B256 {
    s.parse()
        .unwrap_or_else(|e| panic!("parse B256 `{s}`: {e}"))
}

fn parse_bytes32_array(xs: &[String]) -> Vec<B256> {
    xs.iter().map(|s| parse_b256(s)).collect()
}

fn parse_bytes(s: &str) -> Bytes {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    if stripped.is_empty() {
        Bytes::new()
    } else {
        Bytes::from(hex::decode(stripped).expect("hex callData"))
    }
}

fn parse_timestamp(s: &str) -> [u8; 32] {
    // The generator emits timestamps as decimal strings via
    // `vm.toString(uint256)`. Parse as U256 → big-endian 32 bytes.
    let v = U256::from_str(s).expect("uint256 timestamp");
    v.to_be_bytes::<32>()
}

fn build_plan(v: &Vector) -> ProofPlan<EvmProtocol> {
    let proof_system_count = v.proof_system_count as usize;
    // Vectors don't carry proof-system addresses themselves —
    // the publicInputsHash construction doesn't depend on them
    // (it folds vkeys, not addresses). Synthesize a sentinel
    // address per PS slot so `proof_systems.len() ==
    // proof_system_count`; the actual address value doesn't
    // contribute to the hash. Sentinels are arranged ascending
    // so `check_invariants()` passes.
    let proof_systems: Vec<_> = (0..proof_system_count as u64)
        .map(|k| {
            alloy_primitives::Address::from_slice(&{
                let mut bs = [0u8; 20];
                bs[19] = k as u8 + 1;
                bs
            })
        })
        .collect();

    let mut rollup_assignments = Vec::with_capacity(v.rollup_assignments.len());
    let mut per_rollup_context = Vec::with_capacity(v.rollup_assignments.len());
    let mut vk_matrix = Vec::with_capacity(v.rollup_assignments.len());

    for a in &v.rollup_assignments {
        rollup_assignments.push(RollupProofAssignment {
            rollup_id: RollupId(a.rollup_id),
            proof_system_index: a.proof_system_index.clone(),
        });
        per_rollup_context.push(TimestampAndBlockHash {
            timestamp: parse_timestamp(&a.timestamp),
            block_hash: parse_b256(&a.block_hash).into(),
        });
        vk_matrix.push(
            a.vkeys
                .iter()
                .map(|s| -> [u8; 32] { parse_b256(s).into() })
                .collect(),
        );
    }

    ProofPlan {
        proof_systems,
        rollup_assignments,
        per_rollup_context,
        vk_matrix,
        cross_proof_system_interactions: parse_b256(&v.cross_proof_system_interactions).into(),
    }
}

#[test]
fn all_vectors_match() {
    let vectors = load_vectors();
    assert!(
        vectors.len() >= 7,
        "expected at least 7 vectors, got {}",
        vectors.len()
    );

    for v in &vectors {
        let entry_hashes = parse_bytes32_array(&v.entry_hashes);
        let lookup_call_hashes = parse_bytes32_array(&v.lookup_call_hashes);
        let blob_hashes = parse_bytes32_array(&v.blob_hashes);
        let call_data = parse_bytes(&v.call_data);
        let cross_ps = parse_b256(&v.cross_proof_system_interactions);
        let expected_shared = parse_b256(&v.expected_shared_public_input);
        let expected_per_ps = parse_bytes32_array(&v.expected_public_inputs_hashes);

        // Shared hash byte-equality lock.
        let actual_shared = shared_public_input(
            &entry_hashes,
            &lookup_call_hashes,
            &blob_hashes,
            &call_data,
            cross_ps,
        );
        assert_eq!(
            actual_shared, expected_shared,
            "vector `{}`: sharedPublicInput mismatch (Rust vs Foundry)",
            v.name
        );

        // Per-PS hash byte-equality lock.
        let plan = build_plan(v);
        plan.check_invariants()
            .unwrap_or_else(|e| panic!("vector `{}`: plan invariants: {e}", v.name));
        assert_eq!(expected_per_ps.len(), v.proof_system_count as usize);
        for (k, expected_k) in expected_per_ps.iter().enumerate() {
            let actual_k = per_ps_public_inputs_hash(actual_shared, &plan, k as u64);
            assert_eq!(
                actual_k, *expected_k,
                "vector `{}`: publicInputsHash[{}] mismatch (Rust vs Foundry)",
                v.name, k
            );
        }
    }
}
