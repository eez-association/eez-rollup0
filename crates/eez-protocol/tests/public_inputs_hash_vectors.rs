//! Byte-for-byte lock between the pinned Solidity public-input construction
//! and its Rust mirror.
//!
//! The fixture values are asserted by `contracts/test/PublicInputsHashVectors.t.sol`.
//! That test also submits each vector to the pinned EEZ contract with proof
//! systems configured to accept only the expected hash.

use std::{fs, path::PathBuf};

use alloy_primitives::{Address, B256, Bytes, I256, U256, hex, keccak256};
use alloy_sol_types::{SolValue, sol};
use eez_protocol::abi::{ExecutionEntrySol, StateUpdateSol};
use eez_protocol::public_inputs::{all_per_ps_hashes, entry_hash, shared_public_input};
use eez_protocol::{ProofPlan, RollupId, RollupProofAssignment};
use serde::Deserialize;

const EXPECTED_PROTOCOL_COMMIT: &str = "6fcc90b65063831cb7797e9fa361004064d28f9f";
const EXPECTED_SOLIDITY_ORACLE: &str = "contracts/test/PublicInputsHashVectors.t.sol";

sol! {
    struct CustomDataHashInput {
        uint64 rollupId;
        bytes customData;
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    schema_version: u64,
    protocol_commit: String,
    solidity_oracle: String,
    vectors: Vec<Vector>,
}

#[derive(Debug, Deserialize)]
struct Vector {
    name: String,
    #[serde(rename = "entryHashes")]
    entry_hashes: Vec<String>,
    #[serde(rename = "staticEntryHashes")]
    static_entry_hashes: Vec<String>,
    #[serde(rename = "blobHashes")]
    blob_hashes: Vec<String>,
    #[serde(rename = "callData")]
    call_data: String,
    #[serde(rename = "rollupAssignments")]
    rollup_assignments: Vec<Assignment>,
    #[serde(rename = "proofSystemCount")]
    proof_system_count: usize,
    #[serde(rename = "boundSender")]
    bound_sender: String,
    #[serde(rename = "expectedSharedPublicInput")]
    expected_shared_public_input: String,
    #[serde(rename = "expectedPublicInputsHashes")]
    expected_public_inputs_hashes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Assignment {
    #[serde(rename = "rollupId")]
    rollup_id: u64,
    #[serde(rename = "proofSystemIndexes")]
    proof_system_indexes: Vec<u64>,
    vkeys: Vec<String>,
    #[serde(rename = "customData")]
    custom_data: String,
}

fn parse_b256(value: &str) -> B256 {
    value
        .parse()
        .unwrap_or_else(|error| panic!("parse B256 `{value}`: {error}"))
}

fn parse_bytes(value: &str) -> Bytes {
    let value = value.strip_prefix("0x").unwrap_or(value);
    Bytes::from(hex::decode(value).unwrap_or_else(|error| panic!("parse bytes `{value}`: {error}")))
}

fn load_vectors() -> Vec<Vector> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/public_inputs_hash_vectors.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let fixture: Fixture = serde_json::from_str(&raw).expect("valid public-input vector fixture");
    assert_eq!(fixture.schema_version, 1, "fixture schema version");
    assert_eq!(
        fixture.protocol_commit, EXPECTED_PROTOCOL_COMMIT,
        "fixture protocol commit"
    );
    assert_eq!(
        fixture.solidity_oracle, EXPECTED_SOLIDITY_ORACLE,
        "fixture Solidity oracle"
    );
    fixture.vectors
}

fn proof_system_address(index: usize) -> Address {
    let value = u8::try_from(index + 1).expect("fixture has at most 255 proof systems");
    let mut bytes = [0_u8; 20];
    bytes[19] = value;
    Address::from(bytes)
}

fn build_plan(vector: &Vector) -> ProofPlan {
    ProofPlan {
        proof_systems: (0..vector.proof_system_count)
            .map(proof_system_address)
            .collect(),
        rollup_assignments: vector
            .rollup_assignments
            .iter()
            .map(|assignment| RollupProofAssignment {
                rollup_id: RollupId(assignment.rollup_id),
                proof_system_indexes: assignment.proof_system_indexes.clone(),
            })
            .collect(),
        custom_data: vector
            .rollup_assignments
            .iter()
            .map(|assignment| parse_bytes(&assignment.custom_data))
            .collect(),
        vk_matrix: vector
            .rollup_assignments
            .iter()
            .map(|assignment| {
                assignment
                    .vkeys
                    .iter()
                    .map(|vkey| parse_b256(vkey).into())
                    .collect()
            })
            .collect(),
    }
}

#[test]
fn execution_entry_encoding_matches_pinned_solidity() {
    let entry = ExecutionEntrySol {
        stateUpdates: vec![StateUpdateSol {
            rollupId: 1,
            currentState: B256::from(U256::from(0x1111)),
            newState: B256::from(U256::from(0x2222)),
            etherDelta: I256::ZERO,
        }],
        proxyEntryHash: B256::from(U256::from(0x3333)),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: B256::from(U256::from(0x4444)),
        destinationRollupId: 1,
        success: true,
        returnData: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
    };

    assert_eq!(
        entry_hash(&entry),
        parse_b256("0x2c4c8cbc9b39743790f04a13406c6c0e3ab6ca0bf5acb3b923f5549d3aabb759")
    );
}

#[test]
fn rust_matches_pinned_solidity_vectors() {
    let vectors = load_vectors();
    assert!(
        !vectors.is_empty(),
        "fixture must contain at least one vector"
    );

    for vector in vectors {
        let plan = build_plan(&vector);
        plan.check_invariants().unwrap_or_else(|error| {
            panic!("vector `{}` has an invalid plan: {error}", vector.name)
        });

        let entry_hashes: Vec<_> = vector
            .entry_hashes
            .iter()
            .map(|hash| parse_b256(hash))
            .collect();
        let static_entry_hashes: Vec<_> = vector
            .static_entry_hashes
            .iter()
            .map(|hash| parse_b256(hash))
            .collect();
        let blob_hashes: Vec<_> = vector
            .blob_hashes
            .iter()
            .map(|hash| parse_b256(hash))
            .collect();
        let custom_data_hashes: Vec<_> = plan
            .rollup_assignments
            .iter()
            .zip(&plan.custom_data)
            .map(|(assignment, custom_data)| {
                keccak256(
                    CustomDataHashInput {
                        rollupId: assignment.rollup_id.0,
                        customData: custom_data.clone(),
                    }
                    .abi_encode_params(),
                )
            })
            .collect();

        let shared = shared_public_input(
            &entry_hashes,
            &static_entry_hashes,
            &blob_hashes,
            &parse_bytes(&vector.call_data),
            &custom_data_hashes,
            vector
                .bound_sender
                .parse()
                .unwrap_or_else(|error| panic!("parse bound sender: {error}")),
        );
        assert_eq!(
            shared,
            parse_b256(&vector.expected_shared_public_input),
            "vector `{}` shared hash",
            vector.name
        );

        let expected_public_inputs: Vec<_> = vector
            .expected_public_inputs_hashes
            .iter()
            .map(|hash| parse_b256(hash))
            .collect();
        assert_eq!(
            expected_public_inputs.len(),
            vector.proof_system_count,
            "vector `{}` expected hash count",
            vector.name
        );
        assert_eq!(
            all_per_ps_hashes(
                &plan,
                &entry_hashes,
                &static_entry_hashes,
                &blob_hashes,
                &parse_bytes(&vector.call_data),
                vector
                    .bound_sender
                    .parse()
                    .unwrap_or_else(|error| panic!("parse bound sender: {error}")),
            )
            .unwrap_or_else(|error| panic!("vector `{}` hash inputs: {error}", vector.name)),
            expected_public_inputs,
            "vector `{}` public-input hashes",
            vector.name
        );
    }
}
