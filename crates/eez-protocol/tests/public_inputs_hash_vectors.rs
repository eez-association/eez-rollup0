//! Byte-for-byte lock between the pinned Solidity public-input construction
//! and its Rust mirror.
//!
//! The fixture values are asserted by `contracts/test/PublicInputsHashVectors.t.sol`.
//! That test also submits each vector to the pinned EEZ contract with proof
//! systems configured to accept only the expected hash.

use std::{fs, path::PathBuf};

use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use alloy_sol_types::{SolValue, sol};
use eez_protocol::abi::{
    EvmBatch, ExecutionEntrySol, ExpectedRootPerRollupSol, RollupIdWithProofSystemsSol,
    RollupUpdateSol, StaticExecutionEntrySol,
};
use eez_protocol::public_inputs::{
    all_per_ps_hashes, batch_shared_public_input, entry_hash, shared_public_input,
    static_entry_hash,
};
use eez_protocol::{ProofPlan, RollupId, RollupProofAssignment};
use serde::Deserialize;

const EXPECTED_PROTOCOL_COMMIT: &str = "143d87e62cade98bfc122d612638dcaaf1c85633";
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

/// The entry the Solidity oracle hashes for the full-entry vector.
fn full_entry() -> ExecutionEntrySol {
    ExecutionEntrySol {
        rollupUpdates: vec![RollupUpdateSol {
            rollupId: 1,
            currentRoot: B256::from(U256::from(0x1111)),
            newRoot: B256::from(U256::from(0x2222)),
            etherDelta: alloy_primitives::aliases::I192::ZERO,
        }],
        proxyEntryHash: B256::from(U256::from(0x3333)),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: B256::from(U256::from(0x4444)),
        destinationRollupId: 1,
        success: true,
        returnData: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
    }
}

#[test]
fn execution_entry_encoding_matches_pinned_solidity() {
    assert_eq!(
        entry_hash(&full_entry()),
        parse_b256("0x752aa6c5ddc53a6bfdfec261248ee29246f6e831c59d3567d2c22d80dbf93dc1")
    );
}

#[test]
fn static_execution_entry_encoding_matches_pinned_solidity() {
    let entry = StaticExecutionEntrySol {
        expectedRoots: vec![ExpectedRootPerRollupSol {
            rollupId: 1,
            root: B256::from(U256::from(0x1111)),
        }],
        proxyEntryHash: B256::from(U256::from(0x5555)),
        l2ToL1Calls: Vec::new(),
        rollingHash: B256::from(U256::from(0x6666)),
        destinationRollupId: 1,
        success: true,
        returnData: Bytes::from_static(&[0xca, 0xfe]),
    };

    assert_eq!(
        static_entry_hash(&entry),
        parse_b256("0x1a63bcaad1cc1d18331cee8e48f0074de3a9f1f887255d3dfdf44f62a08036c3")
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

/// The scan's composition reproduces the shared input of every vector inside
/// the supported profile.
#[test]
fn batch_shared_public_input_matches_pinned_solidity_vectors() {
    let in_profile = load_vectors().into_iter().filter(|v| {
        v.blob_hashes.is_empty()
            && v.static_entry_hashes.is_empty()
            && v.rollup_assignments.iter().all(|a| a.custom_data == "0x")
    });
    let mut checked = 0;
    for vector in in_profile {
        // The oracle's only entry is `full_entry`.
        let full = entry_hash(&full_entry());
        assert!(
            vector.entry_hashes.iter().all(|h| parse_b256(h) == full),
            "vector `{}`",
            vector.name
        );
        let batch = EvmBatch {
            entries: vec![full_entry(); vector.entry_hashes.len()],
            rollupIdsWithProofSystems: vector
                .rollup_assignments
                .iter()
                .map(|a| RollupIdWithProofSystemsSol {
                    rollupId: a.rollup_id,
                    proofSystemIndexes: a.proof_system_indexes.clone(),
                })
                .collect(),
            callData: parse_bytes(&vector.call_data),
            ..Default::default()
        };
        assert_eq!(
            batch_shared_public_input(&batch, Address::ZERO),
            Ok(parse_b256(&vector.expected_shared_public_input)),
            "vector `{}`",
            vector.name
        );
        checked += 1;
    }
    assert_eq!(checked, 3, "three vectors lie inside the supported profile");
}

#[test]
fn live_context_matches_solidity_and_rejects_missing_context() {
    use eez_protocol::l1_context::LiveSettlementContext;
    use eez_protocol::public_inputs::{
        batch_shared_public_input_with_context, public_inputs_hashes_with_context,
    };
    let context = LiveSettlementContext {
        parent_hash: B256::repeat_byte(0x99),
        timestamp: 42,
    };
    let mut batch = EvmBatch {
        blockNumber: u64::MAX,
        proofSystems: vec![Address::repeat_byte(1)],
        rollupIdsWithProofSystems: vec![RollupIdWithProofSystemsSol {
            rollupId: 1,
            proofSystemIndexes: vec![0],
        }],
        ..Default::default()
    };
    let vkey = B256::with_last_byte(0x42);
    let shared =
        batch_shared_public_input_with_context(&batch, Address::ZERO, Some(context)).unwrap();
    let hashes = public_inputs_hashes_with_context(&batch, vkey, Some(context)).unwrap();
    // Both constants are independently asserted against the real reference EEZ contract.
    assert_eq!(
        shared,
        alloy_primitives::b256!("65c65b6059b34c249f220e4a2f144b0c9bd0447e9248cbd839f6fa2c26e7eec9")
    );
    assert_eq!(
        hashes,
        vec![alloy_primitives::b256!(
            "83c332392dceacc310d1892f43a321b25c7ed31dc84da77d3099e910e3f0bc3e"
        )]
    );
    for changed in [
        LiveSettlementContext {
            timestamp: 43,
            ..context
        },
        LiveSettlementContext {
            parent_hash: B256::repeat_byte(0x98),
            ..context
        },
    ] {
        assert_ne!(
            hashes,
            public_inputs_hashes_with_context(&batch, vkey, Some(changed)).unwrap()
        );
    }
    assert!(public_inputs_hashes_with_context(&batch, vkey, None).is_err());
    batch.blockNumber = 0;
    assert!(public_inputs_hashes_with_context(&batch, vkey, Some(context)).is_err());
    assert!(public_inputs_hashes_with_context(&batch, vkey, None).is_ok());
}
