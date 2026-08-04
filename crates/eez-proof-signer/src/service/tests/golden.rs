//! Captured protocol and wire-format regressions.

use std::sync::Arc;

use alloy_primitives::{Address, B256, Signature};
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, prove_chunk,
};

use super::{
    ServiceState, TestServer, expected_rollup_id, test_attester_for, test_system_transaction_key,
};
use crate::attest::NonZeroProofSystemVkey;
use crate::cancel::CancellationToken;
use crate::testkit::TEST_SYSTEM_ADDRESS;
use crate::{settlement, validate};

const PINNED_PROTOCOL_COMMIT: &str = "6fcc90b65063831cb7797e9fa361004064d28f9f";

fn fixture_hex(value: &str) -> Vec<u8> {
    let value = value.trim();
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).unwrap()
}

fn fixture(dir: &str, name: &str) -> String {
    let path = format!("{}/tests/fixtures/{dir}/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read fixture {path}: {error}"))
}

fn fixture_json(dir: &str, name: &str) -> serde_json::Value {
    serde_json::from_str(&fixture(dir, name)).unwrap()
}

fn fixture_str<'a>(value: &'a serde_json::Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn fixture_u64(value: &serde_json::Value, field: &str) -> u64 {
    value[field].as_u64().unwrap()
}

#[tokio::test]
async fn captured_current_protocol_window_is_validated_and_signed() {
    const FIXTURE: &str = "captured-anchor-40155";

    let oracle = fixture_json(FIXTURE, "oracle.json");
    let expected_hash = fixture_str(&oracle, "public_inputs_hash")
        .parse::<B256>()
        .unwrap();
    let expected_signature = fixture_hex(fixture_str(&oracle, "expected_test_signature"));
    let captured_attester = fixture_str(&oracle, "attester").parse::<Address>().unwrap();
    let proof_system = fixture_str(&oracle, "proof_system")
        .parse::<Address>()
        .unwrap();
    let proof_system_vkey = NonZeroProofSystemVkey::new(
        fixture_str(&oracle, "proof_system_vkey")
            .parse::<B256>()
            .unwrap(),
    )
    .unwrap();
    let from = fixture_u64(&oracle, "from_block");
    let to = fixture_u64(&oracle, "to_block");
    let rollup_id = fixture_u64(&oracle, "rollup_id");

    assert_eq!(
        fixture_str(&oracle, "protocol_commit"),
        PINNED_PROTOCOL_COMMIT
    );
    assert_eq!(rollup_id, 1);

    let calldata = fixture_hex(&fixture(FIXTURE, "postbatch.hex"));
    assert_eq!(
        calldata.len(),
        usize::try_from(fixture_u64(&oracle, "postbatch_calldata_bytes")).unwrap()
    );
    let captured_batch = settlement::decode_canonical_post_batch(calldata.clone()).unwrap();
    assert_eq!(captured_batch.proofSystems.as_slice(), &[proof_system]);
    let [captured_proof] = captured_batch.proofs.as_slice() else {
        panic!("captured batch must carry exactly one proof");
    };
    let captured_signature = Signature::try_from(captured_proof.as_ref()).unwrap();
    assert_eq!(
        captured_signature
            .recover_address_from_prehash(&expected_hash)
            .unwrap(),
        captured_attester,
    );

    let recorded_blocks = fixture_json(FIXTURE, "blocks.json");
    let recorded_blocks = recorded_blocks.as_array().unwrap();
    assert_eq!(
        recorded_blocks.len(),
        usize::try_from(to - from + 1).unwrap()
    );
    let mut window = vec![ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id,
            from_block: from,
            to_block: to,
            post_batch: Some(PostBatch {
                abi_calldata: calldata,
                public_inputs_hash: expected_hash.to_vec(),
                l1_block_hash: Vec::new(),
            }),
        })),
    }];
    for recorded in recorded_blocks {
        let number = fixture_u64(recorded, "number");
        let rlp = fixture_hex(&fixture(FIXTURE, &format!("block-{number}.rlp.hex")));
        let block = alloy_rlp::decode_exact::<reth_ethereum_primitives::Block>(&rlp).unwrap();
        let hash = block.header.hash_slow();
        assert_eq!(number, block.header.number);
        assert_eq!(hash, fixture_str(recorded, "hash").parse::<B256>().unwrap());
        assert_eq!(
            block.header.parent_hash,
            fixture_str(recorded, "parent_hash")
                .parse::<B256>()
                .unwrap()
        );
        window.push(ProveChunk {
            kind: Some(prove_chunk::Kind::Block(BlockWitness {
                number,
                hash: hash.to_vec(),
                parent_hash: block.header.parent_hash.to_vec(),
                rlp,
                witness: Some(recorded_wire_witness(&fixture(
                    FIXTURE,
                    &format!("witness-{number}.json"),
                ))),
            })),
        });
    }

    let chain_config = serde_json::from_str(&fixture(FIXTURE, "chain-config.json")).unwrap();
    let validator = validate::Validator::stateless_for_test(chain_config, TEST_SYSTEM_ADDRESS);
    assert_eq!(validator.chain_id(), fixture_u64(&oracle, "l2_chain_id"));
    let test_attester = test_attester_for(proof_system_vkey, proof_system);
    let test_attester_address = test_attester.address();
    let state = Arc::new(
        ServiceState::new(
            validator,
            expected_rollup_id(rollup_id),
            test_attester,
            test_system_transaction_key(),
        )
        .unwrap(),
    );

    let response = TestServer::new(state).await.attest(window).await;

    assert_eq!(response.public_inputs_hash, expected_hash.as_slice());
    assert_eq!(response.signature, expected_signature);
    let signature = Signature::try_from(response.signature.as_slice()).unwrap();
    assert_eq!(
        signature
            .recover_address_from_prehash(&expected_hash)
            .unwrap(),
        test_attester_address,
    );
}

#[test]
fn captured_legacy_inbound_calldata_is_rejected_by_target_abi() {
    let post_batch = fixture_json("fresh-chain-inbound-2175", "postbatch.json");
    let calldata = fixture_hex(fixture_str(&post_batch, "abi_calldata"));

    assert_eq!(&calldata[..4], &[0x8b, 0x1a, 0x09, 0x5a]);
    assert!(matches!(
        settlement::decode_canonical_post_batch(calldata),
        Err(settlement::PostBatchDecodeError::InvalidAbi { .. })
    ));
}

fn recorded_wire_witness(encoded: &str) -> ExecutionWitness {
    let witness: serde_json::Value = serde_json::from_str(encoded).unwrap();
    let decode_items = |field: &str| {
        witness[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| fixture_hex(item.as_str().unwrap()))
            .collect()
    };
    ExecutionWitness {
        state: decode_items("state"),
        codes: decode_items("codes"),
        keys: decode_items("keys"),
        headers: decode_items("headers"),
    }
}

#[test]
fn captured_five_field_outbound_events_are_not_decoded_as_target_events() {
    let blocks: Vec<(u64, String, String)> = (626..=630)
        .map(|number| {
            (
                number,
                fixture("nonzero-outbound-630", &format!("block-{number}.rlp.hex")),
                fixture("nonzero-outbound-630", &format!("witness-{number}.json")),
            )
        })
        .collect();
    let oracle = fixture_json("nonzero-outbound-630", "oracle.json");
    let post_batch_json = fixture_json("nonzero-outbound-630", "postbatch.json");
    let from = fixture_u64(&oracle, "from_block");
    let to = fixture_u64(&oracle, "to_block");
    let rollup = fixture_u64(&oracle, "expected_rollup_id");
    assert_eq!(blocks.first().unwrap().0, from);
    assert_eq!(blocks.last().unwrap().0, to);
    assert_eq!(fixture_u64(&post_batch_json, "rollup_id"), rollup);
    assert_eq!(
        oracle["outbound_effects"].as_array().unwrap().len(),
        2,
        "fixture must retain its two legacy outbound effects"
    );

    let calldata = fixture_hex(fixture_str(&post_batch_json, "abi_calldata"));
    let mut chunks = vec![ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id: rollup,
            from_block: from,
            to_block: to,
            post_batch: Some(PostBatch {
                abi_calldata: calldata,
                public_inputs_hash: fixture_hex(fixture_str(&oracle, "public_inputs_hash")),
                l1_block_hash: fixture_hex(fixture_str(&post_batch_json, "l1_block_hash")),
            }),
        })),
    }];
    for (number, encoded_rlp, encoded_witness) in &blocks {
        let rlp = fixture_hex(encoded_rlp);
        let block = alloy_rlp::decode_exact::<reth_ethereum_primitives::Block>(&rlp).unwrap();
        assert_eq!(block.header.number, *number);
        chunks.push(ProveChunk {
            kind: Some(prove_chunk::Kind::Block(BlockWitness {
                number: *number,
                hash: block.header.hash_slow().to_vec(),
                parent_hash: block.header.parent_hash.to_vec(),
                rlp,
                witness: Some(recorded_wire_witness(encoded_witness)),
            })),
        });
    }
    let settling_hash = match &chunks.last().unwrap().kind {
        Some(prove_chunk::Kind::Block(block)) => B256::from_slice(&block.hash),
        _ => unreachable!(),
    };
    assert_eq!(
        settling_hash,
        fixture_str(&oracle, "settling_block_hash")
            .parse::<B256>()
            .unwrap()
    );
    assert_eq!(
        fixture_str(&post_batch_json, "block_hash")
            .parse::<B256>()
            .unwrap(),
        settling_hash
    );

    let chain_config =
        serde_json::from_str(&fixture("nonzero-outbound-630", "chain-config.json")).unwrap();
    let validator = validate::Validator::stateless_for_test(chain_config, TEST_SYSTEM_ADDRESS);
    assert_eq!(validator.chain_id(), fixture_u64(&oracle, "l2_chain_id"));

    let mut assembler = crate::window::WindowAssembler::start(
        crate::window::WindowLimits {
            blocks: blocks.len(),
            payload_bytes: 1_000_000,
            witness_items: 10_000,
        },
        chunks[0].clone(),
    )
    .unwrap()
    .verify_rollup_identity(expected_rollup_id(rollup))
    .unwrap();
    for chunk in &chunks[1..] {
        assembler.push(chunk.clone()).unwrap();
    }
    let admitted = assembler.finish().unwrap();
    let (_, admitted_blocks) = admitted.into_parts();
    let validated = validator
        .validate_window(
            admitted_blocks,
            &CancellationToken::default(),
            oracle["transaction_state_checkpoints"]
                .as_array()
                .unwrap()
                .len(),
        )
        .unwrap();
    assert_eq!(
        validated.window_pre_state_root(),
        fixture_str(&oracle, "batch_anchor_root")
            .parse::<B256>()
            .unwrap()
    );
    assert_eq!(
        validated.window_post_state_root(),
        fixture_str(&oracle, "final_state_root")
            .parse::<B256>()
            .unwrap()
    );
    let settling_evidence = validated.settling_block().block().settlement_evidence();
    assert!(
        settling_evidence.observed_outbound_events().is_empty(),
        "the target six-field EEZL2 decoder must not accept captured five-field events"
    );
}
