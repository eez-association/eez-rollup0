//! Captured migration-boundary regressions.

use alloy_primitives::B256;
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, prove_chunk,
};

use super::expected_rollup_id;
use crate::cancel::CancellationToken;
use crate::testkit::TEST_SYSTEM_ADDRESS;
use crate::{settlement, validate};

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
