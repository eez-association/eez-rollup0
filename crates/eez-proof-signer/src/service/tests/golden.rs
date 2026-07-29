//! Captured settlement and attestation regressions.

use alloy_primitives::{Address, B256, I256, Signature, U256, address, b256};
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, prove_chunk,
};
use reth_primitives_traits::{BlockBody as _, SignerRecoverable as _};

use super::{
    SettlementInput, TestServer, assert_attestation, checkpoint, expected_rollup_id,
    run_settlement, test_system_transaction_key, test_system_transaction_reconstructor,
};
use crate::cancel::CancellationToken;
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

fn fixture_i256(value: &serde_json::Value, field: &str) -> I256 {
    let value = value[field].as_i64().unwrap();
    let magnitude = I256::try_from(U256::from(value.unsigned_abs())).unwrap();
    if value.is_negative() {
        -magnitude
    } else {
        magnitude
    }
}

fn fixture_rlp_list<'a>(input: &mut &'a [u8]) -> &'a [u8] {
    alloy_rlp::Header::decode_bytes(input, true).unwrap()
}

fn fixture_rlp_bytes(input: &mut &[u8]) -> Vec<u8> {
    let mut encoded = fixture_rlp_list(input);
    let mut decoded = Vec::new();
    while !encoded.is_empty() {
        decoded.push(<u8 as alloy_rlp::Decodable>::decode(&mut encoded).unwrap());
    }
    decoded
}

fn fixture_da_payload_summary(da_payload: &[u8]) -> (Vec<usize>, usize, Vec<B256>) {
    let Some((&tag, mut payload)) = da_payload.split_first() else {
        panic!("fixture callData is empty");
    };
    assert_eq!(tag, 0);
    let mut body = fixture_rlp_list(&mut payload);
    assert!(payload.is_empty());
    let mut counts = fixture_rlp_list(&mut body);
    let mut transactions = fixture_rlp_list(&mut body);
    let mut entries = fixture_rlp_list(&mut body);
    assert!(body.is_empty());

    let mut block_counts = Vec::new();
    while !counts.is_empty() {
        block_counts.push(usize::from(
            <u16 as alloy_rlp::Decodable>::decode(&mut counts).unwrap(),
        ));
    }
    let mut transaction_count = 0;
    while !transactions.is_empty() {
        fixture_rlp_bytes(&mut transactions);
        transaction_count += 1;
    }
    let mut entry_hashes = Vec::new();
    while !entries.is_empty() {
        entry_hashes.push(alloy_primitives::keccak256(fixture_rlp_bytes(&mut entries)));
    }
    (block_counts, transaction_count, entry_hashes)
}

#[test]
fn real_successful_inbound_fixture_reaches_the_expected_hash_and_signature() {
    let post_batch = fixture_json("fresh-chain-inbound-2175", "postbatch.json");
    let oracle = fixture_json("fresh-chain-inbound-2175", "oracle.json");
    let fixture_blocks = fixture_json("fresh-chain-inbound-2175", "transaction-blocks.json");

    let expected_rollup = fixture_u64(&oracle, "expected_rollup_id");
    let from_block = fixture_u64(&oracle, "from_block");
    let to_block = fixture_u64(&oracle, "to_block");
    let expected_hash = fixture_str(&oracle, "public_inputs_hash");
    assert_eq!(fixture_u64(&post_batch, "rollup_id"), expected_rollup);
    assert_eq!(fixture_u64(&post_batch, "entry_count"), 4);
    assert_eq!(
        fixture_str(&post_batch, "public_inputs_hash"),
        expected_hash
    );
    assert_eq!(fixture_u64(&oracle, "l2_chain_id"), 1);

    let fixture_blocks = fixture_blocks.as_array().unwrap();
    let mut captured = std::collections::BTreeMap::new();
    let mut captured_intermediate_transactions = 0;
    for block in fixture_blocks {
        let number = fixture_u64(block, "number");
        let rlp = fixture_hex(fixture_str(block, "rlp"));
        let decoded = alloy_rlp::decode_exact::<reth_ethereum_primitives::Block>(&rlp).unwrap();
        assert_eq!(decoded.header.number, number);
        if number != to_block {
            captured_intermediate_transactions += decoded.body.transactions.len();
        }
        assert!(
            captured.insert(number, rlp).is_none(),
            "duplicate fixture block"
        );
    }
    assert_eq!(captured_intermediate_transactions, 27);
    let empty_body: reth_ethereum_primitives::BlockBody = Default::default();
    let empty_block = alloy_rlp::encode(reth_ethereum_primitives::Block::new(
        Default::default(),
        empty_body,
    ));
    let mut blocks = (from_block..=to_block)
        .map(|number| {
            // Empty-body positions need no captured header here: this fixture
            // starts after Stateless and exercises only settlement's exact
            // ordered transaction projection.
            let rlp = captured
                .remove(&number)
                .unwrap_or_else(|| empty_block.clone());
            let settlement_evidence = validate::SettlementBlockEvidence::from_rlp_for_test(&rlp);
            validate::ValidatedBlock::for_test(number, rlp, settlement_evidence)
        })
        .collect::<Vec<_>>();
    assert!(
        captured.is_empty(),
        "fixture contains a block outside its window"
    );

    let settling_block = blocks.pop().unwrap();
    assert_eq!(settling_block.number(), to_block);
    let decoded_settling =
        alloy_rlp::decode_exact::<reth_ethereum_primitives::Block>(settling_block.rlp()).unwrap();
    assert_eq!(
        decoded_settling.header.hash_slow(),
        fixture_str(&oracle, "settling_block_hash")
            .parse::<B256>()
            .unwrap()
    );
    assert_eq!(
        decoded_settling
            .body
            .transactions
            .iter()
            .map(alloy_consensus::Transaction::nonce)
            .collect::<Vec<_>>(),
        [90, 91, 92]
    );
    assert!(
        decoded_settling
            .body
            .transactions
            .iter()
            .all(
                |transaction| transaction.recover_signer().unwrap() == eez_protocol::SYSTEM_ADDRESS
            )
    );
    assert_eq!(
        decoded_settling
            .body
            .encoded_2718_transactions_iter()
            .map(|transaction| alloy_primitives::keccak256(&transaction))
            .collect::<Vec<_>>(),
        oracle["system_transaction_hashes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hash| hash.as_str().unwrap().parse::<B256>().unwrap())
            .collect::<Vec<_>>()
    );

    let expected_rollup_id = expected_rollup_id(expected_rollup);
    let proof_system_vkey =
        crate::attest::NonZeroProofSystemVkey::new(fixture_str(&oracle, "vkey").parse().unwrap())
            .unwrap();
    let proof_system = fixture_str(&oracle, "proof_system").parse().unwrap();
    let checkpoints = oracle["transaction_state_checkpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|recorded| {
            checkpoint(
                usize::try_from(fixture_u64(recorded, "transaction_index")).unwrap(),
                fixture_str(recorded, "state_root").parse().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let receipt_successes = oracle["transaction_statuses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|status| status.as_bool().unwrap())
        .collect::<Vec<_>>();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id);
    let cancellation = CancellationToken::default();
    let calldata = fixture_hex(fixture_str(&post_batch, "abi_calldata"));
    let batch = settlement::decode_canonical_post_batch(calldata.clone()).unwrap();
    let (block_counts, transaction_count, l2_entry_hashes) =
        fixture_da_payload_summary(&batch.callData);
    assert_eq!(
        block_counts.len(),
        usize::try_from(to_block - from_block + 1).unwrap()
    );
    assert_eq!(block_counts.iter().sum::<usize>(), 27);
    assert_eq!(transaction_count, 27);
    assert_eq!(
        l2_entry_hashes,
        oracle["l2_entry_hashes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hash| hash.as_str().unwrap().parse::<B256>().unwrap())
            .collect::<Vec<_>>()
    );
    let validated = validate::ValidatedWindow::for_test(
        fixture_str(&oracle, "batch_anchor_root").parse().unwrap(),
        fixture_str(&oracle, "pre_settling_root").parse().unwrap(),
        fixture_str(&oracle, "final_state_root").parse().unwrap(),
        blocks,
        validate::ValidatedSettlingBlock::for_test(settling_block, receipt_successes, checkpoints),
    );
    let recomputed_public_inputs_hash = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata,
        validated_window: &validated,
        expected_rollup_id,
        proof_system_vkey,
        expected_proof_system: proof_system,
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    })
    .unwrap();
    let recomputed_hash = recomputed_public_inputs_hash.into_inner();
    assert_eq!(recomputed_hash, expected_hash.parse::<B256>().unwrap());

    // Sign only after the captured settlement vector has passed every gate.
    // The intentionally public test key is not part of the recording.
    let attester = crate::attest::Attester::new(
        b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"),
        proof_system_vkey,
        proof_system,
    )
    .unwrap();
    let signature_bytes = attester.sign(recomputed_public_inputs_hash).unwrap();
    let signature = Signature::try_from(signature_bytes.as_ref()).unwrap();
    let expected_attester = address!("70997970c51812dc3a010c7d01b50e0d17dc79c8");
    assert_eq!(attester.address(), expected_attester);
    assert_eq!(
        signature
            .recover_address_from_prehash(&recomputed_hash)
            .unwrap(),
        expected_attester
    );
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

#[tokio::test]
async fn real_nonzero_outbound_fixture_reaches_the_expected_hash_and_signature() {
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
    assert_eq!(fixture_u64(&post_batch_json, "entry_count"), 4);
    assert_eq!(
        fixture_str(&post_batch_json, "current_state"),
        fixture_str(&oracle, "batch_anchor_root")
    );
    assert_eq!(
        fixture_str(&post_batch_json, "new_state"),
        fixture_str(&oracle, "final_state_root")
    );

    let calldata = fixture_hex(fixture_str(&post_batch_json, "abi_calldata"));
    let batch = settlement::decode_canonical_post_batch(calldata.clone()).unwrap();
    assert_eq!(
        batch.entries.len(),
        usize::try_from(fixture_u64(&oracle, "entry_count")).unwrap()
    );
    let proof_system = fixture_str(&oracle, "proof_system")
        .parse::<Address>()
        .unwrap();
    assert_eq!(batch.proofSystems, [proof_system]);
    let proof_system_vkey =
        crate::attest::NonZeroProofSystemVkey::new(fixture_str(&oracle, "vkey").parse().unwrap())
            .unwrap();
    let expected_hash = fixture_str(&oracle, "public_inputs_hash")
        .parse::<B256>()
        .unwrap();
    assert_eq!(
        fixture_str(&post_batch_json, "public_inputs_hash")
            .parse::<B256>()
            .unwrap(),
        expected_hash,
    );
    let recomputed_hash = settlement::recompute_public_input_hash(
        &batch,
        proof_system_vkey,
        expected_rollup_id(rollup),
        proof_system,
    )
    .unwrap();
    assert_eq!(recomputed_hash, expected_hash);

    let recorded_outbound = oracle["outbound_effects"].as_array().unwrap();
    assert_eq!(recorded_outbound.len(), 2);
    assert_eq!(
        batch
            .entries
            .iter()
            .flat_map(|entry| &entry.l2ToL1Calls)
            .filter(|call| !call.value.is_zero())
            .count(),
        1,
    );
    for effect in recorded_outbound {
        let entry_index = usize::try_from(fixture_u64(effect, "entry_index")).unwrap();
        let [delta] = batch.entries[entry_index].stateDeltas.as_slice() else {
            panic!("recorded outbound entry must have one state delta");
        };
        let [call] = batch.entries[entry_index].l2ToL1Calls.as_slice() else {
            panic!("recorded outbound entry must have one call");
        };
        assert_eq!(call.value, U256::from(fixture_u64(effect, "value")));
        assert_eq!(delta.etherDelta, fixture_i256(effect, "ether_delta"));
        assert_eq!(
            call.targetAddress,
            fixture_str(effect, "target").parse::<Address>().unwrap()
        );
        assert_eq!(
            call.sourceAddress,
            fixture_str(effect, "source").parse::<Address>().unwrap()
        );
        assert_eq!(
            eez_protocol::cross_chain_call_hash(
                eez_protocol::RollupId::MAINNET,
                call.targetAddress,
                call.value,
                &call.data,
                call.sourceAddress,
                eez_protocol::RollupId(rollup),
            ),
            fixture_str(effect, "call_hash").parse::<B256>().unwrap()
        );
    }
    let inbound = &oracle["inbound_effect"];
    let inbound_entry =
        &batch.entries[usize::try_from(fixture_u64(inbound, "entry_index")).unwrap()];
    assert_eq!(
        inbound_entry.proxyEntryHash,
        fixture_str(inbound, "call_hash").parse::<B256>().unwrap()
    );
    let [inbound_delta] = inbound_entry.stateDeltas.as_slice() else {
        panic!("recorded inbound entry must have one state delta");
    };
    assert_eq!(
        inbound_delta.etherDelta,
        fixture_i256(inbound, "ether_delta")
    );

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
    let validator = validate::Validator::stateless_for_test(chain_config);
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
    assert_eq!(
        validated.settling_pre_state_root(),
        fixture_str(&oracle, "pre_settling_root")
            .parse::<B256>()
            .unwrap()
    );
    let settling_block = validated.settling_block();
    assert_eq!(
        settling_block.receipt_successes(),
        oracle["transaction_statuses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|status| status.as_bool().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        settling_block.transaction_state_checkpoints(),
        oracle["transaction_state_checkpoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|recorded| checkpoint(
                usize::try_from(fixture_u64(recorded, "transaction_index")).unwrap(),
                fixture_str(recorded, "state_root").parse().unwrap(),
            ))
            .collect::<Vec<_>>()
    );
    let settling_evidence = validated.settling_block().block().settlement_evidence();
    assert_eq!(
        settling_evidence.system_sender_flags(),
        oracle["system_senders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|flag| flag.as_bool().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        settling_evidence
            .observed_outbound_events()
            .iter()
            .map(|observation| (
                observation.transaction_index(),
                observation.decoded_call_hash().unwrap()
            ))
            .collect::<Vec<_>>(),
        recorded_outbound
            .iter()
            .map(|effect| (
                usize::try_from(fixture_u64(effect, "transaction_index")).unwrap(),
                fixture_str(effect, "call_hash").parse::<B256>().unwrap(),
            ))
            .collect::<Vec<_>>()
    );

    let attester = crate::attest::Attester::new(
        b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"),
        proof_system_vkey,
        proof_system,
    )
    .unwrap();
    let expected_attester = fixture_str(&oracle, "expected_test_attester")
        .parse::<Address>()
        .unwrap();
    assert_eq!(attester.address(), expected_attester);
    let server = TestServer::new(std::sync::Arc::new(crate::service::ServiceState::new(
        validator,
        expected_rollup_id(rollup),
        attester,
        test_system_transaction_key(),
    )))
    .await;

    let response = server.attest(chunks).await;
    assert_attestation(&response, expected_hash, expected_attester);
}
