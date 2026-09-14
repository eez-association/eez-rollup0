use alloy_consensus::{EthereumReceipt, Header, SignableTransaction as _, TxLegacy};
use alloy_primitives::{B256, Bytes, Log, Signature, TxKind, U256, b256};
use alloy_sol_types::SolEvent as _;
use eez_proof_signer::EEZL2_ADDRESS;
use eez_proof_signer::validate::{DecodedOutboundEvent, OutboundEventObservation};
use eez_proof_signer::window::testing::admitted_block_with_witness;
use eez_protocol::abi::eez_l2_events::CrossChainCallExecuted;
use reth_ethereum_primitives::TransactionSigned;
use reth_primitives_traits::SignerRecoverable as _;

use super::*;
use crate::testkit::{SYSTEM_TX, TEST_SYSTEM_ADDRESS};

fn fixture_chain_config() -> ChainConfig {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-block-13/chain-config.json"
    )))
    .unwrap()
}

fn fixture_input() -> AdmittedBlock {
    let rlp = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-block-13/block-13.rlp"
    ))
    .to_vec();
    let block = alloy_rlp::decode_exact::<Block>(&rlp).unwrap();
    let witness = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-block-13/witness-13.json"
    )))
    .unwrap();
    admitted_block_with_witness(
        block.header.number,
        block.header.hash_slow(),
        block.header.parent_hash,
        rlp,
        witness,
    )
}

fn checkpoint_fixture() -> (AdmittedBlock, ChainConfig, Vec<TransactionStateCheckpoint>) {
    let rlp = hex::decode(
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/stateless-checkpoint-2175/checkpoint-block-2175.rlp.hex"
        ))
        .trim()
        .trim_start_matches("0x"),
    )
    .unwrap();
    let block = alloy_rlp::decode_exact::<Block>(&rlp).unwrap();
    let witness = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-checkpoint-2175/checkpoint-witness-2175.json"
    )))
    .unwrap();
    let chain_config = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-checkpoint-2175/checkpoint-chain-config.json"
    )))
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/stateless-checkpoint-2175/checkpoint-oracle-2175.json"
    )))
    .unwrap();
    let indices = oracle["expected_checkpoint_indices"].as_array().unwrap();
    let roots = oracle["transaction_state_roots"].as_array().unwrap();
    assert_eq!(indices.len(), roots.len());
    let checkpoints = indices
        .iter()
        .zip(roots)
        .map(
            |(transaction_index, state_root)| TransactionStateCheckpoint {
                transaction_index: usize::try_from(transaction_index.as_u64().unwrap()).unwrap(),
                state_root: state_root.as_str().unwrap().parse::<B256>().unwrap(),
            },
        )
        .collect();

    (
        admitted_block_with_witness(
            block.header.number,
            block.header.hash_slow(),
            block.header.parent_hash,
            rlp,
            witness,
        ),
        chain_config,
        checkpoints,
    )
}

fn captured_fixture(dir: &str, name: &str) -> String {
    let path = format!("{}/tests/fixtures/{dir}/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read fixture {path}: {error}"))
}

fn captured_hex(value: &str) -> Vec<u8> {
    let value = value.trim();
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).unwrap()
}

fn captured_witness(encoded: &str) -> alloy_rpc_types_debug::ExecutionWitness {
    let witness: serde_json::Value = serde_json::from_str(encoded).unwrap();
    let items = |field: &str| {
        witness[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| captured_hex(item.as_str().unwrap()).into())
            .collect()
    };
    alloy_rpc_types_debug::ExecutionWitness {
        state: items("state"),
        codes: items("codes"),
        keys: items("keys"),
        headers: items("headers"),
    }
}

fn high_s_system_transaction() -> TransactionSigned {
    let transaction: TransactionSigned =
        alloy_rlp::decode_exact(hex::decode(SYSTEM_TX).unwrap()).unwrap();
    let signature = transaction.signature();
    let curve_order = U256::from_str_radix(
        "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
        16,
    )
    .unwrap();
    let high_s = Signature::new(signature.r(), curve_order - signature.s(), !signature.v());
    TransactionSigned::new_unhashed(transaction.into_typed_transaction(), high_s)
}

fn checkpoint(transaction_index: usize) -> TransactionStateCheckpoint {
    TransactionStateCheckpoint {
        transaction_index,
        state_root: B256::ZERO,
    }
}

fn outbound_log(address: Address, call_hash: B256) -> Log {
    outbound_log_with_gas(address, call_hash, 0)
}

fn outbound_log_with_gas(address: Address, call_hash: B256, call_gas: u64) -> Log {
    Log {
        address,
        data: CrossChainCallExecuted {
            crossChainCallHash: call_hash,
            proxy: Address::ZERO,
            sourceAddress: Address::repeat_byte(0x11),
            callData: Bytes::from_static(&[0xaa, 0xbb]),
            value: U256::from(7),
            callGas: call_gas,
        }
        .encode_log_data(),
    }
}

fn receipt_with_logs(logs: Vec<Log>) -> EthereumReceipt {
    EthereumReceipt {
        success: true,
        logs,
        ..Default::default()
    }
}

#[test]
fn outbound_observations_require_the_eezl2_emitter_and_event_signature() {
    let call_hash = B256::repeat_byte(0x11);
    let other = Address::repeat_byte(0x22);
    let logs = vec![
        outbound_log(other, call_hash),
        Log::new_unchecked(
            EEZL2_ADDRESS,
            vec![B256::repeat_byte(0xff), call_hash],
            Bytes::new(),
        ),
        outbound_log(EEZL2_ADDRESS, call_hash),
    ];

    assert_eq!(
        observe_outbound_events(&[receipt_with_logs(logs)]),
        [OutboundEventObservation {
            transaction_index: 0,
            receipt_log_index: 2,
            decoded_event: Some(DecodedOutboundEvent::new(call_hash, 0)),
        }]
    );
}

#[test]
fn malformed_named_outbound_events_are_retained_without_decoded_fields() {
    let call_hash = B256::repeat_byte(0x33);
    let mut topic0_only = outbound_log(EEZL2_ADDRESS, call_hash);
    topic0_only.data.topics_mut_unchecked().truncate(1);
    let mut missing_proxy = outbound_log(EEZL2_ADDRESS, call_hash);
    missing_proxy.data.topics_mut_unchecked().truncate(2);
    let mut missing_body = outbound_log(EEZL2_ADDRESS, call_hash);
    missing_body.data.data = Bytes::new();
    let mut trailing_body = outbound_log(EEZL2_ADDRESS, call_hash);
    let mut body = trailing_body.data.data.to_vec();
    body.push(0);
    trailing_body.data.data = Bytes::from(body);
    // Alloy can decode the declared tuple while ignoring the suffix. The
    // adapter's exact re-encode comparison is what rejects this candidate.
    assert!(CrossChainCallExecuted::decode_log_validate(&trailing_body).is_ok());

    assert_eq!(
        observe_outbound_events(&[receipt_with_logs(vec![
            topic0_only,
            missing_proxy,
            missing_body,
            trailing_body,
        ])]),
        [
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 0,
                decoded_event: None
            },
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 1,
                decoded_event: None
            },
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 2,
                decoded_event: None
            },
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 3,
                decoded_event: None
            },
        ]
    );
}

#[test]
fn outbound_observations_preserve_receipt_log_order_and_duplicates() {
    let a = B256::repeat_byte(0xaa);
    let b = B256::repeat_byte(0xbb);
    let noise = Log::new_unchecked(Address::ZERO, Vec::new(), Bytes::new());
    let observations = observe_outbound_events(&[
        receipt_with_logs(vec![
            outbound_log(EEZL2_ADDRESS, a),
            outbound_log(EEZL2_ADDRESS, a),
        ]),
        receipt_with_logs(vec![
            noise,
            outbound_log_with_gas(EEZL2_ADDRESS, b, u64::MAX),
            outbound_log(EEZL2_ADDRESS, a),
        ]),
    ]);

    assert_eq!(
        observations,
        [
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 0,
                decoded_event: Some(DecodedOutboundEvent::new(a, 0))
            },
            OutboundEventObservation {
                transaction_index: 0,
                receipt_log_index: 1,
                decoded_event: Some(DecodedOutboundEvent::new(a, 0))
            },
            OutboundEventObservation {
                transaction_index: 1,
                receipt_log_index: 1,
                decoded_event: Some(DecodedOutboundEvent::new(b, u64::MAX))
            },
            OutboundEventObservation {
                transaction_index: 1,
                receipt_log_index: 2,
                decoded_event: Some(DecodedOutboundEvent::new(a, 0))
            },
        ]
    );
}

#[test]
fn checkpoint_response_must_match_the_plan_exactly() {
    let plan = CheckpointPlan::new(vec![0, 2]);
    assert!(
        plan.verify_returned(&[checkpoint(0), checkpoint(2)])
            .is_ok()
    );

    for returned in [
        vec![checkpoint(0)],
        vec![checkpoint(0), checkpoint(1), checkpoint(2)],
        vec![checkpoint(2), checkpoint(0)],
    ] {
        assert!(matches!(
            plan.verify_returned(&returned),
            Err(ValidationError::InvalidBackendOutput(_))
        ));
    }
}

#[test]
fn checkpoint_plan_is_derived_from_recovered_transactions() {
    let transaction = || {
        TxLegacy {
            to: TxKind::Call(EEZL2_ADDRESS),
            ..Default::default()
        }
        .into_signed(alloy_primitives::Signature::test_signature())
        .into()
    };
    let block = Block::new(
        Default::default(),
        alloy_consensus::BlockBody {
            transactions: vec![transaction(), transaction(), transaction()],
            ..Default::default()
        },
    );
    let recovered = RecoveredBlock::new_unhashed(
        block,
        vec![TEST_SYSTEM_ADDRESS, Address::ZERO, TEST_SYSTEM_ADDRESS],
    );

    let (plan, system_sender_flags) =
        CheckpointPlan::from_recovered_block(&recovered, TEST_SYSTEM_ADDRESS);

    assert_eq!(system_sender_flags, [true, false, true]);
    assert_eq!(plan.transaction_indices(), [1, 2]);
}

#[test]
fn checkpoint_plan_includes_every_inbound_boundary() {
    let transaction = || {
        TxLegacy {
            to: TxKind::Call(EEZL2_ADDRESS),
            ..Default::default()
        }
        .into_signed(alloy_primitives::Signature::test_signature())
        .into()
    };

    for transaction_count in [8, 9, 64, 65] {
        let block = Block::new(
            Default::default(),
            alloy_consensus::BlockBody {
                transactions: std::iter::repeat_with(transaction)
                    .take(transaction_count)
                    .collect(),
                ..Default::default()
            },
        );
        let recovered =
            RecoveredBlock::new_unhashed(block, vec![TEST_SYSTEM_ADDRESS; transaction_count]);

        let (plan, system_sender_flags) =
            CheckpointPlan::from_recovered_block(&recovered, TEST_SYSTEM_ADDRESS);

        assert_eq!(system_sender_flags, vec![true; transaction_count]);
        assert_eq!(
            plan.transaction_indices(),
            (0..transaction_count).collect::<Vec<_>>()
        );
    }
}

#[test]
fn sender_classification_uses_the_configured_system_address() {
    let transaction = TxLegacy::default()
        .into_signed(alloy_primitives::Signature::test_signature())
        .into();
    let block = Block::new(
        Default::default(),
        alloy_consensus::BlockBody {
            transactions: vec![transaction],
            ..Default::default()
        },
    );
    let recovered = RecoveredBlock::new_unhashed(block, vec![TEST_SYSTEM_ADDRESS]);

    assert_eq!(system_sender_flags(&recovered, TEST_SYSTEM_ADDRESS), [true]);
    assert_eq!(
        system_sender_flags(&recovered, Address::repeat_byte(0xbb)),
        [false]
    );
}

#[test]
fn recovered_sender_facts_follow_the_homestead_signature_rule() {
    let transaction = high_s_system_transaction();
    assert!(transaction.recover_signer().is_err());
    assert_eq!(
        transaction.recover_signer_unchecked().unwrap(),
        TEST_SYSTEM_ADDRESS
    );
    let header = Header {
        number: 7,
        ..Default::default()
    };
    let block = Block::new(
        header,
        alloy_consensus::BlockBody {
            transactions: vec![transaction],
            ..Default::default()
        },
    );

    let pre_homestead = ChainSpec::from_genesis(Genesis {
        config: ChainConfig {
            homestead_block: Some(8),
            ..Default::default()
        },
        ..Default::default()
    });
    let admitted = admitted_block_with_witness(
        block.header.number,
        block.header.hash_slow(),
        block.header.parent_hash,
        alloy_rlp::encode(block.clone()),
        Default::default(),
    );
    let recovered = decode_match_and_recover_signers(&admitted, &pre_homestead).unwrap();
    assert_eq!(system_sender_flags(&recovered, TEST_SYSTEM_ADDRESS), [true]);

    let post_homestead = ChainSpec::from_genesis(Genesis {
        config: ChainConfig {
            homestead_block: Some(7),
            ..Default::default()
        },
        ..Default::default()
    });
    assert!(decode_match_and_recover_signers(&admitted, &post_homestead).is_err());
}

#[test]
fn validates_the_golden_block_through_stateless() {
    let output = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS)
        .validate(vec![fixture_input()])
        .unwrap();
    let root = b256!("f09d8f7da5bc5036f8dd9536c953e2212390a46fb3e553ece2b7d419131537b1");

    assert_eq!(output.pre_state_root, root);
    assert_eq!(output.blocks.len(), 1);
    let block = &output.blocks[0];
    assert_eq!(
        block.computed_hash,
        b256!("16b64a78e9b3e0d533cafe81f9121735f6a2c8122c69b0bb5994ee75fe7bface")
    );
    assert_eq!(block.post_state_root, root);
    assert!(block.receipt_successes.is_empty());
    assert!(block.transaction_state_checkpoints.is_empty());
    assert!(block.settlement_evidence.system_sender_flags.is_empty());
    assert!(
        block
            .settlement_evidence
            .observed_outbound_events
            .is_empty()
    );
}

#[test]
fn selected_checkpoints_flow_through_the_stateless_adapter() {
    let (mut input, chain_config, expected) = checkpoint_fixture();
    let expected_hash = input.claimed_hash();

    let output = Backend::new(chain_config, TEST_SYSTEM_ADDRESS)
        .validate_admitted(
            std::slice::from_mut(&mut input),
            &CancellationToken::default(),
        )
        .unwrap();

    let block = &output.blocks[0];
    assert_eq!(
        block.settlement_evidence.system_sender_flags,
        [true, true, true]
    );
    assert!(
        block
            .settlement_evidence
            .observed_outbound_events
            .is_empty()
    );
    assert_eq!(block.computed_hash, expected_hash);
    assert_eq!(block.transaction_state_checkpoints, expected);
}

#[test]
fn captured_legacy_outbound_events_are_not_accepted_as_current_events() {
    const FIXTURE: &str = "nonzero-outbound-630";
    let oracle: serde_json::Value =
        serde_json::from_str(&captured_fixture(FIXTURE, "oracle.json")).unwrap();
    let chain_config =
        serde_json::from_str(&captured_fixture(FIXTURE, "chain-config.json")).unwrap();
    let from = oracle["from_block"].as_u64().unwrap();
    let to = oracle["to_block"].as_u64().unwrap();
    let mut blocks = (from..=to)
        .map(|number| {
            let rlp = captured_hex(&captured_fixture(
                FIXTURE,
                &format!("block-{number}.rlp.hex"),
            ));
            let block = alloy_rlp::decode_exact::<Block>(&rlp).unwrap();
            admitted_block_with_witness(
                number,
                block.header.hash_slow(),
                block.header.parent_hash,
                rlp,
                captured_witness(&captured_fixture(
                    FIXTURE,
                    &format!("witness-{number}.json"),
                )),
            )
        })
        .collect::<Vec<_>>();

    let output = Backend::new(chain_config, TEST_SYSTEM_ADDRESS)
        .validate_admitted(&mut blocks, &CancellationToken::default())
        .unwrap();

    assert_eq!(
        output.pre_state_root,
        oracle["batch_anchor_root"]
            .as_str()
            .unwrap()
            .parse::<B256>()
            .unwrap()
    );
    assert_eq!(
        output.blocks.last().unwrap().post_state_root,
        oracle["final_state_root"]
            .as_str()
            .unwrap()
            .parse::<B256>()
            .unwrap()
    );
    assert!(
        output
            .blocks
            .last()
            .unwrap()
            .settlement_evidence
            .observed_outbound_events
            .is_empty()
    );
}

#[test]
fn pre_cancelled_validation_does_not_consume_the_witness() {
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let mut input = fixture_input();
    let state_items = admitted_block_parts_mut(&mut input).witness.state.len();

    let result = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS)
        .validate_admitted(std::slice::from_mut(&mut input), &cancellation);

    assert!(matches!(result, Err(ValidationError::Cancelled)));
    assert_eq!(
        admitted_block_parts_mut(&mut input).witness.state.len(),
        state_items
    );
}

#[test]
fn consensus_rlp_must_decode_exactly() {
    let mut input = fixture_input();
    admitted_block_parts_mut(&mut input).rlp.push(0x80);
    assert!(matches!(
        Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS).validate(vec![input]),
        Err(ValidationError::Rejected(_))
    ));
}

#[test]
fn decoded_header_must_match_the_streamed_number_and_parent() {
    let mutations: [fn(&mut AdmittedBlock); 2] = [
        |input| *admitted_block_parts_mut(input).declared_number += 1,
        |input| {
            *admitted_block_parts_mut(input).claimed_parent_hash = B256::repeat_byte(0xee);
        },
    ];
    for mutate in mutations {
        let mut input = fixture_input();
        mutate(&mut input);
        assert!(matches!(
            Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS).validate(vec![input]),
            Err(ValidationError::Rejected(_))
        ));
    }
}

#[test]
fn decoded_header_hash_must_match_the_streamed_hash_before_reexecution() {
    let mut input = fixture_input();
    let parts = admitted_block_parts_mut(&mut input);
    *parts.claimed_hash = B256::repeat_byte(0xee);
    let mut node = parts.witness.state[0].to_vec();
    node[0] ^= 0x01;
    parts.witness.state[0] = node.into();
    let error = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS)
        .validate(vec![input])
        .unwrap_err();
    assert!(matches!(error, ValidationError::Rejected(_)));
    assert!(
        error
            .to_string()
            .contains("does not match streamed block hash")
    );
}

#[test]
fn execution_must_produce_the_state_root_committed_by_the_header() {
    let mut input = fixture_input();
    let mut block = alloy_rlp::decode_exact::<Block>(input.rlp()).unwrap();
    block.header.state_root = B256::repeat_byte(0xee);
    let parts = admitted_block_parts_mut(&mut input);
    *parts.claimed_hash = block.header.hash_slow();
    *parts.rlp = alloy_rlp::encode(block);

    let error = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS)
        .validate(vec![input])
        .unwrap_err();

    assert!(matches!(
        error,
        ValidationError::Rejected(reason) if reason.contains("mismatched post-state root")
    ));
}

#[test]
fn corrupt_witness_data_is_rejected_by_stateless() {
    let mut input = fixture_input();
    let parts = admitted_block_parts_mut(&mut input);
    let mut node = parts.witness.state[0].to_vec();
    node[0] ^= 0x01;
    parts.witness.state[0] = node.into();
    assert!(matches!(
        Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS).validate(vec![input]),
        Err(ValidationError::Rejected(_))
    ));
}
