mod native_security;

use alloy_consensus::{Header, SignableTransaction as _, TxLegacy};
use alloy_primitives::{B256, Bytes, Log, Signature, U256, b256};
use alloy_sol_types::SolEvent as _;
use eez_primitives::{EezTxEnvelope as TransactionSigned, Receipt as EthereumReceipt};
use eez_proof_signer::EEZL2_ADDRESS;
use eez_proof_signer::validate::{CheckpointAt, DecodedOutboundEvent, OutboundEventObservation};
use eez_proof_signer::window::testing::admitted_block_with_witness;
use eez_protocol::abi::eez_l2_events::CrossChainCallExecuted;
use reth_primitives_traits::SignerRecoverable as _;

use super::*;
use crate::testkit::{SYSTEM_TX, TEST_SYSTEM_ADDRESS};
use eez_primitives::Block;
use reth_primitives_traits::RecoveredBlock;

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

fn incremental_input(
    mut block: AdmittedBlock,
) -> (AdmittedBlock, ExecutionWitness, IncrementalAnchor) {
    let witness = std::mem::take(admitted_block_parts_mut(&mut block).witness);
    let header = witness
        .headers
        .iter()
        .filter_map(|bytes| alloy_rlp::decode_exact::<Header>(bytes).ok())
        .find(|header| header.hash_slow() == block.claimed_parent_hash())
        .expect("fixture includes its parent header");
    let parent = IncrementalAnchor {
        number: header.number,
        hash: header.hash_slow(),
        state_root: header.state_root,
    };
    (block, witness, parent)
}

#[tokio::test]
async fn incremental_ordinal_execution_skips_checkpoints_and_checks_cache_hits() {
    // A real three-user-transaction block, not an empty block that would skip
    // checkpoint work even before the v2 selection fix.
    let (input, config) = checkpoint_fixture();
    let (input, witness, parent) = incremental_input(input);
    let backend = Backend::new(config, TEST_SYSTEM_ADDRESS);
    let deadline = Duration::from_secs(10);
    // A real miss needs a valid witness; failure must not poison the cache.
    assert!(
        backend
            .validate_next(parent, &input, ExecutionWitness::default(), deadline)
            .await
            .is_err()
    );
    // Successful replay must still bind to the supplied parent's state root.
    let wrong_parent = IncrementalAnchor {
        state_root: !parent.state_root,
        ..parent
    };
    assert!(matches!(
        backend
            .validate_next(wrong_parent, &input, witness.clone(), deadline)
            .await,
        Err(ValidationError::Rejected(reason)) if reason.contains("do not telescope")
    ));
    assert!(backend.blocks.lock().unwrap().is_empty());
    let (fresh, reused) = backend
        .validate_next(parent, &input, witness, deadline)
        .await
        .unwrap();
    assert!(!reused);
    assert_eq!(fresh.decoded().body.transactions.len(), 3);
    assert_eq!(fresh.settlement_evidence().system_sender_flags, [false; 3]);
    assert!(fresh.transaction_state_checkpoints().is_empty());
    // No witness is available to re-execute, so success proves backend cache reuse.
    let (cached, reused) = backend
        .validate_next(parent, &input, ExecutionWitness::default(), deadline)
        .await
        .unwrap();
    assert!(reused);
    assert_eq!(cached, fresh);
    for defect in [
        "number",
        "parent hash",
        "RLP",
        "parent number",
        "parent root",
    ] {
        let mut submitted = input.clone();
        let mut submitted_parent = parent;
        match defect {
            "number" => *admitted_block_parts_mut(&mut submitted).declared_number += 1,
            "parent hash" => {
                *admitted_block_parts_mut(&mut submitted).claimed_parent_hash = B256::ZERO
            }
            "RLP" => admitted_block_parts_mut(&mut submitted).rlp.push(0),
            "parent number" => submitted_parent.number -= 1,
            "parent root" => submitted_parent.state_root = B256::ZERO,
            _ => unreachable!(),
        }
        assert!(
            backend
                .validate_next(
                    submitted_parent,
                    &submitted,
                    ExecutionWitness::default(),
                    deadline
                )
                .await
                .is_err(),
            "{defect}"
        );
    }
    assert_eq!(backend.blocks.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn sync_execution_generates_and_reuses_checkpoints() {
    use alloy_consensus::TxReceipt as _;
    use alloy_consensus::proofs::{
        calculate_receipt_root, calculate_transaction_root, calculate_withdrawals_root,
    };
    use alloy_primitives::{KECCAK256_EMPTY, keccak256};
    use eez_primitives::{EezTxType, SystemTransaction};

    // Execute a native call from an empty state. Its only persistent change is
    // SYSTEM_ADDRESS's nonce becoming one; independently seal that account's
    // single-leaf MPT to supply the expected post-state, not a mocked backend root.
    let account = alloy_rlp::encode(vec![
        Bytes::from_static(&[1]),
        Bytes::new(),
        Bytes::copy_from_slice(alloy_consensus::constants::EMPTY_ROOT_HASH.as_slice()),
        Bytes::copy_from_slice(KECCAK256_EMPTY.as_slice()),
    ]);
    let mut path = vec![0x20]; // Hex-prefix encoding of a leaf with an even, full-length key.
    path.extend_from_slice(keccak256(TEST_SYSTEM_ADDRESS).as_slice());
    let post_root = keccak256(alloy_rlp::encode(vec![
        Bytes::from(path),
        Bytes::from(account),
    ]));
    let parent = Header {
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        withdrawals_root: Some(calculate_withdrawals_root(&[])),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        ..Default::default()
    };
    let transactions = vec![
        SystemTransaction {
            chain_id: 1,
            nonce: 0,
            to: EEZL2_ADDRESS,
            value: U256::ZERO,
            input: Bytes::new(),
        }
        .into(),
    ];
    let receipt = EthereumReceipt {
        tx_type: EezTxType::System,
        success: true,
        cumulative_gas_used: 21_000,
        logs: Vec::new(),
    };
    let block = Block::new(
        Header {
            parent_hash: parent.hash_slow(),
            number: 1,
            timestamp: 1,
            state_root: post_root,
            transactions_root: calculate_transaction_root(&transactions),
            receipts_root: calculate_receipt_root(&[receipt.with_bloom_ref()]),
            gas_used: 21_000,
            ..parent.clone()
        },
        eez_primitives::BlockBody {
            transactions,
            withdrawals: Some(Default::default()),
            ..Default::default()
        },
    );
    let input = admitted_block_with_witness(
        1,
        block.header.hash_slow(),
        parent.hash_slow(),
        alloy_rlp::encode(block),
        ExecutionWitness {
            state: vec![Bytes::from_static(&[0x80])],
            headers: vec![alloy_rlp::encode(&parent).into()],
            ..Default::default()
        },
    );
    let mut config = fixture_chain_config();
    config.prague_time = None;
    config.osaka_time = None;
    let backend = Backend::new(config, TEST_SYSTEM_ADDRESS);
    let v1 = backend.validate(vec![input.clone()]).unwrap();
    let (input, witness, parent) = incremental_input(input);
    let (fresh, reused) = backend
        .validate_next(parent, &input, witness, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(!reused);
    let checkpoints = fresh.transaction_state_checkpoints();
    assert_eq!(v1.blocks[0].transaction_state_checkpoints(), checkpoints);
    assert_eq!(checkpoints.len(), 2);
    assert_eq!(checkpoints[0].at, CheckpointAt::PreExecution);
    assert_eq!(checkpoints[0].state_root, parent.state_root);
    assert_ne!(checkpoints[0].block_hash, fresh.hash());
    assert_eq!(checkpoints[1].at, CheckpointAt::Transaction(0));
    assert_eq!(checkpoints[1].state_root, post_root);
    assert_eq!(checkpoints[1].block_hash, fresh.hash());
    let (cached, reused) = backend
        .validate_next(
            parent,
            &input,
            ExecutionWitness::default(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(reused);
    assert_eq!(cached, fresh);
}

#[tokio::test]
async fn concurrent_incremental_misses_publish_one_cached_result() {
    let (input, witness, parent) = incremental_input(fixture_input());
    let backend = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS);
    let slots = backend
        .validation_slots
        .acquire_many(MAX_CONCURRENT_VALIDATIONS as u32)
        .await
        .unwrap();
    let mut first = std::pin::pin!(backend.validate_next(
        parent,
        &input,
        witness.clone(),
        Duration::from_secs(10)
    ));
    let mut second =
        std::pin::pin!(backend.validate_next(parent, &input, witness, Duration::from_secs(10)));
    // Poll each past its cache miss into the occupied semaphore, deterministically.
    tokio::select! { biased; _ = &mut first => panic!("execution slot is held"), _ = std::future::ready(()) => {} }
    tokio::select! { biased; _ = &mut second => panic!("execution slot is held"), _ = std::future::ready(()) => {} }
    drop(slots);
    let (first, second) = tokio::join!(first, second);
    let (first, reused_first) = first.unwrap();
    let (second, reused_second) = second.unwrap();
    assert!(
        !reused_first && !reused_second,
        "both calls must exercise a cache miss"
    );
    assert_eq!(first, second);
    assert_eq!(backend.blocks.lock().unwrap().len(), 1);
    assert!(
        backend
            .validate_next(
                parent,
                &input,
                ExecutionWitness::default(),
                Duration::from_secs(1)
            )
            .await
            .unwrap()
            .1
    );
}

#[tokio::test]
async fn incremental_deadline_includes_waiting_for_backend_capacity() {
    let (input, witness, parent) = incremental_input(fixture_input());
    let backend = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS);
    let slots = backend
        .validation_slots
        .acquire_many(MAX_CONCURRENT_VALIDATIONS as u32)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .validate_next(parent, &input, witness.clone(), Duration::from_millis(10))
            .await,
        Err(ValidationError::DeadlineExceeded)
    ));
    assert!(backend.blocks.lock().unwrap().is_empty());
    drop(slots);
    assert!(
        !backend
            .validate_next(parent, &input, witness, Duration::from_secs(10))
            .await
            .unwrap()
            .1
    );
}

#[test]
fn timed_out_incremental_worker_keeps_its_slot_and_publishes_only_after_success() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (input, witness, parent) = incremental_input(fixture_input());
        let backend = Backend::new(fixture_chain_config(), TEST_SYSTEM_ADDRESS);
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        ready.await.unwrap();
        // The validation owns a backend slot but cannot enter the occupied blocking worker yet.
        let result = backend
            .validate_next(parent, &input, witness, Duration::from_millis(10))
            .await;
        let available = backend.validation_slots.available_permits();
        let cached_before_execution = backend.blocks.lock().unwrap().len();
        release.send(()).unwrap();
        blocker.await.unwrap();
        let slots = tokio::time::timeout(
            Duration::from_secs(5),
            backend
                .validation_slots
                .acquire_many(MAX_CONCURRENT_VALIDATIONS as u32),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(result, Err(ValidationError::DeadlineExceeded)));
        assert_eq!(available, MAX_CONCURRENT_VALIDATIONS - 1);
        assert_eq!(cached_before_execution, 0);
        // The detached job finished normally and populated the cache without needing a session token.
        assert!(
            backend
                .validate_next(
                    parent,
                    &input,
                    ExecutionWitness::default(),
                    Duration::from_secs(1)
                )
                .await
                .unwrap()
                .1
        );
        drop(slots);
    });
}

/// A recorded three-user-transaction block and its execution configuration.
fn checkpoint_fixture() -> (AdmittedBlock, ChainConfig) {
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
    (
        admitted_block_with_witness(
            block.header.number,
            block.header.hash_slow(),
            block.header.parent_hash,
            rlp,
            witness,
        ),
        chain_config,
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

fn high_s_ethereum_transaction() -> TransactionSigned {
    let transaction: reth_ethereum_primitives::TransactionSigned =
        <reth_ethereum_primitives::TransactionSigned as alloy_eips::Decodable2718>::decode_2718_exact(&hex::decode(crate::testkit::LEGACY_TX).unwrap()).unwrap();
    let signature = transaction.signature();
    let curve_order = U256::from_str_radix(
        "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
        16,
    )
    .unwrap();
    let high_s = Signature::new(signature.r(), curve_order - signature.s(), !signature.v());
    reth_ethereum_primitives::TransactionSigned::new_unhashed(
        transaction.into_typed_transaction(),
        high_s,
    )
    .into()
}

fn checkpoint(transaction_index: usize) -> StateCheckpoint {
    StateCheckpoint {
        at: CheckpointAt::Transaction(transaction_index),
        state_root: B256::ZERO,
        block_hash: B256::with_last_byte(0xc0 ^ (transaction_index as u8)),
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
    let plan = CheckpointPlan::new(vec![
        CheckpointAt::Transaction(0),
        CheckpointAt::Transaction(2),
    ]);
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
        <TransactionSigned as alloy_eips::Decodable2718>::decode_2718_exact(
            &hex::decode(SYSTEM_TX).unwrap(),
        )
        .unwrap()
    };
    let block = Block::new(
        Default::default(),
        alloy_consensus::BlockBody {
            transactions: vec![
                transaction(),
                TxLegacy::default()
                    .into_signed(Signature::test_signature())
                    .into(),
                transaction(),
            ],
            ..Default::default()
        },
    );
    let recovered = RecoveredBlock::new_unhashed(
        block,
        vec![TEST_SYSTEM_ADDRESS, Address::ZERO, TEST_SYSTEM_ADDRESS],
    );

    let (plan, system_sender_flags) = CheckpointPlan::for_block(&recovered, TEST_SYSTEM_ADDRESS);

    assert_eq!(system_sender_flags, [true, false, true]);
    // The anchor's candidate leads, then the pair ends.
    assert_eq!(
        plan.positions(),
        [
            CheckpointAt::PreExecution,
            CheckpointAt::Transaction(1),
            CheckpointAt::Transaction(2),
        ]
    );
}

/// Every nonempty plan includes the final transaction, which activates
/// Stateless's post-execution-neutrality check. Ordinary blocks need no plan.
#[test]
fn checkpoint_plan_is_empty_or_ends_at_the_final_transaction() {
    let system = || {
        <TransactionSigned as alloy_eips::Decodable2718>::decode_2718_exact(
            &hex::decode(SYSTEM_TX).unwrap(),
        )
        .unwrap()
    };
    let user = || -> TransactionSigned {
        TxLegacy::default()
            .into_signed(Signature::test_signature())
            .into()
    };

    // Every system/user shape up to six transactions.
    for width in 0..=6usize {
        for mask in 0..(1u32 << width) {
            let shape: Vec<bool> = (0..width).map(|i| mask >> i & 1 == 1).collect();
            let block = Block::new(
                Default::default(),
                alloy_consensus::BlockBody {
                    transactions: shape
                        .iter()
                        .map(|&is_system| if is_system { system() } else { user() })
                        .collect(),
                    ..Default::default()
                },
            );
            let recovered = RecoveredBlock::new_unhashed(block, vec![TEST_SYSTEM_ADDRESS; width]);
            let (plan, _) = CheckpointPlan::for_block(&recovered, TEST_SYSTEM_ADDRESS);

            if mask == 0 {
                assert!(plan.positions().is_empty(), "ordinary shape {shape:?}");
                continue;
            }

            assert_eq!(
                plan.positions().last(),
                Some(&CheckpointAt::Transaction(width - 1)),
                "shape {shape:?} did not plan its final transaction",
            );
        }
    }
}

#[test]
fn checkpoint_plan_includes_every_inbound_boundary() {
    let transaction = || {
        <TransactionSigned as alloy_eips::Decodable2718>::decode_2718_exact(
            &hex::decode(SYSTEM_TX).unwrap(),
        )
        .unwrap()
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
            CheckpointPlan::for_block(&recovered, TEST_SYSTEM_ADDRESS);

        assert_eq!(system_sender_flags, vec![true; transaction_count]);
        let mut expected = vec![CheckpointAt::PreExecution];
        expected.extend((0..transaction_count).map(CheckpointAt::Transaction));
        assert_eq!(plan.positions(), expected);
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
    let transaction = high_s_ethereum_transaction();
    assert!(transaction.recover_signer().is_err());
    assert_eq!(
        transaction.recover_signer_unchecked().unwrap(),
        crate::testkit::LEGACY_SIGNER_ADDRESS
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
    assert_eq!(
        system_sender_flags(&recovered, crate::testkit::LEGACY_SIGNER_ADDRESS),
        [true]
    );

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
    assert_eq!(output.blocks.len(), 1);
    let block = &output.blocks[0];
    assert_eq!(
        block.hash(),
        b256!("16b64a78e9b3e0d533cafe81f9121735f6a2c8122c69b0bb5994ee75fe7bface")
    );
    // Still asserted: `post_state_root` feeds the continuity check that proves
    // each block executed from its predecessor's post-state.
    assert_eq!(
        block.as_anchor().state_root,
        b256!("f09d8f7da5bc5036f8dd9536c953e2212390a46fb3e553ece2b7d419131537b1")
    );
    assert!(block.receipt_successes().is_empty());
    assert!(block.transaction_state_checkpoints().is_empty());
    assert!(block.settlement_evidence().system_sender_flags.is_empty());
    assert!(
        block
            .settlement_evidence()
            .observed_outbound_events
            .is_empty()
    );
}

#[test]
fn ordinary_v1_terminal_executes_without_checkpoints() {
    let (mut input, chain_config) = checkpoint_fixture();
    let expected_hash = input.claimed_hash();

    let output = Backend::new(chain_config, TEST_SYSTEM_ADDRESS)
        .validate_admitted(
            std::slice::from_mut(&mut input),
            &CancellationToken::default(),
        )
        .unwrap();

    let block = &output.blocks[0];
    assert_eq!(block.decoded().body.transactions.len(), 3);
    assert_eq!(
        block.settlement_evidence().system_sender_flags,
        [false, false, false]
    );
    assert!(
        block
            .settlement_evidence()
            .observed_outbound_events
            .is_empty()
    );
    assert_eq!(block.hash(), expected_hash);

    assert!(block.transaction_state_checkpoints().is_empty());
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

    // The window no longer carries a pre-state root; per-block post-state roots
    // are what the continuity check chains, and that check has its own test.
    assert_eq!(
        output.blocks.last().unwrap().as_anchor().state_root,
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
            .settlement_evidence()
            .observed_outbound_events
            .is_empty()
    );
    // Historical signed calls have no native system authority.
    assert!(output.blocks.iter().all(|block| {
        block
            .settlement_evidence()
            .system_sender_flags
            .iter()
            .all(|is_system| !is_system)
    }));
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
