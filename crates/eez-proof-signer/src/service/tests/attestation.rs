//! End-to-end RPC validation and attestation behavior.

use super::*;

#[tokio::test]
async fn a_stubbed_canonical_outbound_window_returns_a_recoverable_attestation() {
    let (batch, block_rlp, _user, call_hash) = canonical_outbound_case();
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    // Receipt extraction has dedicated adapter and golden tests; this stub
    // isolates the settlement-to-signature path.
    let validator = Validator::stub_with_settlement_evidence(
        outbound_backend_output(),
        vec![outbound_evidence(call_hash)],
    );
    let server = TestServer::new(inner(validator)).await;

    let response = server
        .attest(single_block_settlement_window(batch, block_rlp))
        .await;

    assert_attestation(&response, expected_hash, test_attester().address());
}

#[tokio::test]
async fn a_stubbed_mixed_outbound_then_inbound_window_returns_an_attestation() {
    let (batch, block_rlp, outbound_call_hash) = mixed_outbound_inbound_case();
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let validator = Validator::stub_with_settlement_evidence(
        mixed_backend_output(),
        vec![mixed_evidence(outbound_call_hash)],
    );
    let server = TestServer::new(inner(validator)).await;

    let response = server
        .attest(single_block_settlement_window(batch, block_rlp))
        .await;

    assert_attestation(&response, expected_hash, test_attester().address());
}

#[tokio::test]
async fn a_stubbed_value_bearing_outbound_window_returns_an_attestation() {
    let value = U256::from(1);
    let (batch, block_rlp, _user, call_hash) = outbound_case(value);
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let validator = Validator::stub_with_settlement_evidence(
        outbound_backend_output(),
        vec![outbound_evidence(call_hash)],
    );
    let server = TestServer::new(inner(validator)).await;

    let response = server
        .attest(single_block_settlement_window(batch, block_rlp))
        .await;

    assert_attestation(&response, expected_hash, test_attester().address());
}

#[tokio::test]
async fn a_valid_anchor_only_window_returns_a_recoverable_attestation() {
    let server = TestServer::new(one_accepting_validator()).await;
    let window = happy_window();
    let Some(prove_chunk::Kind::Header(header)) = &window[0].kind else {
        unreachable!();
    };
    let batch = settlement::decode_canonical_post_batch(
        header.post_batch.as_ref().unwrap().abi_calldata.clone(),
    )
    .expect("test batch must decode");
    let expected_hash = recompute_test_public_inputs_hash(&batch);

    let response = server.attest(window).await;

    assert_eq!(response.public_inputs_hash, expected_hash.as_slice());
    assert_eq!(response.signature.len(), 65);
    assert!(matches!(response.signature[64], 27 | 28));
    let signature = Signature::try_from(response.signature.as_slice()).unwrap();
    assert!(
        signature.normalize_s().is_none(),
        "signature must use low-s"
    );
    let recovered = signature
        .recover_address_from_prehash(&expected_hash)
        .unwrap();
    assert_eq!(
        recovered,
        address!("70997970c51812dc3a010c7d01b50e0d17dc79c8")
    );
}

#[tokio::test]
async fn matching_intermediate_transaction_da_payload_is_attested() {
    let server = TestServer::new(two_block_validator_with_execution_evidence(
        vec![true],
        vec![false],
        Vec::new(),
        Vec::new(),
    ))
    .await;
    let mut window = vec![
        header_chunk(5, 6),
        transaction_block_chunk(5, 0x04, 0x05, non_system_transaction()),
        block_chunk(6, 0x05, 0x06),
    ];
    replace_batch_bound_to_window(&mut window, anchor_batch());

    let _response = server.attest(window).await;
}

#[tokio::test]
async fn mismatched_intermediate_transaction_da_payload_is_rejected() {
    let inner = two_block_validator_with_execution_evidence(
        vec![true],
        vec![false],
        Vec::new(),
        Vec::new(),
    );
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut window = vec![
        header_chunk(5, 6),
        transaction_block_chunk(5, 0x04, 0x05, non_system_transaction()),
        block_chunk(6, 0x05, 0x06),
    ];
    let payload_source = vec![
        header_chunk(5, 6),
        transaction_block_chunk(5, 0x04, 0x05, system_transaction()),
        block_chunk(6, 0x05, 0x06),
    ];
    let mut batch = anchor_batch();
    batch.callData = da_payload_for_window(&payload_source).into();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn mismatched_immediate_entry_count_is_rejected() {
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output_for(
        &happy_block_inputs(),
    ))])))
    .await;
    let mut batch = anchor_batch();
    batch.immediateEntryCount = U256::ZERO;
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(batch, 3),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn an_empty_batch_is_rejected_by_the_state_update_chain_gate() {
    let server = TestServer::new(one_accepting_validator()).await;
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for(eez_protocol::EvmBatch::default()),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_noncanonical_anchor_is_rejected_by_the_effect_prefix_gate() {
    let server = TestServer::new(one_accepting_validator()).await;
    let mut batch = anchor_batch();
    batch.entries[0].rollingHash = B256::repeat_byte(0xee);
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_nonzero_anchor_ether_delta_is_rejected() {
    let server = TestServer::new(one_accepting_validator()).await;
    let mut batch = anchor_batch();
    batch.entries[0].stateUpdates[0].etherDelta = I256::ONE;
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_second_anchor_is_rejected_by_the_effect_prefix_gate() {
    let server = TestServer::new(one_accepting_validator()).await;
    let mut batch = anchor_batch();
    let second_anchor = batch.entries[0].clone();
    batch.entries.push(second_anchor);
    batch.immediateEntryCount = U256::from(2);
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn empty_transaction_checkpoint_output_accepts_the_anchor_only_prefix() {
    let inputs = happy_block_inputs();
    let mut backend_output = backend_output_for(&inputs);
    backend_output.blocks[2].transaction_state_checkpoints = Vec::new();
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output)]))).await;

    let _response = server.attest(happy_window()).await;
}

#[tokio::test]
async fn an_anchor_only_batch_rejects_a_settling_transaction() {
    let server = TestServer::new(single_block_validator_with_execution_evidence(
        vec![true],
        vec![false],
    ))
    .await;

    let status = server.prove(single_non_system_transaction_window()).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn an_outbound_effect_without_an_observed_call_is_rejected() {
    let (batch, block_rlp, _user, _call_hash) = canonical_outbound_case();
    // The RLP still supplies the canonical [load, user] positions, but this
    // test backend intentionally supplies no receipt observation.
    let server = TestServer::new(inner(Validator::stub(vec![Ok(outbound_backend_output())]))).await;

    let status = server
        .prove(single_block_settlement_window(batch, block_rlp))
        .await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_multi_block_effect_uses_the_penultimate_block_root() {
    let pre_settling_root = B256::repeat_byte(0x55);
    let final_root = B256::repeat_byte(0x66);
    let inputs = [
        AdmittedBlock::test(5, 0x04, 0x05),
        AdmittedBlock::test(6, 0x05, 0x06),
    ];
    let mut backend_output = backend_output_for(&inputs);
    backend_output.blocks[0].post_state_root = pre_settling_root;
    backend_output.blocks[1].post_state_root = final_root;
    backend_output.blocks[0].set_transaction_results_for_test(vec![true]);
    backend_output.blocks[1].set_transaction_results_for_test(vec![true]);
    backend_output.blocks[0]
        .settlement_evidence
        .set_system_sender_flags_for_test(vec![false]);
    backend_output.blocks[1]
        .settlement_evidence
        .set_system_sender_flags_for_test(vec![false]);
    backend_output.blocks[1].transaction_state_checkpoints = vec![checkpoint(0, final_root)];
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output)]))).await;

    let mut window =
        two_block_transaction_window(non_system_transaction(), non_system_transaction());
    replace_post_batch(
        &mut window,
        public_input_post_batch_for(outbound_batch(B256::ZERO, pre_settling_root, final_root)),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_state_update_final_root_mismatch_is_rejected() {
    let window = happy_block_inputs();
    let mut backend_output = backend_output_for(&window);
    backend_output.blocks.last_mut().unwrap().post_state_root = B256::repeat_byte(0xee);
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output)]))).await;

    let status = server.prove(happy_window()).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_state_update_rollup_mismatch_is_rejected() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut batch = anchor_batch();
    batch.entries[0].stateUpdates[0].rollupId = 2;
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn malformed_settling_block_rlp_after_backend_success_is_internal() {
    let server = TestServer::new(one_accepting_validator()).await;
    let mut window = happy_window();
    block_mut(window.last_mut().unwrap()).rlp = vec![0xff];

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::Internal, "{status:?}");
    assert_eq!(
        status.message(),
        "validation backend returned invalid output"
    );
}

#[tokio::test]
async fn distinct_reexecuted_roots_are_attested_when_the_anchor_matches() {
    let parent = B256::repeat_byte(0x11);
    let final_root = B256::repeat_byte(0x22);
    let mut backend_output = backend_output_for(&happy_block_inputs());
    backend_output.pre_state_root = parent;
    backend_output.blocks.last_mut().unwrap().post_state_root = final_root;
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output)]))).await;
    let mut batch = anchor_batch();
    batch.entries[0].stateUpdates[0].currentState = parent;
    batch.entries[0].stateUpdates[0].newState = final_root;
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(batch, 3),
    );

    let _response = server.attest(window).await;
}

#[tokio::test]
async fn a_matching_nondefault_rollup_identity_is_attested() {
    const ROLLUP_ID: u64 = 7;

    let server = TestServer::new(inner_with_rollup(
        Validator::stub(vec![Ok(backend_output_for(&happy_block_inputs()))]),
        expected_rollup_id(ROLLUP_ID),
    ))
    .await;
    let mut window = happy_window();
    header_mut(&mut window[0]).rollup_id = ROLLUP_ID;
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(anchor_batch_for(ROLLUP_ID), 3),
    );

    let _response = server.attest(window).await;
}

#[tokio::test]
async fn the_composer_public_input_hash_cannot_control_the_attestation() {
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output_for(
        &happy_block_inputs(),
    ))])))
    .await;
    let mut post_batch = public_input_post_batch();
    let batch = settlement::decode_canonical_post_batch(post_batch.abi_calldata.clone()).unwrap();
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let composer_claim = vec![0x99; 32];
    post_batch.public_inputs_hash.clone_from(&composer_claim);
    let mut window = happy_window();
    replace_post_batch(&mut window, post_batch);

    let response = server.attest(window).await;

    assert_attestation(&response, expected_hash, test_attester().address());
    assert_ne!(response.public_inputs_hash, composer_claim);
}

#[tokio::test]
async fn short_settling_statuses_are_an_invalid_backend_output() {
    let inner = single_block_validator_with_execution_evidence(Vec::new(), vec![false]);
    let server = TestServer::new(Arc::clone(&inner)).await;

    let status = server.prove(single_non_system_transaction_window()).await;

    assert_eq!(status.code(), Code::Internal, "{status:?}");
    assert_eq!(
        status.message(),
        "validation backend returned invalid output"
    );
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn a_reverted_system_transaction_is_failed_precondition_after_validation() {
    let inner = single_block_validator_with_execution_evidence(vec![false], vec![true]);
    let server = TestServer::new(Arc::clone(&inner)).await;

    let status = server.prove(single_system_transaction_window()).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn a_user_revert_passes_the_system_gate_before_effect_prefix_rejection() {
    let server = TestServer::new(single_block_validator_with_execution_evidence(
        vec![false],
        vec![false],
    ))
    .await;

    let status = server.prove(single_non_system_transaction_window()).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
}

#[tokio::test]
async fn an_intermediate_system_transaction_is_rejected() {
    let inner =
        two_block_validator_with_execution_evidence(vec![true], vec![true], Vec::new(), Vec::new());
    let server = TestServer::new(Arc::clone(&inner)).await;
    let window = vec![
        header_chunk(5, 6),
        transaction_block_chunk(5, 0x04, 0x05, system_transaction()),
        block_chunk(6, 0x05, 0x06),
    ];

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn a_successful_system_transaction_reaches_the_effect_prefix_gate() {
    let server = TestServer::new(two_block_validator_with_execution_evidence(
        vec![false],
        vec![false],
        vec![true],
        vec![true],
    ))
    .await;
    let window = two_block_transaction_window(non_system_transaction(), system_transaction());

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
}

#[tokio::test]
async fn an_inbound_candidate_hidden_in_an_outbound_pair_is_rejected() {
    let inputs = [AdmittedBlock::test(5, 0x04, 0x05)];
    let mut backend_output = backend_output_for(&inputs);
    backend_output.blocks[0].set_transaction_results_for_test(vec![true, true]);
    backend_output.blocks[0]
        .settlement_evidence
        .set_system_sender_flags_for_test(vec![true, false]);
    backend_output.blocks[0].transaction_state_checkpoints = vec![checkpoint(1, B256::ZERO)];
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output)]))).await;
    let mut window = vec![
        header_chunk(5, 5),
        transactions_block_chunk(
            5,
            0x04,
            0x05,
            vec![
                strict_inbound_transaction(U256::ZERO).0,
                non_system_transaction(),
            ],
        ),
    ];
    replace_post_batch(
        &mut window,
        public_input_post_batch_for(outbound_batch(B256::ZERO, B256::ZERO, B256::ZERO)),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn a_static_entry_carrier_is_rejected() {
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output_for(
        &happy_block_inputs(),
    ))])))
    .await;
    let mut batch = anchor_batch();
    batch.staticEntries.push(Default::default());
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
}

#[tokio::test]
async fn invalid_public_input_structure_is_failed_precondition_after_validation() {
    let inner = inner(Validator::stub(vec![Ok(backend_output_for(
        &happy_block_inputs(),
    ))]));
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut batch = anchor_batch();
    batch.proofSystems.clear();
    let mut window = happy_window();
    replace_post_batch(&mut window, public_input_post_batch_for(batch));

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn a_batch_for_a_different_proof_system_is_rejected() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut batch = anchor_batch();
    batch.proofSystems[0] = address!("00000000000000000000000000000000000000bb");
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(batch, 3),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

/// The deployed signer profile attests exactly one proof system. A structurally
/// valid-looking batch must not widen that trust boundary by adding a second
/// proof system.
#[tokio::test]
async fn a_multi_proof_system_batch_is_rejected() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut batch = anchor_batch();
    batch
        .proofSystems
        .push(address!("00000000000000000000000000000000000000bb"));
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(batch, 3),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

/// Block-number-bound public inputs remain disabled until the signer has an
/// authenticated L1 block oracle.
#[tokio::test]
async fn a_non_timeless_batch_is_rejected() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut batch = anchor_batch();
    batch.blockNumber = 7;
    let mut window = happy_window();
    replace_post_batch(
        &mut window,
        public_input_post_batch_for_empty_blocks(batch, 3),
    );

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "settlement validation rejected");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn malformed_settlement_calldata_is_rejected_after_validation() {
    let inner = one_accepting_validator();
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut window = happy_window();
    header_mut(&mut window[0])
        .post_batch
        .as_mut()
        .unwrap()
        .abi_calldata = vec![0xde, 0xad, 0xbe, 0xef];

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert_eq!(status.message(), "invalid PostBatch calldata");
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn malformed_or_trailing_da_payload_is_an_invalid_argument() {
    let backend_outputs = vec![
        Ok(backend_output_for(&happy_block_inputs())),
        Ok(backend_output_for(&happy_block_inputs())),
    ];
    let inner = inner(Validator::stub(backend_outputs));
    let server = TestServer::new(Arc::clone(&inner)).await;
    let mut trailing = settlement::encode_da_payload(&vec![Vec::new(); 3], &[]);
    trailing.push(0xff);

    for payload in [vec![0x00], trailing] {
        let mut batch = anchor_batch();
        batch.callData = payload.into();
        let mut window = happy_window();
        replace_post_batch(&mut window, public_input_post_batch_for(batch));

        let status = server.prove(window).await;

        assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
        assert_eq!(status.message(), "invalid batch callData");
    }
    assert_eq!(inner.validator.stub_remaining(), 0);
}

#[tokio::test]
async fn validator_rejection_precedes_settlement_decoding() {
    let server = TestServer::new(inner(Validator::stub(vec![Err(
        "re-execution mismatch".to_owned()
    )])))
    .await;
    let mut window = happy_window();
    header_mut(&mut window[0])
        .post_batch
        .as_mut()
        .unwrap()
        .abi_calldata
        .clear();

    let status = server.prove(window).await;

    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "window validation rejected");
}

#[tokio::test]
async fn stateless_validation_is_mandatory_and_remains_fail_closed() {
    let server = TestServer::new(inner(Validator::stateless_for_test(
        Default::default(),
        TEST_SYSTEM_ADDRESS,
    )))
    .await;
    let status = server.prove(stateless_window()).await;
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "window validation rejected");
}

#[tokio::test]
async fn stateless_rejections_are_redacted() {
    let server = TestServer::new(inner(Validator::stateless_for_test(
        Default::default(),
        TEST_SYSTEM_ADDRESS,
    )))
    .await;
    let mut window = stateless_window();
    block_mut(&mut window[1]).parent_hash = vec![0xee; 32];

    let status = server.prove(window).await;
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "window validation rejected");
}

#[tokio::test]
async fn invalid_backend_outputs_are_internal_errors() {
    let server = TestServer::new(inner(Validator::stub(vec![Ok(backend_output_for(&[]))]))).await;

    let status = server.prove(happy_window()).await;
    assert_eq!(status.code(), Code::Internal, "{status:?}");
    assert_eq!(
        status.message(),
        "validation backend returned invalid output"
    );
}

#[tokio::test]
async fn the_validator_is_consulted_and_its_rejection_refuses() {
    let inner = inner(Validator::stub(vec![
        Ok(backend_output_for(&happy_block_inputs())),
        Err("re-execution mismatch".to_owned()),
    ]));
    let server = TestServer::new(Arc::clone(&inner)).await;

    // First window: the stub accepts and the service attests.
    let _response = server.attest(happy_window()).await;

    // Second window: the stub rejects with a stable, redacted status.
    let status = server.prove(happy_window()).await;
    assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
    assert_eq!(status.message(), "window validation rejected");

    let validator = &inner.validator;
    assert_eq!(
        validator.stub_remaining(),
        0,
        "both windows reached the backend"
    );
}
