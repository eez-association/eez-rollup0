//! Direct tests for synchronous settlement processing.

use super::*;

fn validated_single_block(
    block: validate::ValidatedBlock,
    receipt_successes: Vec<bool>,
    transaction_state_checkpoints: Vec<validate::TransactionStateCheckpoint>,
) -> validate::ValidatedWindow {
    validate::ValidatedWindow::for_test(
        B256::ZERO,
        B256::ZERO,
        B256::ZERO,
        Vec::new(),
        validate::ValidatedSettlingBlock::for_test(
            block,
            receipt_successes,
            transaction_state_checkpoints,
        ),
    )
}

#[test]
fn settlement_pipeline_errors_have_stable_rpc_mappings() {
    let post_batch = SettlementPipelineError::PostBatchCalldata(
        crate::settlement::PostBatchDecodeError::NonCanonical,
    );
    assert_eq!(post_batch.gate(), "post_batch_calldata");
    let status = post_batch.status();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "invalid PostBatch calldata");

    let da_payload =
        SettlementPipelineError::DaPayload(crate::settlement::DaPayloadError::TrailingBytes {
            trailing: 1,
        });
    assert_eq!(da_payload.gate(), "da_payload");
    let status = da_payload.status();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "invalid batch callData");

    let gate_failures = [
        (
            SettlementPipelineError::PublicInputs(
                crate::settlement::PublicInputError::InvalidStructure("synthetic".to_owned()),
            ),
            "public_inputs",
        ),
        (
            SettlementPipelineError::BlockInspection(
                crate::settlement::BlockInspectionError::RevertedSystemTransaction { index: 0 },
            ),
            "block_inspection",
        ),
        (
            SettlementPipelineError::BlockInspection(
                crate::settlement::BlockInspectionError::ReservedSystemSender { index: 0 },
            ),
            "block_inspection",
        ),
        (
            SettlementPipelineError::StateUpdateChain(
                crate::settlement::StateUpdateChainError::NoEntries,
            ),
            "state_update_chain",
        ),
        (
            SettlementPipelineError::EffectPrefix(
                crate::settlement::EffectPrefixError::LeadingEntryNotAnchor {
                    actual: crate::settlement::ClaimedEntryShape::Invalid,
                },
            ),
            "effect_prefix",
        ),
        (
            SettlementPipelineError::InboundEffects(
                crate::settlement::InboundEffectError::UnexpectedCandidate {
                    transaction_index: 0,
                },
            ),
            "inbound_effects",
        ),
        (
            SettlementPipelineError::OutboundEffects(
                crate::settlement::OutboundEffectError::MissingObservation {
                    entry_index: 1,
                    transaction_index: 0,
                },
            ),
            "outbound_effects",
        ),
        (
            SettlementPipelineError::DaPayload(crate::settlement::DaPayloadError::BlockCount {
                expected: 2,
                actual: 1,
            }),
            "da_payload",
        ),
        (
            SettlementPipelineError::DaPayload(
                crate::settlement::DaPayloadError::UnexpectedItems {
                    field: "transactions",
                    expected: 1,
                },
            ),
            "da_payload",
        ),
        (
            SettlementPipelineError::DaPayload(
                crate::settlement::DaPayloadError::TransactionMismatch {
                    block_number: 1,
                    transaction_index: 0,
                },
            ),
            "da_payload",
        ),
        (
            SettlementPipelineError::DaPayload(
                crate::settlement::DaPayloadError::SyncBlockTransactionMismatch {
                    transaction_index: 0,
                },
            ),
            "da_payload",
        ),
        (
            SettlementPipelineError::DaPayload(
                crate::settlement::DaPayloadError::SyncBlockTransactionCount {
                    expected: 2,
                    actual: 1,
                },
            ),
            "da_payload",
        ),
    ];

    for (error, gate) in gate_failures {
        assert_eq!(error.gate(), gate);
        let status = error.status();
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), "settlement validation rejected");
        assert!(status.details().is_empty());
    }

    let invalid_evidence_errors = [
        SettlementPipelineError::BlockInspection(
            crate::settlement::BlockInspectionError::InvalidRlp {
                block_number: 1,
                reason: "synthetic".to_owned(),
            },
        ),
        SettlementPipelineError::BlockInspection(
            crate::settlement::BlockInspectionError::StatusCount {
                required: 1,
                actual: 0,
            },
        ),
        SettlementPipelineError::BlockInspection(
            crate::settlement::BlockInspectionError::SystemSenderCount {
                block_number: 1,
                required: 1,
                actual: 0,
            },
        ),
        SettlementPipelineError::EffectPrefix(
            crate::settlement::EffectPrefixError::TransactionStateCheckpointCountMismatch {
                expected: 1,
                actual: 0,
            },
        ),
        SettlementPipelineError::EffectPrefix(
            crate::settlement::EffectPrefixError::TransactionStateCheckpointIndexMismatch {
                checkpoint_index: 0,
                expected: 1,
                actual: 0,
            },
        ),
        SettlementPipelineError::DaPayload(crate::settlement::DaPayloadError::InvalidBlockRlp {
            block_number: 1,
            reason: "synthetic".to_owned(),
        }),
        SettlementPipelineError::DaPayload(
            crate::settlement::DaPayloadError::InvalidEffectTransactionLayout,
        ),
        SettlementPipelineError::DaPayload(
            crate::settlement::DaPayloadError::SystemTransactionReconstruction {
                reason: "synthetic".to_owned(),
            },
        ),
    ];
    for error in invalid_evidence_errors {
        let status = error.status();
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(
            status.message(),
            "validation backend returned invalid output"
        );
    }

    for error in [
        SettlementPipelineError::PublicInputs(crate::settlement::PublicInputError::Computation(
            "synthetic".to_owned(),
        )),
        SettlementPipelineError::PublicInputs(crate::settlement::PublicInputError::HashCount {
            actual: 0,
        }),
    ] {
        let status = error.status();
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(
            status.message(),
            "public-input hash computation violated its contract"
        );
    }

    let cancelled = SettlementPipelineError::Cancelled.status();
    assert_eq!(cancelled.code(), Code::Cancelled);
    assert_eq!(cancelled.message(), "Prove request cancelled");
}

#[test]
fn transient_validation_errors_have_stable_rpc_mappings() {
    let cases = [
        (
            validate::ValidationError::Unavailable("synthetic".to_owned()),
            Code::Unavailable,
            "validation backend is temporarily unavailable",
        ),
        (
            validate::ValidationError::Aborted("synthetic".to_owned()),
            Code::Aborted,
            "validation backend snapshot changed during validation",
        ),
    ];

    for (error, code, message) in cases {
        let status = PipelineError::Validation(error).status();
        assert_eq!(status.code(), code);
        assert_eq!(status.message(), message);
    }
}

#[test]
fn cancelled_settlement_stops_before_decoding_untrusted_input() {
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));
    let settling_block = validate::ValidatedBlock::for_test(
        1,
        Vec::new(),
        validate::SettlementBlockEvidence::for_test(Vec::new(), Vec::new()),
    );
    let validated = validated_single_block(settling_block, Vec::new(), Vec::new());

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: b"not ABI calldata".to_vec(),
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert!(matches!(run, Err(SettlementPipelineError::Cancelled)));
}

#[test]
fn an_elapsed_deadline_stops_the_pipeline_between_validation_and_settlement() {
    let mut input = AdmittedBlock::test(5, 0x04, 0x05);
    let empty_body: reth_ethereum_primitives::BlockBody = Default::default();
    input.rlp = alloy_rlp::encode(reth_ethereum_primitives::Block::new(
        Default::default(),
        empty_body,
    ));
    let inputs = vec![input];
    let state = inner(Validator::stub(vec![Ok(backend_output_for(&inputs))]));
    // A deadline captured now is already past when the boundary between
    // execution validation and settlement runs.
    let deadline = tokio::time::Instant::now();

    let result = validate_and_settle(
        &state,
        crate::window::AdmittedBlocks::for_test(inputs),
        b"not ABI calldata".to_vec(),
        deadline,
        &CancellationToken::default(),
    );

    // The exact variant proves settlement never decoded the untrusted bytes:
    // decoding them would have produced a calldata error instead.
    let error = result
        .err()
        .expect("an elapsed deadline must stop the pipeline");
    assert!(matches!(error, PipelineError::DeadlineBeforeSettlement));
    assert_eq!(error.phase(), "settlement");
    assert_eq!(error.gate(), "deadline");
    let status = error.status();
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_eq!(status.message(), "Prove request deadline exceeded");
}

#[test]
fn a_fully_bound_inbound_passes_settlement_and_da_validation() {
    let value = U256::from(7);
    let (transaction, call_hash, return_data, sidecar) = strict_inbound_transaction(value);
    let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![transaction],
        ..Default::default()
    };
    let block = reth_ethereum_primitives::Block::new(Default::default(), body);
    let block_rlp = alloy_rlp::encode(block);
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp.clone(),
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );

    let mut batch = anchor_batch();
    batch.entries.push(ExecutionEntrySol {
        stateUpdates: vec![StateUpdateSol {
            rollupId: 1,
            currentState: B256::ZERO,
            newState: B256::ZERO,
            etherDelta: I256::try_from(value).unwrap(),
        }],
        proxyEntryHash: call_hash,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: B256::ZERO,
        destinationRollupId: 1,
        success: true,
        returnData: return_data,
    });
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
    batch.callData = settlement::encode_da_payload(&[Vec::new()], &[sidecar.abi_encode()]).into();
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let calldata = eez_protocol::entries::encode_postbatch(&batch);
    let statuses = [true];
    let checkpoints = [checkpoint(0, B256::ZERO)];
    let validated = validated_single_block(settling_block, statuses.to_vec(), checkpoints.to_vec());
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata.clone(),
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert_eq!(run.unwrap().into_inner(), expected_hash);

    let reverted_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );
    let reverted = validated_single_block(reverted_block, vec![false], checkpoints.to_vec());
    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata,
        validated_window: &reverted,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    })
    .unwrap_err();
    assert_eq!(error.gate(), "inbound_effects");
    let status = error.status();
    assert_eq!(status.code(), Code::FailedPrecondition);
    let failure = eez_control_rpc::decode_prove_failure(status.details()).unwrap();
    let prove_failure::ActionableFailure::Inbound(inbound) = failure.actionable_failure.unwrap()
    else {
        panic!("reverted inbound must return an inbound failure");
    };
    assert_eq!(inbound.entry_index, 1);
    assert_eq!(
        inbound.entry_hash,
        keccak256(batch.entries[1].abi_encode()).as_slice()
    );
}

#[test]
fn a_reverted_outbound_load_reports_the_paired_user_transaction() {
    let (batch, block_rlp, user, _call_hash) = canonical_outbound_case();
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(vec![true, false], Vec::new()),
    );
    let validated = validated_single_block(
        settling_block,
        vec![false, true],
        vec![checkpoint(1, B256::ZERO)],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    })
    .unwrap_err();

    assert_eq!(error.gate(), "block_inspection");
    let status = error.status();
    assert_eq!(status.code(), Code::FailedPrecondition);
    let failure = eez_control_rpc::decode_prove_failure(status.details()).unwrap();
    let prove_failure::ActionableFailure::Outbound(outbound) = failure.actionable_failure.unwrap()
    else {
        panic!("reverted load must return an outbound failure");
    };
    assert_eq!(outbound.transaction_index, 1);
    assert_eq!(outbound.transaction_hash, keccak256(user).as_slice());
}

#[test]
fn an_outbound_observation_failure_reports_the_user_transaction() {
    let (batch, block_rlp, user, _call_hash) = canonical_outbound_case();
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(vec![true, false], Vec::new()),
    );
    let validated = validated_single_block(
        settling_block,
        vec![true, true],
        vec![checkpoint(1, B256::ZERO)],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    })
    .unwrap_err();

    assert_eq!(error.gate(), "outbound_effects");
    let status = error.status();
    assert_eq!(status.code(), Code::FailedPrecondition);
    let failure = eez_control_rpc::decode_prove_failure(status.details()).unwrap();
    let prove_failure::ActionableFailure::Outbound(outbound) = failure.actionable_failure.unwrap()
    else {
        panic!("missing outbound observation must return an outbound failure");
    };
    assert_eq!(outbound.transaction_index, 1);
    assert_eq!(outbound.transaction_hash, keccak256(user).as_slice());
}

#[test]
fn an_outbound_claim_hash_mismatch_remains_non_actionable() {
    let (mut batch, block_rlp, _user, call_hash) = canonical_outbound_case();
    batch.entries[1].l2ToL1Calls[0].data = Bytes::from_static(b"different claim");
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(
            vec![true, false],
            vec![validate::OutboundEventObservation::decoded_for_test(
                1, 0, call_hash, 0,
            )],
        ),
    );
    let validated = validated_single_block(
        settling_block,
        vec![true, true],
        vec![checkpoint(1, B256::ZERO)],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    })
    .unwrap_err();

    assert_eq!(error.gate(), "outbound_effects");
    let status = error.status();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.details().is_empty());
}

#[test]
fn a_fully_bound_outbound_effect_is_authorized() {
    let (batch, block_rlp, user, call_hash) = canonical_outbound_case();
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(
            vec![true, false],
            vec![validate::OutboundEventObservation::decoded_for_test(
                1, 0, call_hash, 0,
            )],
        ),
    );
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let calldata = eez_protocol::entries::encode_postbatch(&batch);
    let statuses = [true, true];
    let checkpoints = [checkpoint(1, B256::ZERO)];
    let validated = validated_single_block(settling_block, statuses.to_vec(), checkpoints.to_vec());
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata,
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert_eq!(run.unwrap().into_inner(), expected_hash);

    let mut mismatched_da = batch;
    mismatched_da.callData =
        settlement::encode_da_payload(&[vec![user]], &[mismatched_da.entries[1].abi_encode()])
            .into();
    let mismatched_calldata = eez_protocol::entries::encode_postbatch(&mismatched_da);
    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: mismatched_calldata,
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });
    assert!(matches!(
        run,
        Err(SettlementPipelineError::DaPayload(
            settlement::DaPayloadError::L2EntryMismatch { .. }
        ))
    ));
}
