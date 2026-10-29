//! Direct tests for synchronous settlement processing.

use super::*;
use crate::validate::CheckpointAt;

/// A window holding only block 5, sealed on the same `parent -> hash` grid the
/// streamed fixtures use, so batches chain over identical endpoints here and in
/// the RPC tests.
fn validated_single_block(
    block: validate::ValidatedBlock,
    receipt_successes: Vec<bool>,
    transaction_state_checkpoints: Vec<validate::StateCheckpoint>,
) -> validate::ValidatedWindow {
    let (window_pre, _, window_post) = window_endpoints(5, 5);
    validate::ValidatedWindow::for_test(
        window_pre,
        window_post,
        Vec::new(),
        validate::testing::settling_block_for_test(
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
            SettlementPipelineError::RollupUpdateChain(
                crate::settlement::RollupUpdateChainError::NoEntries,
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
                actual: CheckpointAt::Transaction(0),
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
            validate::ValidationError::DeadlineExceeded,
            Code::DeadlineExceeded,
            "block validation deadline exceeded",
        ),
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
        alloy_rlp::encode(eez_primitives::Block::default()),
        validate::SettlementBlockEvidence::for_test(Vec::new(), Vec::new()),
    );
    let validated = validated_single_block(settling_block, Vec::new(), Vec::new());

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: b"not ABI calldata".to_vec(),
        blobs: &[],
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
    let empty_body: eez_primitives::BlockBody = Default::default();
    input.rlp = alloy_rlp::encode(eez_primitives::Block::new(Default::default(), empty_body));
    let inputs = vec![input];
    let state = inner(StubBackend::new(vec![Ok(backend_output_for(&inputs))]));
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
    let body: eez_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![transaction],
        ..Default::default()
    };
    let block = eez_primitives::Block::new(Default::default(), body);
    let block_rlp = alloy_rlp::encode(block);
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp.clone(),
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );

    let (window_pre, _settling_pre, window_post) = window_endpoints(5, 5);
    let mut batch = anchor_batch();
    batch.entries[0].rollupUpdates[0].currentRoot = window_pre;
    batch.entries[0].rollupUpdates[0].newRoot = empty_prefix_candidate();
    batch.entries.push(ExecutionEntrySol {
        rollupUpdates: vec![RollupUpdateSol {
            rollupId: 1,
            currentRoot: empty_prefix_candidate(),
            newRoot: window_post,
            etherDelta: eez_protocol::abi::u256_to_i192(value).unwrap(),
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
    batch.callData =
        settlement::encode_da_payload(&[Vec::new()], std::slice::from_ref(&sidecar)).into();
    let expected_hash = recompute_test_public_inputs_hash(&batch);
    let calldata = eez_protocol::entries::encode_postbatch(&batch);
    let statuses = [true];
    let checkpoints = [
        pre_execution_checkpoint(empty_prefix_candidate()),
        checkpoint(0, block_hash_of(5)),
    ];
    let validated = validated_single_block(settling_block, statuses.to_vec(), checkpoints.to_vec());
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata.clone(),
        blobs: &[],
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert_eq!(run.unwrap().into_inner(), expected_hash);

    // Reuse the same successful execution evidence. Only the submitted DA
    // changes, so rejection must come from DA binding before signing.
    // Backend execution is stubbed here; this exercises the real settlement pipeline.
    let action =
        eez_protocol::entries::manifest::action_from_entry(&sidecar, eez_protocol::RollupId(1))
            .unwrap();
    // A settlement entry has no incoming call descriptor and cannot even be
    // projected into the current action-based DA representation.
    assert!(
        eez_protocol::entries::manifest::action_from_entry(
            &batch.entries[1],
            eez_protocol::RollupId(1),
        )
        .is_err()
    );
    let mut wrong_source = action.clone();
    wrong_source.source_rollup_id = 2;
    let span = [eez_payload_codec::SpanBlock::default()];
    let action_offset = eez_payload_codec::encode_container(1, &span, &[])
        .unwrap()
        .len();
    let mut wrong_destination =
        eez_payload_codec::encode_container(1, &span, std::slice::from_ref(&action)).unwrap();
    let to_chain_offset = action_offset + 11;
    wrong_destination[to_chain_offset..to_chain_offset + 8].copy_from_slice(&2_u64.to_le_bytes());
    for (name, replacement, expected_code) in [
        (
            "L1 settlement ABI substituted for DA",
            batch.entries[1].abi_encode(),
            Code::InvalidArgument,
        ),
        (
            "different source rollup",
            eez_payload_codec::encode_container(1, &span, &[wrong_source]).unwrap(),
            Code::FailedPrecondition,
        ),
        (
            "different destination rollup",
            wrong_destination,
            Code::InvalidArgument,
        ),
    ] {
        let mut substituted = batch.clone();
        substituted.callData = replacement.into();
        let error = run_settlement(SettlementInput {
            submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&substituted),
            blobs: &[],
            validated_window: &validated,
            expected_rollup_id: expected_rollup_id(1),
            expected_l2_system_address: TEST_SYSTEM_ADDRESS,
            proof_system_vkey: test_proof_system_vkey(),
            expected_proof_system: test_proof_system(),
            system_transaction_reconstructor: &system_transaction_reconstructor,
            cancellation: &cancellation,
        })
        .unwrap_err();
        assert_eq!(error.gate(), "da_payload", "{name}: {error:?}");
        assert_eq!(error.status().code(), expected_code, "{name}: {error:?}");
    }

    let reverted_block = validate::ValidatedBlock::for_test(
        5,
        block_rlp,
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );
    let reverted = validated_single_block(reverted_block, vec![false], checkpoints.to_vec());
    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata,
        blobs: &[],
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
        vec![
            pre_execution_checkpoint(empty_prefix_candidate()),
            checkpoint(1, block_hash_of(5)),
        ],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        blobs: &[],
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
        vec![
            pre_execution_checkpoint(empty_prefix_candidate()),
            checkpoint(1, block_hash_of(5)),
        ],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        blobs: &[],
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
        vec![
            pre_execution_checkpoint(empty_prefix_candidate()),
            checkpoint(1, block_hash_of(5)),
        ],
    );
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let error = run_settlement(SettlementInput {
        submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
        blobs: &[],
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
    let checkpoints = [
        pre_execution_checkpoint(empty_prefix_candidate()),
        checkpoint(1, block_hash_of(5)),
    ];
    let validated = validated_single_block(settling_block, statuses.to_vec(), checkpoints.to_vec());
    let cancellation = CancellationToken::default();
    let system_transaction_reconstructor =
        test_system_transaction_reconstructor(expected_rollup_id(1));

    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: calldata,
        blobs: &[],
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        expected_l2_system_address: TEST_SYSTEM_ADDRESS,
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert_eq!(run.unwrap().into_inner(), expected_hash);

    // The DA describes the CALL, so an entry differing only by its attached
    // state update projects to the same action — EEZ binds state updates
    // through the entry hashes in the public input, not through the DA. A
    // mismatch therefore has to be a different call.
    let mut mismatched_da = batch;
    let mut wrong_call = mismatched_da.entries[1].clone();
    wrong_call.l2ToL1Calls[0].value += alloy_primitives::U256::from(1);
    mismatched_da.callData = settlement::encode_da_payload(&[vec![user]], &[wrong_call]).into();
    let mismatched_calldata = eez_protocol::entries::encode_postbatch(&mismatched_da);
    let run = run_settlement(SettlementInput {
        submitted_post_batch_calldata: mismatched_calldata,
        blobs: &[],
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
            settlement::DaPayloadError::ActionMismatch { .. }
        ))
    ));
}

fn inbound_blob_settlement_fixture() -> BlobSettlementFixture {
    let value = U256::from(7);
    let (transaction, call_hash, return_data, sidecar) = strict_inbound_transaction(value);
    let body: eez_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![transaction],
        ..Default::default()
    };
    let validated = blob_settlement_window(
        body,
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );
    let mut batch = anchor_batch_spanning(5, 6);
    batch.entries[0].rollupUpdates[0].newRoot = empty_prefix_candidate();
    batch.entries.push(ExecutionEntrySol {
        rollupUpdates: vec![RollupUpdateSol {
            rollupId: 1,
            currentRoot: empty_prefix_candidate(),
            newRoot: block_hash_of(6),
            etherDelta: eez_protocol::abi::u256_to_i192(value).unwrap(),
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
    let action =
        eez_protocol::entries::manifest::action_from_entry(&sidecar, eez_protocol::RollupId(1))
            .unwrap();
    let span = vec![eez_payload_codec::SpanBlock::default(); 2];
    (batch, validated, span, action)
}

fn outbound_blob_settlement_fixture() -> BlobSettlementFixture {
    use reth_primitives_traits::BlockBody as _;

    let (window_pre, _, window_post) = window_endpoints(5, 6);
    let batch = outbound_batch(window_pre, window_post);
    let call = &batch.entries[1].l2ToL1Calls[0];
    let call_hash = eez_protocol::l2_outbound_call_hash(
        eez_protocol::CallHashInput {
            call_mode: eez_protocol::CallMode::Mutable,
            source_address: call.sourceAddress,
            source_rollup_id: eez_protocol::RollupId(1),
            target_address: call.targetAddress,
            target_rollup_id: eez_protocol::RollupId::MAINNET,
            value: call.value,
            data: &call.data,
        },
        0,
    );
    let user_body: eez_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions: vec![non_system_transaction()],
        ..Default::default()
    };
    let user = user_body
        .encoded_2718_transactions_iter()
        .next()
        .expect("outbound user transaction");
    let mut sidecar = batch.entries[1].clone();
    sidecar.rollupUpdates.clear();
    sidecar.rollingHash = B256::ZERO;
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(sidecar.clone(), Bytes::from(user.clone()))],
        &[],
        &system_transaction_context(),
        0,
    )
    .unwrap();
    let transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs)
        .into_iter()
        .map(|raw| {
            <eez_primitives::EezTxEnvelope as alloy_eips::Decodable2718>::decode_2718_exact(
                raw.as_ref(),
            )
            .unwrap()
        })
        .collect();
    let body: eez_primitives::BlockBody = alloy_consensus::BlockBody {
        transactions,
        ..Default::default()
    };
    let validated = blob_settlement_window(
        body,
        validate::SettlementBlockEvidence::for_test(
            vec![true, false],
            vec![validate::OutboundEventObservation::decoded_for_test(
                1, 0, call_hash, 0,
            )],
        ),
    );
    let span = vec![
        eez_payload_codec::SpanBlock::default(),
        eez_payload_codec::SpanBlock {
            transactions: vec![user],
            ..Default::default()
        },
    ];
    let action =
        eez_protocol::entries::manifest::action_from_entry(&sidecar, eez_protocol::RollupId(1))
            .unwrap();
    (batch, validated, span, action)
}

fn blob_settlement_window(
    body: eez_primitives::BlockBody,
    evidence: validate::SettlementBlockEvidence,
) -> validate::ValidatedWindow {
    let transaction_count = body.transactions.len();
    let settling = validate::ValidatedBlock::for_test(
        6,
        alloy_rlp::encode(eez_primitives::Block::new(
            alloy_consensus::Header {
                timestamp: 124,
                ..Default::default()
            },
            body,
        )),
        evidence,
    );
    let preceding = validate::ValidatedBlock::for_test(
        5,
        alloy_rlp::encode(eez_primitives::Block::default()),
        validate::SettlementBlockEvidence::for_test(Vec::new(), Vec::new()),
    );
    validate::ValidatedWindow::for_test(
        block_hash_of(4),
        block_hash_of(6),
        vec![preceding],
        validate::testing::settling_block_for_test(
            settling,
            vec![true; transaction_count],
            vec![
                pre_execution_checkpoint(empty_prefix_candidate()),
                checkpoint(transaction_count - 1, block_hash_of(6)),
            ],
        ),
    )
}

type BlobSettlementFixture = (
    eez_protocol::abi::EvmBatch,
    validate::ValidatedWindow,
    Vec<eez_payload_codec::SpanBlock>,
    eez_payload_codec::Action,
);

#[tokio::test]
async fn blob_settlement_rejects_mismatched_and_malformed_da() {
    for (direction, (mut batch, validated, span, action)) in [
        ("inbound", inbound_blob_settlement_fixture()),
        ("outbound", outbound_blob_settlement_fixture()),
    ] {
        let blobs =
            eez_payload_codec::encode_blob_container(1, &span, std::slice::from_ref(&action))
                .unwrap();
        batch.blockNumber = 0;
        batch.callData = Bytes::new();
        batch.blobIndices = vec![U256::ZERO];
        let hashes = eez_blob::versioned_hashes(&blobs).unwrap();
        let expected = eez_protocol::public_inputs::public_inputs_hashes_with_blobs(
            &batch,
            test_proof_system_vkey().get(),
            &hashes,
        )
        .unwrap()[0];
        let cancellation = CancellationToken::default();
        let reconstructor = test_system_transaction_reconstructor(expected_rollup_id(1));
        let run = |blobs: &[Vec<u8>]| {
            run_settlement(SettlementInput {
                submitted_post_batch_calldata: eez_protocol::entries::encode_postbatch(&batch),
                validated_window: &validated,
                expected_rollup_id: expected_rollup_id(1),
                expected_l2_system_address: TEST_SYSTEM_ADDRESS,
                proof_system_vkey: test_proof_system_vkey(),
                expected_proof_system: test_proof_system(),
                system_transaction_reconstructor: &reconstructor,
                cancellation: &cancellation,
                blobs,
            })
        };
        assert_eq!(run(&blobs).unwrap().into_inner(), expected, "{direction}");

        let mut wrong_action = action;
        wrong_action.data.push(0xff);
        let substituted =
            eez_payload_codec::encode_blob_container(1, &span, &[wrong_action]).unwrap();
        assert_eq!(
            run(&substituted).unwrap_err().gate(),
            "da_payload",
            "{direction}: mismatched action"
        );
        let mut malformed = blobs;
        malformed[0][0] = 1;
        assert_eq!(
            run(&malformed).unwrap_err().gate(),
            "da_payload",
            "{direction}: malformed blob"
        );
    }
}
