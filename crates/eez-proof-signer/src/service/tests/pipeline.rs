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
            SettlementPipelineError::StateDeltaChain(
                crate::settlement::StateDeltaChainError::NoEntries,
            ),
            "state_delta_chain",
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
        8,
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
    let settling_block = validate::ValidatedBlock::for_test(
        5,
        alloy_rlp::encode(block),
        validate::SettlementBlockEvidence::for_test(vec![true], Vec::new()),
    );

    let mut batch = anchor_batch();
    batch.entries.push(ExecutionEntrySol {
        stateDeltas: vec![StateDeltaSol {
            rollupId: U256::from(1),
            currentState: B256::ZERO,
            newState: B256::ZERO,
            etherDelta: I256::try_from(value).unwrap(),
        }],
        proxyEntryHash: call_hash,
        destinationRollupId: U256::from(1),
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: return_data,
        rollingHash: B256::ZERO,
    });
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
        submitted_post_batch_calldata: calldata,
        validated_window: &validated,
        expected_rollup_id: expected_rollup_id(1),
        proof_system_vkey: test_proof_system_vkey(),
        expected_proof_system: test_proof_system(),
        system_transaction_reconstructor: &system_transaction_reconstructor,
        cancellation: &cancellation,
    });

    assert_eq!(run.unwrap().into_inner(), expected_hash);
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
