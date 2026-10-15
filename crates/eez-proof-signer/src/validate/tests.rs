use super::testing::{StubBackend, backend_output_for};
use super::*;
use alloy_consensus::{SignableTransaction as _, TxLegacy};

#[test]
fn execution_witness_conversion_preserves_every_field() {
    let witness = into_execution_witness(WireExecutionWitness {
        state: vec![vec![0x00, 0xff], vec![0x01]],
        codes: vec![vec![0x02]],
        keys: vec![vec![], vec![0x03]],
        headers: vec![vec![0x04, 0x05]],
    });
    let bytes = |items: &[alloy_primitives::Bytes]| {
        items.iter().map(|item| item.to_vec()).collect::<Vec<_>>()
    };
    assert_eq!(bytes(&witness.state), [vec![0x00, 0xff], vec![0x01]]);
    assert_eq!(bytes(&witness.codes), [vec![0x02]]);
    assert_eq!(bytes(&witness.keys), [vec![], vec![0x03]]);
    assert_eq!(bytes(&witness.headers), [vec![0x04, 0x05]]);
}

#[test]
fn wire_witness_item_count_includes_every_collection() {
    let witness = WireExecutionWitness {
        state: vec![Vec::new(); 2],
        codes: vec![Vec::new()],
        keys: vec![Vec::new(); 2],
        headers: vec![Vec::new()],
    };

    assert_eq!(wire_witness_item_count(&witness), 6);
}

fn admitted_block(number: u64, hash: u8) -> AdmittedBlock {
    let mut input = AdmittedBlock::test(number, 0, hash);
    input.rlp = alloy_rlp::encode(EthereumBlock::default());
    input
}

/// A block sealed on the `repeat_byte(n - 1) -> repeat_byte(n)` grid, so a
/// window built from consecutive numbers has three distinct endpoints.
fn chained_admitted_block(number: u64) -> AdmittedBlock {
    let mut input = admitted_block(number, u8::try_from(number).unwrap());
    input.claimed_parent_hash = B256::repeat_byte(u8::try_from(number - 1).unwrap());
    input
}

fn admitted_block_with_transactions(number: u64, hash: u8, count: usize) -> AdmittedBlock {
    let transaction: eez_primitives::EezTxEnvelope = TxLegacy::default()
        .into_signed(alloy_primitives::Signature::test_signature())
        .into();
    let body = alloy_consensus::BlockBody {
        transactions: vec![transaction; count],
        ..Default::default()
    };

    let mut input = admitted_block(number, hash);
    input.rlp = alloy_rlp::encode(EthereumBlock::new(Default::default(), body));
    input
}

fn checkpoint(transaction_index: usize, state_root: u8) -> StateCheckpoint {
    StateCheckpoint {
        at: CheckpointAt::Transaction(transaction_index),
        state_root: B256::repeat_byte(state_root),
        block_hash: B256::with_last_byte(0xc0 ^ (transaction_index as u8)),
    }
}

#[test]
fn accepts_backend_output_consistent_with_the_window() {
    let window = [admitted_block(5, 0x05)];
    let validator = StubBackend::new(vec![Ok(backend_output_for(&window))]);
    let validated = validator.validate(&window).unwrap();
    assert!(validated.settling_block().receipt_successes().is_empty());
    assert_eq!(validator.remaining_test_actions(), Some(0));
}

#[test]
fn rejects_an_empty_window() {
    let validator = StubBackend::new(vec![Ok(backend_output_for(&[]))]);

    assert!(matches!(
        validator.validate(&[]),
        Err(ValidationError::Rejected(reason))
            if reason == "refusing to validate an empty window"
    ));
    assert_eq!(validator.remaining_test_actions(), Some(1));
}

#[test]
fn rejects_backend_output_with_the_wrong_block_count() {
    let window = [admitted_block(5, 0x05)];
    let two_blocks = backend_output_for(&[admitted_block(5, 0x05), admitted_block(6, 0x06)]);
    let validator = StubBackend::new(vec![Ok(two_blocks)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_backend_output_with_a_mismatched_hash() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].computed_hash = B256::repeat_byte(0xee);
    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(validator.validate(&window).is_err());
}

#[test]
fn rejects_system_sender_flags_that_do_not_cover_the_block() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].settlement_evidence.system_sender_flags = vec![false];
    let validator = StubBackend::new(vec![Ok(output)]);

    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn decoded_block_checks_execution_evidence_before_it_can_be_shared() {
    let block = EthereumBlock::new(
        alloy_consensus::Header {
            number: 11,
            parent_hash: B256::repeat_byte(10),
            ..Default::default()
        },
        Default::default(),
    );
    let admitted = AdmittedBlock {
        declared_number: 11,
        claimed_hash: block.header.hash_slow(),
        claimed_parent_hash: block.header.parent_hash,
        rlp: alloy_rlp::encode(block),
        witness: Default::default(),
    };
    for defect in [
        "none",
        "execution hash",
        "receipts",
        "senders",
        "event",
        "checkpoint",
    ] {
        let decoded = support::decode_match_and_recover_signers(
            &admitted,
            &reth_chainspec::ChainSpec::default(),
        )
        .unwrap();
        let mut output = backend_output_for(std::slice::from_ref(&admitted))
            .blocks
            .pop()
            .unwrap();
        match defect {
            "execution hash" => output.computed_hash = B256::ZERO,
            "receipts" => output.receipt_successes.push(true),
            "senders" => output.settlement_evidence.system_sender_flags.push(false),
            "event" => output.settlement_evidence.observed_outbound_events.push(
                OutboundEventObservation::decoded_for_test(0, 0, B256::ZERO, 0),
            ),
            "checkpoint" => output.transaction_state_checkpoints.push(checkpoint(0, 0)),
            _ => {}
        }
        let checked = decoded.finish(output, B256::repeat_byte(9), true);
        if defect == "none" {
            let checked = checked.unwrap();
            assert_eq!(checked.number(), admitted.declared_number());
            assert_eq!(checked.hash(), admitted.claimed_hash());
            assert_eq!(alloy_rlp::encode(checked.decoded()), admitted.rlp());
            assert_eq!(checked.pre_state_root, B256::repeat_byte(9));
        } else {
            assert!(
                matches!(checked, Err(ValidationError::InvalidBackendOutput(_))),
                "{defect}"
            );
        }
    }
}

#[test]
fn rejects_an_outbound_observation_outside_the_block() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0]
        .settlement_evidence
        .observed_outbound_events = vec![OutboundEventObservation::malformed_for_test(2, 0)];
    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_unordered_outbound_observations() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0]
        .settlement_evidence
        .observed_outbound_events = vec![
        OutboundEventObservation::malformed_for_test(1, 0),
        OutboundEventObservation::malformed_for_test(0, 0),
    ];
    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn accepts_sparse_ordered_transaction_state_checkpoints() {
    let window = [admitted_block_with_transactions(5, 0x05, 3)];
    let pre_execution = StateCheckpoint {
        at: CheckpointAt::PreExecution,
        ..checkpoint(0, 0x99)
    };
    for checkpoints in [
        vec![checkpoint(0, 0xaa), checkpoint(2, 0xcc)],
        vec![pre_execution, checkpoint(0, 0xaa), checkpoint(2, 0xcc)],
    ] {
        let mut output = backend_output_for(&window);
        output.blocks[0].transaction_state_checkpoints = checkpoints;
        let validator = StubBackend::new(vec![Ok(output)]);
        assert!(validator.validate(&window).is_ok());
    }
}

#[test]
fn rejects_incomplete_or_surplus_transaction_statuses() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];

    let mut short = backend_output_for(&window);
    short.blocks[0].receipt_successes = vec![true];
    let short_validator = StubBackend::new(vec![Ok(short)]);
    assert!(matches!(
        short_validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));

    let mut surplus = backend_output_for(&window);
    surplus.blocks[0].receipt_successes = vec![true; 3];
    let surplus_validator = StubBackend::new(vec![Ok(surplus)]);
    assert!(matches!(
        surplus_validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_duplicate_or_descending_state_checkpoints() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let pre_execution = StateCheckpoint {
        at: CheckpointAt::PreExecution,
        ..checkpoint(0, 0x99)
    };
    for checkpoints in [
        vec![checkpoint(0, 0xaa), checkpoint(0, 0xbb)],
        vec![pre_execution, pre_execution],
        vec![checkpoint(1, 0xbb), checkpoint(0, 0xaa)],
        vec![checkpoint(0, 0xaa), pre_execution],
    ] {
        let mut output = backend_output_for(&window);
        output.blocks[0].transaction_state_checkpoints = checkpoints;
        let validator = StubBackend::new(vec![Ok(output)]);
        assert!(matches!(
            validator.validate(&window),
            Err(ValidationError::InvalidBackendOutput(_))
        ));
    }
}

#[test]
fn rejects_out_of_bounds_transaction_state_checkpoint_indices() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(2, 0xcc)];

    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn accepts_a_last_transaction_checkpoint_before_post_block_changes() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(1, 0xaa)];
    let validator = StubBackend::new(vec![Ok(output)]);

    assert!(validator.validate(&window).is_ok());
}

#[test]
fn rejects_transaction_state_checkpoints_when_the_block_rlp_is_malformed() {
    let mut input = admitted_block(5, 0x05);
    input.rlp = vec![0xff];
    let window = [input];

    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = Vec::new();
    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_transaction_state_checkpoints_when_the_block_rlp_has_trailing_data() {
    let mut input = admitted_block_with_transactions(5, 0x05, 2);
    input.rlp.push(0x80);
    let window = [input];

    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = Vec::new();
    let validator = StubBackend::new(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn a_backend_rejection_and_stub_exhaustion_both_fail() {
    let window = [admitted_block(5, 0x05)];
    let validator = StubBackend::new(vec![Err("re-execution mismatch".to_owned())]);
    assert!(validator.validate(&window).is_err());
    // The canned responses are spent; the next call fails loudly.
    assert!(validator.validate(&window).is_err());
}

#[test]
fn rejects_transaction_state_checkpoints_on_preceding_blocks() {
    let two_block_window = [
        admitted_block_with_transactions(5, 0x05, 1),
        admitted_block(6, 0x06),
    ];
    let mut preceding_checkpoints = backend_output_for(&two_block_window);
    preceding_checkpoints.blocks[0].transaction_state_checkpoints = vec![checkpoint(0, 0xaa)];
    let validator = StubBackend::new(vec![Ok(preceding_checkpoints)]);
    assert!(matches!(
        validator.validate(&two_block_window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn normalizes_validated_output_for_settlement() {
    let window = vec![
        chained_admitted_block(5),
        chained_admitted_block(6),
        chained_admitted_block(7),
    ];
    let mut output = backend_output_for(&window);
    for (block, post_state_root) in output.blocks.iter_mut().zip([
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x12),
        B256::repeat_byte(0x13),
    ]) {
        block.post_state_root = post_state_root;
    }
    let validator = StubBackend::new(vec![Ok(output)]);

    let validated = validate_window(
        &validator,
        AdmittedBlocks::for_test(window),
        &CancellationToken::default(),
    )
    .unwrap();

    // Endpoints come from the block headers; the re-executed state roots set
    // above must reach none of them.
    assert_eq!(validated.window_pre_block_hash(), B256::repeat_byte(0x04));
    assert_eq!(validated.settling_pre_block_hash(), B256::repeat_byte(0x06));
    assert_eq!(validated.window_post_block_hash(), B256::repeat_byte(0x07));
    assert_eq!(
        validated
            .preceding_blocks()
            .iter()
            .map(|block| block.number())
            .collect::<Vec<_>>(),
        [5, 6]
    );
    assert_eq!(validated.settling_block().number(), 7);
}

#[test]
fn uses_the_window_pre_state_as_the_settling_pre_state_for_one_block() {
    let window = vec![chained_admitted_block(5)];
    let mut output = backend_output_for(&window);
    output.blocks[0].post_state_root = B256::repeat_byte(0x11);
    let validator = StubBackend::new(vec![Ok(output)]);

    let validated = validate_window(
        &validator,
        AdmittedBlocks::for_test(window),
        &CancellationToken::default(),
    )
    .unwrap();

    assert_eq!(validated.settling_pre_block_hash(), B256::repeat_byte(0x04));
    assert_eq!(
        validated.window_pre_block_hash(),
        validated.settling_pre_block_hash()
    );
    assert!(validated.preceding_blocks().is_empty());
}
