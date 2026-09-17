use super::testing::backend_output_for;
use super::*;
use alloy_consensus::{SignableTransaction as _, TxLegacy};

fn admitted_block(number: u64, hash: u8) -> AdmittedBlock {
    let mut input = AdmittedBlock::test(number, 0, hash);
    input.rlp = alloy_rlp::encode(EthereumBlock::default());
    input
}

fn admitted_block_with_transactions(number: u64, hash: u8, count: usize) -> AdmittedBlock {
    let transaction: alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844> =
        TxLegacy::default()
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

fn checkpoint(transaction_index: usize, state_root: u8) -> TransactionStateCheckpoint {
    TransactionStateCheckpoint {
        transaction_index,
        state_root: B256::repeat_byte(state_root),
    }
}

#[test]
fn accepts_backend_output_consistent_with_the_window() {
    let window = [admitted_block(5, 0x05)];
    let validator = Validator::stub(vec![Ok(backend_output_for(&window))]);
    let validated = validator.validate(&window).unwrap();
    assert!(validated.settling_block().receipt_successes().is_empty());
    assert_eq!(validator.stub_remaining(), 0);
}

#[test]
fn rejects_an_empty_window() {
    let validator = Validator::stub(vec![Ok(backend_output_for(&[]))]);

    assert!(matches!(
        validator.validate(&[]),
        Err(ValidationError::Rejected(reason))
            if reason == "refusing to validate an empty window"
    ));
    assert_eq!(validator.stub_remaining(), 1);
}

#[test]
fn rejects_backend_output_with_the_wrong_block_count() {
    let window = [admitted_block(5, 0x05)];
    let two_blocks = backend_output_for(&[admitted_block(5, 0x05), admitted_block(6, 0x06)]);
    let validator = Validator::stub(vec![Ok(two_blocks)]);
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
    let validator = Validator::stub(vec![Ok(output)]);
    assert!(validator.validate(&window).is_err());
}

#[test]
fn rejects_backend_output_with_a_mismatched_decoded_number() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].decoded_number = 6;
    let validator = Validator::stub(vec![Ok(output)]);

    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_backend_output_with_a_mismatched_decoded_parent() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].decoded_parent_hash = B256::repeat_byte(0xee);
    let validator = Validator::stub(vec![Ok(output)]);

    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_backend_output_with_a_mismatched_decoded_transaction_count() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].decoded_transaction_count = 1;
    let validator = Validator::stub(vec![Ok(output)]);

    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_system_sender_flags_that_do_not_cover_the_block() {
    let window = [admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.blocks[0].settlement_evidence.system_sender_flags = vec![false];
    let validator = Validator::stub(vec![Ok(output)]);

    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_an_outbound_observation_outside_the_block() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0]
        .settlement_evidence
        .observed_outbound_events = vec![OutboundEventObservation::malformed_for_test(2, 0)];
    let validator = Validator::stub(vec![Ok(output)]);
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
    let validator = Validator::stub(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn accepts_sparse_ordered_transaction_state_checkpoints() {
    let window = [admitted_block_with_transactions(5, 0x05, 3)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(0, 0xaa), checkpoint(2, 0xcc)];
    let validator = Validator::stub(vec![Ok(output)]);
    assert!(validator.validate(&window).is_ok());
}

#[test]
fn rejects_incomplete_or_surplus_transaction_statuses() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];

    let mut short = backend_output_for(&window);
    short.blocks[0].receipt_successes = vec![true];
    let short_validator = Validator::stub(vec![Ok(short)]);
    assert!(matches!(
        short_validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));

    let mut surplus = backend_output_for(&window);
    surplus.blocks[0].receipt_successes = vec![true; 3];
    let surplus_validator = Validator::stub(vec![Ok(surplus)]);
    assert!(matches!(
        surplus_validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_duplicate_transaction_state_checkpoint_indices() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(0, 0xaa), checkpoint(0, 0xbb)];

    let validator = Validator::stub(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_descending_transaction_state_checkpoint_indices() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(1, 0xbb), checkpoint(0, 0xaa)];

    let validator = Validator::stub(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn rejects_out_of_bounds_transaction_state_checkpoint_indices() {
    let window = [admitted_block_with_transactions(5, 0x05, 2)];
    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = vec![checkpoint(2, 0xcc)];

    let validator = Validator::stub(vec![Ok(output)]);
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
    let validator = Validator::stub(vec![Ok(output)]);

    assert!(validator.validate(&window).is_ok());
}

#[test]
fn rejects_transaction_state_checkpoints_when_the_block_rlp_is_malformed() {
    let mut input = admitted_block(5, 0x05);
    input.rlp = vec![0xff];
    let window = [input];

    let mut output = backend_output_for(&window);
    output.blocks[0].transaction_state_checkpoints = Vec::new();
    let validator = Validator::stub(vec![Ok(output)]);
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
    let validator = Validator::stub(vec![Ok(output)]);
    assert!(matches!(
        validator.validate(&window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn a_backend_rejection_and_stub_exhaustion_both_fail() {
    let window = [admitted_block(5, 0x05)];
    let validator = Validator::stub(vec![Err("re-execution mismatch".to_owned())]);
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
    let validator = Validator::stub(vec![Ok(preceding_checkpoints)]);
    assert!(matches!(
        validator.validate(&two_block_window),
        Err(ValidationError::InvalidBackendOutput(_))
    ));
}

#[test]
fn normalizes_validated_output_for_settlement() {
    let window = vec![
        admitted_block(5, 0x05),
        admitted_block(6, 0x06),
        admitted_block(7, 0x07),
    ];
    let mut output = backend_output_for(&window);
    output.pre_state_root = B256::repeat_byte(0x10);
    for (block, post_state_root) in output.blocks.iter_mut().zip([
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x12),
        B256::repeat_byte(0x13),
    ]) {
        block.post_state_root = post_state_root;
    }
    let validator = Validator::stub(vec![Ok(output)]);

    let validated = validator
        .validate_window(
            AdmittedBlocks::for_test(window),
            &CancellationToken::default(),
        )
        .unwrap();

    assert_eq!(validated.window_pre_state_root(), B256::repeat_byte(0x10));
    assert_eq!(validated.settling_pre_state_root(), B256::repeat_byte(0x12));
    assert_eq!(validated.window_post_state_root(), B256::repeat_byte(0x13));
    assert_eq!(
        validated
            .preceding_blocks()
            .iter()
            .map(ValidatedBlock::number)
            .collect::<Vec<_>>(),
        [5, 6]
    );
    assert_eq!(validated.settling_block().block().number(), 7);
}

#[test]
fn uses_the_window_pre_state_as_the_settling_pre_state_for_one_block() {
    let window = vec![admitted_block(5, 0x05)];
    let mut output = backend_output_for(&window);
    output.pre_state_root = B256::repeat_byte(0x10);
    output.blocks[0].post_state_root = B256::repeat_byte(0x11);
    let validator = Validator::stub(vec![Ok(output)]);

    let validated = validator
        .validate_window(
            AdmittedBlocks::for_test(window),
            &CancellationToken::default(),
        )
        .unwrap();

    assert_eq!(
        validated.settling_pre_state_root(),
        validated.window_pre_state_root()
    );
    assert!(validated.preceding_blocks().is_empty());
}
