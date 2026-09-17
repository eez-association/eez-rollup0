use super::*;

#[test]
fn derives_system_flags_and_effect_candidates_from_exact_block_rlp() {
    let rlp = block_rlp(vec![
        transaction(SYSTEM_TX),
        transaction(USER_INBOUND_SELECTOR_TX),
        transaction(SYSTEM_TX),
        transaction(SYSTEM_TX),
    ]);

    let facts = inspect_settling_block(&rlp, &[true; 4], expected_rollup_id()).unwrap();

    assert_eq!(facts.system_sender_flags(), [true, false, true, true]);
    assert_eq!(facts.effect_candidate_positions(), [1, 2, 3]);
}

#[test]
fn validated_settling_block_uses_backend_recovered_sender_facts() {
    let rlp = block_rlp(vec![
        transaction(SYSTEM_TX),
        transaction(USER_INBOUND_SELECTOR_TX),
        transaction(USER_INBOUND_SELECTOR_TX),
    ]);
    let observation = OutboundEventObservation::decoded_for_test(2, 1, B256::repeat_byte(0x44), 0);
    let block = ValidatedBlock::for_test(
        42,
        rlp,
        SettlementBlockEvidence::for_test(vec![true, false, false], vec![observation]),
    );

    let facts = inspect_validated_settling_block(&block, &[true; 3], expected_rollup_id()).unwrap();

    assert_eq!(facts.system_sender_flags(), [true, false, false]);
    assert_eq!(facts.effect_candidate_positions(), [1, 2]);
    assert!(facts.inbound_candidates().is_empty());
    assert_eq!(facts.outbound_event_candidates(), [observation]);
}

#[test]
fn validated_settling_block_rejects_unbound_sender_fact_count() {
    let block = validated_block(42, block_rlp(vec![transaction(SYSTEM_TX)]), Vec::new());

    assert_eq!(
        inspect_validated_settling_block(&block, &[true], expected_rollup_id()),
        Err(BlockInspectionError::SystemSenderCount {
            block_number: 42,
            required: 1,
            actual: 0,
        })
    );
}

#[test]
fn typed_transactions_can_be_system_transactions() {
    let rlp = block_rlp(vec![transaction(EIP1559_SYSTEM_TX)]);

    let facts = inspect_settling_block(&rlp, &[true], expected_rollup_id()).unwrap();

    assert_eq!(facts.system_sender_flags(), [true]);
    assert_eq!(facts.effect_candidate_positions(), [0]);
}

#[test]
fn signature_recovery_failures_are_not_system_transactions() {
    let invalid_signature = TransactionSigned::new_unhashed(
        transaction(SYSTEM_TX).into_typed_transaction(),
        Signature::new(U256::ZERO, U256::ZERO, false),
    );
    let rlp = block_rlp(vec![invalid_signature]);

    let facts = inspect_settling_block(&rlp, &[false], expected_rollup_id()).unwrap();

    assert_eq!(facts.system_sender_flags(), [false]);
    assert_eq!(facts.effect_candidate_positions(), [0]);
}

#[test]
fn reserved_system_sender_cannot_masquerade_as_a_user() {
    for transaction in [
        transaction(SYSTEM_SIGNER_OTHER_TARGET_TX),
        transaction(CREATE_TX),
    ] {
        let rlp = block_rlp(vec![transaction]);
        assert_eq!(
            inspect_settling_block(&rlp, &[true], expected_rollup_id()),
            Err(BlockInspectionError::ReservedSystemSender { index: 0 })
        );
    }
}

#[test]
fn malformed_or_trailing_rlp_is_rejected() {
    let mut trailing = block_rlp(vec![transaction(SYSTEM_TX)]);
    trailing.push(0);

    for rlp in [&[0xff][..], trailing.as_slice()] {
        assert!(matches!(
            inspect_settling_block(rlp, &[], expected_rollup_id()),
            Err(BlockInspectionError::InvalidRlp { .. })
        ));
    }
}

#[test]
fn exact_empty_block_is_accepted() {
    let facts = inspect_settling_block(&block_rlp(Vec::new()), &[], expected_rollup_id()).unwrap();

    assert!(facts.system_sender_flags().is_empty());
    assert!(facts.effect_candidate_positions().is_empty());
}

#[test]
fn statuses_must_cover_exactly_every_transaction() {
    let rlp = block_rlp(vec![
        transaction(SYSTEM_TX),
        transaction(SYSTEM_SIGNER_OTHER_TARGET_TX),
    ]);

    assert_eq!(
        inspect_settling_block(&rlp, &[true], expected_rollup_id()),
        Err(BlockInspectionError::StatusCount {
            required: 2,
            actual: 1,
        })
    );
    assert_eq!(
        inspect_settling_block(&rlp, &[true; 3], expected_rollup_id()),
        Err(BlockInspectionError::StatusCount {
            required: 2,
            actual: 3,
        })
    );
}

#[test]
fn reverted_system_transactions_are_rejected_but_user_reverts_are_allowed() {
    let rlp = block_rlp(vec![
        transaction(SYSTEM_TX),
        transaction(USER_INBOUND_SELECTOR_TX),
    ]);

    assert_eq!(
        inspect_settling_block(&rlp, &[false, true], expected_rollup_id()),
        Err(BlockInspectionError::RevertedSystemTransaction { index: 0 })
    );
    let facts = inspect_settling_block(&rlp, &[true, false], expected_rollup_id()).unwrap();
    assert_eq!(facts.system_sender_flags(), [true, false]);
    assert_eq!(facts.effect_candidate_positions(), [1]);
}

#[test]
fn reverted_inbound_system_transactions_remain_positioned_candidates() {
    let rlp = block_rlp(vec![target_system_inbound_transaction()]);

    let facts = inspect_settling_block(&rlp, &[false], expected_rollup_id()).unwrap();

    assert_eq!(facts.inbound_candidates().len(), 1);
    assert_eq!(facts.inbound_candidates()[0].transaction_index, 0);
    assert_eq!(
        facts.inbound_candidates()[0].inspection,
        Err(InboundObservationError::RevertedTransaction)
    );
}

#[test]
fn settling_inbound_candidates_are_retained_independently_of_effect_candidates() {
    let rlp = block_rlp(vec![
        target_system_inbound_transaction(),
        target_user_inbound_selector_transaction(),
    ]);

    let facts = inspect_settling_block(&rlp, &[true, true], expected_rollup_id()).unwrap();

    assert_eq!(facts.system_sender_flags(), [true, false]);
    assert_eq!(facts.effect_candidate_positions(), [1]);
    assert_eq!(facts.inbound_candidates().len(), 1);
    assert_eq!(facts.inbound_candidates()[0].transaction_index, 0);
    assert!(facts.inbound_candidates()[0].inspection.is_ok());
}

#[test]
fn strict_inbound_observation_binds_the_envelope_entry_and_outcome() {
    let value = U256::from(7);
    let calldata = strict_inbound_calldata(value, true);

    let observation =
        inspect_inbound_candidate(value, &calldata, true, expected_rollup_id()).unwrap();

    assert_eq!(observation.value, value);
    assert_eq!(observation.return_data, Bytes::from_static(&[0x01, 0x02]));
    assert_ne!(observation.recomputed_call_hash, B256::ZERO);
}

#[test]
fn strict_inbound_observation_rejects_unbound_envelope_fields() {
    let value = U256::from(7);
    let calldata = strict_inbound_calldata(value, true);

    assert_eq!(
        inspect_inbound_candidate(value, &calldata, false, expected_rollup_id()),
        Err(InboundObservationError::RevertedTransaction)
    );
    assert_eq!(
        inspect_inbound_candidate(U256::from(8), &calldata, true, expected_rollup_id(),),
        Err(InboundObservationError::NativeValueMismatch {
            expected: value,
            actual: U256::from(8),
        })
    );

    let mut trailing = calldata.clone();
    trailing.push(0);
    assert!(matches!(
        inspect_inbound_candidate(value, &trailing, true, expected_rollup_id()),
        Err(InboundObservationError::InvalidAbi { .. } | InboundObservationError::NonCanonicalAbi)
    ));
}

#[test]
fn strict_inbound_observation_rejects_composer_controlled_shape_changes() {
    use eez_protocol::abi::executeIncomingCrossChainCallCall;

    let value = U256::from(7);
    let calldata = strict_inbound_calldata(value, true);
    let call = executeIncomingCrossChainCallCall::abi_decode(&calldata).unwrap();

    let mut wrong_source_rollup = call.clone();
    wrong_source_rollup.sourceRollup = 2;
    wrong_source_rollup._entries[0].incomingCalls[0].sourceRollupId = 2;
    assert_eq!(
        inspect_inbound_candidate(
            value,
            &wrong_source_rollup.abi_encode(),
            true,
            expected_rollup_id(),
        ),
        Err(InboundObservationError::SourceRollup { actual: 2 })
    );

    type Mutation = fn(&mut executeIncomingCrossChainCallCall);
    let mutations: [(Mutation, InboundObservationError); 12] = [
        (
            |call| call._entries.clear(),
            InboundObservationError::EntryCount { actual: 0 },
        ),
        (
            |call| call._entries.push(call._entries[0].clone()),
            InboundObservationError::EntryCount { actual: 2 },
        ),
        (
            |call| call._staticEntries.push(Default::default()),
            InboundObservationError::StaticEntryCount { actual: 1 },
        ),
        (
            |call| call._entries[0].incomingCalls.clear(),
            InboundObservationError::InvalidEntryShape {
                field: "incomingCalls",
            },
        ),
        (
            |call| {
                let duplicate = call._entries[0].incomingCalls[0].clone();
                call._entries[0].incomingCalls.push(duplicate);
            },
            InboundObservationError::InvalidEntryShape {
                field: "incomingCalls",
            },
        ),
        (
            |call| call._entries[0].success = false,
            InboundObservationError::InvalidEntryShape { field: "success" },
        ),
        (
            |call| {
                call._entries[0]
                    .expectedOutgoingCalls
                    .push(l2_expected_outgoing_call());
            },
            InboundObservationError::InvalidEntryShape {
                field: "expectedOutgoingCalls",
            },
        ),
        (
            |call| call._entries[0].incomingCalls[0].targetAddress = Address::ZERO,
            InboundObservationError::OuterInnerMismatch {
                field: "destination",
            },
        ),
        (
            |call| call._entries[0].incomingCalls[0].value = U256::from(8),
            InboundObservationError::OuterInnerMismatch { field: "value" },
        ),
        (
            |call| {
                call._entries[0].incomingCalls[0].data = Bytes::from_static(&[0xff]);
            },
            InboundObservationError::OuterInnerMismatch { field: "data" },
        ),
        (
            |call| call._entries[0].incomingCalls[0].sourceAddress = Address::ZERO,
            InboundObservationError::OuterInnerMismatch {
                field: "sourceAddress",
            },
        ),
        (
            |call| call._entries[0].incomingCalls[0].sourceRollupId = 1,
            InboundObservationError::OuterInnerMismatch {
                field: "sourceRollup",
            },
        ),
    ];
    for (mutate, expected) in mutations {
        let mut malformed = call.clone();
        mutate(&mut malformed);
        assert_eq!(
            inspect_inbound_candidate(value, &malformed.abi_encode(), true, expected_rollup_id(),),
            Err(expected)
        );
    }

    for (mutate, field) in [
        (
            (|call: &mut executeIncomingCrossChainCallCall| {
                call._entries[0].incomingCalls[0].revertNextNCalls = 1;
            }) as Mutation,
            "revertNextNCalls",
        ),
        (
            (|call: &mut executeIncomingCrossChainCallCall| {
                call._entries[0].incomingCalls[0].isStatic = true;
            }) as Mutation,
            "isStatic",
        ),
        (
            (|call: &mut executeIncomingCrossChainCallCall| {
                call._entries[0].incomingCalls[0].gas = 1;
            }) as Mutation,
            "gas",
        ),
    ] {
        let mut malformed = call.clone();
        mutate(&mut malformed);
        assert_eq!(
            inspect_inbound_candidate(value, &malformed.abi_encode(), true, expected_rollup_id()),
            Err(InboundObservationError::InvalidEntryShape { field })
        );
    }

    let mut forged_hash = call.clone();
    forged_hash._entries[0].proxyEntryHash = B256::repeat_byte(0xee);
    assert!(matches!(
        inspect_inbound_candidate(value, &forged_hash.abi_encode(), true, expected_rollup_id(),),
        Err(InboundObservationError::CallHashMismatch { .. })
    ));

    let mut forged_outcome = call;
    forged_outcome._entries[0].rollingHash = B256::repeat_byte(0xdd);
    assert_eq!(
        inspect_inbound_candidate(
            value,
            &forged_outcome.abi_encode(),
            true,
            expected_rollup_id(),
        ),
        Err(InboundObservationError::InvalidEntryShape {
            field: "rollingHash",
        })
    );
}

#[test]
fn inbound_binding_rejects_a_candidate_hidden_inside_an_outbound_pair() {
    let rlp = block_rlp(vec![
        target_system_inbound_transaction(),
        target_user_inbound_selector_transaction(),
    ]);
    let settling = inspect_settling_block(&rlp, &[true, true], expected_rollup_id()).unwrap();
    let pre_settling = B256::repeat_byte(0x0a);
    let effect_root = B256::repeat_byte(0x0b);
    let batch = effect_batch(
        &[B256::ZERO, pre_settling, effect_root],
        &[ClaimedEntryShape::Outbound],
    );

    let effect_prefix = verify_effect_prefix(
        &batch,
        pre_settling,
        &[checkpoint(1, effect_root)],
        &settling,
    )
    .unwrap();
    assert_eq!(
        verify_inbound_effect_entries(&effect_prefix).err(),
        Some(InboundEffectError::UnexpectedCandidate {
            transaction_index: 0,
        })
    );
}

#[test]
fn intermediate_blocks_without_a_recovered_system_signer_are_accepted() {
    let invalid_signature = TransactionSigned::new_unhashed(
        transaction(SYSTEM_TX).into_typed_transaction(),
        Signature::new(U256::ZERO, U256::ZERO, false),
    );
    let empty = block_rlp(Vec::new());
    let unrecoverable = block_rlp(vec![invalid_signature]);

    assert_eq!(
        verify_no_intermediate_system_transactions([
            (40, empty.as_slice()),
            (41, unrecoverable.as_slice()),
        ]),
        Ok(())
    );
}

#[test]
fn intermediate_system_signers_are_rejected_regardless_of_recipient() {
    for encoded in [
        SYSTEM_TX,
        EIP1559_SYSTEM_TX,
        SYSTEM_SIGNER_OTHER_TARGET_TX,
        CREATE_TX,
    ] {
        let preceding = TransactionSigned::new_unhashed(
            transaction(SYSTEM_TX).into_typed_transaction(),
            Signature::new(U256::ZERO, U256::ZERO, false),
        );
        let rlp = block_rlp(vec![preceding, transaction(encoded)]);

        assert_eq!(
            verify_no_intermediate_system_transactions([(41, rlp.as_slice())]),
            Err(BlockInspectionError::IntermediateSystemTransaction {
                block_number: 41,
                transaction_index: 1,
            }),
            "fixture {encoded} was accepted"
        );
    }
}

#[test]
fn validated_intermediate_blocks_use_backend_recovered_sender_facts() {
    let block = validated_block(
        41,
        block_rlp(vec![transaction(USER_INBOUND_SELECTOR_TX)]),
        vec![true],
    );

    assert_eq!(
        verify_validated_intermediate_blocks(&[block]),
        Err(BlockInspectionError::IntermediateSystemTransaction {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

#[test]
fn validated_intermediate_blocks_reject_unbound_sender_fact_count() {
    let block = validated_block(
        41,
        block_rlp(vec![transaction(USER_INBOUND_SELECTOR_TX)]),
        Vec::new(),
    );

    assert_eq!(
        verify_validated_intermediate_blocks(&[block]),
        Err(BlockInspectionError::SystemSenderCount {
            block_number: 41,
            required: 1,
            actual: 0,
        })
    );
}

#[test]
fn validated_intermediate_blocks_reject_outbound_events() {
    let block = ValidatedBlock::for_test(
        41,
        block_rlp(vec![transaction(USER_INBOUND_SELECTOR_TX)]),
        SettlementBlockEvidence::for_test(
            vec![false],
            vec![OutboundEventObservation::decoded_for_test(
                0,
                2,
                B256::repeat_byte(0x11),
                0,
            )],
        ),
    );

    assert_eq!(
        verify_validated_intermediate_blocks(&[block]),
        Err(BlockInspectionError::IntermediateOutboundEvent {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

#[test]
fn exact_intermediate_inbound_candidate_is_rejected() {
    let transaction = target_system_inbound_transaction();
    assert!(
        transaction
            .input()
            .starts_with(EXECUTE_INCOMING_SELECTOR.as_slice())
    );
    assert_eq!(transaction.recover_signer().unwrap(), TEST_SYSTEM_ADDRESS);
    assert_eq!(transaction.to(), Some(crate::EEZL2_ADDRESS));
    let rlp = block_rlp(vec![transaction]);

    assert_eq!(
        verify_no_intermediate_system_transactions([(41, rlp.as_slice())]),
        Err(BlockInspectionError::IntermediateSystemTransaction {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

#[test]
fn selector_spoof_from_a_user_remains_an_ordinary_intermediate_transaction() {
    let transaction = target_user_inbound_selector_transaction();
    assert!(
        transaction
            .input()
            .starts_with(EXECUTE_INCOMING_SELECTOR.as_slice())
    );
    assert_ne!(transaction.recover_signer().unwrap(), TEST_SYSTEM_ADDRESS);
    assert_eq!(transaction.to(), Some(crate::EEZL2_ADDRESS));
    let rlp = block_rlp(vec![transaction]);

    assert_eq!(
        verify_no_intermediate_system_transactions([(41, rlp.as_slice())]),
        Ok(())
    );
}

#[test]
fn reserved_system_transaction_type_is_rejected() {
    let encoded = hex::decode(EIP1559_SYSTEM_TX).unwrap();
    let mut rlp = block_rlp(vec![transaction(EIP1559_SYSTEM_TX)]);
    let transaction_start = rlp
        .windows(encoded.len())
        .position(|window| window == encoded)
        .unwrap();
    rlp[transaction_start] = super::super::RESERVED_SYSTEM_TRANSACTION_TYPE;

    assert!(matches!(
        verify_no_intermediate_system_transactions([(41, rlp.as_slice())]),
        Err(BlockInspectionError::InvalidRlp {
            block_number: 41,
            ..
        } | BlockInspectionError::IntermediateSystemTransaction {
            block_number: 41,
            transaction_index: 0,
        })
    ));
}

#[test]
fn malformed_intermediate_block_rlp_reports_its_block_number() {
    let valid = block_rlp(Vec::new());
    let mut trailing = valid.clone();
    trailing.push(0);

    for malformed in [&[0xff][..], trailing.as_slice()] {
        assert!(matches!(
            verify_no_intermediate_system_transactions([(40, valid.as_slice()), (41, malformed),]),
            Err(BlockInspectionError::InvalidRlp {
                block_number: 41,
                ..
            })
        ));
    }
}
