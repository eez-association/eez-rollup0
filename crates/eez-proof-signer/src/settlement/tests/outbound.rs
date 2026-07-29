use super::*;

#[test]
fn outbound_events_bind_by_transaction_and_preserve_duplicate_hashes() {
    let mut batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Outbound],
    );
    batch.entries[1].l2ToL1Calls[0].data = Bytes::from_static(&[0x01]);
    batch.entries[2].l2ToL1Calls[0].data = Bytes::from_static(&[0x02]);
    let mut settling = settling_with_outbound_pairs(2);
    *settling.outbound_event_candidates_mut_for_test() = vec![
        observed_outbound_call(1, 0, &batch.entries[1].l2ToL1Calls[0]),
        observed_outbound_call(3, 0, &batch.entries[2].l2ToL1Calls[0]),
    ];
    let plan = effect_plan(&batch, &settling);
    assert!(authorize_outbound_effects(&plan).is_ok());

    let duplicate_batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Outbound],
    );
    let call = &duplicate_batch.entries[1].l2ToL1Calls[0];
    let mut duplicates = settling_with_outbound_pairs(2);
    *duplicates.outbound_event_candidates_mut_for_test() = vec![
        observed_outbound_call(1, 0, call),
        observed_outbound_call(3, 0, call),
    ];
    let duplicate_plan = effect_plan(&duplicate_batch, &duplicates);
    assert!(authorize_outbound_effects(&duplicate_plan).is_ok());

    let mut reordered = settling_with_outbound_pairs(2);
    *reordered.outbound_event_candidates_mut_for_test() = vec![
        observed_outbound_call(1, 0, &batch.entries[2].l2ToL1Calls[0]),
        observed_outbound_call(3, 0, &batch.entries[1].l2ToL1Calls[0]),
    ];
    let reordered_plan = effect_plan(&batch, &reordered);
    assert!(matches!(
        authorize_outbound_effects(&reordered_plan),
        Err(OutboundEffectError::CallHashMismatch {
            entry_index: 1,
            transaction_index: 1,
            ..
        })
    ));
}

#[test]
fn outbound_bindings_require_a_preceding_load_and_outbound_first_order() {
    let outbound_batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let mut missing_load = settling_with_system_flags(vec![false]);
    missing_load
        .outbound_event_candidates_mut_for_test()
        .push(observed_outbound_call(
            0,
            0,
            &outbound_batch.entries[1].l2ToL1Calls[0],
        ));
    let missing_load_plan = effect_plan(&outbound_batch, &missing_load);
    assert_eq!(
        authorize_outbound_effects(&missing_load_plan).err(),
        Some(OutboundEffectError::MissingPrecedingSystemTransaction {
            entry_index: 1,
            transaction_index: 0,
        })
    );

    let mixed_batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Inbound, ClaimedEntryShape::Outbound],
    );
    let mut inbound_first = settling_with_system_flags(vec![true, true, false]);
    inbound_first
        .outbound_event_candidates_mut_for_test()
        .push(observed_outbound_call(
            2,
            0,
            &mixed_batch.entries[2].l2ToL1Calls[0],
        ));
    let inbound_first_plan = effect_plan(&mixed_batch, &inbound_first);
    assert_eq!(
        authorize_outbound_effects(&inbound_first_plan).err(),
        Some(OutboundEffectError::NonCanonicalEffectOrder {
            entry_index: 2,
            transaction_index: 2,
        })
    );
}

#[test]
fn outbound_events_reject_missing_extra_multiple_and_malformed_observations() {
    let outbound_batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let call = &outbound_batch.entries[1].l2ToL1Calls[0];

    let missing = settling_with_outbound_pairs(1);
    let missing_plan = effect_plan(&outbound_batch, &missing);
    assert_eq!(
        authorize_outbound_effects(&missing_plan).err(),
        Some(OutboundEffectError::MissingObservation {
            entry_index: 1,
            transaction_index: 1,
        })
    );

    let mut malformed = settling_with_outbound_pairs(1);
    malformed
        .outbound_event_candidates_mut_for_test()
        .push(OutboundEventObservation::for_test(1, 3, None));
    let malformed_plan = effect_plan(&outbound_batch, &malformed);
    assert_eq!(
        authorize_outbound_effects(&malformed_plan).err(),
        Some(OutboundEffectError::MalformedObservation {
            transaction_index: 1,
            receipt_log_index: 3,
        })
    );

    let observation = observed_outbound_call(1, 0, call);
    let mut multiple = settling_with_outbound_pairs(1);
    *multiple.outbound_event_candidates_mut_for_test() = vec![
        observation,
        OutboundEventObservation::for_test(
            observation.transaction_index(),
            1,
            observation.decoded_call_hash(),
        ),
    ];
    let multiple_plan = effect_plan(&outbound_batch, &multiple);
    assert_eq!(
        authorize_outbound_effects(&multiple_plan).err(),
        Some(OutboundEffectError::UnexpectedObservation {
            transaction_index: 1,
            receipt_log_index: 1,
        })
    );

    let mut extra = settling_with_outbound_pairs(1);
    *extra.outbound_event_candidates_mut_for_test() = vec![
        observation,
        OutboundEventObservation::for_test(2, 0, observation.decoded_call_hash()),
    ];
    let extra_plan = effect_plan(&outbound_batch, &extra);
    assert_eq!(
        authorize_outbound_effects(&extra_plan).err(),
        Some(OutboundEffectError::UnexpectedObservation {
            transaction_index: 2,
            receipt_log_index: 0,
        })
    );

    // The system/user pair makes transaction 1 the outbound effect position;
    // an otherwise matching event from the preceding transaction cannot be
    // reassociated with it.
    let mut early = settling_with_system_flags(vec![true, false]);
    early
        .outbound_event_candidates_mut_for_test()
        .push(observed_outbound_call(0, 0, call));
    let early_plan = effect_plan(&outbound_batch, &early);
    assert_eq!(
        authorize_outbound_effects(&early_plan).err(),
        Some(OutboundEffectError::UnexpectedObservation {
            transaction_index: 0,
            receipt_log_index: 0,
        })
    );

    let inbound_batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Inbound]);
    let mut inbound = settling_with_system_flags(vec![true]);
    inbound
        .outbound_event_candidates_mut_for_test()
        .push(observation);
    let inbound_plan = effect_plan(&inbound_batch, &inbound);
    assert_eq!(
        authorize_outbound_effects(&inbound_plan).err(),
        Some(OutboundEffectError::UnexpectedObservation {
            transaction_index: 1,
            receipt_log_index: 0,
        })
    );
}

#[test]
fn outbound_events_bind_the_single_call_and_expected_rollup() {
    let valid = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let observation = observed_outbound_call(1, 0, &valid.entries[1].l2ToL1Calls[0]);

    let verify = |batch: &CanonicalPostBatch| {
        let mut settling = settling_with_outbound_pairs(1);
        settling
            .outbound_event_candidates_mut_for_test()
            .push(observation);
        let plan = effect_plan(batch, &settling);
        authorize_outbound_effects(&plan).err()
    };

    let mut multiple_calls = valid.clone();
    multiple_calls.entries[1].l2ToL1Calls.push(l2_to_l1_call());
    assert_eq!(
        verify(&multiple_calls),
        Some(OutboundEffectError::L2ToL1CallCount {
            entry_index: 1,
            actual: 2,
        })
    );

    let mut wrong_destination = valid.clone();
    wrong_destination.entries[1].destinationRollupId = U256::from(2);
    assert_eq!(
        verify(&wrong_destination),
        Some(OutboundEffectError::DestinationRollupMismatch {
            entry_index: 1,
            expected: 1,
            actual: U256::from(2),
        })
    );

    let mut wrong_source = valid.clone();
    wrong_source.entries[1].l2ToL1Calls[0].sourceRollupId = U256::from(2);
    assert_eq!(
        verify(&wrong_source),
        Some(OutboundEffectError::SourceRollupMismatch {
            entry_index: 1,
            expected: 1,
            actual: U256::from(2),
        })
    );

    let hash_mutations: [fn(&mut L2ToL1CallSol); 4] = [
        |call| call.targetAddress = Address::repeat_byte(0xcc),
        |call| call.value = U256::from(8),
        |call| call.data = Bytes::from_static(&[0xff]),
        |call| call.sourceAddress = Address::repeat_byte(0xdd),
    ];
    for mutate in hash_mutations {
        let mut wrong_call = valid.clone();
        mutate(&mut wrong_call.entries[1].l2ToL1Calls[0]);
        assert!(matches!(
            verify(&wrong_call),
            Some(OutboundEffectError::CallHashMismatch {
                entry_index: 1,
                transaction_index: 1,
                ..
            })
        ));
    }
}

#[test]
fn outbound_effects_require_the_flat_single_call_shape() {
    let valid = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let verify = |batch: &CanonicalPostBatch| {
        let mut settling = settling_with_outbound_pairs(1);
        settling
            .outbound_event_candidates_mut_for_test()
            .push(observed_outbound_call(
                1,
                0,
                &batch.entries[1].l2ToL1Calls[0],
            ));
        let plan = effect_plan(batch, &settling);
        authorize_outbound_effects(&plan).err()
    };

    let shape_mutations: [(fn(&mut ExecutionEntrySol), &str); 4] = [
        (|entry| entry.callCount = U256::from(2), "callCount"),
        (
            |entry| entry.expectedL1ToL2Calls.push(expected_call()),
            "expectedL1ToL2Calls",
        ),
        (
            |entry| entry.expectedLookups.push(expected_lookup()),
            "expectedLookups",
        ),
        (
            |entry| entry.l2ToL1Calls[0].revertSpan = U256::from(1),
            "revertSpan",
        ),
    ];
    for (mutate, field) in shape_mutations {
        let mut batch = valid.clone();
        mutate(&mut batch.entries[1]);
        assert_eq!(
            verify(&batch),
            Some(OutboundEffectError::InvalidEntryShape {
                entry_index: 1,
                field,
            })
        );
    }
}

#[test]
fn outbound_effects_require_a_successful_outcome_and_exact_value_accounting() {
    let valid = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let verify = |batch: &CanonicalPostBatch| {
        let mut settling = settling_with_outbound_pairs(1);
        settling
            .outbound_event_candidates_mut_for_test()
            .push(observed_outbound_call(
                1,
                0,
                &batch.entries[1].l2ToL1Calls[0],
            ));
        let plan = effect_plan(batch, &settling);
        authorize_outbound_effects(&plan).err()
    };

    let mut nonempty_return_data = valid.clone();
    nonempty_return_data.entries[1].returnData = Bytes::from_static(&[0xca, 0xfe]);
    nonempty_return_data.entries[1].rollingHash =
        b256!("db02b0059bb85889526354ec94d73be8588724a34c54f401645973b5e525fa96");
    assert_eq!(verify(&nonempty_return_data), None);

    let mut wrong_return_data = valid.clone();
    wrong_return_data.entries[1].returnData = Bytes::from_static(&[0xca, 0xfe]);
    assert_eq!(
        verify(&wrong_return_data),
        Some(OutboundEffectError::UnsupportedOutcome { entry_index: 1 })
    );

    let mut failed = valid.clone();
    failed.entries[1].returnData = Bytes::from_static(&[0xca, 0xfe]);
    failed.entries[1].rollingHash =
        b256!("f01319463ceddff2696a5a9252bb370015c485dd3526b2f0302c2681c3d5c71a");
    failed.entries[1].stateDeltas[0].etherDelta = I256::ZERO;
    assert_eq!(
        verify(&failed),
        Some(OutboundEffectError::UnsupportedOutcome { entry_index: 1 })
    );

    let value = U256::from(5);
    let mut failed_with_value = failed.clone();
    failed_with_value.entries[1].l2ToL1Calls[0].value = value;
    failed_with_value.entries[1].stateDeltas[0].etherDelta = -I256::try_from(value).unwrap();
    assert_eq!(
        verify(&failed_with_value),
        Some(OutboundEffectError::UnsupportedOutcome { entry_index: 1 })
    );

    let mut wrong_delta = valid.clone();
    wrong_delta.entries[1].stateDeltas[0].etherDelta = I256::ONE;
    assert_eq!(
        verify(&wrong_delta),
        Some(OutboundEffectError::EtherDeltaMismatch {
            entry_index: 1,
            expected: I256::ZERO,
            actual: I256::ONE,
        })
    );

    for value in [U256::from(1), (U256::from(1) << 255) - U256::from(1)] {
        let mut value_bearing = valid.clone();
        value_bearing.entries[1].l2ToL1Calls[0].value = value;
        value_bearing.entries[1].stateDeltas[0].etherDelta = -I256::try_from(value).unwrap();
        assert_eq!(verify(&value_bearing), None);

        value_bearing.entries[1].stateDeltas[0].etherDelta += I256::ONE;
        assert_eq!(
            verify(&value_bearing),
            Some(OutboundEffectError::EtherDeltaMismatch {
                entry_index: 1,
                expected: -I256::try_from(value).unwrap(),
                actual: -I256::try_from(value).unwrap() + I256::ONE,
            })
        );
    }

    for value in [U256::from(1) << 255, U256::MAX] {
        let mut out_of_range = valid.clone();
        out_of_range.entries[1].l2ToL1Calls[0].value = value;
        assert_eq!(
            verify(&out_of_range),
            Some(OutboundEffectError::ValueOutOfRange {
                entry_index: 1,
                value,
            })
        );
    }

    let mut reserved_source = valid;
    reserved_source.entries[1].l2ToL1Calls[0].sourceAddress = SYSTEM_ADDRESS;
    assert_eq!(
        verify(&reserved_source),
        Some(OutboundEffectError::ReservedSourceAddress { entry_index: 1 })
    );
}
