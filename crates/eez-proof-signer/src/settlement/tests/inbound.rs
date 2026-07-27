use super::*;

#[test]
fn inbound_binding_rejects_top_level_lookup_calls() {
    let settling = settling_with_effect_candidates(Vec::new());
    let anchor_only = state_chain(&[B256::ZERO; 2]);
    let mut lookup_batch = anchor_only;
    lookup_batch.inner.l1ToL2lookupCalls.push(lookup(1));
    let lookup_plan = effect_plan(&lookup_batch, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&lookup_plan).err(),
        Some(InboundEffectError::LookupCalls { actual: 1 })
    );
}

#[test]
fn inbound_effect_entries_bind_observations_positionally_and_preserve_duplicates() {
    let mut settling = SettlingBlockObservations::for_test(
        vec![true, true],
        vec![
            observed_inbound_candidate(0, U256::from(1), true),
            observed_inbound_candidate(1, U256::from(2), true),
        ],
        Vec::new(),
    );
    let batch = bindable_inbound_batch(&settling);
    {
        let plan = effect_plan(&batch, &settling);
        assert!(verify_inbound_effect_entries(&plan).is_ok());

        let mut reordered = batch.clone();
        reordered.inner.entries.swap(1, 2);
        let reordered_plan = effect_plan(&reordered, &settling);
        assert!(matches!(
            verify_inbound_effect_entries(&reordered_plan),
            Err(InboundEffectError::CallHashMismatch { entry_index: 1, .. })
        ));
    }

    let first = observed_inbound_candidate(0, U256::from(3), true);
    let mut second = observed_inbound_candidate(1, U256::from(3), true);
    second.inspection.as_mut().unwrap().return_data = Bytes::from_static(&[0x03]);
    *settling.inbound_candidates_mut_for_test() = vec![first, second];
    let duplicates = bindable_inbound_batch(&settling);
    let duplicate_plan = effect_plan(&duplicates, &settling);
    assert!(verify_inbound_effect_entries(&duplicate_plan).is_ok());

    let mut swapped_return_data = duplicates.clone();
    let first_return = swapped_return_data.inner.entries[1].returnData.clone();
    swapped_return_data.inner.entries[1].returnData =
        swapped_return_data.inner.entries[2].returnData.clone();
    swapped_return_data.inner.entries[2].returnData = first_return;
    let swapped_plan = effect_plan(&swapped_return_data, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&swapped_plan).err(),
        Some(InboundEffectError::ReturnDataMismatch { entry_index: 1 })
    );
}

#[test]
fn inbound_effect_entries_handle_mixed_effect_positions_without_authorizing_outbound() {
    // A system/user pair ends at the user transaction (outbound), followed by
    // a terminal system transaction (inbound).
    let observation = observed_inbound_candidate(2, U256::from(4), true);
    let settling =
        SettlingBlockObservations::for_test(vec![true, false, true], vec![observation], Vec::new());
    let mut batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Inbound],
    );
    let observation = settling.inbound_candidates()[0]
        .inspection
        .as_ref()
        .unwrap();
    let inbound = &mut batch.inner.entries[2];
    inbound.proxyEntryHash = observation.recomputed_call_hash;
    inbound.returnData = observation.return_data.clone();
    inbound.stateDeltas[0].etherDelta = I256::try_from(observation.value).unwrap();

    let plan = effect_plan(&batch, &settling);
    assert!(verify_inbound_effect_entries(&plan).is_ok());
}

#[test]
fn inbound_effect_entries_reject_missing_extra_or_invalid_observations() {
    let missing = SettlingBlockObservations::for_test(vec![true], Vec::new(), Vec::new());
    let inbound_batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Inbound]);
    let inbound_plan = effect_plan(&inbound_batch, &missing);
    assert_eq!(
        verify_inbound_effect_entries(&inbound_plan).err(),
        Some(InboundEffectError::MissingCandidate {
            entry_index: 1,
            transaction_index: 0,
        })
    );

    let hidden = SettlingBlockObservations::for_test(
        vec![true, false],
        vec![observed_inbound_candidate(0, U256::ZERO, true)],
        Vec::new(),
    );
    let outbound_batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let outbound_plan = effect_plan(&outbound_batch, &hidden);
    assert_eq!(
        verify_inbound_effect_entries(&outbound_plan).err(),
        Some(InboundEffectError::UnexpectedCandidate {
            transaction_index: 0,
        })
    );

    let conflicting = SettlingBlockObservations::for_test(
        vec![false],
        vec![observed_inbound_candidate(0, U256::ZERO, true)],
        Vec::new(),
    );
    let conflicting_plan = effect_plan(&outbound_batch, &conflicting);
    assert_eq!(
        verify_inbound_effect_entries(&conflicting_plan).err(),
        Some(InboundEffectError::UnexpectedCandidate {
            transaction_index: 0,
        })
    );

    let invalid = SettlingBlockObservations::for_test(
        vec![true],
        vec![invalid_inbound_candidate(0)],
        Vec::new(),
    );
    let invalid_plan = effect_plan(&inbound_batch, &invalid);
    assert!(matches!(
        verify_inbound_effect_entries(&invalid_plan),
        Err(InboundEffectError::InvalidObservation {
            entry_index: 1,
            transaction_index: 0,
            ..
        })
    ));

    let failed = SettlingBlockObservations::for_test(
        vec![true],
        vec![observed_inbound_candidate(0, U256::ZERO, false)],
        Vec::new(),
    );
    let failed_batch = bindable_inbound_batch(&failed);
    let failed_plan = effect_plan(&failed_batch, &failed);
    assert_eq!(
        verify_inbound_effect_entries(&failed_plan).err(),
        Some(InboundEffectError::FailedCall {
            entry_index: 1,
            transaction_index: 0,
        })
    );
}

#[test]
fn inbound_effect_entries_require_the_canonical_deferred_shape() {
    let settling = SettlingBlockObservations::for_test(
        vec![true],
        vec![observed_inbound_candidate(0, U256::from(5), true)],
        Vec::new(),
    );
    let valid = bindable_inbound_batch(&settling);

    let shape_mutations: [(fn(&mut ExecutionEntrySol), &str); 5] = [
        (
            |entry| entry.l2ToL1Calls.push(l2_to_l1_call()),
            "l2ToL1Calls",
        ),
        (
            |entry| entry.expectedL1ToL2Calls.push(expected_call()),
            "expectedL1ToL2Calls",
        ),
        (
            |entry| entry.expectedLookups.push(expected_lookup()),
            "expectedLookups",
        ),
        (|entry| entry.callCount = U256::from(1), "callCount"),
        (
            |entry| entry.rollingHash = B256::repeat_byte(0xaa),
            "rollingHash",
        ),
    ];
    for (mutate, field) in shape_mutations {
        let mut batch = valid.clone();
        mutate(&mut batch.inner.entries[1]);
        let plan = effect_plan(&batch, &settling);
        assert_eq!(
            verify_inbound_effect_entries(&plan).err(),
            Some(InboundEffectError::InvalidEntryShape {
                entry_index: 1,
                field,
            })
        );
    }

    let mut wrong_rollup = valid.clone();
    wrong_rollup.inner.entries[1].destinationRollupId = U256::from(2);
    let plan = effect_plan(&wrong_rollup, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::DestinationRollupMismatch {
            entry_index: 1,
            expected: 1,
            actual: U256::from(2),
        })
    );

    let mut wrong_hash = valid.clone();
    wrong_hash.inner.entries[1].proxyEntryHash = B256::repeat_byte(0xbb);
    let plan = effect_plan(&wrong_hash, &settling);
    assert!(matches!(
        verify_inbound_effect_entries(&plan),
        Err(InboundEffectError::CallHashMismatch { entry_index: 1, .. })
    ));

    let mut wrong_return = valid.clone();
    wrong_return.inner.entries[1].returnData = Bytes::from_static(&[0xff]);
    let plan = effect_plan(&wrong_return, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::ReturnDataMismatch { entry_index: 1 })
    );

    let mut wrong_delta = valid;
    wrong_delta.inner.entries[1].stateDeltas[0].etherDelta = I256::ZERO;
    let plan = effect_plan(&wrong_delta, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::EtherDeltaMismatch {
            entry_index: 1,
            expected: I256::from_raw(U256::from(5)),
            actual: I256::ZERO,
        })
    );
}

#[test]
fn inbound_effect_entries_accept_the_int256_maximum_and_reject_the_next_value() {
    let max_value = (U256::from(1) << 255) - U256::from(1);
    let max_settling = SettlingBlockObservations::for_test(
        vec![true],
        vec![observed_inbound_candidate(0, max_value, true)],
        Vec::new(),
    );
    let max_batch = bindable_inbound_batch(&max_settling);
    let max_plan = effect_plan(&max_batch, &max_settling);
    assert!(verify_inbound_effect_entries(&max_plan).is_ok());
    assert_eq!(
        max_batch.inner.entries[1].stateDeltas[0].etherDelta,
        I256::MAX
    );

    let value = U256::from(1) << 255;
    let candidate = observed_inbound_candidate(0, value, true);
    let observation = candidate.inspection.as_ref().unwrap();
    let call_hash = observation.recomputed_call_hash;
    let return_data = observation.return_data.clone();
    let settling = SettlingBlockObservations::for_test(vec![true], vec![candidate], Vec::new());
    let mut batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Inbound]);
    batch.inner.entries[1].proxyEntryHash = call_hash;
    batch.inner.entries[1].returnData = return_data;
    let plan = effect_plan(&batch, &settling);

    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::ValueOutOfRange {
            entry_index: 1,
            transaction_index: 0,
            value,
        })
    );
}
