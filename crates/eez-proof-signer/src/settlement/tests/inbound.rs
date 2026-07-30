use super::*;

#[test]
fn inbound_candidate_requires_the_exact_l2_shape_and_rolling_hash() {
    use eez_protocol::abi::{
        ExpectedOutgoingCrossChainCallSol, L2StaticExecutionEntrySol,
        executeIncomingCrossChainCallCall,
    };

    let value = U256::from(5);
    let calldata = strict_inbound_calldata(value, true);
    let valid = inspect_inbound_candidate(value, &calldata, true, expected_rollup_id()).unwrap();
    let sidecar = valid.derived_da_entry.as_entry();
    let [sidecar_call] = sidecar.l2ToL1Calls.as_slice() else {
        panic!("derived inbound sidecar must contain one call");
    };
    assert!(sidecar.stateUpdates.is_empty());
    assert_eq!(sidecar.destinationRollupId, 1);
    assert!(sidecar.success);
    assert_eq!(sidecar_call.sourceRollupId, 0);
    assert_eq!(sidecar_call.revertNextNCalls, 0);
    assert!(!sidecar_call.isStatic);
    assert_eq!(sidecar_call.gas, 0);

    let call = executeIncomingCrossChainCallCall::abi_decode(&calldata).unwrap();
    let entry = &call._entries[0];
    assert!(entry.success);
    assert!(entry.expectedOutgoingCalls.is_empty());
    assert!(sidecar.expectedL1ToL2Calls.is_empty());
    let mut expected_rolling =
        eez_protocol::rolling_hash::EntryRollingHash::for_l2(entry.proxyEntryHash);
    expected_rolling.call_begin(entry.proxyEntryHash);
    expected_rolling.call_end(true, &entry.returnData);
    assert_eq!(entry.rollingHash, expected_rolling.current());

    let inspect = |call: &executeIncomingCrossChainCallCall| {
        inspect_inbound_candidate(value, &call.abi_encode(), true, expected_rollup_id()).err()
    };

    let mut with_static = call.clone();
    with_static
        ._staticEntries
        .push(L2StaticExecutionEntrySol::default());
    assert_eq!(
        inspect(&with_static),
        Some(InboundObservationError::StaticEntryCount { actual: 1 })
    );

    let shape_mutations: [(fn(&mut executeIncomingCrossChainCallCall), &str); 6] = [
        (|call| call._entries[0].success = false, "success"),
        (
            |call| {
                call._entries[0]
                    .expectedOutgoingCalls
                    .push(ExpectedOutgoingCrossChainCallSol {
                        expectedOutgoingHash: B256::ZERO,
                        incomingCalls: Vec::new(),
                        revertedOrStaticRollingHash: B256::ZERO,
                        success: true,
                        returnData: Bytes::new(),
                    });
            },
            "expectedOutgoingCalls",
        ),
        (
            |call| call._entries[0].incomingCalls[0].revertNextNCalls = 1,
            "revertNextNCalls",
        ),
        (
            |call| call._entries[0].incomingCalls[0].isStatic = true,
            "isStatic",
        ),
        (|call| call._entries[0].incomingCalls[0].gas = 1, "gas"),
        (
            |call| call._entries[0].rollingHash = B256::repeat_byte(0xaa),
            "rollingHash",
        ),
    ];
    for (mutate, field) in shape_mutations {
        let mut malformed = call.clone();
        mutate(&mut malformed);
        assert_eq!(
            inspect(&malformed),
            Some(InboundObservationError::InvalidEntryShape { field })
        );
    }

    let mut wrong_source_rollup = call;
    wrong_source_rollup.sourceRollup = 2;
    assert_eq!(
        inspect(&wrong_source_rollup),
        Some(InboundObservationError::SourceRollup { actual: 2 })
    );
}

#[test]
fn inbound_effect_prefix_rejects_unsupported_target_shapes() {
    let settling = SettlingBlockObservations::for_test(
        vec![true],
        vec![observed_inbound_candidate(0, U256::ZERO, true)],
        Vec::new(),
    );
    let valid = bindable_inbound_batch(&settling);
    let checkpoints = [checkpoint(0, B256::ZERO)];
    let shape_mutations: [fn(&mut ExecutionEntrySol); 3] = [
        |entry| entry.success = false,
        |entry| entry.expectedL1ToL2Calls.push(expected_call()),
        |entry| entry.l2ToL1Calls.push(l2_to_l1_call()),
    ];

    for mutate in shape_mutations {
        let mut batch = valid.clone();
        mutate(&mut batch.entries[1]);
        assert_eq!(
            verify_effect_prefix(&batch, B256::ZERO, &checkpoints, &settling).err(),
            Some(EffectPrefixError::InvalidEntry { entry_index: 1 })
        );
    }
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
        reordered.entries.swap(1, 2);
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
    let first_return = swapped_return_data.entries[1].returnData.clone();
    swapped_return_data.entries[1].returnData = swapped_return_data.entries[2].returnData.clone();
    swapped_return_data.entries[2].returnData = first_return;
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
    let inbound = &mut batch.entries[2];
    inbound.proxyEntryHash = observation.recomputed_call_hash;
    inbound.returnData = observation.return_data.clone();
    inbound.stateUpdates[0].etherDelta = I256::try_from(observation.value).unwrap();
    inbound.rollingHash = eez_protocol::rolling_hash::EntryRollingHash::for_l1(
        [(
            inbound.stateUpdates[0].rollupId,
            inbound.stateUpdates[0].currentState,
        )],
        inbound.proxyEntryHash,
    )
    .current();

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
    let failed_plan = effect_plan(&inbound_batch, &failed);
    assert!(matches!(
        verify_inbound_effect_entries(&failed_plan),
        Err(InboundEffectError::InvalidObservation {
            entry_index: 1,
            transaction_index: 0,
            source: InboundObservationError::InvalidEntryShape { field: "success" },
        })
    ));
}

#[test]
fn inbound_effect_entries_require_the_canonical_deferred_shape() {
    let settling = SettlingBlockObservations::for_test(
        vec![true],
        vec![observed_inbound_candidate(0, U256::from(5), true)],
        Vec::new(),
    );
    let valid = bindable_inbound_batch(&settling);

    let mut wrong_rolling_hash = valid.clone();
    wrong_rolling_hash.entries[1].rollingHash = B256::repeat_byte(0xaa);
    let plan = effect_plan(&wrong_rolling_hash, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::InvalidEntryShape {
            entry_index: 1,
            field: "rollingHash",
        })
    );

    let entry = &valid.entries[1];
    let update = &entry.stateUpdates[0];
    assert_eq!(
        entry.rollingHash,
        eez_protocol::rolling_hash::EntryRollingHash::for_l1(
            [(update.rollupId, update.currentState)],
            entry.proxyEntryHash,
        )
        .current(),
        "deferred L1 entries use the exact state-update plus proxy seed",
    );

    let mut wrong_rollup = valid.clone();
    wrong_rollup.entries[1].destinationRollupId = 2;
    let plan = effect_plan(&wrong_rollup, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::DestinationRollupMismatch {
            entry_index: 1,
            expected: 1,
            actual: 2,
        })
    );

    let mut wrong_hash = valid.clone();
    wrong_hash.entries[1].proxyEntryHash = B256::repeat_byte(0xbb);
    let plan = effect_plan(&wrong_hash, &settling);
    assert!(matches!(
        verify_inbound_effect_entries(&plan),
        Err(InboundEffectError::CallHashMismatch { entry_index: 1, .. })
    ));

    let mut wrong_return = valid.clone();
    wrong_return.entries[1].returnData = Bytes::from_static(&[0xff]);
    let plan = effect_plan(&wrong_return, &settling);
    assert_eq!(
        verify_inbound_effect_entries(&plan).err(),
        Some(InboundEffectError::ReturnDataMismatch { entry_index: 1 })
    );

    let mut wrong_delta = valid;
    wrong_delta.entries[1].stateUpdates[0].etherDelta = I256::ZERO;
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
    assert_eq!(max_batch.entries[1].stateUpdates[0].etherDelta, I256::MAX);

    let value = U256::from(1) << 255;
    let candidate = observed_inbound_candidate(0, value, true);
    let observation = candidate.inspection.as_ref().unwrap();
    let call_hash = observation.recomputed_call_hash;
    let return_data = observation.return_data.clone();
    let settling = SettlingBlockObservations::for_test(vec![true], vec![candidate], Vec::new());
    let mut batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Inbound]);
    batch.entries[1].proxyEntryHash = call_hash;
    batch.entries[1].returnData = return_data;
    let update = &batch.entries[1].stateUpdates[0];
    let rolling_seed = (update.rollupId, update.currentState);
    let proxy_entry_hash = batch.entries[1].proxyEntryHash;
    batch.entries[1].rollingHash =
        eez_protocol::rolling_hash::EntryRollingHash::for_l1([rolling_seed], proxy_entry_hash)
            .current();
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
