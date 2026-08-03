use super::*;

#[test]
fn accepts_a_single_or_multi_entry_state_delta_chain() {
    let a = B256::repeat_byte(0x0a);
    let b = B256::repeat_byte(0x0b);
    let c = B256::repeat_byte(0x0c);

    assert_eq!(
        verify_state_delta_chain(&state_chain(&[a, a]), expected_rollup_id(), a, a).map(|_| ()),
        Ok(())
    );
    assert_eq!(
        verify_state_delta_chain(&state_chain(&[a, b, c]), expected_rollup_id(), a, c).map(|_| ()),
        Ok(())
    );
}

#[test]
fn rejects_missing_or_non_singular_state_deltas() {
    let root = B256::ZERO;
    let empty = CanonicalPostBatch::from_decoded_for_test(EvmBatch::default());
    assert_eq!(
        verify_state_delta_chain(&empty, expected_rollup_id(), root, root).map(|_| ()),
        Err(StateDeltaChainError::NoEntries)
    );

    for entry_index in [0, 1] {
        for actual in [0, 2] {
            let mut batch = state_chain(&[root, root, root]);
            let delta = batch.entries[entry_index].stateDeltas[0].clone();
            batch.entries[entry_index].stateDeltas = vec![delta; actual];
            assert_eq!(
                verify_state_delta_chain(&batch, expected_rollup_id(), root, root).map(|_| ()),
                Err(StateDeltaChainError::DeltaCount {
                    entry_index,
                    actual,
                })
            );
        }
    }
}

#[test]
fn rejects_invalid_or_inconsistent_state_delta_rollup_ids() {
    let root = B256::ZERO;

    let mut zero = state_chain(&[root, root]);
    zero.entries[0].stateDeltas[0].rollupId = U256::ZERO;
    assert_eq!(
        verify_state_delta_chain(&zero, expected_rollup_id(), root, root).map(|_| ()),
        Err(StateDeltaChainError::ExpectedRollupMismatch {
            expected: 1,
            claimed: U256::ZERO,
        })
    );

    let too_large = U256::from(u64::MAX) + U256::from(1);
    let mut out_of_range = state_chain(&[root, root]);
    out_of_range.entries[0].stateDeltas[0].rollupId = too_large;
    assert_eq!(
        verify_state_delta_chain(&out_of_range, expected_rollup_id(), root, root).map(|_| ()),
        Err(StateDeltaChainError::ExpectedRollupMismatch {
            expected: 1,
            claimed: too_large,
        })
    );

    let mut wrong_expected_rollup = state_chain(&[root, root]);
    wrong_expected_rollup.entries[0].stateDeltas[0].rollupId = U256::from(2);
    assert_eq!(
        verify_state_delta_chain(&wrong_expected_rollup, expected_rollup_id(), root, root)
            .map(|_| ()),
        Err(StateDeltaChainError::ExpectedRollupMismatch {
            expected: 1,
            claimed: U256::from(2),
        })
    );

    let mut mixed = state_chain(&[root, root, root]);
    mixed.entries[1].stateDeltas[0].rollupId = U256::from(2);
    assert_eq!(
        verify_state_delta_chain(&mixed, expected_rollup_id(), root, root).map(|_| ()),
        Err(StateDeltaChainError::RollupMismatch {
            entry_index: 1,
            expected: U256::from(1),
            claimed: U256::from(2),
        })
    );
}

#[test]
fn rejects_wrong_state_delta_endpoints_or_a_chain_break() {
    let a = B256::repeat_byte(0x0a);
    let b = B256::repeat_byte(0x0b);
    let c = B256::repeat_byte(0x0c);
    let wrong = B256::repeat_byte(0xee);
    let batch = state_chain(&[a, b, c]);

    assert_eq!(
        verify_state_delta_chain(&batch, expected_rollup_id(), wrong, c).map(|_| ()),
        Err(StateDeltaChainError::InitialRootMismatch {
            validated: wrong,
            claimed: a,
        })
    );
    assert_eq!(
        verify_state_delta_chain(&batch, expected_rollup_id(), a, wrong).map(|_| ()),
        Err(StateDeltaChainError::FinalMismatch {
            validated: wrong,
            claimed: c,
        })
    );

    let mut broken = batch;
    broken.entries[1].stateDeltas[0].currentState = wrong;
    assert_eq!(
        verify_state_delta_chain(&broken, expected_rollup_id(), a, c).map(|_| ()),
        Err(StateDeltaChainError::ChainBreak {
            entry_index: 1,
            previous_claimed_post_state: b,
            next_claimed_pre_state: wrong,
        })
    );
}

#[test]
fn accepts_only_a_canonical_anchor_for_an_empty_settling_block() {
    let root = B256::ZERO;
    let batch = state_chain(&[root, root]);
    let settling = settling_with_effect_candidates(Vec::new());

    let empty = verify_effect_prefix(&batch, root, &[], &settling).unwrap();
    assert_eq!((empty.inbound_count(), empty.outbound_count()), (0, 0));

    let recorded = recorded_batch();
    let recorded = verify_effect_prefix(&recorded, root, &[], &settling).unwrap();
    assert_eq!(
        (recorded.inbound_count(), recorded.outbound_count()),
        (0, 0)
    );
}

#[test]
fn canonical_anchor_requires_a_zero_ether_delta() {
    let root = B256::ZERO;
    let settling = settling_with_effect_candidates(Vec::new());
    for claimed in [I256::ONE, -I256::ONE] {
        let mut batch = state_chain(&[root, root]);
        batch.entries[0].stateDeltas[0].etherDelta = claimed;
        assert_eq!(
            verify_effect_prefix(&batch, root, &[], &settling).err(),
            Some(EffectPrefixError::NonZeroAnchorEtherDelta { claimed })
        );
    }
}

#[test]
fn rejects_every_noncanonical_anchor_field() {
    let root = B256::ZERO;
    let valid = state_chain(&[root, root]);
    let mut cases = Vec::new();

    let mut batch = valid.clone();
    batch.entries[0].destinationRollupId = U256::from(2);
    cases.push(batch);

    let mut batch = valid.clone();
    batch.entries[0].expectedL1ToL2Calls.push(expected_call());
    cases.push(batch);

    let mut batch = valid.clone();
    batch.entries[0].expectedLookups.push(expected_lookup());
    cases.push(batch);

    let mut batch = valid.clone();
    batch.entries[0].callCount = U256::from(1);
    cases.push(batch);

    let mut batch = valid.clone();
    batch.entries[0].returnData = Bytes::from_static(b"not inert");
    cases.push(batch);

    let mut batch = valid;
    batch.entries[0].rollingHash = B256::repeat_byte(0xee);
    cases.push(batch);

    let settling = settling_with_effect_candidates(Vec::new());
    for batch in &cases {
        assert_eq!(
            verify_effect_prefix(batch, root, &[], &settling).err(),
            Some(EffectPrefixError::LeadingEntryNotAnchor {
                actual: ClaimedEntryShape::Invalid,
            })
        );
    }
}

#[test]
fn rejects_an_effect_in_the_leading_anchor_position() {
    let root = B256::ZERO;
    let settling = settling_with_effect_candidates(Vec::new());

    let mut inbound = state_chain(&[root, root]);
    inbound.entries[0].proxyEntryHash = B256::repeat_byte(0x11);
    assert_eq!(
        verify_effect_prefix(&inbound, root, &[], &settling).err(),
        Some(EffectPrefixError::LeadingEntryNotAnchor {
            actual: ClaimedEntryShape::Inbound,
        })
    );

    let mut outbound = state_chain(&[root, root]);
    outbound.entries[0].l2ToL1Calls.push(l2_to_l1_call());
    assert_eq!(
        verify_effect_prefix(&outbound, root, &[], &settling).err(),
        Some(EffectPrefixError::LeadingEntryNotAnchor {
            actual: ClaimedEntryShape::Outbound,
        })
    );
}

#[test]
fn rejects_later_anchors_and_invalid_entries() {
    let root = B256::ZERO;
    let settling = settling_with_effect_candidates(vec![0]);

    let anchors = state_chain(&[root, root, root]);
    assert_eq!(
        verify_effect_prefix(&anchors, root, &[], &settling).err(),
        Some(EffectPrefixError::LaterAnchor { entry_index: 1 })
    );

    let mut invalid = anchors;
    invalid.entries[1].callCount = U256::from(1);
    assert_eq!(
        verify_effect_prefix(&invalid, root, &[], &settling).err(),
        Some(EffectPrefixError::InvalidEntry { entry_index: 1 })
    );
}

#[test]
fn validates_effect_kinds_and_roots_by_candidate_position() {
    let anchor = B256::repeat_byte(0x0a);
    let pre_settling = B256::repeat_byte(0x0b);
    let outbound_root = B256::repeat_byte(0x0c);
    let inbound_root = B256::repeat_byte(0x0d);
    let batch = effect_batch(
        &[anchor, pre_settling, outbound_root, inbound_root],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Inbound],
    );
    let settling = settling_with_system_flags(vec![false, true]);
    let checkpoints = [checkpoint(0, outbound_root), checkpoint(1, inbound_root)];

    let facts = verify_effect_prefix(&batch, pre_settling, &checkpoints, &settling).unwrap();
    assert_eq!((facts.inbound_count(), facts.outbound_count()), (1, 1));
    assert_eq!(
        facts
            .effects()
            .iter()
            .map(|effect| (effect.entry_index(), effect.transaction_index()))
            .collect::<Vec<_>>(),
        [(1, 0), (2, 1)]
    );
}

#[test]
fn duplicate_roots_cannot_hide_reordered_effect_kinds() {
    let anchor = B256::repeat_byte(0x0a);
    let pre_settling = B256::repeat_byte(0x0b);
    let duplicate = B256::repeat_byte(0x0c);
    let settling = settling_with_system_flags(vec![false, true]);
    let checkpoints = [checkpoint(0, duplicate), checkpoint(1, duplicate)];

    let valid = effect_batch(
        &[anchor, pre_settling, duplicate, duplicate],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Inbound],
    );
    assert!(verify_effect_prefix(&valid, pre_settling, &checkpoints, &settling).is_ok());

    let reordered = effect_batch(
        &[anchor, pre_settling, duplicate, duplicate],
        &[ClaimedEntryShape::Inbound, ClaimedEntryShape::Outbound],
    );
    assert_eq!(
        verify_effect_prefix(&reordered, pre_settling, &checkpoints, &settling,).err(),
        Some(EffectPrefixError::EffectKindMismatch {
            entry_index: 1,
            transaction_index: 0,
            claimed: ClaimedEntryShape::Inbound,
            observed: ObservedEffectKind::Outbound,
        })
    );
}

#[test]
fn rejects_wrong_anchor_or_invalid_effect_checkpoints() {
    let anchor = B256::repeat_byte(0x0a);
    let pre_settling = B256::repeat_byte(0x0b);
    let effect_root = B256::repeat_byte(0x0c);
    let wrong = B256::repeat_byte(0xee);
    let batch = effect_batch(
        &[anchor, pre_settling, effect_root],
        &[ClaimedEntryShape::Outbound],
    );
    let settling = settling_with_system_flags(vec![false]);
    let valid_checkpoints = [checkpoint(0, effect_root)];

    assert_eq!(
        verify_effect_prefix(&batch, wrong, &valid_checkpoints, &settling).err(),
        Some(EffectPrefixError::AnchorRootMismatch {
            validated_pre_settling_root: wrong,
            claimed_anchor_post_state: pre_settling,
        })
    );
    assert_eq!(
        verify_effect_prefix(&batch, pre_settling, &[], &settling).err(),
        Some(EffectPrefixError::TransactionStateCheckpointCountMismatch {
            expected: 1,
            actual: 0,
        })
    );
    assert_eq!(
        verify_effect_prefix(
            &batch,
            pre_settling,
            &[checkpoint(0, effect_root), checkpoint(1, effect_root)],
            &settling,
        )
        .err(),
        Some(EffectPrefixError::TransactionStateCheckpointCountMismatch {
            expected: 1,
            actual: 2,
        })
    );
    assert_eq!(
        verify_effect_prefix(
            &batch,
            pre_settling,
            &[checkpoint(1, effect_root)],
            &settling,
        )
        .err(),
        Some(EffectPrefixError::TransactionStateCheckpointIndexMismatch {
            checkpoint_index: 0,
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(
        verify_effect_prefix(&batch, pre_settling, &[checkpoint(0, wrong)], &settling,).err(),
        Some(EffectPrefixError::EffectStateRootMismatch {
            entry_index: 1,
            transaction_index: 0,
            recomputed_checkpoint: wrong,
            claimed_post_state: effect_root,
        })
    );
}

#[test]
fn effect_checkpoints_cannot_hide_a_post_block_state_change() {
    let anchor = B256::repeat_byte(0x0a);
    let pre_settling = B256::repeat_byte(0x0b);
    let transaction_root = B256::repeat_byte(0x0c);
    let final_root = B256::repeat_byte(0x0d);
    let settling = settling_with_system_flags(vec![false]);
    let checkpoints = [checkpoint(0, transaction_root)];

    let transaction_endpoint = effect_batch(
        &[anchor, pre_settling, transaction_root],
        &[ClaimedEntryShape::Outbound],
    );
    assert_eq!(
        verify_state_delta_chain(
            &transaction_endpoint,
            expected_rollup_id(),
            anchor,
            final_root,
        )
        .map(|_| ()),
        Err(StateDeltaChainError::FinalMismatch {
            validated: final_root,
            claimed: transaction_root,
        })
    );

    let final_endpoint = effect_batch(
        &[anchor, pre_settling, final_root],
        &[ClaimedEntryShape::Outbound],
    );
    assert!(
        verify_state_delta_chain(&final_endpoint, expected_rollup_id(), anchor, final_root).is_ok()
    );
    assert_eq!(
        verify_effect_prefix(&final_endpoint, pre_settling, &checkpoints, &settling,).err(),
        Some(EffectPrefixError::EffectStateRootMismatch {
            entry_index: 1,
            transaction_index: 0,
            recomputed_checkpoint: transaction_root,
            claimed_post_state: final_root,
        })
    );
}

#[test]
fn rejects_reexecuted_effects_and_unused_checkpoints_for_an_anchor_only_claim() {
    let root = B256::ZERO;
    let batch = state_chain(&[root, root]);

    assert_eq!(
        verify_effect_prefix(&batch, root, &[], &settling_with_effect_candidates(vec![0]),).err(),
        Some(EffectPrefixError::EffectCountMismatch {
            claimed: 0,
            observed: 1,
        })
    );
    assert_eq!(
        verify_effect_prefix(
            &batch,
            root,
            &[checkpoint(0, root)],
            &settling_with_effect_candidates(Vec::new()),
        )
        .err(),
        Some(EffectPrefixError::TransactionStateCheckpointCountMismatch {
            expected: 0,
            actual: 1,
        })
    );
}
