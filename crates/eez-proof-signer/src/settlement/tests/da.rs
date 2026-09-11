use super::*;

/// Refresh the exact L1 commitment after a test mutates a claimed entry.
fn refresh_l1_rolling_hash(entry: &mut ExecutionEntrySol) {
    let mut rolling_hash = EntryRollingHash::seed_for_l1(
        entry
            .stateUpdates
            .iter()
            .map(|update| (update.rollupId, update.currentState)),
        entry.proxyEntryHash,
    );
    match entry.l2ToL1Calls.as_slice() {
        [] => {}
        [call] => {
            let mode = if call.isStatic {
                CallMode::Static
            } else {
                CallMode::Mutable
            };
            let call_hash = common_cross_chain_call_hash(CallHashInput {
                call_mode: mode,
                source_address: call.sourceAddress,
                source_rollup_id: RollupId(call.sourceRollupId),
                target_address: call.targetAddress,
                target_rollup_id: RollupId::MAINNET,
                value: call.value,
                data: &call.data,
            });
            rolling_hash.call_begin(call_hash);
            rolling_hash.call_end(entry.success, &entry.returnData);
        }
        calls => panic!(
            "DA fixture supports at most one L2-to-L1 call, got {}",
            calls.len()
        ),
    }
    entry.rollingHash = rolling_hash.current();
}

#[test]
fn test_da_payload_encoder_matches_the_wire_format() {
    // 00            EEZ protocol version (never a message)
    // 02            ChainOperation
    // <u64 LE>      chain_id
    // 1a <26 bytes> operations: the published 0x00 span, length-prefixed
    let span = [
        vec![0x00, 0x01, 0x00],
        vec![0x01],
        vec![0x00; 20],
        vec![0x01, 0x00],
    ]
    .concat();
    let expected = [
        vec![0x00, 0x02],
        7331u64.to_le_bytes().to_vec(),
        vec![u8::try_from(span.len()).unwrap()],
        span,
    ]
    .concat();
    assert_eq!(encode_da_payload(&[Vec::new()], &[]), expected);
}

#[test]
fn da_payload_matches_exact_transactions_in_every_validated_block() {
    let (first_rlp, first_transactions) =
        block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let (second_rlp, second_transactions) =
        block_and_payload_transactions(vec![transaction(EIP1559_SYSTEM_TX)]);
    let payload = encode_da_payload(&[first_transactions, second_transactions], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(
            &payload,
            [(41, first_rlp.as_slice()), (42, second_rlp.as_slice())],
        ),
        Ok(())
    );
}

#[test]
fn da_payload_rejects_trailing_bytes() {
    let (block_rlp, transactions) = block_and_payload_transactions(Vec::new());
    let mut payload = encode_da_payload(&[transactions], &[]);
    payload.extend_from_slice(&[0xde, 0xad]);

    // The stream must end exactly at a message boundary, so trailing bytes are
    // read as the start of another bracket and fail the grammar rather than
    // being counted as a tail.
    assert!(matches!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::Decode { .. })
    ));
}

#[test]
fn da_payload_rejects_noncanonical_outer_and_integer_encodings() {
    let block_rlp = block_rlp(Vec::new());
    let canonical = encode_da_payload(&[Vec::new()], &[]);
    // 0: stream version | 1: message type | 2..10: chain_id | 10: operations len
    let verify_rejects = |payload: &[u8]| {
        matches!(
            verify_anchor_only_da_payload(payload, [(41, block_rlp.as_slice())]),
            Err(DaPayloadError::Decode { .. })
        )
    };

    let mut wrong_stream_version = canonical.clone();
    wrong_stream_version[0] = 0x01;
    assert!(verify_rejects(&wrong_stream_version), "stream version");

    let mut wrong_message_type = canonical.clone();
    wrong_message_type[1] = 0x04; // Call where a ChainOperation must open
    assert!(verify_rejects(&wrong_message_type), "message type");

    let mut wrong_span_version = canonical.clone();
    wrong_span_version[11] = 0x01; // the span's own payload version
    assert!(verify_rejects(&wrong_span_version), "span version");

    // A padded two-byte varint is a second encoding of the same length.
    let non_shortest_len = [&canonical[..10], &[0x81, 0x00][..], &canonical[11..]].concat();
    assert!(verify_rejects(&non_shortest_len), "non-shortest varint");
}

#[test]
fn da_payload_rejects_missing_or_surplus_blocks() {
    let (first_rlp, first_transactions) = block_and_payload_transactions(Vec::new());
    let (second_rlp, second_transactions) = block_and_payload_transactions(Vec::new());
    let blocks = [(41, first_rlp.as_slice()), (42, second_rlp.as_slice())];

    let missing = encode_da_payload(&[first_transactions], &[]);
    assert_eq!(
        verify_anchor_only_da_payload(&missing, blocks),
        Err(DaPayloadError::BlockCount {
            expected: 2,
            actual: 1,
        })
    );

    let surplus = encode_da_payload(&[Vec::new(), second_transactions, Vec::new()], &[]);
    // The span declares its block count up front, so a surplus is a count
    // disagreement rather than leftover items in a trailing list.
    assert_eq!(
        verify_anchor_only_da_payload(&surplus, blocks),
        Err(DaPayloadError::BlockCount {
            expected: 2,
            actual: 3,
        })
    );
}

#[test]
fn da_payload_rejects_transactions_assigned_to_the_wrong_block() {
    let (first_rlp, first_transactions) =
        block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let (second_rlp, _) = block_and_payload_transactions(Vec::new());
    let payload = encode_da_payload(&[Vec::new(), first_transactions], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(
            &payload,
            [(41, first_rlp.as_slice()), (42, second_rlp.as_slice())],
        ),
        Err(DaPayloadError::ProjectedTransactionCount {
            block_number: 41,
            submitted: 0,
            expected: 1,
        })
    );
}

#[test]
fn da_payload_rejects_a_canonical_short_transaction_list_as_a_window_mismatch() {
    let block_rlp = block_rlp(vec![transaction(CREATE_TX)]);
    // A perfectly canonical payload that simply omits the block's transaction:
    // the disagreement is with the validated window, not within the encoding.
    let payload = encode_da_payload(&[Vec::new()], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::ProjectedTransactionCount {
            block_number: 41,
            submitted: 0,
            expected: 1,
        })
    );
}

#[test]
fn da_payload_rejects_different_transaction_bytes_at_the_same_position() {
    let (block_rlp, mut different_transactions) =
        block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    different_transactions[0][0] ^= 1;
    let payload = encode_da_payload(&[different_transactions], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::TransactionMismatch {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

#[test]
fn da_payload_rejects_transactions_reordered_within_one_block() {
    let (block_rlp, _) = block_and_payload_transactions(vec![
        transaction(CREATE_TX),
        transaction(EIP1559_SYSTEM_TX),
    ]);
    let (_, reordered) = block_and_payload_transactions(vec![
        transaction(EIP1559_SYSTEM_TX),
        transaction(CREATE_TX),
    ]);
    let payload = encode_da_payload(&[reordered], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::TransactionMismatch {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

#[test]
fn da_payload_stops_lists_that_exceed_validated_window_bounds() {
    let block_rlp = block_rlp(Vec::new());
    // A payload claiming more transactions than the window's block holds. The
    // span derives its transaction total from the per-block counts and the
    // codec requires the columns to agree, so surplus items cannot survive
    // decoding — the overclaim can only show up against the validated block.
    let (_, two_transactions) = block_and_payload_transactions(vec![
        transaction(CREATE_TX),
        transaction(SYSTEM_SIGNER_OTHER_TARGET_TX),
    ]);
    let payload = encode_da_payload(&[two_transactions], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::ProjectedTransactionCount {
            block_number: 41,
            submitted: 2,
            expected: 0,
        })
    );
}

#[test]
fn da_payload_compares_oversized_transactions_without_decoding_them_into_a_vec() {
    let (block_rlp, transactions) = block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let validated_bytes = transactions[0].len();
    let oversized = encode_da_payload(&[vec![vec![0; validated_bytes + 1]]], &[]);

    assert_eq!(
        verify_anchor_only_da_payload(&oversized, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::TransactionMismatch {
            block_number: 41,
            transaction_index: 0,
        })
    );
}

/// A well-formed entry that no authorized effect claims.
fn unbound_entry() -> ExecutionEntrySol {
    ExecutionEntrySol {
        l2ToL1Calls: vec![eez_protocol::abi::L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: alloy_primitives::Address::ZERO,
            sourceRollupId: 0,
            targetAddress: alloy_primitives::Address::ZERO,
            value: alloy_primitives::U256::ZERO,
            data: alloy_primitives::Bytes::new(),
        }],
        destinationRollupId: 1,
        success: true,
        ..Default::default()
    }
}

#[test]
fn da_payload_rejects_l2_entries_without_bound_inbound_effects() {
    let (block_rlp, transactions) = block_and_payload_transactions(Vec::new());
    let payload = encode_da_payload(&[transactions], std::slice::from_ref(&unbound_entry()));

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::UnexpectedItems {
            field: "actions",
            expected: 0,
        })
    );
}

#[test]
fn da_payload_binds_inbound_sidecars_and_complete_reconstructed_transactions() {
    let settling = SettlingBlockObservations::for_test(
        vec![true, true],
        vec![
            observed_inbound_candidate(0, U256::from(7), true),
            observed_inbound_candidate(1, U256::from(9), true),
        ],
        Vec::new(),
    );
    let batch = bindable_inbound_batch(&settling);
    let plan = effect_plan(&batch, &settling);
    let inbound = verify_inbound_effect_entries(&plan).unwrap();
    let sidecars = settling
        .inbound_candidates()
        .iter()
        .map(|candidate| {
            candidate
                .inspection
                .as_ref()
                .unwrap()
                .derived_da_entry
                .encoded()
        })
        .collect::<Vec<_>>();
    let entries = sidecars
        .iter()
        .map(|encoded| ExecutionEntrySol::abi_decode(encoded).unwrap())
        .collect::<Vec<_>>();
    let sidecars = entries.clone();
    let transactions = build_inbound_transactions(&entries, &system_transaction_context(), 11);
    let (intermediate_rlp, intermediate_transactions) =
        block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let (settling_rlp, raw_transactions) = block_and_payload_transactions(transactions.clone());
    let verifier = system_transactions();
    let outbound = AuthorizedOutboundEffects::default();
    let verify = |payload: &[u8], blocks: [(u64, &[u8]); 2]| {
        let (settling, intermediates) = blocks.split_last().unwrap();
        verify_da_payload(
            payload,
            intermediates.iter().copied(),
            *settling,
            &outbound,
            &inbound,
            &verifier,
            1,
        )
    };
    let blocks = [
        (40, intermediate_rlp.as_slice()),
        (41, settling_rlp.as_slice()),
    ];

    // Ordinary transactions remain in DA for intermediate blocks. Only the
    // two bound inbound transactions are omitted from the terminal block.
    let payload = encode_da_payload(&[intermediate_transactions.clone(), Vec::new()], &sidecars);
    assert_eq!(verify(&payload, blocks), Ok(()));

    let missing = encode_da_payload(
        &[intermediate_transactions.clone(), Vec::new()],
        &sidecars[..1],
    );
    assert_eq!(
        verify(&missing, blocks),
        Err(DaPayloadError::MissingAction {
            entry_index: 1,
            transaction_index: 1,
        })
    );

    let mut extra_sidecars = sidecars.clone();
    extra_sidecars.push(sidecars[0].clone());
    let extra = encode_da_payload(
        &[intermediate_transactions.clone(), Vec::new()],
        &extra_sidecars,
    );
    assert_eq!(
        verify(&extra, blocks),
        Err(DaPayloadError::UnexpectedItems {
            field: "actions",
            expected: 2,
        })
    );

    let mut reordered_sidecars = sidecars.clone();
    reordered_sidecars.swap(0, 1);
    let reordered = encode_da_payload(
        &[intermediate_transactions.clone(), Vec::new()],
        &reordered_sidecars,
    );
    assert_eq!(
        verify(&reordered, blocks),
        Err(DaPayloadError::ActionMismatch {
            entry_index: 0,
            transaction_index: 0,
        })
    );

    let mut mutated_sidecars = sidecars.clone();
    mutated_sidecars[1].l2ToL1Calls[0].value += alloy_primitives::U256::from(1);
    let mutated_second_sidecar = encode_da_payload(
        &[intermediate_transactions.clone(), Vec::new()],
        &mutated_sidecars,
    );
    assert_eq!(
        verify(&mutated_second_sidecar, blocks),
        Err(DaPayloadError::ActionMismatch {
            entry_index: 1,
            transaction_index: 1,
        })
    );

    let system_txs_in_da = encode_da_payload(
        &[intermediate_transactions.clone(), raw_transactions],
        &sidecars,
    );
    assert_eq!(
        verify(&system_txs_in_da, blocks),
        Err(DaPayloadError::ProjectedTransactionCount {
            block_number: 41,
            submitted: 2,
            expected: 0,
        })
    );

    let mut noncanonical_context = system_transaction_context();
    noncanonical_context.l2_gas_limit += 1;
    let noncanonical_transactions = build_inbound_transactions(&entries, &noncanonical_context, 11);
    let mut mutated_second_transaction = transactions;
    mutated_second_transaction[1] = noncanonical_transactions[1].clone();
    let wrong_block = block_rlp(mutated_second_transaction);
    assert_eq!(
        verify(
            &payload,
            [
                (40, intermediate_rlp.as_slice()),
                (41, wrong_block.as_slice()),
            ],
        ),
        Err(DaPayloadError::SyncBlockTransactionMismatch {
            transaction_index: 1,
        })
    );
}

#[test]
fn da_payload_binds_outbound_sidecars_users_and_system_loads() {
    let mut batch = effect_batch(&[B256::ZERO; 3], &[ClaimedEntryShape::Outbound]);
    let value = U256::from(7);
    batch.entries[1].l2ToL1Calls[0].value = value;
    batch.entries[1].stateUpdates[0].etherDelta = -I256::try_from(value).unwrap();
    refresh_l1_rolling_hash(&mut batch.entries[1]);
    let mut settling = settling_with_outbound_pairs(1);
    settling
        .outbound_event_candidates_mut_for_test()
        .push(observed_outbound_call(
            1,
            0,
            &batch.entries[1].l2ToL1Calls[0],
        ));
    let plan = effect_plan(&batch, &settling);
    let outbound = authorize_outbound_effects(&plan).unwrap();
    let mut sidecar = batch.entries[1].clone();
    sidecar.stateUpdates.clear();
    sidecar.rollingHash = B256::ZERO;

    let (_, mut user_payload) = block_and_payload_transactions(vec![user_transaction(7)]);
    let user = user_payload.pop().unwrap();
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(sidecar.clone(), Bytes::from(user.clone()))],
        &[],
        &system_transaction_context(),
        11,
    )
    .unwrap();
    let raw_transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);
    let transactions = raw_transactions
        .iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect::<Vec<_>>();
    let settling_rlp = block_rlp(transactions);
    let verifier = system_transactions();
    let payload = encode_da_payload(&[vec![user.clone()]], &[sidecar.clone()]);
    let inbound = AuthorizedInboundEffects::default();
    let verify = |payload: &[u8], blocks: [(u64, &[u8]); 1]| {
        let (settling, intermediates) = blocks.split_last().unwrap();
        verify_da_payload(
            payload,
            intermediates.iter().copied(),
            *settling,
            &outbound,
            &inbound,
            &verifier,
            1,
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let missing_sidecar = encode_da_payload(&[vec![user.clone()]], &[]);
    assert_eq!(
        verify(&missing_sidecar, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::MissingAction {
            entry_index: 0,
            transaction_index: 1,
        })
    );

    let (_, mut extra_payload) = block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let extra = extra_payload.pop().unwrap();
    let mut extra_transactions = raw_transactions;
    extra_transactions.push(Bytes::from(extra.clone()));
    let extra_block = block_rlp(
        extra_transactions
            .into_iter()
            .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
            .collect(),
    );
    let extra_transaction = encode_da_payload(&[vec![user.clone(), extra]], &[sidecar.clone()]);
    assert_eq!(
        verify(&extra_transaction, [(41, extra_block.as_slice())]),
        Err(DaPayloadError::SyncBlockTransactionCount {
            expected: 2,
            actual: 3,
        })
    );

    // An action describes the CALL, so the pre-settlement projection and the
    // same entry with its state update attached project identically — the DA
    // no longer distinguishes them, and does not need to: EEZ hashes the full
    // entry (state updates included) into the public input this signer
    // recomputes, and enforces the delta chain at execution.
    let with_state_update = encode_da_payload(&[vec![user.clone()]], &[batch.entries[1].clone()]);
    assert_eq!(
        verify(&with_state_update, [(41, settling_rlp.as_slice())]),
        Ok(())
    );

    let unrelated_call = encode_da_payload(&[vec![user.clone()]], &[unbound_entry()]);
    assert_eq!(
        verify(&unrelated_call, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::ActionMismatch {
            entry_index: 0,
            transaction_index: 1,
        })
    );

    let (_, mut different_user) = block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let wrong_user = encode_da_payload(&[vec![different_user.pop().unwrap()]], &[sidecar.clone()]);
    assert_eq!(
        verify(&wrong_user, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::TransactionMismatch {
            block_number: 41,
            transaction_index: 1,
        })
    );

    let mut noncanonical_context = system_transaction_context();
    noncanonical_context.l2_gas_limit += 1;
    let wrong_pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(sidecar.clone(), Bytes::from(user))],
        &[],
        &noncanonical_context,
        11,
    )
    .unwrap();
    let wrong_transactions = eez_protocol::system_tx::interleave_sync_block_txs(&wrong_pairs)
        .into_iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect::<Vec<_>>();
    let wrong_block = block_rlp(wrong_transactions);
    assert_eq!(
        verify(&payload, [(41, wrong_block.as_slice())]),
        Err(DaPayloadError::SyncBlockTransactionMismatch {
            transaction_index: 0,
        })
    );
}

#[test]
fn da_payload_binds_multiple_outbound_pairs_and_system_nonce_progression() {
    let mut batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Outbound],
    );
    batch.entries[2].l2ToL1Calls[0].data = Bytes::from_static(&[0x02]);
    batch.entries[2].returnData = Bytes::from_static(&[0xca, 0xfe]);
    refresh_l1_rolling_hash(&mut batch.entries[2]);
    assert_eq!(
        batch.entries[2].rollingHash,
        b256!("78f69e61b6a717b35a9ded7bd8eb7b8782e70680d1335623833272cfa66f5921")
    );
    let mut settling = settling_with_outbound_pairs(2);
    *settling.outbound_event_candidates_mut_for_test() = vec![
        observed_outbound_call(1, 0, &batch.entries[1].l2ToL1Calls[0]),
        observed_outbound_call(3, 0, &batch.entries[2].l2ToL1Calls[0]),
    ];
    let plan = effect_plan(&batch, &settling);
    let outbound = authorize_outbound_effects(&plan).unwrap();
    let sidecars = batch
        .entries
        .iter()
        .skip(1)
        .cloned()
        .map(|mut entry| {
            entry.stateUpdates.clear();
            entry.rollingHash = B256::ZERO;
            entry
        })
        .collect::<Vec<_>>();
    let (_, users) = block_and_payload_transactions(vec![user_transaction(7), user_transaction(8)]);
    let outbound_inputs = sidecars
        .iter()
        .cloned()
        .zip(users.iter().cloned().map(Bytes::from))
        .collect::<Vec<_>>();
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &outbound_inputs,
        &[],
        &system_transaction_context(),
        11,
    )
    .unwrap();
    let raw_transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);
    let first_load: TransactionSigned =
        alloy_rlp::decode_exact(raw_transactions[0].as_ref()).unwrap();
    let second_load: TransactionSigned =
        alloy_rlp::decode_exact(raw_transactions[2].as_ref()).unwrap();
    assert_eq!((first_load.nonce(), second_load.nonce()), (11, 12));
    let settling_rlp = block_rlp(
        raw_transactions
            .iter()
            .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
            .collect(),
    );
    let payload = encode_da_payload(std::slice::from_ref(&users), &sidecars);
    let verifier = system_transactions();
    let inbound = AuthorizedInboundEffects::default();
    let verify = |payload: &[u8], blocks: [(u64, &[u8]); 1]| {
        let (settling, intermediates) = blocks.split_last().unwrap();
        verify_da_payload(
            payload,
            intermediates.iter().copied(),
            *settling,
            &outbound,
            &inbound,
            &verifier,
            1,
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let mut reversed_sidecars = sidecars.clone();
    reversed_sidecars.reverse();
    let wrong_sidecars = encode_da_payload(std::slice::from_ref(&users), &reversed_sidecars);
    assert_eq!(
        verify(&wrong_sidecars, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::ActionMismatch {
            entry_index: 0,
            transaction_index: 1,
        })
    );

    let mut noncanonical_context = system_transaction_context();
    noncanonical_context.l2_gas_limit += 1;
    let wrong_pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &outbound_inputs,
        &[],
        &noncanonical_context,
        11,
    )
    .unwrap();
    let wrong_transactions = eez_protocol::system_tx::interleave_sync_block_txs(&wrong_pairs);
    let mut mutated_second_load = raw_transactions;
    mutated_second_load[2] = wrong_transactions[2].clone();
    let wrong_block = block_rlp(
        mutated_second_load
            .into_iter()
            .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
            .collect(),
    );
    assert_eq!(
        verify(&payload, [(41, wrong_block.as_slice())]),
        Err(DaPayloadError::SyncBlockTransactionMismatch {
            transaction_index: 2,
        })
    );
}

#[test]
fn da_payload_binds_the_complete_mixed_sync_sequence_and_sidecar_order() {
    let mut settling = SettlingBlockObservations::for_test(
        vec![true, false, true],
        vec![observed_inbound_candidate(2, U256::from(9), true)],
        Vec::new(),
    );
    let mut batch = effect_batch(
        &[B256::ZERO; 4],
        &[ClaimedEntryShape::Outbound, ClaimedEntryShape::Inbound],
    );
    let inbound_observation = settling.inbound_candidates()[0]
        .inspection
        .as_ref()
        .unwrap();
    batch.entries[2].proxyEntryHash = inbound_observation.recomputed_call_hash;
    batch.entries[2].returnData = inbound_observation.return_data.clone();
    batch.entries[2].stateUpdates[0].etherDelta =
        I256::try_from(inbound_observation.value).unwrap();
    refresh_l1_rolling_hash(&mut batch.entries[2]);
    settling
        .outbound_event_candidates_mut_for_test()
        .push(observed_outbound_call(
            1,
            0,
            &batch.entries[1].l2ToL1Calls[0],
        ));

    let plan = effect_plan(&batch, &settling);
    let outbound = authorize_outbound_effects(&plan).unwrap();
    let inbound = verify_inbound_effect_entries(&plan).unwrap();
    let mut outbound_sidecar = batch.entries[1].clone();
    outbound_sidecar.stateUpdates.clear();
    outbound_sidecar.rollingHash = B256::ZERO;
    let inbound_sidecar = settling.inbound_candidates()[0]
        .inspection
        .as_ref()
        .unwrap()
        .derived_da_entry
        .as_entry()
        .clone();
    let (_, mut user_payload) = block_and_payload_transactions(vec![user_transaction(7)]);
    let user = user_payload.pop().unwrap();
    let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
        &[(outbound_sidecar.clone(), Bytes::from(user.clone()))],
        std::slice::from_ref(&inbound_sidecar),
        &system_transaction_context(),
        11,
    )
    .unwrap();
    let raw_transactions = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);
    let transactions = raw_transactions
        .iter()
        .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
        .collect::<Vec<_>>();
    let settling_rlp = block_rlp(transactions);
    let sidecars = vec![outbound_sidecar.clone(), inbound_sidecar.clone()];
    let payload = encode_da_payload(&[vec![user]], &sidecars);
    let verifier = system_transactions();
    let verify = |payload: &[u8], blocks: [(u64, &[u8]); 1]| {
        let (settling, intermediates) = blocks.split_last().unwrap();
        verify_da_payload(
            payload,
            intermediates.iter().copied(),
            *settling,
            &outbound,
            &inbound,
            &verifier,
            1,
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let mut reversed_sidecars = sidecars.clone();
    reversed_sidecars.reverse();
    let wrong_sidecar_order =
        encode_da_payload(&[vec![raw_transactions[1].to_vec()]], &reversed_sidecars);
    assert_eq!(
        verify(&wrong_sidecar_order, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::ActionMismatch {
            entry_index: 0,
            transaction_index: 1,
        })
    );

    let mut reordered = raw_transactions;
    reordered.swap(0, 2);
    let reordered_block = block_rlp(
        reordered
            .into_iter()
            .map(|raw| alloy_rlp::decode_exact(raw.as_ref()).unwrap())
            .collect(),
    );
    assert_eq!(
        verify(&payload, [(41, reordered_block.as_slice())]),
        Err(DaPayloadError::SyncBlockTransactionMismatch {
            transaction_index: 0,
        })
    );
}

#[test]
fn da_payload_distinguishes_malformed_payloads_from_invalid_validated_blocks() {
    let valid_block = block_rlp(Vec::new());
    let malformed = verify_anchor_only_da_payload(&[], [(41, valid_block.as_slice())]);
    assert!(matches!(malformed, Err(DaPayloadError::Decode { .. })));

    let payload = encode_da_payload(&[Vec::new()], &[]);
    assert!(matches!(
        verify_anchor_only_da_payload(&payload, [(41, [0xff].as_slice())]),
        Err(DaPayloadError::InvalidBlockRlp {
            block_number: 41,
            ..
        })
    ));
}
