use super::*;

/// Refresh the exact L1 commitment after a test mutates a claimed entry.
fn refresh_l1_rolling_hash(entry: &mut ExecutionEntrySol) {
    let mut rolling_hash = EntryRollingHash::for_l1(
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
    assert_eq!(
        encode_da_payload(&[Vec::new()], &[]),
        [0x00, 0xc4, 0xc1, 0x80, 0xc0, 0xc0]
    );
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

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::TrailingBytes { trailing: 2 })
    );
}

#[test]
fn da_payload_rejects_noncanonical_outer_and_integer_encodings() {
    let block_rlp = block_rlp(Vec::new());
    let canonical = encode_da_payload(&[Vec::new()], &[]);

    let mut wrong_tag = canonical.clone();
    wrong_tag[0] = 0x01;
    assert!(matches!(
        verify_anchor_only_da_payload(&wrong_tag, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::Decode { .. })
    ));

    let mut long_outer_header = vec![0x00, 0xf8, 0x04];
    long_outer_header.extend_from_slice(&canonical[2..]);
    assert!(matches!(
        verify_anchor_only_da_payload(&long_outer_header, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::Decode { .. })
    ));

    let mut leading_zero_count = canonical.clone();
    leading_zero_count[3] = 0x00;
    assert!(matches!(
        verify_anchor_only_da_payload(&leading_zero_count, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::Decode { .. })
    ));

    let mut extra_field = canonical;
    extra_field[1] += 1;
    extra_field.push(alloy_rlp::EMPTY_LIST_CODE);
    assert!(matches!(
        verify_anchor_only_da_payload(&extra_field, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::Decode { .. })
    ));
}

#[test]
fn da_payload_rejects_a_noncanonical_transaction_byte() {
    assert!(matches!(
        super::super::encoded_bytes_match(&[0x81, 0x01], &[0x01]),
        Err(DaPayloadError::Decode { .. })
    ));
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
    assert_eq!(
        verify_anchor_only_da_payload(&surplus, blocks),
        Err(DaPayloadError::UnexpectedItems {
            field: "blockTxCounts",
            expected: 2,
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
    let mut payload = encode_da_payload(&[Vec::new()], &[]);
    // The sole block count is one, while the canonical transaction list stays
    // empty. Both values have one-byte encodings, so the list headers remain
    // canonical and unchanged.
    payload[3] = 0x01;

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::TransactionMismatch {
            block_number: 41,
            transaction_index: 0,
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
    let mut payload = encode_da_payload(&[vec![Vec::new(), Vec::new()]], &[]);
    // Keep both transaction items but claim zero for this one-block payload.
    // The short-list encoding places its sole block count at byte 3.
    payload[3] = alloy_rlp::EMPTY_STRING_CODE;

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::UnexpectedItems {
            field: "transactions",
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

#[test]
fn da_payload_rejects_l2_entries_without_bound_inbound_effects() {
    let (block_rlp, transactions) = block_and_payload_transactions(Vec::new());
    let payload = encode_da_payload(&[transactions], &[vec![0x01]]);

    assert_eq!(
        verify_anchor_only_da_payload(&payload, [(41, block_rlp.as_slice())]),
        Err(DaPayloadError::UnexpectedItems {
            field: "l2Entries",
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
        Err(DaPayloadError::MissingL2Entry {
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
            field: "l2Entries",
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
        Err(DaPayloadError::L2EntryMismatch {
            entry_index: 0,
            transaction_index: 0,
        })
    );

    let mut mutated_sidecars = sidecars.clone();
    mutated_sidecars[1][0] ^= 1;
    let mutated_second_sidecar = encode_da_payload(
        &[intermediate_transactions.clone(), Vec::new()],
        &mutated_sidecars,
    );
    assert_eq!(
        verify(&mutated_second_sidecar, blocks),
        Err(DaPayloadError::L2EntryMismatch {
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
    let payload = encode_da_payload(&[vec![user.clone()]], &[sidecar.abi_encode()]);
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
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let missing_sidecar = encode_da_payload(&[vec![user.clone()]], &[]);
    assert_eq!(
        verify(&missing_sidecar, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::MissingL2Entry {
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
    let extra_transaction =
        encode_da_payload(&[vec![user.clone(), extra]], &[sidecar.abi_encode()]);
    assert_eq!(
        verify(&extra_transaction, [(41, extra_block.as_slice())]),
        Err(DaPayloadError::SyncBlockTransactionCount {
            expected: 2,
            actual: 3,
        })
    );

    // Composer DA carries the pre-settlement projection, not the batch entry
    // after its state update has been attached.
    let with_state_update =
        encode_da_payload(&[vec![user.clone()]], &[batch.entries[1].abi_encode()]);
    assert_eq!(
        verify(&with_state_update, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::L2EntryMismatch {
            entry_index: 0,
            transaction_index: 1,
        })
    );

    let (_, mut different_user) = block_and_payload_transactions(vec![transaction(CREATE_TX)]);
    let wrong_user = encode_da_payload(
        &[vec![different_user.pop().unwrap()]],
        &[sidecar.abi_encode()],
    );
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
        b256!("f3e1e42a3edb716155d9e5fee16a8cab4324b20ce27abaa729df594dca70b98d")
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
    let encoded_sidecars = sidecars
        .iter()
        .map(alloy_sol_types::SolValue::abi_encode)
        .collect::<Vec<_>>();
    let payload = encode_da_payload(std::slice::from_ref(&users), &encoded_sidecars);
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
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let mut reversed_sidecars = encoded_sidecars.clone();
    reversed_sidecars.reverse();
    let wrong_sidecars = encode_da_payload(std::slice::from_ref(&users), &reversed_sidecars);
    assert_eq!(
        verify(&wrong_sidecars, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::L2EntryMismatch {
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
    let sidecars = vec![outbound_sidecar.abi_encode(), inbound_sidecar.abi_encode()];
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
        )
    };

    assert_eq!(verify(&payload, [(41, settling_rlp.as_slice())]), Ok(()));

    let mut reversed_sidecars = sidecars.clone();
    reversed_sidecars.reverse();
    let wrong_sidecar_order =
        encode_da_payload(&[vec![raw_transactions[1].to_vec()]], &reversed_sidecars);
    assert_eq!(
        verify(&wrong_sidecar_order, [(41, settling_rlp.as_slice())]),
        Err(DaPayloadError::L2EntryMismatch {
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
