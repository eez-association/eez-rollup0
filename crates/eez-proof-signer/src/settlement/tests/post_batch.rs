use super::*;

fn assert_structure_rejected(name: &str, batch: &EvmBatch) {
    assert!(
        matches!(
            recompute_public_input_hash(
                batch,
                test_proof_system_vkey(),
                expected_rollup_id(),
                EXPECTED_PROOF_SYSTEM,
            ),
            Err(PublicInputError::InvalidStructure(_))
        ),
        "{name} was accepted"
    );
}

#[test]
fn canonical_calldata_decodes_without_applying_later_semantic_gates() {
    let expected = EvmBatch {
        blockNumber: 42,
        ..Default::default()
    };
    let calldata = encode_postbatch(&expected);

    let decoded = decode_canonical_post_batch(calldata).unwrap();

    assert_eq!(decoded.blockNumber, 42);
    assert!(decoded.entries.is_empty());
}

#[test]
fn target_batch_round_trips_through_the_canonical_abi() {
    let expected = target_anchor_batch();
    let decoded = decode_canonical_post_batch(encode_postbatch(&expected)).unwrap();

    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.entries[0].stateUpdates.len(), 1);
    assert_eq!(
        decoded.entries[0].abi_encode(),
        expected.entries[0].abi_encode()
    );
}

#[test]
fn target_batch_has_a_valid_state_update_chain() {
    let root = B256::repeat_byte(0x42);

    assert_eq!(
        verify_state_update_chain(&target_anchor_batch(), expected_rollup_id(), root, root)
            .map(|_| ()),
        Ok(())
    );
}

#[test]
fn wrong_selector_is_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::default());
    calldata[0] ^= 0xff;

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::InvalidAbi { .. })
    ));
}

#[test]
fn truncated_calldata_is_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::default());
    calldata.truncate(calldata.len() - 1);

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::InvalidAbi { .. })
    ));
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::default());
    calldata.extend_from_slice(&[0; 32]);

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::NonCanonical)
    ));
}

#[test]
fn signer_hash_matches_the_two_argument_protocol_api() {
    let batch = target_anchor_batch();
    let proof_system_vkey = test_proof_system_vkey();
    let expected = public_inputs_hashes(&batch, proof_system_vkey.get()).unwrap()[0];

    let result = recompute_public_input_hash(
        &batch,
        proof_system_vkey,
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();

    assert_eq!(result, expected);
}

#[test]
fn state_update_endpoints_are_committed_and_bound_to_reexecution() {
    let parent = B256::repeat_byte(0x11);
    let final_root = B256::repeat_byte(0x22);
    let wrong = B256::repeat_byte(0xee);
    let mut batch = carrier_batch();
    batch.entries.push(state_entry(1, parent, final_root));
    batch.immediateEntryCount = U256::from(1);

    assert_eq!(
        verify_state_update_chain(&batch, expected_rollup_id(), parent, final_root).map(|_| ()),
        Ok(())
    );
    let committed = recompute_public_input_hash(
        &batch,
        test_proof_system_vkey(),
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();

    let mut wrong_parent = batch.clone();
    wrong_parent.entries[0].stateUpdates[0].currentState = wrong;
    assert_ne!(
        recompute_public_input_hash(
            &wrong_parent,
            test_proof_system_vkey(),
            expected_rollup_id(),
            EXPECTED_PROOF_SYSTEM,
        )
        .unwrap(),
        committed
    );
    assert_eq!(
        verify_state_update_chain(&wrong_parent, expected_rollup_id(), parent, final_root)
            .map(|_| ()),
        Err(StateUpdateChainError::InitialRootMismatch {
            validated: parent,
            claimed: wrong,
        })
    );

    let mut wrong_final = batch;
    wrong_final.entries[0].stateUpdates[0].newState = wrong;
    assert_ne!(
        recompute_public_input_hash(
            &wrong_final,
            test_proof_system_vkey(),
            expected_rollup_id(),
            EXPECTED_PROOF_SYSTEM,
        )
        .unwrap(),
        committed
    );
    assert_eq!(
        verify_state_update_chain(&wrong_final, expected_rollup_id(), parent, final_root)
            .map(|_| ()),
        Err(StateUpdateChainError::FinalMismatch {
            validated: final_root,
            claimed: wrong,
        })
    );
}

#[test]
fn proofs_are_not_an_input_to_the_hash_gate() {
    let mut batch = carrier_batch();
    let expected = public_inputs_hashes(&batch, test_proof_system_vkey().get()).unwrap()[0];
    batch.proofs = vec![Bytes::from_static(b"ignored"), Bytes::from(vec![0xaa; 65])];

    let result = recompute_public_input_hash(
        &batch,
        test_proof_system_vkey(),
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();

    assert_eq!(result, expected);
}

#[test]
fn accepts_only_the_exact_leading_zero_proxy_run_as_immediate() {
    let root = B256::ZERO;
    let mut valid = carrier_batch();
    valid.entries = vec![
        state_entry(1, root, root),
        state_entry(1, root, root),
        state_entry(1, root, root),
    ];
    valid.entries[2].proxyEntryHash = B256::repeat_byte(0x33);
    valid.immediateEntryCount = U256::from(2);
    assert!(
        recompute_public_input_hash(
            &valid,
            test_proof_system_vkey(),
            expected_rollup_id(),
            EXPECTED_PROOF_SYSTEM,
        )
        .is_ok()
    );

    for (name, count) in [
        ("count shorter than leading run", U256::from(1)),
        ("count longer than leading run", U256::from(3)),
        ("count does not fit usize", U256::MAX),
    ] {
        let mut batch = valid.clone();
        batch.immediateEntryCount = count;
        assert_structure_rejected(name, &batch);
    }
}

#[test]
fn rejects_every_public_input_structural_violation() {
    let valid = carrier_batch();
    let mut cases = Vec::new();

    let mut batch = valid.clone();
    batch.proofSystems.clear();
    cases.push(("no proof system", batch));

    let mut batch = valid.clone();
    batch.proofSystems[0] = Address::ZERO;
    cases.push(("zero proof system", batch));

    let mut batch = valid.clone();
    batch.proofSystems[0] = address!("00000000000000000000000000000000000000bb");
    cases.push(("different nonzero proof system", batch));

    let mut batch = valid.clone();
    batch
        .proofSystems
        .push(address!("00000000000000000000000000000000000000bb"));
    cases.push(("multiple proof systems", batch));

    let mut batch = valid.clone();
    batch.rollupIdsWithProofSystems.clear();
    cases.push(("no rollup assignments", batch));

    let mut batch = valid.clone();
    batch.rollupIdsWithProofSystems[0].rollupId = 2;
    cases.push(("assignment for a different rollup", batch));

    let mut batch = valid.clone();
    batch.rollupIdsWithProofSystems[0].rollupId = 0;
    cases.push(("zero rollup id", batch));

    let mut batch = valid.clone();
    batch.rollupIdsWithProofSystems.push(rollup_row(2));
    cases.push(("multiple rollup assignments", batch));

    for indices in [Vec::new(), vec![1], vec![0, 0], vec![0, 1]] {
        let mut batch = valid.clone();
        batch.rollupIdsWithProofSystems[0].proofSystemIndexes = indices;
        cases.push(("invalid proof-system indices", batch));
    }

    let mut batch = valid.clone();
    batch.expectedStateRootPerRollup = vec![ExpectedStateRootPerRollupSol {
        rollupId: 1,
        stateRoot: B256::ZERO,
    }];
    cases.push(("expected state-root pin", batch));

    let mut batch = target_anchor_batch();
    batch.entries[0].destinationRollupId = 2;
    cases.push(("entry destination outside batch", batch));

    let mut batch = valid.clone();
    batch.staticEntries.push(StaticExecutionEntrySol::default());
    cases.push(("static entry", batch));

    let mut batch = valid.clone();
    batch.immediateStaticEntryCount = U256::from(1);
    cases.push(("immediate static entry count", batch));

    let mut batch = valid.clone();
    batch.blockNumber = 1;
    cases.push(("bound block number", batch));

    let mut batch = valid.clone();
    batch.blobIndices.push(U256::ZERO);
    cases.push(("blob index", batch));

    let mut batch = valid;
    batch.bindMsgSenderInPublicInput = true;
    cases.push(("bound sender", batch));

    for (name, batch) in &cases {
        assert_structure_rejected(name, batch);
    }
}
