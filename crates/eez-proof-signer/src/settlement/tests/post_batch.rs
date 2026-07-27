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
    let mut expected = EvmBatch::empty();
    expected.inner.blockNumber = 42;
    let calldata = encode_postbatch(&expected);

    let decoded = decode_canonical_post_batch(calldata).unwrap();

    assert_eq!(decoded.inner.blockNumber, 42);
    assert!(decoded.inner.entries.is_empty());
}

#[test]
fn recorded_post_batch_decodes() {
    let decoded = recorded_batch();

    assert_eq!(decoded.inner.entries.len(), 1);
    assert_eq!(decoded.inner.entries[0].stateDeltas.len(), 1);
}

#[test]
fn recorded_post_batch_has_a_valid_state_delta_chain() {
    let root = b256!("f09d8f7da5bc5036f8dd9536c953e2212390a46fb3e553ece2b7d419131537b1");

    assert_eq!(
        verify_state_delta_chain(&recorded_batch(), expected_rollup_id(), root, root).map(|_| ()),
        Ok(())
    );
}

#[test]
fn wrong_selector_is_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::empty());
    calldata[0] ^= 0xff;

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::InvalidAbi { .. })
    ));
}

#[test]
fn truncated_calldata_is_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::empty());
    calldata.truncate(calldata.len() - 1);

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::InvalidAbi { .. })
    ));
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut calldata = encode_postbatch(&EvmBatch::empty());
    calldata.extend_from_slice(&[0; 32]);

    assert!(matches!(
        decode_canonical_post_batch(calldata),
        Err(PostBatchDecodeError::NonCanonical)
    ));
}

#[test]
fn recomputes_the_recorded_public_input_hash() {
    let proof_system_vkey = NonZeroProofSystemVkey::new(b256!(
        "000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266"
    ))
    .unwrap();
    let expected = b256!("e5cd0221135432a8f42b61e68f71f809d7e9b973c6866da2446fca8dd1339c98");

    let batch = recorded_batch();
    let expected_proof_system = batch.inner.proofSystems[0];
    let result = recompute_public_input_hash(
        &batch,
        proof_system_vkey,
        expected_rollup_id(),
        expected_proof_system,
    )
    .unwrap();

    assert_eq!(result, expected);
}

#[test]
fn state_delta_endpoints_are_committed_and_bound_to_reexecution() {
    let parent = B256::repeat_byte(0x11);
    let final_root = B256::repeat_byte(0x22);
    let wrong = B256::repeat_byte(0xee);
    let mut batch = carrier_batch();
    batch
        .inner
        .entries
        .push(state_entry(U256::from(1), parent, final_root));

    assert_eq!(
        verify_state_delta_chain(&batch, expected_rollup_id(), parent, final_root).map(|_| ()),
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
    wrong_parent.inner.entries[0].stateDeltas[0].currentState = wrong;
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
        verify_state_delta_chain(&wrong_parent, expected_rollup_id(), parent, final_root)
            .map(|_| ()),
        Err(StateDeltaChainError::InitialRootMismatch {
            validated: parent,
            claimed: wrong,
        })
    );

    let mut wrong_final = batch;
    wrong_final.inner.entries[0].stateDeltas[0].newState = wrong;
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
        verify_state_delta_chain(&wrong_final, expected_rollup_id(), parent, final_root)
            .map(|_| ()),
        Err(StateDeltaChainError::FinalMismatch {
            validated: final_root,
            claimed: wrong,
        })
    );
}

#[test]
fn proofs_are_not_an_input_to_the_hash_gate() {
    let mut batch = carrier_batch();
    let hash = public_inputs_hashes(&batch, test_proof_system_vkey().get(), None).unwrap()[0];
    batch.inner.proofs = vec![Bytes::from_static(b"ignored"), Bytes::from(vec![0xaa; 65])];

    let result = recompute_public_input_hash(
        &batch,
        test_proof_system_vkey(),
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();

    assert_eq!(result, hash);
}

#[test]
fn transient_counts_are_not_inputs_to_the_hash_gate() {
    let batch = carrier_batch();
    let expected = recompute_public_input_hash(
        &batch,
        test_proof_system_vkey(),
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();
    let mut rescheduled = batch;
    rescheduled.inner.transientExecutionEntryCount = U256::MAX;
    rescheduled.inner.transientLookupCallCount = U256::MAX;

    let result = recompute_public_input_hash(
        &rescheduled,
        test_proof_system_vkey(),
        expected_rollup_id(),
        EXPECTED_PROOF_SYSTEM,
    )
    .unwrap();

    assert_eq!(result, expected);
}

#[test]
fn rejects_every_public_input_structural_violation() {
    let valid = carrier_batch();
    let mut cases = Vec::new();

    let mut batch = valid.clone();
    batch.inner.proofSystems.clear();
    cases.push(("no proof system", batch));

    let mut batch = valid.clone();
    batch.inner.proofSystems[0] = Address::ZERO;
    cases.push(("zero proof system", batch));

    let mut batch = valid.clone();
    batch.inner.proofSystems[0] = address!("00000000000000000000000000000000000000bb");
    cases.push(("different nonzero proof system", batch));

    let mut batch = valid.clone();
    batch
        .inner
        .proofSystems
        .push(address!("00000000000000000000000000000000000000bb"));
    cases.push(("multiple proof systems", batch));

    let mut batch = valid.clone();
    batch.inner.rollupIdsWithProofSystems.clear();
    cases.push(("no rollup assignments", batch));

    let mut batch = valid.clone();
    batch.inner.rollupIdsWithProofSystems[0].rollupId = U256::from(2);
    cases.push(("assignment for a different rollup", batch));

    let mut batch = valid.clone();
    batch.inner.rollupIdsWithProofSystems[0].rollupId = U256::ZERO;
    cases.push(("zero rollup id", batch));

    let mut batch = valid.clone();
    batch.inner.rollupIdsWithProofSystems[0].rollupId = U256::from(u64::MAX) + U256::from(1);
    cases.push(("rollup id above u64", batch));

    let mut batch = valid.clone();
    batch.inner.rollupIdsWithProofSystems.push(rollup_row(2));
    cases.push(("multiple rollup assignments", batch));

    for indices in [Vec::new(), vec![1], vec![0, 0], vec![0, 1]] {
        let mut batch = valid.clone();
        batch.inner.rollupIdsWithProofSystems[0].proofSystemIndex = indices;
        cases.push(("invalid proof-system indices", batch));
    }

    let mut batch = valid.clone();
    batch.inner.crossProofSystemInteractions = B256::repeat_byte(0x42);
    cases.push(("cross-proof-system interactions in single-PS mode", batch));

    let mut batch = recorded_batch();
    batch.inner.entries[0].destinationRollupId = U256::from(2);
    cases.push(("entry destination outside batch", batch));

    let mut batch = valid.clone();
    batch.inner.l1ToL2lookupCalls.push(lookup(2));
    cases.push(("lookup destination outside batch", batch));

    let mut batch = valid.clone();
    batch.inner.blockNumber = 1;
    cases.push(("bound block number", batch));

    let mut batch = valid;
    batch.inner.blobIndices.push(U256::ZERO);
    cases.push(("blob index", batch));

    for (name, batch) in &cases {
        assert_structure_rejected(name, batch);
    }
}
