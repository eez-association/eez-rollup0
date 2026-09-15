use super::*;

#[test]
fn prover_rejects_beacon_withdrawal_mints() {
    use alloy_consensus::{Header, proofs::calculate_withdrawals_root};
    use alloy_eips::eip4895::Withdrawal;
    use reth_primitives_traits::SealedBlock;

    let mut chain_config: alloy_genesis::ChainConfig = serde_json::from_str(include_str!(
        "../../../tests/fixtures/stateless-block-13/chain-config.json"
    ))
    .unwrap();
    chain_config.prague_time = None;
    chain_config.osaka_time = None;
    let chain_spec = Arc::new(reth_chainspec::ChainSpec::from_genesis(
        alloy_genesis::Genesis {
            config: chain_config.clone(),
            ..Default::default()
        },
    ));
    let parent = Header {
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        withdrawals_root: Some(calculate_withdrawals_root(&[])),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        ..Default::default()
    };
    let withdrawals = vec![Withdrawal {
        index: 0,
        validator_index: 0,
        address: Address::repeat_byte(0xab),
        amount: 7_000_000_000,
    }];
    let block = eez_primitives::Block::new(
        Header {
            parent_hash: parent.hash_slow(),
            number: 1,
            timestamp: 1,
            withdrawals_root: Some(calculate_withdrawals_root(&withdrawals)),
            ..parent.clone()
        },
        eez_primitives::BlockBody {
            withdrawals: Some(withdrawals.into()),
            ..Default::default()
        },
    );
    let witness = stateless_reth::ExecutionWitness {
        state: vec![Bytes::from_static(&[0x80])],
        headers: vec![alloy_rlp::encode(&parent).into()],
        ..Default::default()
    };
    let error = stateless_reth::stateless_validation_recovered(
        SealedBlock::seal_slow(block.clone()).try_recover().unwrap(),
        witness.clone(),
        chain_spec.clone(),
        eez_evm::EezEvmConfig::new(chain_spec),
    )
    .unwrap_err();
    assert!(
        format!("{error:?}").contains("L2 blocks cannot contain beacon withdrawals"),
        "unexpected stateless rejection: {error:?}"
    );
    let admitted = AdmittedBlock {
        declared_number: 1,
        claimed_hash: block.header.hash_slow(),
        claimed_parent_hash: block.header.parent_hash,
        rlp: alloy_rlp::encode(&block),
        witness,
    };
    let error = Validator::stateless_for_test(chain_config, TEST_SYSTEM_ADDRESS)
        .validate(&[admitted])
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("L2 blocks cannot contain beacon withdrawals"),
        "prover did not preserve the L2 withdrawal rejection: {error:?}"
    );
}
