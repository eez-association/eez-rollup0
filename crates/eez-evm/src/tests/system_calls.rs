use super::*;
use alloy_consensus::{BlockBody, TxLegacy};
use alloy_eips::{
    eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE},
    eip4788::{BEACON_ROOTS_ADDRESS, BEACON_ROOTS_CODE},
    eip6110::MAINNET_DEPOSIT_CONTRACT_ADDRESS,
    eip7002::{WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS, WITHDRAWAL_REQUEST_PREDEPLOY_CODE},
    eip7251::{CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS, CONSOLIDATION_REQUEST_PREDEPLOY_CODE},
};
use alloy_evm::block::{BlockExecutor, BlockExecutorFactory};
use revm::{Database, database::State, database::states::bundle_state::BundleRetention};

fn system_config() -> EezEvmConfig {
    EezEvmConfig::new(Arc::new(
        ChainSpecBuilder::mainnet().osaka_activated().build(),
    ))
    .unwrap()
}

#[test]
fn empty_block_only_updates_history() {
    let config = system_config();
    let mut db = InMemoryDB::default();
    for (address, code) in [
        (HISTORY_STORAGE_ADDRESS, &HISTORY_STORAGE_CODE),
        (BEACON_ROOTS_ADDRESS, &BEACON_ROOTS_CODE),
        (
            WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS,
            &WITHDRAWAL_REQUEST_PREDEPLOY_CODE,
        ),
        (
            CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS,
            &CONSOLIDATION_REQUEST_PREDEPLOY_CODE,
        ),
    ] {
        let code = Bytecode::new_raw(code.clone());
        db.insert_account_info(
            address,
            AccountInfo {
                nonce: 1,
                code_hash: code.hash_slow(),
                code: Some(code),
                ..Default::default()
            },
        );
    }
    // Canonical EIP-7002 and EIP-7251 calls decrement excess even with empty
    // queues. A test with absent contracts or zero excess would miss this.
    for address in [
        WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS,
        CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS,
    ] {
        db.insert_account_storage(address, U256::ZERO, U256::from(10))
            .unwrap();
    }
    // At timestamp 1, EIP-4788 would overwrite both of these slots, even when
    // parentBeaconBlockRoot is zero.
    db.insert_account_storage(BEACON_ROOTS_ADDRESS, U256::from(1), U256::from(55))
        .unwrap();
    db.insert_account_storage(BEACON_ROOTS_ADDRESS, U256::from(8192), U256::from(66))
        .unwrap();
    let parent_hash = B256::repeat_byte(0x42);
    let ethereum_block = alloy_consensus::Block::new(
        Header {
            parent_hash,
            ..block(0, 1).into_block().header
        },
        BlockBody::default(),
    );
    let block =
        SealedBlock::seal_slow(ethereum_block.clone().map_transactions(EezTxEnvelope::from));
    let mut state = State::builder()
        .with_database(db.clone())
        .with_bundle_update()
        .build();
    let evm = config.evm_with_env(&mut state, config.evm_env(block.header()).unwrap());
    let mut executor = config
        .block_executor_factory()
        .create_executor(evm, config.context_for_block(&block).unwrap());
    executor.apply_pre_execution_changes().unwrap();
    // The history update must be visible before transactions/checkpoints.
    assert_eq!(
        executor
            .evm_mut()
            .db_mut()
            .storage(HISTORY_STORAGE_ADDRESS, U256::ZERO)
            .unwrap(),
        U256::from_be_bytes(parent_hash.0),
    );
    assert!(
        executor
            .apply_post_execution_changes()
            .unwrap()
            .requests
            .is_empty()
    );
    state.merge_transitions(BundleRetention::Reverts);
    // No storage, balance, nonce, or code changes outside the history contract.
    assert_eq!(state.bundle_state.state.len(), 1);
    assert!(
        state
            .bundle_state
            .state
            .contains_key(&HISTORY_STORAGE_ADDRESS)
    );

    // Prove the same fixture actually triggers Ethereum's system calls.
    let ethereum = EthEvmConfig::new(config.chain_spec().clone());
    let mut executor = ethereum.executor(db);
    let block = SealedBlock::seal_slow(ethereum_block)
        .try_recover()
        .unwrap();
    assert!(executor.execute_one(&block).unwrap().requests.is_empty());
    let mut state = executor.into_state();
    for (address, slot, expected) in [
        (WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS, 0, 8),
        (CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS, 0, 9),
        (BEACON_ROOTS_ADDRESS, 1, 1),
        (BEACON_ROOTS_ADDRESS, 8192, 0),
    ] {
        assert_eq!(
            state.storage(address, U256::from(slot)).unwrap(),
            U256::from(expected),
            "Ethereum control, contract={address}"
        );
    }
}

#[test]
fn deposit_logs_remain_receipts_without_l2_consensus_requests() {
    // Emit a real ABI-encoded DepositEvent from the Ethereum deposit address.
    // Keeping the log in the receipt must not turn it into an L2 request.
    let mut data = Vec::new();
    let mut fields = Vec::new();
    let mut expected_request = vec![0]; // EIP-6110 request type.
    for (index, len) in [48, 32, 8, 96, 8].into_iter().enumerate() {
        data.extend_from_slice(&U256::from(160 + fields.len()).to_be_bytes::<32>());
        fields.extend_from_slice(&U256::from(len).to_be_bytes::<32>());
        let bytes = vec![index as u8 + 1; len];
        expected_request.extend_from_slice(&bytes);
        fields.extend_from_slice(&bytes);
        fields.resize(fields.len().next_multiple_of(32), 0);
    }
    data.extend_from_slice(&fields);
    let topic = alloy_primitives::keccak256("DepositEvent(bytes,bytes,bytes,bytes,bytes)");
    let mut code = vec![0x36, 0x5f, 0x5f, 0x37, 0x7f]; // Copy calldata; PUSH32 topic.
    code.extend_from_slice(topic.as_slice());
    code.extend_from_slice(&[0x36, 0x5f, 0xa1, 0x00]); // LOG1(calldata); STOP.
    let code = Bytecode::new_raw(code.into());
    let signed = TxLegacy {
        chain_id: Some(1),
        gas_limit: 200_000,
        gas_price: 2,
        to: TxKind::Call(MAINNET_DEPOSIT_CONTRACT_ADDRESS),
        input: data.clone().into(),
        ..Default::default()
    }
    .into_signed(Signature::test_signature());
    let sender = signed.recover_signer().unwrap();
    let mut db = InMemoryDB::default();
    db.insert_account_info(
        MAINNET_DEPOSIT_CONTRACT_ADDRESS,
        AccountInfo {
            nonce: 1,
            code_hash: code.hash_slow(),
            code: Some(code),
            ..Default::default()
        },
    );
    db.insert_account_info(
        sender,
        AccountInfo {
            balance: U256::from(1_000_000),
            ..Default::default()
        },
    );
    let ethereum_block = alloy_consensus::Block::new(
        block(0, 1).into_block().header,
        BlockBody {
            transactions: vec![signed.into()],
            ..Default::default()
        },
    );
    let block =
        SealedBlock::seal_slow(ethereum_block.clone().map_transactions(EezTxEnvelope::from))
            .try_recover()
            .unwrap();
    let config = system_config();
    let l2 = config.executor(db.clone()).execute_one(&block).unwrap();
    let l1 = EthEvmConfig::new(config.chain_spec().clone())
        .executor(db)
        .execute_one(
            &SealedBlock::seal_slow(ethereum_block)
                .try_recover()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(l2.receipts.len(), 1);
    assert!(l2.receipts[0].success);
    let logs = &l2.receipts[0].logs;
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].address, MAINNET_DEPOSIT_CONTRACT_ADDRESS);
    assert_eq!(logs[0].topics(), &[topic]);
    assert_eq!(logs[0].data.data.as_ref(), data.as_slice());
    assert!(l2.requests.is_empty());
    // Ensure the event is valid and would generate an Ethereum request.
    assert_eq!(l1.requests.take(), vec![Bytes::from(expected_request)]);
}

#[test]
fn l2_rejects_nonzero_beacon_root() {
    let mut block = block(0, 1).into_block();
    block.body.transactions.clear();
    block.header.parent_beacon_block_root = Some(B256::repeat_byte(0x42));
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    let error = system_config()
        .executor(InMemoryDB::default())
        .execute_one(&block)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("L2 parent beacon block root must be zero")
    );
}
