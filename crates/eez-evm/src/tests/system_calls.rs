use super::*;
use alloy_consensus::{BlockBody, TxLegacy};
use alloy_eips::{
    eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE},
    eip4788::{BEACON_ROOTS_ADDRESS, BEACON_ROOTS_CODE},
    eip6110::MAINNET_DEPOSIT_CONTRACT_ADDRESS,
    eip7002::{WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS, WITHDRAWAL_REQUEST_PREDEPLOY_CODE},
    eip7251::{CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS, CONSOLIDATION_REQUEST_PREDEPLOY_CODE},
    eip7685::EMPTY_REQUESTS_HASH,
};
use alloy_evm::block::{BlockExecutor, BlockExecutorFactory};
use revm::{Database, database::State, database::states::bundle_state::BundleRetention};

fn system_config(l1: bool) -> EezEvmConfig {
    let spec = Arc::new(ChainSpecBuilder::mainnet().osaka_activated().build());
    if l1 {
        EezEvmConfig::new_l1(spec).unwrap()
    } else {
        EezEvmConfig::new(spec).unwrap()
    }
}

fn system_database() -> InMemoryDB {
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
    db
}

#[test]
fn l2_only_runs_history_update_in_import_build_and_engine_paths() {
    for l1 in [false, true] {
        let config = system_config(l1);
        let parent = SealedHeader::seal_slow(Header {
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            ..Default::default()
        });
        let block = SealedBlock::seal_slow(Block {
            header: Header {
                parent_hash: parent.hash(),
                number: 1,
                timestamp: 1,
                gas_limit: 30_000_000,
                base_fee_per_gas: Some(1),
                excess_blob_gas: Some(0),
                blob_gas_used: Some(0),
                parent_beacon_block_root: Some(B256::ZERO),
                requests_hash: Some(EMPTY_REQUESTS_HASH),
                ..Default::default()
            },
            body: BlockBody {
                withdrawals: Some(Default::default()),
                ..Default::default()
            },
        });
        let payload: ExecutionData = eez_primitives::engine::EezBuiltPayload::new(
            Arc::new(block.clone().try_recover().unwrap()),
            U256::ZERO,
            None,
            None,
        )
        .into();
        for path in ["import", "build", "engine"] {
            let mut state = State::builder()
                .with_database(system_database())
                .with_bundle_update()
                .build();
            let ctx = match path {
                "import" => config.context_for_block(&block).unwrap(),
                "build" => config
                    .context_for_next_block(
                        &parent,
                        NextBlockEnvAttributes {
                            timestamp: 1,
                            suggested_fee_recipient: Address::ZERO,
                            prev_randao: B256::ZERO,
                            gas_limit: 30_000_000,
                            parent_beacon_block_root: Some(B256::ZERO),
                            withdrawals: Some(Default::default()),
                            extra_data: Bytes::new(),
                            slot_number: None,
                        },
                    )
                    .unwrap(),
                "engine" => config.context_for_payload(&payload).unwrap(),
                _ => unreachable!(),
            };
            let evm = config.evm_with_env(&mut state, config.evm_env(block.header()).unwrap());
            let mut executor = config.block_executor_factory().create_executor(evm, ctx);
            executor.apply_pre_execution_changes().unwrap();
            // EIP-2935 must be visible before any transaction/checkpoint.
            assert_eq!(
                executor
                    .evm_mut()
                    .db_mut()
                    .storage(HISTORY_STORAGE_ADDRESS, U256::ZERO)
                    .unwrap(),
                U256::from_be_bytes(parent.hash().0),
                "{path}, l1={l1}"
            );
            let result = executor.apply_post_execution_changes().unwrap();
            assert!(result.requests.is_empty());
            assert_eq!(result.requests.requests_hash(), EMPTY_REQUESTS_HASH);
            assert_eq!(result.gas_used, 0);
            for (address, slot, expected) in [
                (
                    WITHDRAWAL_REQUEST_PREDEPLOY_ADDRESS,
                    0,
                    if l1 { 8 } else { 10 },
                ),
                (
                    CONSOLIDATION_REQUEST_PREDEPLOY_ADDRESS,
                    0,
                    if l1 { 9 } else { 10 },
                ),
                (BEACON_ROOTS_ADDRESS, 1, if l1 { 1 } else { 55 }),
                (BEACON_ROOTS_ADDRESS, 8192, if l1 { 0 } else { 66 }),
            ] {
                assert_eq!(
                    state.storage(address, U256::from(slot)).unwrap(),
                    U256::from(expected),
                    "{path}, l1={l1}, contract={address}"
                );
            }
            state.merge_transitions(BundleRetention::Reverts);
            if !l1 {
                // Also catch unexpected nonce, balance, or other storage writes.
                assert_eq!(state.bundle_state.state.len(), 1, "{path}");
                assert!(
                    state
                        .bundle_state
                        .state
                        .contains_key(&HISTORY_STORAGE_ADDRESS)
                );
            }
        }
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
    let mut db = system_database();
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
    let mut block = block(0, 1).into_block();
    block.body.transactions = vec![signed.into()];
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    for l1 in [false, true] {
        let result = system_config(l1)
            .executor(db.clone())
            .execute_one(&block)
            .unwrap();
        assert_eq!(result.receipts.len(), 1);
        assert!(result.receipts[0].success);
        let logs = &result.receipts[0].logs;
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].address, MAINNET_DEPOSIT_CONTRACT_ADDRESS);
        assert_eq!(logs[0].topics(), &[topic]);
        assert_eq!(logs[0].data.data.as_ref(), data.as_slice());
        if l1 {
            assert_eq!(
                result.requests.take(),
                vec![Bytes::from(expected_request.clone())]
            );
        } else {
            assert!(result.requests.is_empty());
            assert_eq!(result.requests.requests_hash(), EMPTY_REQUESTS_HASH);
        }
    }
}

#[test]
fn l2_rejects_nonzero_beacon_root_while_l1_preserves_it() {
    let beacon_root = B256::repeat_byte(0x42);
    let mut block = block(0, 1).into_block();
    block.body.transactions.clear();
    block.header.parent_beacon_block_root = Some(beacon_root);
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    let error = system_config(false)
        .executor(system_database())
        .execute_one(&block)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("L2 parent beacon block root must be zero")
    );
    let config = system_config(true);
    let mut executor = config.executor(system_database());
    executor.execute_one(&block).unwrap();
    assert_eq!(
        executor
            .into_state()
            .storage(BEACON_ROOTS_ADDRESS, U256::from(8192))
            .unwrap(),
        U256::from_be_bytes(beacon_root.0)
    );
}
