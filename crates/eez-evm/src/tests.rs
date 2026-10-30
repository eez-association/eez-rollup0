use super::*;
use alloy_consensus::{SignableTransaction, TxEip1559, TxEip4844};
use alloy_eips::eip4895::Withdrawal;
use alloy_evm::{EvmFactory, FromRecoveredTx};
use alloy_primitives::{B256, Signature, TxKind, U256, address, bytes};
use eez_primitives::{
    EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_CAP, SYSTEM_TX_TYPE, SystemTransaction,
};
use reth_chainspec::ChainSpecBuilder;
use reth_evm::{ConvertTx, ExecutableTxTuple, execute::Executor};
use revm::{
    Database as _, DatabaseCommit,
    context::TxEnv,
    context_interface::result::EVMError,
    database::InMemoryDB,
    inspector::CountInspector,
    state::{AccountInfo, Bytecode},
};

mod fees;
mod system_calls;

fn config() -> EezEvmConfig {
    EezEvmConfig::new(Arc::new(
        ChainSpecBuilder::mainnet().cancun_activated().build(),
    ))
    .unwrap()
}

#[test]
fn declared_cap_tracks_osaka_and_amsterdam_semantics() {
    use alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
    use revm::context::CfgEnv;
    use revm::primitives::hardfork::SpecId;

    let prague = CfgEnv::new_with_spec(SpecId::PRAGUE);
    assert_eq!(prague.tx_gas_limit_cap, None);
    assert_eq!(declared_tx_gas_limit_cap(&prague), None);

    let osaka = CfgEnv::new_with_spec(SpecId::OSAKA);
    assert_eq!(osaka.tx_gas_limit_cap, None);
    assert_eq!(
        declared_tx_gas_limit_cap(&osaka),
        Some(MAX_TX_GAS_LIMIT_OSAKA)
    );

    let amsterdam = CfgEnv::new_with_spec(SpecId::AMSTERDAM);
    assert!(amsterdam.enable_amsterdam_eip8037);
    assert_eq!(declared_tx_gas_limit_cap(&amsterdam), None);
}

fn block(_group: u64, chain_id: u64) -> reth_primitives_traits::RecoveredBlock<Block> {
    let tx: EezTxEnvelope = SystemTransaction {
        chain_id,
        source_hash: B256::repeat_byte(0x11),
        gas_limit: eez_primitives::SYSTEM_TX_GAS_CAP,
        to: EEZL2_ADDRESS,
        value: U256::from(13),
        input: Bytes::from_static(&eez_protocol::entries::EXECUTE_INCOMING_SELECTOR),
    }
    .into();
    let block = Block::new(
        Header {
            number: 1,
            timestamp: 1,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        },
        alloy_consensus::BlockBody {
            transactions: vec![tx],
            ..Default::default()
        },
    );
    SealedBlock::seal_slow(block).try_recover().unwrap()
}

fn database(balance: U256, code: Bytes) -> InMemoryDB {
    let mut db = InMemoryDB::default();
    // Zero means the account is absent, matching a genesis without an alloc entry.
    if !balance.is_zero() {
        db.insert_account_info(
            SYSTEM_ADDRESS,
            AccountInfo {
                balance,
                ..Default::default()
            },
        );
    }
    let code = Bytecode::new_raw(code);
    db.insert_account_info(
        EEZL2_ADDRESS,
        AccountInfo {
            code_hash: code.hash_slow(),
            code: Some(code),
            ..Default::default()
        },
    );
    db
}

#[test]
fn native_mints_without_prefunding_and_rolls_back_failed_calls_without_spending_existing_eth() {
    for balance in [U256::ZERO, U256::from(5), U256::from(1_000_000)] {
        for (code, success, out_of_gas) in [
            // Store CALLER; revert after the store; loop until out of gas.
            (bytes!("3360005500"), true, false),
            (bytes!("3360005560006000fd"), false, false),
            (bytes!("5b600056"), false, true),
        ] {
            let config = config();
            let mut executor = config.executor(database(balance, code));
            let result = executor.execute_one(&block(0, 1)).unwrap();
            let receipt = &result.receipts[0];
            assert_eq!(receipt.tx_type.ty(), SYSTEM_TX_TYPE);
            assert_eq!(receipt.success, success);
            assert!(receipt.cumulative_gas_used > 0);
            assert!(receipt.cumulative_gas_used <= SYSTEM_TX_GAS_CAP);
            if out_of_gas {
                assert_eq!(receipt.cumulative_gas_used, SYSTEM_TX_GAS_CAP);
            }
            let mut state = executor.into_state();
            let system = state.basic(SYSTEM_ADDRESS).unwrap();
            let bundle = state.take_bundle();
            assert_eq!(system.as_ref().map_or(0, |info| info.nonce), 0);
            assert_eq!(system.map_or(U256::ZERO, |info| info.balance), balance);
            let beneficiary_balance = bundle
                .state
                .get(&alloy_primitives::Address::ZERO)
                .and_then(|account| account.info.as_ref())
                .map(|info| info.balance)
                .unwrap_or_default();
            assert_eq!(beneficiary_balance, U256::ZERO);
            let target_balance = bundle
                .state
                .get(&EEZL2_ADDRESS)
                .and_then(|account| account.info.as_ref())
                .map(|info| info.balance)
                .unwrap_or_default();
            assert_eq!(
                target_balance,
                if success { U256::from(13) } else { U256::ZERO }
            );
            if success {
                let target = bundle.state.get(&EEZL2_ADDRESS).unwrap();
                assert_eq!(
                    target.storage.get(&U256::ZERO).unwrap().present_value,
                    U256::from_be_slice(SYSTEM_ADDRESS.as_slice())
                );
            }
        }
    }
}

#[test]
fn native_execution_retains_chain_and_exact_position_gas_validation() {
    let error = config()
        .executor(database(U256::ZERO, bytes!("00")))
        .execute_one(&block(0, 2))
        .unwrap_err();
    assert!(format!("{error:?}").contains("InvalidChainId"));
    let mut block = block(0, 1).into_block();
    block.header.gas_limit = SYSTEM_TX_GAS_CAP - 1;
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    let error = config()
        .executor(database(U256::ZERO, bytes!("00")))
        .execute_one(&block)
        .unwrap_err();
    assert!(format!("{error:?}").contains("native gas does not match execution position"));
}

#[test]
fn l2_rejects_beacon_withdrawals_during_construction_import_and_engine_replay() {
    let withdrawals = vec![Withdrawal {
        index: 0,
        validator_index: 0,
        address: Address::repeat_byte(0xab),
        amount: 7_000_000_000,
    }];
    let config = config();
    let parent = SealedHeader::seal_slow(Header::default());
    let error = config
        .context_for_next_block(
            &parent,
            NextBlockEnvAttributes {
                timestamp: 1,
                suggested_fee_recipient: Address::ZERO,
                prev_randao: B256::ZERO,
                gas_limit: 30_000_000,
                parent_beacon_block_root: Some(B256::ZERO),
                withdrawals: Some(withdrawals.clone().into()),
                extra_data: Bytes::new(),
                slot_number: None,
            },
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("L2 blocks cannot contain beacon withdrawals")
    );

    let mut block = block(0, 1).into_block();
    block.body.transactions.clear();
    block.body.withdrawals = Some(withdrawals.into());
    let sealed = SealedBlock::seal_slow(block);
    let error = config.context_for_block(&sealed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("L2 blocks cannot contain beacon withdrawals")
    );

    let payload: ExecutionData = eez_primitives::engine::EezBuiltPayload::new(
        Arc::new(sealed.try_recover().unwrap()),
        U256::ZERO,
        None,
        None,
    )
    .into();
    let error = config.context_for_payload(&payload).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("L2 blocks cannot contain beacon withdrawals")
    );
}

#[test]
fn native_execution_rejects_noncanonical_gas_fields_without_mutating_state() {
    let block = block(0, 1);
    let balance = U256::from(20_000_000);
    let mut evm = EezEvmFactory::default().create_evm(
        database(balance, bytes!("00")),
        config().evm_env(block.header()).unwrap(),
    );
    let tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    for value in [U256::ZERO, U256::from(13)] {
        for (gas_limit, gas_price) in [(SYSTEM_TX_GAS_CAP + 1, 0), (SYSTEM_TX_GAS_CAP, 7)] {
            let invalid = TxEnv {
                gas_limit,
                gas_price,
                value,
                ..tx.clone()
            };
            assert!(
                matches!(evm.transact_raw(invalid), Err(EVMError::Custom(_))),
                "accepted noncanonical native gas fields: limit={gas_limit}, price={gas_price}, value={value}"
            );
            let account = &evm.db().cache.accounts[&SYSTEM_ADDRESS].info;
            assert_eq!(account.balance, balance);
            assert_eq!(account.nonce, 0);
        }
    }
    let output = evm.transact_raw(tx).unwrap();
    assert!(output.result.is_success());
    assert_eq!(output.state[&SYSTEM_ADDRESS].info.balance, balance);
    assert_eq!(output.state[&SYSTEM_ADDRESS].info.nonce, 0);
    assert_eq!(output.state[&EEZL2_ADDRESS].info.balance, U256::from(13));
}

#[test]
fn failed_validation_and_uncommitted_execution_do_not_leak_minted_value() {
    let block = block(0, 1);
    let env = config().evm_env(block.header()).unwrap();
    let mut evm = EezEvmFactory::default().create_evm(database(U256::from(5), bytes!("00")), env);
    let tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    for (nonce, chain) in [(1, 1), (0, 2)] {
        let mut invalid = tx.clone();
        invalid.nonce = nonce;
        invalid.chain_id = Some(chain);
        assert!(evm.transact_raw(invalid).is_err());
        assert_eq!(
            evm.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
            U256::from(5)
        );
    }
    let preview = evm.transact_raw(tx.clone()).unwrap();
    let replay = evm.transact_raw(tx.clone()).unwrap();
    assert_eq!(
        preview, replay,
        "preview must not modify the underlying database"
    );
    assert_eq!(replay.state[&SYSTEM_ADDRESS].info.balance, U256::from(5));
    assert_eq!(replay.state[&EEZL2_ADDRESS].info.balance, U256::from(13));
    evm.db_mut().commit(replay.state);
    // Raw execution carries no settlement authority. Replaying this TxEnv is
    // intentionally possible; the signer and L1 settlement reject replayed sources.
    let next = evm.transact_raw(tx).unwrap();
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.nonce, 0);
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.balance, U256::from(5));
    assert_eq!(next.state[&EEZL2_ADDRESS].info.balance, U256::from(26));
}

#[test]
fn mint_overflow_does_not_poison_the_next_execution() {
    let block = block(0, 1);
    let mut evm = EezEvmFactory::default().create_evm(
        database(U256::MAX, bytes!("00")),
        config().evm_env(block.header()).unwrap(),
    );
    let tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    assert!(
        format!("{:?}", evm.transact_raw(tx.clone()).unwrap_err())
            .contains("deposit balance overflow")
    );
    let out = evm
        .transact_raw(TxEnv {
            value: U256::ZERO,
            ..tx
        })
        .unwrap();
    assert!(out.result.is_success());
    assert_eq!(out.state[&SYSTEM_ADDRESS].info.balance, U256::MAX);
    assert_eq!(out.state[&SYSTEM_ADDRESS].info.nonce, 0);
}

#[test]
fn inspection_and_normal_execution_apply_identical_mint_and_rollback_rules() {
    let user = Address::repeat_byte(0x11);
    // Exercise each delegation path: minting, zero-value native, and ordinary.
    for (tx_type, caller, value, gas_price) in [
        (SYSTEM_TX_TYPE, SYSTEM_ADDRESS, 13, 0),
        (SYSTEM_TX_TYPE, SYSTEM_ADDRESS, 0, 0),
        (0, user, 13, 1),
    ] {
        // Successful storage write, revert, and invalid-opcode halt.
        for code in [
            bytes!("3360005500"),
            bytes!("3360005560006000fd"),
            bytes!("fe"),
        ] {
            let block = block(0, 1);
            let env = config().evm_env(block.header()).unwrap();
            let mut db = database(U256::from(5), code);
            db.insert_account_info(
                user,
                AccountInfo {
                    balance: U256::from(30_000_000),
                    ..Default::default()
                },
            );
            let mut plain = EezEvmFactory::default().create_evm(db.clone(), env.clone());
            let mut inspected = EezEvmFactory::default().create_evm_with_inspector(
                db,
                env,
                CountInspector::default(),
            );
            let tx = TxEnv {
                tx_type,
                caller,
                value: U256::from(value),
                gas_price,
                ..TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS)
            };
            let expected = plain.transact_raw(tx.clone()).unwrap();
            // First verify that the factory enables the inspector itself, then
            // check that runtime toggles preserve execution while gating callbacks.
            for enabled in [None, Some(false), Some(true)] {
                if let Some(enabled) = enabled {
                    inspected.set_inspector_enabled(enabled);
                }
                inspected.components_mut().1.clear();
                assert_eq!(expected, inspected.transact_raw(tx.clone()).unwrap());
                let inspector = inspected.components().1;
                let enabled = enabled.unwrap_or(true);
                assert_eq!(inspector.call_count(), u64::from(enabled));
                assert_eq!(inspector.call_end_count(), u64::from(enabled));
                assert_eq!(inspector.step_count() > 0, enabled);
            }
        }
    }
}

#[test]
fn native_gasprice_is_zero_in_nested_calls_including_reimbursement_and_positive_price_guards() {
    let target = Address::repeat_byte(0x22);
    let user = Address::repeat_byte(0x11);
    // Return [outer GASPRICE, inner GASPRICE, inner 21_000 * GASPRICE, CALL success].
    // The stand-in EEZL2 contract calls a second contract through an ordinary CALL.
    let mut outer = bytes!("3a6000526040602060006000600073").to_vec();
    outer.extend_from_slice(target.as_slice());
    outer.extend_from_slice(&bytes!("5af160605260806000f3"));
    for require_positive_price in [false, true] {
        let mut inner = Vec::new();
        if require_positive_price {
            // require(tx.gasprice > 0), then continue at JUMPDEST 7.
            inner.extend_from_slice(&bytes!("3a6007575f5ffd5b"));
        }
        inner.extend_from_slice(&bytes!("3a6000523a6152080260205260406000f3"));
        for native in [true, false] {
            let mut db = database(U256::ZERO, outer.clone().into());
            let code = Bytecode::new_raw(inner.clone().into());
            db.insert_account_info(
                target,
                AccountInfo {
                    code_hash: code.hash_slow(),
                    code: Some(code),
                    ..Default::default()
                },
            );
            db.insert_account_info(
                user,
                AccountInfo {
                    balance: U256::from(200_000_000),
                    ..Default::default()
                },
            );
            let block = block(0, 1);
            let mut evm =
                EezEvmFactory::default().create_evm(db, config().evm_env(block.header()).unwrap());
            assert_eq!(evm.block().basefee, 1);
            let mut tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
            let price = if native { 0 } else { 7 };
            if !native {
                tx.tx_type = 0;
                tx.caller = user;
                tx.gas_price = price;
            }
            let result = evm.transact_raw(tx).unwrap();
            assert!(result.result.is_success());
            let output = result.result.output().unwrap();
            let words = output
                .as_chunks::<32>()
                .0
                .iter()
                .copied()
                .map(U256::from_be_bytes)
                .collect::<Vec<_>>();
            assert_eq!(
                words,
                vec![
                    U256::from(price),
                    U256::from(price),
                    U256::from(21_000 * price),
                    U256::from(u64::from(!native || !require_positive_price)),
                ],
                "native={native}, require_positive_price={require_positive_price}"
            );
        }
    }
}

#[test]
fn ordinary_transactions_still_pay_fees_and_outbound_eth_stays_at_the_system_address() {
    let user = address!("1111111111111111111111111111111111111111");
    let balance = U256::from(1_000_000);
    let block = block(0, 1);
    let mut db = database(U256::ZERO, bytes!("00"));
    db.insert_account_info(
        user,
        AccountInfo {
            balance,
            ..Default::default()
        },
    );
    let mut evm =
        EezEvmFactory::default().create_evm(db, config().evm_env(block.header()).unwrap());
    let native = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    let mint = evm.transact_raw(native.clone()).unwrap();
    evm.db_mut().commit(mint.state);
    let ordinary = TxEnv {
        caller: user,
        kind: TxKind::Call(SYSTEM_ADDRESS),
        value: U256::from(17),
        gas_limit: 21_000,
        gas_price: 7,
        chain_id: Some(1),
        ..Default::default()
    };
    assert!(
        evm.transact_raw(TxEnv {
            gas_price: 0,
            ..ordinary.clone()
        })
        .is_err()
    );
    let out = evm.transact_raw(ordinary).unwrap();
    assert!(out.result.is_success());
    assert_eq!(
        out.state[&user].info.balance,
        balance - U256::from(21_000 * 7 + 17)
    );
    assert_eq!(out.state[&SYSTEM_ADDRESS].info.balance, U256::from(17));
    evm.db_mut().commit(out.state);
    let next = evm.transact_raw(native).unwrap();
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.balance, U256::from(17));
    assert_eq!(next.state[&EEZL2_ADDRESS].info.balance, U256::from(26));
}

#[test]
fn l2_block_and_engine_replay_reject_blobs_but_l1_simulation_still_executes_them() {
    let signature = Signature::new(U256::ONE, U256::ONE, false);
    let ordinary = EezTxEnvelope::Ethereum(
        TxEip1559 {
            chain_id: 1,
            gas_limit: 21_000,
            max_fee_per_gas: 7,
            to: TxKind::Call(EEZL2_ADDRESS),
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    let blob = EezTxEnvelope::Ethereum(
        TxEip4844 {
            chain_id: 1,
            gas_limit: 21_000,
            max_fee_per_gas: 7,
            max_fee_per_blob_gas: 1,
            blob_versioned_hashes: vec![B256::repeat_byte(1)],
            to: EEZL2_ADDRESS,
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    let config = config();
    for tx in [block(0, 1).body().transactions[0].clone(), ordinary, blob] {
        let is_blob = tx.is_eip4844();
        let signer = tx.try_recover().unwrap();
        let mut block = block(0, 1).into_block();
        block.body.transactions = vec![tx.clone()];
        let block = SealedBlock::seal_slow(block).try_recover().unwrap();
        let mut db = database(U256::ZERO, bytes!("00"));
        db.insert_account_info(
            signer,
            AccountInfo {
                balance: U256::from(1_000_000),
                ..Default::default()
            },
        );

        // The same raw-EVM path is used for cross-chain L1 simulation.
        let mut evm = config.evm_with_env(db.clone(), config.evm_env(block.header()).unwrap());
        let result = evm.transact(tx.with_signer(signer)).unwrap();
        assert!(result.result.is_success());

        let result = config.executor(db).execute_one(&block);
        if is_blob {
            assert!(format!("{:?}", result.unwrap_err()).contains("Eip4844Disabled"));
        } else {
            assert!(result.unwrap().receipts[0].success);
        }

        let payload: ExecutionData =
            eez_primitives::engine::EezBuiltPayload::new(Arc::new(block), U256::ZERO, None, None)
                .into();
        let (raw, convert) = config
            .tx_iterator_for_payload(&payload)
            .unwrap()
            .into_parts();
        let result = convert.convert(raw.into_iter().next().unwrap());
        if is_blob {
            let error = result
                .err()
                .expect("Engine replay must reject blob transactions");
            assert!(format!("{error:?}").contains("Eip4844Disabled"));
        } else {
            assert!(result.is_ok());
        }
    }
}

fn native_tx(gas_limit: u64, input: Bytes) -> EezTxEnvelope {
    SystemTransaction {
        chain_id: 1,
        source_hash: B256::repeat_byte(42),
        gas_limit,
        to: EEZL2_ADDRESS,
        value: U256::from(13),
        input: [
            eez_protocol::entries::EXECUTE_INCOMING_SELECTOR.as_slice(),
            input.as_ref(),
        ]
        .concat()
        .into(),
    }
    .into()
}

const LOOP: Address = address!("3333333333333333333333333333333333333333");

/// An EIP-1559 call burning `gas_limit` in `LOOP`, from a recoverable dummy
/// signature. Returns the transaction and its recovered sender.
fn looping_user_tx(gas_limit: u64) -> (EezTxEnvelope, Address) {
    let tx = EezTxEnvelope::Ethereum(
        TxEip1559 {
            chain_id: 1,
            gas_limit,
            max_fee_per_gas: 1,
            to: TxKind::Call(LOOP),
            ..Default::default()
        }
        .into_signed(Signature::new(U256::ONE, U256::ONE, false))
        .into(),
    );
    let sender = alloy_consensus::transaction::SignerRecoverable::recover_signer(&tx).unwrap();
    (tx, sender)
}

fn looping_database(user: Address) -> InMemoryDB {
    let mut db = database(U256::ZERO, bytes!("5b600056"));
    let code = Bytecode::new_raw(bytes!("5b600056"));
    db.insert_account_info(
        LOOP,
        AccountInfo {
            code_hash: code.hash_slow(),
            code: Some(code),
            ..Default::default()
        },
    );
    db.insert_account_info(
        user,
        AccountInfo {
            balance: U256::from(u64::MAX),
            ..Default::default()
        },
    );
    db
}

#[test]
fn native_budget_is_the_remaining_block_gas_after_ordinary_transactions() {
    // 16M is spent first. Ethereum's declared-limit rule would refuse any native
    // transaction declaring the 16,777,216 cap here; the native budget is the
    // 14M remainder, which an out-of-gas call burns exactly.
    let (user_tx, user) = looping_user_tx(16_000_000);
    let mut body = block(0, 1).into_block();
    body.body.transactions = vec![user_tx, native_tx(14_000_000, Bytes::new())];
    let recovered = SealedBlock::seal_slow(body).try_recover().unwrap();
    let run = || {
        config()
            .executor(looping_database(user))
            .execute_one(&recovered)
            .unwrap()
    };
    let output = run();
    let cumulative: Vec<_> = output
        .receipts
        .iter()
        .map(|r| r.cumulative_gas_used)
        .collect();
    assert_eq!(cumulative, [16_000_000, 30_000_000]);
    assert_eq!(output.gas_used, 30_000_000);
    // The budget derives only from block contents, so independent replay agrees.
    let replay = run();
    assert_eq!(replay.receipts, output.receipts);
    assert_eq!(replay.gas_used, output.gas_used);
}

#[test]
fn native_raw_environment_rejects_non_wire_fields_and_forged_recovery() {
    let block = block(0, 1);
    let valid = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    let mut invalid = Vec::new();
    let mut tx = valid.clone();
    tx.chain_id = None;
    invalid.push(tx);
    let mut tx = valid.clone();
    tx.gas_priority_fee = Some(0);
    invalid.push(tx);
    let mut tx = valid.clone();
    tx.max_fee_per_blob_gas = 1;
    invalid.push(tx);
    let mut tx = valid.clone();
    tx.blob_hashes.push(B256::repeat_byte(1));
    invalid.push(tx);
    let mut tx = valid.clone();
    tx.access_list.0.push(alloy_eips::eip2930::AccessListItem {
        address: EEZL2_ADDRESS,
        storage_keys: Vec::new(),
    });
    invalid.push(tx);
    // Native conversion always supplies the implicit sender. Direct raw callers
    // still cannot override it with a forged Ethereum transaction environment.
    assert_eq!(
        TxEnv::from_recovered_tx(&block.body().transactions[0], Address::ZERO).caller,
        SYSTEM_ADDRESS
    );
    let mut tx = valid.clone();
    tx.caller = Address::ZERO;
    invalid.push(tx);
    let mut tx = valid.clone();
    tx.tx_type = 0;
    invalid.push(tx);
    for (index, tx) in invalid.into_iter().enumerate() {
        let balance = U256::from(7);
        let mut evm = EezEvmFactory::default().create_evm(
            database(balance, bytes!("00")),
            config().evm_env(block.header()).unwrap(),
        );
        assert!(
            matches!(evm.transact_raw(tx), Err(EVMError::Custom(_))),
            "accepted invalid native environment {index}"
        );
        let result = evm.transact_raw(valid.clone()).unwrap();
        assert!(result.result.is_success());
        assert_eq!(result.state[&SYSTEM_ADDRESS].info.balance, balance);
        assert_eq!(result.state[&SYSTEM_ADDRESS].info.nonce, 0);
        assert_eq!(result.state[&EEZL2_ADDRESS].info.balance, U256::from(13));
    }
}

#[test]
fn outbound_group_preserves_inventory_and_requires_paired_user() {
    use alloy_sol_types::SolCall as _;
    for (code, user_succeeds) in [
        (bytes!("4760005500"), true),
        // The native selector succeeds; the empty-data ordinary call reverts.
        (bytes!("476000553615600a57005b5f5ffd"), false),
    ] {
        use alloy_consensus::transaction::SignerRecoverable;
        let user: EezTxEnvelope = TxEip1559 {
            chain_id: 1,
            gas_limit: 100_000,
            max_fee_per_gas: 1,
            to: TxKind::Call(EEZL2_ADDRESS),
            ..Default::default()
        }
        .into_signed(Signature::new(U256::ONE, U256::ONE, false))
        .into();
        let sender = user.recover_signer().unwrap();
        // Store SELFBALANCE at slot 0. Both calls see and retain the inventory.
        let mut db = database(U256::from(5), code);
        db.cache
            .accounts
            .get_mut(&SYSTEM_ADDRESS)
            .unwrap()
            .info
            .nonce = 7;
        db.cache
            .accounts
            .get_mut(&EEZL2_ADDRESS)
            .unwrap()
            .info
            .balance = U256::from(19);
        db.insert_account_info(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000),
                ..Default::default()
            },
        );
        let mut body = block(0, 1).into_block();
        body.body.transactions = vec![
            SystemTransaction {
                chain_id: 1,
                source_hash: B256::repeat_byte(3),
                gas_limit: SYSTEM_TX_GAS_CAP,
                to: EEZL2_ADDRESS,
                value: U256::ZERO,
                input: Bytes::from_static(&eez_protocol::abi::loadExecutionTableCall::SELECTOR),
            }
            .into(),
            user,
        ];
        let recovered = SealedBlock::seal_slow(body.clone()).try_recover().unwrap();
        let evm_config = config();
        let mut executor = evm_config.executor(db.clone());
        let result = executor.execute_one(&recovered).unwrap();
        assert!(result.receipts[0].success);
        assert_eq!(result.receipts[1].success, user_succeeds);
        let mut state = executor.into_state();
        let system = state.basic(SYSTEM_ADDRESS).unwrap().unwrap();
        let bundle = state.take_bundle();
        let target = &bundle.state[&EEZL2_ADDRESS];
        assert_eq!(
            target
                .storage
                .get(&U256::ZERO)
                .map(|slot| slot.present_value),
            Some(U256::from(19)),
            "user_succeeds={user_succeeds}: {target:?}"
        );
        assert_eq!(target.info.as_ref().unwrap().balance, U256::from(19));
        assert_eq!(system.balance, U256::from(5));
        assert_eq!(system.nonce, 7);
        // A load is not an independently publishable group.
        body.body.transactions.pop();
        let error = config()
            .executor(db)
            .execute_one(&SealedBlock::seal_slow(body).try_recover().unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("block ends inside an outbound"));
    }
}
