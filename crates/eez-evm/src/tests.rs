use super::*;
use alloy_consensus::Typed2718;
use alloy_evm::{EvmFactory, FromRecoveredTx};
use alloy_primitives::{B256, TxKind, U256, address, bytes};
use eez_primitives::{
    EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_LIMIT, SYSTEM_TX_TYPE, SystemTransaction,
};
use reth_chainspec::ChainSpecBuilder;
use reth_evm::execute::Executor;
use revm::{
    DatabaseCommit,
    context::TxEnv,
    database::InMemoryDB,
    inspector::NoOpInspector,
    state::{AccountInfo, Bytecode},
};

fn config() -> EezEvmConfig {
    EezEvmConfig::new(Arc::new(
        ChainSpecBuilder::mainnet().cancun_activated().build(),
    ))
}

fn block(nonce: u64, chain_id: u64) -> reth_primitives_traits::RecoveredBlock<Block> {
    let tx: EezTxEnvelope = SystemTransaction {
        chain_id,
        nonce,
        to: EEZL2_ADDRESS,
        value: U256::from(13),
        input: Bytes::new(),
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
            assert!(receipt.cumulative_gas_used <= SYSTEM_TX_GAS_LIMIT);
            if out_of_gas {
                assert_eq!(receipt.cumulative_gas_used, SYSTEM_TX_GAS_LIMIT);
            }
            let mut state = executor.into_state();
            let bundle = state.take_bundle();
            let system = bundle
                .state
                .get(&SYSTEM_ADDRESS)
                .unwrap()
                .info
                .as_ref()
                .unwrap();
            assert_eq!(system.nonce, 1);
            assert_eq!(system.balance, balance);
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
fn native_execution_retains_nonce_chain_and_block_gas_validation() {
    for (nonce, chain, expected) in [
        (1, 1, "NonceTooHigh"),
        (0, 2, "InvalidChainId"),
        (u64::MAX, 1, "NonceOverflowInTransaction"),
    ] {
        let error = config()
            .executor(database(U256::ZERO, bytes!("00")))
            .execute_one(&block(nonce, chain))
            .unwrap_err();
        assert!(
            format!("{error:?}").contains(expected),
            "wrong validation error: {error:?}"
        );
    }
    let mut block = block(0, 1).into_block();
    block.header.gas_limit = SYSTEM_TX_GAS_LIMIT - 1;
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    let error = config()
        .executor(database(U256::ZERO, bytes!("00")))
        .execute_one(&block)
        .unwrap_err();
    assert!(format!("{error:?}").contains("TransactionGasLimitMoreThanAvailableBlockGas"));
}

#[test]
fn failed_validation_and_uncommitted_execution_do_not_leak_minted_value() {
    let block = block(0, 1);
    let env = config().evm_env(block.header()).unwrap();
    let mut evm = EezEvmFactory.create_evm(database(U256::from(5), bytes!("00")), env);
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
    assert!(
        evm.transact_raw(tx.clone()).is_err(),
        "committed nonce cannot replay"
    );
    let next = evm.transact_raw(TxEnv { nonce: 1, ..tx }).unwrap();
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.nonce, 2);
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.balance, U256::from(5));
    assert_eq!(next.state[&EEZL2_ADDRESS].info.balance, U256::from(26));
}

#[test]
fn mint_overflow_does_not_poison_the_next_execution() {
    let block = block(0, 1);
    let mut evm = EezEvmFactory.create_evm(
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
    assert_eq!(out.state[&SYSTEM_ADDRESS].info.nonce, 1);
}

#[test]
fn inspection_and_normal_execution_apply_identical_mint_and_rollback_rules() {
    for code in [bytes!("3360005500"), bytes!("3360005560006000fd")] {
        let block = block(0, 1);
        let env = config().evm_env(block.header()).unwrap();
        let db = database(U256::ZERO, code);
        let mut plain = EezEvmFactory.create_evm(db.clone(), env.clone());
        let mut inspected = EezEvmFactory.create_evm_with_inspector(db, env, NoOpInspector);
        let tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
        assert_eq!(
            plain.transact_raw(tx.clone()).unwrap(),
            inspected.transact_raw(tx).unwrap()
        );
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
    let mut evm = EezEvmFactory.create_evm(db, config().evm_env(block.header()).unwrap());
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
    let next = evm.transact_raw(TxEnv { nonce: 1, ..native }).unwrap();
    assert_eq!(next.state[&SYSTEM_ADDRESS].info.balance, U256::from(17));
    assert_eq!(next.state[&EEZL2_ADDRESS].info.balance, U256::from(26));
}
