use super::*;
use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip4844, TxLegacy, transaction::Recovered,
};
use alloy_eips::eip4895::Withdrawal;
use alloy_primitives::{B256, Signature, TxKind, U256, bytes};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_LIMIT, SystemTransaction};
use evm2::{
    Inspector, InspectorConfig, SpecId, Version,
    bytecode::Bytecode,
    ethereum::TxEnvelope,
    evm::{AccountInfo, InMemoryDB},
    interpreter::{Interpreter, Message, MessageResult},
};
use reth_chainspec::ChainSpecBuilder;
use reth_evm::{ConvertTx, ExecutableTxTuple, execute::Executor};

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

    assert_eq!(
        declared_tx_gas_limit_cap(&Version::new(SpecId::PRAGUE)),
        None
    );
    assert_eq!(
        declared_tx_gas_limit_cap(&Version::new(SpecId::OSAKA)),
        Some(MAX_TX_GAS_LIMIT_OSAKA)
    );
    // EIP-8037 bounds `tx.gas` by the total cap, not the execution cap.
    assert_eq!(
        declared_tx_gas_limit_cap(&Version::new(SpecId::AMSTERDAM)),
        Some(evm2::constants::MAX_TX_TOTAL_GAS_LIMIT_AMSTERDAM)
    );
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

fn contract(code: Bytes) -> AccountInfo {
    let code = Bytecode::new_raw(code);
    AccountInfo {
        code_hash: code.hash_slow(),
        code: Some(code),
        ..Default::default()
    }
}

fn funded(balance: u64) -> AccountInfo {
    AccountInfo::empty().with_balance(U256::from(balance))
}

fn database(balance: U256, code: Bytes) -> InMemoryDB {
    let mut db = InMemoryDB::default();
    // Zero means the account is absent, matching a genesis without an alloc entry.
    if !balance.is_zero() {
        db.insert_account_info(&SYSTEM_ADDRESS, AccountInfo::empty().with_balance(balance));
    }
    db.insert_account_info(&EEZL2_ADDRESS, contract(code));
    db
}

/// The native transaction of [`block`].
fn native(nonce: u64, chain_id: u64, value: u64) -> Recovered<EezEvmTx> {
    let tx = SystemTransaction {
        chain_id,
        nonce,
        to: EEZL2_ADDRESS,
        value: U256::from(value),
        input: Bytes::new(),
    };
    Recovered::new_unchecked(EezEvmTx::System(tx), SYSTEM_ADDRESS)
}

fn legacy(caller: Address, tx: TxLegacy) -> Recovered<EezEvmTx> {
    Recovered::new_unchecked(EezEvmTx::Ethereum(TxEnvelope::Legacy(tx)), caller)
}

fn evm(db: InMemoryDB) -> evm2::Evm<'static, EezEvmTypes> {
    let config = config();
    config.evm_with_env(db, config.evm_env(block(0, 1).header()).unwrap())
}

fn balance(evm: &mut evm2::Evm<'_, EezEvmTypes>, address: Address) -> U256 {
    evm.read_account_info(&address)
        .unwrap()
        .map(|account| account.balance)
        .unwrap_or_default()
}

#[test]
fn native_execution_retains_nonce_chain_and_block_gas_validation() {
    for (nonce, chain, expected) in [
        (1, 1, "InvalidNonce { expected: 0, got: 1 }"),
        (0, 2, "InvalidChainId"),
        (u64::MAX, 1, "NonceOverflow"),
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
    assert!(
        format!("{error:?}").contains("TransactionGasLimitMoreThanAvailableBlockGas"),
        "wrong validation error: {error:?}"
    );
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

    let payload: ExecutionData = eez_engine::EezBuiltPayload::new(
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
fn failed_validation_and_uncommitted_execution_do_not_leak_minted_value() {
    let mut evm = evm(database(U256::from(5), bytes!("00")));
    for (nonce, chain) in [(1, 1), (0, 2)] {
        assert!(evm.transact(&native(nonce, chain, 13)).is_err());
        assert_eq!(balance(&mut evm, SYSTEM_ADDRESS), U256::from(5));
    }
    let preview = evm.transact(&native(0, 1, 13)).unwrap().discard();
    let replay = evm.transact(&native(0, 1, 13)).unwrap().detach();
    assert_eq!(preview, replay.result, "preview must not modify the state");
    // The system account keeps its own ETH; only the minted value moves.
    let state = &replay.pending_state;
    assert_eq!(
        state.account_info(&SYSTEM_ADDRESS).unwrap().balance,
        U256::from(5)
    );
    assert_eq!(
        state.account_info(&EEZL2_ADDRESS).unwrap().balance,
        U256::from(13)
    );
    evm.overlay_db_mut().commit_pending(state);
    assert!(
        evm.transact(&native(0, 1, 13)).is_err(),
        "committed nonce cannot replay"
    );
    let next = evm.transact(&native(1, 1, 13)).unwrap().detach();
    let state = &next.pending_state;
    assert_eq!(state.account_info(&SYSTEM_ADDRESS).unwrap().nonce, 2);
    assert_eq!(
        state.account_info(&SYSTEM_ADDRESS).unwrap().balance,
        U256::from(5)
    );
    assert_eq!(
        state.account_info(&EEZL2_ADDRESS).unwrap().balance,
        U256::from(26)
    );
}

#[test]
fn mint_overflow_does_not_poison_the_next_execution() {
    let mut evm = evm(database(U256::MAX, bytes!("00")));
    let error = evm.transact(&native(0, 1, 13)).unwrap_err();
    assert!(
        format!("{error}").contains("deposit balance overflow"),
        "{error}"
    );
    let out = evm.transact(&native(0, 1, 0)).unwrap().detach();
    assert!(out.result.is_success());
    let system = out.pending_state.account_info(&SYSTEM_ADDRESS).unwrap();
    assert_eq!((system.balance, system.nonce), (U256::MAX, 1));
}

/// Counts call hooks.
#[derive(Debug, Default)]
struct CallCounter {
    calls: u64,
    call_ends: u64,
}

impl Inspector<EezEvmTypes> for CallCounter {
    fn inspector_config(&self) -> InspectorConfig {
        InspectorConfig::without_steps()
    }

    fn call(
        &mut self,
        _interp: &mut Interpreter<'_, '_, EezEvmTypes>,
        _message: &mut Message<EezEvmTypes>,
    ) -> Option<MessageResult<EezEvmTypes>> {
        self.calls += 1;
        None
    }

    fn call_end(
        &mut self,
        _interp: &mut Interpreter<'_, '_, EezEvmTypes>,
        _message: &Message<EezEvmTypes>,
        _result: &mut MessageResult<EezEvmTypes>,
    ) {
        self.call_ends += 1;
    }
}

#[test]
fn inspection_and_normal_execution_apply_identical_mint_and_rollback_rules() {
    let user = Address::repeat_byte(0x11);
    // Exercise each path: minting, zero-value native, and ordinary.
    let ordinary = legacy(
        user,
        TxLegacy {
            chain_id: Some(1),
            gas_price: 1,
            gas_limit: SYSTEM_TX_GAS_LIMIT,
            to: TxKind::Call(EEZL2_ADDRESS),
            value: U256::from(13),
            ..Default::default()
        },
    );
    for tx in [native(0, 1, 13), native(0, 1, 0), ordinary] {
        // Successful storage write, revert, and invalid-opcode halt.
        for code in [
            bytes!("3360005500"),
            bytes!("3360005560006000fd"),
            bytes!("fe"),
        ] {
            let mut db = database(U256::from(5), code);
            db.insert_account_info(&user, funded(3_000_000));
            let expected = evm(db.clone()).transact(&tx).unwrap().detach();
            let mut counter = CallCounter::default();
            let inspected = evm(db)
                .transact_with_inspector(&tx, &mut counter)
                .unwrap()
                .detach();
            assert_eq!(expected, inspected);
            // The composer's probe reads native deliveries through these hooks.
            assert_eq!((counter.calls, counter.call_ends), (1, 1));
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
        for native_tx in [true, false] {
            let mut db = database(U256::ZERO, outer.clone().into());
            db.insert_account_info(&target, contract(inner.clone().into()));
            db.insert_account_info(&user, funded(20_000_000));
            let mut evm = evm(db);
            assert_eq!(evm.block().basefee, U256::from(1));
            let price = if native_tx { 0 } else { 7 };
            let tx = if native_tx {
                native(0, 1, 13)
            } else {
                legacy(
                    user,
                    TxLegacy {
                        chain_id: Some(1),
                        gas_price: price,
                        gas_limit: SYSTEM_TX_GAS_LIMIT,
                        to: TxKind::Call(EEZL2_ADDRESS),
                        value: U256::from(13),
                        ..Default::default()
                    },
                )
            };
            let result = evm.transact(&tx).unwrap().discard();
            assert!(result.is_success());
            let words = result
                .output
                .as_chunks::<32>()
                .0
                .iter()
                .copied()
                .map(U256::from_be_bytes)
                .collect::<Vec<_>>();
            let price = u64::try_from(price).unwrap();
            assert_eq!(
                words,
                vec![
                    U256::from(price),
                    U256::from(price),
                    U256::from(21_000 * price),
                    U256::from(u64::from(!native_tx || !require_positive_price)),
                ],
                "native={native_tx}, require_positive_price={require_positive_price}"
            );
        }
    }
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
        db.insert_account_info(&signer, funded(1_000_000));

        // The same raw-EVM path is used for cross-chain L1 simulation.
        let mut evm = config.evm_with_env(db.clone(), config.evm_env(block.header()).unwrap());
        let tx = Recovered::new_unchecked(EezEvmTx::from(tx), signer);
        assert!(evm.transact(&tx).unwrap().discard().is_success());

        let result = config.executor(db).execute_one(&block);
        if is_blob {
            assert!(format!("{:?}", result.unwrap_err()).contains("Eip4844Disabled"));
        } else {
            assert!(result.unwrap().receipts[0].success);
        }

        let payload: ExecutionData =
            eez_engine::EezBuiltPayload::new(Arc::new(block), U256::ZERO, None, None).into();
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
