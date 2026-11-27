use super::*;
use alloy_consensus::{
    SignableTransaction, TxEip1559, TxEip4844, TxLegacy, transaction::Recovered,
};
use alloy_eips::eip4895::Withdrawal;
use alloy_primitives::{B256, Signature, TxKind, U256, address, bytes};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_CAP, SystemTransaction};
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
fn l2_configuration_rejects_future_amsterdam_without_restricting_l1() {
    let mut spec = ChainSpecBuilder::mainnet().cancun_activated().build();
    spec.genesis.config.amsterdam_time = Some(u64::MAX);
    let spec = Arc::new(spec);
    assert!(EezEvmConfig::new(Arc::clone(&spec)).is_ok());
    assert!(EezEvmConfig::new_l2(spec).is_err());
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

fn block(_group: u64, chain_id: u64) -> reth_primitives_traits::RecoveredBlock<Block> {
    let tx: EezTxEnvelope = SystemTransaction {
        chain_id,
        source_hash: B256::repeat_byte(42),
        gas_limit: SYSTEM_TX_GAS_CAP,
        to: EEZL2_ADDRESS,
        value: U256::from(13),
        input: Bytes::from_static(&eez_primitives::EXECUTE_INCOMING_SELECTOR),
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
fn native(group: u64, chain_id: u64, value: u64) -> Recovered<EezEvmTx> {
    let tx = SystemTransaction {
        chain_id,
        source_hash: B256::from(U256::from(group)),
        gas_limit: SYSTEM_TX_GAS_CAP,
        to: EEZL2_ADDRESS,
        value: U256::from(value),
        input: Bytes::from_static(&eez_primitives::EXECUTE_INCOMING_SELECTOR),
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
fn native_execution_retains_chain_and_exact_position_gas_validation() {
    let error = config()
        .executor(database(U256::ZERO, bytes!("00")))
        .execute_one(&block(0, 2))
        .unwrap_err();
    assert!(format!("{error:?}").contains("InvalidChainId"), "{error:?}");
    let mut block = block(0, 1).into_block();
    block.header.gas_limit = SYSTEM_TX_GAS_CAP - 1;
    let error = config()
        .executor(database(U256::ZERO, bytes!("00")))
        .execute_one(&SealedBlock::seal_slow(block).try_recover().unwrap())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("native gas does not match execution position"),
        "{error:?}"
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
    for chain in [2, 3] {
        assert!(evm.transact(&native(0, chain, 13)).is_err());
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
    // Source identity is authenticated by derivation, not an EVM nonce guard.
    assert!(
        evm.transact(&native(0, 1, 13))
            .unwrap()
            .discard()
            .is_success()
    );
    let next = evm.transact(&native(1, 1, 13)).unwrap().detach();
    let state = &next.pending_state;
    assert_eq!(state.account_info(&SYSTEM_ADDRESS).unwrap().nonce, 0);
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
    assert_eq!((system.balance, system.nonce), (U256::MAX, 0));
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
            gas_limit: 2_000_000,
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
                        gas_limit: 2_000_000,
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

fn native_tx(gas_limit: u64, input: Bytes) -> EezTxEnvelope {
    SystemTransaction {
        chain_id: 1,
        source_hash: B256::repeat_byte(42),
        gas_limit,
        to: EEZL2_ADDRESS,
        value: U256::from(13),
        input: [
            eez_primitives::EXECUTE_INCOMING_SELECTOR.as_slice(),
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
        &LOOP,
        AccountInfo {
            code_hash: code.hash_slow(),
            code: Some(code),
            ..Default::default()
        },
    );
    db.insert_account_info(
        &user,
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
fn outbound_group_preserves_inventory_and_requires_paired_user() {
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
        let mut db = database(U256::from(5), code.clone());
        db.insert_account_info(&SYSTEM_ADDRESS, funded(5).with_nonce(7));
        db.insert_account_info(&EEZL2_ADDRESS, contract(code).with_balance(U256::from(19)));
        db.insert_account_info(
            &sender,
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
                input: Bytes::from_static(&eez_primitives::LOAD_EXECUTION_TABLE_SELECTOR),
            }
            .into(),
            user,
        ];
        let recovered = SealedBlock::seal_slow(body.clone()).try_recover().unwrap();
        let evm_config = config();
        let output = evm_config.executor(db.clone()).execute(&recovered).unwrap();
        let result = output.result;
        assert!(result.receipts[0].success);
        assert_eq!(result.receipts[1].success, user_succeeds);
        let bundle = output.state;
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
        // EVM2 omits unchanged accounts from the bundle. If present, the
        // native sender must retain its pre-existing balance and nonce.
        if let Some(system) = bundle.state.get(&SYSTEM_ADDRESS) {
            let system = system.info.as_ref().expect("system account was deleted");
            assert_eq!(system.balance, U256::from(5));
            assert_eq!(system.nonce, 7);
        }
        // A load is not an independently publishable group.
        body.body.transactions.pop();
        let error = config()
            .executor(db)
            .execute_one(&SealedBlock::seal_slow(body).try_recover().unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            reth_evm::BlockExecutionError::Validation(_)
        ));
        assert!(error.to_string().contains("block ends inside an outbound"));
    }
}

#[test]
fn native_handler_rejects_forged_sender_target_and_gas_without_leaking_state() {
    let valid = native(0, 1, 13);
    let EezEvmTx::System(base) = valid.inner() else {
        unreachable!()
    };
    let mut wrong_target = base.clone();
    wrong_target.to = Address::ZERO;
    let mut oversized = base.clone();
    oversized.gas_limit = SYSTEM_TX_GAS_CAP + 1;
    let mut undersized = base.clone();
    undersized.gas_limit = 21_000;
    for tx in [
        Recovered::new_unchecked(valid.clone().into_inner(), Address::ZERO),
        Recovered::new_unchecked(EezEvmTx::System(wrong_target), SYSTEM_ADDRESS),
        Recovered::new_unchecked(EezEvmTx::System(oversized), SYSTEM_ADDRESS),
        Recovered::new_unchecked(EezEvmTx::System(undersized), SYSTEM_ADDRESS),
        legacy(
            SYSTEM_ADDRESS,
            TxLegacy {
                chain_id: Some(1),
                gas_limit: 21_000,
                gas_price: 0,
                to: TxKind::Call(EEZL2_ADDRESS),
                ..Default::default()
            },
        ),
    ] {
        let config = config();
        let mut env = config.evm_env(block(0, 1).header()).unwrap();
        // The ordinary forgery must reach the reserved-sender check.
        env.version
            .features
            .remove(evm2::EvmFeatures::BASE_FEE_CHECK);
        let mut evm = config.evm_with_env(database(U256::from(7), bytes!("00")), env);
        assert!(evm.transact(&tx).is_err());
        assert_eq!(balance(&mut evm, SYSTEM_ADDRESS), U256::from(7));
        let result = evm.transact(&valid).unwrap().detach();
        assert!(result.result.is_success());
        assert_eq!(
            result
                .pending_state
                .account_info(&SYSTEM_ADDRESS)
                .unwrap()
                .nonce,
            0
        );
        assert_eq!(
            result
                .pending_state
                .account_info(&SYSTEM_ADDRESS)
                .unwrap()
                .balance,
            U256::from(7)
        );
        assert_eq!(
            result
                .pending_state
                .account_info(&EEZL2_ADDRESS)
                .unwrap()
                .balance,
            U256::from(13)
        );
    }
}

#[test]
fn native_nonce_is_unchanged_during_execution_and_mint_rolls_back() {
    #[derive(Debug)]
    struct CheckNonce;
    impl Inspector<EezEvmTypes> for CheckNonce {
        fn inspector_config(&self) -> InspectorConfig {
            InspectorConfig::without_steps()
        }
        fn call(
            &mut self,
            interp: &mut Interpreter<'_, '_, EezEvmTypes>,
            _: &mut Message<EezEvmTypes>,
        ) -> Option<MessageResult<EezEvmTypes>> {
            assert_eq!(
                interp
                    .host()
                    .state_mut()
                    .account(&SYSTEM_ADDRESS)
                    .unwrap()
                    .nonce(),
                7
            );
            None
        }
    }
    for (code, success) in [
        (bytes!("00"), true),
        (bytes!("5f5ffd"), false),
        (bytes!("fe"), false),
    ] {
        let mut db = database(U256::from(5), code);
        db.insert_account_info(&SYSTEM_ADDRESS, funded(5).with_nonce(7));
        let mut evm = evm(db);
        let out = evm
            .transact_with_inspector(&native(0, 1, 13), &mut CheckNonce)
            .unwrap()
            .detach();
        assert_eq!(out.result.is_success(), success);
        assert!(out.result.tx_gas_used() > 0);
        evm.overlay_db_mut().commit_pending(&out.pending_state);
        assert_eq!(balance(&mut evm, SYSTEM_ADDRESS), U256::from(5));
        assert_eq!(
            evm.read_account_info(&SYSTEM_ADDRESS)
                .unwrap()
                .unwrap()
                .nonce,
            7
        );
        assert_eq!(
            balance(&mut evm, EEZL2_ADDRESS),
            U256::from(if success { 13 } else { 0 })
        );
    }
}

#[test]
fn discarded_native_execution_preserves_group_cursor_and_gas_position() {
    use reth_evm::{BlockExecutor, CommitChanges};

    for conditional in [false, true] {
        let mut body = block(0, 1).into_block();
        body.header.gas_limit = 100_000;
        let block = SealedBlock::seal_slow(body).try_recover().unwrap();
        let config = config();
        let mut executor = config
            .executor_for_block(database(U256::ZERO, bytes!("00")), block.sealed_block())
            .unwrap();
        executor.apply_pre_execution_changes().unwrap();
        let load: EezTxEnvelope = SystemTransaction {
            chain_id: 1,
            source_hash: B256::repeat_byte(1),
            gas_limit: 100_000,
            to: EEZL2_ADDRESS,
            value: U256::ZERO,
            input: Bytes::from_static(&eez_primitives::LOAD_EXECUTION_TABLE_SELECTOR),
        }
        .into();
        let load = Recovered::new_unchecked(load, SYSTEM_ADDRESS);
        if conditional {
            assert!(
                executor
                    .execute_transaction_with_commit_condition(load.clone(), |result, _| {
                        assert!(result.is_success());
                        CommitChanges::No
                    })
                    .unwrap()
                    .is_none()
            );
        } else {
            drop(
                executor
                    .execute_transaction_without_commit(load.clone())
                    .unwrap(),
            );
        }
        // A discarded load must neither require a paired user nor consume gas.
        let delivery = Recovered::new_unchecked(native_tx(100_000, Bytes::new()), SYSTEM_ADDRESS);
        executor.execute_transaction(delivery.clone()).unwrap();
        // Once a load is committed, another native transaction cannot follow it.
        let used = executor.receipts()[0].cumulative_gas_used;
        let EezTxEnvelope::System(tx) = load.into_inner() else {
            unreachable!()
        };
        let mut load = tx.into_inner();
        load.gas_limit = 100_000 - used;
        let load: EezTxEnvelope = load.into();
        executor
            .execute_transaction(Recovered::new_unchecked(load, SYSTEM_ADDRESS))
            .unwrap();
        let error = executor
            .execute_transaction_without_commit(delivery)
            .unwrap_err();
        assert!(matches!(
            error,
            reth_evm::BlockExecutionError::Validation(_)
        ));
        assert!(error.to_string().contains("must be followed immediately"));
        assert!(
            executor
                .finish()
                .unwrap_err()
                .to_string()
                .contains("block ends inside an outbound")
        );
    }
}
