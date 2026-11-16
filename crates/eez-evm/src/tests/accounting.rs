use super::*;
use alloy_sol_types::SolEvent;
use eez_primitives::eez_l2_events::CrossChainCallExecuted;

const USER: Address = Address::repeat_byte(0x31);
const CHILD: Address = Address::repeat_byte(0x32);
const COLLECTOR: Address = Address::repeat_byte(0x33);

// Real EVM transfers and LOG instructions, using the production event ABI. The
// live accounting suite also exercises the deployed EEZL2 and proxy bytecode.
fn withdrawal_code(values: &[U256], ending: &[u8]) -> Bytes {
    let mut code = Vec::new();
    let mut bodies = Vec::new();
    for value in values {
        code.extend_from_slice(&[0x5f, 0x5f, 0x5f, 0x5f, 0x7f]);
        code.extend_from_slice(&value.to_be_bytes::<32>());
        code.push(0x73);
        code.extend_from_slice(SYSTEM_ADDRESS.as_slice());
        code.extend_from_slice(&[0x5a, 0xf1, 0x50]); // CALL SYSTEM, POP
        let log = CrossChainCallExecuted {
            crossChainCallHash: B256::ZERO,
            proxy: USER,
            sourceAddress: USER,
            callData: bytes!("deadbeef"),
            value: *value,
            callGas: 0,
        }
        .encode_log_data();
        let len = u16::try_from(log.data.len()).unwrap().to_be_bytes();
        code.push(0x61);
        code.extend_from_slice(&len);
        code.push(0x61);
        let offset = code.len();
        code.extend_from_slice(&[0, 0, 0x5f, 0x39]); // CODECOPY
        for topic in log.topics().iter().rev() {
            code.push(0x7f);
            code.extend_from_slice(topic.as_slice());
        }
        code.push(0x61);
        code.extend_from_slice(&len);
        code.extend_from_slice(&[0x5f, 0xa3]);
        bodies.push((offset, log.data));
    }
    if ending == [0x5b, 0x61, 0, 0, 0x56] {
        let dest = u16::try_from(code.len()).unwrap().to_be_bytes();
        code.extend_from_slice(&[0x5b, 0x61, dest[0], dest[1], 0x56]);
    } else {
        code.extend_from_slice(ending);
    }
    for (offset, data) in bodies {
        let start = u16::try_from(code.len()).unwrap().to_be_bytes();
        code[offset..offset + 2].copy_from_slice(&start);
        code.extend_from_slice(&data);
    }
    code.into()
}

fn tx(to: Address, value: U256) -> TxEnv {
    TxEnv {
        caller: USER,
        kind: TxKind::Call(to),
        value,
        gas_limit: 300_000,
        gas_price: 3,
        chain_id: Some(1),
        ..Default::default()
    }
}

fn funded(code: Bytes) -> InMemoryDB {
    let mut db = database(U256::from(12345), code);
    db.cache
        .accounts
        .get_mut(&EEZL2_ADDRESS)
        .unwrap()
        .info
        .balance = U256::from(23456);
    db.insert_account_info(
        USER,
        AccountInfo {
            balance: U256::from(10_000_000),
            ..Default::default()
        },
    );
    db
}

fn supply(db: &InMemoryDB) -> U256 {
    db.cache.accounts.values().map(|a| a.info.balance).sum()
}

#[test]
fn committed_withdrawals_burn_exact_value_preserving_donations_and_fees() {
    for values in [
        vec![U256::ZERO],
        vec![U256::from(17)],
        vec![U256::from(7), U256::from(11)],
    ] {
        for (ending, success) in [
            (&[0x00][..], true),
            (&[0x5f, 0x5f, 0xfd][..], false),
            (&[0xfe][..], false),
            (&[0x5b, 0x61, 0, 0, 0x56][..], false),
        ] {
            let value: U256 = values.iter().copied().sum();
            let db = funded(withdrawal_code(&values, ending));
            let initial = supply(&db);
            let env = config().evm_env(block(0, 1).header()).unwrap();
            let factory = EezEvmFactory::new(Some(COLLECTOR));
            let mut plain = factory.create_evm(db.clone(), env.clone());
            let mut inspected =
                factory.create_evm_with_inspector(db, env, CountInspector::default());
            let result = plain.transact_raw(tx(EEZL2_ADDRESS, value)).unwrap();
            assert_eq!(
                result,
                inspected.transact_raw(tx(EEZL2_ADDRESS, value)).unwrap()
            );
            assert_eq!(result.result.is_success(), success);
            assert_eq!(
                result.result.logs().len(),
                if success { values.len() } else { 0 }
            );
            plain.db_mut().commit(result.state);
            assert_eq!(
                plain.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
                U256::from(12345)
            );
            assert_eq!(
                plain.db().cache.accounts[&EEZL2_ADDRESS].info.balance,
                U256::from(23456)
            );
            assert_eq!(
                supply(plain.db()),
                initial - if success { value } else { U256::ZERO }
            );
        }
    }
}

#[test]
fn caught_reverted_withdrawal_does_not_burn_but_committed_sibling_does() {
    // USER calls CHILD, which calls EEZL2 twice. The first call transfers/logs
    // then reverts; the caller catches it. The second withdrawal commits.
    // EEZL2 switches between revert/success using calldata size.
    let body = withdrawal_code(
        &[U256::from(17)],
        &[0x36, 0x60, 0x00, 0x57, 0x00, 0x5b, 0x5f, 0x5f, 0xfd],
    );
    let mut body = body.to_vec();
    // Find the terminal CALLDATASIZE, PUSH1, destination sequence before data.
    let terminal = body
        .windows(9)
        .position(|w| w == [0x36, 0x60, 0, 0x57, 0, 0x5b, 0x5f, 0x5f, 0xfd])
        .unwrap();
    body[terminal + 2] = u8::try_from(terminal + 5).unwrap();
    let mut db = funded(body.into());
    let mut caller = Vec::new();
    for size in [1, 0] {
        caller.extend_from_slice(&[0x5f, 0x5f, 0x60, size, 0x5f, 0x60, 17, 0x73]);
        caller.extend_from_slice(EEZL2_ADDRESS.as_slice());
        caller.extend_from_slice(&[0x5a, 0xf1, 0x50]);
    }
    caller.push(0x00);
    let code = Bytecode::new_raw(caller.into());
    db.insert_account_info(
        CHILD,
        AccountInfo {
            code_hash: code.hash_slow(),
            code: Some(code),
            ..Default::default()
        },
    );
    let initial = supply(&db);
    let mut evm = EezEvmFactory::new(Some(COLLECTOR))
        .create_evm(db, config().evm_env(block(0, 1).header()).unwrap());
    let result = evm.transact_raw(tx(CHILD, U256::from(34))).unwrap();
    assert!(result.result.is_success());
    assert_eq!(
        result.result.logs().len(),
        1,
        "one reverted log and one committed log must be exercised"
    );
    evm.db_mut().commit(result.state);
    assert_eq!(
        evm.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
        U256::from(12345)
    );
    assert_eq!(evm.db().cache.accounts[&CHILD].info.balance, U256::from(17));
    assert_eq!(supply(evm.db()), initial - U256::from(17));
}

#[test]
fn foreign_logs_and_l1_simulation_do_not_authorize_burns() {
    for (l1, target) in [(false, CHILD), (true, EEZL2_ADDRESS)] {
        let code = withdrawal_code(&[U256::from(17)], &[0x00]);
        let mut db = funded(code.clone());
        if target == CHILD {
            let code = Bytecode::new_raw(code);
            db.insert_account_info(
                CHILD,
                AccountInfo {
                    code_hash: code.hash_slow(),
                    code: Some(code),
                    ..Default::default()
                },
            );
        }
        let factory = if l1 {
            EezEvmFactory::for_l1_simulation()
        } else {
            EezEvmFactory::default()
        };
        let mut evm = factory.create_evm(db, config().evm_env(block(0, 1).header()).unwrap());
        let result = evm.transact_raw(tx(target, U256::from(17))).unwrap();
        assert!(result.result.is_success());
        assert_eq!(result.result.logs().len(), 1);
        assert_eq!(
            result.state[&SYSTEM_ADDRESS].info.balance,
            U256::from(12362)
        );
    }
}

#[test]
fn preview_does_not_burn_twice_and_failed_validation_does_not_leak_state() {
    let mut evm = EezEvmFactory::new(Some(COLLECTOR)).create_evm(
        funded(withdrawal_code(&[U256::from(17)], &[0x00])),
        config().evm_env(block(0, 1).header()).unwrap(),
    );
    let transaction = tx(EEZL2_ADDRESS, U256::from(17));
    assert!(
        evm.transact_raw(TxEnv {
            nonce: 1,
            ..transaction.clone()
        })
        .is_err()
    );
    let preview = evm.transact_raw(transaction.clone()).unwrap();
    assert_eq!(preview, evm.transact_raw(transaction.clone()).unwrap());
    assert_eq!(
        evm.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
        U256::from(12345)
    );
    evm.db_mut().commit(preview.state);
    assert!(evm.transact_raw(transaction.clone()).is_err());
    let result = evm
        .transact_raw(TxEnv {
            nonce: 1,
            ..transaction
        })
        .unwrap();
    assert_eq!(
        result.state[&SYSTEM_ADDRESS].info.balance,
        U256::from(12345)
    );
}

#[test]
fn invalid_accounting_fails_closed_without_poisoning_the_next_transaction() {
    let mut malformed = withdrawal_code(&[U256::from(17)], &[0x00]).to_vec();
    let log = malformed.iter().rposition(|op| *op == 0xa3).unwrap();
    malformed[log] = 0xa2; // Authentic address/signature, missing indexed topic.
    for code in [
        Bytes::from(malformed),
        withdrawal_code(&[U256::MAX, U256::ONE], &[0x00]),
        withdrawal_code(&[U256::from(1_000_000)], &[0x00]),
    ] {
        let db = funded(code);
        let initial = supply(&db);
        let mut evm = EezEvmFactory::new(Some(COLLECTOR))
            .create_evm(db, config().evm_env(block(0, 1).header()).unwrap());
        assert!(evm.transact_raw(tx(EEZL2_ADDRESS, U256::ZERO)).is_err());
        assert_eq!(supply(evm.db()), initial);
        assert_eq!(evm.db().cache.accounts[&USER].info.nonce, 0);
        let output = evm
            .transact_raw(tx(SYSTEM_ADDRESS, U256::from(19)))
            .unwrap();
        assert!(output.result.is_success());
        assert_eq!(
            output.state[&SYSTEM_ADDRESS].info.balance,
            U256::from(12364)
        );
    }
}

#[test]
fn l1_simulation_rejects_native_minting() {
    let block = block(0, 1);
    let config = EezEvmConfig::for_l1_simulation(config().chain_spec().clone());
    let mut evm = config.evm_with_env(
        database(U256::from(5), bytes!("00")),
        config.evm_env(block.header()).unwrap(),
    );
    let tx = TxEnv::from_recovered_tx(&block.body().transactions[0], SYSTEM_ADDRESS);
    assert!(
        evm.transact_raw(tx)
            .unwrap_err()
            .to_string()
            .contains("L2-only")
    );
    assert_eq!(
        evm.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
        U256::from(5)
    );
}

#[test]
fn protocol_fee_credits_survive_and_fee_free_simulations_still_burn() {
    for collector in [COLLECTOR, SYSTEM_ADDRESS, EEZL2_ADDRESS] {
        for disable_fee_charge in [false, true] {
            let db = funded(withdrawal_code(&[U256::from(17)], &[0x00]));
            let initial = supply(&db);
            let mut env = config().evm_env(block(0, 1).header()).unwrap();
            env.cfg_env.disable_base_fee = disable_fee_charge;
            let mut evm = EezEvmFactory::new(Some(collector)).create_evm(db, env);
            let mut transaction = tx(EEZL2_ADDRESS, U256::from(17));
            if disable_fee_charge {
                transaction.gas_price = 0;
            }
            let output = evm.transact_raw(transaction).unwrap();
            assert!(output.result.is_success());
            let fee = if disable_fee_charge {
                U256::ZERO
            } else {
                U256::from(output.result.tx_gas_used())
            };
            evm.db_mut().commit(output.state);
            assert_eq!(supply(evm.db()), initial - U256::from(17));
            for (address, donation) in [(SYSTEM_ADDRESS, 12345), (EEZL2_ADDRESS, 23456)] {
                assert_eq!(
                    evm.db().cache.accounts[&address].info.balance,
                    U256::from(donation)
                        + if collector == address {
                            fee
                        } else {
                            U256::ZERO
                        }
                );
            }
        }
    }
}

#[test]
fn ordinary_protocol_payment_in_the_withdrawal_transaction_survives() {
    // EEZL2's stand-in forwards another five wei without a withdrawal event.
    // An end-of-transaction balance sweep would incorrectly burn that payment.
    let mut ending = vec![0x5f, 0x5f, 0x5f, 0x5f, 0x60, 5, 0x73];
    ending.extend_from_slice(SYSTEM_ADDRESS.as_slice());
    ending.extend_from_slice(&[0x5a, 0xf1, 0x50, 0x00]);
    let db = funded(withdrawal_code(&[U256::from(17)], &ending));
    let initial = supply(&db);
    let mut evm = EezEvmFactory::new(Some(COLLECTOR))
        .create_evm(db, config().evm_env(block(0, 1).header()).unwrap());
    let output = evm.transact_raw(tx(EEZL2_ADDRESS, U256::from(22))).unwrap();
    assert!(output.result.is_success());
    assert_eq!(output.result.logs().len(), 1);
    evm.db_mut().commit(output.state);
    assert_eq!(supply(evm.db()), initial - U256::from(17));
    assert_eq!(
        evm.db().cache.accounts[&SYSTEM_ADDRESS].info.balance,
        U256::from(12350)
    );
    assert_eq!(
        evm.db().cache.accounts[&EEZL2_ADDRESS].info.balance,
        U256::from(23456)
    );
}
