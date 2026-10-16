use super::*;

const COLLECTOR: Address = address!("0000000000000000000000000000000000000fee");
const USER: Address = address!("0000000000000000000000000000000000000011");
const BENEFICIARY: Address = address!("0000000000000000000000000000000000000022");

fn collecting_config(collector: Address) -> EezEvmConfig {
    let mut chain_spec = config().chain_spec().as_ref().clone();
    chain_spec.genesis.config.extra_fields.insert(
        "feeCollector".into(),
        serde_json::to_value(collector).unwrap(),
    );
    EezEvmConfig::new(Arc::new(chain_spec)).unwrap()
}

fn funded_database(code: Bytes) -> InMemoryDB {
    let mut db = database(U256::ZERO, code);
    db.insert_account_info(
        USER,
        AccountInfo {
            balance: U256::from(1_000_000),
            ..Default::default()
        },
    );
    db
}

#[test]
fn collects_base_fee_using_final_gas_after_refunds_and_failures() {
    let config = collecting_config(COLLECTOR);
    for (code, success, refund) in [
        (bytes!("600060005500"), true, true), // Clear an existing slot: real refund.
        (bytes!("600060005560006000fd"), false, false),
        (bytes!("fe"), false, false),
    ] {
        let mut env = config.evm_env(block(0, 1).header()).unwrap();
        env.block_env.basefee = 7;
        let mut db = funded_database(code);
        db.insert_account_storage(EEZL2_ADDRESS, U256::ZERO, U256::ONE)
            .unwrap();
        let mut evm = config.evm_with_env_and_inspector(db, env, CountInspector::default());
        let output = evm
            .transact_raw(TxEnv {
                caller: USER,
                kind: TxKind::Call(EEZL2_ADDRESS),
                gas_limit: 100_000,
                gas_price: 10,
                chain_id: Some(1),
                ..Default::default()
            })
            .unwrap();
        // Confirm the fixtures really reach refund, revert, and halt accounting.
        assert_eq!(output.result.is_success(), success);
        assert_eq!(
            output.result.gas().total_gas_spent() > output.result.tx_gas_used(),
            refund,
        );
        assert_eq!(
            output.state[&COLLECTOR].info.balance,
            U256::from(output.result.tx_gas_used() * 7),
        );
    }
}

#[test]
fn shared_recipient_receives_both_base_fee_and_tip() {
    let config = collecting_config(COLLECTOR);
    let mut env = config.evm_env(block(0, 1).header()).unwrap();
    env.block_env.basefee = 7;
    env.block_env.beneficiary = COLLECTOR;
    let mut db = funded_database(bytes!("00"));
    db.insert_account_info(
        COLLECTOR,
        AccountInfo {
            balance: U256::from(11),
            ..Default::default()
        },
    );
    let output = config
        .evm_with_env(db, env)
        .transact_raw(TxEnv {
            caller: USER,
            kind: TxKind::Call(EEZL2_ADDRESS),
            gas_limit: 21_000,
            gas_price: 10,
            chain_id: Some(1),
            ..Default::default()
        })
        .unwrap();
    // The added base fee must accumulate with Reth's tip, not overwrite it.
    assert_eq!(
        output.state[&COLLECTOR].info.balance,
        U256::from(11 + 210_000)
    );
}

#[test]
fn native_transactions_credit_no_base_fee() {
    let config = collecting_config(COLLECTOR);
    let mut evm = config.evm_with_env(
        database(U256::ZERO, bytes!("00")),
        config.evm_env(block(0, 1).header()).unwrap(),
    );
    let tx = TxEnv::from_recovered_tx(&block(0, 1).body().transactions[0], SYSTEM_ADDRESS);
    let output = evm.transact_raw(tx).unwrap();
    assert!(output.result.is_success());
    assert!(output.result.tx_gas_used() > 0);
    assert!(!output.state.contains_key(&COLLECTOR));
}

#[test]
fn fee_credit_is_visible_to_later_transactions_without_warming_the_collector() {
    let config = collecting_config(COLLECTOR);
    let signature = Signature::new(U256::ONE, U256::ONE, false);
    let transactions: Vec<EezTxEnvelope> = (0..2)
        .map(|nonce| {
            EezTxEnvelope::Ethereum(
                TxEip1559 {
                    chain_id: 1,
                    nonce,
                    gas_limit: 100_000,
                    max_fee_per_gas: 7,
                    max_priority_fee_per_gas: 2,
                    to: TxKind::Call(EEZL2_ADDRESS),
                    ..Default::default()
                }
                .into_signed(signature)
                .into(),
            )
        })
        .collect();
    let mut block = block(0, 1).into_block();
    block.header.beneficiary = BENEFICIARY;
    block.body.transactions = transactions;
    let block = SealedBlock::seal_slow(block).try_recover().unwrap();
    // Store BALANCE(collector); collection itself must not warm the account.
    let mut code = vec![0x73];
    code.extend_from_slice(COLLECTOR.as_slice());
    code.extend_from_slice(&[0x31, 0x60, 0x00, 0x55, 0x00]);
    let mut db = database(U256::ZERO, code.into());
    for signer in block.senders() {
        db.insert_account_info(
            *signer,
            AccountInfo {
                nonce: 0,
                balance: U256::from(1_000_000),
                ..Default::default()
            },
        );
    }
    // Synthetic signatures recover different funded senders. Set the second sender's nonce.
    db.cache
        .accounts
        .get_mut(&block.senders()[1])
        .unwrap()
        .info
        .nonce = 1;
    let mut executor = config.executor(db);
    let result = executor.execute_one(&block).unwrap();
    let first_gas = result.receipts[0].cumulative_gas_used;
    let total_gas = result.receipts[1].cumulative_gas_used;
    // Each BALANCE must pay 2,600 for a cold collector. The first SSTORE keeps
    // zero (2,200 gas); the second writes zero -> nonzero (22,100 gas).
    assert_eq!(first_gas, 21_000 + 3 + 2_600 + 3 + 2_200);
    assert_eq!(total_gas - first_gas, 21_000 + 3 + 2_600 + 3 + 22_100);
    let bundle = executor.into_state().take_bundle();
    assert_eq!(
        bundle.state[&COLLECTOR].info.as_ref().unwrap().balance,
        U256::from(total_gas)
    );
    assert_eq!(
        bundle.state[&EEZL2_ADDRESS].storage[&U256::ZERO].present_value,
        U256::from(first_gas)
    );
}

#[test]
fn simulations_do_not_commit_fee_credits_or_credit_uncharged_fees() {
    let config = collecting_config(COLLECTOR);
    let mut env = config.evm_env(block(0, 1).header()).unwrap();
    env.block_env.basefee = 7;
    let db = funded_database(bytes!("00"));
    let tx = TxEnv {
        caller: USER,
        kind: TxKind::Call(EEZL2_ADDRESS),
        gas_limit: 21_000,
        gas_price: 10,
        chain_id: Some(1),
        ..Default::default()
    };
    let mut evm = config.evm_with_env(db.clone(), env.clone());
    let preview = evm.transact_raw(tx.clone()).unwrap();
    assert_eq!(preview.state[&COLLECTOR].info.balance, U256::from(147_000));
    assert_eq!(evm.db().cache.accounts[&COLLECTOR].info.balance, U256::ZERO);

    // eth_call-style execution can skip the base-fee price check. Only collect
    // fees actually charged, even when the block's base fee exceeds the tx price.
    env.cfg_env.disable_base_fee = true;
    for gas_price in [0, 3] {
        let output = config
            .evm_with_env(db.clone(), env.clone())
            .transact_raw(TxEnv {
                gas_price,
                ..tx.clone()
            })
            .unwrap();
        assert!(output.result.is_success());
        if gas_price == 0 {
            assert!(!output.state.contains_key(&COLLECTOR));
        } else {
            assert_eq!(
                output.state[&COLLECTOR].info.balance,
                U256::from(21_000 * gas_price),
            );
        }
    }
}
