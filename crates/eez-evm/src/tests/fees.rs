use super::*;
use alloy_primitives::address;
use evm2::{EvmFeatures, NoopInspector};

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
    db.insert_account_info(&USER, funded(1_000_000));
    db
}

fn user_call(gas_limit: u64, gas_price: u128) -> Recovered<EezEvmTx> {
    legacy(
        USER,
        TxLegacy {
            chain_id: Some(1),
            gas_price,
            gas_limit,
            to: TxKind::Call(EEZL2_ADDRESS),
            ..Default::default()
        },
    )
}

fn env_with_base_fee(config: &EezEvmConfig) -> EvmEnvFor<EezEvmConfig> {
    let mut env = config.evm_env(block(0, 1).header()).unwrap();
    env.block.basefee = U256::from(7);
    env
}

#[test]
fn collects_base_fee_using_final_gas_after_refunds_and_failures() {
    let config = collecting_config(COLLECTOR);
    for (code, success, refund) in [
        (bytes!("600060005500"), true, true), // Clear an existing slot: real refund.
        (bytes!("600060005560006000fd"), false, false),
        (bytes!("fe"), false, false),
    ] {
        let mut db = funded_database(code);
        db.insert_account_storage(&EEZL2_ADDRESS, &U256::ZERO, &U256::ONE);
        // Inspected, so the inspected path's settlement is covered too.
        let output = config
            .evm_with_env(db, env_with_base_fee(&config))
            .transact_with_inspector(&user_call(100_000, 10), &mut NoopInspector::default())
            .unwrap()
            .detach();
        // Confirm the fixtures really reach refund, revert, and halt accounting.
        let result = &output.result;
        assert_eq!(result.is_success(), success);
        assert_eq!(result.total_gas_spent > result.tx_gas_used(), refund);
        assert_eq!(
            output
                .pending_state
                .account_info(&COLLECTOR)
                .unwrap()
                .balance,
            U256::from(result.tx_gas_used() * 7),
        );
    }
}

#[test]
fn shared_recipient_receives_both_base_fee_and_tip() {
    let config = collecting_config(COLLECTOR);
    let mut env = env_with_base_fee(&config);
    env.block.beneficiary = COLLECTOR;
    let mut db = funded_database(bytes!("00"));
    db.insert_account_info(&COLLECTOR, funded(11));
    let output = config
        .evm_with_env(db, env)
        .transact(&user_call(21_000, 10))
        .unwrap()
        .detach();
    // The added base fee must accumulate with the tip, not overwrite it.
    assert_eq!(
        output
            .pending_state
            .account_info(&COLLECTOR)
            .unwrap()
            .balance,
        U256::from(11 + 210_000)
    );
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
    // Synthetic signatures recover different funded senders; the second
    // transaction carries nonce 1.
    for (nonce, signer) in (0..).zip(block.senders()) {
        db.insert_account_info(signer, funded(1_000_000).with_nonce(nonce));
    }
    let output = config.executor(db).execute(&block).unwrap();
    let receipts = &output.result.receipts;
    let first_gas = receipts[0].cumulative_gas_used;
    let total_gas = receipts[1].cumulative_gas_used;
    // Each BALANCE must pay 2,600 for a cold collector. The first SSTORE keeps
    // zero (2,200 gas); the second writes zero -> nonzero (22,100 gas).
    assert_eq!(first_gas, 21_000 + 3 + 2_600 + 3 + 2_200);
    assert_eq!(total_gas - first_gas, 21_000 + 3 + 2_600 + 3 + 22_100);
    let bundle = &output.state;
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
fn simulations_do_not_credit_uncharged_fees() {
    let config = collecting_config(COLLECTOR);
    // eth_call-style execution can skip the base-fee price check. Only collect
    // fees actually charged, even when the block's base fee exceeds the tx price.
    let mut env = env_with_base_fee(&config);
    env.version.features.remove(EvmFeatures::BASE_FEE_CHECK);
    for gas_price in [0, 3] {
        let output = config
            .evm_with_env(funded_database(bytes!("00")), env.clone())
            .transact(&user_call(21_000, gas_price))
            .unwrap()
            .detach();
        assert!(output.result.is_success());
        let collector = output.pending_state.account_info(&COLLECTOR);
        if gas_price == 0 {
            assert!(collector.is_none());
        } else {
            assert_eq!(
                collector.unwrap().balance,
                U256::from(21_000 * u64::try_from(gas_price).unwrap())
            );
        }
    }
}
