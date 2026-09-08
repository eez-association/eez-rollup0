use super::*;
use alloy_consensus::Typed2718;
use alloy_primitives::{B256, U256, bytes};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_TYPE, SystemTransaction};
use reth_chainspec::ChainSpecBuilder;
use reth_evm::execute::Executor;
use revm::{
    database::InMemoryDB,
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
        gas_price: 7,
        gas_limit: 100_000,
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

fn database(balance: U256, revert: bool) -> InMemoryDB {
    let mut db = InMemoryDB::default();
    db.insert_account_info(
        SYSTEM_ADDRESS,
        AccountInfo {
            balance,
            ..Default::default()
        },
    );
    // Record CALLER in slot zero; optionally revert after the write.
    let code = Bytecode::new_raw(if revert {
        bytes!("3360005560006000fd")
    } else {
        bytes!("3360005500")
    });
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
fn native_execution_uses_fixed_sender_and_ordinary_nonce_balance_and_revert_rules() {
    let balance = U256::from(1_000_000);
    for revert in [false, true] {
        let config = config();
        let mut executor = config.executor(database(balance, revert));
        let result = executor.execute_one(&block(0, 1)).unwrap();
        let receipt = &result.receipts[0];
        assert_eq!(receipt.tx_type.ty(), SYSTEM_TX_TYPE);
        assert_eq!(receipt.success, !revert);
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
        let transferred = if revert { U256::ZERO } else { U256::from(13) };
        assert_eq!(
            system.balance,
            balance - U256::from(receipt.cumulative_gas_used * 7) - transferred
        );
        if !revert {
            let target = bundle.state.get(&EEZL2_ADDRESS).unwrap();
            assert_eq!(target.info.as_ref().unwrap().balance, transferred);
            assert_eq!(
                target.storage.get(&U256::ZERO).unwrap().present_value,
                U256::from_be_slice(SYSTEM_ADDRESS.as_slice())
            );
        }
    }
}

#[test]
fn native_execution_does_not_bypass_nonce_chain_or_funding_validation() {
    for (balance, nonce, chain, expected) in [
        (1_000_000, 1, 1, "NonceTooHigh"),
        (1_000_000, 0, 2, "InvalidChainId"),
        (0, 0, 1, "LackOfFundForMaxFee"),
    ] {
        let error = config()
            .executor(database(U256::from(balance), false))
            .execute_one(&block(nonce, chain))
            .unwrap_err();
        assert!(
            format!("{error:?}").contains(expected),
            "wrong validation error: {error:?}"
        );
    }
}
