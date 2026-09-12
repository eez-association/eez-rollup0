//! Native deposit minting around the unmodified Ethereum EVM.

use alloy_evm::{
    Database, EthEvm, EthEvmFactory, Evm, EvmEnv, EvmFactory, eth::EthEvmContext,
    precompiles::PrecompilesMap,
};
use alloy_primitives::{Address, Bytes, TxKind};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_TYPE};
use revm::{
    Inspector,
    context::{BlockEnv, CfgEnv, TxEnv},
    context_interface::{
        JournalTr,
        journaled_state::account::JournaledAccountTr,
        result::{EVMError, HaltReason, ResultAndState},
    },
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
};

pub struct EezEvm<DB: Database, I> {
    ethereum: EthEvm<DB, I, PrecompilesMap>,
}

impl<DB: Database, I> std::fmt::Debug for EezEvm<DB, I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EezEvm").finish_non_exhaustive()
    }
}

impl<DB: Database, I: Inspector<EthEvmContext<DB>>> Evm for EezEvm<DB, I> {
    type DB = DB;
    type Tx = TxEnv;
    type Error = EVMError<DB::Error>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;
    type Inspector = I;

    fn block(&self) -> &BlockEnv {
        self.ethereum.block()
    }

    fn cfg_env(&self) -> &CfgEnv {
        self.ethereum.cfg_env()
    }

    fn chain_id(&self) -> u64 {
        self.ethereum.chain_id()
    }

    /// Native value is minted for this call, independently of the system
    /// account's existing balance (which may contain outbound ETH). The mint
    /// enters revm's journal before its ordinary nonce/funding validation and
    /// value transfer. Invalid transactions discard it through revm's error
    /// cleanup; an outer EVM revert/halt needs the explicit rollback below.
    /// Fee exemption comes from the native zero-price TxEnv, not a cfg flag.
    fn transact_raw(&mut self, tx: TxEnv) -> Result<ResultAndState, Self::Error> {
        if tx.tx_type != SYSTEM_TX_TYPE {
            return self.ethereum.transact_raw(tx);
        }
        if tx.caller != SYSTEM_ADDRESS || tx.kind != TxKind::Call(EEZL2_ADDRESS) {
            return Err(EVMError::Custom(
                "invalid native system caller or target".into(),
            ));
        }
        if tx.value.is_zero() {
            return self.ethereum.transact_raw(tx);
        }

        let journal = &mut self.ethereum.ctx_mut().journaled_state;
        let funding = journal
            .load_account_with_code_mut(SYSTEM_ADDRESS)
            .map_err(EVMError::Database)
            .and_then(|mut account| {
                let balance = *account.balance();
                let funded = balance
                    .checked_add(tx.value)
                    .ok_or_else(|| EVMError::Custom("native deposit balance overflow".into()))?;
                account.set_balance(funded);
                Ok(balance)
            });
        let previous_balance = match funding {
            Ok(balance) => balance,
            Err(error) => {
                // These errors occur before Ethereum's transact/finalize path.
                // Discard the loaded state so the next transaction starts clean.
                journal.finalize();
                return Err(error);
            }
        };
        let mut output = self.ethereum.transact_raw(tx)?;
        if !output.result.is_success() {
            // The outer call already rolled back all transfers, but the mint
            // precedes that call's checkpoint. Remove only that mint; retain
            // the normal nonce increment and the receipt's metered gas usage.
            output
                .state
                .get_mut(&SYSTEM_ADDRESS)
                .expect("executed native transaction has a caller account")
                .info
                .balance = previous_balance;
        }
        Ok(output)
    }

    fn transact_system_call(
        &mut self,
        caller: Address,
        contract: Address,
        data: Bytes,
    ) -> Result<ResultAndState, Self::Error> {
        self.ethereum.transact_system_call(caller, contract, data)
    }

    fn finish(self) -> (DB, EvmEnv) {
        self.ethereum.finish()
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.ethereum.set_inspector_enabled(enabled);
    }

    fn components(&self) -> (&DB, &I, &PrecompilesMap) {
        self.ethereum.components()
    }

    fn components_mut(&mut self) -> (&mut DB, &mut I, &mut PrecompilesMap) {
        self.ethereum.components_mut()
    }
}

/// Reuses Ethereum's factory, including its inspector and precompile setup.
/// The wrapper adds only native minting at the transaction execution boundary.
#[derive(Debug, Default, Clone, Copy)]
pub struct EezEvmFactory;

impl EvmFactory for EezEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>>> = EezEvm<DB, I>;
    type Context<DB: Database> = EthEvmContext<DB>;
    type Tx = TxEnv;
    type Error<DBError: std::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> EezEvm<DB, NoOpInspector> {
        EezEvm {
            ethereum: EthEvmFactory::default().create_evm(db, input),
        }
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> EezEvm<DB, I> {
        EezEvm {
            ethereum: EthEvmFactory::default().create_evm_with_inspector(db, input, inspector),
        }
    }
}
