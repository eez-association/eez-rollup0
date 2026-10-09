//! Native deposit minting and fee collection around the Ethereum EVM.
//!
//! Delegates execution and inspector dispatch to Alloy 0.39.0's [`EthEvm`]:
//! <https://github.com/alloy-rs/alloy-evm/blob/ba6f83b80aba8cf005175f4d776d8b90796c72d9/crates/evm/src/eth/mod.rs>.

use alloy_evm::{
    Database, EthEvm, EthEvmFactory, Evm, EvmEnv, EvmFactory, eth::EthEvmContext,
    precompiles::PrecompilesMap,
};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_LIMIT, SYSTEM_TX_TYPE};
use revm::{
    Inspector,
    context::{BlockEnv, CfgEnv, TxEnv},
    context_interface::{
        Cfg, JournalTr, Transaction,
        journaled_state::account::JournaledAccountTr,
        result::{EVMError, HaltReason, ResultAndState},
    },
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    state::Account,
};

pub struct EezEvm<DB: Database, I> {
    ethereum: EthEvm<DB, I, PrecompilesMap>,
    fee_collector: Option<Address>,
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

    /// Mint native value before Ethereum's validation and transfer, preserving
    /// existing system funds. Revm errors discard the mint; reverted or halted
    /// calls need the explicit rollback below. The native TxEnv supplies zero fees.
    /// `EthEvm::transact_raw` dispatches to `inspect_tx` when its inspector is enabled,
    /// so all paths below preserve the inspector installed by our factory.
    fn transact_raw(&mut self, tx: TxEnv) -> Result<ResultAndState, Self::Error> {
        if tx.tx_type != SYSTEM_TX_TYPE {
            let Some(collector) = self.fee_collector else {
                return self.ethereum.transact_raw(tx);
            };
            if self.cfg_env().is_fee_charge_disabled() {
                return self.ethereum.transact_raw(tx);
            }
            // Ethereum has already deducted these fees and will refund unused
            // gas and reward the beneficiary. Restore only the amount it burns.
            // The minimum also handles simulations with base-fee checks disabled.
            let base_fee = if self.cfg_env().spec.is_enabled_in(SpecId::LONDON) {
                u128::from(self.block().basefee)
                    .min(tx.effective_gas_price(u128::from(self.block().basefee)))
            } else {
                0
            };
            let mut output = self.ethereum.transact_raw(tx)?;
            let fee = U256::from(base_fee) * U256::from(output.result.tx_gas_used());
            if !fee.is_zero() {
                // Load only after execution: preloading would warm the collector
                // and change BALANCE/CALL gas costs in the transaction itself.
                let account = match output.state.entry(collector) {
                    revm::primitives::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    revm::primitives::hash_map::Entry::Vacant(entry) => {
                        entry.insert(Account::from(
                            self.ethereum
                                .db_mut()
                                .basic(collector)
                                .map_err(EVMError::Database)?
                                .unwrap_or_default(),
                        ))
                    }
                };
                // Preserve Ethereum's account lifecycle. A balance credit does
                // not cancel deletion of a selfdestructed collector contract.
                account.info.balance = account
                    .info
                    .balance
                    .checked_add(fee)
                    .ok_or_else(|| EVMError::Custom("fee collector balance overflow".into()))?;
                account.mark_touch();
            }
            return Ok(output);
        }
        // Direct TxEnv callers bypass envelope conversion; enforce native sender,
        // target, and gas rules here before executing or minting value.
        if tx.caller != SYSTEM_ADDRESS
            || tx.kind != TxKind::Call(EEZL2_ADDRESS)
            || tx.gas_limit != SYSTEM_TX_GAS_LIMIT
            || tx.gas_price != 0
        {
            return Err(EVMError::Custom(
                "invalid native system caller, target, or gas fields".into(),
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

/// Installs identical minting and fee routing for ordinary and inspected execution.
#[derive(Debug, Default, Clone, Copy)]
pub struct EezEvmFactory {
    fee_collector: Option<Address>,
}

impl EezEvmFactory {
    pub const fn new(fee_collector: Option<Address>) -> Self {
        Self { fee_collector }
    }
}

impl EvmFactory for EezEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>>> = EezEvm<DB, I>;
    type Context<DB: Database> = EthEvmContext<DB>;
    type Tx = TxEnv;
    type Error<DBError: revm::database_interface::DBErrorMarker> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> EezEvm<DB, NoOpInspector> {
        EezEvm {
            ethereum: EthEvmFactory::default().create_evm(db, input),
            fee_collector: self.fee_collector,
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
            fee_collector: self.fee_collector,
        }
    }
}
