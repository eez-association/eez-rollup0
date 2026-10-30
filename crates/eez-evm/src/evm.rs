//! Native deposit minting and fee collection around the Ethereum EVM.
//!
//! Uses the Ethereum Revm handler for ordinary transactions and a native handler
//! that omits nonce validation/increment, with matching inspector dispatch.

use alloy_evm::{
    Database, EthEvmFactory, Evm, EvmEnv, EvmFactory, eth::EthEvmContext,
    precompiles::PrecompilesMap,
};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SYSTEM_TX_GAS_CAP, SYSTEM_TX_TYPE};
use revm::{
    ExecuteEvm, InspectEvm, Inspector, SystemCallEvm,
    context::{BlockEnv, CfgEnv, Evm as RevmEvm, TxEnv},
    context_interface::{
        Cfg, ContextTr, JournalTr, Transaction,
        journaled_state::account::JournaledAccountTr,
        result::{EVMError, HaltReason, ResultAndState},
    },
    handler::{EthFrame, Handler, instructions::EthInstructions, pre_execution},
    inspector::InspectorHandler,
    inspector::NoOpInspector,
    interpreter::{InitialAndFloorGas, interpreter::EthInterpreter},
    primitives::hardfork::SpecId,
    state::Account,
};

type Inner<DB, I> = RevmEvm<
    EthEvmContext<DB>,
    I,
    EthInstructions<EthInterpreter, EthEvmContext<DB>>,
    PrecompilesMap,
    EthFrame,
>;

pub struct EezEvm<DB: Database, I> {
    ethereum: Inner<DB, I>,
    inspect: bool,
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
        &self.ethereum.ctx.block
    }

    fn cfg_env(&self) -> &CfgEnv {
        &self.ethereum.ctx.cfg
    }

    fn chain_id(&self) -> u64 {
        self.ethereum.ctx.cfg.chain_id
    }

    /// Mint native value before Ethereum's validation and transfer, preserving
    /// existing system funds. Revm errors discard the mint; reverted or halted
    /// calls need the explicit rollback below. The native TxEnv supplies zero fees.
    /// Both handlers preserve the inspector installed by our factory.
    fn transact_raw(&mut self, tx: TxEnv) -> Result<ResultAndState, Self::Error> {
        if tx.tx_type != SYSTEM_TX_TYPE {
            if tx.caller == SYSTEM_ADDRESS {
                return Err(EVMError::Custom(
                    "reserved system sender requires native type".into(),
                ));
            }
            let Some(collector) = self.fee_collector else {
                return self.execute(tx);
            };
            if self.cfg_env().is_fee_charge_disabled() {
                return self.execute(tx);
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
            let mut output = self.execute(tx)?;
            let fee = U256::from(base_fee) * U256::from(output.result.tx_gas_used());
            if !fee.is_zero() {
                // Load only after execution: preloading would warm the collector
                // and change BALANCE/CALL gas costs in the transaction itself.
                let account = match output.state.entry(collector) {
                    revm::primitives::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    revm::primitives::hash_map::Entry::Vacant(entry) => {
                        entry.insert(Account::from(
                            self.ethereum
                                .ctx
                                .journaled_state
                                .database
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
            || tx.gas_limit > SYSTEM_TX_GAS_CAP
            || tx.nonce != 0
            || tx.chain_id.is_none()
            || tx.gas_priority_fee.is_some()
            || !tx.access_list.0.is_empty()
            || !tx.blob_hashes.is_empty()
            || tx.max_fee_per_blob_gas != 0
            || !tx.authorization_list.is_empty()
            || tx.gas_price != 0
        {
            return Err(EVMError::Custom(
                "invalid native system caller, target, or gas fields".into(),
            ));
        }
        if tx.value.is_zero() {
            return self.execute(tx);
        }

        let journal = &mut self.ethereum.ctx.journaled_state;
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
        let mut output = self.execute(tx)?;
        if !output.result.is_success() {
            // The outer call already rolled back all transfers, but the mint
            // precedes that call's checkpoint. Remove only that mint; retain
            // the unchanged nonce and the receipt's metered gas usage.
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
        self.ethereum
            .system_call_with_caller(caller, contract, data)
    }

    fn finish(self) -> (DB, EvmEnv) {
        let revm::Context {
            block,
            cfg,
            journaled_state,
            ..
        } = self.ethereum.ctx;
        (
            journaled_state.database,
            EvmEnv {
                block_env: block,
                cfg_env: cfg,
            },
        )
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.inspect = enabled;
    }

    fn components(&self) -> (&DB, &I, &PrecompilesMap) {
        (
            &self.ethereum.ctx.journaled_state.database,
            &self.ethereum.inspector,
            &self.ethereum.precompiles,
        )
    }

    fn components_mut(&mut self) -> (&mut DB, &mut I, &mut PrecompilesMap) {
        (
            &mut self.ethereum.ctx.journaled_state.database,
            &mut self.ethereum.inspector,
            &mut self.ethereum.precompiles,
        )
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
            ethereum: EthEvmFactory::default().create_evm(db, input).into_inner(),
            inspect: false,
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
            ethereum: EthEvmFactory::default()
                .create_evm_with_inspector(db, input, inspector)
                .into_inner(),
            inspect: true,
            fee_collector: self.fee_collector,
        }
    }
}

impl<DB: Database, I: Inspector<EthEvmContext<DB>>> EezEvm<DB, I> {
    fn execute(&mut self, tx: TxEnv) -> Result<ResultAndState, EVMError<DB::Error>> {
        if tx.tx_type != SYSTEM_TX_TYPE {
            return if self.inspect {
                self.ethereum.inspect_tx(tx)
            } else {
                self.ethereum.transact(tx)
            };
        }
        self.ethereum.ctx.tx = tx;
        let mut handler = NativeHandler(std::marker::PhantomData);
        let result = if self.inspect {
            handler.inspect_run(&mut self.ethereum)
        } else {
            handler.run(&mut self.ethereum)
        };
        // Match ExecuteEvm::transact: clear the journal on success and error.
        let state = self.ethereum.finalize();
        Ok(ResultAndState::new(result?, state))
    }
}

struct NativeHandler<DB: Database, I>(std::marker::PhantomData<(DB, I)>);

impl<DB: Database, I: Inspector<EthEvmContext<DB>>> Handler for NativeHandler<DB, I> {
    type Evm = Inner<DB, I>;
    type Error = EVMError<DB::Error>;
    type HaltReason = HaltReason;

    /// Derivation supplies identity, so native calls neither validate nor bump
    /// the sender nonce. Keep upstream code and funding checks; the wrapper
    /// has already credited exactly the authorized deposit value.
    fn validate_against_state_and_deduct_caller(
        &self,
        evm: &mut Self::Evm,
        _: &mut InitialAndFloorGas,
    ) -> Result<(), Self::Error> {
        let (block, tx, cfg, journal, _, _) = evm.ctx.all_mut();
        let mut caller = journal
            .load_account_with_code_mut(tx.caller)
            .map_err(EVMError::Database)?;
        pre_execution::validate_account_nonce_and_code(&caller.account().info, 0, false, true)?;
        let balance = pre_execution::calculate_caller_fee(*caller.balance(), tx, block, cfg)?;
        caller.set_balance(balance);
        Ok(())
    }
}

impl<DB: Database, I: Inspector<EthEvmContext<DB>>> InspectorHandler for NativeHandler<DB, I> {
    type IT = EthInterpreter;
}
