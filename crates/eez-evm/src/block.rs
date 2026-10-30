//! L2 block processing around Ethereum transaction execution.

use crate::balances::clear_protocol_balances;
use alloy_evm::{
    Evm, EvmFactory, RecoveredTx,
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        BlockValidationError, ExecutableTx, GasOutput, StateDB, TxResult,
    },
    eth::{
        EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthTxResult,
        spec::EthExecutorSpec,
    },
};
use eez_protocol::system_tx::ExecutionGroupState;
use revm::context_interface::Block as _;
use revm::{DatabaseCommit, context::result::ResultAndState};
use revm::{Inspector, primitives::hardfork::SpecId};
use std::sync::Arc;

use crate::{EezEvmFactory, EezReceiptBuilder};
use eez_primitives::{EezTxEnvelope, EezTxType, Receipt};

#[derive(Debug, Clone)]
pub struct EezBlockExecutorFactory<C> {
    pub(crate) ethereum: EthBlockExecutorFactory<EezReceiptBuilder, Arc<C>, EezEvmFactory>,
}

impl<C: EthExecutorSpec + 'static> BlockExecutorFactory for EezBlockExecutorFactory<C> {
    type EvmFactory = EezEvmFactory;
    type ExecutionCtx<'a> = EthBlockExecutionCtx<'a>;
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;
    type TxExecutionResult = EezTxResult<<EezEvmFactory as EvmFactory>::HaltReason>;
    type Executor<'a, DB: StateDB, I: Inspector<<EezEvmFactory as EvmFactory>::Context<DB>>> =
        EezBlockExecutor<'a, <EezEvmFactory as EvmFactory>::Evm<DB, I>, &'a Arc<C>>;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.ethereum.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: <EezEvmFactory as EvmFactory>::Evm<DB, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a, DB, I>
    where
        DB: StateDB,
        I: Inspector<<EezEvmFactory as EvmFactory>::Context<DB>>,
    {
        EezBlockExecutor {
            ethereum: self.ethereum.create_executor(evm, ctx),
            group_state: ExecutionGroupState::default(),
        }
    }
}

#[derive(Debug)]
pub struct EezBlockExecutor<'a, E, Spec> {
    ethereum: EthBlockExecutor<'a, E, Spec, &'a EezReceiptBuilder>,
    group_state: ExecutionGroupState,
}

/// Execution output and its provisional group cursor, committed together.
#[derive(Debug)]
pub struct EezTxResult<H> {
    inner: EthTxResult<H, EezTxType>,
    group_state: ExecutionGroupState,
}
impl<H: Send + 'static> TxResult for EezTxResult<H> {
    type HaltReason = H;
    fn result(&self) -> &ResultAndState<H> {
        &self.inner.result
    }
    fn into_result(self) -> ResultAndState<H> {
        self.inner.result
    }
}

impl<E, Spec> BlockExecutor for EezBlockExecutor<'_, E, Spec>
where
    E: Evm<DB: StateDB, Spec: Into<SpecId> + Clone, Tx = revm::context::TxEnv>,
    Spec: EthExecutorSpec,
{
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;
    type Evm = E;
    type Result = EezTxResult<E::HaltReason>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        if self
            .ethereum
            .ctx
            .parent_beacon_block_root
            .is_some_and(|root| !root.is_zero())
        {
            return Err(
                BlockValidationError::msg("L2 parent beacon block root must be zero").into(),
            );
        }
        // Rollup0 keeps EIP-2935, but has no beacon chain for EIP-4788.
        self.ethereum
            .system_caller
            .apply_blockhashes_contract_call(self.ethereum.ctx.parent_hash, &mut self.ethereum.evm)
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, recovered_tx) = tx.into_parts();
        let (group_state, complete) = self
            .group_state
            .after(recovered_tx.tx())
            .map_err(BlockExecutionError::msg)?;
        if let EezTxEnvelope::System(native) = recovered_tx.tx() {
            let expected = eez_primitives::system_tx_gas_limit(
                self.ethereum.evm.block().gas_limit(),
                self.ethereum.cumulative_tx_gas_used,
            );
            if native.gas_limit != expected || tx_env.gas_limit != expected {
                return Err(BlockValidationError::msg(
                    "native gas does not match execution position",
                )
                .into());
            }
        }
        let mut inner = self
            .ethereum
            .execute_transaction_without_commit((tx_env, recovered_tx))?;
        if complete {
            clear_protocol_balances(self.ethereum.evm.db_mut(), &mut inner.result.state)
                .map_err(|_| BlockExecutionError::msg("cannot load protocol cleanup accounts"))?;
        }
        Ok(EezTxResult { inner, group_state })
    }

    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        self.group_state = output.group_state;
        self.ethereum.commit_transaction(output.inner)
    }

    fn finish(mut self) -> Result<(Self::Evm, BlockExecutionResult<Receipt>), BlockExecutionError> {
        self.group_state
            .ensure_complete()
            .map_err(BlockExecutionError::msg)?;
        let mut cleanup = Default::default();
        clear_protocol_balances(self.ethereum.evm.db_mut(), &mut cleanup)
            .map_err(|_| BlockExecutionError::msg("cannot load protocol cleanup accounts"))?;
        self.ethereum.evm.db_mut().commit(cleanup);
        // L2 has no consensus-layer requests (EIP-6110/7002/7251), beacon
        // withdrawals, or mining rewards. Do not execute those transitions and
        // discard their output: even an empty request list can change state.
        let gas_used = if self.ethereum.evm.cfg_env().enable_amsterdam_eip8037 {
            self.ethereum.max_block_gas_used()
        } else {
            self.ethereum.cumulative_tx_gas_used
        };
        Ok((
            self.ethereum.evm,
            BlockExecutionResult {
                receipts: self.ethereum.receipts,
                requests: Default::default(),
                gas_used,
                blob_gas_used: self.ethereum.blob_gas_used,
            },
        ))
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        self.ethereum.evm_mut()
    }

    fn evm(&self) -> &Self::Evm {
        self.ethereum.evm()
    }

    fn receipts(&self) -> &[Receipt] {
        self.ethereum.receipts()
    }
}
