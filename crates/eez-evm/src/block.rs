//! L2 block processing around Ethereum transaction execution.

use alloy_evm::{
    Evm, EvmFactory, FromRecoveredTx, FromTxWithEncoded,
    block::{
        BlockExecutionError, BlockExecutionResult, BlockExecutor, BlockExecutorFactory,
        BlockValidationError, ExecutableTx, GasOutput, StateDB,
    },
    eth::{
        EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthTxResult,
        spec::EthExecutorSpec,
    },
};
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
    type TxExecutionResult = EthTxResult<<EezEvmFactory as EvmFactory>::HaltReason, EezTxType>;
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
        }
    }
}

#[derive(Debug)]
pub struct EezBlockExecutor<'a, E, Spec> {
    ethereum: EthBlockExecutor<'a, E, Spec, &'a EezReceiptBuilder>,
}

impl<E, Spec> BlockExecutor for EezBlockExecutor<'_, E, Spec>
where
    E: Evm<
            DB: StateDB,
            Spec: Into<SpecId> + Clone,
            Tx: FromRecoveredTx<EezTxEnvelope> + FromTxWithEncoded<EezTxEnvelope>,
        >,
    Spec: EthExecutorSpec,
{
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;
    type Evm = E;
    type Result = EthTxResult<E::HaltReason, EezTxType>;

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
        self.ethereum.execute_transaction_without_commit(tx)
    }

    fn commit_transaction(&mut self, output: Self::Result) -> GasOutput {
        self.ethereum.commit_transaction(output)
    }

    fn finish(self) -> Result<(Self::Evm, BlockExecutionResult<Receipt>), BlockExecutionError> {
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
