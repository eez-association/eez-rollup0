//! L2 block processing around Ethereum transaction execution.
//!
//! The executor is reth's `EthBlockExecutor` with its block-level hooks pinned
//! to the Merge: L2 has no beacon chain, so no withdrawals, beacon roots or
//! consensus-layer requests. EIP-2935 block-hash history runs by hand.

use alloy_eip7928::{BlockAccessIndex, BlockAccessList};
use evm2::{
    SpecId,
    evm::{Bal, HISTORY_STORAGE_ADDRESS, SystemTx},
};
use reth_evm::{
    BlockExecutionError, BlockExecutionOutput, BlockExecutor, BlockExecutorFactory,
    BlockValidationError, BundleState, CommitChanges, ExecutorTx, GasOutput, StateUpdate,
};
use reth_evm_ethereum::{
    EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthEvmEnv,
};
use reth_execution_types::BlockState;
use std::sync::Arc;

use crate::{EezEvmFactory, EezEvmTypes, EezReceiptBuilder};
use eez_primitives::{EezTxEnvelope, Receipt};

#[derive(Debug, Clone)]
pub struct EezBlockExecutorFactory<C> {
    pub(crate) ethereum: EthBlockExecutorFactory<EezReceiptBuilder, C, EezEvmFactory>,
}

impl<C> BlockExecutorFactory for EezBlockExecutorFactory<C>
where
    C: reth_chainspec::EthChainSpec<Header = alloy_consensus::Header>
        + reth_chainspec::EthereumHardforks,
{
    type EvmFactory = EezEvmFactory;
    type EvmTypes = EezEvmTypes;
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;
    type Evm<'a> = evm2::Evm<'a, EezEvmTypes>;
    type EvmEnv = EthEvmEnv<EezEvmTypes>;
    type ExecutionCtx<'a>
        = EthBlockExecutionCtx<'a>
    where
        Self: 'a;
    type Executor<'a>
        = EezBlockExecutor<'a>
    where
        Self: 'a;

    fn create_executor<'a>(
        &'a self,
        evm: Self::Evm<'a>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> Self::Executor<'a>
    where
        Self: 'a,
    {
        let mut ethereum = self.ethereum.create_executor(evm, ctx);
        // Block-level hooks only; transactions still follow the block's own fork.
        ethereum.set_spec_id(SpecId::MERGE);
        EezBlockExecutor { ethereum }
    }

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.ethereum.evm_factory()
    }

    fn evm_with_env<'a, DB>(&self, db: DB, evm_env: Self::EvmEnv) -> Self::Evm<'a>
    where
        DB: reth_evm::DynDatabase + 'a,
    {
        self.ethereum.evm_with_env(db, evm_env)
    }
}

pub struct EezBlockExecutor<'a> {
    ethereum: EthBlockExecutor<'a, EezEvmTypes, &'a EezReceiptBuilder>,
}

impl std::fmt::Debug for EezBlockExecutor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EezBlockExecutor").finish_non_exhaustive()
    }
}

impl EezBlockExecutor<'_> {
    /// The block's state changes so far. Clone it to finalize a mid-block snapshot.
    pub const fn block_state(&self) -> &BlockState {
        self.ethereum.block_state()
    }
}

impl<'a> BlockExecutor for EezBlockExecutor<'a> {
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;
    type Evm = evm2::Evm<'a, EezEvmTypes>;
    type TransactionResultWithState =
        <EthBlockExecutor<'a, EezEvmTypes, &'a EezReceiptBuilder> as BlockExecutor>::TransactionResultWithState;
    type BlockAccessList = Bal;

    fn evm(&self) -> &Self::Evm {
        self.ethereum.evm()
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        self.ethereum.evm_mut()
    }

    fn set_state_hook(&mut self, hook: impl FnMut(StateUpdate) + Send + 'static) -> bool {
        self.ethereum.set_state_hook(hook)
    }

    fn convert_block_access_list(
        block_access_list: &BlockAccessList,
    ) -> Result<Self::BlockAccessList, BlockExecutionError> {
        <EthBlockExecutor<'a, EezEvmTypes, &'a EezReceiptBuilder> as BlockExecutor>::convert_block_access_list(
            block_access_list,
        )
    }

    fn set_block_access_list(&mut self, block_access_list: Arc<Self::BlockAccessList>) {
        self.ethereum.set_block_access_list(block_access_list);
    }

    fn set_block_access_index(&mut self, index: BlockAccessIndex) {
        self.ethereum.set_block_access_index(index);
    }

    fn enable_block_access_list_builder(&mut self) {
        self.ethereum.enable_block_access_list_builder();
    }

    fn take_block_access_list(&mut self) -> Option<BlockAccessList> {
        self.ethereum.take_block_access_list()
    }

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        let ctx = self.ethereum.context();
        if ctx
            .parent_beacon_block_root
            .is_some_and(|root| !root.is_zero())
        {
            return Err(
                BlockValidationError::msg("L2 parent beacon block root must be zero").into(),
            );
        }
        let parent_hash = ctx.parent_hash;
        // Pinned to the Merge, this runs no system calls.
        self.ethereum.apply_pre_execution_changes()?;
        // EIP-2935, gated as Ethereum gates it.
        let evm = self.ethereum.evm_mut();
        if evm.spec_id().enables(SpecId::PRAGUE) && !evm.block().number.is_zero() {
            let outcome = evm
                .system_call(SystemTx::new(HISTORY_STORAGE_ADDRESS, parent_hash.0.into()))
                .map_err(BlockExecutionError::other)?
                .detach();
            self.ethereum.commit_pending_state(&outcome.pending_state);
        }
        Ok(())
    }

    fn into_state(self) -> BundleState {
        self.ethereum.into_state()
    }

    fn validate_transaction_gas_limit(
        &mut self,
        gas_limit: u64,
    ) -> Result<(), BlockExecutionError> {
        self.ethereum.validate_transaction_gas_limit(gas_limit)
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        transaction: impl ExecutorTx<Self>,
        f: impl FnOnce(&evm2::TxResult<EezEvmTypes>, evm2::TxStateChanges<'_>) -> CommitChanges,
    ) -> Result<Option<GasOutput>, BlockExecutionError> {
        self.ethereum
            .execute_transaction_with_commit_condition(transaction, f)
    }

    fn execute_transaction_without_commit(
        &mut self,
        transaction: impl ExecutorTx<Self>,
    ) -> Result<Self::TransactionResultWithState, BlockExecutionError> {
        self.ethereum
            .execute_transaction_without_commit(transaction)
    }

    fn commit_transaction(
        &mut self,
        output: Self::TransactionResultWithState,
    ) -> Result<GasOutput, BlockExecutionError> {
        self.ethereum.commit_transaction(output)
    }

    fn receipts(&self) -> &[Receipt] {
        self.ethereum.receipts()
    }

    fn finish_with_block_access_list(
        self,
    ) -> Result<(BlockExecutionOutput<Receipt>, Option<BlockAccessList>), BlockExecutionError> {
        self.ethereum.finish_with_block_access_list()
    }

    fn finish_with_prepared_block_access_list(
        self,
    ) -> Result<(BlockExecutionOutput<Receipt>, Option<Bal>), BlockExecutionError> {
        self.ethereum.finish_with_prepared_block_access_list()
    }
}
