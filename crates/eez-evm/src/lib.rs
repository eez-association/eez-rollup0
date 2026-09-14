//! Adapts Ethereum execution to EEZ transaction and receipt types.
//!
//! The pinned `EthEvmConfig` fixes its primitives to Ethereum types, which cannot
//! represent native type `0x76`. This wrapper selects `EezPrimitives`, preserves
//! the native receipt type, and decodes native transactions in Engine payloads.
//! Ethereum's executor, gas accounting, system calls, block assembly, and header
//! environment are reused. `eez-primitives` defines the zero-fee native TxEnv;
//! `evm` adds deposit minting and rollback around the upstream Ethereum EVM.

use alloy_consensus::{Header, Typed2718};
use alloy_eips::Decodable2718;
use alloy_evm::eth::{
    EthBlockExecutionCtx, EthBlockExecutorFactory,
    receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    spec::EthExecutorSpec,
};
use alloy_primitives::Bytes;
use alloy_rpc_types_engine::ExecutionData;
use eez_primitives::{Block, EezPrimitives, EezTxEnvelope, EezTxType, Receipt};
use reth_chainspec::{ChainSpec, EthChainSpec, Hardforks};
use reth_evm::{
    ConfigureEngineEvm, ConfigureEvm, Evm, EvmEnv, EvmEnvFor, ExecutableTxIterator,
    ExecutionCtxFor, NextBlockEnvAttributes,
};
use reth_evm_ethereum::{EthBlockAssembler, EthEvmConfig};
use reth_primitives_traits::{
    SealedBlock, SealedHeader, SignedTransaction, transaction::error::InvalidTransactionError,
};
use reth_storage_errors::any::AnyError;
use std::{borrow::Cow, sync::Arc};

mod evm;
pub use evm::EezEvmFactory;

// Enforce L2 transaction support at block replay boundaries. The underlying EVM
// is also used for L1 simulation, where blob transactions remain valid.
fn ensure_supported_transaction(tx: &EezTxEnvelope) -> Result<(), AnyError> {
    if tx.is_eip4844() {
        return Err(AnyError::new(InvalidTransactionError::Eip4844Disabled));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EezReceiptBuilder;

impl ReceiptBuilder for EezReceiptBuilder {
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;

    /// Preserve native type `0x76` in the receipt root; Ethereum's receipt builder
    /// only accepts its own type enum. Status, gas, and logs are unchanged.
    fn build_receipt<E: Evm>(&self, ctx: ReceiptBuilderCtx<'_, EezTxType, E>) -> Receipt {
        Receipt {
            tx_type: ctx.tx_type,
            success: ctx.result.is_success(),
            cumulative_gas_used: ctx.cumulative_gas_used,
            logs: ctx.result.into_logs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EezEvmConfig<C = ChainSpec> {
    ethereum: EthEvmConfig<C>,
    executor: EthBlockExecutorFactory<EezReceiptBuilder, Arc<C>, EezEvmFactory>,
}

impl<C> EezEvmConfig<C> {
    pub fn new(chain_spec: Arc<C>) -> Self {
        Self {
            ethereum: EthEvmConfig::new(chain_spec.clone()),
            executor: EthBlockExecutorFactory::new(EezReceiptBuilder, chain_spec, EezEvmFactory),
        }
    }

    pub fn chain_spec(&self) -> &Arc<C> {
        self.ethereum.chain_spec()
    }
}

impl<C: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static> ConfigureEvm
    for EezEvmConfig<C>
{
    type Primitives = EezPrimitives;
    type Error = AnyError;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = EthBlockExecutorFactory<EezReceiptBuilder, Arc<C>, EezEvmFactory>;
    type BlockAssembler = EthBlockAssembler<C>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.ethereum.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv, Self::Error> {
        self.ethereum.evm_env(header).map_err(AnyError::new)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv, Self::Error> {
        self.ethereum
            .next_evm_env(parent, attributes)
            .map_err(AnyError::new)
    }

    /// Reject blobs on block import and stateless proof replay, then reuse
    /// Ethereum's context fields with EEZ transaction types.
    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error> {
        block
            .body()
            .transactions
            .iter()
            .try_for_each(ensure_supported_transaction)?;
        Ok(EthBlockExecutionCtx {
            tx_count_hint: Some(block.transaction_count()),
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            ommers: &block.body().ommers,
            withdrawals: block
                .body()
                .withdrawals
                .as_ref()
                .map(|w| Cow::Borrowed(w.as_slice())),
            extra_data: block.header().extra_data.clone(),
            slot_number: block.header().slot_number,
        })
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: NextBlockEnvAttributes,
    ) -> Result<EthBlockExecutionCtx<'_>, Self::Error> {
        self.ethereum
            .context_for_next_block(parent, attributes)
            .map_err(AnyError::new)
    }
}

impl<C: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static>
    ConfigureEngineEvm<ExecutionData> for EezEvmConfig<C>
{
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.ethereum
            .evm_env_for_payload(payload)
            .map_err(AnyError::new)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.ethereum
            .context_for_payload(payload)
            .map_err(AnyError::new)
    }

    /// Decode native envelopes and reject blobs before Engine replay. Native
    /// recovery assigns the protocol sender; derivation and proof checks
    /// establish the call's authority.
    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        let convert = |raw: Bytes| -> Result<_, AnyError> {
            let tx = EezTxEnvelope::decode_2718_exact(&raw).map_err(AnyError::new)?;
            ensure_supported_transaction(&tx)?;
            let signer = tx.try_recover().map_err(AnyError::new)?;
            Ok(tx.with_signer(signer))
        };
        Ok((payload.payload.transactions().clone(), convert))
    }
}

#[cfg(test)]
mod tests;
