//! Adapts Ethereum execution to EEZ transaction and receipt types.
//!
//! The pinned `EthEvmConfig` fixes its primitives to Ethereum types, which cannot
//! represent native type `0x76`. This wrapper selects `EezPrimitives`, preserves
//! the native receipt type, and decodes native transactions in Engine payloads.
//! Ethereum's executor, gas accounting, system calls, block assembly, and header
//! environment are reused. `eez-primitives` defines the zero-fee native TxEnv;
//! `evm` adds deposit minting and rollback around the upstream Ethereum EVM.

use alloy_consensus::Header;
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
use reth_primitives_traits::{SealedBlock, SealedHeader, SignedTransaction};
use std::{borrow::Cow, convert::Infallible, sync::Arc};

mod evm;
pub use evm::EezEvmFactory;

#[derive(Debug, Clone, Copy, Default)]
pub struct EezReceiptBuilder;

impl ReceiptBuilder for EezReceiptBuilder {
    type Transaction = EezTxEnvelope;
    type Receipt = Receipt;

    /// Uses the same status, cumulative gas, and logs as `RethReceiptBuilder`,
    /// but accepts `EezTxType` and returns `EthereumReceipt<EezTxType>`. The
    /// upstream builder fixes both to Ethereum's type enum, which cannot carry
    /// `0x76`; preserving that type is required for the consensus receipt root.
    fn build_receipt<E: Evm>(&self, ctx: ReceiptBuilderCtx<'_, EezTxType, E>) -> Receipt {
        Receipt {
            tx_type: ctx.tx_type,
            success: ctx.result.is_success(),
            cumulative_gas_used: ctx.cumulative_gas_used,
            logs: ctx.result.into_logs(),
        }
    }
}

/// Selects EEZ primitives while delegating Ethereum execution rules to the
/// upstream config. Its primitive and receipt types are fixed upstream, so they
/// cannot be changed by configuring `EthEvmConfig` alone.
#[derive(Debug, Clone)]
pub struct EezEvmConfig<C = ChainSpec> {
    ethereum: EthEvmConfig<C>,
    executor: EthBlockExecutorFactory<EezReceiptBuilder, Arc<C>, EezEvmFactory>,
}

impl<C> EezEvmConfig<C> {
    /// Uses Ethereum's executor with the EEZ minting wrapper and receipt builder so
    /// it accepts native transactions. The upstream config supplies the shared
    /// header environment and block assembler unchanged.
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
    type Error = Infallible;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = EthBlockExecutorFactory<EezReceiptBuilder, Arc<C>, EezEvmFactory>;
    type BlockAssembler = EthBlockAssembler<C>;

    /// Selects the executor with EEZ transaction/receipt types; returning the
    /// inner Ethereum config's factory would restore its Ethereum-only types.
    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.ethereum.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv, Self::Error> {
        self.ethereum.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv, Self::Error> {
        self.ethereum.next_evm_env(parent, attributes)
    }

    /// Copies the upstream context fields without changing their meaning. This
    /// cannot delegate to `EthEvmConfig::context_for_block` because that method
    /// accepts an Ethereum block, whereas this block contains `EezTxEnvelope`s.
    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error> {
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
        self.ethereum.context_for_next_block(parent, attributes)
    }
}

impl<C: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static>
    ConfigureEngineEvm<ExecutionData> for EezEvmConfig<C>
{
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.ethereum.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.ethereum.context_for_payload(payload)
    }

    /// Decodes Engine transactions as EEZ envelopes so imports can include
    /// `0x76`, which the upstream Ethereum decoder rejects. EEZ signer recovery
    /// returns the fixed system sender for native transactions and performs
    /// ordinary signature recovery for Ethereum transactions. This identifies
    /// the sender; canonical derivation and proof checks establish authorization.
    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        let convert = |raw: Bytes| -> Result<_, reth_storage_errors::any::AnyError> {
            let tx = EezTxEnvelope::decode_2718_exact(&raw)
                .map_err(reth_storage_errors::any::AnyError::new)?;
            let signer = tx
                .try_recover()
                .map_err(reth_storage_errors::any::AnyError::new)?;
            Ok(tx.with_signer(signer))
        };
        Ok((payload.payload.transactions().clone(), convert))
    }
}

#[cfg(test)]
mod tests;
