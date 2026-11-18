//! Adapts Ethereum execution to EEZ transaction and receipt types.
//!
//! reth's `EthEvmConfig` fixes its primitives to Ethereum types, which cannot
//! represent native type `0x76`. This configuration selects `EezPrimitives` and
//! the EEZ evm2 type family ([`EezEvmTypes`]), preserves the native receipt
//! type, and decodes native transactions in Engine payloads. Ethereum
//! environments, gas accounting and block assembly are reused. L2 block
//! processing keeps EIP-2935 but has no beacon roots, withdrawals or
//! consensus-layer requests ([`block`]). L1 composition only uses transaction
//! simulation, bypassing block processing.
//!
//! Configuration adapted from reth's `EthEvmConfig` at the pinned revision:
//! <https://github.com/paradigmxyz/reth/blob/v2.11.0/crates/ethereum/evm/src/lib.rs>.

use alloy_consensus::{Header, Typed2718};
use alloy_eips::Decodable2718;
use alloy_primitives::{Address, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use eez_primitives::{Block, EezPrimitives, EezTxEnvelope, EezTxType, Receipt};
use reth_chainspec::{ChainSpec, EthChainSpec, EthereumHardforks, Hardforks};
use reth_evm::{
    ConfigureEngineEvm, ConfigureEvm, EvmEnvFor, ExecutableTxIterator, ExecutionCtxFor,
    NextBlockEnvAttributes, ReceiptBuilder, ReceiptBuilderCtx,
};
use reth_evm_ethereum::{
    EthBlockAssembler, EthBlockExecutionCtx, EthBlockExecutorFactory, EthEvmConfig, EthEvmEnv,
};
use reth_primitives_traits::{
    SealedBlock, SealedHeader, SignedTransaction, transaction::error::InvalidTransactionError,
};
use reth_storage_errors::any::AnyError;
use std::{borrow::Cow, fmt, sync::Arc};

mod block;
mod evm;
pub use block::{EezBlockExecutor, EezBlockExecutorFactory};
pub use evm::{EezEvmError, EezEvmExt, EezEvmFactory, EezEvmTx, EezEvmTypes, EezTxHooks};

/// Read the consensus fee destination. Absence retains historical Ethereum fee
/// burning, including Ethereum L1 simulation. A configured value must be an address.
pub fn fee_collector(config: &alloy_genesis::ChainConfig) -> Result<Option<Address>, AnyError> {
    config
        .extra_fields
        .get("feeCollector")
        .map(|value| serde_json::from_value(value.clone()).map_err(AnyError::new))
        .transpose()
}

/// Maximum declared `tx.gas` accepted by the current EVM rules, `None` when uncapped.
/// Under EIP-8037 `tx.gas` is bounded by the total cap, not the execution cap.
#[must_use]
pub fn declared_tx_gas_limit_cap(version: &evm2::Version) -> Option<u64> {
    let cap = version.tx_gas_caps().0;
    (cap != u64::MAX).then_some(cap)
}

/// Enforce L2 transaction support during construction and replay. The underlying
/// EVM is also used for L1 simulation, where blob transactions remain valid.
pub fn ensure_supported_transaction(tx: &EezTxEnvelope) -> Result<(), AnyError> {
    if tx.is_eip4844() {
        return Err(AnyError::new(InvalidTransactionError::Eip4844Disabled));
    }
    Ok(())
}

struct L2WithdrawalsUnsupported {
    count: usize,
}

// Debug matches Display because stateless formats checkpoint executor errors with `{:?}`.
impl fmt::Debug for L2WithdrawalsUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for L2WithdrawalsUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "L2 blocks cannot contain beacon withdrawals (got {})",
            self.count
        )
    }
}

impl std::error::Error for L2WithdrawalsUnsupported {}

/// L2 ETH issuance is authorized only by validated native deposit transactions.
/// Reject Ethereum's beacon-withdrawal mint path everywhere this configuration
/// constructs or replays an L2 block.
fn ensure_no_withdrawals(count: usize) -> Result<(), AnyError> {
    if count != 0 {
        return Err(AnyError::new(L2WithdrawalsUnsupported { count }));
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
    fn build_receipt<T: evm2::EvmTypes>(
        &self,
        ctx: ReceiptBuilderCtx<EezTxType, evm2::TxResult<T>>,
    ) -> Receipt {
        let ReceiptBuilderCtx {
            tx_type,
            result,
            cumulative_gas_used,
        } = ctx;
        Receipt {
            tx_type,
            success: result.status,
            cumulative_gas_used,
            logs: result.logs,
        }
    }
}

/// Copies an environment derived for Ethereum types into the EEZ type family.
/// Both share the spec, runtime version and block environment.
fn eez_env(env: EthEvmEnv) -> EthEvmEnv<EezEvmTypes> {
    EthEvmEnv::new_with_version(env.spec, env.block, env.version)
}

#[derive(Debug, Clone)]
pub struct EezEvmConfig<C = ChainSpec> {
    /// Derives environments and contexts; never executes.
    ethereum: EthEvmConfig<C>,
    executor: EezBlockExecutorFactory<C>,
}

impl<C: EthChainSpec> EezEvmConfig<C> {
    /// Construct the shared Rollup0 execution configuration.
    pub fn new(chain_spec: Arc<C>) -> Result<Self, AnyError> {
        let factory = EezEvmFactory::new(fee_collector(&chain_spec.genesis().config)?);
        Ok(Self {
            ethereum: EthEvmConfig::new(chain_spec.clone()),
            executor: EezBlockExecutorFactory {
                ethereum: EthBlockExecutorFactory::new_with_receipt_builder(
                    EezReceiptBuilder,
                    chain_spec,
                    factory,
                ),
            },
        })
    }

    pub fn chain_spec(&self) -> &Arc<C> {
        self.ethereum.chain_spec()
    }
}

impl<C: EthChainSpec<Header = Header> + EthereumHardforks + Hardforks + 'static> ConfigureEvm
    for EezEvmConfig<C>
{
    type Primitives = EezPrimitives;
    type Error = AnyError;
    type NextBlockEnvCtx = NextBlockEnvAttributes;
    type BlockExecutorFactory = EezBlockExecutorFactory<C>;
    type BlockAssembler = EthBlockAssembler<C>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.ethereum.block_assembler
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnvFor<Self>, Self::Error> {
        let Ok(env) = self.ethereum.evm_env(header);
        Ok(eez_env(env))
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        let Ok(env) = self.ethereum.next_evm_env(parent, attributes);
        Ok(eez_env(env))
    }

    /// Reject blobs on block import and stateless proof replay, then reuse
    /// Ethereum's context fields with EEZ transaction types.
    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error>
    where
        Self: 'a,
    {
        ensure_no_withdrawals(
            block
                .body()
                .withdrawals
                .as_ref()
                .map_or(0, |withdrawals| withdrawals.len()),
        )?;
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
        ensure_no_withdrawals(
            attributes
                .withdrawals
                .as_ref()
                .map_or(0, |withdrawals| withdrawals.len()),
        )?;
        let Ok(ctx) = self.ethereum.context_for_next_block(parent, attributes);
        Ok(ctx)
    }
}

impl<C: EthChainSpec<Header = Header> + EthereumHardforks + Hardforks + 'static>
    ConfigureEngineEvm<ExecutionData> for EezEvmConfig<C>
{
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        let Ok(env) = self.ethereum.evm_env_for_payload(payload);
        Ok(eez_env(env))
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        ensure_no_withdrawals(payload.payload.withdrawals().map_or(0, Vec::len))?;
        let Ok(ctx) = self.ethereum.context_for_payload(payload);
        Ok(ctx)
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
