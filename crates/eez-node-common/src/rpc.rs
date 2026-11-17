//! Ethereum RPC methods with EEZ transaction and receipt response types.
//!
//! RPC wiring adapted from reth's Ethereum node at the pinned revision:
//! <https://github.com/paradigmxyz/reth/blob/v2.11.0/crates/ethereum/node/src/node.rs>.
//! Receipt/log conversion adapted from `EthReceiptConverter`:
//! <https://github.com/paradigmxyz/reth/blob/v2.11.0/crates/rpc/rpc-eth-types/src/receipt.rs>.

use crate::node::{EezEngineValidatorBuilder, EezNode};
use alloy_consensus::transaction::Recovered;
use alloy_consensus::{Receipt as RpcReceipt, TxReceipt};
use alloy_consensus_any::AnyReceiptEnvelope;
use alloy_rpc_types_eth::{Header, Log, Transaction, TransactionReceipt, TransactionRequest};
use eez_evm::{EezEvmConfig, EezEvmTx};
use eez_primitives::{EezTxEnvelope, Receipt};
use evm2::ethereum::RecoveredTxEnvelope;
use reth_chainspec::{ChainSpec, ChainSpecProvider};
use reth_evm::{EvmEnvFor, TxEnvFor};
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::{
    BasicEngineApiBuilder, BasicEngineValidatorBuilder, EthApiBuilder, EthApiCtx, Identity,
    RpcAddOns, RpcNodeCoreFor,
};
use reth_primitives_traits::TransactionMeta;
use reth_rpc::EthApi;
use reth_rpc_convert::{
    EthTxEnvError, RpcConverter, RpcTypes, TryIntoTxEnv, transaction::TxEnvConverter,
};
use reth_rpc_eth_types::receipt::EthReceiptConverter;
use std::future::ready;

#[derive(Debug, Clone)]
pub struct EezRpcTypes;

impl RpcTypes for EezRpcTypes {
    type Header = Header;
    type Receipt = TransactionReceipt<AnyReceiptEnvelope<Log>>;
    type Log = Log;
    type TransactionResponse = Transaction<EezTxEnvelope>;
    type TransactionRequest = TransactionRequest;
}

type ReceiptBuilder = fn(Receipt, usize, TransactionMeta) -> AnyReceiptEnvelope<Log>;
type Converter = RpcConverter<
    EezRpcTypes,
    EezEvmConfig,
    EthReceiptConverter<ChainSpec, ReceiptBuilder>,
    (),
    (),
    (),
    (),
    EezTxEnvConverter,
>;

/// Builds call and estimate environments from RPC requests. Requests are always
/// Ethereum transactions; native `0x76` transactions never come from RPC.
#[derive(Debug, Clone, Copy, Default)]
pub struct EezTxEnvConverter;

impl TxEnvConverter<TransactionRequest, EezEvmConfig> for EezTxEnvConverter {
    type Error = EthTxEnvError;

    fn convert_tx_env(
        &self,
        tx_req: TransactionRequest,
        evm_env: &EvmEnvFor<EezEvmConfig>,
    ) -> Result<TxEnvFor<EezEvmConfig>, Self::Error> {
        let tx: RecoveredTxEnvelope = tx_req.try_into_tx_env(evm_env)?;
        let signer = tx.signer();
        Ok(Recovered::new_unchecked(
            EezEvmTx::Ethereum(tx.into_inner()),
            signer,
        ))
    }
}

#[derive(Debug, Default)]
pub struct EezEthApiBuilder;

impl<N: FullNodeComponents<Types = EezNode, Evm = EezEvmConfig>> EthApiBuilder<N>
    for EezEthApiBuilder
{
    type EthApi = EthApi<RpcNodeCoreFor<N>, Converter>;

    fn build_eth_api(
        self,
        ctx: EthApiCtx<'_, N>,
    ) -> impl Future<Output = eyre::Result<Self::EthApi>> {
        let receipts = EthReceiptConverter::new(ctx.components.provider().chain_spec())
            .with_builder(build_receipt as ReceiptBuilder);
        ready(Ok(ctx
            .eth_api_builder()
            .map_converter(|_| RpcConverter::new(receipts).with_tx_env_converter(EezTxEnvConverter))
            .build()))
    }
}

fn build_receipt(
    receipt: Receipt,
    next_log_index: usize,
    meta: TransactionMeta,
) -> AnyReceiptEnvelope<Log> {
    let logs_bloom = receipt.bloom();
    let r#type = receipt.tx_type.into();
    let logs = receipt
        .logs
        .into_iter()
        .enumerate()
        .map(|(index, inner)| Log {
            inner,
            block_hash: Some(meta.block_hash),
            block_number: Some(meta.block_number),
            block_timestamp: Some(meta.timestamp),
            transaction_hash: Some(meta.tx_hash),
            transaction_index: Some(meta.index),
            log_index: Some((next_log_index + index) as u64),
            removed: false,
        })
        .collect();
    AnyReceiptEnvelope {
        r#type,
        inner: alloy_consensus::ReceiptWithBloom {
            logs_bloom,
            receipt: RpcReceipt {
                status: receipt.success.into(),
                cumulative_gas_used: receipt.cumulative_gas_used,
                logs,
            },
        },
    }
}

pub fn eez_add_ons<N: FullNodeComponents<Types = EezNode, Evm = EezEvmConfig>>()
-> RpcAddOns<N, EezEthApiBuilder, EezEngineValidatorBuilder> {
    RpcAddOns::new(
        EezEthApiBuilder,
        EezEngineValidatorBuilder,
        BasicEngineApiBuilder::default(),
        BasicEngineValidatorBuilder::default(),
        Default::default(),
        Identity::new(),
    )
}
