//! [`EezPayloadBuilder`] — drop-in replacement for
//! [`reth_node_ethereum::EthereumPayloadBuilder`] that pins the payload
//! builder's `gas_limit` and `extra_data` to the shared
//! [`eez_driver::BUILDER_GAS_LIMIT`] / [`eez_driver::BUILDER_EXTRA_DATA`]
//! constants instead of reading reth's `--builder.gaslimit` /
//! `--builder.extradata` CLI flags. The Deriver's `execute_block`
//! reads the same constants, so the two block-construction paths can't
//! drift via operator misconfiguration.

use alloy_primitives::Bytes;
use eez_driver::{BUILDER_EXTRA_DATA, BUILDER_GAS_LIMIT};
use reth_ethereum_engine_primitives::{EthBuiltPayload, EthPayloadAttributes};
use reth_ethereum_payload_builder::EthereumBuilderConfig;
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes};
use reth_node_api::{FullNodeTypes, NodeTypes, PrimitivesTy, TxTy};
use reth_node_builder::{BuilderContext, PayloadTypes, components::PayloadBuilderBuilder};
use reth_transaction_pool::{PoolTransaction, TransactionPool};

/// Marker type — mirrors `EthereumPayloadBuilder` but uses our constants.
#[derive(Clone, Default, Debug)]
#[non_exhaustive]
pub struct EezPayloadBuilder;

impl<Types, Node, Pool, Evm> PayloadBuilderBuilder<Node, Pool, Evm> for EezPayloadBuilder
where
    Types: NodeTypes<ChainSpec: reth_chainspec::EthereumHardforks, Primitives = EthPrimitives>,
    Node: FullNodeTypes<Types = Types>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TxTy<Node::Types>>>
        + Unpin
        + 'static,
    Evm: ConfigureEvm<Primitives = PrimitivesTy<Types>, NextBlockEnvCtx = NextBlockEnvAttributes>
        + 'static,
    Types::Payload:
        PayloadTypes<BuiltPayload = EthBuiltPayload, PayloadAttributes = EthPayloadAttributes>,
{
    type PayloadBuilder =
        reth_ethereum_payload_builder::EthereumPayloadBuilder<Pool, Node::Provider, Evm>;

    async fn build_payload_builder(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
        evm_config: Evm,
    ) -> eyre::Result<Self::PayloadBuilder> {
        Ok(reth_ethereum_payload_builder::EthereumPayloadBuilder::new(
            ctx.provider().clone(),
            pool,
            evm_config,
            EthereumBuilderConfig::new()
                .with_gas_limit(BUILDER_GAS_LIMIT)
                .with_extra_data(Bytes::from_static(BUILDER_EXTRA_DATA)),
        ))
    }
}
