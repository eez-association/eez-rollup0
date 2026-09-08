//! Shared L2 node configuration. L1 nodes retain their upstream Ethereum/Gnosis types.
use crate::{EezPayloadBuilder, EezPoolBuilder};
use alloy_rpc_types_engine::{ExecutionData, PayloadAttributes};
use eez_evm::EezEvmConfig;
use eez_primitives::{Block, EezPrimitives, EezTxEnvelope, engine::EezEngineTypes};
use reth_chainspec::ChainSpec;
use reth_engine_primitives::{EngineApiValidator, PayloadValidator};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_payload_builder::EthereumExecutionPayloadValidator;
use reth_node_api::{
    AddOnsContext, FullNodeComponents, FullNodeTypes, NodeTypes, PayloadAttributesBuilder,
};
use reth_node_builder::{
    BuilderContext, DebugNode,
    components::{
        BasicPayloadServiceBuilder, ComponentsBuilder, ConsensusBuilder, ExecutorBuilder,
    },
    rpc::PayloadValidatorBuilder,
};
use reth_node_ethereum::{EthereumEngineValidator, node::EthereumNetworkBuilder};
use reth_payload_primitives::{
    EngineApiMessageVersion, EngineObjectValidationError, NewPayloadError, PayloadOrAttributes,
};
use reth_primitives_traits::SealedBlock;
use reth_provider::EthStorage;
use std::sync::Arc;

#[derive(Debug, Default, Clone, Copy)]
pub struct EezNode;
impl NodeTypes for EezNode {
    type Primitives = EezPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage<EezTxEnvelope>;
    type Payload = EezEngineTypes;
}
impl EezNode {
    pub fn components<N: FullNodeTypes<Types = Self>>() -> ComponentsBuilder<
        N,
        EezPoolBuilder,
        BasicPayloadServiceBuilder<EezPayloadBuilder>,
        EthereumNetworkBuilder,
        EezExecutorBuilder,
        EezConsensusBuilder,
    > {
        ComponentsBuilder::default()
            .node_types::<N>()
            .pool(EezPoolBuilder::new(eez_primitives::SYSTEM_ADDRESS))
            .executor(EezExecutorBuilder)
            .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder::default()))
            .network(EthereumNetworkBuilder::default())
            .consensus(EezConsensusBuilder)
    }
}
impl<N: FullNodeComponents<Types = Self>> DebugNode<N> for EezNode {
    type RpcBlock = alloy_rpc_types_eth::Block<alloy_rpc_types_eth::Transaction<EezTxEnvelope>>;
    fn rpc_to_primitive_block(block: Self::RpcBlock) -> Block {
        block
            .into_consensus()
            .map_transactions(|tx| tx.inner.into_inner())
    }
    fn local_payload_attributes_builder(
        chain_spec: &ChainSpec,
    ) -> impl PayloadAttributesBuilder<PayloadAttributes> {
        reth_engine_local::LocalPayloadAttributesBuilder::new(Arc::new(chain_spec.clone()))
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct EezExecutorBuilder;
impl<N: FullNodeTypes<Types = EezNode>> ExecutorBuilder<N> for EezExecutorBuilder {
    type EVM = EezEvmConfig;
    async fn build_evm(self, ctx: &BuilderContext<N>) -> eyre::Result<Self::EVM> {
        Ok(EezEvmConfig::new(ctx.chain_spec()))
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct EezConsensusBuilder;
impl<N: FullNodeTypes<Types = EezNode>> ConsensusBuilder<N> for EezConsensusBuilder {
    type Consensus = Arc<EthBeaconConsensus<ChainSpec>>;
    async fn build_consensus(self, ctx: &BuilderContext<N>) -> eyre::Result<Self::Consensus> {
        Ok(Arc::new(EthBeaconConsensus::new(ctx.chain_spec())))
    }
}
#[derive(Debug, Clone)]
pub struct EezEngineValidator {
    payload: EthereumExecutionPayloadValidator<ChainSpec>,
    api: EthereumEngineValidator<ChainSpec>,
}
impl EezEngineValidator {
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            payload: EthereumExecutionPayloadValidator::new(chain_spec.clone()),
            api: EthereumEngineValidator::new(chain_spec),
        }
    }
}
impl PayloadValidator<EezEngineTypes> for EezEngineValidator {
    type Block = Block;
    fn convert_payload_to_block(
        &self,
        payload: ExecutionData,
    ) -> Result<SealedBlock<Block>, NewPayloadError> {
        self.payload
            .ensure_well_formed_payload(payload)
            .map_err(Into::into)
    }
}
impl EngineApiValidator<EezEngineTypes> for EezEngineValidator {
    fn validate_version_specific_fields(
        &self,
        version: EngineApiMessageVersion,
        payload: PayloadOrAttributes<'_, ExecutionData, PayloadAttributes>,
    ) -> Result<(), EngineObjectValidationError> {
        <EthereumEngineValidator as EngineApiValidator<EezEngineTypes>>::validate_version_specific_fields(&self.api, version, payload)
    }
    fn ensure_well_formed_attributes(
        &self,
        version: EngineApiMessageVersion,
        attrs: &PayloadAttributes,
    ) -> Result<(), EngineObjectValidationError> {
        <EthereumEngineValidator as EngineApiValidator<EezEngineTypes>>::ensure_well_formed_attributes(&self.api, version, attrs)
    }
}
#[derive(Debug, Default, Clone)]
pub struct EezEngineValidatorBuilder;
impl<N: FullNodeComponents<Types = EezNode>> PayloadValidatorBuilder<N>
    for EezEngineValidatorBuilder
{
    type Validator = EezEngineValidator;
    async fn build(self, ctx: &AddOnsContext<'_, N>) -> eyre::Result<Self::Validator> {
        Ok(EezEngineValidator::new(ctx.config.chain.clone()))
    }
}

impl<N: FullNodeTypes<Types = Self>> reth_node_builder::Node<N> for EezNode {
    type ComponentsBuilder = ComponentsBuilder<
        N,
        EezPoolBuilder,
        BasicPayloadServiceBuilder<EezPayloadBuilder>,
        EthereumNetworkBuilder,
        EezExecutorBuilder,
        EezConsensusBuilder,
    >;
    type AddOns = reth_node_builder::rpc::RpcAddOns<
        reth_node_builder::NodeAdapter<N>,
        crate::rpc::EezEthApiBuilder,
        EezEngineValidatorBuilder,
    >;
    fn components_builder(&self) -> Self::ComponentsBuilder {
        Self::components()
    }
    fn add_ons(&self) -> Self::AddOns {
        crate::rpc::eez_add_ons()
    }
}
