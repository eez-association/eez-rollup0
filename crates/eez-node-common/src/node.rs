//! Shared L2 node configuration. L1 nodes retain their upstream Ethereum/Gnosis types.
//!
//! Component wiring adapted from reth's Ethereum node at the pinned revision:
//! <https://github.com/paradigmxyz/reth/blob/v2.7.0/crates/ethereum/node/src/node.rs>.
//! Engine validation delegates to:
//! <https://github.com/paradigmxyz/reth/blob/v2.7.0/crates/ethereum/node/src/engine.rs>.

use crate::{EezNetworkBuilder, EezPayloadBuilder, EezPoolBuilder, HeldTxs};
use alloy_rpc_types_engine::{ExecutionData, PayloadAttributes};
use alloy_rpc_types_eth::TransactionRequest;
use eez_engine::EezEngineTypes;
use eez_primitives::{Block, EezPrimitives, EezTxEnvelope};
use reth_chainspec::ChainSpec;
use reth_engine_primitives::{EngineApiValidator, PayloadValidator};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_payload_builder::EthereumExecutionPayloadValidator;
use reth_evm::{ConfigureEngineEvm, NextBlockEnvAttributes};
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
use reth_node_ethereum::EthereumEngineValidator;
use reth_payload_primitives::{
    EngineApiMessageVersion, EngineObjectValidationError, NewPayloadError, PayloadOrAttributes,
};
use reth_primitives_traits::SealedBlock;
use reth_provider::EthStorage;
use reth_rpc_convert::{EthTxEnvError, transaction::TxEnvConverter};
use std::{future::ready, sync::Arc};

/// Execution selected by the binary and shared by import, payloads and derivation.
pub trait NodeEvm:
    ConfigureEngineEvm<
        ExecutionData,
        Primitives = EezPrimitives,
        NextBlockEnvCtx = NextBlockEnvAttributes,
    > + 'static
{
}
impl<T> NodeEvm for T where
    T: ConfigureEngineEvm<
            ExecutionData,
            Primitives = EezPrimitives,
            NextBlockEnvCtx = NextBlockEnvAttributes,
        > + 'static
{
}

/// The L2 node. A composer's pool also refuses the cross-chain txs it holds.
#[derive(Debug, Clone)]
pub struct EezNode<Evm, TxEnv = ()> {
    pub tx_env_converter: TxEnv,
    pub evm: Evm,
    pub held: Option<HeldTxs>,
}

impl<Evm: NodeEvm, TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default>
    NodeTypes for EezNode<Evm, TxEnv>
{
    type Primitives = EezPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage<EezTxEnvelope>;
    type Payload = EezEngineTypes;
}

impl<Evm: NodeEvm, TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default>
    EezNode<Evm, TxEnv>
{
    pub fn components<N: FullNodeTypes<Types = Self>>(
        &self,
    ) -> ComponentsBuilder<
        N,
        EezPoolBuilder,
        BasicPayloadServiceBuilder<EezPayloadBuilder>,
        EezNetworkBuilder,
        EezExecutorBuilder<Evm>,
        EezConsensusBuilder,
    > {
        ComponentsBuilder::default()
            .node_types::<N>()
            .pool(EezPoolBuilder::new(
                eez_primitives::SYSTEM_ADDRESS,
                self.held.clone(),
            ))
            .executor(EezExecutorBuilder(self.evm.clone()))
            .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder::default()))
            .network(EezNetworkBuilder)
            .consensus(EezConsensusBuilder)
    }
}

impl<
    Evm: NodeEvm,
    TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default,
    N: FullNodeComponents<Types = Self>,
> DebugNode<N> for EezNode<Evm, TxEnv>
where
    reth_rpc_eth_types::EthApiError: reth_rpc_eth_types::error::FromEvmError<Evm>,
{
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
pub struct EezExecutorBuilder<Evm>(Evm);

impl<
    Evm: NodeEvm,
    TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default,
    N: FullNodeTypes<Types = EezNode<Evm, TxEnv>>,
> ExecutorBuilder<N> for EezExecutorBuilder<Evm>
{
    type EVM = Evm;

    fn build_evm(self, ctx: &BuilderContext<N>) -> impl Future<Output = eyre::Result<Self::EVM>> {
        // EezEvmFactory has no JIT backend; refuse `--jit` rather than ignore it.
        if ctx.config().jit.enabled {
            return ready(Err(eyre::eyre!(
                "--jit is not supported: the L2 EVM (EezEvmFactory) has no JIT backend"
            )));
        }
        ready(Ok(self.0))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EezConsensusBuilder;

impl<
    Evm: NodeEvm,
    TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default,
    N: FullNodeTypes<Types = EezNode<Evm, TxEnv>>,
> ConsensusBuilder<N> for EezConsensusBuilder
{
    type Consensus = Arc<EthBeaconConsensus<ChainSpec>>;

    fn build_consensus(
        self,
        ctx: &BuilderContext<N>,
    ) -> impl Future<Output = eyre::Result<Self::Consensus>> {
        ready(Ok(Arc::new(EthBeaconConsensus::new(ctx.chain_spec()))))
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

impl<
    Evm: NodeEvm,
    TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default,
    N: FullNodeComponents<Types = EezNode<Evm, TxEnv>>,
> PayloadValidatorBuilder<N> for EezEngineValidatorBuilder
{
    type Validator = EezEngineValidator;

    fn build(
        self,
        ctx: &AddOnsContext<'_, N>,
    ) -> impl Future<Output = eyre::Result<Self::Validator>> {
        ready(Ok(EezEngineValidator::new(ctx.config.chain.clone())))
    }
}

impl<
    Evm: NodeEvm,
    TxEnv: TxEnvConverter<TransactionRequest, Evm, Error = EthTxEnvError> + Default,
    N: FullNodeTypes<Types = Self>,
> reth_node_builder::Node<N> for EezNode<Evm, TxEnv>
where
    reth_rpc_eth_types::EthApiError: reth_rpc_eth_types::error::FromEvmError<Evm>,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        EezPoolBuilder,
        BasicPayloadServiceBuilder<EezPayloadBuilder>,
        EezNetworkBuilder,
        EezExecutorBuilder<Evm>,
        EezConsensusBuilder,
    >;
    type AddOns = reth_node_builder::rpc::RpcAddOns<
        reth_node_builder::NodeAdapter<N>,
        crate::rpc::EezEthApiBuilder<TxEnv>,
        EezEngineValidatorBuilder,
    >;

    fn components_builder(&self) -> Self::ComponentsBuilder {
        self.components()
    }

    fn add_ons(&self) -> Self::AddOns {
        crate::rpc::eez_add_ons(self.tx_env_converter.clone())
    }
}
