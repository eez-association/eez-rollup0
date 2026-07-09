//! EEZ-specific witness RPC.
//!
//! Reth's stock `debug_executionWitness` proves the parent->final block root.
//! EEZ also proves selected intermediate per-transaction roots, so the archive
//! node must serve the same augmented witness shape as the live prover feed.

use alloy_consensus::Header;
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use jsonrpsee::core::server::RpcModule;
use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::ConfigureEvm;
use reth_node_api::FullNodeComponents;
use reth_node_builder::rpc::RpcContext;
use reth_rpc_eth_api::EthApiTypes;
use reth_storage_api::{BlockReader, HeaderProvider, StateProviderFactory, TransactionVariant};
use std::str::FromStr;
use tracing::{Level, event};

#[derive(Clone)]
struct AugmentedWitnessRpc<P, E> {
    provider: P,
    evm_config: E,
}

/// Register `eez_executionWitnessAugmented(block)` on the L2 node RPC server.
///
/// The method returns the standard `ExecutionWitness` JSON shape
/// `{state,codes,keys,headers}`, but generated through
/// `eez_driver::witness::block_execution_witness`, not stock reth
/// `debug_executionWitness`.
pub fn install_augmented_witness_rpc<Node, EthApi>(
    ctx: RpcContext<'_, Node, EthApi>,
) -> eyre::Result<()>
where
    Node: FullNodeComponents,
    Node::Provider: BlockReader<Block = Block>
        + HeaderProvider<Header = Header>
        + StateProviderFactory
        + Clone
        + Send
        + Sync
        + 'static,
    Node::Evm: ConfigureEvm<Primitives = EthPrimitives> + Clone + Send + Sync + 'static,
    EthApi: EthApiTypes + 'static,
{
    let rpc = AugmentedWitnessRpc {
        provider: ctx.node().provider().clone(),
        evm_config: ctx.node().evm_config().clone(),
    };
    let mut module = RpcModule::new(rpc);

    module.register_async_method(
        "eez_executionWitnessAugmented",
        |params: jsonrpsee::types::Params<'static>, rpc, _ext| async move {
            let block_param: String = params.one().map_err(|e| {
                ErrorObject::owned(
                    -32602,
                    format!("invalid eez_executionWitnessAugmented params: {e}"),
                    None::<()>,
                )
            })?;
            let block_id = parse_block_hash_or_number(&block_param)?;
            let provider = rpc.provider.clone();
            let evm_config = rpc.evm_config.clone();

            tokio::task::spawn_blocking(move || {
                let block = provider
                    .recovered_block(block_id, TransactionVariant::WithHash)
                    .map_err(internal_error)?
                    .ok_or_else(|| {
                        ErrorObject::owned(
                            -32001,
                            format!("eez_executionWitnessAugmented: block {block_param} not found"),
                            None::<()>,
                        )
                    })?;
                eez_driver::witness::block_execution_witness(
                    &provider,
                    &evm_config,
                    &block,
                    eez_driver::witness::ExecutionWitnessMode::Legacy,
                )
                .map_err(internal_error)
            })
            .await
            .map_err(internal_error)?
        },
    )?;

    ctx.modules.merge_configured(module)?;
    event!(
        name: "eez.node.witness_rpc.installed",
        Level::INFO,
        method = "eez_executionWitnessAugmented",
        "EEZ augmented witness RPC installed for durable prover backfill",
    );
    Ok(())
}

fn parse_block_hash_or_number(value: &str) -> Result<BlockHashOrNumber, ErrorObjectOwned> {
    if value.len() == 66 {
        let hash = B256::from_str(value).map_err(|e| {
            ErrorObject::owned(
                -32602,
                format!("invalid block hash for eez_executionWitnessAugmented: {e}"),
                None::<()>,
            )
        })?;
        return Ok(BlockHashOrNumber::Hash(hash));
    }

    let hex = value.strip_prefix("0x").ok_or_else(|| {
        ErrorObject::owned(
            -32602,
            "eez_executionWitnessAugmented expects a hex block number or block hash".to_string(),
            None::<()>,
        )
    })?;
    let number = u64::from_str_radix(hex, 16).map_err(|e| {
        ErrorObject::owned(
            -32602,
            format!("invalid block number for eez_executionWitnessAugmented: {e}"),
            None::<()>,
        )
    })?;
    Ok(BlockHashOrNumber::Number(number))
}

fn internal_error(error: impl std::fmt::Display) -> ErrorObjectOwned {
    ErrorObject::owned(-32000, error.to_string(), None::<()>)
}
