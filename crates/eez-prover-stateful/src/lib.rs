//! Stateful proof signer for an L1-derived follower.

#![forbid(unsafe_code)]

mod backend;
mod config;

pub use backend::Backend;
pub use config::Config;

use std::sync::Arc;

use eez_proof_signer::ServerConfig;
use reth_chainspec::ChainSpec;
use reth_storage_api::{BlockHashReader, BlockNumReader, HeaderProvider, StateProviderFactory};

/// Serve stateful proofs from an already-running L1-derived follower.
pub async fn serve<P>(
    config: Config,
    provider: P,
    chain_spec: Arc<ChainSpec>,
    shutdown: impl Future<Output = ()>,
) -> eyre::Result<()>
where
    P: BlockHashReader
        + BlockNumReader
        + HeaderProvider<Header = alloy_consensus::Header>
        + StateProviderFactory
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    let Config {
        listen_addr,
        expected_rollup_id,
        expected_l2_system_address,
        attester,
        system_transaction_key,
        limits,
    } = config;
    let backend = Backend::new(provider, chain_spec, expected_l2_system_address);
    eez_proof_signer::serve(
        ServerConfig {
            listen_addr,
            limits,
            expected_rollup_id,
            attester,
            system_transaction_key,
        },
        backend,
        shutdown,
    )
    .await
}
