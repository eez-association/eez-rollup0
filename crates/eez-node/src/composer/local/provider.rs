//! Reth-specific chain-provider abstractions shared by the local
//! client and the execution session.
//!
//! [`ChainProvider`] bundles the three reth handles every EVM
//! simulation needs:
//!
//! - type-erased `StateProviderFactory` (for opening a state snapshot)
//! - [`HeaderSource`] (concrete enum over the node's two provider
//!   families; `HeaderProvider` has generic methods that block direct
//!   `dyn` use)
//! - `EthEvmConfig` (for building EVM envs from headers)
//!
//! Held once per rollup inside [`crate::composer::local::LocalChainClient`]
//! and cloned cheaply per execution-session open.

use std::sync::Arc;

use reth_evm_ethereum::EthEvmConfig;
use reth_storage_api::{BlockNumReader, HeaderProvider, StateProviderFactory};

use super::gnosis_adapter::GnosisL1Adapter;
use crate::{ChiadoNodeProvider, EthNodeProvider};

/// Concrete header source over the two provider families the node runs.
/// (`HeaderProvider` has generic methods that prevent `dyn HeaderProvider`;
/// this enum replaces the old dyn-compatible `HeaderReader` seam.)
#[derive(Clone)]
pub enum HeaderSource {
    /// `EthereumNode`-backed provider (the L2 node and the embedded dev L1).
    Eth(EthNodeProvider),
    /// Chiado L1 provider behind the Gnosis header shim.
    Chiado(GnosisL1Adapter<ChiadoNodeProvider>),
}

impl HeaderSource {
    /// Look up a block header by number. Returns `Ok(None)` if the
    /// block does not exist.
    pub fn header_by_number(
        &self,
        num: u64,
    ) -> Result<Option<alloy_consensus::Header>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Eth(p) => HeaderProvider::header_by_number(p, num).map_err(|e| Box::new(e) as _),
            Self::Chiado(p) => {
                HeaderProvider::header_by_number(p, num).map_err(|e| Box::new(e) as _)
            }
        }
    }

    /// Highest known block number.
    pub fn best_block_number(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            Self::Eth(p) => BlockNumReader::best_block_number(p).map_err(|e| Box::new(e) as _),
            Self::Chiado(p) => BlockNumReader::best_block_number(p).map_err(|e| Box::new(e) as _),
        }
    }

    /// Erase this source into the reth `StateProviderFactory` view —
    /// the state half of [`ChainProvider`], sharing the same provider.
    pub fn state_factory(&self) -> Arc<dyn StateProviderFactory> {
        match self {
            Self::Eth(p) => Arc::new(p.clone()),
            Self::Chiado(p) => Arc::new(p.clone()),
        }
    }
}

impl std::fmt::Debug for HeaderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eth(_) => f.debug_tuple("Eth").field(&"..").finish(),
            Self::Chiado(_) => f.debug_tuple("Chiado").field(&"..").finish(),
        }
    }
}

/// Everything needed to simulate calls on a chain.
pub struct ChainProvider {
    /// State provider factory — `.latest()` opens a fresh state snapshot.
    pub provider: Arc<dyn StateProviderFactory>,
    /// Header source — concrete enum over the node's provider families.
    pub headers: HeaderSource,
    /// EVM config for building envs from headers.
    pub evm_config: EthEvmConfig,
}

impl Clone for ChainProvider {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            headers: self.headers.clone(),
            evm_config: self.evm_config.clone(),
        }
    }
}

impl std::fmt::Debug for ChainProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainProvider")
            .field("provider", &"..")
            .field("headers", &"..")
            .field("evm_config", &"..")
            .finish()
    }
}
