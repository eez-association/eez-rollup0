//! Reth-specific chain-provider abstractions shared by the local
//! client and the execution session.
//!
//! [`ChainProvider`] bundles the three reth handles every EVM
//! simulation needs:
//!
//! - type-erased [`StateSnapshotProvider`] (for opening a state snapshot)
//! - type-erased [`HeaderReader`] (dyn-compatible wrapper around
//!   `HeaderProvider`, which has generic methods that block direct
//!   `dyn` use)
//! - `EezEvmConfig` (for building EVM envs from headers)
//!
//! Held once per rollup inside [`crate::composer::local::LocalChainClient`]
//! and cloned cheaply per execution-session open.

use std::sync::Arc;

use alloy_primitives::B256;
use eez_evm::EezEvmConfig;
use reth_storage_api::{
    BlockNumReader, HeaderProvider, StateProviderBox, StateProviderFactory,
    errors::provider::ProviderResult,
};

/// Dyn-compatible state-snapshot opener.
///
/// reth's `StateProviderFactory` names the chain's `Primitives`, so one
/// `dyn` type cannot cover both L2 and L1 (Ethereum or Gnosis) providers.
/// Simulation only opens snapshots, which is chain-agnostic.
pub trait StateSnapshotProvider: Send + Sync {
    /// Open a snapshot of the latest state.
    fn latest(&self) -> ProviderResult<StateProviderBox>;

    /// Open a snapshot of the state after block `hash`, including an
    /// in-memory (not yet persisted) block.
    fn state_by_block_hash(&self, hash: B256) -> ProviderResult<StateProviderBox>;
}

impl<T: StateProviderFactory + Sync> StateSnapshotProvider for T {
    fn latest(&self) -> ProviderResult<StateProviderBox> {
        StateProviderFactory::latest(self)
    }

    fn state_by_block_hash(&self, hash: B256) -> ProviderResult<StateProviderBox> {
        StateProviderFactory::state_by_block_hash(self, hash)
    }
}

/// Dyn-compatible header reader (`HeaderProvider` has generic methods
/// that prevent `dyn HeaderProvider`).
pub trait HeaderReader: Send + Sync {
    /// Look up a block header by number. Returns `Ok(None)` if the
    /// block does not exist.
    fn header_by_number(
        &self,
        num: u64,
    ) -> Result<Option<alloy_consensus::Header>, Box<dyn std::error::Error + Send + Sync>>;

    /// Highest known block number.
    fn best_block_number(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>;
}

impl<T> HeaderReader for T
where
    T: HeaderProvider<Header = alloy_consensus::Header> + BlockNumReader + Send + Sync,
{
    fn header_by_number(
        &self,
        num: u64,
    ) -> Result<Option<alloy_consensus::Header>, Box<dyn std::error::Error + Send + Sync>> {
        HeaderProvider::header_by_number(self, num).map_err(|e| Box::new(e) as _)
    }

    fn best_block_number(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        BlockNumReader::best_block_number(self).map_err(|e| Box::new(e) as _)
    }
}

/// Everything needed to simulate calls on a chain.
pub struct ChainProvider {
    /// State snapshot opener — `.latest()` opens a fresh state snapshot.
    pub provider: Arc<dyn StateSnapshotProvider>,
    /// Header reader — dyn-compatible wrapper around `HeaderProvider`.
    pub headers: Arc<dyn HeaderReader>,
    /// EVM config for building envs from headers.
    pub evm_config: EezEvmConfig,
}

impl Clone for ChainProvider {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            headers: Arc::clone(&self.headers),
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
