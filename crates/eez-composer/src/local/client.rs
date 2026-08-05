//! Local reth-backed implementation of [`ChainClient`].
//!
//! [`Role`] selects source-simulation capability and the chain-local dispatch
//! address. [`eez_protocol::ChainDialect`] selects the proxy-mapping layout.
//! Each client owns an overlay channel used to propagate in-flight state
//! through same-rollup re-entry.

use std::sync::Arc;

use alloy_primitives::Address;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::{ConfigureEvm, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::SignerRecoverable;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{BlockNumReader, HeaderProvider, StateProviderFactory};

use eez_evm_inspector::{OverlayChannelHandle, SessionInspectorFactory, new_overlay_channel};
use eez_protocol::{
    ChainClient, CompositionBuilder, ExecutorError, ExecutorErrorKind, ExecutorResult,
    ProxyLookupConfig, RollupId, TargetExecutionSession,
};

use super::provider::{ChainProvider, HeaderReader};
use super::session::LocalExecutionSession;

/// Discriminates how this client operates within the composition.
///
/// Both variants carry the contract holding this chain's
/// `authorizedProxies` mapping. The client's dialect supplies the mapping slot.
#[derive(Debug, Clone)]
pub enum Role {
    /// May initiate source simulation and participate through target sessions.
    Entry {
        /// Contract holding `authorizedProxies` on this chain: `EEZ`
        /// on L1 or `EEZL2` on L2.
        dispatch_address: Address,
    },
    /// Cannot initiate source simulation; participates through target sessions.
    Follower {
        /// Contract holding `authorizedProxies` on this chain.
        dispatch_address: Address,
    },
}

impl Role {
    /// Address of the dispatch contract on this chain.
    fn dispatch_address(&self) -> Address {
        match self {
            Role::Entry { dispatch_address } | Role::Follower { dispatch_address } => {
                *dispatch_address
            }
        }
    }
}

/// Unified local chain client.
///
/// Implements [`ChainClient`] for every role. Entry-only behavior checks
/// `Role::Entry` at runtime and returns `Unavailable` for follower clients.
pub struct LocalChainClient {
    /// Type-erased chain provider used by every execution path.
    provider: ChainProvider,
    rollup_id: RollupId,
    role: Role,
    /// Contract dialect used to select the proxy-mapping layout.
    dialect: eez_protocol::ChainDialect,
    /// Bidirectional cache channel used to propagate in-flight state through
    /// nested dispatches that re-enter this rollup.
    overlay_channel: OverlayChannelHandle,
}

impl std::fmt::Debug for LocalChainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalChainClient")
            .field("rollup_id", &self.rollup_id)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl LocalChainClient {
    fn build_chain_provider<P>(provider: &P, evm_config: EthEvmConfig) -> ChainProvider
    where
        P: StateProviderFactory
            + HeaderProvider<Header = alloy_consensus::Header>
            + BlockNumReader
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let headers: Arc<dyn HeaderReader> = Arc::new(provider.clone());
        let state_provider: Arc<dyn StateProviderFactory> = Arc::new(provider.clone());
        ChainProvider {
            provider: state_provider,
            headers,
            evm_config,
        }
    }

    /// Build an entry-role client.
    pub fn new_entry<P>(
        provider: P,
        evm_config: EthEvmConfig,
        rollup_id: RollupId,
        dispatch_address: Address,
        dialect: eez_protocol::ChainDialect,
    ) -> Arc<Self>
    where
        P: StateProviderFactory
            + HeaderProvider<Header = alloy_consensus::Header>
            + BlockNumReader
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let cp = Self::build_chain_provider(&provider, evm_config);
        Arc::new(Self {
            provider: cp,
            rollup_id,
            role: Role::Entry { dispatch_address },
            dialect,
            overlay_channel: new_overlay_channel(),
        })
    }

    /// Build a follower-role client.
    pub fn new_follower<P>(
        provider: P,
        evm_config: EthEvmConfig,
        rollup_id: RollupId,
        dispatch_address: Address,
        dialect: eez_protocol::ChainDialect,
    ) -> Arc<Self>
    where
        P: StateProviderFactory
            + HeaderProvider<Header = alloy_consensus::Header>
            + BlockNumReader
            + Clone
            + Send
            + Sync
            + 'static,
    {
        let cp = Self::build_chain_provider(&provider, evm_config);
        Arc::new(Self {
            provider: cp,
            rollup_id,
            role: Role::Follower { dispatch_address },
            dialect,
            overlay_channel: new_overlay_channel(),
        })
    }

    /// Build the proxy lookup used by source and target-session inspectors.
    /// The role supplies the chain-local contract address and the dialect
    /// supplies its storage layout.
    fn proxy_lookup_config(&self) -> ProxyLookupConfig {
        ProxyLookupConfig {
            contract_address: self.role.dispatch_address(),
            authorized_proxies_slot: self.dialect.proxy_lookup_slot(),
        }
    }
}

impl ChainClient for LocalChainClient {
    fn begin_execution_session(&self) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
        tracing::debug!(
            rollup_id = %self.rollup_id,
            manager = %self.role.dispatch_address(),
            role = ?self.role,
            "opening execution session"
        );
        // Inspect every target session because nested proxy calls may dispatch
        // again. The inspector exchanges cache snapshots through this client's
        // configured channel.
        let inspector_factory = Some(SessionInspectorFactory::new(
            self.proxy_lookup_config(),
            self.rollup_id,
            Arc::clone(&self.overlay_channel),
        ));
        // Preload the top cache snapshot when one is available.
        let preloaded_cache = self.overlay_channel.peek_pre_snapshot();
        let manager_address = self.role.dispatch_address();
        let session = LocalExecutionSession::new(
            &self.provider,
            manager_address,
            inspector_factory,
            preloaded_cache,
            self.overlay_channel.clone(),
        )?;
        Ok(Box::new(session))
    }

    fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        use alloy_eips::eip2718::Decodable2718;
        use std::time::Instant;

        // Only entry-role clients are authorized to simulate source
        // transactions. Keep the check here because callers use the uniform
        // `ChainClient` interface for both roles.
        let Role::Entry { .. } = &self.role else {
            return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                "simulate_source_tx called on follower LocalChainClient".into(),
            )));
        };

        let t_total = Instant::now();

        // ── 1. Decode raw tx ──────────────────────────────────────
        let t_decode = Instant::now();
        let mut raw: &[u8] = &raw_tx;
        let tx = TransactionSigned::decode_2718(&mut raw)
            .map_err(|e| ExecutorError::from(ExecutorErrorKind::Decode(e.to_string())))?;
        let signer = tx
            .recover_signer()
            .map_err(|e| ExecutorError::from(ExecutorErrorKind::Decode(e.to_string())))?;
        let decode_us = t_decode.elapsed().as_micros();

        tracing::info!(
            ?signer,
            to = ?alloy_consensus::Transaction::to(&tx),
            "simulating source tx for cross-chain call detection"
        );

        // ── 2. Open source state ──────────────────────────────────
        let t_state = Instant::now();
        let latest_num = self
            .provider
            .headers
            .best_block_number()
            .map_err(ExecutorError::provider)?;
        let header = self
            .provider
            .headers
            .header_by_number(latest_num)
            .map_err(ExecutorError::provider)?
            .ok_or_else(|| {
                ExecutorError::from(ExecutorErrorKind::Missing("source header at latest block"))
            })?;
        // Own the provider for the lifetime of the revm state.
        let evm_state = self
            .provider
            .provider
            .latest()
            .map_err(ExecutorError::provider)?;
        let db = StateProviderDatabase::new(evm_state);
        let mut state = State::builder().with_database(db).build();
        let state_us = t_state.elapsed().as_micros();

        // ── 3. Run source EVM with inspector ──────────────────────
        let t_env = Instant::now();
        let mut evm_env = self
            .provider
            .evm_config
            .evm_env(&header)
            .map_err(ExecutorError::evm)?;
        // A system-signed source transaction can use nonce N+1 because the
        // preceding `loadExecutionTable` transaction consumes nonce N, while
        // source simulation reads parent state. Disable only nonce validation
        // so inspection can run; retain the other transaction checks.
        evm_env.cfg_env.disable_nonce_check = true;
        let recovered = reth_primitives_traits::Recovered::new_unchecked(tx, signer);
        let tx_env = self.provider.evm_config.tx_env(&recovered);
        let env_us = t_env.elapsed().as_micros();

        let t_sim = Instant::now();

        // The source-simulation inspector dispatches every detected proxy CALL
        // through the composition builder, which records calls in preorder.
        // Attach the cache channel around each downstream dispatch.
        let factory = SessionInspectorFactory::new(
            self.proxy_lookup_config(),
            self.rollup_id,
            Arc::clone(&self.overlay_channel),
        );
        let inspector = factory.build(dispatcher);
        let mut evm = self
            .provider
            .evm_config
            .evm_with_env_and_inspector(&mut state, evm_env, inspector);
        let (gas_used, success) = match evm.transact(tx_env) {
            Ok(r) => (r.result.tx_gas_used(), r.result.is_success()),
            Err(e) => {
                tracing::warn!(%e, "source sim reverted");
                (0, false)
            }
        };
        let inspector_error = evm.inspector_mut().take_error();
        drop(evm);
        let sim_us = t_sim.elapsed().as_micros();

        if let Some(err) = inspector_error {
            return Err(err);
        }

        tracing::info!(gas_used, success, "source simulation complete");

        let total_us = t_total.elapsed().as_micros();
        tracing::debug!(
            timing.decode_us = decode_us,
            timing.state_us = state_us,
            timing.env_us = env_us,
            timing.sim_us = sim_us,
            timing.total_us = total_us,
            "source simulation timing"
        );

        Ok(())
    }
}
