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
use reth_storage_api::{BlockNumReader, HeaderProvider, StateProviderBox, StateProviderFactory};
use revm::DatabaseCommit;
use revm::context::result::EVMError;

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

    /// Reth handles (state factory, headers, `EvmConfig`) backing this chain.
    #[must_use]
    pub fn chain_provider(&self) -> &ChainProvider {
        &self.provider
    }

    /// Contract holding this chain's `authorizedProxies` mapping and driving
    /// cross-chain execution: `EEZ` on L1, `EEZL2` on L2.
    #[must_use]
    pub fn manager_address(&self) -> Address {
        self.role.dispatch_address()
    }

    /// The inspector factory both `begin_execution_session` and
    /// [`Self::simulate_source_tx_on`] build: this chain's proxy lookup, rollup
    /// id, and overlay channel.
    #[must_use]
    pub fn inspector_factory(&self) -> SessionInspectorFactory {
        SessionInspectorFactory::new(
            self.proxy_lookup_config(),
            self.rollup_id,
            Arc::clone(&self.overlay_channel),
        )
    }

    /// Source-simulate a raw tx over a caller-provided live state + env — a
    /// fork of the slot's execution context — COMMITTING the result state into
    /// that fork so later simulations see this tx's writes.
    ///
    /// Entry-role only; the caller owns the env, which must already be derived
    /// from the fork's own header.
    ///
    /// # Errors
    ///
    /// [`ExecutorErrorKind::Unavailable`] on a follower client,
    /// [`ExecutorErrorKind::Decode`] when the raw tx cannot be decoded or its
    /// signer recovered, [`ExecutorErrorKind::Provider`] when the backing store
    /// fails mid-execution, plus any error a nested dispatch raises.
    pub fn simulate_source_tx_on(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
        state: &mut State<StateProviderDatabase<StateProviderBox>>,
        evm_env: reth_evm::EvmEnvFor<EthEvmConfig>,
    ) -> ExecutorResult<()> {
        self.source_sim(raw_tx, dispatcher, state, evm_env)
    }

    /// Shared source-simulation body: decode, build the tx env, run under the
    /// session inspector, commit.
    fn source_sim(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
        state: &mut State<StateProviderDatabase<StateProviderBox>>,
        evm_env: reth_evm::EvmEnvFor<EthEvmConfig>,
    ) -> ExecutorResult<()> {
        use alloy_eips::eip2718::Decodable2718;

        // Only entry-role clients are authorized to simulate source
        // transactions. Keep the check here because callers use the uniform
        // `ChainClient` interface for both roles.
        let Role::Entry { .. } = &self.role else {
            return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                "simulate_source_tx_on called on follower LocalChainClient".into(),
            )));
        };

        let mut raw: &[u8] = &raw_tx;
        let tx = TransactionSigned::decode_2718(&mut raw)
            .map_err(|e| ExecutorError::from(ExecutorErrorKind::Decode(e.to_string())))?;
        let signer = tx
            .recover_signer()
            .map_err(|e| ExecutorError::from(ExecutorErrorKind::Decode(e.to_string())))?;

        tracing::info!(
            ?signer,
            to = ?alloy_consensus::Transaction::to(&tx),
            "simulating source tx for cross-chain call detection"
        );

        let recovered = reth_primitives_traits::Recovered::new_unchecked(tx, signer);
        let tx_env = self.provider.evm_config.tx_env(&recovered);

        // The source-simulation inspector dispatches every detected proxy CALL
        // through the composition builder, which records calls in preorder.
        // Attach the cache channel around each downstream dispatch.
        let inspector = self.inspector_factory().build(dispatcher);
        let mut evm =
            self.provider
                .evm_config
                .evm_with_env_and_inspector(&mut *state, evm_env, inspector);
        let (gas_used, success, changes) = match evm.transact(tx_env) {
            Ok(r) => (r.result.tx_gas_used(), r.result.is_success(), Some(r.state)),
            // The backing store is unreachable — that is the slot's problem, not
            // the tx's, so it must not degrade into an empty composition (which
            // the drain reads as poison and evicts on).
            Err(EVMError::Database(e)) => return Err(ExecutorError::provider(e)),
            // Rejected before execution (nonce, balance, fee). Same outcome as a
            // revert — no calls, so the drain evicts — but named for what it is.
            Err(EVMError::Transaction(e)) => {
                tracing::warn!(%e, "source tx rejected at validation; it records no cross-chain call");
                (0, false, None)
            }
            Err(e) => {
                tracing::warn!(%e, "source sim reverted");
                (0, false, None)
            }
        };
        let inspector_error = evm.inspector_mut().take_error();
        drop(evm);

        if let Some(err) = inspector_error {
            return Err(err);
        }
        // The trait-method caller's `State` is function-local, so committing
        // into it is unobservable there; the fork caller needs the writes.
        if let Some(changes) = changes {
            state.commit(changes);
        }

        tracing::info!(gas_used, success, "source simulation complete");
        Ok(())
    }
}

impl ChainClient for LocalChainClient {
    fn reset_composition_state(&self) {
        self.overlay_channel.reset();
    }

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
        let inspector_factory = Some(self.inspector_factory());
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
}
