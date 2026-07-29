//! Unified local reth-backed chain client.
//!
//! [`LocalChainClient`] replaces the pre-Run-A `LocalSourceChainClient`
//! + `LocalTargetChainClient` split. One struct, one reth provider
//! handle, one [`ChainProvider`] inside. Role is carried by the
//! [`Role`] enum; the constructor's return type
//! (`Arc<dyn EntryChainClient>` vs `Arc<dyn ChainClient>`) locks
//! capability at the call site — a follower instance cannot be
//! accidentally registered as the composition entry.
//!
//! # Role-derived `proxy_lookup`
//!
//! The per-rollup [`ProxyLookupConfig`] is derived from [`Role`]
//! internally; `main.rs` does not pass it (and cannot configure the
//! storage slot by hand — per the mandatory no-EVM-in-protocol
//! invariant, the slot selector is an associated type, not a TOML
//! field). L1-style clients use `EEZ.sol`'s `authorizedProxies`
//! at slot 3; L2-style clients use `CrossChainManagerL2.sol`'s
//! `authorizedProxies` at slot 2.

use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::{ConfigureEvm, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::SignerRecoverable;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{BlockNumReader, HeaderProvider, StateProvider, StateProviderFactory};

use eez_evm_inspector::{OverlayChannelHandle, SessionInspectorFactory, new_overlay_channel};
use eez_protocol::{
    ChainClient, CompositionBuilder, ExecutorError, ExecutorResult, ProxyLookupConfig, RollupId,
    TargetExecutionSession,
};

use super::provider::{ChainProvider, HeaderReader};
use super::session::LocalExecutionSession;

/// Discriminates how this client operates within the composition.
///
/// Both variants carry `dispatch_address` — the contract holding this
/// chain's `authorizedProxies` mapping (and, for `EvmL1Style` chains,
/// the canonical committed-root storage). The slot is derived from the
/// client's [`eez_protocol::ChainDialect`] (stored alongside) at
/// `proxy_lookup_config` time.
#[derive(Debug, Clone)]
pub enum Role {
    /// This rollup initiates user transactions and anchors the composition.
    Entry {
        /// Contract holding `authorizedProxies` on this chain. L1
        /// entry: `EEZ.sol`. L2 entry: `CrossChainManagerL2`.
        dispatch_address: Address,
    },
    /// This rollup only receives cross-chain dispatches.
    Follower {
        /// Contract holding `authorizedProxies` on this chain. For
        /// EvmL1-style followers (the L1 follower in L2-as-entry
        /// topology) this is `EEZ.sol`, allowing the follower to
        /// serve [`eez_protocol::CommittedRootReader`].
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
/// Implements [`ChainClient`] for every role and [`EntryChainClient`]
/// additionally for the entry role — the entry-only methods
/// assert `Role::Entry` via `Unavailable`, not panic.
pub struct LocalChainClient {
    /// Type-erased chain provider used by every execution path.
    provider: ChainProvider,
    rollup_id: RollupId,
    role: Role,
    ccm_address: Address,
    /// Per-chain dialect — drives proxy-lookup slot, CCM-verify shape,
    /// and the [`eez_protocol::CommittedRootReader`] capability
    /// honestly: only `EvmL1Style` clients actually serve canonical
    /// committed-root reads (the L1 `EEZ.sol` storage layout
    /// `compute_state_root_slot` assumes).
    dialect: eez_protocol::ChainDialect,
    /// Bidirectional overlay channel for shared-source-state nested
    /// dispatch. `Some(channel)` only on entry-role clients —
    /// populated by the source-sim inspector with source's in-flight
    /// cache so the entry rollup's [`LocalExecutionSession`] preloads
    /// it; the entry session then writes its post-execute cache back,
    /// and the source-sim inspector applies the diff onto source's
    /// journal. Follower clients leave this `None`.
    overlay_channel: Option<OverlayChannelHandle>,
}

impl std::fmt::Debug for LocalChainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalChainClient")
            .field("rollup_id", &self.rollup_id)
            .field("role", &self.role)
            .field("ccm_address", &self.ccm_address)
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

    /// Build an entry-role client. Returned as `Arc<Self>` so the call
    /// site can erase it into `Arc<dyn ChainClient>` for
    /// [`CrossChainWiring::entry_client`](crate::composer::CrossChainWiring::entry_client),
    /// which also serves the committed-root reads when the entry chain
    /// is L1.
    pub fn new_entry<P>(
        provider: P,
        evm_config: EthEvmConfig,
        rollup_id: RollupId,
        dispatch_address: Address,
        ccm_address: Address,
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
            ccm_address,
            dialect,
            overlay_channel: Some(new_overlay_channel()),
        })
    }

    /// Build a follower-role client. Returned as `Arc<Self>` so the
    /// caller can erase to BOTH `Arc<dyn ChainClient>` (always) AND
    /// `Arc<dyn CommittedRootReader>` (when this follower's dialect
    /// is `EvmL1Style`, i.e. it serves canonical committed roots).
    /// Local `LocalChainClient<...>` implements all three capability
    /// traits unconditionally; the constructed instance only honestly
    /// answers committed-root reads when the dialect matches the
    /// underlying contract's storage layout.
    pub fn new_follower<P>(
        provider: P,
        evm_config: EthEvmConfig,
        rollup_id: RollupId,
        dispatch_address: Address,
        ccm_address: Address,
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
            ccm_address,
            dialect,
            overlay_channel: Some(new_overlay_channel()),
        })
    }

    /// Project this client's dialect into a [`ProxyLookupConfig`]
    /// matching its `authorizedProxies` layout. L1-style clients read
    /// slot 3 on `EEZ.sol`; L2-style clients read slot 2 on
    /// `CrossChainManagerL2.sol`. The slot derives from `self.dialect`,
    /// not from role — in the L2-as-entry topology an entry uses the
    /// L2 slot. Consumed by source-sim and target-session inspectors
    /// via the shared [`SessionInspectorFactory`].
    fn proxy_lookup_config(&self) -> ProxyLookupConfig {
        // Slot derives from dialect (per Codex's design constraint:
        // ChainDialect terminates at config accessors — `LocalChainClient`
        // reads the plain `u8` slot; the dialect itself never crosses
        // into inspector code).
        ProxyLookupConfig {
            contract_address: self.role.dispatch_address(),
            authorized_proxies_slot: self.dialect.proxy_lookup_slot(),
        }
    }

    /// Internal helper — read the stored state root for
    /// `target_rollup_id` from the entry-chain rollups contract.
    fn read_stored_target_state_root(
        &self,
        state: &dyn StateProvider,
        rollups_address: Address,
        target_rollup_id: RollupId,
    ) -> ExecutorResult<[u8; 32]> {
        let root_slot = eez_protocol::action::compute_state_root_slot(target_rollup_id);
        let value = state
            .storage(rollups_address, root_slot)
            .map_err(|e| ExecutorError::Provider(e.into()))?
            .unwrap_or(U256::ZERO);
        Ok(value.to_be_bytes::<32>())
    }
}

#[async_trait::async_trait]
impl ChainClient for LocalChainClient {
    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
        tracing::debug!(
            rollup_id = %self.rollup_id,
            ccm = %self.ccm_address,
            role = ?self.role,
            "opening execution session"
        );
        // Both roles install a target-side inspector. Entry overlay
        // sessions can themselves dispatch outgoing proxy calls (e.g.
        // `reentrantCrossChainCalls`'s deeper alternation), and the
        // rollup's `overlay_channel` is attached to the inspector
        // factory unconditionally so every session participates in
        // the pre/post stack mechanism for state propagation across
        // re-entered sessions of THIS rollup.
        let mut factory = SessionInspectorFactory::new(self.proxy_lookup_config(), self.rollup_id);
        if let Some(channel) = &self.overlay_channel {
            factory = factory.with_overlay_channel(Arc::clone(channel));
        }
        let inspector_factory = Some(factory);
        // Overlay path. When the entry
        // rollup's session is lazily opened by the `CompositionBuilder`
        // because an inner target-session frame is dispatching back to
        // entry, the source-sim inspector has already snapshotted
        // source's in-flight cache into the channel's `source_cache`
        // slot. We preload the new session's `State` with that
        // snapshot, and forward the channel handle so the session
        // writes its post-execute cache back to `overlay_cache` for
        // the inspector to diff-apply onto source.
        //
        // `None` channel or `None` snapshot opens a fresh `State`,
        // which is byte-identical for fixtures whose source tx makes
        // no entry-chain state changes before the cross-chain
        // dispatch (`nestedCounter`, `nestedCallRevert`).
        //
        // Peek (don't pop) the source_cache stack top — the most
        // recent inspector pre-snapshot for THIS rollup is "the
        // latest live state we know about." When re-entering a
        // session whose outer instance is `take()`-en mid-execute,
        // this gives the new session continuation from outer's state
        // instead of fresh from disk.
        let preloaded_cache = self
            .overlay_channel
            .as_ref()
            .and_then(|c| c.peek_pre_snapshot());
        // `compute_proxy_address` (called from `build_tx_env` for every
        // session execute) calls `computeCrossChainProxyAddress` on a
        // chain-local contract. Both Rollups (L1) and CrossChainManager
        // (L2) implement that function with the same signature, but
        // they live at different addresses. For the entry rollup we
        // want Rollups (L1's `rollups_address`); for followers we want
        // their CCM (`ccm_address`). Without this split, an entry
        // overlay session targets `source_block.ccm_address` (L2's
        // CCM address) on L1 — a non-contract address — and silently
        // returns `Address::ZERO` as the proxy caller, causing the
        // overlay's CALL to revert at the proxy's
        // `executeCrossChainCall` site.
        // Entry session: target the dispatch contract for `compute_proxy_address`.
        // Follower session: target the CCM contract (the actual cross-chain
        // dispatch endpoint). For EVM these may coincide on a chain whose
        // dispatch contract is the CCM (L2 CrossChainManagerL2); they
        // differ only when the chain has a separate Rollups + CCM split.
        let proxy_lookup_addr = match &self.role {
            Role::Entry { dispatch_address } => *dispatch_address,
            Role::Follower { .. } => self.ccm_address,
        };
        let session = LocalExecutionSession::new(
            &self.provider,
            proxy_lookup_addr,
            inspector_factory,
            preloaded_cache,
            self.overlay_channel.clone(),
        )?;
        Ok(Box::new(session))
    }

    /// Read the latest block header's `stateRoot` from this chain's
    /// own provider. Orthogonal to invariant-6 anchoring; useful for
    /// diagnostics and future paths.
    async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
        let num = self
            .provider
            .headers
            .best_block_number()
            .map_err(ExecutorError::Provider)?;
        let header = self
            .provider
            .headers
            .header_by_number(num)
            .map_err(ExecutorError::Provider)?
            .ok_or_else(|| {
                ExecutorError::Missing("header at latest block for current_state_root")
            })?;
        Ok(header.state_root.0)
    }

    async fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        use alloy_eips::eip2718::Decodable2718;
        use std::time::Instant;

        // Soundness guard: a Follower instance cannot be reached via
        // `dyn EntryChainClient` through any public API (new_follower
        // returns `dyn ChainClient`, and Rust can't upcast to the
        // supertrait). Keep the defensive error in case internal code
        // acquires a concrete `LocalChainClient` reference and calls
        // this method directly.
        let Role::Entry { .. } = &self.role else {
            return Err(ExecutorError::Unavailable(
                "simulate_source_tx called on follower LocalChainClient".into(),
            ));
        };

        let t_total = Instant::now();

        // ── 1. Decode raw tx ──────────────────────────────────────
        let t_decode = Instant::now();
        let mut raw: &[u8] = &raw_tx;
        let tx = TransactionSigned::decode_2718(&mut raw)
            .map_err(|e| ExecutorError::Decode(e.to_string()))?;
        let signer = tx
            .recover_signer()
            .map_err(|e| ExecutorError::Decode(e.to_string()))?;
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
            .map_err(ExecutorError::Provider)?;
        let header = self
            .provider
            .headers
            .header_by_number(latest_num)
            .map_err(ExecutorError::Provider)?
            .ok_or_else(|| ExecutorError::Missing("source header at latest block"))?;
        // Take ownership of the state provider so `State<DB>` is
        // `'static`. `StateProviderBox = Box<dyn StateProvider + Send
        // + 'static>` owns the provider, which keeps the resulting
        // `State<DB>` `'static` and unblocks any `Box<dyn Any>`-erased
        // clone path.
        let evm_state = self
            .provider
            .provider
            .latest()
            .map_err(|e| ExecutorError::Provider(e.into()))?;
        let db = StateProviderDatabase::new(evm_state);
        let mut state = State::builder().with_database(db).build();
        let state_us = t_state.elapsed().as_micros();

        // Reth's `StateProviderBox` is not `Sync`, so a cross-thread
        // overlay design (e.g. closures over `*mut State<DB>`) is not
        // viable. The current overlay path keeps both the source-sim
        // EVM and the entry overlay session on the same OS thread via
        // `block_in_place`, so this constraint never bites.

        // ── 3. Run source EVM with inspector ──────────────────────
        let t_env = Instant::now();
        let mut evm_env = self
            .provider
            .evm_config
            .evm_env(&header)
            .map_err(|e| ExecutorError::Evm(e.into()))?;
        // Relax nonce check in source-sim. For L2-as-entry topologies
        // the user-tx's nonce is N+1 because `loadExecutionTable`
        // (also signed by the system address $PK on L2) lands first
        // in the same block and bumps $PK's nonce to N+1; chain state
        // at sim time still has $PK at nonce N, so strict nonce
        // validation rejects the tx before any opcode runs and the
        // inspector never sees the cross-chain dispatch. The other
        // checks (balance, base fee, etc.) stay on so the simulator
        // catches genuine issues — only the L2-entry strict-ordering
        // mismatch is whitelisted.
        evm_env.cfg_env.disable_nonce_check = true;
        let recovered = reth_primitives_traits::Recovered::new_unchecked(tx, signer);
        let tx_env = self.provider.evm_config.tx_env(&recovered);
        let env_us = t_env.elapsed().as_micros();

        let t_sim = Instant::now();

        // Source-sim inspector runs at the composition root: every
        // detected proxy CALL dispatches through `Dispatcher` and
        // lands in the composition's preorder `recorded[..]` slice.
        // Attach the overlay channel so the inspector snapshots
        // source's cache before each downstream dispatch (preload
        // for the entry overlay session) and applies the post-execute
        // diff onto source's journal after dispatch returns.
        let mut factory = SessionInspectorFactory::new(self.proxy_lookup_config(), self.rollup_id);
        if let Some(channel) = &self.overlay_channel {
            factory = factory.with_overlay_channel(Arc::clone(channel));
        }
        let handle = tokio::runtime::Handle::current();
        let inspector = factory.build(dispatcher, handle);
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
        let proxy_lookups = evm.inspector().proxy_lookups();

        let inspector_error = evm.inspector_mut().take_error();
        drop(evm);
        let sim_us = t_sim.elapsed().as_micros();

        if let Some(err) = inspector_error {
            return Err(err);
        }

        // Drain the entry overlay's per-tx roots (one per overlay
        // execute, in chronological dispatch order) and forward to
        // the dispatcher so `finalize` can populate
        // `per_tx_roots_by_rollup[entry]`. Without this, nested calls
        // attributed to the entry rollup hit `InvalidCheckpoint` in
        // `build_batch` (the CCM-verify loop skips the entry rollup,
        // leaving its slot in the map empty).
        if let Some(channel) = &self.overlay_channel {
            let roots = channel.drain_post_roots();
            if !roots.is_empty() {
                dispatcher.set_extra_per_tx_roots(self.rollup_id, roots);
            }
        }

        tracing::info!(
            gas_used,
            success,
            proxy_lookups,
            "source simulation complete"
        );

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

    /// Committed-root reads — only meaningful when this client is
    /// connected to the chain hosting the canonical committed-root storage
    /// (L1's `EEZ.sol` in this protocol).
    ///
    /// Today's L1-as-entry topology has the entry client itself serve this
    /// role: `CrossChainWiring::entry_client` is the single erased
    /// `Arc<dyn ChainClient>` that runs source simulation AND serves the
    /// committed-root reads.
    ///
    /// L2-as-entry topology will need a follower variant: when this is a
    /// L1 follower client, it must implement this trait honestly. That
    /// honestly serves committed-root reads only when the client's dialect
    /// is `EvmL1Style` — that's the only chain whose `dispatch_address`
    /// points to a `EEZ.sol`-shaped contract whose storage layout
    /// `compute_state_root_slot` assumes. Both entry and follower roles
    /// can serve when L1-style: the entry case covers L1-as-entry single-binary;
    /// the follower case covers L1-as-follower in L2-as-entry topology.
    /// Non-L1 clients return `Unavailable` so misregistration fails loudly.
    async fn stored_target_state_root(&self, rollup_id: RollupId) -> ExecutorResult<[u8; 32]> {
        // Only L1-style clients honestly serve committed-root reads —
        // the storage-slot math `compute_state_root_slot` assumes the
        // L1 `EEZ.sol` layout. L2-style clients return `Unavailable`
        // so a misregistered root_reader fails loudly at first dispatch.
        if self.dialect != eez_protocol::ChainDialect::EvmL1Style {
            return Err(ExecutorError::Unavailable(
                "stored_target_state_root called on a non-L1 LocalChainClient \
                 (only EvmL1Style clients hold canonical committed-root storage)"
                    .into(),
            ));
        }
        let dispatch_address = self.role.dispatch_address();

        // Self-query path: the L1 client (entry or follower) asking
        // about its OWN state. Returns the latest header root, which
        // on the test devnet coincides with `Rollups.rollups[L1_id].stateRoot`
        // because every postBatch atomically updates both. The
        // cross-rollup path below is the protocol-correct read for
        // OTHER rollups (where storage and header may diverge between
        // batches).
        if rollup_id == self.rollup_id {
            let num = self
                .provider
                .headers
                .best_block_number()
                .map_err(ExecutorError::Provider)?;
            let header = self
                .provider
                .headers
                .header_by_number(num)
                .map_err(ExecutorError::Provider)?
                .ok_or_else(|| ExecutorError::Missing("L1 header at latest block"))?;
            let root = header.state_root.0;
            tracing::debug!(
                %rollup_id,
                root = ?B256::from(root),
                "L1 self-query: returned latest header state_root"
            );
            return Ok(root);
        }

        // Cross-rollup path: read from the L1 Rollups contract's storage.
        let state = self
            .provider
            .provider
            .latest()
            .map_err(|e| ExecutorError::Provider(e.into()))?;
        let root =
            self.read_stored_target_state_root(state.as_ref(), dispatch_address, rollup_id)?;
        tracing::debug!(
            %rollup_id,
            root = ?B256::from(root),
            "stored target state root read"
        );
        Ok(root)
    }
}
