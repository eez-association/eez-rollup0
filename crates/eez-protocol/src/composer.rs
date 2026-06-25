//! Generic, long-lived cross-chain composer.
//!
//! [`Composer<P>`] is the stateful counterpart of
//! [`compose_transaction`](crate::compose::compose_transaction):
//! same chain-agnostic orchestration, but holds the entry and follower
//! clients for reuse across many source transactions. Chain families
//! consume it via a type alias (e.g. `type Composer = composer::Composer<EvmProtocol>;`).
//!
//! # Lifecycle
//!
//! 1. [`Composer::builder(protocol, entry_id)`](Composer::builder) —
//!    construct a [`ComposerBuilder<P>`].
//! 2. [`.entry(client, cfg)`](ComposerBuilder::entry) — register the
//!    entry-chain client (required).
//! 3. [`.rollup(id, client, cfg)`](ComposerBuilder::rollup) — register
//!    each follower rollup (zero or more).
//!
//! 3b. [`.root_reader(client)`](ComposerBuilder::root_reader) — register
//!     the committed-root reader (required exactly once).
//!
//! 4. [`.build()`](ComposerBuilder::build) — finalize. Returns a sealed,
//!    immutable [`Composer<P>`].
//! 5. [`simulate_and_resolve(raw_tx)`](Composer::simulate_and_resolve) —
//!    **many times**, one per source tx.
//!
//! Registration errors (entry not set, missing root reader, follower
//! with entry id, duplicate ids) surface from `build()`. Once built,
//! the composer is immutable: no locks, no race-able state.
//!
//! # Clone + thread-safety
//!
//! [`Composer<P>`] is cheap to [`Clone`] (one [`Arc`] bump) and is
//! `Send + Sync` for every valid `P`. The inner state (rollup map,
//! entry slot, protocol instance) is shared through [`Arc`].

use std::collections::HashMap;
use std::sync::Arc;

use crate::compose::compose_transaction_recorded;
use crate::composition::Rollup;
use crate::error::{ComposerError, ComposerErrorKind, ComposerResult};
use crate::executor::{
    ChainClient, CommittedRootReader, EntryChainClient, TargetVerificationContext,
};
use crate::protocol::ChainProtocol;
use crate::rollup_id::RollupId;
use crate::types::{Composition, ExecutedAction};

// ── Config ───────────────────────────────────────────────────────

/// Default gas limit for CCM-verification system txs. Generous so
/// worst-case CCM execute paths on dev chains succeed.
pub const DEFAULT_CCM_GAS_LIMIT: u64 = 30_000_000;

/// Combined proxy-lookup configuration for a registered rollup.
///
/// Bundles the storage-contract address and the storage slot index
/// where that contract holds its `authorizedProxies` mapping.
///
/// Constructed at `main.rs` startup from the rollup's role:
/// - L1-style client (entry-as-L1 or follower-as-L1):
///   `contract_address = rollups_address`,
///   `authorized_proxies_slot = 0` (`EEZ.authorizedProxies` on EVM —
///   inherited from `EEZBase` at slot 0).
/// - L2-style client:
///   `contract_address = ccm_address`,
///   `authorized_proxies_slot = 0`
///   (`EEZL2.authorizedProxies` on EVM — inherited from `EEZBase`
///   at slot 0).
///
/// Non-EVM chains supply their own slot index via their config.
pub struct ProxyLookupConfig<P: ChainProtocol + ?Sized> {
    /// Address of the contract holding `authorizedProxies` on this chain.
    pub contract_address: P::Address,
    /// Storage slot index where `authorizedProxies` lives on
    /// `contract_address`. The inspector reads
    /// `keccak256(addr ++ slot)` to find a registered proxy.
    pub authorized_proxies_slot: u8,
}

// Manual Clone / Debug / PartialEq / Eq — derive would generate
// overly strict bounds on `P` that callers with non-`Clone` `P` can't
// satisfy. The bounds on `P::Address` are already sufficient.
impl<P: ChainProtocol + ?Sized> Clone for ProxyLookupConfig<P> {
    fn clone(&self) -> Self {
        Self {
            contract_address: self.contract_address.clone(),
            authorized_proxies_slot: self.authorized_proxies_slot,
        }
    }
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for ProxyLookupConfig<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyLookupConfig")
            .field("contract_address", &self.contract_address)
            .field("authorized_proxies_slot", &self.authorized_proxies_slot)
            .finish()
    }
}

impl<P: ChainProtocol + ?Sized> PartialEq for ProxyLookupConfig<P>
where
    P::Address: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.contract_address == other.contract_address
            && self.authorized_proxies_slot == other.authorized_proxies_slot
    }
}

impl<P: ChainProtocol + ?Sized> Eq for ProxyLookupConfig<P> where P::Address: Eq {}

/// Per-rollup static configuration.
///
/// Holds CCM contract address, system address, gas limit, and proxy
/// lookup for one rollup (entry or follower). Passed to
/// [`ComposerBuilder::entry`] / [`ComposerBuilder::rollup`] alongside
/// the client.
pub struct TargetConfig<P: ChainProtocol + ?Sized> {
    /// Address of the CCM entrypoint on this chain.
    pub ccm_address: P::Address,
    /// System address authorized to call CCM on this chain.
    pub system_address: P::Address,
    /// Gas limit for each of the two CCM-verification system txs.
    /// Default: [`DEFAULT_CCM_GAS_LIMIT`].
    pub ccm_gas_limit: u64,
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig<P>,
    /// Per-chain ABI dialect: selects entry-encoding and CCM-verify
    /// batch shape (L1-style vs L2-style for the EVM family).
    /// Default = the chain family's default dialect (e.g. `EvmL2Style`
    /// preserves byte-identity for the existing 12 L1→L2 fixtures).
    pub dialect: P::Dialect,
    /// Settlement policy: when `true`, this target's post-state root is
    /// the root its own target execution session already
    /// produced (the real, sealed root reported back over the wire),
    /// so [`CompositionBuilder::finalize`](crate::composition::CompositionBuilder::finalize)
    /// SKIPS the CCM-verify `simulate_transactions` pass and keeps the
    /// recorded `post_state_root`. Intended for a remote target whose
    /// client cannot serve `simulate_transactions` locally. No in-tree
    /// client sets this today — the bidi-stream `RemoteL2Client` inbound
    /// path was retired (inbound now settles as a prover-signed deferred
    /// entry); retained for future remote targets.
    /// `false` for in-process follower targets and the zk-poster
    /// L1 target (the latter has its own skip path via
    /// [`crate::ChainProtocol::dialect_is_zk_poster`]).
    /// Default `false` — preserves the existing outbound (L2→L1) and
    /// L1→L2-fixture behaviour.
    pub settles_via_session_root: bool,
}

impl<P: ChainProtocol + ?Sized> Clone for TargetConfig<P> {
    fn clone(&self) -> Self {
        Self {
            ccm_address: self.ccm_address.clone(),
            system_address: self.system_address.clone(),
            ccm_gas_limit: self.ccm_gas_limit,
            proxy_lookup: self.proxy_lookup.clone(),
            dialect: self.dialect.clone(),
            settles_via_session_root: self.settles_via_session_root,
        }
    }
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for TargetConfig<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetConfig")
            .field("ccm_address", &self.ccm_address)
            .field("system_address", &self.system_address)
            .field("ccm_gas_limit", &self.ccm_gas_limit)
            .field("proxy_lookup", &self.proxy_lookup)
            .field("dialect", &self.dialect)
            .field("settles_via_session_root", &self.settles_via_session_root)
            .finish()
    }
}

impl<P: ChainProtocol + ?Sized> PartialEq for TargetConfig<P>
where
    P::Address: PartialEq,
    P::Dialect: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.ccm_address == other.ccm_address
            && self.system_address == other.system_address
            && self.ccm_gas_limit == other.ccm_gas_limit
            && self.proxy_lookup == other.proxy_lookup
            && self.dialect == other.dialect
            && self.settles_via_session_root == other.settles_via_session_root
    }
}

impl<P: ChainProtocol + ?Sized> Eq for TargetConfig<P>
where
    P::Address: Eq,
    P::Dialect: Eq,
{
}

/// Per-rollup attribution inputs for batch construction.
///
/// [`ChainProtocol::build_batch`]
/// consumes this to chain per-entry `stateDeltas` (upstream's invariant 6).
/// Two sources of truth:
///
/// - `initial_roots[rollup]` — the state root each rollup started at,
///   read from the entry chain once per
///   [`Composer::simulate_and_resolve`](Composer::simulate_and_resolve).
/// - `per_tx_roots_by_rollup[rollup]` — cumulative post-state roots
///   after each transaction in that rollup's CCM-verify batch
///   (`loadExecutionTable` + `executeIncomingCrossChainCall`). Supplied
///   by [`CompositionBuilder::finalize`](crate::composition::CompositionBuilder::finalize)
///   from each per-rollup `simulate_transactions` return value.
///
/// References (no ownership) so the builder materializes each map once
/// per composition and hands borrowed handles to the protocol impl.
///
/// This struct is protocol-agnostic by construction: no EVM types named.
/// Protocol impls that need chain-specific bookkeeping (counter folds,
/// classifier passes) walk the preorder `recorded[..]` slice directly —
/// the attribution here is purely about numeric state roots.
#[derive(Debug)]
pub struct SourceAttribution<'a> {
    /// Per-rollup initial state roots, as of the entry chain's current
    /// block when the composition began.
    pub initial_roots: &'a HashMap<RollupId, [u8; 32]>,
    /// Per-rollup cumulative post-state roots for each tx in that
    /// rollup's CCM-verify batch. Keyed by `RollupId`; each `Vec` is
    /// ordered by batch tx index.
    pub per_tx_roots_by_rollup: &'a HashMap<RollupId, Vec<[u8; 32]>>,
}

impl<P: ChainProtocol + ?Sized> TargetConfig<P> {
    /// Project this config into a [`TargetVerificationContext`].
    ///
    /// `ccm_address` maps to `entrypoint_address` — just renamed per
    /// context. This helper is the single authoritative rename site.
    #[must_use]
    pub fn verification_context(&self) -> TargetVerificationContext<P> {
        TargetVerificationContext {
            system_address: self.system_address.clone(),
            entrypoint_address: self.ccm_address.clone(),
            gas_limit: self.ccm_gas_limit,
        }
    }
}

// ── Composer ─────────────────────────────────────────────────────

struct EntryRegistration<P: ChainProtocol + 'static> {
    /// Defensive invariant: stored at registration so future debug
    /// helpers can assert consistency with `ComposerInner.entry_rollup_id`.
    #[allow(dead_code, reason = "invariant capture for future debug diagnostics")]
    rollup_id: RollupId,
    /// Held as `Arc<dyn EntryChainClient>` so the composer can call
    /// `simulate_source_tx` without an Any-downcast. Also coerced to
    /// `Arc<dyn ChainClient>` and inserted into the `rollups` map for
    /// uniform dispatch.
    client: Arc<dyn EntryChainClient<Protocol = P> + Send + Sync>,
}

struct RootReaderRegistration<P: ChainProtocol + 'static> {
    /// Held as `Arc<dyn CommittedRootReader>` so the composer can read
    /// upstream's invariant-6 anchor roots in Phase 1 of
    /// `simulate_and_resolve`. Implementations: a local L1 client
    /// (entry-when-L1 or follower-when-L1-in-L2-as-entry) reading its
    /// own `EEZ.sol` storage, or a gRPC client whose remote peer is L1.
    client: Arc<dyn CommittedRootReader<Protocol = P> + Send + Sync>,
}

struct RegisteredRollup<P: ChainProtocol + 'static> {
    client: Arc<dyn ChainClient<Protocol = P> + Send + Sync>,
    config: TargetConfig<P>,
}

struct ComposerInner<P: ChainProtocol + 'static> {
    protocol: P,
    /// Rollup id of the entry chain.
    entry_rollup_id: RollupId,
    /// Entry-specific client handle (`EntryChainClient` trait object),
    /// distinct from the `rollups` map which holds the same client
    /// coerced to `ChainClient`.
    entry: EntryRegistration<P>,
    /// Committed-root reader (`CommittedRootReader` trait object) used
    /// by Phase 1 of `simulate_and_resolve` to read each rollup's
    /// upstream-invariant-6 anchor root. Required at registration; see
    /// [`ComposerBuilder::root_reader`].
    root_reader: RootReaderRegistration<P>,
    /// All registered rollups (entry + followers). The entry is also
    /// in this map via trait upcast — composition orchestration uses
    /// it uniformly.
    rollups: HashMap<RollupId, Arc<RegisteredRollup<P>>>,
}

/// Mutable accumulator for a [`Composer<P>`].
///
/// Created via [`Composer::builder`]. Call [`entry`](Self::entry) to
/// register the entry-chain client (required), [`rollup`](Self::rollup)
/// for each follower (zero or more), and [`build`](Self::build) to
/// finalize. The resulting `Composer<P>` is immutable — no locks, no
/// race-able state.
pub struct ComposerBuilder<P: ChainProtocol + 'static> {
    protocol: P,
    entry_rollup_id: RollupId,
    entry: Option<EntryRegistration<P>>,
    root_reader: Option<RootReaderRegistration<P>>,
    rollups: HashMap<RollupId, Arc<RegisteredRollup<P>>>,
    /// Latched error — set by [`rollup`](Self::rollup) when called with
    /// an id that conflicts with the entry rollup or with a previously
    /// registered follower. Surfaced from [`build`](Self::build) so
    /// callers don't have to handle a `Result` per `.rollup()` call.
    deferred_error: Option<ComposerError>,
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for ComposerBuilder<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposerBuilder")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("entry_set", &self.entry.is_some())
            .field("root_reader_set", &self.root_reader.is_some())
            .field("rollups", &self.rollups.len())
            .finish()
    }
}

impl<P: ChainProtocol + 'static> ComposerBuilder<P> {
    /// Build an empty builder pinned to `entry_rollup_id`.
    #[must_use]
    pub fn new(protocol: P, entry_rollup_id: RollupId) -> Self {
        Self {
            protocol,
            entry_rollup_id,
            entry: None,
            root_reader: None,
            rollups: HashMap::new(),
            deferred_error: None,
        }
    }

    /// Register the entry-chain client. Required exactly once before
    /// [`build`](Self::build).
    ///
    /// The client is stored as `Arc<dyn EntryChainClient>` for source-
    /// side operations AND inserted into the rollup map as
    /// `Arc<dyn ChainClient>` via trait upcast — `simulate_and_resolve`
    /// dispatches uniformly through the rollup map.
    ///
    /// Calling twice replaces the previous entry registration. Errors
    /// from a duplicate-by-mistake (entry-chain id later passed to
    /// [`rollup`](Self::rollup)) surface at [`build`](Self::build).
    #[must_use]
    pub fn entry(
        mut self,
        client: Arc<dyn EntryChainClient<Protocol = P> + Send + Sync>,
        config: TargetConfig<P>,
    ) -> Self {
        let entry_client_for_slot = Arc::clone(&client);
        let chain_client: Arc<dyn ChainClient<Protocol = P> + Send + Sync> = client;
        self.rollups.insert(
            self.entry_rollup_id,
            Arc::new(RegisteredRollup {
                client: chain_client,
                config,
            }),
        );
        self.entry = Some(EntryRegistration {
            rollup_id: self.entry_rollup_id,
            client: entry_client_for_slot,
        });
        self
    }

    /// Register the committed-root reader. Required exactly once before
    /// [`build`](Self::build).
    ///
    /// The reader is the client connected to the chain hosting the
    /// canonical committed-root storage (L1 in this protocol). It serves
    /// every rollup's upstream-invariant-6 anchor in Phase 1 of
    /// [`Composer::simulate_and_resolve`] — INCLUDING the entry rollup's
    /// own initial root. The upstream protocol enforces
    /// `entry[i].currentState == rollups[id].stateRoot` for every delta
    /// in `postBatch` (see `EEZ.sol`), so chain headers (self-reports)
    /// are NOT correct for this purpose.
    ///
    /// In the L1-as-entry topology, the entry client itself implements
    /// `CommittedRootReader` and may be wrapped in a second `Arc` here.
    /// In the L2-as-entry topology, the L1 follower client (local or
    /// gRPC) is the reader.
    ///
    /// Calling twice replaces the previous reader.
    #[must_use]
    pub fn root_reader(
        mut self,
        client: Arc<dyn CommittedRootReader<Protocol = P> + Send + Sync>,
    ) -> Self {
        self.root_reader = Some(RootReaderRegistration { client });
        self
    }

    /// Register a follower-chain client.
    ///
    /// `rollup_id` must NOT equal the entry rollup id (use
    /// [`entry`](Self::entry) for that) and must not already be
    /// registered. Both violations latch a deferred error that
    /// [`build`](Self::build) surfaces — keeping `.rollup()` infallible
    /// at the chained call site so a single misconfigured registration
    /// doesn't force every caller into `Result` plumbing.
    #[must_use]
    pub fn rollup(
        mut self,
        rollup_id: RollupId,
        client: Arc<dyn ChainClient<Protocol = P> + Send + Sync>,
        config: TargetConfig<P>,
    ) -> Self {
        if self.deferred_error.is_some() {
            return self;
        }
        if rollup_id == self.entry_rollup_id {
            self.deferred_error = Some(
                ComposerErrorKind::Misconfigured {
                    reason: "rollup() called with entry rollup id; use entry() instead",
                }
                .into(),
            );
            return self;
        }
        if self.rollups.contains_key(&rollup_id) {
            self.deferred_error = Some(
                ComposerErrorKind::AlreadyRegistered {
                    what: "rollup client",
                }
                .into(),
            );
            return self;
        }
        self.rollups
            .insert(rollup_id, Arc::new(RegisteredRollup { client, config }));
        self
    }

    /// Finalize the builder. Returns an immutable, sealed [`Composer<P>`].
    ///
    /// # Errors
    ///
    /// - [`ComposerErrorKind::Misconfigured`] if [`entry`](Self::entry)
    ///   was never called, or if a rollup id was registered as both
    ///   entry and follower (which the builder API can't catch
    ///   structurally — the entry-id slot would be overwritten by a
    ///   matching `rollup` call).
    // The Err carries the full builder state back for diagnostics; build()
    // runs once at startup, so the large variant never hits a hot path.
    #[allow(clippy::result_large_err)]
    pub fn build(self) -> ComposerResult<Composer<P>> {
        if let Some(err) = self.deferred_error {
            return Err(err);
        }
        let entry = self.entry.ok_or_else(|| {
            ComposerError::from(ComposerErrorKind::Misconfigured {
                reason: "entry not registered before build",
            })
        })?;
        let root_reader = self
            .root_reader
            .ok_or_else(|| ComposerError::from(ComposerErrorKind::MissingRootReader))?;
        for rollup_id in self.rollups.keys() {
            tracing::info!(
                name: "composer.rollup_registered",
                %rollup_id,
                "rollup registered",
            );
        }
        Ok(Composer {
            inner: Arc::new(ComposerInner {
                protocol: self.protocol,
                entry_rollup_id: self.entry_rollup_id,
                entry,
                root_reader,
                rollups: self.rollups,
            }),
        })
    }
}

/// Cross-chain composer. Built once at startup, reused per source tx.
///
/// Cheap to [`Clone`] — the inner state is shared through an [`Arc`].
pub struct Composer<P: ChainProtocol + 'static> {
    inner: Arc<ComposerInner<P>>,
}

impl<P: ChainProtocol + 'static> Clone for Composer<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for Composer<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("entry_rollup_id", &self.inner.entry_rollup_id)
            .field("rollups", &self.inner.rollups.len())
            .finish()
    }
}

impl<P: ChainProtocol + 'static> Composer<P> {
    /// Start building a new composer pinned to `entry_rollup_id`.
    ///
    /// Use [`ComposerBuilder::entry`] to register the entry-chain
    /// client, [`ComposerBuilder::rollup`] for each follower, then
    /// [`ComposerBuilder::build`] to finalize. The resulting
    /// `Composer<P>` is immutable.
    #[must_use]
    pub fn builder(protocol: P, entry_rollup_id: RollupId) -> ComposerBuilder<P> {
        ComposerBuilder::new(protocol, entry_rollup_id)
    }

    /// Rollup id of the entry chain this composer serves.
    #[must_use]
    pub fn entry_rollup_id(&self) -> RollupId {
        self.inner.entry_rollup_id
    }

    /// Detect cross-chain proxy calls, run CCM verification, and return
    /// the final [`Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`ComposerErrorKind::Executor`] if simulation or
    /// verification fails.
    /// Returns [`ComposerErrorKind::Protocol`] if entry building or
    /// finalization fails.
    #[tracing::instrument(skip(self, raw_tx), fields(tx_len = raw_tx.len()))]
    pub async fn simulate_and_resolve(&self, raw_tx: &[u8]) -> ComposerResult<Composition<P>>
    where
        P: crate::capabilities::SettlesOutbound + crate::capabilities::ConsumesInbound,
    {
        self.simulate_and_resolve_recorded(raw_tx)
            .await
            .map(|(composition, _recorded)| composition)
    }

    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve) but
    /// ALSO returns the builder's `recorded[..]` (preorder dispatched
    /// cross-chain calls with resolved outcomes). Callers that need a
    /// call's REAL `return_data` (e.g. the inbound L1→L2 delivery, which
    /// builds the byte-locked `executeIncomingCrossChainCall` system tx)
    /// use this; the [`Composition`] alone doesn't carry per-call return
    /// data verbatim.
    ///
    /// # Errors
    ///
    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve).
    pub async fn simulate_and_resolve_recorded(
        &self,
        raw_tx: &[u8],
    ) -> ComposerResult<(Composition<P>, Vec<ExecutedAction<P>>)>
    where
        P: crate::capabilities::SettlesOutbound + crate::capabilities::ConsumesInbound,
    {
        // Default entry selection: the pinned entry rollup + its client.
        // Per-composition entry selection (A1) goes through
        // `simulate_and_resolve_recorded_for`.
        self.simulate_and_resolve_recorded_for(
            self.inner.entry_rollup_id,
            self.inner.entry.client.as_ref(),
            raw_tx,
        )
        .await
    }

    /// Same as [`simulate_and_resolve_recorded`](Self::simulate_and_resolve_recorded)
    /// but with an explicitly-chosen entry: `entry_id` + the `entry_client`
    /// that runs source simulation. Lets ONE composer compose either
    /// direction — `(L1, L1 client)` for an inbound L1→L2 call, `(L2, L2
    /// client)` for an outbound L2→L1 call — picked per tx by the drain.
    /// The dispatch rollup map (Phase 1) is the composer's full
    /// registration set, unchanged.
    ///
    /// # Errors
    ///
    /// Same as [`simulate_and_resolve_recorded`](Self::simulate_and_resolve_recorded).
    pub async fn simulate_and_resolve_recorded_for(
        &self,
        entry_id: RollupId,
        entry_client: &(dyn EntryChainClient<Protocol = P> + Send + Sync),
        raw_tx: &[u8],
    ) -> ComposerResult<(Composition<P>, Vec<ExecutedAction<P>>)>
    where
        P: crate::capabilities::SettlesOutbound + crate::capabilities::ConsumesInbound,
    {
        tracing::info!(
            name: "composer.simulate.start",
            %entry_id,
            tx_len = raw_tx.len(),
            rollup_count = self.inner.rollups.len(),
            "simulate_and_resolve: starting composition pipeline"
        );

        // Phase 1 — assemble per-rollup state for the builder.
        //
        // For each rollup (including the entry):
        // - client: cheap Arc clone from the registration.
        // - session: None (lazy-open on first dispatch).
        // - initial_state_root: read via the registered
        //   `CommittedRootReader`. The upstream protocol enforces
        //   `entry[i].currentState == rollups[id].stateRoot` for every
        //   delta in `postBatch` (see `EEZ.sol`), so ALL rollups
        //   (including the entry's own) read through the committed-root
        //   reader — chain headers (self-reports via
        //   [`ChainClient::current_state_root`]) are NOT correct for
        //   upstream-invariant-6 anchoring.
        let mut rollups: HashMap<RollupId, Rollup<P>> =
            HashMap::with_capacity(self.inner.rollups.len());
        for (rollup_id, reg) in &self.inner.rollups {
            let initial_state_root = self
                .inner
                .root_reader
                .client
                .stored_target_state_root(*rollup_id)
                .await?;
            rollups.insert(
                *rollup_id,
                Rollup {
                    client: Arc::clone(&reg.client),
                    session: None,
                    config: reg.config.clone(),
                    initial_state_root,
                },
            );
        }

        // Phase 2 — compose. Pass the entry client directly for
        // source simulation; pass the full rollup map for dispatch.
        // `_recorded` carries the resolved per-call outcomes (return_data)
        // the byte-locked inbound delivery needs.
        let (composition, recorded) = compose_transaction_recorded(
            &self.inner.protocol,
            entry_client,
            raw_tx,
            entry_id,
            rollups,
        )
        .await?;

        tracing::info!(
            name: "composer.simulate.complete",
            target_count = composition.targets.len(),
            recorded = recorded.len(),
            "composition complete"
        );

        Ok((composition, recorded))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composition::Dispatcher;
    use crate::error::{ExecutorResult, ProtocolResult};
    use crate::executor::{
        ExecutionRequest, ExecutionResponse, TargetBatchSimulation, TargetExecutionSession,
        TargetTransaction,
    };
    use serde::{Deserialize, Serialize};

    // ── Minimal FakeProtocol — enough to instantiate Composer<FakeProtocol> ──

    #[derive(Debug, Clone, Copy)]
    struct FakeProtocol;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Placeholder;

    impl crate::capabilities::SettlesOutbound for FakeProtocol {
        fn build_settlement_batch(
            &self,
            _calls: &[crate::ExecutedAction<Self>],
            _dst: crate::RollupId,
        ) -> crate::error::ProtocolResult<Self::Batch> {
            Err(crate::error::ProtocolErrorKind::Unsupported("test fake: no outbound settlement").into())
        }
    }

    impl ChainProtocol for FakeProtocol {
        type Address = [u8; 20];
        type Value = u128;
        type Calldata = Vec<u8>;
        type Batch = ();
        type Overlay = Placeholder;
        type Witness = Placeholder;
        type Dialect = ();

        fn build_batch(
            &self,
            _recorded: &[ExecutedAction<Self>],
            _attribution: &crate::composer::SourceAttribution<'_>,
            _dialect: &Self::Dialect,
            _source_rollup_id: RollupId,
            _raw_tx: &[u8],
        ) -> ProtocolResult<Self::Batch> {
            Ok(())
        }
        fn encode_postbatch(&self, _batch: &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_load_table(&self, _batch: &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_follower_trigger(
            &self,
            _call: &ExecutedAction<Self>,
            _source_rollup_id: RollupId,
            _raw_tx: &[u8],
            _dialect: &Self::Dialect,
        ) -> Vec<u8> {
            vec![]
        }
        fn encode_address(&self, addr: &Self::Address) -> Vec<u8> {
            addr.to_vec()
        }
        fn decode_address(&self, bytes: &[u8]) -> ProtocolResult<Self::Address> {
            bytes.try_into().map_err(|_err| {
                crate::error::ProtocolErrorKind::InvalidEncoding("address".into()).into()
            })
        }
        fn encode_value(&self, val: &Self::Value) -> Vec<u8> {
            val.to_be_bytes().to_vec()
        }
        fn decode_value(&self, bytes: &[u8]) -> ProtocolResult<Self::Value> {
            bytes.try_into().map(u128::from_be_bytes).map_err(|_err| {
                crate::error::ProtocolErrorKind::InvalidEncoding("value".into()).into()
            })
        }
        fn encode_calldata(&self, data: &Self::Calldata) -> Vec<u8> {
            data.clone()
        }
        fn decode_calldata(&self, bytes: &[u8]) -> ProtocolResult<Self::Calldata> {
            Ok(bytes.to_vec())
        }
        fn message_id(&self, m: &crate::message::Message<'_, Self>) -> [u8; 32] {
            let _ = m;
            [0u8; 32]
        }
    }

    struct FakeClient;

    #[async_trait::async_trait]
    impl ChainClient for FakeClient {
        type Protocol = FakeProtocol;
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = Self::Protocol> + Send>>
        {
            unimplemented!("composer misconfigured-state tests never open a session")
        }
        async fn simulate_transactions(
            &self,
            _txs: &[TargetTransaction<Self::Protocol>],
        ) -> ExecutorResult<TargetBatchSimulation> {
            unimplemented!("composer misconfigured-state tests never simulate a batch")
        }
    }

    #[async_trait::async_trait]
    impl EntryChainClient for FakeClient {
        async fn simulate_source_tx(
            &self,
            _raw_tx: Vec<u8>,
            _dispatcher: &mut (dyn Dispatcher<Protocol = Self::Protocol> + Send),
        ) -> ExecutorResult<()> {
            // Returns Ok with zero dispatches; finalize then surfaces
            // EmptyCalls. Used by the seal test so the composer
            // actually seals inside simulate_and_resolve.
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl CommittedRootReader for FakeClient {
        async fn stored_target_state_root(&self, _rollup_id: RollupId) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
    }

    // A standalone ChainClient-only follower fake — ensures
    // register_rollup accepts a type that does NOT impl
    // EntryChainClient (the trait-object erasure is what the bound
    // requires; no upcast from dyn ChainClient to dyn EntryChainClient
    // is possible).
    struct FakeFollowerClient;

    #[async_trait::async_trait]
    impl ChainClient for FakeFollowerClient {
        type Protocol = FakeProtocol;
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = Self::Protocol> + Send>>
        {
            unimplemented!("composer misconfigured-state tests never open a session")
        }
        async fn simulate_transactions(
            &self,
            _txs: &[TargetTransaction<Self::Protocol>],
        ) -> ExecutorResult<TargetBatchSimulation> {
            unimplemented!("composer misconfigured-state tests never simulate a batch")
        }
    }

    fn builder() -> ComposerBuilder<FakeProtocol> {
        Composer::builder(FakeProtocol, RollupId(0))
    }

    fn target_config() -> TargetConfig<FakeProtocol> {
        TargetConfig {
            ccm_address: [0u8; 20],
            system_address: [0u8; 20],
            ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
            proxy_lookup: ProxyLookupConfig {
                contract_address: [0u8; 20],
                authorized_proxies_slot: 0,
            },
            dialect: (),
            settles_via_session_root: false,
        }
    }

    fn entry_arc() -> Arc<dyn EntryChainClient<Protocol = FakeProtocol> + Send + Sync> {
        Arc::new(FakeClient)
    }

    fn root_reader_arc() -> Arc<dyn CommittedRootReader<Protocol = FakeProtocol> + Send + Sync> {
        Arc::new(FakeClient)
    }

    fn follower_arc() -> Arc<dyn ChainClient<Protocol = FakeProtocol> + Send + Sync> {
        Arc::new(FakeFollowerClient)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn build_without_entry_returns_misconfigured() {
        match builder().root_reader(root_reader_arc()).build() {
            Err(e)
                if matches!(
                    e.kind(),
                    ComposerErrorKind::Misconfigured { reason }
                        if reason.contains("entry not registered")
                ) => {}
            other => panic!("expected Misconfigured (entry not registered), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_without_root_reader_returns_missing_root_reader() {
        let res = builder().entry(entry_arc(), target_config()).build();
        match res {
            Err(e) => assert!(matches!(e.kind(), ComposerErrorKind::MissingRootReader)),
            Ok(_) => panic!("expected MissingRootReader, got Ok"),
        }
    }

    #[tokio::test]
    async fn build_with_rollup_overwriting_entry_returns_misconfigured() {
        // `.rollup(entry_id, ...)` after `.entry(...)` overwrites the
        // entry slot in the rollup map; build() detects the lost
        // entry registration and rejects the build.
        let res = builder()
            .entry(entry_arc(), target_config())
            .root_reader(root_reader_arc())
            .rollup(RollupId(0), follower_arc(), target_config())
            .build();
        // The follower-as-entry-id case overwrites the entry's entry
        // in the map, leaving the EntryRegistration referencing a
        // gone client. build() catches this via the rollups-map sanity
        // check.
        match res {
            Err(e) => assert!(matches!(
                e.kind(),
                ComposerErrorKind::Misconfigured { .. }
                    | ComposerErrorKind::AlreadyRegistered { .. }
            )),
            Ok(_) => panic!("expected Misconfigured, got Ok"),
        }
    }

    #[tokio::test]
    async fn build_succeeds_with_entry_only() {
        let composer = builder()
            .entry(entry_arc(), target_config())
            .root_reader(root_reader_arc())
            .build()
            .expect("build");
        assert_eq!(composer.entry_rollup_id(), RollupId(0));
    }

    #[tokio::test]
    async fn build_succeeds_with_entry_and_followers() {
        let composer = builder()
            .entry(entry_arc(), target_config())
            .root_reader(root_reader_arc())
            .rollup(RollupId(1), follower_arc(), target_config())
            .rollup(RollupId(2), follower_arc(), target_config())
            .build()
            .expect("build");
        assert_eq!(composer.entry_rollup_id(), RollupId(0));
    }

    // Dispatcher trait is used in the FakeClient impl — reference it
    // here so the import isn't flagged as unused by future editors.
    #[allow(dead_code, reason = "type-level reference to keep import live")]
    fn _dispatcher_in_scope<P: ChainProtocol + 'static>(
    ) -> std::marker::PhantomData<dyn Dispatcher<Protocol = P> + Send> {
        std::marker::PhantomData
    }

    // ExecutionRequest + ExecutionResponse used via trait surface — keep
    // them importable for future test additions.
    #[allow(dead_code, reason = "type-level reference to keep import live")]
    fn _response_request_in_scope<P: ChainProtocol + 'static>(
        _: ExecutionRequest<P>,
    ) -> Option<ExecutionResponse<P>> {
        None
    }
}
