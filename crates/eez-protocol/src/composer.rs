//! Long-lived cross-chain composer.
//!
//! [`Composer`] holds the entry and follower
//! clients for reuse across many source transactions and runs one
//! [`CompositionBuilder`] pass per source tx.
//!
//! # Lifecycle
//!
//! 1. [`Composer::builder(entry_id)`](Composer::builder) —
//!    construct a [`ComposerBuilder`].
//! 2. [`.entry(client, cfg)`](ComposerBuilder::entry) — register the
//!    entry-chain client (required).
//! 3. [`.rollup(id, client, cfg)`](ComposerBuilder::rollup) — register
//!    each follower rollup (zero or more).
//!
//! 3b. [`.root_reader(client)`](ComposerBuilder::root_reader) — register
//!     the committed-root reader (required exactly once).
//!
//! 4. [`.build()`](ComposerBuilder::build) — finalize. Returns a sealed,
//!    immutable [`Composer`].
//! 5. [`simulate_and_resolve(raw_tx)`](Composer::simulate_and_resolve) —
//!    **many times**, one per source tx.
//!
//! Registration errors (entry not set, missing root reader, follower
//! with entry id, duplicate ids) surface from `build()`. Once built,
//! the composer is immutable: no locks, no race-able state.
//!
//! # Clone + thread-safety
//!
//! [`Composer`] is cheap to [`Clone`] (one [`Arc`] bump) and is
//! `Send + Sync`. The inner state (rollup map,
//! entry slot) is shared through [`Arc`].

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::Address;

use crate::composition::{CompositionBuilder, Rollup};
use crate::dialect::ChainDialect;
use crate::error::{ComposerError, ComposerErrorKind, ComposerResult};
use crate::executor::{ChainClient, CommittedRootReader, EntryChainClient};
use crate::rollup_id::RollupId;
use crate::types::{Composition, ExecutedAction};

// ── Config ───────────────────────────────────────────────────────

/// Combined proxy-lookup configuration for a registered rollup.
///
/// Bundles the storage-contract address and the storage slot index
/// where that contract holds its `authorizedProxies` mapping.
///
/// Constructed at `main.rs` startup from the rollup's role:
/// - L1-style client (entry-as-L1 or follower-as-L1):
///   `contract_address = rollups_address`,
///   `authorized_proxies_slot = 0` (`EEZ.authorizedProxies` —
///   inherited from `EEZBase` at slot 0).
/// - L2-style client:
///   `contract_address = ccm_address`,
///   `authorized_proxies_slot = 0`
///   (`EEZL2.authorizedProxies` — inherited from `EEZBase`
///   at slot 0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyLookupConfig {
    /// Address of the contract holding `authorizedProxies` on this chain.
    pub contract_address: Address,
    /// Storage slot index where `authorizedProxies` lives on
    /// `contract_address`. The inspector reads
    /// `keccak256(addr ++ slot)` to find a registered proxy.
    pub authorized_proxies_slot: u8,
}

/// Per-rollup static configuration.
///
/// Holds the proxy lookup and ABI dialect for one rollup (entry or
/// follower). Passed to
/// [`ComposerBuilder::entry`] / [`ComposerBuilder::rollup`] alongside
/// the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetConfig {
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig,
    /// ABI dialect: selects entry-encoding and
    /// batch shape (L1-style vs L2-style).
    /// Default = `EvmL2Style`
    /// (preserves byte-identity for the existing 12 L1→L2 fixtures).
    pub dialect: ChainDialect,
}

/// Per-rollup attribution inputs for batch construction.
///
/// [`crate::entries::build_batch`]
/// consumes this to chain per-entry `stateDeltas` (upstream's invariant 6).
/// Two sources of truth:
///
/// - `initial_roots[rollup]` — the state root each rollup started at,
///   read from the entry chain once per
///   [`Composer::simulate_and_resolve`](Composer::simulate_and_resolve).
/// - `per_tx_roots_by_rollup[rollup]` — the post-state roots
///   `finalize` attributed per rollup (zk-poster settlement root or
///   inbound delivery root).
///
/// References (no ownership) so the builder materializes each map once
/// per composition and hands borrowed handles to the batch builder.
///
/// This struct is protocol-agnostic by construction: no EVM types named.
/// Builders that need chain-specific bookkeeping (counter folds,
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

// ── Composer ─────────────────────────────────────────────────────

struct RegisteredRollup {
    client: Arc<dyn ChainClient + Send + Sync>,
    config: TargetConfig,
}

struct ComposerInner {
    /// Rollup id of the entry chain.
    entry_rollup_id: RollupId,
    /// Entry-specific client handle (`EntryChainClient` trait object),
    /// distinct from the `rollups` map which holds the same client
    /// coerced to `ChainClient`. Held so the composer can call
    /// `simulate_source_tx` without an Any-downcast.
    entry: Arc<dyn EntryChainClient + Send + Sync>,
    /// Committed-root reader (`CommittedRootReader` trait object) used
    /// by Phase 1 of `simulate_and_resolve` to read each rollup's
    /// upstream-invariant-6 anchor root. Required at registration; see
    /// [`ComposerBuilder::root_reader`]. Implementations: a local L1
    /// client (entry-when-L1 or follower-when-L1-in-L2-as-entry)
    /// reading its own `EEZ.sol` storage, or a gRPC client whose
    /// remote peer is L1.
    root_reader: Arc<dyn CommittedRootReader + Send + Sync>,
    /// All registered rollups (entry + followers). The entry is also
    /// in this map via trait upcast — composition orchestration uses
    /// it uniformly.
    rollups: HashMap<RollupId, RegisteredRollup>,
}

/// Mutable accumulator for a [`Composer`].
///
/// Created via [`Composer::builder`]. Call [`entry`](Self::entry) to
/// register the entry-chain client (required), [`rollup`](Self::rollup)
/// for each follower (zero or more), and [`build`](Self::build) to
/// finalize. The resulting `Composer` is immutable — no locks, no
/// race-able state.
pub struct ComposerBuilder {
    entry_rollup_id: RollupId,
    entry: Option<Arc<dyn EntryChainClient + Send + Sync>>,
    root_reader: Option<Arc<dyn CommittedRootReader + Send + Sync>>,
    rollups: HashMap<RollupId, RegisteredRollup>,
    /// Latched error — set by [`rollup`](Self::rollup) when called with
    /// an id that conflicts with the entry rollup or with a previously
    /// registered follower. Surfaced from [`build`](Self::build) so
    /// callers don't have to handle a `Result` per `.rollup()` call.
    deferred_error: Option<ComposerError>,
}

impl std::fmt::Debug for ComposerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComposerBuilder")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("entry_set", &self.entry.is_some())
            .field("root_reader_set", &self.root_reader.is_some())
            .field("rollups", &self.rollups.len())
            .finish()
    }
}

impl ComposerBuilder {
    /// Build an empty builder pinned to `entry_rollup_id`.
    #[must_use]
    pub fn new(entry_rollup_id: RollupId) -> Self {
        Self {
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
        client: Arc<dyn EntryChainClient + Send + Sync>,
        config: TargetConfig,
    ) -> Self {
        let entry_client_for_slot = Arc::clone(&client);
        let chain_client: Arc<dyn ChainClient + Send + Sync> = client;
        self.rollups.insert(
            self.entry_rollup_id,
            RegisteredRollup {
                client: chain_client,
                config,
            },
        );
        self.entry = Some(entry_client_for_slot);
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
    pub fn root_reader(mut self, client: Arc<dyn CommittedRootReader + Send + Sync>) -> Self {
        self.root_reader = Some(client);
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
        client: Arc<dyn ChainClient + Send + Sync>,
        config: TargetConfig,
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
            .insert(rollup_id, RegisteredRollup { client, config });
        self
    }

    /// Finalize the builder. Returns an immutable, sealed [`Composer`].
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
    pub fn build(self) -> ComposerResult<Composer> {
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
#[derive(Clone)]
pub struct Composer {
    inner: Arc<ComposerInner>,
}

impl std::fmt::Debug for Composer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("entry_rollup_id", &self.inner.entry_rollup_id)
            .field("rollups", &self.inner.rollups.len())
            .finish()
    }
}

impl Composer {
    /// Start building a new composer pinned to `entry_rollup_id`.
    ///
    /// Use [`ComposerBuilder::entry`] to register the entry-chain
    /// client, [`ComposerBuilder::rollup`] for each follower, then
    /// [`ComposerBuilder::build`] to finalize. The resulting
    /// `Composer` is immutable.
    #[must_use]
    pub fn builder(entry_rollup_id: RollupId) -> ComposerBuilder {
        ComposerBuilder::new(entry_rollup_id)
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
    pub async fn simulate_and_resolve(&self, raw_tx: &[u8]) -> ComposerResult<Composition> {
        // Default entry selection: the pinned entry rollup + its client.
        // Per-composition entry selection (A1) goes through
        // `simulate_and_resolve_recorded_for`.
        self.simulate_and_resolve_recorded_for(
            self.inner.entry_rollup_id,
            self.inner.entry.as_ref(),
            raw_tx,
        )
        .await
        .map(|(composition, _recorded)| composition)
    }

    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve) but
    /// with an explicitly-chosen entry — `entry_id` + the `entry_client`
    /// that runs source simulation — and ALSO returning the builder's
    /// `recorded[..]` (preorder dispatched cross-chain calls with
    /// resolved outcomes). Callers that need a
    /// call's REAL `return_data` (e.g. the inbound L1→L2 delivery, which
    /// builds the byte-locked `executeIncomingCrossChainCall` system tx)
    /// use this; the [`Composition`] alone doesn't carry per-call return
    /// data verbatim. The explicit entry lets ONE composer compose either
    /// direction — `(L1, L1 client)` for an inbound L1→L2 call, `(L2, L2
    /// client)` for an outbound L2→L1 call — picked per tx by the drain.
    /// The dispatch rollup map (Phase 1) is the composer's full
    /// registration set, unchanged.
    ///
    /// # Errors
    ///
    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve).
    pub async fn simulate_and_resolve_recorded_for(
        &self,
        entry_id: RollupId,
        entry_client: &(dyn EntryChainClient + Send + Sync),
        raw_tx: &[u8],
    ) -> ComposerResult<(Composition, Vec<ExecutedAction>)> {
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
        let mut rollups: HashMap<RollupId, Rollup> =
            HashMap::with_capacity(self.inner.rollups.len());
        for (rollup_id, reg) in &self.inner.rollups {
            let initial_state_root = self
                .inner
                .root_reader
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

        // Phase 2 — compose: drive source simulation (which dispatches
        // every detected proxy call back into the builder), then
        // finalize. `recorded` carries the resolved per-call outcomes
        // (return_data) the byte-locked inbound delivery needs,
        // captured BEFORE `finalize` consumes the builder.
        let mut builder = CompositionBuilder::new(entry_id, rollups);
        entry_client
            .simulate_source_tx(raw_tx.to_vec(), &mut builder)
            .await
            .map_err(crate::error::CompositionError::from)?;
        let recorded = builder.recorded().to_vec();
        let composition = builder.finalize(raw_tx).await?;

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

    use crate::error::ExecutorResult;
    use crate::executor::TargetExecutionSession;

    struct FakeClient;

    #[async_trait::async_trait]
    impl ChainClient for FakeClient {
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            unimplemented!("composer misconfigured-state tests never open a session")
        }
    }

    #[async_trait::async_trait]
    impl EntryChainClient for FakeClient {
        async fn simulate_source_tx(
            &self,
            _raw_tx: Vec<u8>,
            _dispatcher: &mut CompositionBuilder,
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
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            unimplemented!("composer misconfigured-state tests never open a session")
        }
    }

    fn builder() -> ComposerBuilder {
        Composer::builder(RollupId(0))
    }

    fn target_config() -> TargetConfig {
        TargetConfig {
            proxy_lookup: ProxyLookupConfig {
                contract_address: Address::ZERO,
                authorized_proxies_slot: 0,
            },
            dialect: ChainDialect::EvmL2Style,
        }
    }

    fn entry_arc() -> Arc<dyn EntryChainClient + Send + Sync> {
        Arc::new(FakeClient)
    }

    fn root_reader_arc() -> Arc<dyn CommittedRootReader + Send + Sync> {
        Arc::new(FakeClient)
    }

    fn follower_arc() -> Arc<dyn ChainClient + Send + Sync> {
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
}
