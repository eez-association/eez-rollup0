#![deny(missing_docs)]
#![allow(
    clippy::result_large_err,
    reason = "Error structs carry a Backtrace plus an enum kind — \
              large by design. The struct-plus-backtrace shape is the \
              intentional M-ERRORS-CANONICAL-STRUCTS pattern."
)]

//! Chain-agnostic traits and types for cross-chain composition.
//!
//! The abstract layer beneath every chain-specific implementation.
//! `eez-evm` is the EVM instantiation; sibling crates implement
//! the same traits for non-EVM chain families. Nothing in this crate
//! knows about Solidity, ABI, keccak, alloy, reth, or revm. It defines
//! what a cross-chain call *is*, how chains communicate, and how a
//! composition is built — in terms generic enough for any chain family
//! to implement.
//!
//! # Where it fits
//!
//! ```text
//! eez-protocol        ← this crate (chain-agnostic traits + generic orchestrator)
//!         ↑
//! eez-evm             (EvmProtocol: ABI + entry building)
//!         ↑
//! eez-evm-inspector             eez-evm-grpc
//!         ↑                                    ↑
//!         └──────────── eez-composer ───────────┘
//!                       (reth integration)
//! ```
//!
//! `eez-evm-inspector` and `eez-evm-grpc` both depend
//! only on `eez-evm`; they do not depend on each other.
//! `eez-composer` is the only crate that imports both.
//!
//! # How composition works
//!
//! One source transaction produces one [`Composition<P>`] — the
//! chain-agnostic shape of "here's what to post on the source chain,
//! and here's what to run on each target". The orchestrator runs four
//! phases per source tx:
//!
//! ```text
//!   raw source tx ─┐
//!                  ▼
//!   ┌──────────────────────────────────────────────────────────────┐
//!   │ Composer::simulate_and_resolve                               │
//!   │                                                              │
//!   │  1. clone the sealed rollup map from `Arc<ComposerInner>`    │
//!   │  2. assemble HashMap<RollupId, Rollup<P>>                    │
//!   │     (read stored initial roots; sessions open lazily)        │
//!   │  3. compose_transaction(protocol, entry_client, raw_tx,      │
//!   │     entry_id, rollups):                                      │
//!   │       ┌────────────────────────────────────────────────────┐ │
//!   │       │ CompositionBuilder::new(entry_id, rollups)         │ │
//!   │       │ entry.simulate_source_tx(raw_tx, &mut b)           │ │
//!   │       │   └─ source sim detects proxy → b.dispatch_call    │ │
//!   │       │         → rollup.session.execute(req, &mut b)      │ │
//!   │       │         → preorder open_call / close_call          │ │
//!   │       │ b.finalize(&protocol):                             │ │
//!   │       │   per-target CCM verify → patch terminal root      │ │
//!   │       │   protocol.build_batch(recorded, attribution, …)   │ │
//!   │       │     × source + N targets                           │ │
//!   │       └────────────────────────────────────────────────────┘ │
//!   │  4. log + return Composition<P>                              │
//!   └──────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//!                    Composition<P> { source, targets: Vec<_> }
//! ```
//!
//! # Example
//!
//! The canonical flow — build a composer once, register source +
//! targets, simulate one source tx at a time. `MyProtocol`,
//! `MySourceClient`, `MyTargetClient` come from a concrete chain
//! family (e.g. `eez-evm`).
//!
//! ```ignore
//! use eez_protocol::{
//!     Composer, ProxyLookupConfig, RollupId, TargetConfig,
//!     DEFAULT_CCM_GAS_LIMIT,
//! };
//!
//! // Built once at startup. Entry rollup id is pinned at builder
//! // construction; the builder seals on `.build()`.
//! let composer = Composer::builder(MyProtocol, RollupId(0))
//!     .entry(entry_client, TargetConfig {
//!         ccm_address,
//!         system_address,
//!         ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
//!         proxy_lookup: ProxyLookupConfig { /* ... */ },
//!     })
//!     .rollup(RollupId(1), follower_client, TargetConfig {
//!         /* ... */
//!     })
//!     .build()?;
//!
//! // Per source tx: detect cross-chain calls, verify CCM, emit the
//! // ready-to-broadcast composition.
//! // `raw_tx: &[u8]` — the user's signed source transaction bytes.
//! let composition = composer.simulate_and_resolve(&raw_tx).await?;
//!
//! // composition.source and composition.targets carry both entries
//! // and pre-encoded calldata; wrap each payload in a tx of your
//! // choice to finalize.
//! # Ok::<_, eez_protocol::ComposerError>(())
//! ```
//!
//! For a runnable EVM instantiation of the above, substitute
//! `EvmProtocol` (from `eez-evm`) + `LocalChainClient`
//! (from `eez-composer`) — see `eez-composer/src/main.rs` for the
//! full wiring.
//!
//! # Trait cheat-sheet
//!
//! | Trait | Role | Dyn-compatible? |
//! |---|---|---|
//! | [`ChainProtocol`]          | per-chain encoding + entry building              | **No** — static dispatch by design (associated types + `Self` in args) |
//! | [`ChainClient`]            | uniform per-rollup surface (session + batch sim) | Yes (`#[async_trait]`) |
//! | [`EntryChainClient`]       | entry-only: source sim + stored state-root reads | Yes (supertrait of [`ChainClient`]) |
//! | [`TargetExecutionSession`] | one open per-tx session; accumulates state       | Yes |
//! | [`Dispatcher`]             | dispatch surface an inspector calls on detection | Yes |
//!
//! The concrete generic [`CompositionBuilder<P>`] is **not a trait** —
//! one struct, three methods:
//! [`new`](CompositionBuilder::new) seals the target set,
//! [`dispatch_call`](CompositionBuilder::dispatch_call) routes + records
//! during source simulation,
//! [`finalize`](CompositionBuilder::finalize) consumes the builder and
//! produces a [`Composition`]. The single struct carries the
//! "what was dispatched is what gets composed" invariant in its type
//! shape — there is no second trait to keep in sync.
//!
//! # Ownership at runtime
//!
//! Who owns what during one call to `simulate_and_resolve`:
//!
//! ```text
//!   Composer<P>  ─owns─▶  Arc<ComposerInner<P>>
//!                              │
//!                              ├─ entry_rollup_id: RollupId                          (pinned at builder()
//!                              ├─ entry:           EntryRegistration<P>              (ComposerBuilder::entry)
//!                              │                   └─ client: Arc<dyn EntryChainClient + Send + Sync>
//!                              └─ rollups:         HashMap<RollupId, Arc<_>>         (ComposerBuilder::rollup)
//!                                                                 │
//!                                                                 ├─ client: Arc<dyn ChainClient + Send + Sync>
//!                                                                 └─ config: TargetConfig<P>
//!
//!   Per call:  CompositionBuilder<P>  ─owns─▶  HashMap<RollupId, Rollup<P>>    ─ sealed at construction
//!                                     ─owns─▶  Vec<RecordedCall<P>>            ─ grows during dispatch
//!
//!              Rollup<P>  ─owns─▶  Arc<dyn ChainClient + Send + Sync>
//!                         ─owns─▶  Option<Box<dyn TargetExecutionSession + Send>>  (lazy-open)
//!                         ─owns─▶  TargetConfig<P>
//!                         ─owns─▶  initial_state_root: [u8; 32]
//! ```
//!
//! Clone semantics: `Composer<P>` is cheap to clone (`Arc` bump);
//! target clients are shared across plans via `Arc::clone` (no
//! reallocations). [`CompositionBuilder`] and [`Rollup`] are owned
//! and consumed.
//!
//! # Module map + reading order
//!
//! Modules are listed here in the order that makes this crate
//! easiest to learn. The first four trace the composition pipeline
//! "what I build" → "what it uses"; `protocol` is the sibling-chain
//! extension point, rarely needed by users of the crate; the rest
//! are reference types.
//!
//! 1. [`compose`] — [`compose_transaction`], the stateless canonical
//!    three-line pipeline. Read this first — every other module
//!    exists to serve it.
//! 2. [`composer`] — [`Composer<P>`], the long-lived handle that
//!    caches clients across calls and delegates to
//!    [`compose_transaction`].
//! 3. [`composition`] — [`CompositionBuilder<P>`] + [`Rollup<P>`] +
//!    [`Dispatcher`], the concrete engine that `compose_transaction`
//!    constructs.
//! 4. [`executor`] — [`ChainClient`] / [`EntryChainClient`] /
//!    [`CommittedRootReader`] / [`TargetExecutionSession`], the traits
//!    the builder calls (local reth, gRPC, test fakes plug in here).
//! 5. [`protocol`] — the [`ChainProtocol`] trait, the static-dispatch
//!    extension point a **sibling chain family** implements. Users
//!    of the crate rarely read this; implementers of a new chain
//!    family land here first.
//! 6. [`types`] — [`RecordedCall<P>`], [`ExecutionOutcome`],
//!    [`Composition<P>`], and its `SourceComposition` / `TargetComposition` halves.
//! 7. [`rolling_hash`] — `EntryRollingHash` / `StaticCallRollingHash`
//!    commitment folds (invariant 6).
//! 8. [`checkpoint`] — [`ExecutionCheckpoint<O, W>`] (overlay + witness).
//! 9. [`rollup_id`] — [`RollupId`] newtype.
//! 10. [`error`] — four error families; see the module doc for the
//!    wrapping shape.
//!
//! # Design constraints (explicit, so they don't sneak up on readers)
//!
//! - **32-byte state roots.** [`ExecutionOutcome`] and
//!   [`ExecutionCheckpoint`] carry `[u8; 32]` roots. A chain family
//!   using a different hash size would need a different checkpoint
//!   type; not a drop-in.
//! - **Static dispatch for `ChainProtocol`.** Not dyn-compatible: six
//!   associated types plus `Self` in argument positions. This is
//!   deliberate — `ChainProtocol` encodes the chain family, which is
//!   fixed at compile time per binary. Every other trait uses
//!   `#[async_trait]` and IS dyn-compatible, which is load-bearing for
//!   transport polymorphism and heterogeneous N-target dispatch.
//! - **`Send + Sync` on clients; `Send` on sessions.** Clients are
//!   long-lived and shared across tasks; sessions are owned per
//!   transaction and single-threaded.
//! - **Serde on `Overlay` + `Witness`.** Required for gRPC transport
//!   and proof-system handoff.
//! - **No tokio dep.** [`Composer<P>`] uses [`std::sync`] primitives
//!   for interior mutability; the crate is runtime-agnostic and works
//!   under any executor that spawns `#[async_trait]` futures.

pub mod checkpoint;
pub mod compose;
pub mod composer;
pub mod composition;
pub mod error;
pub mod executor;
pub mod proof_plan;
pub mod protocol;
pub mod rolling_hash;
pub mod rollup_id;
pub mod types;

/// Shared test doubles. Gated behind the `testing` Cargo feature for
/// downstream consumers; always available to this crate's own tests.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

mod assertions;

#[doc(inline)]
pub use checkpoint::ExecutionCheckpoint;
#[doc(inline)]
pub use compose::compose_transaction;
#[doc(inline)]
pub use composer::{
    Composer, ProxyLookupConfig, SourceAttribution, TargetConfig, DEFAULT_CCM_GAS_LIMIT,
};
#[doc(inline)]
pub use composition::{CompositionBuilder, Dispatcher, Rollup};
#[doc(inline)]
pub use error::{
    ComposerError, ComposerErrorKind, ComposerResult, CompositionError, CompositionErrorKind,
    CompositionResult, ExecutorError, ExecutorErrorKind, ExecutorResult, ProtocolError,
    ProtocolErrorKind, ProtocolResult,
};
#[doc(inline)]
pub use executor::{
    ChainClient, CommittedRootReader, EntryChainClient, ExecutionRequest, ExecutionResponse,
    ProtocolCheckpoint, SessionSnapshot, TargetBatchSimulation, TargetExecutionSession,
    TargetTransaction, TargetVerificationContext,
};
#[doc(inline)]
pub use proof_plan::{
    ProofPlan, ProofPlanInvariantError, ProofPlanResolver, RollupProofAssignment,
    TimestampAndBlockHash,
};
#[doc(inline)]
pub use protocol::ChainProtocol;
#[doc(inline)]
pub use rolling_hash::{EntryRollingHash, StaticCallRollingHash};
#[doc(inline)]
pub use rollup_id::RollupId;
#[doc(inline)]
pub use types::{
    Composition, ExecutionOutcome, RecordedCall, SourceComposition, StaticMeta, TargetComposition,
};
