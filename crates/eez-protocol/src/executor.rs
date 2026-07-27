//! Chain-client and execution-session interfaces.
//!
//! The composer talks to every registered rollup through two traits:
//! the uniform [`ChainClient`] (all rollups) and the entry-only
//! [`EntryChainClient`] (extends `ChainClient`; only the rollup
//! designated as composition entry implements it). Nested cross-chain
//! dispatch goes through the borrowed
//! [`CompositionBuilder`](crate::composition::CompositionBuilder).
//!
//! All three traits are synchronous — every in-tree impl does
//! in-process reth/EVM work with no I/O to await. The composer stores
//! clients as trait objects so transports (local reth, test fake) can
//! swap without upstream changes.
//!
//! # Capability split
//!
//! ```text
//!   ChainClient                every registered rollup
//!       │
//!       ├─ begin_execution_session  opens a stateful session for one
//!       │                           source transaction's worth of
//!       │                           cross-chain calls
//!       └─ simulate_transactions    atomic batch sim for CCM-verify
//!                                   (loadExecutionTable + execute*)
//!
//!   EntryChainClient : ChainClient    entry rollup only
//!       │
//!       └─ simulate_source_tx       runs source simulation, dispatching
//!                                   every detected proxy call through
//!                                   a borrowed CompositionBuilder
//!
//!   CommittedRootReader : ChainClient   committed-root host (L1) only
//!       │
//!       └─ stored_target_state_root reads `EEZ.rollups[id].stateRoot`
//!                                   (the invariant-6 anchor); registered
//!                                   via `ComposerBuilder::root_reader`
//!
//!   TargetExecutionSession      one per builder; the slot drain may
//!                               chain it across source txs (F1)
//!       │
//!       ├─ execute              one call, owned by the builder
//!       └─ take_checkpoint      drain accumulated state
//! ```
//!
//! Follower clients (gRPC peers, non-entry local clients) implement
//! `ChainClient` only. Attempting to register a non-entry client via
//! [`ComposerBuilder::entry`](crate::composer::ComposerBuilder::entry)
//! fails to compile because the bound requires `EntryChainClient`.

use alloy_primitives::{Address, Bytes, U256};

use crate::checkpoint::ExecutionCheckpoint;
use crate::composition::CompositionBuilder;
use crate::error::ExecutorResult;
#[allow(
    unused_imports,
    reason = "ExecutorError / its Kind enum used in rustdoc intra-doc links"
)]
use crate::error::{ExecutorError, ExecutorErrorKind};
use crate::rollup_id::RollupId;
use crate::types::ExecutionOutcome;

/// Request for a single cross-chain execution on the target chain.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    /// Contract the target-chain call lands on. Spec: `Action.targetAddress`.
    pub target_address: Address,
    /// Encoded calldata for the target-chain call. Spec: `Action.data`.
    pub data: Bytes,
    /// Native value sent with the call. Spec: `Action.value`.
    pub value: U256,
    /// Original caller on the source chain — becomes `msg.sender` in the
    /// target invocation. Spec: `Action.sourceAddress`.
    pub source_address: Address,
    /// Rollup ID of the source chain; used for routing and action-hash
    /// derivation. Spec: `Action.sourceRollupId`.
    pub source_rollup_id: RollupId,
}

/// Full executor response: lean outcome + checkpoint for continuation/proving.
#[derive(Clone)]
pub struct ExecutionResponse {
    /// Lean result used to synthesize the source-side call's return
    /// value for the source transaction that triggered it.
    pub outcome: ExecutionOutcome,
    /// Accumulated state (overlay + optional witness) for continuation
    /// across calls and for proof-system handoff.
    pub checkpoint: ExecutionCheckpoint,
}

// Manual Debug — intentional: print a placeholder for `checkpoint` —
// real checkpoints are large (overlay + witness state) and would
// flood logs.
impl std::fmt::Debug for ExecutionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionResponse")
            .field("outcome", &self.outcome)
            .field("checkpoint", &"<ExecutionCheckpoint>")
            .finish()
    }
}

/// One target-chain transaction for batch simulation.
///
/// This is owned rather than borrowed so it maps cleanly to future transport
/// implementations. Local reth-backed clients can still execute it in-process.
#[derive(Debug, Clone)]
pub struct TargetTransaction {
    /// Address to set as `msg.sender` in the simulation.
    pub caller: Address,
    /// Contract the call lands on.
    pub destination: Address,
    /// Encoded calldata.
    pub calldata: Bytes,
    /// Native value attached to the call.
    pub value: U256,
    /// Maximum gas this transaction may consume.
    pub gas_limit: u64,
}

/// Result of simulating an ordered target-chain transaction batch.
///
/// `per_tx_roots[i]` is the cumulative post-state root after transaction
/// `i` committed. `final_state_root` equals `per_tx_roots.last()`. Both
/// fields are populated so consumers that only need the terminal root
/// (legacy CCM-verify overwrite path) can keep using `final_state_root`
/// while nested-composition attribution consumes `per_tx_roots` for
/// per-entry state-delta chaining (upstream's invariant 6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetBatchSimulation {
    /// State root after all batch transactions committed in order.
    pub final_state_root: [u8; 32],
    /// Cumulative post-state root after each transaction in the batch.
    /// `per_tx_roots[i]` is the state root once `txs[0..=i]` are
    /// committed. Empty batches are rejected with
    /// [`ExecutorErrorKind::EmptyBatch`],
    /// so this vector is always non-empty when `simulate_transactions`
    /// returns `Ok`.
    pub per_tx_roots: Vec<[u8; 32]>,
}

/// Inputs for the CCM-verification system transactions
/// (`loadExecutionTable` + `executeIncomingCrossChainCall`) that
/// [`CompositionBuilder::finalize`](crate::composition::CompositionBuilder::finalize)
/// runs when a target plan opts in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetVerificationContext {
    /// Caller for the two system txs (target chain's configured system address).
    pub system_address: Address,
    /// Destination for the two system txs (target chain's CCM entrypoint).
    pub entrypoint_address: Address,
    /// Gas limit for each system tx.
    pub gas_limit: u64,
}

/// Stateful execution session driving target-chain calls during
/// source simulation.
///
/// One session per builder, lazily opened; the slot drain may chain a
/// live session across consecutive source txs in the same slot (F1 —
/// see [`CompositionBuilder::with_sessions`](crate::composition::CompositionBuilder::with_sessions));
/// sessions never outlive their slot. Accumulates state across calls;
/// `&mut self` on every method reflects that.
///
/// `Send` only (source simulation is single-threaded). No `'static`
/// bound on the trait itself so the source simulator can borrow a
/// session as `&'a mut (dyn TargetExecutionSession + 'a)`.
///
/// The `dispatcher` argument on [`execute`](Self::execute) supports
/// nested cross-chain dispatch: a target-session inspector can call
/// back into the composer through `dispatcher` to route a nested
/// proxy call.
pub trait TargetExecutionSession: Send {
    /// Execute a single call on the target chain.
    ///
    /// `dispatcher` is consumed by nested cross-chain dispatch.
    ///
    /// Returns outcome (for source simulation) + checkpoint (for continuation).
    ///
    /// # Errors
    ///
    /// Returns any [`ExecutorError`] depending on the impl — local
    /// impls surface [`ExecutorErrorKind::Evm`] /
    /// [`ExecutorErrorKind::Provider`]; the gRPC impl surfaces
    /// [`ExecutorErrorKind::Transport`] / [`ExecutorErrorKind::Serde`] /
    /// [`ExecutorErrorKind::Missing`].
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionResponse>;

    /// Capture an opaque snapshot of the session's current state. The
    /// returned box is fed back to [`rollback`](Self::rollback) to
    /// restore the session to its pre-call state. Drop the snapshot
    /// to commit forward (no-op).
    ///
    /// Used by the composer's revertSpan path:
    /// [`CompositionBuilder::open_call`] snapshots the target session
    /// BEFORE delegating to `execute`, stashes the snapshot keyed by
    /// call idx, and either drops it (success path) or rolls back
    /// (revert-span path) when
    /// [`CompositionBuilder::annotate_revert_span`] fires.
    ///
    /// The snapshot is type-erased via `Box<dyn Any + Send>` so the
    /// composer can stash it without naming `Self::Snapshot`. Each
    /// impl downcasts privately inside `rollback`.
    ///
    /// # Errors
    ///
    /// Implementation-dependent — `LocalChainClient` is infallible
    /// (deep-clones revm `State<DB>`'s 7 fields); the gRPC impl can
    /// surface [`ExecutorErrorKind::Transport`] /
    /// [`ExecutorErrorKind::Missing`].
    fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot>;

    /// Restore the session to the state captured by `snapshot`. The
    /// snapshot must have come from this session's `checkpoint` call;
    /// passing a snapshot from a different session is an error.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Decode`] if the snapshot's
    /// concrete type does not match this session's snapshot shape.
    fn rollback(&mut self, snapshot: SessionSnapshot) -> ExecutorResult<()>;

    /// Retrieve the accumulated witness/overlay checkpoint after all
    /// calls — the prover-facing handoff distinct from the rollback
    /// snapshot above. Returns `None` if no calls have been executed.
    fn take_checkpoint(&mut self) -> Option<ExecutionCheckpoint>;
}

/// Type-erased session snapshot. Each [`TargetExecutionSession`] impl
/// chooses its own concrete `Box<dyn Any + Send>`-wrapped state.
/// Holding this type alone is enough for composition.rs to stash and
/// hand snapshots back to the originating session without naming the
/// concrete type.
pub type SessionSnapshot = Box<dyn std::any::Any + Send>;

/// Uniform chain-client interface every registered rollup satisfies.
///
/// `Composer` talks to every rollup through this trait. The entry rollup
/// additionally implements [`EntryChainClient`] (source-sim capability);
/// the chain hosting the canonical committed-root storage (L1 in this
/// protocol) additionally implements [`CommittedRootReader`].
///
/// Stored as `Arc<dyn ChainClient + Send + Sync>` in the composer's
/// rollup map.
pub trait ChainClient: Send + Sync + 'static {
    /// Read this chain's own latest block-header `stateRoot`.
    ///
    /// Orthogonal to upstream-invariant-6 anchoring (which uses
    /// [`CommittedRootReader::stored_target_state_root`] against L1's
    /// canonical storage). Used for diagnostics and health checks.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Provider`] if the underlying state
    /// provider is inaccessible; [`ExecutorErrorKind::Unavailable`] when
    /// the implementation does not (or cannot) report its own header
    /// (e.g. a remote gRPC peer that does not expose this).
    fn current_state_root(&self) -> ExecutorResult<[u8; 32]>;

    /// Create a fresh stateful execution session. The slot drain may
    /// keep the returned session alive across consecutive source txs
    /// in the same slot (F1); it never outlives its slot.
    ///
    /// # Errors
    ///
    /// Returns any [`ExecutorError`] depending on impl — local surfaces
    /// [`ExecutorErrorKind::Provider`] / [`ExecutorErrorKind::Evm`] /
    /// [`ExecutorErrorKind::Missing`]; gRPC surfaces
    /// [`ExecutorErrorKind::Transport`].
    fn begin_execution_session(&self) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>>;

    /// Simulate an ordered batch of target-chain transactions on a fresh
    /// target state. Implementations must commit each successful transaction
    /// in order and return the final state root.
    ///
    /// # Errors
    ///
    /// Returns any [`ExecutorError`] depending on impl — local surfaces
    /// [`ExecutorErrorKind::TargetTransactionReverted`] (contract-level
    /// revert) or [`ExecutorErrorKind::Evm`] (internal execution
    /// failure); gRPC surfaces [`ExecutorErrorKind::Transport`] /
    /// [`ExecutorErrorKind::Serde`].
    fn simulate_transactions(
        &self,
        txs: &[TargetTransaction],
    ) -> ExecutorResult<TargetBatchSimulation>;
}

/// Source-simulation capability. Supertrait of [`ChainClient`].
///
/// Implemented only by the client for the rollup registered as the
/// composition entry point (via [`ComposerBuilder::entry`](crate::composer::ComposerBuilder::entry)).
/// Follower clients implement [`ChainClient`] only; gRPC peers
/// structurally cannot serve source simulation (the inspector runs
/// in-process against live EVM state), so the split mirrors the wire
/// reality.
///
/// Stored as `Arc<dyn EntryChainClient + Send + Sync>` in the
/// composer's `entry` slot. Trait upcasting (Rust 1.86+)
/// re-registers it as `Arc<dyn ChainClient>` in the rollup map.
pub trait EntryChainClient: ChainClient {
    /// Simulate a source-chain transaction, dispatching every detected
    /// cross-chain proxy call through `dispatcher`.
    ///
    /// Takes `raw_tx: Vec<u8>` (owned) so the impl can decode without
    /// holding a borrow on the caller's buffer across async boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Decode`] if the raw tx cannot be
    /// decoded. Returns [`ExecutorErrorKind::Provider`] if the source
    /// state provider is inaccessible. Returns [`ExecutorErrorKind::Evm`]
    /// if source EVM execution fails. Propagates any [`ExecutorError`]
    /// surfaced by `dispatcher` during proxy call dispatch.
    fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()>;
}

/// Committed-state-root reader capability. Supertrait of [`ChainClient`].
///
/// Implemented only by clients connected to the chain that hosts the
/// canonical committed-root storage. In this protocol that is L1's
/// `EEZ.sol` — `rollups[id].stateRoot` is the value
/// `postAndVerifyBatch` will check
/// `entry[i].stateDeltas[j].currentState` against (upstream's invariant 6).
///
/// Implementations:
/// - Local L1 client (whether registered as entry or as a follower in
///   L2-as-entry topology) — reads its own EVM storage.
/// - gRPC client whose remote peer is L1 — wires through a
///   `GetStateRoot` RPC.
///
/// `Composer::builder` requires exactly one
/// [`std::sync::Arc<dyn CommittedRootReader>`] via [`ComposerBuilder::root_reader`](crate::composer::ComposerBuilder::root_reader);
/// [`Composer::simulate_and_resolve`](crate::composer::Composer::simulate_and_resolve) Phase 1 reads ALL rollups'
/// initial roots through this reader, including the entry rollup's
/// own. The protocol expects committed roots — `EEZ.sol`'s
/// `_applyStateDeltas` reverts `StateRootMismatch(rollupId)` for every
/// delta in a batch — not chain-header self-reports.
pub trait CommittedRootReader: ChainClient {
    /// Read what the canonical committed-root storage currently has
    /// for `rollup_id`. For this protocol that is
    /// `EEZ.rollups(rollup_id).stateRoot` on L1.
    ///
    /// This is the upstream-invariant-6 anchor —
    /// `postAndVerifyBatch` enforces the
    /// returned value matches each delta's `currentState` for every
    /// state delta in the batch.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Provider`] if the underlying state
    /// provider is inaccessible; [`ExecutorErrorKind::Transport`] for
    /// gRPC implementations; [`ExecutorErrorKind::Unavailable`] if the
    /// implementation cannot serve this capability (e.g. a non-L1
    /// node — though in practice such an impl would not be wrapped as
    /// `Arc<dyn CommittedRootReader>` in the first place).
    fn stored_target_state_root(&self, rollup_id: RollupId) -> ExecutorResult<[u8; 32]>;
}
