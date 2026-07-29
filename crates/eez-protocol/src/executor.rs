//! Chain-client and execution-session interfaces.
//!
//! The composer talks to every registered rollup through one trait,
//! [`ChainClient`]. Role-specific operations (`simulate_source_tx` for
//! the entry rollup, `stored_target_state_root` for the committed-root
//! host) have default implementations that refuse with
//! [`ExecutorError::Unavailable`], so a misregistered client fails
//! loudly at the first role-specific call. Nested cross-chain
//! dispatch goes through the borrowed
//! [`CompositionBuilder`].
//!
//! Both traits are `#[async_trait]` — one heap allocation per call
//! in exchange for dyn-compatibility. Native `async fn in trait` is
//! not dyn-compatible today, and the composer stores clients as trait
//! objects so transports (local reth, gRPC peer, test fake) can swap
//! without upstream changes.

use alloy_primitives::{Address, Bytes, U256};

use crate::composition::CompositionBuilder;
#[allow(
    unused_imports,
    reason = "ExecutorError used in rustdoc intra-doc links"
)]
use crate::error::ExecutorError;
use crate::error::ExecutorResult;
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
#[async_trait::async_trait]
pub trait TargetExecutionSession: Send {
    /// Execute a single call on the target chain.
    ///
    /// `dispatcher` is consumed by nested cross-chain dispatch.
    ///
    /// Returns the outcome for source simulation.
    ///
    /// # Errors
    ///
    /// Returns any [`ExecutorError`] depending on the impl — local
    /// impls surface [`ExecutorError::Evm`] /
    /// [`ExecutorError::Provider`]; the gRPC impl surfaces
    /// [`ExecutorError::Transport`] / [`ExecutorError::Serde`] /
    /// [`ExecutorError::Missing`].
    async fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome>;

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
    /// surface [`ExecutorError::Transport`] /
    /// [`ExecutorError::Missing`].
    async fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot>;

    /// Restore the session to the state captured by `snapshot`. The
    /// snapshot must have come from this session's `checkpoint` call;
    /// passing a snapshot from a different session is an error.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Decode`] if the snapshot's
    /// concrete type does not match this session's snapshot shape.
    async fn rollback(&mut self, snapshot: SessionSnapshot) -> ExecutorResult<()>;
}

/// Type-erased session snapshot. Each [`TargetExecutionSession`] impl
/// chooses its own concrete `Box<dyn Any + Send>`-wrapped state.
/// Holding this type alone is enough for composition.rs to stash and
/// hand snapshots back to the originating session without naming the
/// concrete type.
pub type SessionSnapshot = Box<dyn std::any::Any + Send>;

/// Uniform chain-client interface every registered rollup satisfies.
///
/// `Composer` talks to every rollup through this trait. Role-specific
/// methods (`simulate_source_tx`, `stored_target_state_root`) default
/// to a loud [`ExecutorError::Unavailable`] refusal.
///
/// Stored as `Arc<dyn ChainClient + Send + Sync>` in the composer's
/// rollup map.
#[async_trait::async_trait]
pub trait ChainClient: Send + Sync + 'static {
    /// Read this chain's own latest block-header `stateRoot`.
    ///
    /// Orthogonal to upstream-invariant-6 anchoring (which uses
    /// [`stored_target_state_root`](Self::stored_target_state_root)
    /// against L1's canonical storage). Used for diagnostics and
    /// health checks.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Provider`] if the underlying state
    /// provider is inaccessible; [`ExecutorError::Unavailable`] when
    /// the implementation does not (or cannot) report its own header
    /// (e.g. a remote gRPC peer that does not expose this).
    async fn current_state_root(&self) -> ExecutorResult<[u8; 32]>;

    /// Create a fresh stateful execution session. The slot drain may
    /// keep the returned session alive across consecutive source txs
    /// in the same slot (F1); it never outlives its slot.
    ///
    /// # Errors
    ///
    /// Returns any [`ExecutorError`] depending on impl — local surfaces
    /// [`ExecutorError::Provider`] / [`ExecutorError::Evm`] /
    /// [`ExecutorError::Missing`]; gRPC surfaces
    /// [`ExecutorError::Transport`].
    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>>;

    /// Simulate a source-chain transaction, dispatching every detected
    /// cross-chain proxy call through `dispatcher`. Entry-role clients
    /// only; the default refuses with
    /// [`ExecutorError::Unavailable`] so follower registrations
    /// fail loudly if they ever reach source simulation.
    ///
    /// Takes `raw_tx: Vec<u8>` (owned) so the impl can decode without
    /// holding a borrow on the caller's buffer across async boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Decode`] if the raw tx cannot be
    /// decoded. Returns [`ExecutorError::Provider`] if the source
    /// state provider is inaccessible. Returns [`ExecutorError::Evm`]
    /// if source EVM execution fails. Propagates any [`ExecutorError`]
    /// surfaced by `dispatcher` during proxy call dispatch.
    async fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        let _ = (raw_tx, dispatcher);
        Err(ExecutorError::Unavailable(
            "simulate_source_tx: not an entry-role client".into(),
        ))
    }

    /// Read what the canonical committed-root storage currently has
    /// for `rollup_id` — `EEZ.rollups(rollup_id).stateRoot` on L1, the
    /// upstream-invariant-6 anchor `postAndVerifyBatch` enforces
    /// against each delta's `currentState`. Only clients connected to
    /// the chain hosting that storage implement this; the default
    /// refuses with [`ExecutorError::Unavailable`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Provider`] if the underlying state
    /// provider is inaccessible; [`ExecutorError::Transport`] for
    /// gRPC implementations; [`ExecutorError::Unavailable`] if the
    /// client does not host the committed-root storage.
    async fn stored_target_state_root(&self, rollup_id: RollupId) -> ExecutorResult<[u8; 32]> {
        let _ = rollup_id;
        Err(ExecutorError::Unavailable(
            "stored_target_state_root: client does not host the committed-root storage".into(),
        ))
    }
}
