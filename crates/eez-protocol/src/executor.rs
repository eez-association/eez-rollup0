//! Chain-client and execution-session interfaces.
//!
//! The composer talks to every registered rollup through one trait,
//! [`ChainClient`]. The role-specific `simulate_source_tx` (entry
//! rollup only) has a default implementation that refuses with
//! [`ExecutorErrorKind::Unavailable`], so a misregistered client fails
//! loudly at the first role-specific call. Nested cross-chain
//! dispatch goes through the borrowed
//! [`CompositionBuilder`].
//!
//! Both traits are synchronous and natively dyn-compatible — clients
//! are stored as `Arc<dyn ChainClient>` and sessions as `Box<dyn
//! TargetExecutionSession>`. Cross-chain execution is all in-process
//! (local reth), so there is no I/O to await.

use alloy_primitives::{Address, Bytes, U256};

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

/// Stateful execution session driving target-chain calls during
/// source simulation.
///
/// One session per builder, lazily opened; sessions never outlive their
/// slot. Accumulates state across calls; `&mut self` on every method
/// reflects that.
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
    /// Execute a single call on the target chain. `dispatcher` is
    /// consumed by nested cross-chain dispatch. Returns the outcome for
    /// source simulation.
    ///
    /// # Errors
    ///
    /// Surfaces [`ExecutorErrorKind::Evm`] / [`ExecutorErrorKind::Provider`].
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome>;

    /// Capture an opaque snapshot of the session's current state, fed
    /// back to [`rollback`](Self::rollback) to restore it. Drop the
    /// snapshot to commit forward (no-op).
    ///
    /// Used by the revertSpan path: [`CompositionBuilder::open_call`]
    /// snapshots before `execute`, then drops (success) or rolls back
    /// (revert) when [`CompositionBuilder::annotate_revert_span`] fires.
    /// Type-erased as `Box<dyn Any + Send>`; each impl downcasts inside
    /// `rollback`.
    ///
    /// # Errors
    ///
    /// Implementation-dependent.
    fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot>;

    /// Restore the session to the state captured by `snapshot` (which
    /// must have come from this session's `checkpoint`).
    ///
    /// # Errors
    ///
    /// [`ExecutorErrorKind::Decode`] if the snapshot's concrete type
    /// does not match this session's snapshot shape.
    fn rollback(&mut self, snapshot: SessionSnapshot) -> ExecutorResult<()>;
}

/// Type-erased session snapshot. Each [`TargetExecutionSession`] impl
/// chooses its own concrete `Box<dyn Any + Send>`-wrapped state.
/// Holding this type alone is enough for composition.rs to stash and
/// hand snapshots back to the originating session without naming the
/// concrete type.
pub type SessionSnapshot = Box<dyn std::any::Any + Send>;

/// Uniform chain-client interface every registered rollup satisfies.
///
/// Stored as `Arc<dyn ChainClient + Send + Sync>` in the composer's
/// rollup map. `simulate_source_tx` defaults to a loud
/// [`ExecutorErrorKind::Unavailable`] refusal; entry-role clients
/// override it.
pub trait ChainClient: Send + Sync + 'static {
    /// Create a fresh stateful execution session. The slot drain may
    /// keep the returned session alive across consecutive source txs
    /// in the same slot (F1); it never outlives its slot.
    ///
    /// # Errors
    ///
    /// [`ExecutorErrorKind::Provider`] / [`ExecutorErrorKind::Evm`] /
    /// [`ExecutorErrorKind::Missing`].
    fn begin_execution_session(&self)
    -> ExecutorResult<Box<dyn TargetExecutionSession + Send>>;

    /// Simulate a source-chain transaction, dispatching every detected
    /// cross-chain proxy call through `dispatcher`. Entry-role clients
    /// only; the default refuses so follower registrations fail loudly
    /// if they ever reach source simulation.
    ///
    /// # Errors
    ///
    /// [`ExecutorErrorKind::Decode`] on an undecodable tx;
    /// [`ExecutorErrorKind::Provider`] / [`ExecutorErrorKind::Evm`] on
    /// source execution; propagates any dispatch error.
    fn simulate_source_tx(
        &self,
        _raw_tx: Vec<u8>,
        _dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
            "simulate_source_tx: not an entry-role client".into(),
        )))
    }
}
