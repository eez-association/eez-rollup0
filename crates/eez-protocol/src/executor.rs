//! Synchronous chain-client and execution-session interfaces used by
//! composition.
//!
//! [`ChainClient`] opens stateful target sessions for registered rollups.
//! Entry-role implementations also override `simulate_source_tx`; its default
//! returns [`ExecutorErrorKind::Unavailable`]. Nested cross-chain dispatch
//! goes through the borrowed [`CompositionBuilder`].
//!
//! Both traits are synchronous because execution is in-process. Trait objects
//! let local implementations and test fakes share the same orchestration.

use alloy_primitives::{Address, Bytes, U256};

use crate::action::CallMode;
use crate::composition::CompositionBuilder;
use crate::error::ExecutorResult;
use crate::error::{ExecutorError, ExecutorErrorKind};
use crate::rollup_id::RollupId;
use crate::types::ExecutionOutcome;

/// Request for a single cross-chain execution on the target chain.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    /// Effective EVM mode, including static context inherited from a parent call.
    pub call_mode: CallMode,
    /// Address invoked on the destination chain.
    pub target_address: Address,
    /// Calldata sent to the destination contract.
    pub data: Bytes,
    /// Native value sent with the call.
    pub value: U256,
    /// Original source-chain caller, used to derive the destination-side proxy
    /// address that executes as `msg.sender`.
    pub source_address: Address,
    /// Authoritative ID of the chain on which `source_address` made the
    /// intercepted call. Used for re-entry checks and recording,
    /// destination-proxy derivation during execution, and call-hash
    /// materialization.
    pub source_rollup_id: RollupId,
}

/// Stateful execution session driving target-chain calls during
/// source simulation.
///
/// A builder lazily opens at most one session per rollup. Sessions accumulate
/// state across calls and can be transferred explicitly through
/// [`CompositionBuilder::with_sessions`]. `&mut self` on every method reflects
/// that stateful lifecycle.
///
/// The `dispatcher` argument on [`execute`](Self::execute) supports
/// nested cross-chain dispatch: a target-session inspector can call
/// back into the composer through `dispatcher` to route a nested
/// proxy call.
pub trait TargetExecutionSession: Send {
    /// Execute a single call on the target chain.
    ///
    /// `dispatcher` routes nested cross-chain calls observed during execution.
    ///
    /// Returns the outcome for source simulation.
    ///
    /// # Errors
    ///
    /// Returns implementation-specific target execution errors and propagates
    /// errors from nested dispatch.
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome>;

    /// Capture an opaque checkpoint accepted by [`rollback`](Self::rollback).
    ///
    /// Used to restore nested execution after a reverted frame:
    /// [`CompositionBuilder::open_call`] snapshots the target session
    /// before delegating to `execute` and stashes it by call index.
    /// Reverted spans queue the matching snapshots for rollback before a later
    /// dispatch; unused snapshots are dropped with the builder.
    ///
    /// # Errors
    ///
    /// Implementations may fail while capturing their checkpoint state.
    fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot>;

    /// Apply a checkpoint produced by this session.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Encoding`] when the snapshot has the wrong
    /// concrete type for this session.
    fn rollback(&mut self, snapshot: SessionSnapshot) -> ExecutorResult<()>;
}

/// Opaque checkpoint returned by [`TargetExecutionSession::checkpoint`] and
/// consumed by [`TargetExecutionSession::rollback`].
pub type SessionSnapshot = Box<dyn std::any::Any + Send>;

/// Uniform chain-client interface every registered rollup satisfies.
///
/// The runtime composer talks to every rollup through this trait.
/// `simulate_source_tx` is role-specific and defaults to an
/// [`ExecutorErrorKind::Unavailable`] refusal.
///
pub trait ChainClient: Send + Sync + 'static {
    /// Reset client-local state that must not cross transaction compositions.
    fn reset_composition_state(&self) {}

    /// Create a fresh stateful execution session.
    ///
    /// # Errors
    ///
    /// Returns an executor error when the session cannot be initialized.
    fn begin_execution_session(&self) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>>;

    /// Simulate a source-chain transaction, dispatching every detected
    /// cross-chain proxy call through `dispatcher`. Entry-role clients
    /// only; the default refuses with
    /// [`ExecutorErrorKind::Unavailable`] so follower registrations
    /// fail loudly if they ever reach source simulation.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Decode`] if the raw transaction cannot be
    /// decoded, [`ExecutorErrorKind::Provider`] if source state is inaccessible,
    /// or [`ExecutorErrorKind::Evm`] if EVM setup fails. Propagates errors from
    /// nested dispatch.
    fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        let _ = (raw_tx, dispatcher);
        Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
            "simulate_source_tx: not an entry-role client".into(),
        )))
    }
}
