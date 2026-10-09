//! Typed errors for protocol materialization, target execution, composition,
//! and runtime composition orchestration.
//!
//! Each error family is a plain non-exhaustive `thiserror` enum.
//! [`ComposerError`] flattens the intermediate
//! [`CompositionError`] layer while preserving the originating protocol or
//! executor error.

/// Boxed source error used by provider-specific variants without exposing
/// their concrete error types.
///
/// Crate-private on purpose: downstream code can box concrete source errors at
/// construction sites without depending on this alias.
pub(crate) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

// ── ProtocolError ────────────────────────────────────────────────

/// Errors from pure protocol logic.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Composition was attempted with no cross-chain calls to include.
    #[error("no cross-chain calls to compose")]
    EmptyCalls,
    /// A recorded call references an unregistered rollup.
    #[error("recorded call targets rollup {got}, which has no registered target plan")]
    UnknownTarget {
        /// The unknown rollup ID.
        got: crate::rollup_id::RollupId,
    },
    /// A value required for protocol materialization is unresolved or
    /// structurally incomplete.
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    /// The observed execution shape is outside the supported materialization
    /// profile.
    #[error("unsupported protocol operation: {0}")]
    Unsupported(&'static str),
}

/// Shorthand for protocol results.
pub type ProtocolResult<T> = Result<T, ProtocolError>;

// ── ExecutorError ────────────────────────────────────────────────

/// Errors from target-chain client/session implementations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutorError {
    /// The target chain is not reachable or not configured for the
    /// requested operation.
    #[error("target chain unavailable: {0}")]
    Unavailable(String),
    /// Underlying state/block provider failed (e.g. reth MDBX read).
    #[error("provider: {0}")]
    Provider(#[source] BoxedError),
    /// Target EVM setup or execution failed before a normal outcome could be
    /// returned.
    #[error("evm: {0}")]
    Evm(#[source] BoxedError),
    /// The target-side caller cannot yet fund the transaction. Unlike other
    /// transaction-validation failures, later chain state can make it valid.
    #[error("insufficient target funds: available {available}, required {required}")]
    InsufficientFunds {
        /// Balance visible to the target-side simulation.
        available: alloy_primitives::U256,
        /// Value plus maximum fee required by the simulated transaction.
        required: alloy_primitives::U256,
    },
    /// Executor data has an invalid representation or concrete type.
    #[error("encoding: {0}")]
    Encoding(String),
    /// Required provider or execution data was absent.
    #[error("missing {0}")]
    Missing(&'static str),
    /// Failed to decode a higher-level input such as a raw transaction.
    #[error("decode: {0}")]
    Decode(String),
    /// A dispatch targeted a non-entry rollup that cannot safely accept
    /// re-entry: either the caller targets itself or the target session is
    /// already executing an outer call.
    #[error("invalid re-entry from rollup {caller} to rollup {target}")]
    InvalidReentry {
        /// Rollup whose inspector issued the dispatch.
        caller: crate::rollup_id::RollupId,
        /// Requested target rollup.
        target: crate::rollup_id::RollupId,
    },
}

/// Shorthand for executor results.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

// ── CompositionError ─────────────────────────────────────────────

/// Error from composing one source transaction. Preserves protocol
/// materialization failures and target-execution failures from the surrounding
/// composition pipeline.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompositionError {
    /// Protocol-layer failure during entry building or composition validation.
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// Target-chain execution failure raised during composition.
    #[error("executor: {0}")]
    Executor(#[from] ExecutorError),
}

/// Shorthand for composition results.
pub type CompositionResult<T> = Result<T, CompositionError>;

// ── ComposerError ────────────────────────────────────────────────

impl From<CompositionError> for ComposerError {
    fn from(e: CompositionError) -> Self {
        // Flatten the intermediate layer while preserving the originating
        // protocol or executor error.
        match e {
            CompositionError::Protocol(p) => Self::Protocol(p),
            CompositionError::Executor(ex) => Self::Executor(ex),
        }
    }
}

/// Error surfaced by runtime composition orchestration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ComposerError {
    /// Protocol-layer failure surfaced through the orchestrator.
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// Executor-layer failure surfaced through the orchestrator.
    #[error("executor: {0}")]
    Executor(#[from] ExecutorError),
}

/// Shorthand for composer results.
pub type ComposerResult<T> = Result<T, ComposerError>;
