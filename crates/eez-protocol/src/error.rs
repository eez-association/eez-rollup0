//! Typed errors for protocol materialization, target execution, composition,
//! and runtime composition orchestration.
//!
//! Public wrapper structs expose non-exhaustive kind enums through `kind()`.
//! [`ComposerError`] flattens the intermediate
//! [`CompositionError`] layer while preserving the originating protocol or
//! executor error.

/// Boxed source error used by provider-specific variants without exposing
/// their concrete error types.
///
/// Crate-private on purpose: downstream code shouldn't need to think
/// about `Box<dyn Error>`. Use the public constructors
/// ([`ExecutorError::provider`], [`ExecutorError::evm`]) — they accept
/// any `impl std::error::Error + Send + Sync + 'static` and box it
/// internally.
pub(crate) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Generate the struct-layer boilerplate for an error type that wraps a
/// `*Kind` enum. Emits the struct, its `kind()` accessor, `Display`,
/// `Error::source` forwarding, and `From<*Kind>`. The corresponding `*Kind`
/// enum is declared separately and carries the actual variants + thiserror.
macro_rules! error_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident wraps $kind:ident;
    ) => {
        $(#[$meta])*
        $vis struct $name {
            kind: $kind,
        }

        impl $name {
            /// The underlying variant.
            #[must_use]
            pub fn kind(&self) -> &$kind {
                &self.kind
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.kind, f)
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.kind.source()
            }
        }

        impl From<$kind> for $name {
            fn from(kind: $kind) -> Self {
                Self { kind }
            }
        }
    };
}

// ── ProtocolError ────────────────────────────────────────────────

error_struct! {
    /// Errors from pure protocol logic.
    ///
    /// Wraps a [`ProtocolErrorKind`].
    #[derive(Debug)]
    pub struct ProtocolError wraps ProtocolErrorKind;
}

/// Variants of [`ProtocolError`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolErrorKind {
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

error_struct! {
    /// Errors from target-chain client/session implementations.
    ///
    /// Wraps an [`ExecutorErrorKind`].
    #[derive(Debug)]
    pub struct ExecutorError wraps ExecutorErrorKind;
}

impl ExecutorError {
    /// Build a `Provider` error from any `Error + Send + Sync + 'static`.
    /// Also accepts an already-boxed `Box<dyn Error + Send + Sync>`.
    pub fn provider(e: impl Into<BoxedError>) -> Self {
        ExecutorErrorKind::Provider(e.into()).into()
    }
    /// Build an `Evm` error from any `Error + Send + Sync + 'static`.
    pub fn evm(e: impl Into<BoxedError>) -> Self {
        ExecutorErrorKind::Evm(e.into()).into()
    }
}

/// Variants of [`ExecutorError`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutorErrorKind {
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

error_struct! {
    /// Error from composing one source transaction. Preserves protocol
    /// materialization failures and target-execution failures from the
    /// surrounding composition pipeline.
    #[derive(Debug)]
    pub struct CompositionError wraps CompositionErrorKind;
}

impl From<ProtocolError> for CompositionError {
    fn from(e: ProtocolError) -> Self {
        CompositionErrorKind::Protocol(e).into()
    }
}

impl From<ExecutorError> for CompositionError {
    fn from(e: ExecutorError) -> Self {
        CompositionErrorKind::Executor(e).into()
    }
}

impl From<ProtocolErrorKind> for CompositionError {
    fn from(k: ProtocolErrorKind) -> Self {
        ProtocolError::from(k).into()
    }
}

impl From<ExecutorErrorKind> for CompositionError {
    fn from(k: ExecutorErrorKind) -> Self {
        ExecutorError::from(k).into()
    }
}

/// Variants of [`CompositionError`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompositionErrorKind {
    /// Protocol-layer failure during entry building or composition validation.
    #[error("protocol: {0}")]
    Protocol(#[source] ProtocolError),
    /// Target-chain execution failure raised during composition.
    #[error("executor: {0}")]
    Executor(#[source] ExecutorError),
}

/// Shorthand for composition results.
pub type CompositionResult<T> = Result<T, CompositionError>;

// ── ComposerError ────────────────────────────────────────────────

error_struct! {
    /// Error surfaced by runtime composition orchestration.
    #[derive(Debug)]
    pub struct ComposerError wraps ComposerErrorKind;
}

impl From<ProtocolError> for ComposerError {
    fn from(e: ProtocolError) -> Self {
        ComposerErrorKind::Protocol(e).into()
    }
}

impl From<ExecutorError> for ComposerError {
    fn from(e: ExecutorError) -> Self {
        ComposerErrorKind::Executor(e).into()
    }
}

impl From<CompositionError> for ComposerError {
    fn from(e: CompositionError) -> Self {
        // Flatten the intermediate kind while preserving the originating
        // protocol or executor error.
        match e.kind {
            CompositionErrorKind::Protocol(p) => ComposerErrorKind::Protocol(p).into(),
            CompositionErrorKind::Executor(ex) => ComposerErrorKind::Executor(ex).into(),
        }
    }
}

impl From<ProtocolErrorKind> for ComposerError {
    fn from(k: ProtocolErrorKind) -> Self {
        ProtocolError::from(k).into()
    }
}

impl From<ExecutorErrorKind> for ComposerError {
    fn from(k: ExecutorErrorKind) -> Self {
        ExecutorError::from(k).into()
    }
}

/// Variants of [`ComposerError`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ComposerErrorKind {
    /// Protocol-layer failure surfaced through the orchestrator.
    #[error("protocol: {0}")]
    Protocol(#[source] ProtocolError),
    /// Executor-layer failure surfaced through the orchestrator.
    #[error("executor: {0}")]
    Executor(#[source] ExecutorError),
}

/// Shorthand for composer results.
pub type ComposerResult<T> = Result<T, ComposerError>;
