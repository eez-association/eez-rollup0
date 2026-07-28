//! Error types for the cross-chain protocol.
//!
//! Four public error families — all follow the same **struct + kind +
//! backtrace** shape. Matching goes through `.kind()`; construction is
//! via `From<XxxErrorKind>` (or named helpers like
//! [`ExecutorError::provider`]).
//!
//! # Hierarchy (wrapping direction)
//!
//! ```text
//!   ComposerError                ← public face of Composer<P>
//!       ├─ ComposerErrorKind::Protocol(ProtocolError)
//!       ├─ ComposerErrorKind::Executor(ExecutorError)
//!       ├─ ComposerErrorKind::AlreadyRegistered { .. }     ← lifecycle
//!       ├─ ComposerErrorKind::Misconfigured   { .. }       ← lifecycle
//!       ├─ ComposerErrorKind::MissingRootReader            ← lifecycle
//!       └─ ComposerErrorKind::LockPoisoned    { .. }       ← internal bug
//!
//!   CompositionError             ← public face of compose_transaction
//!       ├─ CompositionErrorKind::Protocol(ProtocolError)
//!       └─ CompositionErrorKind::Executor(ExecutorError)
//!
//!   ProtocolError                ← pure protocol logic (entries, validation)
//!       └─ ProtocolErrorKind     EmptyCalls | InvalidCheckpoint
//!                                | UnknownTarget | InvalidEncoding
//!                                | Unsupported
//!
//!   ExecutorError                ← target-chain client/session failures
//!       └─ ExecutorErrorKind     Unavailable | Provider | Evm | Transport
//!                                | Encoding | Serde | Missing
//!                                | TargetTransactionReverted | Decode
//!                                | EmptyBatch | InvalidReentry
//! ```
//!
//! [`ComposerError`] flattens across the composition layer: the
//! `From<CompositionError>` impl decomposes into the underlying
//! protocol / executor layer, so a caller matches one level of
//! `.kind()` instead of two.
//!
//! # Backtraces
//!
//! Each error records a [`Backtrace`] at construction. Backtrace
//! capture is cheap when `RUST_BACKTRACE` is unset (the stdlib
//! records the "disabled" sentinel); enabled backtraces are
//! captured once per conversion point, so the wrapping chain shows
//! where each layer intercepted.

use std::backtrace::Backtrace;

/// Boxed source error — any `Send + Sync` error the underlying library
/// emits. Used for `Provider`, `Evm`, `Transport`, and `Serde`
/// variants so callers can inspect the root cause via
/// `std::error::Error::source()` while keeping the protocol crate free
/// of alloy/reth/tonic deps.
///
/// Crate-private on purpose: downstream code shouldn't need to think
/// about `Box<dyn Error>`. Use the public constructors
/// ([`ExecutorError::provider`], [`ExecutorError::evm`],
/// [`ExecutorError::transport`], [`ExecutorError::serde`]) — they
/// accept any `impl std::error::Error + Send + Sync + 'static` and box
/// it internally.
pub(crate) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Generate the struct-layer boilerplate for an error type that wraps
/// a `*Kind` enum plus a [`Backtrace`]. Emits the struct, its
/// `kind()`/`backtrace()` accessors, `Display`, `Error::source`
/// forwarding, and `From<*Kind>`. The corresponding `*Kind` enum is
/// declared separately and carries the actual variants + thiserror.
macro_rules! error_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident wraps $kind:ident;
    ) => {
        $(#[$meta])*
        $vis struct $name {
            kind: $kind,
            bt: Backtrace,
        }

        impl $name {
            /// The underlying variant.
            #[must_use]
            pub fn kind(&self) -> &$kind {
                &self.kind
            }
            /// Backtrace captured at the construction site.
            pub fn backtrace(&self) -> &Backtrace {
                &self.bt
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
                Self { kind, bt: Backtrace::capture() }
            }
        }
    };
}

// ── ProtocolError ────────────────────────────────────────────────

error_struct! {
    /// Errors from pure protocol logic.
    ///
    /// Wraps a [`ProtocolErrorKind`] plus a captured backtrace.
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
    /// Checkpoint content failed structural validation (e.g. per-rollup
    /// state-delta chaining broke).
    #[error("invalid checkpoint: {reason}")]
    InvalidCheckpoint {
        /// Human-readable description of the validation failure.
        reason: String,
    },
    /// A recorded call references a rollup for which no target plan was
    /// supplied.
    #[error("recorded call targets rollup {got}, which has no registered target plan")]
    UnknownTarget {
        /// The unknown rollup ID.
        got: crate::rollup_id::RollupId,
    },
    /// Byte-level decoding of a chain-specific field (address, value,
    /// calldata) failed.
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    /// A protocol capability was invoked that this chain family does not
    /// implement — e.g. a
    /// [`build_l1_postbatch`](crate::entries::build_l1_postbatch)
    /// impl that cannot actually settle outbound. Today only the
    /// in-tree test fakes construct this variant.
    #[error("unsupported protocol operation: {0}")]
    Unsupported(&'static str),
}

/// Shorthand for protocol results.
pub type ProtocolResult<T> = Result<T, ProtocolError>;

// ── ExecutorError ────────────────────────────────────────────────

error_struct! {
    /// Errors from target-chain client/session implementations.
    ///
    /// Wraps an [`ExecutorErrorKind`] plus a captured backtrace.
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
    /// Build a `Transport` error from any `Error + Send + Sync + 'static`.
    pub fn transport(e: impl Into<BoxedError>) -> Self {
        ExecutorErrorKind::Transport(e.into()).into()
    }
    /// Build a `Serde` error from any `Error + Send + Sync + 'static`.
    pub fn serde(e: impl Into<BoxedError>) -> Self {
        ExecutorErrorKind::Serde(e.into()).into()
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
    /// EVM execution failed internally (revm halt, unknown opcode,
    /// etc.). `TargetTransactionReverted` is distinct — it's the
    /// contract-level revert.
    ///
    /// **Naming debt**: the variant name `Evm` is EVM-specific, but
    /// the concept ("chain's execution engine failed internally") is
    /// general. A future sibling chain family (WASM, Move, ...) would
    /// need either this renamed to something like `EngineInternal`
    /// (breaking API — `ExecutorError::evm` is the current public
    /// constructor) or a fresh variant added (additive; the enum is
    /// `#[non_exhaustive]`).
    #[error("evm: {0}")]
    Evm(#[source] BoxedError),
    /// Wire-level failure (gRPC status, connection error).
    #[error("transport: {0}")]
    Transport(#[source] BoxedError),
    /// Message crossed the wire but didn't decode (wrong byte length,
    /// malformed proto field, bad address encoding).
    #[error("encoding: {0}")]
    Encoding(String),
    /// Serialization / deserialization failure crossing the executor
    /// boundary — typically a checkpoint blob. The exact backend
    /// (`serde_json` today) is an implementation detail of the
    /// transport crate, not part of this crate's public surface.
    #[error("serde: {0}")]
    Serde(#[source] BoxedError),
    /// Expected data (header, outcome, etc.) was absent with no
    /// underlying error to wrap — used for synthetic "`.ok_or_else`"
    /// sites that aren't really provider failures.
    #[error("missing {0}")]
    Missing(&'static str),
    /// A target-chain transaction in a batch simulation reverted at
    /// the contract level (distinct from internal EVM failures).
    #[error("target transaction {index} reverted: return_data={return_data:?}")]
    TargetTransactionReverted {
        /// Zero-based position of the reverting transaction within the
        /// simulated batch.
        index: usize,
        /// Raw revert data returned by the contract, if any.
        return_data: Vec<u8>,
    },
    /// Failed to decode a higher-level structure (raw transaction,
    /// checkpoint, etc.) — distinct from byte-level `Encoding`.
    #[error("decode: {0}")]
    Decode(String),
    /// A batch simulation was asked to run with zero transactions.
    /// Distinct from a successful batch with zero post-state change —
    /// this signals the caller passed an empty slice, which the
    /// upstream protocol's "invariant 7" (no silent failures) says
    /// must be a loud error rather than a synthesized zero root.
    #[error("batch simulation requires at least one transaction")]
    EmptyBatch,
    /// A nested dispatch attempted to route back to the same non-entry
    /// chain that issued it (e.g. L2 → L2 self-dispatch).
    /// Architecturally disallowed; L1→L2→L1 (re-entry through the
    /// entry rollup) IS valid and is handled inline by the EVM
    /// inspector via the overlay path. Raised by
    /// [`Dispatcher::dispatch_call`](crate::CompositionBuilder::dispatch_call)'s
    /// guard.
    #[error(
        "invalid re-entry: caller rollup {caller} attempted to dispatch to same rollup {target} \
         (not the entry rollup)"
    )]
    InvalidReentry {
        /// Rollup whose inspector issued the dispatch.
        caller: crate::rollup_id::RollupId,
        /// Requested target rollup (equal to `caller`, not the entry).
        target: crate::rollup_id::RollupId,
    },
}

/// Shorthand for executor results.
pub type ExecutorResult<T> = Result<T, ExecutorError>;

// ── CompositionError ─────────────────────────────────────────────

error_struct! {
    /// Composition-pipeline error. `finalize` crosses the protocol /
    /// executor boundary (CCM verification runs target-chain
    /// simulation), so this type preserves both layers losslessly via
    /// [`CompositionErrorKind`].
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
    /// Protocol-layer failure during composition (entry building,
    /// checkpoint validation, etc.).
    #[error("protocol: {0}")]
    Protocol(#[source] ProtocolError),
    /// Target-chain execution failure raised during CCM verification.
    #[error("executor: {0}")]
    Executor(#[source] ExecutorError),
}

/// Shorthand for composition session results.
pub type CompositionResult<T> = Result<T, CompositionError>;

// ── ComposerError ────────────────────────────────────────────────

error_struct! {
    /// Errors from the generic [`Composer`](crate::Composer) orchestrator.
    ///
    /// Wraps the composition error family plus composer-specific
    /// lifecycle failures (already-registered source/target, missing
    /// registration).
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
        // Decompose the composition error into its underlying layer so
        // the composer surface flattens to a single-hop kind chain.
        //
        // Note: `e.bt` (the CompositionError's own backtrace) is
        // discarded here. The inner ProtocolError / ExecutorError
        // preserves the origin backtrace, which is what debugging
        // needs. The discarded outer backtrace would just point at
        // the `?` operator that performed this conversion —
        // derivable from stack context already.
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
    /// A registration called a second time for a slot that only accepts
    /// one entry (source client) or for an already-present key
    /// (target rollup id).
    #[error("already registered: {what}")]
    AlreadyRegistered {
        /// Which slot was double-registered — e.g. `"source client"` or
        /// `"target client"`.
        what: &'static str,
    },
    /// Startup / configuration failure with a compile-time-constant
    /// reason — e.g. source or target clients not yet registered when
    /// an operation is requested.
    #[error("composer misconfigured: {reason}")]
    Misconfigured {
        /// Static human-readable reason for the misconfiguration.
        reason: &'static str,
    },
    /// An internal [`std::sync`] lock was poisoned by a prior panic
    /// while a critical section was held. Distinct from `Misconfigured`
    /// because it signals a **programming bug** (a panic-and-recover
    /// happened somewhere) rather than a configuration mistake.
    #[error("internal lock poisoned: {what}")]
    LockPoisoned {
        /// Which lock was poisoned — e.g. `"target map"`.
        what: &'static str,
    },
    /// `ComposerBuilder::build` was called without a prior
    /// `.root_reader(...)` registration. Phase 1 of every
    /// `simulate_and_resolve` call needs a `CommittedRootReader` to
    /// read invariant-6 anchor roots; the builder fails-fast at
    /// construction rather than at first dispatch.
    #[error(
        "composer build: no committed-root reader registered (call .root_reader(...) on the builder)"
    )]
    MissingRootReader,
}

/// Shorthand for composer results.
pub type ComposerResult<T> = Result<T, ComposerError>;
