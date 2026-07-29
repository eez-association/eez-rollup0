//! Error types for the cross-chain protocol.
//!
//! Four public error families — all plain enums deriving
//! [`thiserror::Error`]. Variants carry their payload fields directly.
//!
//! # Hierarchy (wrapping direction)
//!
//! ```text
//!   ComposerError                ← public face of Composer<P>
//!       ├─ ComposerError::Protocol(ProtocolError)
//!       ├─ ComposerError::Executor(ExecutorError)
//!
//!   CompositionError             ← public face of compose_transaction
//!       ├─ CompositionError::Protocol(ProtocolError)
//!       └─ CompositionError::Executor(ExecutorError)
//!
//!   ProtocolError                ← pure protocol logic (entries, validation)
//!       EmptyCalls | InvalidCheckpoint | UnknownTarget
//!       | InvalidEncoding | Unsupported
//!
//!   ExecutorError                ← target-chain client/session failures
//!       Unavailable | Provider | Evm | Transport | Encoding | Serde
//!       | Missing | TargetTransactionReverted | Decode | EmptyBatch
//!       | InvalidReentry
//! ```
//!
//! [`ComposerError`] flattens across the composition layer: the
//! `From<CompositionError>` impl decomposes into the underlying
//! protocol / executor layer, so a caller matches one level instead
//! of two.

/// Boxed source error — any `Send + Sync` error the underlying library
/// emits. Used for `Provider`, `Evm`, `Transport`, and `Serde`
/// variants so callers can inspect the root cause via
/// `std::error::Error::source()` while keeping the protocol crate free
/// of alloy/reth/tonic deps.
///
/// Crate-private on purpose: downstream code shouldn't need to think
/// about `Box<dyn Error>` — any `impl std::error::Error + Send + Sync
/// + 'static` (or a `String`) converts in via `.into()` at the
/// construction site.
// TODO(post-#58): move ExecutorError next to its producers and type the
// payloads (`Provider(#[from] ProviderError)`) so `?` replaces the map_err
// boxing. Blocked here: this crate can't name reth error types (orphan rule).
pub(crate) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

// ── ProtocolError ────────────────────────────────────────────────

/// Errors from pure protocol logic.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolError {
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
    /// EVM execution failed internally (revm halt, unknown opcode,
    /// etc.). `TargetTransactionReverted` is distinct — it's the
    /// contract-level revert.
    ///
    /// **Naming debt**: the variant name `Evm` is EVM-specific, but
    /// the concept ("chain's execution engine failed internally") is
    /// general. A future sibling chain family (WASM, Move, ...) would
    /// need either this renamed to something like `EngineInternal`
    /// (breaking API) or a fresh variant added (additive; the enum is
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

/// Composition-pipeline error. `finalize` crosses the protocol /
/// executor boundary (CCM verification runs target-chain
/// simulation), so this type preserves both layers losslessly.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompositionError {
    /// Protocol-layer failure during composition (entry building,
    /// checkpoint validation, etc.).
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// Target-chain execution failure raised during CCM verification.
    #[error("executor: {0}")]
    Executor(#[from] ExecutorError),
}

/// Shorthand for composition session results.
pub type CompositionResult<T> = Result<T, CompositionError>;

// ── ComposerError ────────────────────────────────────────────────

impl From<CompositionError> for ComposerError {
    fn from(e: CompositionError) -> Self {
        // Decompose the composition error into its underlying layer so
        // the composer surface flattens to a single-hop match.
        match e {
            CompositionError::Protocol(p) => Self::Protocol(p),
            CompositionError::Executor(ex) => Self::Executor(ex),
        }
    }
}

/// Errors from the [`Composer`](crate::Composer) orchestrator.
///
/// Wraps the composition error family.
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
