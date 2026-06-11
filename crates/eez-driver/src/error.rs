//! Error type returned by the driver.
//!
//! Follows M-ERRORS-CANONICAL-STRUCTS: a single public [`DriverError`] struct
//! wrapping a private [`ErrorKind`] enum, with a captured [`Backtrace`]. The
//! kind enum is private; callers discriminate via `is_*` helper methods so
//! adding a variant doesn't break the public API.
//!
//! All error construction goes through pub(crate) constructors that capture
//! the backtrace once, at the construction site closest to the failure.

use core::fmt;
use std::backtrace::Backtrace;

use alloy_primitives::B256;

/// Convenience [`Result`] alias used throughout the crate.
pub type DriverResult<T> = Result<T, DriverError>;

/// Error returned by [`Sequencer`](crate::Sequencer) operations.
pub struct DriverError {
    kind: ErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    /// Underlying provider returned an error during a state lookup.
    Provider(String),
    /// Expected a header at the given block number but the provider returned
    /// `None` — typically a "best block doesn't exist" race during startup.
    MissingHeader { block_number: u64 },
    /// `engine_forkchoiceUpdated` returned a non-`VALID` status.
    InvalidForkchoice(String),
    /// `engine_newPayload` returned a non-`VALID` status.
    InvalidPayload(String),
    /// Payload builder returned no payload for an issued ID.
    PayloadMissing,
    /// Engine-API RPC transport error.
    EngineRpc(String),
    /// `BlockCommitter` actor task is gone (channel closed). The Sequencer
    /// can't recover; the caller (typically eez-node main) will log + exit.
    CommitterClosed,
    /// Sequencer's snapshotted `parent_hash` no longer matches
    /// `last_header`. Caller retries next tick.
    StaleParent { expected: B256, actual: B256 },
}

impl DriverError {
    pub(crate) fn provider(err: impl fmt::Display) -> Self {
        Self::new(ErrorKind::Provider(err.to_string()))
    }

    pub(crate) fn missing_header(block_number: u64) -> Self {
        Self::new(ErrorKind::MissingHeader { block_number })
    }

    pub(crate) fn invalid_forkchoice(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidForkchoice(detail.into()))
    }

    pub(crate) fn invalid_payload(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidPayload(detail.into()))
    }

    pub(crate) fn payload_missing() -> Self {
        Self::new(ErrorKind::PayloadMissing)
    }

    pub(crate) fn engine_rpc(err: impl fmt::Display) -> Self {
        Self::new(ErrorKind::EngineRpc(err.to_string()))
    }

    pub(crate) fn committer_closed() -> Self {
        Self::new(ErrorKind::CommitterClosed)
    }

    pub(crate) fn stale_parent(expected: B256, actual: B256) -> Self {
        Self::new(ErrorKind::StaleParent { expected, actual })
    }

    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// Returns true if the error came from the underlying state provider.
    #[must_use]
    pub fn is_provider(&self) -> bool {
        matches!(self.kind, ErrorKind::Provider(_))
    }

    /// Returns true if the expected head header was missing on startup.
    #[must_use]
    pub fn is_missing_header(&self) -> bool {
        matches!(self.kind, ErrorKind::MissingHeader { .. })
    }

    /// Returns true if the engine rejected a forkchoice update.
    #[must_use]
    pub fn is_invalid_forkchoice(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidForkchoice(_))
    }

    /// Returns true if the engine rejected a built payload.
    #[must_use]
    pub fn is_invalid_payload(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidPayload(_))
    }

    /// Returns true if the `BlockCommitter` actor task has exited and
    /// can no longer receive commands.
    #[must_use]
    pub fn is_committer_closed(&self) -> bool {
        matches!(self.kind, ErrorKind::CommitterClosed)
    }

    /// Sequencer's snapshot was stale before the commit could run;
    /// caller should retry on the next tick.
    #[must_use]
    pub fn is_stale_parent(&self) -> bool {
        matches!(self.kind, ErrorKind::StaleParent { .. })
    }

    /// Returns the captured backtrace for diagnostics.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl fmt::Debug for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverError")
            .field("kind", &self.kind)
            .field("backtrace", &"<captured>")
            .finish()
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Provider(msg) => write!(f, "provider error: {msg}"),
            ErrorKind::MissingHeader { block_number } => {
                write!(f, "no header found at block {block_number}")
            }
            ErrorKind::InvalidForkchoice(detail) => {
                write!(f, "engine rejected forkchoice update: {detail}")
            }
            ErrorKind::InvalidPayload(detail) => {
                write!(f, "engine rejected new payload: {detail}")
            }
            ErrorKind::PayloadMissing => {
                write!(f, "payload builder returned no payload for issued id")
            }
            ErrorKind::EngineRpc(msg) => write!(f, "engine-API transport error: {msg}"),
            ErrorKind::CommitterClosed => {
                write!(f, "block committer actor task has exited")
            }
            ErrorKind::StaleParent { expected, actual } => {
                write!(
                    f,
                    "stale parent on sequence: snapshot was {expected}, last_header is now {actual}"
                )
            }
        }
    }
}

impl std::error::Error for DriverError {}
