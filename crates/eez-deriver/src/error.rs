//! Error type returned by the deriver.
//!
//! Follows M-ERRORS-CANONICAL-STRUCTS: a single public [`DeriverError`]
//! struct wrapping a private [`ErrorKind`] enum, with a captured
//! [`Backtrace`]. Callers discriminate via `is_*` helper methods so
//! adding a variant doesn't break the public API.

use core::fmt;
use std::backtrace::Backtrace;

/// Convenience [`Result`] alias used throughout the crate.
pub type DeriverResult<T> = Result<T, DeriverError>;

/// Error returned by [`Deriver`](crate::Deriver) operations.
pub struct DeriverError {
    kind: ErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
pub(crate) enum ErrorKind {
    /// L2 provider lookup failed (e.g., reading a block at a given
    /// height to compare against an L1-derived batch).
    L2Provider(String),
    /// `eez-payload-codec::decode` rejected an L1-posted batch's
    /// `call_data`. Usually indicates a contract that posted a payload
    /// in a version we don't speak.
    Codec(eez_payload_codec::CodecError),
    /// L1 catch-up scan failed. Callers can inspect nested typed L1 errors
    /// through helper methods without deriver re-encoding their meaning.
    L1Scan(eez_l1::L1Error),
    /// `BlockCommitter` actor task is gone; the deriver can't push
    /// safe-head advances any further.
    CommitterClosed,
    /// `engine_forkchoiceUpdated` rejected the safe/finalized cursors
    /// the deriver tried to set. Usually means the L1-derived hashes
    /// don't match reth's canonical chain — a genuine divergence.
    InvalidForkchoice(String),
    /// Local L2 chain diverged from an L1-confirmed batch — our block
    /// at `l2_block` has different content than the batch says it
    /// should. Real reorg/replay is a follow-up; today this halts the
    /// deriver loudly.
    LocalDiverged {
        l2_block: u64,
        /// Why the divergence was raised (gate failure, prefix mismatch,
        /// replay error). Surfaced in `Display` — silent failures are bugs.
        detail: Option<String>,
    },
}

impl DeriverError {
    pub(crate) fn l2_provider(err: impl fmt::Display) -> Self {
        Self::new(ErrorKind::L2Provider(err.to_string()))
    }

    pub(crate) fn l1_scan(err: eez_l1::L1Error) -> Self {
        Self::new(ErrorKind::L1Scan(err))
    }

    pub(crate) fn committer_closed() -> Self {
        Self::new(ErrorKind::CommitterClosed)
    }

    pub(crate) fn invalid_forkchoice(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidForkchoice(detail.into()))
    }

    pub(crate) fn local_diverged(l2_block: u64) -> Self {
        Self::new(ErrorKind::LocalDiverged {
            l2_block,
            detail: None,
        })
    }

    pub(crate) fn local_diverged_with_msg(l2_block: u64, msg: &str) -> Self {
        Self::new(ErrorKind::LocalDiverged {
            l2_block,
            detail: Some(msg.to_string()),
        })
    }

    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// Returns true if the underlying L2 provider failed.
    #[must_use]
    pub fn is_l2_provider(&self) -> bool {
        matches!(self.kind, ErrorKind::L2Provider(_))
    }

    /// Returns true if the payload codec rejected an L1-posted batch.
    #[must_use]
    pub fn is_codec(&self) -> bool {
        matches!(self.kind, ErrorKind::Codec(_))
    }

    /// Returns true if L1 is missing data for an otherwise-observed
    /// canonical item and the caller may retry after the source catches up.
    #[must_use]
    pub fn is_source_incomplete(&self) -> bool {
        matches!(&self.kind, ErrorKind::L1Scan(err) if err.is_source_incomplete())
    }

    /// Returns true if the `BlockCommitter` actor task has exited.
    #[must_use]
    pub fn is_committer_closed(&self) -> bool {
        matches!(self.kind, ErrorKind::CommitterClosed)
    }

    /// Returns true if reth rejected a safe / finalized FCU.
    #[must_use]
    pub fn is_invalid_forkchoice(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidForkchoice(_))
    }

    /// Returns true if the local L2 diverged from an L1-confirmed batch.
    /// Operator must reconcile — full replay is future work.
    #[must_use]
    pub fn is_local_diverged(&self) -> bool {
        matches!(self.kind, ErrorKind::LocalDiverged { .. })
    }

    /// Returns the captured backtrace for diagnostics.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl From<eez_payload_codec::CodecError> for DeriverError {
    fn from(err: eez_payload_codec::CodecError) -> Self {
        Self::new(ErrorKind::Codec(err))
    }
}

impl From<eez_driver::DriverError> for DeriverError {
    fn from(err: eez_driver::DriverError) -> Self {
        match err {
            eez_driver::DriverError::CommitterClosed => Self::committer_closed(),
            eez_driver::DriverError::InvalidForkchoice(_) => {
                Self::invalid_forkchoice(err.to_string())
            }
            _ => Self::invalid_forkchoice(format!("driver: {err}")),
        }
    }
}

impl fmt::Debug for DeriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeriverError")
            .field("kind", &self.kind)
            .field("backtrace", &"<captured>")
            .finish()
    }
}

impl fmt::Display for DeriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::L2Provider(msg) => write!(f, "L2 provider error: {msg}"),
            ErrorKind::Codec(err) => write!(f, "payload codec error: {err}"),
            ErrorKind::L1Scan(err) => write!(f, "L1 catch-up scan error: {err}"),
            ErrorKind::CommitterClosed => {
                write!(f, "block committer actor task has exited")
            }
            ErrorKind::InvalidForkchoice(detail) => {
                write!(f, "engine rejected safe/finalized FCU: {detail}")
            }
            ErrorKind::LocalDiverged { l2_block, detail } => {
                write!(
                    f,
                    "local L2 block {l2_block} diverged from L1-confirmed batch; \
                     the on-chain claimed newState doesn't match local STF output"
                )?;
                if let Some(detail) = detail {
                    write!(f, " ({detail})")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for DeriverError {}
