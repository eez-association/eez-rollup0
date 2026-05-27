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
    /// `engine_forkchoiceUpdated` returned `payloadStatus: INVALID`.
    InvalidForkchoicePayload(String),
    /// `engine_forkchoiceUpdated` returned `ForkchoiceUpdateError::InvalidState`.
    InvalidForkchoiceState(String),
    /// `engine_forkchoiceUpdated` returned `ForkchoiceUpdateError::UnknownFinalBlock`.
    UnknownFinalBlock(String),
    /// `engine_forkchoiceUpdated` returned `ForkchoiceUpdateError::UpdatedInvalidPayloadAttributes`.
    InvalidPayloadAttributes(String),
    /// `engine_newPayload` returned a non-`VALID` status.
    InvalidPayload(String),
    /// Payload builder returned no payload for an issued ID.
    PayloadMissing,
    /// Engine-API RPC transport error.
    EngineRpc(String),
    /// `BlockCommitter` actor task is gone (channel closed). The Sequencer
    /// can't recover; the caller (typically eez-node main) will log + exit.
    CommitterClosed,
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

    pub(crate) fn invalid_forkchoice_payload(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidForkchoicePayload(detail.into()))
    }

    pub(crate) fn invalid_forkchoice_state(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidForkchoiceState(detail.into()))
    }

    pub(crate) fn unknown_final_block(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::UnknownFinalBlock(detail.into()))
    }

    pub(crate) fn invalid_payload_attributes(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidPayloadAttributes(detail.into()))
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
        matches!(
            self.kind,
            ErrorKind::InvalidForkchoice(_)
                | ErrorKind::InvalidForkchoicePayload(_)
                | ErrorKind::InvalidForkchoiceState(_)
                | ErrorKind::UnknownFinalBlock(_)
                | ErrorKind::InvalidPayloadAttributes(_)
        )
    }

    /// Returns true if reth rejected the FCU as an invalid or inconsistent
    /// forkchoice state.
    #[must_use]
    pub fn is_invalid_forkchoice_state(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidForkchoiceState(_))
    }

    /// Returns true if reth returned `payloadStatus: INVALID` for the FCU.
    #[must_use]
    pub fn is_invalid_forkchoice_payload(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidForkchoicePayload(_))
    }

    /// Returns true if reth could not resolve the requested finalized block.
    #[must_use]
    pub fn is_unknown_final_block(&self) -> bool {
        matches!(self.kind, ErrorKind::UnknownFinalBlock(_))
    }

    /// Returns true if reth rejected supplied payload attributes.
    #[must_use]
    pub fn is_invalid_payload_attributes(&self) -> bool {
        matches!(self.kind, ErrorKind::InvalidPayloadAttributes(_))
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
            ErrorKind::InvalidForkchoicePayload(detail) => {
                write!(f, "engine marked forkchoice payload invalid: {detail}")
            }
            ErrorKind::InvalidForkchoiceState(detail) => {
                write!(
                    f,
                    "engine rejected forkchoice state as inconsistent: {detail}"
                )
            }
            ErrorKind::UnknownFinalBlock(detail) => {
                write!(f, "engine could not resolve finalized block: {detail}")
            }
            ErrorKind::InvalidPayloadAttributes(detail) => {
                write!(f, "engine rejected payload attributes: {detail}")
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
        }
    }
}

impl std::error::Error for DriverError {}
