//! Error type returned by the deriver.

/// Convenience [`Result`] alias used throughout the crate.
pub type DeriverResult<T> = Result<T, DeriverError>;

/// Error returned by [`Deriver`](crate::Deriver) operations.
#[derive(Debug, thiserror::Error)]
pub enum DeriverError {
    /// L2 provider lookup failed (e.g., reading a block at a given
    /// height to compare against an L1-derived batch).
    #[error("L2 provider error: {0}")]
    L2Provider(String),
    /// `eez-payload-codec::decode` rejected an L1-posted batch's
    /// `call_data`. Usually indicates a contract that posted a payload
    /// in a version we don't speak.
    #[error("payload codec error: {0}")]
    Codec(#[from] eez_payload_codec::CodecError),
    /// L1 catch-up scan failed. Callers can inspect nested typed L1
    /// errors by matching on the wrapped [`eez_l1::L1Error`].
    #[error("L1 catch-up scan error: {0}")]
    L1Scan(eez_l1::L1Error),
    /// `BlockCommitter` actor task is gone; the deriver can't push
    /// safe-head advances any further.
    #[error("block committer actor task has exited")]
    CommitterClosed,
    /// `engine_forkchoiceUpdated` rejected the safe/finalized cursors
    /// the deriver tried to set. Usually means the L1-derived hashes
    /// don't match reth's canonical chain — a genuine divergence.
    #[error("engine rejected safe/finalized FCU: {0}")]
    InvalidForkchoice(String),
    /// Local L2 chain diverged from an L1-confirmed batch — our block
    /// at `l2_block` has different content than the batch says it
    /// should. Real reorg/replay is a follow-up; today this halts the
    /// deriver loudly.
    #[error(
        "local L2 block {l2_block} diverged from L1-confirmed batch; \
         the on-chain claimed newState doesn't match local STF output{}",
        detail.as_ref().map(|d| format!(" ({d})")).unwrap_or_default()
    )]
    LocalDiverged {
        /// L2 block height at which the divergence was detected.
        l2_block: u64,
        /// Why the divergence was raised (gate failure, prefix mismatch,
        /// replay error). Surfaced in `Display` — silent failures are bugs.
        detail: Option<String>,
    },
}

impl From<eez_driver::DriverError> for DeriverError {
    fn from(err: eez_driver::DriverError) -> Self {
        match err {
            eez_driver::DriverError::CommitterClosed => Self::CommitterClosed,
            eez_driver::DriverError::InvalidForkchoice(_) => {
                Self::InvalidForkchoice(err.to_string())
            }
            _ => Self::InvalidForkchoice(format!("driver: {err}")),
        }
    }
}
