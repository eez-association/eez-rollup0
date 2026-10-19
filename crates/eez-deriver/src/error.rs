//! Error type returned by the deriver.

use thiserror::Error;

/// Convenience [`Result`] alias used throughout the crate.
pub type DeriverResult<T> = Result<T, DeriverError>;

/// Error returned by [`Deriver`](crate::Deriver) operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DeriverError {
    /// L2 provider lookup failed.
    #[error("L2 provider error: {0}")]
    L2Provider(String),
    /// An L1-posted payload could not be decoded.
    #[error("payload codec error: {0}")]
    Codec(#[from] eez_payload_codec::CodecError),
    /// L1 catch-up scan failed.
    #[error("L1 catch-up scan error: {0}")]
    L1Scan(#[source] eez_l1::L1Error),
    /// `BlockCommitter` actor task is gone.
    #[error("block committer actor task has exited")]
    CommitterClosed,
    /// `engine_forkchoiceUpdated` rejected the safe/finalized cursors.
    #[error("engine rejected safe/finalized FCU: {0}")]
    InvalidForkchoice(String),
    /// Local L2 chain diverged from an L1-confirmed batch.
    #[error(
        "local L2 block {l2_block} diverged from L1-confirmed batch; the on-chain claimed newRoot doesn't match local STF output{}",
        detail.as_deref().map(|value| format!(" ({value})")).unwrap_or_default()
    )]
    LocalDiverged {
        /// Diverging L2 block number.
        l2_block: u64,
        /// Why the divergence was raised.
        detail: Option<String>,
    },
}

impl From<eez_driver::DriverError> for DeriverError {
    fn from(err: eez_driver::DriverError) -> Self {
        match err {
            eez_driver::DriverError::CommitterClosed => Self::CommitterClosed,
            eez_driver::DriverError::InvalidForkchoice(detail) => {
                Self::InvalidForkchoice(format!("engine rejected forkchoice update: {detail}"))
            }
            other => Self::InvalidForkchoice(format!("driver: {other}")),
        }
    }
}
