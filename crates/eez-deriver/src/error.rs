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
    /// An L1 reorg or index revalidation reached below the seed floor, an
    /// L1-finalized block: finality broke or the RPC lied.
    #[error(
        "L1 reorg to block {l1_block} is below the seed floor at L1-finalized block {floor_l1_block}"
    )]
    BelowSeedFloor {
        /// Highest L1 block the retreat would keep.
        l1_block: u64,
        /// L1 block of the seed record.
        floor_l1_block: u64,
    },
    /// Peers mode found an empty batch index: the boot never seeded it.
    #[error("batch index is empty in peers mode; boot must seed it before catch-up")]
    Unseeded,
    /// A resumed batch appends to a Sync block that is already finalized.
    #[error(
        "resumed batch would rewrite Sync block {l2_block}, at or below the finalized L2 head {finalized_l2}"
    )]
    FinalizedResume {
        /// Height of the finalized Sync block.
        l2_block: u64,
        /// The finalized L2 head that refuses it.
        finalized_l2: u64,
    },
}

/// `true` for errors no resync can fix; the deriver stops the node on them.
#[must_use]
pub(crate) fn is_fatal(err: &DeriverError) -> bool {
    matches!(
        err,
        DeriverError::BelowSeedFloor { .. }
            | DeriverError::Unseeded
            | DeriverError::FinalizedResume { .. }
    )
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

#[cfg(test)]
mod tests {
    use super::{DeriverError, is_fatal};

    #[test]
    fn floor_errors_are_fatal() {
        assert!(is_fatal(&DeriverError::BelowSeedFloor {
            l1_block: 99,
            floor_l1_block: 100,
        }));
        assert!(is_fatal(&DeriverError::Unseeded));
        assert!(is_fatal(&DeriverError::FinalizedResume {
            l2_block: 7,
            finalized_l2: 9,
        }));
        assert!(!is_fatal(&DeriverError::LocalDiverged {
            l2_block: 7,
            detail: None,
        }));
        assert!(!is_fatal(&DeriverError::L1Scan(eez_l1::L1Error::Provider(
            "x".into()
        ))));
    }
}
