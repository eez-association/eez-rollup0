//! Error type returned by the driver.

use alloy_primitives::B256;

/// Convenience [`Result`] alias used throughout the crate.
pub type DriverResult<T> = Result<T, DriverError>;

/// Error returned by [`Sequencer`](crate::Sequencer) operations.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    /// Underlying provider returned an error during a state lookup.
    #[error("provider error: {0}")]
    Provider(String),
    /// Expected a header at the given block number but the provider returned
    /// `None` — typically a "best block doesn't exist" race during startup.
    #[error("no header found at block {block_number}")]
    MissingHeader {
        /// Block number the header was expected at.
        block_number: u64,
    },
    /// `engine_forkchoiceUpdated` returned a non-`VALID` status.
    #[error("engine rejected forkchoice update: {0}")]
    InvalidForkchoice(String),
    /// `engine_newPayload` returned a non-`VALID` status.
    #[error("engine rejected new payload: {0}")]
    InvalidPayload(String),
    /// Payload builder returned no payload for an issued ID.
    #[error("payload builder returned no payload for issued id")]
    PayloadMissing,
    /// Engine-API RPC transport error.
    #[error("engine-API transport error: {0}")]
    EngineRpc(String),
    /// `BlockCommitter` actor task is gone (channel closed). The Sequencer
    /// can't recover; the caller (typically eez-node main) will log + exit.
    #[error("block committer actor task has exited")]
    CommitterClosed,
    /// `RollupTiming` env loading or validation failed (operator misconfig
    /// at startup). The Sequencer refuses to start.
    #[error("RollupTiming misconfig: {0}")]
    TimingConfig(String),
    /// Sequencer's snapshotted `parent_hash` no longer matches
    /// `last_header`. Caller retries next tick.
    #[error("stale parent on sequence: snapshot was {expected}, last_header is now {actual}")]
    StaleParent {
        /// Parent hash the sequencer snapshotted.
        expected: B256,
        /// Actual `last_header` hash at commit time.
        actual: B256,
    },
}
