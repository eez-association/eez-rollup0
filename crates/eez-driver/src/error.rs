//! Error type returned by the driver.

use alloy_primitives::B256;
use thiserror::Error;

/// Convenience [`Result`] alias used throughout the crate.
pub type DriverResult<T> = Result<T, DriverError>;

/// Error returned by [`Sequencer`](crate::Sequencer) operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DriverError {
    /// Underlying provider returned an error during a state lookup.
    #[error("provider error: {0}")]
    Provider(String),
    /// Expected a header at the given block number but the provider returned
    /// `None` — typically a "best block doesn't exist" race during startup.
    #[error("no header found at block {block_number}")]
    MissingHeader { block_number: u64 },
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
    /// `BlockCommitter` actor task is gone (channel closed).
    #[error("block committer actor task has exited")]
    CommitterClosed,
    /// `RollupTiming` env loading or validation failed.
    #[error("RollupTiming misconfig: {0}")]
    TimingConfig(String),
    /// Sequencer's snapshotted `parent_hash` no longer matches `last_header`.
    #[error("stale parent on sequence: snapshot was {expected}, last_header is now {actual}")]
    StaleParent { expected: B256, actual: B256 },
}
