//! Error type for the sequencer-RPC unsafe-head follower task.

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum FollowerError {
    /// Sequencer JSON-RPC transport or decode failure.
    #[error("sequencer RPC error: {0}")]
    Rpc(String),

    /// Shared driver/committer error.
    #[error("driver error: {0}")]
    Driver(String),
}
