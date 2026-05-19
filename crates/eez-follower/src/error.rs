//! Error type returned by the follower task.
//!
//! Unlike `eez-driver`'s [`DriverError`](eez_driver::DriverError), which is a
//! public library API following the M-ERRORS-CANONICAL-STRUCTS pattern, this
//! type is internal to the follower binary and uses `thiserror` for brevity.

use thiserror::Error;

/// Errors produced while driving the follower's engine-API loop.
#[derive(Debug, Error)]
pub(crate) enum FollowerError {
    /// Sequencer JSON-RPC transport or decode failure.
    #[error("sequencer RPC error: {0}")]
    Rpc(String),

    /// `engine_forkchoiceUpdated` returned a non-`VALID`, non-`SYNCING` status.
    #[error("engine rejected forkchoice update: {0}")]
    InvalidForkchoice(String),

    /// Engine-API transport error.
    #[error("engine-API transport error: {0}")]
    EngineRpc(String),
}
