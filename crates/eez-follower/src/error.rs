//! Error type returned by the follower task and its L1 watcher.
//!
//! Unlike `eez-driver`'s [`DriverError`](eez_driver::DriverError), which is a
//! public library API following the M-ERRORS-CANONICAL-STRUCTS pattern, this
//! type is internal to the follower binary and uses `thiserror` for brevity.

use thiserror::Error;

/// Errors produced while driving the follower's engine-API loop or its L1
/// watcher.
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

    /// Local reth provider returned an error or unexpected `None`. Fatal at
    /// startup (we can't run without genesis); per-tick lookups fall back
    /// gracefully and don't surface as this error.
    #[error("local L2 provider error: {0}")]
    L2Provider(String),

    /// L1 JSON-RPC transport / log fetch / abi decode failure. Per-tick;
    /// logged and the watcher loop continues.
    #[error("L1 RPC error: {0}")]
    L1Rpc(String),

    /// Required L1 config env var missing or malformed at startup. Fatal —
    /// the follower won't run without L1.
    #[error("L1 config error: {0}")]
    L1Config(String),

    /// A posted batch's payload decoded to nonsense. Logged, batch skipped.
    #[error("payload codec error: {0}")]
    Codec(#[from] eez_payload_codec::CodecError),
}
