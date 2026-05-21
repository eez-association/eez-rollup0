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

    /// L1 JSON-RPC transport / log fetch failure. Treated as transient by
    /// the watcher: log + retry on the next tick without advancing the
    /// cursor.
    #[error("L1 RPC error: {0}")]
    L1Rpc(String),

    /// Required L1 config env var missing or malformed at startup. Fatal —
    /// the follower won't run without L1.
    #[error("L1 config error: {0}")]
    L1Config(String),

    /// A posted batch's calldata or payload couldn't be decoded
    /// (`postAndVerifyBatchCall` ABI mismatch or `eez_payload_codec::decode`
    /// failure). Treated as **permanent** by the watcher — silently skipping
    /// would drift the cumulative L2-block count below what the L1 contract
    /// accepted, so we halt instead and let the operator diagnose.
    #[error("permanently-malformed batch in L1 tx {tx_hash} at L1 block {l1_block}: {detail}")]
    BatchMalformed {
        l1_block: u64,
        tx_hash: alloy_primitives::TxHash,
        detail: String,
    },
}

impl FollowerError {
    /// True for errors that indicate a permanently-corrupt state the
    /// watcher can't recover from by retrying. Currently: only
    /// [`Self::BatchMalformed`]. The watcher's `run` loop exits on these.
    pub(crate) fn is_permanent_batch_failure(&self) -> bool {
        matches!(self, Self::BatchMalformed { .. })
    }
}
