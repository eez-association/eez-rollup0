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

    /// Engine refused our forkchoice triplet as inconsistent — typically
    /// `safe`/`finalized` not in head's canonical chain, or zero head hash.
    /// Surfaced from reth as `ForkchoiceUpdateError`, distinct from
    /// transport failures. Transient: the next tick re-attempts with a
    /// freshly-built triplet after rolling back tentative state.
    #[error("engine rejected forkchoice update: {0}")]
    InvalidForkchoice(String),

    /// Engine-API transport error (RPC channel down, engine task crashed,
    /// etc.). Transient at the transport layer; retry next tick.
    #[error("engine-API transport error: {0}")]
    EngineRpc(String),

    /// Reth reported the FCU head links back to a block it rejected via
    /// payload validation (STF failure: bad state root, gas overflow,
    /// invalid tx, etc.). In our follower the rejection comes from reth's
    /// P2P backfill executing a sequencer-published block and finding it
    /// invalid — meaning the sequencer is publishing a chain we can't
    /// canonicalize. **Permanent**: the run loop halts the follower task
    /// so the operator notices instead of looping forever.
    #[error("sequencer chain rejected at block {block_number} ({block_hash}): {detail}")]
    InvalidChain {
        block_number: u64,
        block_hash: alloy_primitives::B256,
        detail: String,
    },

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
    /// True for errors that indicate a permanently-corrupt watcher state —
    /// the L1 has accepted a batch our codec can't decode, so our
    /// cumulative L2-block sum would silently drift below truth.
    /// The watcher's `run` loop exits on these.
    pub(crate) fn is_permanent_batch_failure(&self) -> bool {
        matches!(self, Self::BatchMalformed { .. })
    }

    /// True for errors that indicate a permanently-corrupt chain state the
    /// follower can't recover from by retrying — the sequencer has
    /// published a block reth rejects via STF validation. The follower's
    /// `run` loop exits on these.
    pub(crate) fn is_permanent_chain_failure(&self) -> bool {
        matches!(self, Self::InvalidChain { .. })
    }
}
