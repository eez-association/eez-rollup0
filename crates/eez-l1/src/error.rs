//! Error type for L1 interactions. `thiserror`-derived; small surface.

use alloy_primitives::B256;
use thiserror::Error;

/// Convenience [`Result`] alias.
pub type L1Result<T> = Result<T, L1Error>;

/// Error returned by the [`Submitter`](crate::Submitter).
#[derive(Debug, Error)]
pub enum L1Error {
    /// Bad / missing env var, malformed URL, etc.
    #[error("configuration error: {0}")]
    Config(String),
    /// Anything that went wrong talking to L1 (RPC transport, JSON-RPC error).
    #[error("L1 provider error: {0}")]
    Provider(String),
    /// On-chain BYTES we cannot decode. Deterministic, so callers fail loudly
    /// rather than loop. A malformed RPC *response* is `Provider`, not this.
    #[error("L1 decode error: {0}")]
    Decode(String),
    /// L1 exposed canonical metadata but not all data needed to decode
    /// or replay it yet. This is expected while a local L1 source is
    /// warming up and should be retried by boot reconciliation.
    #[error("L1 source incomplete at block {block}: tx {tx_hash} unavailable ({detail})")]
    SourceIncomplete {
        block: u64,
        tx_hash: B256,
        detail: String,
    },
    /// postBatch tx didn't land or reverted on-chain.
    #[error("postBatch submission failed: {0}")]
    Submission(String),
    /// The configured builder relay answered `eth_sendBundle` with
    /// JSON-RPC `-32601` (method not found) — it is a plain execution
    /// RPC, not a bundle relay. [`Submitter::send_bundle`] catches this
    /// and degrades to ordered mempool submission (dev L1, anvil).
    ///
    /// [`Submitter::send_bundle`]: crate::Submitter::send_bundle
    #[error("builder relay does not support eth_sendBundle")]
    BundleRpcUnsupported,
    /// L1 reorg deeper than the configured tolerance — beyond Ethereum
    /// finality. Operator must intervene (most likely a transient L1
    /// node anomaly; check L1 health and restart).
    #[error(
        "L1 reorg too deep: walked {walked} blocks without finding a common ancestor (max {max})"
    )]
    ReorgTooDeep { walked: usize, max: usize },
}

impl L1Error {
    /// Returns true when the L1 source may simply need more time to
    /// expose already-observed canonical data.
    #[must_use]
    pub const fn is_source_incomplete(&self) -> bool {
        matches!(self, Self::SourceIncomplete { .. })
    }

    /// True for a transport failure (DNS, TCP, HTTP) — the peer never answered,
    /// so it says nothing about the payload we sent. Decode failures are NOT
    /// transport: they are deterministic and never clear on retry.
    #[must_use]
    pub const fn is_transport(&self) -> bool {
        matches!(self, Self::Provider(_))
    }

    /// True when retrying re-reads the same bytes or re-walks the same chain,
    /// so a polling caller would spin forever instead of failing. Both arms
    /// document operator intervention; this is what makes that real.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Decode(_) | Self::ReorgTooDeep { .. })
    }
}
