//! Error type for L1 interactions. `thiserror`-derived; small surface.

use thiserror::Error;

/// Convenience [`Result`] alias.
pub type L1Result<T> = Result<T, L1Error>;

/// Error returned by L1 readers, watchers, and the [`Submitter`](crate::Submitter).
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
    /// L1 served part of what a read needs but not the rest yet: a lagging
    /// source, a reorg between two calls, or logs from two forks of one block.
    #[error("L1 source incomplete at block {block}: {detail}")]
    SourceIncomplete { block: u64, detail: String },
    /// A batch post for our rollup in a shape this node does not read.
    #[error("unsupported L1 batch post at block {block}, log {log_index}: {detail}")]
    Unsupported {
        block: u64,
        log_index: u64,
        detail: String,
    },
    /// The trace endpoint cannot trace a block this node must trace.
    #[error("L1 trace unavailable at block {block}: {detail}")]
    TraceUnavailable { block: u64, detail: String },
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
    /// The same read can succeed later: transport trouble, or a source that
    /// has not caught up.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Provider(_) | Self::SourceIncomplete { .. })
    }

    /// Retrying cannot change the answer, so readers halt instead of looping.
    /// `Config`, `Submission` and `BundleRpcUnsupported` are handled where they arise.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Decode(_)
                | Self::Unsupported { .. }
                | Self::TraceUnavailable { .. }
                | Self::ReorgTooDeep { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::L1Error;

    #[test]
    fn each_variant_is_retryable_terminal_or_handled_where_it_arises() {
        const RETRYABLE: (bool, bool) = (true, false);
        const TERMINAL: (bool, bool) = (false, true);
        const NEITHER: (bool, bool) = (false, false);
        let (block, d) = (7, String::new);
        let cases = [
            (L1Error::Config(d()), NEITHER),
            (L1Error::Provider(d()), RETRYABLE),
            (L1Error::Decode(d()), TERMINAL),
            (L1Error::SourceIncomplete { block, detail: d() }, RETRYABLE),
            (
                L1Error::Unsupported {
                    block,
                    log_index: 3,
                    detail: d(),
                },
                TERMINAL,
            ),
            (L1Error::TraceUnavailable { block, detail: d() }, TERMINAL),
            (L1Error::Submission(d()), NEITHER),
            (L1Error::BundleRpcUnsupported, NEITHER),
            (L1Error::ReorgTooDeep { walked: 0, max: 0 }, TERMINAL),
        ];
        for (err, class) in cases {
            assert_eq!((err.is_retryable(), err.is_terminal()), class, "{err:?}");
        }
    }
}
