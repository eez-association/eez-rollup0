//! Error type for L1 interactions. `thiserror`-derived; small surface.

use thiserror::Error;

/// Convenience [`Result`] alias.
pub type L1Result<T> = Result<T, L1Error>;

/// Error returned by the [`Submitter`](crate::Submitter).
#[derive(Debug, Error)]
pub enum L1Error {
    /// Bad / missing env var, malformed URL, etc.
    #[error("configuration error: {0}")]
    Config(String),
    /// Anything that went wrong talking to L1 (RPC transport, contract call decode).
    #[error("L1 provider error: {0}")]
    Provider(String),
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
    /// Could not read a block from the L2 reth provider.
    #[error("L2 source error: {0}")]
    L2Source(String),
    /// Payload codec rejected the input.
    #[error("payload codec error: {0}")]
    Codec(#[from] eez_payload_codec::CodecError),
    /// Prover failed to produce a proof for the public inputs hash.
    #[error("prover error: {0}")]
    Prover(String),
    /// Local L2 head is behind what the contract has already accepted —
    /// likely a wiped datadir or a fresh node against a running contract.
    /// Refusing to start; operator must reconcile (restore datadir or
    /// redeploy a fresh contract). Stage-3 Deriver will catch this up
    /// automatically once it lands.
    #[error("local L2 head ({local}) is behind on-chain posted head ({on_chain})")]
    L2Behind { local: u64, on_chain: u64 },
    /// L1 reorg deeper than the configured tolerance — beyond Ethereum
    /// finality. Operator must intervene (most likely a transient L1
    /// node anomaly; check L1 health and restart).
    #[error(
        "L1 reorg too deep: walked {walked} blocks without finding a common ancestor (max {max})"
    )]
    ReorgTooDeep { walked: usize, max: usize },
}
