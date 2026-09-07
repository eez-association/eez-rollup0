//! Configuration for read-only L1 access and signed batch submission.
//!
//! Per-rollup composer/orchestration knobs remain in
//! `eez-composer::RollupConfig`; the registry deploy block lives here because
//! it defines the lower bound of the L1 reader's historical scan.

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use url::Url;

/// Read-only L1 connectivity used by the Deriver's canonical-chain scans.
#[derive(Clone)]
pub struct L1ReaderConfig {
    /// L1 RPC endpoint (HTTP / HTTPS).
    pub rpc_url: Url,
    /// Deployed `EEZ` (rollups registry) address.
    pub eez: Address,
    /// Our rollup's id. Used by the batch-log scanner to filter the
    /// `L2ExecutionPerformed(rollupId indexed, ...)` event topic so
    /// each historical batch is tagged winner / loser.
    pub rollup_id: u64,
    /// L1 block where `EEZ` was deployed. Lower bound for historical batch
    /// scans and boot-time source-readiness checks.
    pub deploy_block: u64,
}

impl std::fmt::Debug for L1ReaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L1ReaderConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("eez", &self.eez)
            .field("rollup_id", &self.rollup_id)
            .field("deploy_block", &self.deploy_block)
            .finish()
    }
}

/// Signed submission configuration. Read-side settings are grouped in
/// [`L1ReaderConfig`]; the remaining fields exist only for posting batches.
#[derive(Clone)]
pub struct SubmitterConfig {
    /// Canonical L1 source used for reads and as the default target-tip RPC.
    pub reader: L1ReaderConfig,
    /// L1 builder relay accepting `eth_sendBundle`. All postBatch txs
    /// go here; [`L1ReaderConfig::rpc_url`] is used for reads only.
    pub builder_rpc_url: Url,
    /// Optional RPC used ONLY for `BundleTarget::NextBlock` target-block
    /// calculation. The embedded L1 can lag the canonical tip by 2-3
    /// blocks, so `target = local.latest + slack` lands already-past and
    /// the bundler drops it; point this at a tip-following node to pick
    /// an unproposed future block. `None` falls back to
    /// [`L1ReaderConfig::rpc_url`].
    pub target_rpc_url: Option<Url>,
    /// EOA that signs L1 txs (pays gas).
    pub poster: PrivateKeySigner,
}

impl std::fmt::Debug for SubmitterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitterConfig")
            .field("reader", &self.reader)
            .field("builder_rpc_url", &self.builder_rpc_url.as_str())
            .field(
                "target_rpc_url",
                &self.target_rpc_url.as_ref().map(Url::as_str),
            )
            .field("poster", &self.poster.address())
            .finish()
    }
}
