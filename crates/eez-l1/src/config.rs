//! Configuration for read-only L1 access and signed batch submission,
//! populated from `EEZ_*` environment variables.
//!
//! Per-rollup composer/orchestration knobs remain in
//! `eez-composer::RollupConfig`; the registry deploy block lives here because
//! it defines the lower bound of the L1 reader's historical scan.

use std::{env, str::FromStr};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_BUILDER_RPC_URL: &str = "EEZ_L1_BUILDER_RPC_URL";
const ENV_TARGET_RPC_URL: &str = "EEZ_L1_TARGET_RPC_URL";
const ENV_POSTER_KEY: &str = "EEZ_L1_POSTER_KEY";
const ENV_EEZ_ADDRESS: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";
const ENV_REGISTRY_DEPLOY_BLOCK: &str = "EEZ_REGISTRY_DEPLOY_BLOCK";

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

impl L1ReaderConfig {
    /// Read the canonical-chain scan configuration from `EEZ_*` env vars.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env() -> L1Result<Self> {
        Ok(Self {
            rpc_url: parse_url(ENV_RPC_URL)?,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
            deploy_block: parse_u64(ENV_REGISTRY_DEPLOY_BLOCK)?,
        })
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

impl SubmitterConfig {
    /// Read from `EEZ_*` env vars.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env() -> L1Result<Self> {
        let target_rpc_url = match env::var(ENV_TARGET_RPC_URL) {
            Ok(raw) if !raw.is_empty() => Some(
                Url::parse(&raw)
                    .map_err(|e| L1Error::Config(format!("{ENV_TARGET_RPC_URL}: {e}")))?,
            ),
            _ => None,
        };
        // Builder relay endpoint. On chiado this is an external
        // Flashbots-style relay. On the embedded dev/testing L1 the node
        // serves `eth_sendBundle` itself (see `eez-node::bundle_rpc`), so
        // set this to the same value as EEZ_L1_RPC_URL.
        let builder_rpc_url = parse_url(ENV_BUILDER_RPC_URL)?;
        Ok(Self {
            reader: L1ReaderConfig::from_env()?,
            builder_rpc_url,
            target_rpc_url,
            poster: parse_key(ENV_POSTER_KEY)?,
        })
    }
}

fn require(name: &str) -> L1Result<String> {
    env::var(name).map_err(|_| L1Error::Config(format!("{name} is required (see .env.example)")))
}

fn parse_url(name: &str) -> L1Result<Url> {
    Url::parse(&require(name)?).map_err(|e| L1Error::Config(format!("{name}: {e}")))
}

fn parse_address(name: &str) -> L1Result<Address> {
    Address::from_str(&require(name)?).map_err(|e| L1Error::Config(format!("{name}: {e}")))
}

fn parse_key(name: &str) -> L1Result<PrivateKeySigner> {
    let raw = require(name)?;
    PrivateKeySigner::from_str(raw.trim_start_matches("0x"))
        .map_err(|e| L1Error::Config(format!("{name}: {e}")))
}

fn parse_u64(name: &str) -> L1Result<u64> {
    require(name)?
        .parse::<u64>()
        .map_err(|e| L1Error::Config(format!("{name}: {e}")))
}
