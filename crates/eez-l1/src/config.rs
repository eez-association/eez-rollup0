//! Configuration for the [`Submitter`](crate::Submitter) (send-path),
//! populated from `EEZ_*` environment variables.
//!
//! Per-rollup composer/orchestration knobs (rollup id, proof system,
//! deploy block, mode flag) now live in
//! `eez-composer::RollupConfig` per the S4.2 umbrella extraction. See
//! `docs/plans/IMPLEMENTATION.md` §5.4.8.

use std::{env, str::FromStr};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_BUILDER_RPC_URL: &str = "EEZ_L1_BUILDER_RPC_URL";
const ENV_POSTER_KEY: &str = "EEZ_L1_POSTER_KEY";
const ENV_EEZ_ADDRESS: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";

/// L1 connectivity for the [`Submitter`](crate::Submitter) — what's
/// needed to send and read transactions against the EEZ registry.
#[derive(Clone)]
pub struct SubmitterConfig {
    /// L1 RPC endpoint (HTTP / HTTPS).
    pub rpc_url: Url,
    /// L1 builder relay accepting `eth_sendBundle`. All postBatch txs
    /// go here; `rpc_url` is used for reads only.
    pub builder_rpc_url: Url,
    /// EOA that signs L1 txs (pays gas).
    pub poster: PrivateKeySigner,
    /// Deployed `EEZ` (rollups registry) address.
    pub eez: Address,
    /// Our rollup's id. Used by `scan_batches` to filter the
    /// `L2ExecutionPerformed(rollupId indexed, ...)` event topic so
    /// each historical batch is tagged winner / loser.
    pub rollup_id: u64,
}

impl std::fmt::Debug for SubmitterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitterConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("builder_rpc_url", &self.builder_rpc_url.as_str())
            .field("poster", &self.poster.address())
            .field("eez", &self.eez)
            .field("rollup_id", &self.rollup_id)
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
        Ok(Self {
            rpc_url: parse_url(ENV_RPC_URL)?,
            builder_rpc_url: parse_url(ENV_BUILDER_RPC_URL)?,
            poster: parse_key(ENV_POSTER_KEY)?,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
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
