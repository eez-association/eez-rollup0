//! Submitter configuration, populated from `EEZ_*` environment variables.

use std::{env, str::FromStr, time::Duration};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_POSTER_KEY: &str = "EEZ_L1_POSTER_KEY";
const ENV_EEZ_ADDRESS: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_PROOF_SYSTEM_ADDRESS: &str = "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";
const ENV_DEPLOY_BLOCK: &str = "EEZ_REGISTRY_DEPLOY_BLOCK";
const ENV_INTERVAL_SECS: &str = "EEZ_SUBMITTER_INTERVAL_SECS";

const DEFAULT_INTERVAL_SECS: u64 = 60;

/// Submitter configuration.
#[derive(Clone)]
pub struct SubmitterConfig {
    /// L1 RPC endpoint (HTTP / HTTPS).
    pub rpc_url: Url,
    /// EOA that signs L1 txs (pays gas).
    pub poster: PrivateKeySigner,
    /// Deployed `EEZ` (Rollups registry) address.
    pub eez: Address,
    /// Deployed `ECDSAProofSystem` address — the one PS we attest with.
    pub proof_system: Address,
    /// `rollupId` returned by `EEZ.createRollup` for our L2.
    pub rollup_id: u64,
    /// L1 block where `EEZ` was deployed. Lower bound for the startup
    /// `BatchPosted` log scan that seeds `posted_through`. Keeps the scan
    /// bounded on busy chains.
    pub deploy_block: u64,
    /// Tick interval. One tick = at most one postBatch tx.
    pub interval: Duration,
}

impl std::fmt::Debug for SubmitterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitterConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("poster", &self.poster.address())
            .field("eez", &self.eez)
            .field("proof_system", &self.proof_system)
            .field("rollup_id", &self.rollup_id)
            .field("deploy_block", &self.deploy_block)
            .field("interval", &self.interval)
            .finish()
    }
}

impl SubmitterConfig {
    /// Read from `EEZ_*` env vars.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or malformed value.
    pub fn from_env() -> L1Result<Self> {
        let rpc_url = parse_url(ENV_RPC_URL)?;
        let poster = parse_key(ENV_POSTER_KEY)?;
        let eez = parse_address(ENV_EEZ_ADDRESS)?;
        let proof_system = parse_address(ENV_PROOF_SYSTEM_ADDRESS)?;
        let rollup_id = parse_u64(ENV_ROLLUP_ID)?;
        let deploy_block = parse_u64(ENV_DEPLOY_BLOCK)?;
        let interval = Duration::from_secs(parse_u64_or(ENV_INTERVAL_SECS, DEFAULT_INTERVAL_SECS)?);

        Ok(Self {
            rpc_url,
            poster,
            eez,
            proof_system,
            rollup_id,
            deploy_block,
            interval,
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

fn parse_u64_or(name: &str, default: u64) -> L1Result<u64> {
    match env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .map_err(|e| L1Error::Config(format!("{name}: {e}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => {
            Err(L1Error::Config(format!("{name} contains non-UTF-8 bytes")))
        }
    }
}
