//! Configuration for the [`Submitter`](crate::Submitter) (send-path) and
//! [`Composer`](crate::Composer) (orchestration), populated from `EEZ_*`
//! environment variables.
//!
//! The split mirrors the runtime split: [`SubmitterConfig`] carries what
//! the L1 send primitive needs (RPC, wallet, EEZ address), while
//! [`ComposerConfig`] carries orchestration knobs (rollup id, proof
//! system, deploy block for the startup scan, tick interval). The
//! eez-node binary reads both at startup.

use std::{env, str::FromStr, time::Duration};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_POSTER_KEY: &str = "EEZ_L1_POSTER_KEY";
const ENV_EEZ_ADDRESS: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_PROOF_SYSTEM_ADDRESS: &str = "EEZ_MOCK_PROOF_SYSTEM_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";
const ENV_DEPLOY_BLOCK: &str = "EEZ_REGISTRY_DEPLOY_BLOCK";
const ENV_INTERVAL_SECS: &str = "EEZ_COMPOSER_INTERVAL_SECS";

const DEFAULT_INTERVAL_SECS: u64 = 60;

/// L1 connectivity for the [`Submitter`](crate::Submitter) — what's
/// needed to send and read transactions against the EEZ registry.
#[derive(Clone)]
pub struct SubmitterConfig {
    /// L1 RPC endpoint (HTTP / HTTPS).
    pub rpc_url: Url,
    /// EOA that signs L1 txs (pays gas).
    pub poster: PrivateKeySigner,
    /// Deployed `EEZ` (rollups registry) address.
    pub eez: Address,
}

impl std::fmt::Debug for SubmitterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitterConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("poster", &self.poster.address())
            .field("eez", &self.eez)
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
            poster: parse_key(ENV_POSTER_KEY)?,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
        })
    }
}

/// Orchestration knobs for the [`Composer`](crate::Composer) — protocol
/// identifiers, the L1 block to start the startup scan from, and the
/// tick interval.
#[derive(Debug, Clone)]
pub struct ComposerConfig {
    /// Deployed `MockECDSAProofSystem` address — the one PS we attest with.
    pub proof_system: Address,
    /// `rollupId` returned by `EEZ.registerRollup` for our L2.
    pub rollup_id: u64,
    /// L1 block where `EEZ` was deployed. Lower bound for the startup
    /// `BatchPosted` log scan that seeds `posted_through`. Keeps the scan
    /// bounded on busy chains.
    pub deploy_block: u64,
    /// Tick interval. One tick = at most one postBatch tx.
    pub interval: Duration,
}

impl ComposerConfig {
    /// Read from `EEZ_*` env vars.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env() -> L1Result<Self> {
        Ok(Self {
            proof_system: parse_address(ENV_PROOF_SYSTEM_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
            deploy_block: parse_u64(ENV_DEPLOY_BLOCK)?,
            interval: Duration::from_secs(parse_u64_or(ENV_INTERVAL_SECS, DEFAULT_INTERVAL_SECS)?),
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
