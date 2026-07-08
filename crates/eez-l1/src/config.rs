//! Configuration for the [`Submitter`](crate::Submitter) (send-path),
//! populated from `EEZ_*` environment variables.
//!
//! Per-rollup composer/orchestration knobs (rollup id, proof system,
//! deploy block, mode flag) now live in
//! `eez-composer::RollupConfig` per the S4.2 umbrella extraction. See
//! `docs/plans/IMPLEMENTATION.md` §5.4.8.

use std::{env, str::FromStr};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_BUILDER_RPC_URL: &str = "EEZ_L1_BUILDER_RPC_URL";
const ENV_TARGET_RPC_URL: &str = "EEZ_L1_TARGET_RPC_URL";
const ENV_POSTER_KEY: &str = "EEZ_L1_POSTER_KEY";
const ENV_EEZ_ADDRESS: &str = "EEZ_REGISTRY_ADDRESS";
const ENV_ROLLUP_ID: &str = "EEZ_ROLLUP_ID";
const ENV_DEPLOY_BLOCK: &str = "EEZ_REGISTRY_DEPLOY_BLOCK";

/// L1 connectivity for the [`Submitter`](crate::Submitter) — what's
/// needed to send and read transactions against the EEZ registry.
#[derive(Clone)]
pub struct SubmitterConfig {
    /// L1 RPC endpoint (HTTP / HTTPS).
    pub rpc_url: Url,
    /// L1 builder relay accepting `eth_sendBundle`. All postBatch txs
    /// go here; `rpc_url` is used for reads only.
    pub builder_rpc_url: Url,
    /// Optional RPC used ONLY for `BundleTarget::NextBlock` target-block
    /// calculation. The embedded L1 can lag the canonical tip by 2-3
    /// blocks, so `target = local.latest + slack` lands already-past and
    /// the bundler drops it; point this at a tip-following node to pick
    /// an unproposed future block. `None` → falls back to `rpc_url`.
    pub target_rpc_url: Option<Url>,
    /// EOA that signs L1 txs (pays gas).
    pub poster: PrivateKeySigner,
    /// Deployed `EEZ` (rollups registry) address.
    pub eez: Address,
    /// Our rollup's id. Used by the batch-log scanner to filter the
    /// `L2ExecutionPerformed(rollupId indexed, ...)` event topic so
    /// each historical batch is tagged winner / loser.
    pub rollup_id: u64,
}

impl std::fmt::Debug for SubmitterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitterConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("builder_rpc_url", &self.builder_rpc_url.as_str())
            .field(
                "target_rpc_url",
                &self.target_rpc_url.as_ref().map(Url::as_str),
            )
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
        let rpc_url = parse_url(ENV_RPC_URL)?;
        let builder_rpc_url = parse_url(ENV_BUILDER_RPC_URL)?;
        Ok(Self {
            rpc_url,
            builder_rpc_url,
            target_rpc_url,
            poster: parse_key(ENV_POSTER_KEY)?,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
        })
    }

    /// Read L1 config for read-only consumers. If
    /// `EEZ_L1_POSTER_KEY` is absent, install a deterministic dummy key
    /// because scan paths never sign or send transactions.
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required read-side var
    /// or malformed value.
    pub fn from_env_read_only() -> L1Result<Self> {
        let rpc_url = parse_url(ENV_RPC_URL)?;
        Ok(Self {
            rpc_url: rpc_url.clone(),
            builder_rpc_url: rpc_url,
            // Read-only / scan consumers never compute a NextBlock
            // target, so the override is irrelevant here.
            target_rpc_url: None,
            poster: parse_key_or_dummy(ENV_POSTER_KEY)?,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
        })
    }
}

/// Reads the EEZ registry deploy block without requiring composer-only
/// settings such as proof-system addresses.
///
/// # Errors
///
/// Returns [`L1Error::Config`] if `EEZ_REGISTRY_DEPLOY_BLOCK` is
/// missing or malformed.
pub fn registry_deploy_block_from_env() -> L1Result<u64> {
    parse_u64(ENV_DEPLOY_BLOCK)
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

fn parse_key_or_dummy(name: &str) -> L1Result<PrivateKeySigner> {
    match env::var(name) {
        Ok(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                dummy_key(name)
            } else {
                PrivateKeySigner::from_str(raw.trim_start_matches("0x"))
                    .map_err(|e| L1Error::Config(format!("{name}: {e}")))
            }
        }
        Err(env::VarError::NotPresent) => dummy_key(name),
        Err(env::VarError::NotUnicode(_)) => {
            Err(L1Error::Config(format!("{name} contains non-UTF-8 bytes")))
        }
    }
}

fn dummy_key(name: &str) -> L1Result<PrivateKeySigner> {
    PrivateKeySigner::from_bytes(&B256::with_last_byte(1))
        .map_err(|e| L1Error::Config(format!("dummy {name}: {e}")))
}

fn parse_u64(name: &str) -> L1Result<u64> {
    require(name)?
        .parse::<u64>()
        .map_err(|e| L1Error::Config(format!("{name}: {e}")))
}
