//! Configuration for read-only L1 access and signed batch submission,
//! populated from `EEZ_*` environment variables.
//!
//! Per-rollup composer/orchestration knobs remain in
//! `eez-composer::RollupConfig`; the registry deploy block lives here because
//! it defines the lower bound of the L1 reader's historical scan.

use std::{env, str::FromStr};

use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use eez_protocol::Rollup0Deployment;
use url::Url;

use crate::error::{L1Error, L1Result};

const ENV_RPC_URL: &str = "EEZ_L1_RPC_URL";
const ENV_TRACE_RPC_URL: &str = "EEZ_L1_TRACE_RPC_URL";
const ENV_BEACON_URL: &str = "EEZ_L1_BEACON_URL";
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
    /// L1 RPC serving `debug_traceBlockByHash`, read only for blocks where a
    /// contract posted a batch for our rollup.
    pub trace_rpc_url: Url,
    /// Deployed `EEZ` (rollups registry) address.
    pub eez: Address,
    /// Our rollup's id. The batch scan reads only posts whose `BatchPosted`
    /// lists it.
    pub rollup_id: u64,
    /// L1 block where `EEZ` was deployed. Lower bound for historical batch
    /// scans and boot-time source-readiness checks.
    pub deploy_block: u64,
    /// Blob deployment used to reproduce scanned commitments; `None` reads calldata only.
    pub candidate: Option<Rollup0Deployment>,
    /// Beacon REST endpoint, required when a blob deployment is configured.
    pub beacon_url: Option<Url>,
}

impl std::fmt::Debug for L1ReaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L1ReaderConfig")
            .field("rpc_url", &self.rpc_url.as_str())
            .field("trace_rpc_url", &self.trace_rpc_url.as_str())
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
        let rpc_url = parse_url(ENV_RPC_URL)?;
        Ok(Self {
            trace_rpc_url: trace_rpc_url(&rpc_url)?,
            rpc_url,
            eez: parse_address(ENV_EEZ_ADDRESS)?,
            rollup_id: parse_u64(ENV_ROLLUP_ID)?,
            deploy_block: parse_u64(ENV_REGISTRY_DEPLOY_BLOCK)?,
            candidate: None,
            beacon_url: None,
        })
    }

    /// Read candidate-V1 posts of `candidate`; `None` keeps the calldata profile.
    ///
    /// # Errors
    ///
    /// [`L1Error::Config`] for a missing or malformed `EEZ_L1_BEACON_URL`
    /// when a candidate deployment is configured.
    pub fn with_candidate(mut self, candidate: Option<Rollup0Deployment>) -> L1Result<Self> {
        if candidate.is_some() && self.beacon_url.is_none() {
            self.beacon_url = Some(parse_url(ENV_BEACON_URL)?);
        }
        self.candidate = candidate;
        Ok(self)
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

/// `EEZ_L1_TRACE_RPC_URL`, else `rpc_url`: the reader and the watcher both
/// resolve their trace endpoint here.
pub(crate) fn trace_rpc_url(rpc_url: &Url) -> L1Result<Url> {
    resolve_trace_rpc_url(rpc_url, env::var(ENV_TRACE_RPC_URL).ok())
}

fn resolve_trace_rpc_url(rpc_url: &Url, raw: Option<String>) -> L1Result<Url> {
    match raw {
        Some(raw) if !raw.is_empty() => {
            Url::parse(&raw).map_err(|e| L1Error::Config(format!("{ENV_TRACE_RPC_URL}: {e}")))
        }
        _ => Ok(rpc_url.clone()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_trace_rpc_url_defaults_to_the_rpc_url() {
        let rpc_url = Url::parse("http://logs.example").expect("static url");
        for (raw, want) in [
            (None, "http://logs.example/"),
            (Some(""), "http://logs.example/"),
            (Some("http://archive.example"), "http://archive.example/"),
        ] {
            let got = resolve_trace_rpc_url(&rpc_url, raw.map(String::from)).expect("valid");
            assert_eq!(got.as_str(), want, "{raw:?}");
        }
        let err = resolve_trace_rpc_url(&rpc_url, Some("not a url".into())).expect_err("garbage");
        assert!(
            matches!(err, L1Error::Config(ref m) if m.starts_with("EEZ_L1_TRACE_RPC_URL")),
            "{err}"
        );
    }
}
