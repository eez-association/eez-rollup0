//! File-backed configuration shared by the Composer and follower launchers.

use std::{fmt, path::Path, str::FromStr};

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use eyre::Context as _;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Path to the role-specific eez configuration file.
#[derive(clap::Args, Debug, Clone)]
pub struct ConfigArgs {
    /// TOML file containing the complete role-specific EEZ configuration.
    #[arg(long = "eez.config", value_name = "PATH")]
    pub eez_config_path: std::path::PathBuf,
}

/// Load one role's complete configuration document.
///
/// # Errors
///
/// Returns an error when the file cannot be read or is not valid TOML for `T`.
pub fn load<T: DeserializeOwned>(path: &Path) -> eyre::Result<T> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read eez config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parse eez config {}", path.display()))
}

/// Secret secp256k1 key read from a `0x`-prefixed TOML string.
#[derive(Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct SecretKey(B256);

impl SecretKey {
    /// Parse the configured key as a local signer.
    ///
    /// # Errors
    ///
    /// Returns an error when the scalar is not a valid secp256k1 private key.
    pub fn signer(&self) -> eyre::Result<PrivateKeySigner> {
        PrivateKeySigner::from_bytes(&self.0).map_err(Into::into)
    }
}

impl FromStr for SecretKey {
    type Err = alloy_primitives::hex::FromHexError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        B256::from_str(raw.strip_prefix("0x").unwrap_or(raw)).map(Self)
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Canonical L1 source and deployed-rollup identity shared by both roles.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct L1Config {
    pub rpc_url: url::Url,
    pub chain_id: u64,
    pub registry_address: alloy_primitives::Address,
    pub registry_deploy_block: u64,
    pub rollup_id: u64,
    #[serde(default = "default_reorg_max_depth")]
    pub reorg_max_depth: usize,
}

impl L1Config {
    #[cfg(feature = "l1-startup")]
    #[must_use]
    pub fn reader(&self) -> eez_l1::L1ReaderConfig {
        eez_l1::L1ReaderConfig {
            rpc_url: self.rpc_url.clone(),
            eez: self.registry_address,
            rollup_id: self.rollup_id,
            deploy_block: self.registry_deploy_block,
        }
    }

    #[cfg(feature = "l1-startup")]
    #[must_use]
    pub fn watcher(&self) -> eez_l1::L1WatcherConfig {
        eez_l1::L1WatcherConfig {
            rpc_url: self.rpc_url.clone(),
            eez: self.registry_address,
            rollup_id: self.rollup_id,
            reorg_max_depth: self.reorg_max_depth,
        }
    }

    /// Validate constraints that serde cannot express.
    ///
    /// # Errors
    ///
    /// Returns an error when the rollup id or reorg bound is empty.
    pub fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(self.rollup_id > 0, "l1.rollup_id must be >= 1");
        eyre::ensure!(self.reorg_max_depth > 0, "l1.reorg_max_depth must be >= 1");
        Ok(())
    }
}

const fn default_reorg_max_depth() -> usize {
    62
}

/// Wall-clock inputs that define the rollup's L1/L2 slot layout.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimingConfig {
    pub l1_block_time_ms: u32,
    pub l2_block_time_ms: u32,
    pub proof_time_ms: u32,
    #[serde(default = "default_submission_slack_ms")]
    pub submission_slack_ms: u32,
}

impl TimingConfig {
    /// Build and validate the runtime timing value.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured slot geometry is invalid.
    pub fn build(self) -> eyre::Result<eez_driver::RollupTiming> {
        let timing = eez_driver::RollupTiming::new(
            self.l1_block_time_ms,
            self.l2_block_time_ms,
            self.proof_time_ms,
            self.submission_slack_ms,
        );
        timing.validate()?;
        Ok(timing)
    }
}

const fn default_submission_slack_ms() -> u32 {
    100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_fields() {
        let err = toml::from_str::<TimingConfig>(
            "l1_block_time_ms=4000\nl2_block_time_ms=2000\nproof_time_ms=1000\ntyop=1",
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field `tyop`"));
    }

    #[test]
    fn defaults_only_nonessential_shared_values() {
        let l1: L1Config = toml::from_str(
            "rpc_url='http://127.0.0.1:8545'\nchain_id=31337\nregistry_address='0x0000000000000000000000000000000000000001'\nregistry_deploy_block=7\nrollup_id=1",
        )
        .unwrap();
        assert_eq!(l1.reorg_max_depth, 62);
        l1.validate().unwrap();

        let timing: TimingConfig =
            toml::from_str("l1_block_time_ms=4000\nl2_block_time_ms=2000\nproof_time_ms=1000")
                .unwrap();
        assert_eq!(timing.submission_slack_ms, 100);
        timing.build().unwrap();
    }
}
