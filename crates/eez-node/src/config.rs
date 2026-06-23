use std::{env, path::PathBuf};

use eyre::{Context as _, Result, bail};
use serde::Deserialize;

use crate::Mode;

const EEZ_ENV_VARS: &[&str] = &[
    "EEZ_L1_RPC_URL",
    "EEZ_L1_BUILDER_RPC_URL",
    "EEZ_L1_TARGET_RPC_URL",
    "EEZ_L1_POSTER_KEY",
    "EEZ_L1_CHAIN_ID",
    "EEZ_L1_POSTBATCH_PRIORITY_FEE",
    "EEZ_L1_REORG_MAX_DEPTH_BLOCKS",
    "EEZ_L1_EMBEDDED",
    "EEZ_L1_CHAIN",
    "EEZ_L1_HTTP_PORT",
    "EEZ_L1_AUTH_PORT",
    "EEZ_L1_P2P_PORT",
    "EEZ_L1_DATADIR",
    "EEZ_L1_CHAIN_PATH",
    "EEZ_L1_JWT_SECRET",
    "EEZ_L1_ROLLUP_ID",
    "EEZ_ROLLUP_ID",
    "EEZ_REGISTRY_ADDRESS",
    "EEZ_REGISTRY_DEPLOY_BLOCK",
    "EEZ_ROLLUP_MANAGER_ADDRESS",
    "EEZ_MOCK_PROOF_SYSTEM_ADDRESS",
    "EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES",
    "EEZ_L1_BLOCK_TIME_MS",
    "EEZ_L2_BLOCK_TIME_MS",
    "EEZ_PROOF_TIME_MS",
    "EEZ_SUBMISSION_SLACK_MS",
    "EEZ_PROOF_SIGNER_KEY",
    "EEZ_L2_SYSTEM_KEY",
    "EEZ_CCM_L2_ADDRESS",
    "EEZ_L2_SYSTEM_ADDRESS",
    "EEZ_SEQUENCER_RPC",
    "EEZ_CROSS_CHAIN_PROXY_ADDRESSES",
    "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS",
    "EEZ_MAX_SPECULATIVE_DEPTH",
    "EEZ_COMPOSER_INTERVAL_SECS",
    "EEZ_SEQUENCER_DISABLED",
    "EEZ_COMPOSER_DISABLED",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EezConfig {
    mode: ConfigMode,
    #[serde(default)]
    l1: L1Config,
    #[serde(default)]
    rollup: RollupConfig,
    #[serde(default)]
    timing: TimingConfig,
    #[serde(default)]
    follower: FollowerConfig,
    #[serde(default)]
    system_tx: SystemTxConfig,
    #[serde(default)]
    keys: KeysConfig,
    #[serde(default)]
    ingress: IngressConfig,
    #[serde(default)]
    sequencer: SequencerConfig,
}

impl EezConfig {
    pub(crate) fn load(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read EEZ config {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse EEZ config {}", path.display()))
    }

    pub(crate) const fn mode(&self) -> Mode {
        match self.mode {
            ConfigMode::Standalone => Mode::Standalone,
            ConfigMode::Follower => Mode::Follower,
            ConfigMode::Composer => Mode::Composer,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self.mode() {
            Mode::Standalone => {
                self.reject(
                    self.follower.sequencer_rpc.is_some(),
                    "standalone mode cannot set follower.sequencer_rpc",
                )?;
                self.reject(
                    self.keys.proof_signer_key.is_some(),
                    "standalone mode cannot set keys.proof_signer_key",
                )?;
            }
            Mode::Follower => {
                self.reject(
                    self.keys.proof_signer_key.is_some(),
                    "follower mode cannot set keys.proof_signer_key",
                )?;
                self.require(self.l1.rpc_url.as_ref(), "l1.rpc_url")?;
                self.require(
                    self.rollup.registry_address.as_ref(),
                    "rollup.registry_address",
                )?;
                self.require(
                    self.rollup.registry_deploy_block.as_ref(),
                    "rollup.registry_deploy_block",
                )?;
                self.require(self.rollup.id.as_ref(), "rollup.id")?;
                self.validate_l1_timing()?;
            }
            Mode::Composer => {
                self.reject(
                    self.follower.sequencer_rpc.is_some(),
                    "composer mode cannot set follower.sequencer_rpc",
                )?;
                self.require(self.l1.rpc_url.as_ref(), "l1.rpc_url")?;
                self.require(self.l1.builder_rpc_url.as_ref(), "l1.builder_rpc_url")?;
                self.require(self.keys.l1_poster_key.as_ref(), "keys.l1_poster_key")?;
                self.require(self.keys.proof_signer_key.as_ref(), "keys.proof_signer_key")?;
                self.require(
                    self.rollup.registry_address.as_ref(),
                    "rollup.registry_address",
                )?;
                self.require(
                    self.rollup.registry_deploy_block.as_ref(),
                    "rollup.registry_deploy_block",
                )?;
                self.require(self.rollup.id.as_ref(), "rollup.id")?;
                self.require(
                    self.rollup.mock_proof_system_address.as_ref(),
                    "rollup.mock_proof_system_address",
                )?;
                self.validate_l1_timing()?;
            }
        }

        if self.keys.l2_system_key.is_some() || self.system_tx.ccm_l2_address.is_some() {
            self.require(self.keys.l2_system_key.as_ref(), "keys.l2_system_key")?;
            self.require(
                self.system_tx.ccm_l2_address.as_ref(),
                "system_tx.ccm_l2_address",
            )?;
            self.require(self.rollup.id.as_ref(), "rollup.id")?;
        }

        Ok(())
    }

    pub(crate) fn apply_env(&self) {
        clear_env();

        set_if_some("EEZ_L1_RPC_URL", self.l1.rpc_url.as_deref());
        set_if_some("EEZ_L1_BUILDER_RPC_URL", self.l1.builder_rpc_url.as_deref());
        set_if_some("EEZ_L1_TARGET_RPC_URL", self.l1.target_rpc_url.as_deref());
        set_if_some("EEZ_L1_POSTER_KEY", self.keys.l1_poster_key.as_deref());
        set_display("EEZ_L1_CHAIN_ID", self.l1.chain_id);
        set_display(
            "EEZ_L1_POSTBATCH_PRIORITY_FEE",
            self.l1.postbatch_priority_fee,
        );
        set_display(
            "EEZ_L1_REORG_MAX_DEPTH_BLOCKS",
            self.l1.reorg_max_depth_blocks,
        );
        set_bool_10("EEZ_L1_EMBEDDED", self.l1.embedded);
        set_if_some("EEZ_L1_CHAIN", self.l1.chain.as_deref());
        set_display("EEZ_L1_HTTP_PORT", self.l1.http_port);
        set_display("EEZ_L1_AUTH_PORT", self.l1.auth_port);
        set_display("EEZ_L1_P2P_PORT", self.l1.p2p_port);
        set_if_some("EEZ_L1_DATADIR", self.l1.datadir.as_deref());
        set_if_some("EEZ_L1_CHAIN_PATH", self.l1.chain_path.as_deref());
        set_if_some("EEZ_L1_JWT_SECRET", self.l1.jwt_secret.as_deref());
        set_display("EEZ_L1_ROLLUP_ID", self.rollup.l1_rollup_id);

        set_display("EEZ_ROLLUP_ID", self.rollup.id);
        set_if_some(
            "EEZ_REGISTRY_ADDRESS",
            self.rollup.registry_address.as_deref(),
        );
        set_display(
            "EEZ_REGISTRY_DEPLOY_BLOCK",
            self.rollup.registry_deploy_block,
        );
        set_if_some(
            "EEZ_ROLLUP_MANAGER_ADDRESS",
            self.rollup.rollup_manager_address.as_deref(),
        );
        set_if_some(
            "EEZ_MOCK_PROOF_SYSTEM_ADDRESS",
            self.rollup.mock_proof_system_address.as_deref(),
        );
        set_display(
            "EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES",
            self.rollup.expect_external_batches,
        );

        set_display("EEZ_L1_BLOCK_TIME_MS", self.timing.l1_block_time_ms);
        set_display("EEZ_L2_BLOCK_TIME_MS", self.timing.l2_block_time_ms);
        set_display("EEZ_PROOF_TIME_MS", self.timing.proof_time_ms);
        set_display("EEZ_SUBMISSION_SLACK_MS", self.timing.submission_slack_ms);

        set_if_some(
            "EEZ_PROOF_SIGNER_KEY",
            self.keys.proof_signer_key.as_deref(),
        );
        set_if_some("EEZ_L2_SYSTEM_KEY", self.keys.l2_system_key.as_deref());
        set_if_some(
            "EEZ_CCM_L2_ADDRESS",
            self.system_tx.ccm_l2_address.as_deref(),
        );
        set_if_some(
            "EEZ_L2_SYSTEM_ADDRESS",
            self.system_tx.l2_system_address.as_deref(),
        );
        set_if_some("EEZ_SEQUENCER_RPC", self.follower.sequencer_rpc.as_deref());

        set_joined(
            "EEZ_CROSS_CHAIN_PROXY_ADDRESSES",
            self.ingress.cross_chain_proxy_addresses.as_deref(),
        );
        set_joined_u64(
            "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS",
            self.ingress.cross_chain_source_chain_ids.as_deref(),
        );
        set_display(
            "EEZ_MAX_SPECULATIVE_DEPTH",
            self.sequencer.max_speculative_depth,
        );
    }

    fn validate_l1_timing(&self) -> Result<()> {
        self.require(
            self.timing.l1_block_time_ms.as_ref(),
            "timing.l1_block_time_ms",
        )?;
        self.require(
            self.timing.l2_block_time_ms.as_ref(),
            "timing.l2_block_time_ms",
        )?;
        self.require(self.timing.proof_time_ms.as_ref(), "timing.proof_time_ms")?;
        Ok(())
    }

    fn require<T>(&self, value: Option<&T>, name: &str) -> Result<()> {
        if value.is_none() {
            bail!("{name} is required for {} mode", self.mode().name());
        }
        Ok(())
    }

    fn reject(&self, invalid: bool, message: &str) -> Result<()> {
        if invalid {
            bail!("{message}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ConfigMode {
    Standalone,
    Follower,
    #[serde(alias = "sequencer")]
    Composer,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct L1Config {
    rpc_url: Option<String>,
    builder_rpc_url: Option<String>,
    target_rpc_url: Option<String>,
    chain_id: Option<u64>,
    postbatch_priority_fee: Option<u128>,
    reorg_max_depth_blocks: Option<u64>,
    embedded: Option<bool>,
    chain: Option<String>,
    http_port: Option<u16>,
    auth_port: Option<u16>,
    p2p_port: Option<u16>,
    datadir: Option<String>,
    chain_path: Option<String>,
    jwt_secret: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RollupConfig {
    id: Option<u64>,
    registry_address: Option<String>,
    registry_deploy_block: Option<u64>,
    rollup_manager_address: Option<String>,
    mock_proof_system_address: Option<String>,
    expect_external_batches: Option<bool>,
    l1_rollup_id: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimingConfig {
    l1_block_time_ms: Option<u32>,
    l2_block_time_ms: Option<u32>,
    proof_time_ms: Option<u32>,
    submission_slack_ms: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FollowerConfig {
    sequencer_rpc: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemTxConfig {
    ccm_l2_address: Option<String>,
    l2_system_address: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct KeysConfig {
    l1_poster_key: Option<String>,
    proof_signer_key: Option<String>,
    l2_system_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressConfig {
    cross_chain_proxy_addresses: Option<Vec<String>>,
    cross_chain_source_chain_ids: Option<Vec<u64>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SequencerConfig {
    max_speculative_depth: Option<u64>,
}

pub(crate) fn find_config_path(argv: &[String]) -> Option<PathBuf> {
    let mut args = argv.iter();
    while let Some(arg) = args.next() {
        if let Some(value) = arg
            .strip_prefix("--eez.config=")
            .or_else(|| arg.strip_prefix("--eez-config="))
        {
            return Some(PathBuf::from(value));
        }
        if arg == "--eez.config" || arg == "--eez-config" {
            return args.next().map(PathBuf::from);
        }
    }

    env::var_os("EEZ_CONFIG").map(PathBuf::from)
}

fn clear_env() {
    for name in EEZ_ENV_VARS {
        // SAFETY: called during single-threaded startup before reth launches.
        unsafe {
            env::remove_var(name);
        }
    }
}

fn set_if_some(name: &str, value: Option<&str>) {
    if let Some(value) = value {
        set_env(name, value);
    }
}

fn set_display<T: std::fmt::Display>(name: &str, value: Option<T>) {
    if let Some(value) = value {
        set_env(name, value.to_string());
    }
}

fn set_bool_10(name: &str, value: Option<bool>) {
    if let Some(value) = value {
        set_env(name, if value { "1" } else { "0" });
    }
}

fn set_joined(name: &str, value: Option<&[String]>) {
    if let Some(value) = value {
        set_env(name, value.join(","));
    }
}

fn set_joined_u64(name: &str, value: Option<&[u64]>) {
    if let Some(value) = value {
        set_env(
            name,
            value
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );
    }
}

fn set_env(name: &str, value: impl AsRef<std::ffi::OsStr>) {
    // SAFETY: called during single-threaded startup before reth launches.
    unsafe {
        env::set_var(name, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follower_rejects_proof_signer_key() {
        let cfg: EezConfig = toml::from_str(
            r#"
mode = "follower"

[l1]
rpc_url = "http://127.0.0.1:8545"

[rollup]
id = 1
registry_address = "0x0000000000000000000000000000000000000001"
registry_deploy_block = 1

[timing]
l1_block_time_ms = 2000
l2_block_time_ms = 1000
proof_time_ms = 500

[keys]
proof_signer_key = "0xabc"
"#,
        )
        .unwrap();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("follower mode cannot set keys.proof_signer_key"));
    }

    #[test]
    fn composer_rejects_follower_sequencer_rpc() {
        let cfg: EezConfig = toml::from_str(
            r#"
mode = "composer"

[l1]
rpc_url = "http://127.0.0.1:8545"
builder_rpc_url = "http://127.0.0.1:8645"

[rollup]
id = 1
registry_address = "0x0000000000000000000000000000000000000001"
registry_deploy_block = 1
mock_proof_system_address = "0x0000000000000000000000000000000000000002"

[timing]
l1_block_time_ms = 2000
l2_block_time_ms = 1000
proof_time_ms = 500

[keys]
l1_poster_key = "0xabc"
proof_signer_key = "0xdef"

[follower]
sequencer_rpc = "http://127.0.0.1:8745"
"#,
        )
        .unwrap();

        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("composer mode cannot set follower.sequencer_rpc"));
    }

    #[test]
    fn rejects_non_env_system_tx_fields() {
        let parsed = toml::from_str::<EezConfig>(
            r#"
mode = "follower"

[system_tx]
gas_price = 1000000000
"#,
        );

        assert!(parsed.is_err());
    }
}
