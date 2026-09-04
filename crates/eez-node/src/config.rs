//! Composer-only file configuration.

use std::path::PathBuf;

use alloy_primitives::Address;
use eez_composer::ComposerLimits;
use eez_driver::DEFAULT_MAX_SPECULATIVE_DEPTH;
use eez_node_common::config::{L1Config, SecretKey, TimingConfig};
use serde::{Deserialize, Serialize};

use crate::l1_embedded::{EmbeddedL1Config, L1ChainKind};

/// Complete Composer configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub l1: L1Config,
    pub timing: TimingConfig,
    pub prover: ProverConfig,
    pub submission: SubmissionConfig,
    pub cross_chain: CrossChainConfig,
    pub embedded_l1: EmbeddedL1Settings,
    pub l2_system_key: SecretKey,
    #[serde(default)]
    pub expect_external_batches: bool,
    #[serde(default = "default_max_speculative_depth")]
    pub max_speculative_depth: u64,
    #[serde(default)]
    pub limits: LimitsConfig,
}

impl Config {
    /// Validate the complete document before either embedded node starts.
    ///
    /// # Errors
    ///
    /// Returns an error when any shared or Composer-only value is invalid.
    pub fn validate(&self) -> eyre::Result<()> {
        self.l1.validate()?;
        self.timing.build()?;
        self.l2_system_key.signer()?;
        self.submission.poster_key.signer()?;
        self.limits.validate()?;
        self.embedded_l1.build()?;
        Ok(())
    }
}

const fn default_max_speculative_depth() -> u64 {
    DEFAULT_MAX_SPECULATIVE_DEPTH
}

/// Remote prover endpoint and the signer whose result it must return.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProverConfig {
    pub url: url::Url,
    pub attester_address: Address,
}

/// L1 batch transaction submission.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionConfig {
    pub builder_rpc_url: url::Url,
    #[serde(default)]
    pub target_rpc_url: Option<url::Url>,
    pub poster_key: SecretKey,
    pub proof_system_address: Address,
    #[serde(default = "default_priority_fee")]
    pub priority_fee: u128,
}

const fn default_priority_fee() -> u128 {
    10_000_000_000
}

/// Ports for the two source-chain JSON-RPC fronts.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrossChainConfig {
    pub l1_port: u16,
    pub l2_port: u16,
}

/// Operational Composer limits. Defaults match the protocol implementation.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_blocks_per_batch: u64,
    pub max_postbatch_gas: u64,
    pub max_user_txs_per_bundle: usize,
}

impl LimitsConfig {
    fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(
            self.max_blocks_per_batch > 0,
            "limits.max_blocks_per_batch must be >= 1"
        );
        eyre::ensure!(
            self.max_postbatch_gas > 0,
            "limits.max_postbatch_gas must be >= 1"
        );
        eyre::ensure!(
            self.max_user_txs_per_bundle > 0,
            "limits.max_user_txs_per_bundle must be >= 1"
        );
        Ok(())
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        let limits = ComposerLimits::default();
        Self {
            max_blocks_per_batch: limits.max_blocks_per_batch,
            max_postbatch_gas: limits.max_postbatch_gas,
            max_user_txs_per_bundle: limits.max_user_txs_per_bundle,
        }
    }
}

impl From<LimitsConfig> for ComposerLimits {
    fn from(value: LimitsConfig) -> Self {
        Self {
            max_blocks_per_batch: value.max_blocks_per_batch,
            max_postbatch_gas: value.max_postbatch_gas,
            max_user_txs_per_bundle: value.max_user_txs_per_bundle,
        }
    }
}

/// Embedded L1 node settings. Ports default to the existing single-node values.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedL1Settings {
    #[serde(default)]
    pub kind: EmbeddedL1Kind,
    #[serde(default = "default_chain")]
    pub chain: String,
    pub datadir: PathBuf,
    #[serde(default = "default_l1_http_port")]
    pub http_port: u16,
    #[serde(default)]
    pub auth_port: Option<u16>,
    #[serde(default = "default_l1_p2p_port")]
    pub p2p_port: u16,
    #[serde(default)]
    pub discv5_port: Option<u16>,
    #[serde(default)]
    pub jwt_secret: Option<PathBuf>,
    #[serde(default)]
    pub trusted_peers: Vec<String>,
}

impl EmbeddedL1Settings {
    /// Resolve strings and dependent port defaults into reth's native config.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid chain spec, peer, or derived port.
    pub fn build(&self) -> eyre::Result<EmbeddedL1Config> {
        use reth_cli::chainspec::ChainSpecParser as _;

        let dev_chain_spec =
            reth_ethereum_cli::chainspec::EthereumChainSpecParser::parse(&self.chain)
                .map_err(|err| eyre::eyre!("embedded_l1.chain={:?}: {err}", self.chain))?;
        let auth_port = match self.auth_port {
            Some(port) => port,
            None => self.http_port.checked_add(6).ok_or_else(|| {
                eyre::eyre!("embedded_l1.http_port too high for default auth port")
            })?,
        };
        let discv5_port = match self.discv5_port {
            Some(port) => port,
            None => self.p2p_port.checked_add(10).ok_or_else(|| {
                eyre::eyre!("embedded_l1.p2p_port too high for default discv5 port")
            })?,
        };
        let trusted_peers = self
            .trusted_peers
            .iter()
            .map(|peer| peer.parse())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(EmbeddedL1Config {
            dev_chain_spec,
            kind: self.kind.into(),
            datadir: self.datadir.clone(),
            http_port: self.http_port,
            auth_port,
            p2p_port: self.p2p_port,
            discv5_port,
            jwtsecret: self.jwt_secret.clone(),
            trusted_peers,
        })
    }
}

/// Execution-layer flavor hosted by the Composer process.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EmbeddedL1Kind {
    /// Gnosis Chiado execution node driven by an external consensus client.
    Chiado,
    /// Private Ethereum execution node driven by an external consensus client.
    Devnet,
    /// Auto-mining Ethereum development node.
    #[default]
    Testing,
}

impl From<EmbeddedL1Kind> for L1ChainKind {
    fn from(value: EmbeddedL1Kind) -> Self {
        match value {
            EmbeddedL1Kind::Chiado => Self::Chiado,
            EmbeddedL1Kind::Devnet => Self::Devnet,
            EmbeddedL1Kind::Testing => Self::Testing,
        }
    }
}

fn default_chain() -> String {
    "dev".to_owned()
}

const fn default_l1_http_port() -> u16 {
    18_545
}

const fn default_l1_p2p_port() -> u16 {
    30_444
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
l2_system_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

[l1]
rpc_url = "http://127.0.0.1:8545"
chain_id = 31337
registry_address = "0x0000000000000000000000000000000000000001"
registry_deploy_block = 7
rollup_id = 1

[timing]
l1_block_time_ms = 4000
l2_block_time_ms = 2000
proof_time_ms = 1000

[prover]
url = "http://127.0.0.1:50061"
attester_address = "0x0000000000000000000000000000000000000002"

[submission]
builder_rpc_url = "http://127.0.0.1:8645"
poster_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
proof_system_address = "0x0000000000000000000000000000000000000003"

[cross_chain]
l1_port = 18999
l2_port = 18998

[embedded_l1]
datadir = "/tmp/eez-test-l1"
"#;

    #[test]
    fn minimal_config_uses_only_runtime_defaults() {
        let config: Config = toml::from_str(MINIMAL).unwrap();
        config.validate().unwrap();
        assert_eq!(config.max_speculative_depth, DEFAULT_MAX_SPECULATIVE_DEPTH);
        assert_eq!(config.limits.max_user_txs_per_bundle, 50);
        assert_eq!(config.embedded_l1.kind, EmbeddedL1Kind::Testing);
    }

    #[test]
    fn rejects_future_fields() {
        let err = toml::from_str::<Config>(&format!("{MINIMAL}\nfuture_field = true"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field `future_field`"));
    }
}
