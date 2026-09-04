//! Follower-only file configuration.

use eez_node_common::config::{L1Config, SecretKey, TimingConfig};
use serde::{Deserialize, Serialize};

/// Complete follower configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub l1: L1Config,
    pub timing: TimingConfig,
    pub l2_system_key: SecretKey,
    /// Optional low-latency unsafe head. Safe and finalized remain L1-derived.
    #[serde(default)]
    pub sequencer_rpc: Option<url::Url>,
}

impl Config {
    /// Validate the complete document before launching reth.
    ///
    /// # Errors
    ///
    /// Returns an error when any shared or follower-only value is invalid.
    pub fn validate(&self) -> eyre::Result<()> {
        self.l1.validate()?;
        self.timing.build()?;
        self.l2_system_key.signer()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_minimal_follower_and_rejects_extra_fields() {
        let raw = r#"
l2_system_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

[l1]
rpc_url = "http://127.0.0.1:8545"
chain_id = 10200
registry_address = "0x0000000000000000000000000000000000000001"
registry_deploy_block = 7
rollup_id = 1

[timing]
l1_block_time_ms = 4000
l2_block_time_ms = 2000
proof_time_ms = 1000
"#;
        let config: Config = toml::from_str(raw).unwrap();
        config.validate().unwrap();
        assert!(config.sequencer_rpc.is_none());

        let err = toml::from_str::<Config>(&format!("{raw}\nfuture_field = true"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field `future_field`"));
    }
}
