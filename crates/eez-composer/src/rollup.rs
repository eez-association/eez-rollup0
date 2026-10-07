//! Per-rollup state held by the [`Composer`](crate::Composer) umbrella.
//!
//! [`RollupConfig`] is the immutable, env-derived knobs for one rollup
//! (its id and mode flag). [`RollupState`] adds the
//! runtime references the umbrella reads while building batches: the
//! local L2 provider, the L1-confirmed cursor.
//!
//! Single-rollup-in-`HashMap<RollupId, _>` from day one (S4.2 has one
//! entry; stage-N grows to N entries without restructuring).

use std::sync::Arc;

use alloy_primitives::Address;
use eez_l1::{L1CanonicalHead, L1Error, L1Result};

use crate::held_pool::HeldPool;
use crate::optimistic::OptimisticallyIncluded;

/// Immutable per-rollup configuration. Sourced from env at startup.
#[derive(Debug, Clone)]
pub struct RollupConfig {
    /// `rollupId` returned by `EEZ.registerRollup` for this L2.
    pub rollup_id: u64,
    /// Based-rollup mode flag. `true`: external batches log at INFO
    /// (anyone can post). `false`: external batches log at ERROR (this
    /// rollup is sequenced, no one else should be posting). Same code
    /// path either way; only log level differs.
    pub expect_external_batches: bool,
    /// Address receiving priority fees from locally composed L2 blocks.
    pub fee_recipient: Address,
}

impl RollupConfig {
    /// Read from `EEZ_*` env vars: `EEZ_ROLLUP_ID` and
    /// `EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES` (defaults to `false`), and
    /// `EEZ_L2_FEE_RECIPIENT` (defaults to the supplied genesis fee collector,
    /// or zero for a legacy chain with no collector).
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env(default_fee_recipient: Address) -> L1Result<Self> {
        use std::env;

        let rollup_id = env::var("EEZ_ROLLUP_ID")
            .map_err(|_| L1Error::Config("EEZ_ROLLUP_ID is required".into()))?
            .parse::<u64>()
            .map_err(|e| L1Error::Config(format!("EEZ_ROLLUP_ID: {e}")))?;
        let expect_external_batches = match env::var("EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES") {
            Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => true,
                "0" | "false" | "no" | "off" | "" => false,
                other => {
                    return Err(L1Error::Config(format!(
                        "EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES: expected boolean, got {other:?}"
                    )));
                }
            },
            Err(env::VarError::NotPresent) => false,
            Err(_) => {
                return Err(L1Error::Config(
                    "EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES contains non-UTF-8 bytes".into(),
                ));
            }
        };
        let fee_recipient = match env::var("EEZ_L2_FEE_RECIPIENT") {
            Ok(value) => value.trim().parse::<Address>().map_err(|e| {
                L1Error::Config(format!("EEZ_L2_FEE_RECIPIENT: expected address: {e}"))
            })?,
            Err(env::VarError::NotPresent) => default_fee_recipient,
            Err(_) => {
                return Err(L1Error::Config(
                    "EEZ_L2_FEE_RECIPIENT contains non-UTF-8 bytes".into(),
                ));
            }
        };

        Ok(Self {
            rollup_id,
            expect_external_batches,
            fee_recipient,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_recipient_configuration() {
        let default = Address::repeat_byte(0x11);
        let explicit = Address::repeat_byte(0x22);
        let cases = [
            (None, Some(default)),
            (
                Some("  0x2222222222222222222222222222222222222222  "),
                Some(explicit),
            ),
            (
                Some("0x0000000000000000000000000000000000000000"),
                Some(Address::ZERO),
            ),
            (Some(""), None),
            (Some("0x1234"), None),
        ];
        if let Ok(index) = std::env::var("EEZ_FEE_RECIPIENT_TEST_CASE") {
            let index = index.parse::<usize>().unwrap();
            let result = RollupConfig::from_env(default);
            if let Some(expected) = cases[index].1 {
                assert_eq!(result.unwrap().fee_recipient, expected);
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("EEZ_L2_FEE_RECIPIENT")
                );
            }
            return;
        }

        // Exercise the actual env reader in isolated child processes, without
        // changing the parent test process's environment under parallel tests.
        for (index, (value, _)) in cases.iter().enumerate() {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "rollup::tests::fee_recipient_configuration"])
                .env_clear()
                .env("EEZ_ROLLUP_ID", "1")
                .env("EEZ_FEE_RECIPIENT_TEST_CASE", index.to_string());
            if let Some(value) = value {
                command.env("EEZ_L2_FEE_RECIPIENT", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout)
                        .contains("test result: ok. 1 passed;"),
                "configuration case {index}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }
}

/// Runtime state for one rollup the umbrella manages.
///
/// `Composer<L2>` is generic over the L2 provider type; all entries in
/// the `HashMap<u64, RollupState<L2>>` share that type today
/// (single chainspec for all rollups). Stage-N multi-L2 with
/// heterogeneous chainspecs is a separate refactor.
#[derive(Debug)]
pub struct RollupState<L2> {
    pub config: RollupConfig,
    pub l2_provider: Arc<L2>,
    pub l1_head: Arc<L1CanonicalHead>,
    /// Per-rollup cross-chain held-tx pool, drained on each Sync-slot
    /// trigger by the umbrella's `compose_sync_slot`.
    pub held_pool: Arc<HeldPool>,
    /// Ledger of optimistically-committed Sync blocks whose L1 bundle
    /// is in flight or settled-but-unfinalized. Always present (empty
    /// map costs nothing); only the cross-chain compose path writes it.
    pub optimistic: Arc<OptimisticallyIncluded>,
}
