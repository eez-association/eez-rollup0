//! Per-rollup state held by the [`Composer`](crate::Composer) umbrella.
//!
//! [`RollupConfig`] is the immutable, env-derived knobs for one rollup
//! (its id, proof system address, mode flag). [`RollupState`] adds the
//! runtime references the umbrella reads while building batches: the
//! local L2 provider, the L1-confirmed cursor, the per-rollup
//! [`RollupTiming`](eez_driver::RollupTiming).
//!
//! Single-rollup-in-`HashMap<RollupId, _>` from day one (S4.2 has one
//! entry; stage-N grows to N entries without restructuring).

use std::sync::Arc;

use alloy_primitives::Address;
use eez_driver::RollupTiming;
use eez_l1::{L1CanonicalHead, L1Error, L1Result};

use crate::held_pool::HeldPool;

/// Immutable per-rollup configuration. Sourced from env at startup.
#[derive(Debug, Clone)]
pub struct RollupConfig {
    /// `rollupId` returned by `EEZ.registerRollup` for this L2.
    pub rollup_id: u64,
    /// Deployed proof-system address — the `IProofSystem` instance this
    /// rollup attests with.
    pub proof_system: Address,
    /// L1 block where `EEZ` was deployed. Lower bound for the startup
    /// `BatchPosted` log scan that seeds the L1-confirmed cursor.
    ///
    /// Semantically a global value (one EEZ contract serves all
    /// rollups). Held per-rollup for now to keep this S4.2 move
    /// minimal; refactor to a top-level field when multi-rollup
    /// startup orchestration lands.
    pub deploy_block: u64,
    /// Based-rollup mode flag. `true`: external batches log at INFO
    /// (anyone can post). `false`: external batches log at ERROR (this
    /// rollup is sequenced, no one else should be posting). Same code
    /// path either way; only log level differs.
    pub expect_external_batches: bool,
}

impl RollupConfig {
    /// Read from `EEZ_*` env vars: `EEZ_ROLLUP_ID`,
    /// `EEZ_MOCK_PROOF_SYSTEM_ADDRESS`, `EEZ_REGISTRY_DEPLOY_BLOCK`,
    /// `EEZ_COMPOSER_EXPECT_EXTERNAL_BATCHES` (defaults to `false`).
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env() -> L1Result<Self> {
        use std::{env, str::FromStr};

        let rollup_id = env::var("EEZ_ROLLUP_ID")
            .map_err(|_| L1Error::Config("EEZ_ROLLUP_ID is required".into()))?
            .parse::<u64>()
            .map_err(|e| L1Error::Config(format!("EEZ_ROLLUP_ID: {e}")))?;
        let proof_system =
            Address::from_str(&env::var("EEZ_MOCK_PROOF_SYSTEM_ADDRESS").map_err(|_| {
                L1Error::Config("EEZ_MOCK_PROOF_SYSTEM_ADDRESS is required".into())
            })?)
            .map_err(|e| L1Error::Config(format!("EEZ_MOCK_PROOF_SYSTEM_ADDRESS: {e}")))?;
        let deploy_block = env::var("EEZ_REGISTRY_DEPLOY_BLOCK")
            .map_err(|_| L1Error::Config("EEZ_REGISTRY_DEPLOY_BLOCK is required".into()))?
            .parse::<u64>()
            .map_err(|e| L1Error::Config(format!("EEZ_REGISTRY_DEPLOY_BLOCK: {e}")))?;
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

        Ok(Self {
            rollup_id,
            proof_system,
            deploy_block,
            expect_external_batches,
        })
    }
}

/// Runtime state for one rollup the umbrella manages.
///
/// `Composer<L2>` is generic over the L2 provider type; all entries in
/// the `HashMap<RollupId, RollupState<L2>>` share that type today
/// (single chainspec for all rollups). Stage-N multi-L2 with
/// heterogeneous chainspecs is a separate refactor.
#[derive(Debug)]
pub struct RollupState<L2> {
    pub config: RollupConfig,
    pub timing: RollupTiming,
    pub l2_provider: Arc<L2>,
    pub l1_head: Arc<L1CanonicalHead>,
    /// Per-rollup cross-chain held-tx pool. `None` for rollups that
    /// don't participate in cross-chain composition (entry-only
    /// deployments without cross-chain content, or pure-follower
    /// roles). When present, drained on each Sync-slot trigger by
    /// the umbrella's `compose_sync_slot`.
    pub held_pool: Option<Arc<HeldPool>>,
}
