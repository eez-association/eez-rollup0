//! Per-rollup state held by the [`Composer`](crate::Composer) umbrella.
//!
//! [`RollupConfig`] is the immutable startup configuration for one rollup
//! (its id and mode flag). [`RollupState`] adds the
//! runtime references the umbrella reads while building batches: the
//! local L2 provider, the L1-confirmed cursor.
//!
//! Single-rollup-in-`HashMap<RollupId, _>` from day one (S4.2 has one
//! entry; stage-N grows to N entries without restructuring).

use std::sync::Arc;

use eez_l1::L1CanonicalHead;

use crate::held_pool::HeldPool;
use crate::optimistic::OptimisticallyIncluded;

/// Immutable per-rollup configuration.
#[derive(Debug, Clone)]
pub struct RollupConfig {
    /// `rollupId` returned by `EEZ.registerRollup` for this L2.
    pub rollup_id: u64,
    /// Based-rollup mode flag. `true`: external batches log at INFO
    /// (anyone can post). `false`: external batches log at ERROR (this
    /// rollup is sequenced, no one else should be posting). Same code
    /// path either way; only log level differs.
    pub expect_external_batches: bool,
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
