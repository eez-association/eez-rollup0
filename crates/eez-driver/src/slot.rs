//! Slot timing primitives: kinds, events, and the spawners that emit
//! them.
//!
//! [`SlotKind`] is the per-block label (Live / Future / Sync) attached
//! at production time for logging + dashboards.
//!
//! [`SlotEvent`] is the trigger event sent into the Sequencer's
//! `mpsc::Receiver<SlotEvent>`. Two variants:
//!
//! - [`SlotEvent::Live`] — fixed-interval tick, standalone-mode only.
//!   Produced by [`spawn_interval`].
//! - [`SlotEvent::SyncSlot`] — L1-anchored sync-slot trigger. Produced
//!   by [`spawn_l1_anchored`], which subscribes to an [`L1HeadSource`]
//!   and sleeps until `L1.timestamp + proof_window_open` after each
//!   head — i.e. the wall-clock moment when the prover should start
//!   so the postBatch bundle reaches the relay before the next L1 block.
//!
//! The Sequencer is source-agnostic — it just consumes `SlotEvent`s.
//! Tests inject fake schedules by sending events directly into the
//! channel.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};

use crate::timing::RollupTiming;

/// Classification of the slot a single block is being produced for.
///
/// Logged on every commit so dashboards can show the Live / Future /
/// Sync mix per slot. Distinct from [`SlotEvent`] — that's a *trigger*
/// (one per sync slot or one per interval tick), whereas `SlotKind`
/// labels each individual block produced inside a trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKind {
    /// Normal block at current wall-clock cadence.
    Live,
    /// Proof-window padding block — timestamp is in the future
    /// relative to wall-clock at production time; reth accepts as
    /// long as timestamps strictly increase.
    Future,
    /// Sync block — last block of a sync slot, where cross-chain
    /// system txs and the matching `ExecutionEntries` land. Timestamp
    /// matches the L1 block that will carry the corresponding
    /// `postBatch`.
    Sync,
}

impl core::fmt::Display for SlotKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
            Self::Future => f.write_str("future"),
            Self::Sync => f.write_str("sync"),
        }
    }
}

/// Trigger event from a spawner telling the Sequencer when to act.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotEvent {
    /// L1-anchored: produce the rest of a sync slot. Sequencer reads
    /// current head and
    /// [`RollupTiming::per_trigger_composition`](crate::RollupTiming::per_trigger_composition)
    /// to decide the Live / Future / Sync block count for this trigger.
    SyncSlot {
        /// L2 block height the Sync block will land at.
        block_height: u64,
        /// Wall-clock unix timestamp the Sync block will advertise.
        /// = `L1.timestamp_of_anchor + L1_block_time`.
        timestamp: u64,
    },
    /// Interval-mode tick — produce one Live block at this target
    /// wall-clock timestamp. Sequencer's greedy-backfill catches up
    /// if behind.
    Live {
        /// Wall-clock unix timestamp the block should advertise.
        target_timestamp: u64,
    },
}

/// Minimal L1 head info the L1-anchored spawner needs.
///
/// Defined here (in `eez-driver`) so the spawner stays L1-implementation-
/// agnostic. `eez-l1` provides an adapter
/// (`eez_l1::L1HeadStream`) that implements [`L1HeadSource`] over its
/// `broadcast::Receiver<L1Event>` — but a test or alternative
/// L1-client crate can implement [`L1HeadSource`] just as well.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct L1HeadInfo {
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub timestamp: u64,
}

/// Source of canonical L1 head events. Implemented by an adapter in
/// `eez-l1`; consumed by [`spawn_l1_anchored`].
///
/// `next_head` returns `None` when the source closes (broadcast lagged
/// past tolerance, `L1Watcher` task died, etc) — the spawner exits
/// cleanly so the surrounding `spawn_critical_task` notices.
#[async_trait]
pub trait L1HeadSource: Send + 'static {
    async fn next_head(&mut self) -> Option<L1HeadInfo>;
}

/// Spawn an interval ticker that emits [`SlotEvent::Live`] every
/// `block_time`. Returns the receiver side of the channel.
///
/// Used in standalone mode (no L1 stack). [`spawn_l1_anchored`] is
/// the L1-anchored production counterpart.
///
/// First tick fires after `block_time` (not immediately) so startup
/// has time to settle. [`MissedTickBehavior::Delay`] keeps cadence
/// under load.
#[must_use]
pub fn spawn_interval(block_time: Duration) -> mpsc::Receiver<SlotEvent> {
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let start = Instant::now() + block_time;
        let mut interval = interval_at(start, block_time);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let target_timestamp = now_unix();
            if tx.send(SlotEvent::Live { target_timestamp }).await.is_err() {
                break; // Sequencer dropped the receiver; exit cleanly.
            }
        }
    });
    rx
}

/// Spawn the L1-anchored scheduler. Returns the receiver side of the
/// channel.
///
/// On each L1 head from `source`:
///
/// 1. Sleep until wall-clock `L1.timestamp + proof_window_open` —
///    the moment the prover should start so its result is ready
///    before the next L1 block lands.
/// 2. Compute the deterministic sync-slot block height + timestamp
///    from `(l2_genesis_timestamp, L1.timestamp, L1_block_time,
///    L2_block_time)`. Same formula across all nodes given the same
///    genesis.
/// 3. Send [`SlotEvent::SyncSlot`].
///
/// Exits when the source closes or the receiver is dropped.
#[must_use]
pub fn spawn_l1_anchored<S>(
    mut source: S,
    timing: RollupTiming,
    l2_genesis_timestamp: u64,
) -> mpsc::Receiver<SlotEvent>
where
    S: L1HeadSource,
{
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        loop {
            let Some(head) = source.next_head().await else {
                event!(
                    name: "eez.slot.l1_anchored.source_closed",
                    Level::ERROR,
                    "L1 head source closed; L1-anchored scheduler exiting",
                );
                return;
            };

            // Sleep until proof_window_open after the L1 block timestamp.
            let trigger_at = head.timestamp + timing.proof_window_open().as_secs();
            let now = now_unix();
            let wait = trigger_at.saturating_sub(now);
            if wait > 0 {
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }

            let sync_slot_timestamp = head.timestamp + timing.l1_block_time().as_secs();
            let block_height = sync_slot_timestamp.saturating_sub(l2_genesis_timestamp)
                / timing.l2_block_time().as_secs();

            event!(
                name: "eez.slot.l1_anchored.trigger",
                Level::DEBUG,
                l1_head = head.block_number,
                l1_timestamp = head.timestamp,
                sync_slot_timestamp,
                block_height,
                "sync-slot trigger firing",
            );

            if tx
                .send(SlotEvent::SyncSlot {
                    block_height,
                    timestamp: sync_slot_timestamp,
                })
                .await
                .is_err()
            {
                return; // Sequencer dropped the receiver; exit cleanly.
            }
        }
    });
    rx
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
