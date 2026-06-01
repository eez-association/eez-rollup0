//! Schedule event types + standalone-mode interval spawner.
//!
//! Sequencer consumes [`ScheduleEvent`]s from an `mpsc::Receiver`,
//! agnostic to source. The source is one of:
//!
//! - **Interval** ([`spawn_interval`], this module): fixed wall-clock
//!   cadence emitting [`ScheduleEvent::LiveTick`]. No L1 awareness;
//!   used in standalone-mode dev workflows.
//!
//! - **L1-anchored** (`eez-composer::schedule::spawn_l1_anchored`):
//!   subscribes to `L1Event::NewHead`, sleeps `proof_window_open`
//!   after each L1 block lands, emits [`ScheduleEvent::SyncSlotTrigger`]
//!   with the deterministic Sync block height + timestamp. Production
//!   path.
//!
//! Decoupling via channel: Sequencer holds a `Receiver<ScheduleEvent>`,
//! source-side is whatever spawned that channel's sender. Tests inject
//! fake schedulers by sending events directly.

use core::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

/// Classification of the slot a single block is being produced for.
///
/// Logged on every commit so dashboards can show the Live / Future /
/// Sync mix per slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKind {
    /// Normal block at current wall-clock cadence.
    Live,
    /// Proof-window padding block — timestamp is in the future
    /// relative to wall-clock at production time; reth accepts as
    /// long as timestamps strictly increase.
    Future,
    /// Sync block — last block of a slot, where cross-chain system
    /// txs and the matching `ExecutionEntries` land. Timestamp matches
    /// the L1 block that will carry the corresponding `postBatch`.
    Sync,
}

impl fmt::Display for SlotKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
            Self::Future => f.write_str("future"),
            Self::Sync => f.write_str("sync"),
        }
    }
}

/// Event from the Scheduler telling the Sequencer when to act.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScheduleEvent {
    /// L1-anchored: produce the rest of a sync slot. Sequencer reads
    /// current head and
    /// [`RollupTiming::per_trigger_composition`](crate::RollupTiming::per_trigger_composition)
    /// to decide the Live / Future / Sync block count for this trigger.
    SyncSlotTrigger {
        /// L2 block height the Sync block will land at.
        sync_slot_block_height: u64,
        /// Wall-clock unix timestamp the Sync block will advertise.
        /// = `L1.timestamp_of_anchor + L1_block_time`.
        sync_slot_timestamp: u64,
    },
    /// Interval-mode tick — produce one Live block at this target
    /// wall-clock timestamp. Sequencer's existing greedy-backfill
    /// path handles it (catches up if behind).
    LiveTick {
        /// Wall-clock unix timestamp the block should advertise.
        target_timestamp: u64,
    },
}

/// Spawn an interval ticker that emits [`ScheduleEvent::LiveTick`]
/// every `block_time`. Returns the receiver side of the channel.
///
/// The spawned task lives until the receiver is dropped. First tick
/// fires after `block_time` (not immediately) to leave room for
/// startup. [`MissedTickBehavior::Delay`] keeps cadence under load.
///
/// Used in standalone mode (no L1 stack). L1-anchored mode uses
/// `eez_composer::schedule::spawn_l1_anchored` instead.
#[must_use]
pub fn spawn_interval(block_time: Duration) -> mpsc::Receiver<ScheduleEvent> {
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let start = Instant::now() + block_time;
        let mut interval = interval_at(start, block_time);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let target_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if tx
                .send(ScheduleEvent::LiveTick { target_timestamp })
                .await
                .is_err()
            {
                break; // Sequencer dropped the receiver; exit cleanly.
            }
        }
    });
    rx
}
