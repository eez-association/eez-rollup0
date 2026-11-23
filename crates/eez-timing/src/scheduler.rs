//! Slot kinds, trigger events, and the sans-IO core of the L1-anchored
//! scheduler.
//!
//! [`SlotKind`] is the per-block label (Live / Future / Sync) attached
//! at production time for logging + dashboards.
//!
//! [`SlotEvent`] is the trigger event sent into the Sequencer's
//! `mpsc::Receiver<SlotEvent>`.
//!
//! [`SlotScheduler`] decides when each event is due. It does no I/O and
//! reads no clock: the caller feeds it L1 heads and the current time, and
//! owns the sleeping and sending (`eez_driver::spawn_l1_anchored`).

use std::time::Duration;

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
        /// L1 block this slot is anchored to; the postBatch targets
        /// `l1_head + 1` in steady state.
        l1_head: u64,
    },
    /// L1-anchored wall-clock tick — commit exactly one Live block,
    /// but only while the head is below the live region of the next
    /// sync slot (`sync_slot_block_height - future_count - 1`). The
    /// Future + Sync reserve is produced by the [`Self::SyncSlot`]
    /// trigger; the Sequencer no-ops ticks once the live region is
    /// full so ticks cannot outrun the anchor.
    LiveTick {
        /// Latest anchor-computed L2 height the Sync block will land
        /// at. The Sequencer derives the live-region bound from this
        /// plus its own [`RollupTiming`](crate::RollupTiming).
        sync_slot_block_height: u64,
    },
}

/// A Sync-slot trigger armed by an L1 head, waiting for its
/// proof-window-open instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SyncSlotTrigger {
    /// Unix millisecond at which to fire:
    /// `(L1.timestamp + proof_window_open)·1000`.
    pub trigger_at_ms: u64,
    /// L2 block height the Sync block will land at.
    pub block_height: u64,
    /// Unix timestamp the Sync block will advertise.
    pub timestamp: u64,
    /// L1 block of the anchoring head; carried into [`SlotEvent::SyncSlot`].
    pub l1_head: u64,
    /// L1 block timestamp of the anchoring head (logging only).
    pub l1_timestamp: u64,
}

impl SyncSlotTrigger {
    /// The [`SlotEvent::SyncSlot`] this trigger sends when it fires.
    #[must_use]
    pub const fn event(&self) -> SlotEvent {
        SlotEvent::SyncSlot {
            block_height: self.block_height,
            timestamp: self.timestamp,
            l1_head: self.l1_head,
        }
    }
}

/// Sans-IO core of the L1-anchored scheduler.
///
/// Each L1 head arms one Sync-slot trigger at `L1.timestamp +
/// proof_window_open`, for the Sync block at `L1.timestamp + L1_block_time`.
/// A newer head supersedes a trigger that has not fired. Live ticks carry
/// the latest anchor's Sync height and are withheld until the first head.
#[derive(Debug, Clone)]
pub struct SlotScheduler {
    timing: RollupTiming,
    l2_genesis_timestamp: u64,
    /// Latest anchor-computed Sync height; `None` until the first head.
    latest_sync_height: Option<u64>,
    /// Armed trigger; `None` between a fire and the next head.
    pending: Option<SyncSlotTrigger>,
}

impl SlotScheduler {
    /// A scheduler with no head seen and nothing armed.
    #[must_use]
    pub const fn new(timing: RollupTiming, l2_genesis_timestamp: u64) -> Self {
        Self {
            timing,
            l2_genesis_timestamp,
            latest_sync_height: None,
            pending: None,
        }
    }

    /// Arm the trigger for the L1 head `number` at unix `timestamp`.
    /// Returns the armed trigger it supersedes, if one had not fired.
    ///
    /// The Sync height is `(timestamp + L1_block_time − genesis) / L2_block_time`,
    /// the same on every node with the same genesis.
    pub fn on_l1_head(&mut self, number: u64, timestamp: u64) -> Option<SyncSlotTrigger> {
        let trigger_at_ms = timestamp
            .saturating_add(self.timing.proof_window_open().as_secs())
            .saturating_mul(1_000);
        let sync_slot_timestamp = timestamp.saturating_add(self.timing.l1_block_time().as_secs());
        let block_height = sync_slot_timestamp.saturating_sub(self.l2_genesis_timestamp)
            / self.timing.l2_block_time().as_secs();
        self.latest_sync_height = Some(block_height);
        self.pending.replace(SyncSlotTrigger {
            trigger_at_ms,
            block_height,
            timestamp: sync_slot_timestamp,
            l1_head: number,
            l1_timestamp: timestamp,
        })
    }

    /// The armed trigger, if any.
    #[must_use]
    pub const fn pending(&self) -> Option<SyncSlotTrigger> {
        self.pending
    }

    /// Time left until the armed trigger is due at unix `now_ms`; zero once
    /// it is due, `None` when nothing is armed.
    #[must_use]
    pub fn wait(&self, now_ms: u64) -> Option<Duration> {
        self.pending
            .map(|p| Duration::from_millis(p.trigger_at_ms.saturating_sub(now_ms)))
    }

    /// Disarm and return the armed trigger. The caller sends its
    /// [`SyncSlotTrigger::event`].
    pub const fn fire(&mut self) -> Option<SyncSlotTrigger> {
        self.pending.take()
    }

    /// The Live tick to send now, or `None` before the first head.
    #[must_use]
    pub fn live_tick(&self) -> Option<SlotEvent> {
        self.latest_sync_height
            .map(|sync_slot_block_height| SlotEvent::LiveTick {
                sync_slot_block_height,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 12 s L1, 2 s L2, proof 4000, slack 1500: the trigger opens 6 s after the head.
    fn mainnet() -> RollupTiming {
        RollupTiming::new(12_000, 2_000, 4_000, 1_500)
    }

    const GENESIS: u64 = 1_700_000_000;

    #[test]
    fn scheduler_triggers_at_proof_window_open() {
        let mut s = SlotScheduler::new(mainnet(), GENESIS);
        let head_ts = GENESIS + 120;
        assert_eq!(s.on_l1_head(42, head_ts), None);

        let armed = s.pending().unwrap();
        assert_eq!(armed.trigger_at_ms, (head_ts + 6) * 1_000);
        assert_eq!(armed.timestamp, head_ts + 12);
        assert_eq!(armed.block_height, 66);
        assert_eq!((armed.l1_head, armed.l1_timestamp), (42, head_ts));

        assert_eq!(s.wait(head_ts * 1_000), Some(Duration::from_secs(6)));
        assert_eq!(
            s.wait(armed.trigger_at_ms - 1),
            Some(Duration::from_millis(1))
        );
        assert_eq!(s.wait(armed.trigger_at_ms), Some(Duration::ZERO));
        assert_eq!(s.wait(armed.trigger_at_ms + 5_000), Some(Duration::ZERO));
    }

    #[test]
    fn newer_head_supersedes_pending_trigger() {
        let mut s = SlotScheduler::new(mainnet(), GENESIS);
        s.on_l1_head(1, GENESIS);
        let first = s.pending().unwrap();

        let stale = s.on_l1_head(2, GENESIS + 12);
        assert_eq!(stale, Some(first));
        let second = s.pending().unwrap();
        assert_eq!(second.l1_head, 2);
        assert_eq!(second.block_height, first.block_height + 6);
        assert_eq!(
            s.live_tick(),
            Some(SlotEvent::LiveTick {
                sync_slot_block_height: second.block_height
            })
        );
    }

    #[test]
    fn live_ticks_wait_for_first_head() {
        let mut s = SlotScheduler::new(mainnet(), GENESIS);
        assert_eq!(s.live_tick(), None);
        assert_eq!(s.wait(0), None);
        assert_eq!(s.fire(), None);

        s.on_l1_head(7, GENESIS);
        assert_eq!(
            s.live_tick(),
            Some(SlotEvent::LiveTick {
                sync_slot_block_height: 6
            })
        );
    }

    #[test]
    fn fire_clears_the_pending_trigger() {
        let mut s = SlotScheduler::new(mainnet(), GENESIS);
        s.on_l1_head(7, GENESIS);
        let fired = s.fire().unwrap();
        assert_eq!(
            fired.event(),
            SlotEvent::SyncSlot {
                block_height: 6,
                timestamp: GENESIS + 12,
                l1_head: 7,
            }
        );
        assert_eq!(s.pending(), None);
        assert_eq!(s.wait(0), None);
        assert_eq!(s.fire(), None);
        // Live ticks keep the fired anchor's height until the next head.
        assert_eq!(
            s.live_tick(),
            Some(SlotEvent::LiveTick {
                sync_slot_block_height: 6
            })
        );
    }

    #[test]
    fn scheduler_matches_the_closed_form() {
        let timings = [
            mainnet(),
            RollupTiming::new(5_000, 1_000, 200, 1_700),
            RollupTiming::new(4_000, 2_000, 1_500, 100),
            RollupTiming::new(12_000, 2_000, 200, 1_500),
        ];
        // xorshift64: a fixed seed keeps the sample reproducible.
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        for i in 0..1_000_u64 {
            let timing = timings[(i % 4) as usize];
            let genesis = next(2_000_000_000);
            // Mostly after genesis, sometimes before it (the height saturates at 0).
            let head_ts = (genesis + next(100_000_000)).saturating_sub(next(50));

            // The closed-form expressions.
            let trigger_at = head_ts + timing.proof_window_open().as_secs();
            let sync_slot_timestamp = head_ts + timing.l1_block_time().as_secs();
            let block_height =
                sync_slot_timestamp.saturating_sub(genesis) / timing.l2_block_time().as_secs();

            let mut s = SlotScheduler::new(timing, genesis);
            s.on_l1_head(i, head_ts);
            let armed = s.pending().unwrap();
            assert_eq!(armed.trigger_at_ms, trigger_at.saturating_mul(1_000));
            assert_eq!(armed.timestamp, sync_slot_timestamp);
            assert_eq!(armed.block_height, block_height);
        }
    }
}
