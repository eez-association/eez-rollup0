//! Slot kinds and the trigger events the scheduler sends the Sequencer.
//!
//! [`SlotKind`] is the per-block label (Live / Future / Sync) attached
//! at production time for logging + dashboards.
//!
//! [`SlotEvent`] is the trigger event sent into the Sequencer's
//! `mpsc::Receiver<SlotEvent>`.

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
