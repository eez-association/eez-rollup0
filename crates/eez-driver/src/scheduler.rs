//! Decides when the sequencer should propose the next block.
//!
//! Stage 1 contains the simplest possible scheduler: a wall-clock interval
//! that fires one [`ProposalRequest`] every `block_time`. The interval is
//! configurable so a deployment can pick its L2 block time (2s for Rollup-1
//! v1).
//!
//! Later stages will extend [`Scheduler`] to also accept external events
//! (safe-head updates from L1, prover-ready signals) without changing the
//! [`Sequencer`](crate::Sequencer)'s consuming interface — the contract is
//! "scheduler emits [`ProposalRequest`]; sequencer turns each into one
//! block."

use core::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

/// Classification of the slot a proposal is being built for.
///
/// Stage 1 only constructs [`SlotKind::Live`]. The variant exists so that
/// later stages (batch pre-building, cross-chain sync slots) can add their
/// own kinds and the [`Sequencer`](crate::Sequencer) loop's `match` stays
/// exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKind {
    /// Normal block at current wall-clock time.
    Live,
    // Will also include Sync, Future etc. here
}

impl fmt::Display for SlotKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
        }
    }
}

/// One request from the scheduler to build a single block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalRequest {
    /// What this block is for.
    pub kind: SlotKind,
    /// Wall-clock timestamp the resulting block should advertise, in unix
    /// seconds. Stage 1 sets this to "now".
    pub target_timestamp: u64,
}

/// Emits [`ProposalRequest`]s on a fixed interval.
#[derive(Debug)]
pub struct Scheduler {
    interval: Interval,
}

impl Scheduler {
    /// Creates a scheduler that fires every `block_time`.
    ///
    /// The first tick fires after `block_time` (not immediately) so the node
    /// has a moment to finish startup before the first proposal lands.
    ///
    /// Uses [`MissedTickBehavior::Delay`] — if a tick is missed because a
    /// prior `advance` ran long, the next tick fires `block_time` after the
    /// previous one completed rather than immediately catching up. This
    /// matches the cadence guarantee we want: blocks advertised at strict
    /// `block_time` spacing even under load.
    #[must_use]
    pub fn interval(block_time: Duration) -> Self {
        let start = Instant::now() + block_time;
        let mut interval = interval_at(start, block_time);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self { interval }
    }

    /// Waits for the next tick and returns the resulting [`ProposalRequest`].
    pub async fn next(&mut self) -> ProposalRequest {
        self.interval.tick().await;
        let target_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        ProposalRequest {
            kind: SlotKind::Live,
            target_timestamp,
        }
    }
}
