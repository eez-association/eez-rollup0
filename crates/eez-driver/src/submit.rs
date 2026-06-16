//! Sequencer → Submitter channel surface: the [`BatchCandidate`] handed
//! off when a batch window closes, and the [`BatchPolicy`] that decides
//! when to close one.
//!
//! Replaces the stage-2 polling model where the
//! [`Composer`](../../../eez-l1/src/composer.rs) ticked on a fixed
//! `EEZ_COMPOSER_INTERVAL_SECS` cadence and asked the L2 provider "is
//! there anything new?". From stage 4 onward the Sequencer drives
//! submission cadence by sending one [`BatchCandidate`] per closed
//! window; the Composer fires whenever one arrives.
//!
//! Per the §5.4.1 plan, this surface is multi-rollup-ready: in stage-N
//! each Sequencer owns its own `mpsc::Sender<BatchCandidate>` and the
//! Aggregator drains N receivers in a `select!`.

use std::time::Duration;

use tokio::sync::mpsc;

/// A closed batch window ready to be submitted: the Sequencer signals
/// "L2 has been advanced through `to_block`; you can submit now."
///
/// `to_block` is **authoritative for the upper bound** of the submitted
/// range — the Composer's batch covers `cursor + 1 ..= to_block`,
/// capped at `MAX_BLOCKS_PER_BATCH` only as a calldata-gas guardrail.
/// This shape is what stage-N's multi-rollup Submitter requires: the
/// proof's `ExecutionEntry[]` is pinned to a specific sync slot's
/// `to_block`, so the Composer cannot legitimately extend beyond it
/// without losing alignment with the cross-chain content.
///
/// Lower bound (`from_block = cursor + 1`) is intentionally **not** on
/// the schema: cursor is the only correct source for it (external
/// batches landing under our build would invalidate any Sequencer-side
/// `from_block`).
///
/// Catchup vs. steady-state regime (S4.2+):
/// - Steady-state: `to_block` is a sync slot boundary; the batch
///   carries cross-chain `ExecutionEntry[]` from the composer.
/// - Catchup: `to_block` is the last Live block produced this trigger;
///   partial extension is permitted by Rollup-1 §13.4.23.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchCandidate {
    /// Rollup the candidate is for. Plain `u64` to mirror
    /// `ComposerConfig::rollup_id`; promoted to a newtype in stage-N
    /// when `HashMap<RollupId, _>` routing wires up. `crosschain-protocol`
    /// already defines a `RollupId` newtype that the port to
    /// `eez-protocol` (S4.4) will lift here.
    pub rollup_id: u64,
    /// Inclusive upper bound of the L2-block range the Composer should
    /// submit. Authoritative — the Composer respects it exactly,
    /// modulo the calldata-gas cap.
    pub to_block: u64,
}

/// When should a Sequencer close a batch window and emit a
/// [`BatchCandidate`]?
///
/// Stage 4 only implements [`Self::EveryKBlocks`]. The remaining
/// variants are scaffolding so the Sequencer's `match` stays
/// exhaustive once stage-N (sync slots, wall-clock-driven posters)
/// adds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchPolicy {
    /// Emit every `k` committed L2 blocks. Block-number-based — survives
    /// catch-up bursts where many blocks land inside one scheduler tick.
    EveryKBlocks(u64),
    /// Emit on every Sync slot (Rollup-1 §1.3). Stage-N default once
    /// the Scheduler emits `SlotKind::Sync`.
    OnSyncSlot,
    /// Emit on a wall-clock cadence regardless of L2 block production.
    /// Useful for time-driven posters that don't care about sync slots.
    WallClockInterval(Duration),
}

/// Sequencer-internal emitter state: tracks the block count since the
/// last emission. Not part of the public surface — constructed by
/// [`Sequencer::with_batch_emitter`](crate::Sequencer::with_batch_emitter)
/// and consumed inside the produce-loop.
#[derive(Debug)]
pub(crate) struct BatchEmitter {
    pub(crate) rollup_id: u64,
    pub(crate) policy: BatchPolicy,
    pub(crate) tx: mpsc::Sender<BatchCandidate>,
    blocks_since_last: u64,
}

impl BatchEmitter {
    pub(crate) const fn new(
        rollup_id: u64,
        policy: BatchPolicy,
        tx: mpsc::Sender<BatchCandidate>,
    ) -> Self {
        Self {
            rollup_id,
            policy,
            tx,
            blocks_since_last: 0,
        }
    }

    /// Records a freshly-committed block. Returns the candidate to emit
    /// when the policy says the window is closed, else `None`.
    pub(crate) fn on_block_committed(&mut self, block_number: u64) -> Option<BatchCandidate> {
        self.blocks_since_last = self.blocks_since_last.saturating_add(1);
        let due = match self.policy {
            BatchPolicy::EveryKBlocks(k) => k > 0 && self.blocks_since_last >= k,
            // Stage-N variants — Scheduler doesn't emit Sync slots yet
            // and wall-clock emission isn't a per-block decision.
            BatchPolicy::OnSyncSlot | BatchPolicy::WallClockInterval(_) => false,
        };
        if !due {
            return None;
        }
        self.blocks_since_last = 0;
        Some(BatchCandidate {
            rollup_id: self.rollup_id,
            to_block: block_number,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn emitter(k: u64) -> (BatchEmitter, mpsc::Receiver<BatchCandidate>) {
        let (tx, rx) = mpsc::channel(8);
        (BatchEmitter::new(7, BatchPolicy::EveryKBlocks(k), tx), rx)
    }

    #[test]
    fn every_k_blocks_emits_on_kth_commit() {
        let (mut e, _rx) = emitter(3);
        assert!(e.on_block_committed(10).is_none());
        assert!(e.on_block_committed(11).is_none());
        let c = e.on_block_committed(12).expect("kth block emits");
        assert_eq!(c.to_block, 12);
        assert_eq!(c.rollup_id, 7);
    }

    #[test]
    fn subsequent_windows_advance_to_block() {
        let (mut e, _rx) = emitter(2);
        assert!(e.on_block_committed(100).is_none());
        let c1 = e.on_block_committed(101).expect("first window");
        assert_eq!(c1.to_block, 101);
        assert!(e.on_block_committed(102).is_none());
        let c2 = e.on_block_committed(103).expect("second window");
        assert_eq!(c2.to_block, 103);
    }

    #[test]
    fn stage_n_policies_dont_emit_yet() {
        let (tx, _rx) = mpsc::channel(1);
        let mut e = BatchEmitter::new(1, BatchPolicy::OnSyncSlot, tx);
        for n in 0..10 {
            assert!(e.on_block_committed(n).is_none());
        }
    }
}
