//! The L1-anchored spawner and the Sync-slot composer hook.
//!
//! [`SlotEvent`] (in `eez-timing`) is the trigger event sent into the
//! Sequencer's `mpsc::Receiver<SlotEvent>`. Two variants:
//!
//! - [`SlotEvent::SyncSlot`] — L1-anchored sync-slot trigger. Produced
//!   by [`spawn_l1_anchored`], which subscribes to an
//!   [`eez_l1::L1HeadStream`]
//!   and sleeps until `L1.timestamp + proof_window_open` after each
//!   head — i.e. the wall-clock moment when the prover should start
//!   so the postBatch bundle reaches the relay before the next L1 block.
//! - [`SlotEvent::LiveTick`] — wall-clock `l2_block_time` tick, also
//!   produced by [`spawn_l1_anchored`] between sync-slot triggers, so
//!   the RPC head advances one Live block per cadence instead of in
//!   per-anchor bursts. The Sequencer commits at most ONE Live block
//!   per tick, and only while the head is below the live region of the
//!   carried sync-slot height (the Future + Sync reserve belongs to
//!   the anchor trigger — ticks never outrun it).
//!
//! The Sequencer is source-agnostic — it just consumes `SlotEvent`s.
//! Tests inject fake schedules by sending events directly into the
//! channel.

use std::sync::Arc;

use alloy_consensus::Header;
use alloy_rpc_types_engine::ExecutionData;
use async_trait::async_trait;
use reth_primitives_traits::SealedHeader;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};

use eez_timing::{RollupTiming, SharedClock, SlotEvent, SlotScheduler};

/// Pre-built Sync block produced by a [`SyncSlotComposer`] — ready to
/// commit via [`BlockCommitterHandle::commit_derived`].
///
/// [`BlockCommitterHandle::commit_derived`]: crate::block_committer::BlockCommitterHandle::commit_derived
#[derive(Debug)]
pub struct SyncSlotBlock {
    /// Engine-API payload.
    pub payload: ExecutionData,
    /// Sealed header of the new block (head-mirror update on commit).
    pub header: SealedHeader<Header>,
}

/// Parent context the Sequencer supplies to a [`SyncSlotComposer`].
///
/// The full `SealedHeader<Header>` from the Sequencer's synchronous
/// `last_header` mirror — already reflects the just-committed parent,
/// and passing it whole avoids a `sealed_header_by_hash` re-lookup that
/// can race reth's provider-index propagation for the newest block.
#[derive(Debug, Clone)]
pub struct ParentContext {
    /// Sealed header of the parent block — full struct so the composer
    /// hands it to `evm_config.builder_for_next_block` directly.
    pub header: SealedHeader<Header>,
}

/// How a Sync slot is being produced. Gates cross-chain content: only a
/// caught-up [`Steady`](Self::Steady) block can settle its postBatch in the
/// same L1 slot (same timestamp), so [`Catchup`](Self::Catchup) blocks are
/// structural-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncSlotMode {
    /// Caught-up, aim-ahead slot — drains the cross-chain pool.
    Steady,
    /// Behind-the-wall-clock catchup — empty Sync block; cross-chain
    /// waits for the next [`Steady`](Self::Steady) slot.
    Catchup,
    /// Deterministic EMPTY block, no emission (deferred-late slots). Unlike
    /// the mempool-fed `commit_one` fallback it keeps grid heights
    /// batch-boundary-eligible.
    Empty,
}

/// Per-Sync-slot block producer for cross-chain content.
///
/// Called by the Sequencer before each Sync block. The production impl
/// (`eez-composer::Composer`) drains its `HeldPool`, resolves each held
/// tx into system txs, builds the Sync block in-process, and returns it.
/// `None` = no content → the Sequencer does a pool-driven Sync commit.
/// `mode` gates the drain (see [`SyncSlotMode`]). Lives here, not in
/// `eez-composer`, to avoid a dependency cycle.
#[async_trait]
pub trait SyncSlotComposer: Send + Sync + 'static {
    /// Compose a Sync slot. `target_l1_block`: `Some(n)` supplies the exact L1
    /// target for cap-triggered historical chunks; steady mode also uses it
    /// for the regular postBatch and pins its timestamp. `None` leaves that
    /// historical emission aimed at the next available block. `mode` gates the
    /// drain: `Catchup` blocks are empty (cross-chain waits for the next
    /// `Steady` slot), and their regular minimal postBatch remains
    /// next-available.
    async fn compose_sync_slot(
        &self,
        rollup_id: u64,
        parent: ParentContext,
        timestamp: u64,
        target_l1_block: Option<u64>,
        mode: SyncSlotMode,
    ) -> Option<SyncSlotBlock>;

    /// Run pending failed-bundle recovery before this tick's commit. May move
    /// the head (reorg + substitution), so callers re-read it. Default: no-op.
    async fn recover_failed(&self, rollup_id: u64) {
        let _ = rollup_id;
    }
}

/// Cheap clone-able handle for a [`SyncSlotComposer`].
pub type SyncSlotComposerHandle = Arc<dyn SyncSlotComposer>;

/// Spawn the L1-anchored scheduler. Returns the receiver side of the
/// channel.
///
/// A tokio shell over [`SlotScheduler`], which holds every decision. On
/// each L1 head from `source`:
///
/// 1. Compute the deterministic sync-slot block height + timestamp
///    from `(l2_genesis_timestamp, L1.timestamp, L1_block_time,
///    L2_block_time)`. Same formula across all nodes given the same
///    genesis.
/// 2. Arm a trigger for wall-clock `L1.timestamp + proof_window_open`
///    — the moment the prover should start so its result is ready
///    before the next L1 block lands.
/// 3. When the trigger instant passes, send [`SlotEvent::SyncSlot`].
///
/// Between anchor triggers an internal `l2_block_time` ticker emits
/// [`SlotEvent::LiveTick`] carrying the latest anchor-computed sync
/// height, so the Sequencer advances one Live block per cadence rather
/// than in per-anchor bursts. Ticks are suppressed until the first L1
/// head (so they can't outrun the anchor) and sent with `try_send` — a
/// full channel just means the Sequencer is busy and the next tick
/// re-nudges, which beats blocking the armed trigger behind queued ticks.
///
/// Every wall-clock read goes through `clock`.
///
/// Exits when the source closes or the receiver is dropped.
#[must_use]
pub fn spawn_l1_anchored(
    mut source: eez_l1::L1HeadStream,
    timing: RollupTiming,
    l2_genesis_timestamp: u64,
    clock: SharedClock,
) -> mpsc::Receiver<SlotEvent> {
    let (tx, rx) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut scheduler = SlotScheduler::new(timing, l2_genesis_timestamp);
        let mut ticker = interval_at(
            Instant::now() + timing.l2_block_time(),
            timing.l2_block_time(),
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            // Remaining wait for the armed trigger, recomputed from the
            // clock each iteration so re-creating the sleep when another
            // select branch wins doesn't stretch the deadline. The wait is
            // aligned to the trigger's millisecond: a whole-second sleep
            // wakes ~0.8s late and eats the send-lead before the slot.
            // `None` while disarmed — the branch is disabled.
            let trigger_wait = scheduler.wait(clock.now_unix_ms());

            tokio::select! {
                maybe_head = source.next_head() => {
                    let Some(head) = maybe_head else {
                        event!(
                            name: "eez.slot.l1_anchored.source_closed",
                            Level::ERROR,
                            "L1 head source closed; L1-anchored scheduler exiting",
                        );
                        return;
                    };

                    let stale = scheduler.on_l1_head(head.block_number, head.timestamp);
                    if let (Some(stale), Some(armed)) = (stale, scheduler.armed()) {
                        // Only reachable when heads arrive faster than
                        // proof_window_open apart — a watcher catch-up
                        // burst. The newest anchor wins; the Sequencer's
                        // Catchup covers the skipped slot's span.
                        event!(
                            name: "eez.slot.l1_anchored.trigger_superseded",
                            Level::WARN,
                            stale_block_height = stale.block_height,
                            block_height = armed.block_height,
                            l1_head = head.block_number,
                            "L1 head arrived before armed sync-slot trigger fired; superseding with newest anchor",
                        );
                    }
                }
                () = tokio::time::sleep(trigger_wait.unwrap_or_default()), if trigger_wait.is_some() => {
                    let fired = scheduler
                        .fire()
                        .expect("branch enabled only while a trigger is armed");
                    event!(
                        name: "eez.slot.l1_anchored.trigger",
                        Level::DEBUG,
                        l1_head = fired.l1_head,
                        l1_timestamp = fired.l1_timestamp,
                        sync_slot_timestamp = fired.timestamp,
                        block_height = fired.block_height,
                        "sync-slot trigger firing",
                    );
                    if tx.send(fired.event()).await.is_err() {
                        return; // Sequencer dropped the receiver; exit cleanly.
                    }
                }
                _ = ticker.tick() => {
                    let Some(tick @ SlotEvent::LiveTick { sync_slot_block_height }) =
                        scheduler.live_tick()
                    else {
                        continue; // No anchor yet; ticks must not outrun it.
                    };
                    match tx.try_send(tick) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            // Sequencer is mid-burst; the next tick
                            // re-nudges, nothing is lost.
                            event!(
                                name: "eez.slot.l1_anchored.live_tick_skipped",
                                Level::DEBUG,
                                sync_slot_block_height,
                                "schedule channel full; skipping live tick",
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            return; // Sequencer dropped the receiver; exit cleanly.
                        }
                    }
                }
            }
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use eez_l1::{L1Event, L1HeadStream};
    use eez_timing::{Clock, TokioClock};
    use tokio::sync::broadcast;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn anchored_shell_fires_sync_slot_at_trigger() {
        // 12 s L1, 2 s L2, proof 4000, slack 1500: the trigger opens 6 s after the head.
        let timing = RollupTiming::new(12_000, 2_000, 4_000, 1_500);
        let genesis_ts = 1_700_000_000;
        let head_ts = genesis_ts + 120;
        // Start half a second after the head, so no Live tick lands on the trigger.
        let start_ms = head_ts * 1_000 + 500;
        let clock = Arc::new(TokioClock::starting_at(start_ms));

        let (l1_tx, l1_rx) = broadcast::channel(4);
        let mut rx = spawn_l1_anchored(
            L1HeadStream::from_receiver(l1_rx),
            timing,
            genesis_ts,
            clock.clone(),
        );
        l1_tx
            .send(L1Event::NewHead {
                block_number: 9,
                block_hash: B256::ZERO,
                timestamp: head_ts,
            })
            .unwrap();

        let mut seen = Vec::new();
        for _ in 0..5 {
            let event = rx.recv().await.unwrap();
            seen.push((clock.now_unix_ms(), event));
        }

        let height = (head_ts + 12 - genesis_ts) / 2;
        let tick = SlotEvent::LiveTick {
            sync_slot_block_height: height,
        };
        let sync = SlotEvent::SyncSlot {
            block_height: height,
            timestamp: head_ts + 12,
            l1_head: 9,
        };
        assert_eq!(
            seen,
            vec![
                (start_ms + 2_000, tick),
                (start_ms + 4_000, tick),
                (head_ts * 1_000 + 6_000, sync),
                (start_ms + 6_000, tick),
                (start_ms + 8_000, tick),
            ]
        );
    }
}
