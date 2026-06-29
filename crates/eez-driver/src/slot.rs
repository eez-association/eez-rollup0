//! Slot timing primitives: kinds, events, and the spawners that emit
//! them.
//!
//! [`SlotKind`] is the per-block label (Live / Future / Sync) attached
//! at production time for logging + dashboards.
//!
//! [`SlotEvent`] is the trigger event sent into the Sequencer's
//! `mpsc::Receiver<SlotEvent>`. Three variants:
//!
//! - [`SlotEvent::Live`] — fixed-interval tick, standalone-mode only.
//!   Produced by [`spawn_interval`].
//! - [`SlotEvent::SyncSlot`] — L1-anchored sync-slot trigger. Produced
//!   by [`spawn_l1_anchored`], which subscribes to an [`L1HeadSource`]
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_consensus::Header;
use alloy_rpc_types_engine::ExecutionData;
use async_trait::async_trait;
use reth_primitives_traits::SealedHeader;
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
        /// L1 block this slot is anchored to; the postBatch targets
        /// `l1_head + 1` in steady state.
        l1_head: u64,
    },
    /// Interval-mode tick — produce one Live block at this target
    /// wall-clock timestamp. Sequencer's greedy-backfill catches up
    /// if behind.
    Live {
        /// Wall-clock unix timestamp the block should advertise.
        target_timestamp: u64,
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
        /// plus its own [`RollupTiming`].
        sync_slot_block_height: u64,
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
///
/// `next_head` must be cancel-safe: [`spawn_l1_anchored`] polls it
/// inside a `tokio::select!` alongside its Live ticker, so the future
/// is dropped and re-created whenever another branch wins. The
/// `eez-l1` adapter satisfies this via `broadcast::Receiver::recv`
/// (cancel-safe; no event is consumed by a cancelled call).
#[async_trait]
pub trait L1HeadSource: Send + 'static {
    async fn next_head(&mut self) -> Option<L1HeadInfo>;
}

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
    /// Compose a Sync slot. `target_l1_block`: `Some(n)` aims the postBatch
    /// at exactly L1 block `n` (steady state, `l1_head + 1`); `None` aims the
    /// next available block (catch-up). `mode` gates the drain: `Catchup`
    /// blocks are empty (cross-chain waits for the next `Steady` slot).
    async fn compose_sync_slot(
        &self,
        rollup_id: u64,
        parent: ParentContext,
        timestamp: u64,
        target_l1_block: Option<u64>,
        mode: SyncSlotMode,
    ) -> Option<SyncSlotBlock>;
}

/// No-op [`SyncSlotComposer`] — used when no umbrella is wired
/// (standalone-mode dev) or when the per-rollup `HeldPool` is absent.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoCrossChainContent;

#[async_trait]
impl SyncSlotComposer for NoCrossChainContent {
    async fn compose_sync_slot(
        &self,
        _rollup_id: u64,
        _parent: ParentContext,
        _timestamp: u64,
        _target_l1_block: Option<u64>,
        _mode: SyncSlotMode,
    ) -> Option<SyncSlotBlock> {
        None
    }
}

/// Cheap clone-able handle for a [`SyncSlotComposer`].
pub type SyncSlotComposerHandle = Arc<dyn SyncSlotComposer>;

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

/// Sync-slot trigger armed by an L1 head, waiting for its
/// proof-window-open instant.
#[derive(Debug, Clone, Copy)]
struct PendingSyncSlot {
    /// Wall-clock unix second at which to fire
    /// (`L1.timestamp + proof_window_open`).
    trigger_at: u64,
    /// L2 block height the Sync block will land at.
    block_height: u64,
    /// Wall-clock unix timestamp the Sync block will advertise.
    timestamp: u64,
    /// L1 block of the anchoring head; carried into [`SlotEvent::SyncSlot`].
    l1_head: u64,
    /// L1 block timestamp of the anchoring head (logging only).
    l1_timestamp: u64,
}

/// Spawn the L1-anchored scheduler. Returns the receiver side of the
/// channel.
///
/// On each L1 head from `source`:
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
        let mut ticker = interval_at(
            Instant::now() + timing.l2_block_time(),
            timing.l2_block_time(),
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Latest anchor-computed sync-slot height. None until the
        // first L1 head; LiveTicks are suppressed until then.
        let mut latest_sync_height: Option<u64> = None;
        // Armed sync-slot trigger. None while idle between fire and
        // the next L1 head.
        let mut pending: Option<PendingSyncSlot> = None;

        loop {
            // Remaining wait for the armed trigger, recomputed from unix
            // time each iteration so re-creating the sleep when another
            // select branch wins doesn't stretch the deadline. ZERO when
            // disarmed — the branch is disabled, the future never polled.
            let trigger_wait = pending.map_or(Duration::ZERO, |p| {
                // Align the wake to the `trigger_at` instant in ms. A
                // whole-second sleep wakes N seconds after the loop's
                // fractional phase, firing the trigger ~0.8s late and
                // eating the send-lead before the slot.
                let trigger_at_ms = p.trigger_at.saturating_mul(1_000);
                Duration::from_millis(trigger_at_ms.saturating_sub(now_unix_millis()))
            });

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

                    let trigger_at = head.timestamp + timing.proof_window_open().as_secs();
                    let sync_slot_timestamp = head.timestamp + timing.l1_block_time().as_secs();
                    let block_height = sync_slot_timestamp.saturating_sub(l2_genesis_timestamp)
                        / timing.l2_block_time().as_secs();

                    if let Some(stale) = pending {
                        // Only reachable when heads arrive faster than
                        // proof_window_open apart — a watcher catch-up
                        // burst. The newest anchor wins; the Sequencer's
                        // Catchup covers the skipped slot's span.
                        event!(
                            name: "eez.slot.l1_anchored.trigger_superseded",
                            Level::WARN,
                            stale_block_height = stale.block_height,
                            block_height,
                            l1_head = head.block_number,
                            "L1 head arrived before armed sync-slot trigger fired; superseding with newest anchor",
                        );
                    }
                    latest_sync_height = Some(block_height);
                    pending = Some(PendingSyncSlot {
                        trigger_at,
                        block_height,
                        timestamp: sync_slot_timestamp,
                        l1_head: head.block_number,
                        l1_timestamp: head.timestamp,
                    });
                }
                () = tokio::time::sleep(trigger_wait), if pending.is_some() => {
                    let fired = pending
                        .take()
                        .expect("branch enabled only while pending.is_some()");
                    event!(
                        name: "eez.slot.l1_anchored.trigger",
                        Level::DEBUG,
                        l1_head = fired.l1_head,
                        l1_timestamp = fired.l1_timestamp,
                        sync_slot_timestamp = fired.timestamp,
                        block_height = fired.block_height,
                        "sync-slot trigger firing",
                    );
                    if tx
                        .send(SlotEvent::SyncSlot {
                            block_height: fired.block_height,
                            timestamp: fired.timestamp,
                            l1_head: fired.l1_head,
                        })
                        .await
                        .is_err()
                    {
                        return; // Sequencer dropped the receiver; exit cleanly.
                    }
                }
                _ = ticker.tick() => {
                    let Some(sync_slot_block_height) = latest_sync_height else {
                        continue; // No anchor yet; ticks must not outrun it.
                    };
                    match tx.try_send(SlotEvent::LiveTick { sync_slot_block_height }) {
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

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Wall-clock unix time in milliseconds — the defer-on-lateness check
/// needs sub-second precision that [`now_unix`] lacks.
pub(crate) fn now_unix_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
