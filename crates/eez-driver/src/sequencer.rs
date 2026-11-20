//! Engine-API consumer that advances the chain on a 2s cadence aligned to
//! wall-clock time, with **greedy backfill** of any timestamp slots that
//! were missed due to a reorg or downtime.
//!
//! [`Sequencer::run`] is the long-running loop. Each iteration handles one
//! of:
//!
//! 1. A scheduler tick. `Sequencer::advance` runs in a loop, producing
//!    blocks at deterministic `parent.timestamp() + BLOCK_TIME_SECS`
//!    slots until the chain catches up to the tick's wall-clock target.
//!    Most ticks produce a single block (the chain is on cadence); after
//!    a reorg or a stall, the same tick may produce several filler
//!    blocks to backfill the slots that passed during the rewind.
//! 2. A slower FCU-refresh timer, used to keep reth's engine view alive
//!    during quiet periods.
//!
//! The Sequencer does **not** keep a local mirror of the canonical head.
//! Every iteration of `advance` reads the current head from the
//! [`BlockCommitterHandle::last_header`] — the actor is the single source
//! of truth, updated synchronously inside every Sequence + Derive command.
//! This is what fixes the race where the Deriver-driven canonical change
//! was visible to the actor immediately, but the Sequencer's own mirror
//! lagged behind reth's `CanonStateNotification` broadcast — producing
//! "invalid payload attributes" errors when the Sequencer issued FCU+attrs
//! based on a stale parent.

use core::fmt;
use std::{sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_payload_primitives::{BuiltPayload, PayloadTypes};
use reth_primitives_traits::{
    AlloyBlockHeader, HeaderTy, NodePrimitives, SealedHeader, SealedHeaderFor,
};
use tracing::{Level, event};

use tokio::sync::mpsc;

use crate::block_committer::BlockCommitterHandle;
use crate::error::{DriverError, DriverResult};
use crate::slot::{SyncSlotComposerHandle, SyncSlotMode};
use eez_timing::deadline::misses_l1_slot;
use eez_timing::{RollupTiming, SharedClock, SlotComposition, SlotEvent, SlotKind};

/// How often the sequencer re-publishes the current forkchoice state.
///
/// Even when no new block is being proposed, sending an FCU keeps reth's
/// engine convinced the chain is alive. Mirrors the cadence reth's own dev
/// miner uses.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Max speculative gap the Sequencer can run ahead of L1-confirmed
/// cursor. Pauses beyond this so reth's state-retention
/// window keeps recent ancestors alive for the Deriver's replay.
pub const DEFAULT_MAX_SPECULATIVE_DEPTH: u64 = 64;

/// Log a benign stale-parent skip: the Deriver advanced the canonical
/// head between our snapshot and the engine FCU, so this commit
/// targeted a no-longer-canonical parent. The caller bails (its
/// backfill loop or the whole trigger) and the next trigger rebuilds
/// from the fresh head. `phase` names the commit site for attribution.
fn log_stale_parent(phase: &str) {
    event!(
        name: "eez.sequencer.stale_parent",
        Level::DEBUG,
        phase,
        "deriver advanced the chain under this commit; bailing — next trigger rebuilds from fresh head",
    );
}

/// Builds [`EthPayloadAttributes`] for the next block.
///
/// Stage 1 produces minimal valid attributes: a strictly-increasing timestamp
/// honoring the caller's target, a placeholder `prev_randao`, a
/// caller-configured `suggested_fee_recipient`, and the post-Shanghai /
/// post-Cancun fields filled in based on the chainspec.
///
/// Stage 3 will replace the placeholder `prev_randao` with an L1-derived
/// value per the protocol spec (§13.14). Stage 4 will add a hook for
/// composer-supplied system transactions.
#[derive(Debug, Clone)]
pub struct EthAttributesBuilder<ChainSpec> {
    chain_spec: Arc<ChainSpec>,
    suggested_fee_recipient: Address,
}

impl<ChainSpec> EthAttributesBuilder<ChainSpec> {
    /// Creates a builder using the given chainspec and a zero fee recipient.
    #[must_use]
    pub const fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            chain_spec,
            suggested_fee_recipient: Address::ZERO,
        }
    }

    /// Configures the `suggested_fee_recipient` field on built attributes.
    #[must_use]
    pub const fn with_fee_recipient(mut self, recipient: Address) -> Self {
        self.suggested_fee_recipient = recipient;
        self
    }
}

impl<ChainSpec> EthAttributesBuilder<ChainSpec>
where
    ChainSpec: EthChainSpec + EthereumHardforks + Send + Sync + 'static,
{
    /// Builds attributes for the block following `parent` with timestamp
    /// `target_timestamp` (clamped upward to preserve strict monotonicity).
    #[must_use]
    pub fn build(
        &self,
        parent: &SealedHeader<ChainSpec::Header>,
        target_timestamp: u64,
    ) -> EthPayloadAttributes {
        let timestamp = core::cmp::max(parent.timestamp().saturating_add(1), target_timestamp);
        EthPayloadAttributes {
            timestamp,
            // TODO(stage 3): derive from L1 per spec §13.14. Zeroed for
            // stage 1 — no security depends on prev_randao yet.
            prev_randao: B256::ZERO,
            suggested_fee_recipient: self.suggested_fee_recipient,
            withdrawals: self
                .chain_spec
                .is_shanghai_active_at_timestamp(timestamp)
                .then(Default::default),
            parent_beacon_block_root: self
                .chain_spec
                .is_cancun_active_at_timestamp(timestamp)
                .then_some(B256::ZERO),
            // Amsterdam-fork additions; not active for stage-1 dev chains.
            slot_number: None,
            target_gas_limit: None,
        }
    }
}

/// Translates scheduler proposals into engine-API `forkchoiceUpdated`
/// then `newPayload` via the [`BlockCommitter`](crate::BlockCommitterHandle).
/// Stateless w.r.t. head — reads `committer.last_header()` (the actor
/// writes synchronously on every Sequence/Derive).
pub struct Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    attributes: EthAttributesBuilder<ChainSpec>,
    schedule_rx: mpsc::Receiver<SlotEvent>,
    committer: BlockCommitterHandle<T>,
    /// Per-rollup timing (L1/L2 cadence, proof window, slack). Used to
    /// compute per-trigger Live/Future/Sync block composition and per-
    /// block timestamps.
    timing: RollupTiming,
    /// Wall clock for the defer-on-lateness check.
    clock: SharedClock,
    rollup_id: u64,
    composer: SyncSlotComposerHandle,
    max_speculative_depth: u64,
    l1_head: Arc<eez_l1::L1CanonicalHead>,
    /// Consecutive Sync blocks skipped for off-grid drift. A skip self-heals
    /// next trigger; a run of them is a stall and must be loud (invariant 7).
    off_grid_skips: u32,
}

impl<T, ChainSpec> fmt::Debug for Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("committer", &self.committer)
            .field("rollup_id", &self.rollup_id)
            .field("max_speculative_depth", &self.max_speculative_depth)
            .finish_non_exhaustive()
    }
}

impl<T, ChainSpec> Sequencer<T, ChainSpec>
where
    T: PayloadTypes<
            PayloadAttributes = EthPayloadAttributes,
            ExecutionData = alloy_rpc_types_engine::ExecutionData,
        > + Send
        + Sync
        + 'static,
    <T::BuiltPayload as BuiltPayload>::Primitives:
        NodePrimitives<BlockHeader = alloy_consensus::Header> + Send + Sync + 'static,
    SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Send,
    ChainSpec: EthChainSpec<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>
        + EthereumHardforks
        + Send
        + Sync
        + 'static,
{
    /// Construct an L1-anchored composer sequencer. All composer-only
    /// dependencies are mandatory and share the binary-owned committer with
    /// the Composer and Deriver.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn composer(
        attributes: EthAttributesBuilder<ChainSpec>,
        committer: BlockCommitterHandle<T>,
        schedule_rx: mpsc::Receiver<SlotEvent>,
        timing: RollupTiming,
        clock: SharedClock,
        rollup_id: u64,
        composer: SyncSlotComposerHandle,
        max_speculative_depth: u64,
        l1_head: Arc<eez_l1::L1CanonicalHead>,
    ) -> Self {
        Self {
            attributes,
            schedule_rx,
            committer,
            timing,
            clock,
            rollup_id,
            composer,
            max_speculative_depth,
            l1_head,
            off_grid_skips: 0,
        }
    }

    /// Runs the sequencer loop until cancellation. Errors during advance
    /// are logged and the loop continues; channel-closed = Scheduler
    /// task exited, Sequencer exits cleanly.
    pub async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                maybe_event = self.schedule_rx.recv() => {
                    let Some(event) = maybe_event else {
                        event!(
                            name: "eez.sequencer.schedule_rx.closed",
                            Level::ERROR,
                            "schedule channel closed; sequencer task exiting",
                        );
                        break;
                    };
                    if let Err(err) = self.advance(event).await {
                        event!(
                            name: "eez.sequencer.advance.failed",
                            Level::ERROR,
                            error = %err,
                            "advance failed: {{error}}",
                        );
                    }
                }
                _ = fcu_interval.tick() => {
                    if let Err(err) = self.committer.refresh_forkchoice().await {
                        event!(
                            name: "eez.sequencer.fcu.failed",
                            Level::WARN,
                            error = %err,
                            "forkchoice refresh failed: {{error}}",
                        );
                    }
                }
            }
        }
    }

    /// Dispatch on slot event variant.
    async fn advance(&mut self, event: SlotEvent) -> DriverResult<()> {
        match event {
            SlotEvent::SyncSlot {
                block_height,
                timestamp,
                l1_head,
            } => {
                self.advance_sync_slot(block_height, timestamp, l1_head)
                    .await
            }
            SlotEvent::LiveTick {
                sync_slot_block_height,
            } => {
                self.advance_anchored_live_tick(sync_slot_block_height)
                    .await
            }
        }
    }

    /// L1-anchored wall-clock tick handler: commit exactly ONE Live
    /// block, and only while the head is below the live region of the
    /// next sync slot (`sync_slot_block_height - future_count - 1`).
    /// The Future + Sync reserve belongs to the anchor trigger
    /// ([`Self::advance_sync_slot`]); ticks no-op once the live region
    /// is full, so they can't outrun the anchor.
    async fn advance_anchored_live_tick(
        &mut self,
        sync_slot_block_height: u64,
    ) -> DriverResult<()> {
        let rollup_id = self.rollup_id;
        let composer = Arc::clone(&self.composer);
        // Before the head read: a mid-slot verdict would otherwise wait K
        // blocks, and recovery can reorg + substitute under us.
        composer.recover_failed(rollup_id).await;
        let last_header = self.committer.last_header();
        let head = last_header.number();
        // Reserve = Future blocks + the Sync block itself. Mirrors
        // `live_region_end` in `RollupTiming::per_trigger_composition`.
        let reserve = u64::from(self.timing.future_count()) + 1;
        let live_region_end = sync_slot_block_height.saturating_sub(reserve);
        if head >= live_region_end {
            // Live region full (steady state between anchor fire and
            // the next L1 head). Future + Sync come from the anchor.
            return Ok(());
        }
        if self.speculative_limit_paused(head) {
            return Ok(());
        }
        match self.commit_one(SlotKind::Live, &last_header).await {
            Ok(()) => Ok(()),
            Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                log_stale_parent("live_tick");
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// True if the bundle (ready at ~`now + proof_time`) can't reach the
    /// relay by `sync_slot_timestamp − submission_slack`, i.e. it would miss
    /// the immediate next L1 slot. Steady state has headroom; fires only on
    /// a late trigger, where the slot is deferred.
    fn sync_block_misses_l1_slot(&self, sync_slot_timestamp: u64) -> bool {
        misses_l1_slot(self.timing, self.clock.now_unix_ms(), sync_slot_timestamp)
    }

    /// One skip self-heals next trigger; a run of them means production is
    /// wedged, so escalate past a few.
    fn note_off_grid_skip(&mut self) {
        self.off_grid_skips = self.off_grid_skips.saturating_add(1);
        if self.off_grid_skips >= 3 {
            event!(
                name: "eez.sequencer.sync_slot.off_grid_stalled",
                Level::ERROR,
                consecutive_skips = self.off_grid_skips,
                "consecutive Sync blocks skipped for off-grid drift; production is not advancing",
            );
        }
    }

    /// L1-anchored handler: read current head, compute the per-trigger
    /// Live/Future/Sync split via
    /// [`RollupTiming::per_trigger_composition`], produce accordingly.
    async fn advance_sync_slot(
        &mut self,
        sync_slot_block_height: u64,
        sync_slot_timestamp: u64,
        l1_head: u64,
    ) -> DriverResult<()> {
        let rollup_id = self.rollup_id;
        let composer = Arc::clone(&self.composer);
        // Before the composition: a pending verdict would seed the whole burst
        // on a doomed head, for compose_sync_slot to reorg away at its end.
        composer.recover_failed(rollup_id).await;
        let head = self.committer.last_header().number();
        // Catchup ignores the speculative cap (a steady-state bound): a
        // dropped pb heals by recomposing next trigger, which freezing to
        // Idle would block. The steady (Slot) arm still enforces the cap.
        let comp = self.timing.per_trigger_composition(
            head,
            sync_slot_block_height,
            crate::MAX_BLOCKS_PER_CATCHUP,
        );

        event!(
            name: "eez.sequencer.sync_slot.composition",
            Level::INFO,
            head,
            sync_slot_block_height,
            sync_slot_timestamp,
            composition = ?comp,
            "computed per-trigger slot composition",
        );

        // Target an absolute height, not a count. The committer is shared, so a
        // count re-derived from a moved head would land the Sync block off-grid.
        let terminal = match comp {
            SlotComposition::Catchup { live } => Some(head + live + 1),
            SlotComposition::Slot { .. } | SlotComposition::Idle => None,
        };

        match comp {
            SlotComposition::Idle => {}
            SlotComposition::Catchup { live } => {
                // `live` is grid-snapped + capped by per_trigger_composition;
                // produce that many Live blocks then the terminal Sync.
                for _ in 0..live {
                    let last_header = self.committer.last_header();
                    match self.commit_one(SlotKind::Live, &last_header).await {
                        Ok(()) => {}
                        Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                            log_stale_parent("catchup.live");
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }

                // Terminal Sync block, parent-paced. Empty
                // (`SyncSlotMode::Catchup`): a behind block can't ts-align its
                // postBatch, so cross-chain waits for the next Slot.
                if terminal != Some(self.committer.last_header().number() + 1) {
                    event!(
                        name: "eez.sequencer.sync_slot.off_grid_skipped",
                        Level::WARN,
                        head_now = self.committer.last_header().number(),
                        terminal = ?terminal,
                        "head moved during the burst; skipping the Sync block rather than placing it off-grid",
                    );
                    self.note_off_grid_skip();
                    return Ok(());
                }
                let last_header = self.committer.last_header();
                let sync_ts = last_header
                    .timestamp()
                    .saturating_add(self.timing.l2_block_time().as_secs());

                let parent = crate::slot::ParentContext {
                    header: last_header.clone(),
                };
                let prebuilt = composer
                    // Supply the immediate next L1 block for a cap-triggered
                    // historical chunk. The ordinary catch-up marker remains
                    // unpinned so it can survive an L1 reorg.
                    .compose_sync_slot(
                        rollup_id,
                        parent,
                        sync_ts,
                        Some(l1_head + 1),
                        SyncSlotMode::Catchup,
                    )
                    .await;
                if let Some(built) = prebuilt {
                    if let Err(err) = self.commit_one_prebuilt(SlotKind::Sync, built).await {
                        if matches!(&err, DriverError::StaleParent { .. }) {
                            log_stale_parent("catchup.sync.prebuilt");
                            return Ok(());
                        }
                        return Err(err);
                    }
                    self.off_grid_skips = 0;
                } else {
                    match self.commit_one(SlotKind::Sync, &last_header).await {
                        Ok(()) => {}
                        Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                            log_stale_parent("catchup.sync");
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                    self.off_grid_skips = 0;
                }
            }
            SlotComposition::Slot { live, future } => {
                for _ in 0..live {
                    let last_header = self.committer.last_header();
                    if self.speculative_limit_paused(last_header.number()) {
                        return Ok(()); // Defer Future + Sync to next trigger.
                    }
                    match self.commit_one(SlotKind::Live, &last_header).await {
                        Ok(()) => {}
                        Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                            log_stale_parent("slot.live");
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }
                for _ in 0..future {
                    let last_header = self.committer.last_header();
                    if self.speculative_limit_paused(last_header.number()) {
                        return Ok(()); // Defer remaining Future + Sync to next trigger.
                    }
                    match self.commit_one(SlotKind::Future, &last_header).await {
                        Ok(()) => {}
                        Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                            log_stale_parent("slot.future");
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }
                let last_header = self.committer.last_header();
                if self.speculative_limit_paused(last_header.number()) {
                    return Ok(()); // Defer the Sync (postBatch terminal) to next trigger.
                }
                let expected_sync_ts = last_header
                    .timestamp()
                    .saturating_add(self.timing.l2_block_time().as_secs());
                if expected_sync_ts != sync_slot_timestamp {
                    // The head moved under us. This block would settle, and an
                    // off-grid root wedges the deriver. Skip; next trigger realigns.
                    event!(
                        name: "eez.sequencer.sync_slot.off_grid_skipped",
                        Level::WARN,
                        expected = expected_sync_ts,
                        sync_slot_timestamp,
                        head = last_header.number(),
                        sync_slot_block_height,
                        "sync-slot drift: parent + L2_block_time != the L1-derived sync timestamp; skipping the Sync block rather than settling off-grid",
                    );
                    self.note_off_grid_skip();
                    return Ok(());
                }

                // Defer-on-lateness: a late trigger that can't land the bundle
                // in the next L1 slot commits an empty block and holds the pool
                // (next slot covers it). Else drain into a prebuilt Sync block.
                // Check against expected_sync_ts — the ts the bundle is pinned
                // to below — so an off-grid trigger already past its pin defers
                // instead of composing a bundle that can't make its slot.
                let prebuilt = if self.sync_block_misses_l1_slot(expected_sync_ts) {
                    event!(
                        name: "eez.sequencer.sync_slot.deferred_late",
                        Level::WARN,
                        sync_slot_block_height,
                        sync_slot_timestamp,
                        l1_head,
                        "trigger too late for the immediate L1 slot; committing an empty block and holding the pool — next slot's batch covers it",
                    );
                    // Empty mode keeps this grid height batch-boundary-eligible
                    // (a tx-bearing block is not). If the empty build fails,
                    // control falls through to the mempool-fed commit below.
                    let parent = crate::slot::ParentContext {
                        header: last_header.clone(),
                    };
                    composer
                        .compose_sync_slot(
                            rollup_id,
                            parent,
                            expected_sync_ts,
                            None,
                            SyncSlotMode::Empty,
                        )
                        .await
                } else {
                    let parent = crate::slot::ParentContext {
                        header: last_header.clone(),
                    };
                    composer
                        // Steady: aim the immediate next L1 block, pinned to
                        // expected_sync_ts (re-derivable; == the L1 slot ts on-grid).
                        .compose_sync_slot(
                            rollup_id,
                            parent,
                            expected_sync_ts,
                            Some(l1_head + 1),
                            SyncSlotMode::Steady,
                        )
                        .await
                };
                if let Some(built) = prebuilt {
                    match self.commit_one_prebuilt(SlotKind::Sync, built).await {
                        Ok(()) => {}
                        Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                            log_stale_parent("slot.sync.prebuilt");
                        }
                        Err(err) => return Err(err),
                    }
                    self.off_grid_skips = 0;
                    return Ok(());
                }

                match self.commit_one(SlotKind::Sync, &last_header).await {
                    Ok(()) => {}
                    Err(err) if matches!(&err, DriverError::StaleParent { .. }) => {
                        log_stale_parent("slot.sync");
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                }
                self.off_grid_skips = 0;
            }
        }
        Ok(())
    }

    /// True if the speculative-depth limit is reached; logged at DEBUG.
    fn speculative_limit_paused(&self, head_number: u64) -> bool {
        if self.max_speculative_depth == 0 {
            return false;
        }
        let confirmed = self.l1_head.cursor();
        let speculative_depth = head_number.saturating_sub(confirmed);
        if speculative_depth >= self.max_speculative_depth {
            event!(
                name: "eez.sequencer.speculative.paused",
                Level::DEBUG,
                head_number,
                confirmed,
                speculative_depth,
                max_depth = self.max_speculative_depth,
                "paused: speculative depth at cap; waiting for Deriver to catch up",
            );
            return true;
        }
        false
    }

    /// Commit a pre-built Sync block (from the cross-chain composer)
    /// via `commit_derived` — same engine-API tail the Deriver uses
    /// for L1-derived blocks. Holds the reconcile lock so a sync-slot
    /// commit can't race with a deriver-side reconcile. Releases the
    /// block's bundle only after the commit; on any error it is withheld.
    async fn commit_one_prebuilt(
        &mut self,
        kind: SlotKind,
        built: crate::slot::SyncSlotBlock,
    ) -> DriverResult<()> {
        let crate::slot::SyncSlotBlock {
            payload,
            header,
            release,
        } = built;
        // `commit_derived` doesn't take the lock itself (only
        // `commit_sequenced` does, and the Deriver already holds it via
        // `begin_reconcile`), so hold it here too.
        let _reconcile_guard = self.committer.begin_reconcile().await;
        // Stale-parent check under the lock, like commit_sequenced: the
        // Deriver (or a failed-batch recovery) can move the head between
        // compose and commit, and a prebuilt block on a stale parent
        // would silently reorg that newer work via commit_derived's FCU.
        let current_head = self.committer.last_header().hash();
        let built_parent = header.parent_hash();
        if current_head != built_parent {
            return Err(DriverError::StaleParent {
                expected: built_parent,
                actual: current_head,
            });
        }
        let block_number = header.number();
        let block_hash = header.hash();
        let block_timestamp = header.timestamp();
        let _outcome = self
            .committer
            // feed_witness=true: this is a PRODUCED Sync block — emit it to the
            // prover witness-capture feed (the follower/deriver passes false).
            .commit_derived(payload, header, true)
            .await?;
        if let Some(release) = release {
            release.committed(block_hash);
        }

        event!(
            name: "eez.sequencer.block.produced",
            Level::INFO,
            slot.kind = %kind,
            block.number = block_number,
            block.hash = %block_hash,
            block.timestamp = block_timestamp,
            "produced block {block_number} hash={block_hash} ts={block_timestamp} kind={kind}",
        );
        Ok(())
    }

    /// Commit one block of the given kind at
    /// `parent.timestamp() + L2_block_time`. Logs `eez.sequencer.block.produced`.
    async fn commit_one(
        &mut self,
        kind: SlotKind,
        parent: &SealedHeader<ChainSpec::Header>,
    ) -> DriverResult<()> {
        let parent_hash = parent.hash();
        let parent_ts = parent.timestamp();
        let next_ts = parent_ts.saturating_add(self.timing.l2_block_time().as_secs());
        let attrs = self.attributes.build(parent, next_ts);
        let timestamp = attrs.timestamp;
        let outcome = self.committer.commit_sequenced(parent_hash, attrs).await?;
        let block_number = outcome.header.number();
        let block_hash = outcome.header.hash();

        event!(
            name: "eez.sequencer.block.produced",
            Level::INFO,
            slot.kind = %kind,
            block.number = block_number,
            block.hash = %block_hash,
            block.timestamp = timestamp,
            "produced block {block_number} hash={block_hash} ts={timestamp} kind={kind}",
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use alloy_consensus::Header;
    use async_trait::async_trait;
    use eez_l1::{BatchRecord, L1CanonicalHead, scan::PostId};
    use eez_timing::TokioClock;
    use reth_chainspec::{ChainSpec, ChainSpecBuilder};
    use reth_ethereum_engine_primitives::EthEngineTypes;

    use super::*;
    use crate::slot::{CommitRelease, ParentContext, SyncSlotBlock, SyncSlotComposer};
    use crate::test_engine::{EngineCall, MockEngine, MockStatus};
    use reth_ethereum_engine_primitives::EthEngineTypes as Engine;
    use tokio::sync::oneshot;

    /// 12 s L1, 2 s L2, proof 4000, slack 1500: live 3, future 2, K 6.
    const TIMING: RollupTiming = RollupTiming::new(12_000, 2_000, 4_000, 1_500);
    const GENESIS_TS: u64 = 1_700_000_000;
    /// The Sync block of the first slot: height 6, one L1 block after genesis.
    const SYNC_HEIGHT: u64 = 6;
    const SYNC_TS: u64 = GENESIS_TS + 12;
    const L1_HEAD: u64 = 500;
    /// The trigger instant, `proof_window_open` after the anchor head.
    const TRIGGER_MS: u64 = (GENESIS_TS + 6) * 1_000;
    /// The last instant at which the bundle still makes the L1 slot.
    const CUTOFF_MS: u64 = SYNC_TS * 1_000 - 1_500 - 4_000;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ComposeCall {
        parent: u64,
        timestamp: u64,
        target: Option<u64>,
        mode: SyncSlotMode,
        /// Engine calls logged before this compose call.
        engine_calls: usize,
    }

    /// What the composer returns for the Sync block.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Prebuild {
        /// No block: the Sequencer commits the Sync block through the engine.
        None,
        /// A prebuilt child of the parent, with a bundle release.
        Child,
        /// A prebuilt child of a sibling of the parent.
        StaleChild,
    }

    type Release = tokio::task::JoinHandle<Result<B256, oneshot::error::RecvError>>;

    /// Records every compose call and returns the [`Prebuild`] block.
    #[derive(Debug)]
    struct RecordingComposer {
        engine: MockEngine,
        calls: Mutex<Vec<ComposeCall>>,
        prebuild: Mutex<Prebuild>,
        /// Resolves when the prebuilt block's bundle is released or withheld.
        release: Mutex<Option<Release>>,
    }

    impl RecordingComposer {
        /// A prebuilt child of `parent` whose release is logged in engine order.
        fn prebuilt(&self, parent: &SealedHeader, timestamp: u64) -> SyncSlotBlock {
            let header = SealedHeader::seal_slow(Header {
                parent_hash: parent.hash(),
                number: parent.number + 1,
                timestamp,
                gas_limit: parent.gas_limit,
                ..Default::default()
            });
            let block = reth_primitives_traits::SealedBlock::<
                reth_ethereum_primitives::Block,
            >::from_sealed_parts(header.clone(), Default::default());
            let (release, released) = CommitRelease::new();
            let engine = self.engine.clone();
            *self.release.lock().unwrap() = Some(tokio::spawn(async move {
                let hash = released.await?;
                engine.note_release(hash);
                Ok(hash)
            }));
            SyncSlotBlock {
                payload: <Engine as PayloadTypes>::block_to_payload(block, None),
                header,
                release: Some(release),
            }
        }

        async fn released(&self) -> Result<B256, oneshot::error::RecvError> {
            let release = self
                .release
                .lock()
                .unwrap()
                .take()
                .expect("no prebuilt block");
            release.await.unwrap()
        }
    }

    #[async_trait]
    impl SyncSlotComposer for RecordingComposer {
        async fn compose_sync_slot(
            &self,
            _rollup_id: u64,
            parent: ParentContext,
            timestamp: u64,
            target_l1_block: Option<u64>,
            mode: SyncSlotMode,
        ) -> Option<SyncSlotBlock> {
            self.calls.lock().unwrap().push(ComposeCall {
                parent: parent.header.number(),
                timestamp,
                target: target_l1_block,
                mode,
                engine_calls: self.engine.calls().len(),
            });
            match *self.prebuild.lock().unwrap() {
                Prebuild::None => None,
                Prebuild::Child => Some(self.prebuilt(&parent.header, timestamp)),
                Prebuild::StaleChild => {
                    let mut sibling = parent.header.clone_header();
                    sibling.extra_data = vec![1].into();
                    Some(self.prebuilt(&SealedHeader::seal_slow(sibling), timestamp))
                }
            }
        }
    }

    struct Fixture {
        engine: MockEngine,
        composer: Arc<RecordingComposer>,
        sequencer: Sequencer<EthEngineTypes, ChainSpec>,
    }

    impl Fixture {
        fn new(now_ms: u64, max_speculative_depth: u64, l1_head: L1CanonicalHead) -> Self {
            let genesis = SealedHeader::seal_slow(Header {
                timestamp: GENESIS_TS,
                gas_limit: 30_000_000,
                ..Default::default()
            });
            let (engine, committer) = MockEngine::committer(&genesis);
            let composer = Arc::new(RecordingComposer {
                engine: engine.clone(),
                calls: Mutex::default(),
                prebuild: Mutex::new(Prebuild::None),
                release: Mutex::default(),
            });
            let chain_spec = Arc::new(ChainSpecBuilder::mainnet().build());
            let (_schedule_tx, schedule_rx) = mpsc::channel(1);
            let sequencer = Sequencer::composer(
                EthAttributesBuilder::new(chain_spec),
                committer,
                schedule_rx,
                TIMING,
                Arc::new(TokioClock::starting_at(now_ms)),
                1,
                composer.clone(),
                max_speculative_depth,
                Arc::new(l1_head),
            );
            Self {
                engine,
                composer,
                sequencer,
            }
        }

        async fn sync_slot(&mut self, timestamp: u64) {
            self.try_sync_slot(timestamp).await.unwrap();
        }

        async fn try_sync_slot(&mut self, timestamp: u64) -> DriverResult<()> {
            self.sequencer
                .advance(SlotEvent::SyncSlot {
                    block_height: SYNC_HEIGHT,
                    timestamp,
                    l1_head: L1_HEAD,
                })
                .await
        }

        fn compose_calls(&self) -> Vec<ComposeCall> {
            self.composer.calls.lock().unwrap().clone()
        }

        fn commits(&self) -> usize {
            self.engine
                .calls()
                .iter()
                .filter(|call| matches!(call, EngineCall::NewPayload { .. }))
                .count()
        }

        fn head(&self) -> u64 {
            self.sequencer.committer.last_header().number()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn steady_trigger_commits_live_and_future_then_composes_steady() {
        let mut f = Fixture::new(
            TRIGGER_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        f.sync_slot(SYNC_TS).await;

        // Each sequenced commit is FCU+attrs, resolve, newPayload, head FCU.
        let calls = f.engine.calls();
        assert_eq!(calls.len(), 6 * 4);
        for commit in calls.chunks(4) {
            assert!(matches!(
                commit,
                [
                    EngineCall::Fcu {
                        with_attrs: true,
                        ..
                    },
                    EngineCall::Resolve { .. },
                    EngineCall::NewPayload { .. },
                    EngineCall::Fcu {
                        with_attrs: false,
                        ..
                    },
                ]
            ));
        }
        // 3 Live + 2 Future, then the steady compose, then the Sync commit.
        assert_eq!(
            f.compose_calls(),
            vec![ComposeCall {
                parent: 5,
                timestamp: SYNC_TS,
                target: Some(L1_HEAD + 1),
                mode: SyncSlotMode::Steady,
                engine_calls: 5 * 4,
            }]
        );
        assert_eq!(f.head(), SYNC_HEIGHT);
        assert_eq!(f.sequencer.committer.last_header().timestamp(), SYNC_TS);
    }

    #[tokio::test(start_paused = true)]
    async fn late_trigger_composes_empty_sync_block() {
        // On time at the deadline; one millisecond later the slot is deferred.
        let mut on_time = Fixture::new(
            CUTOFF_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        on_time.sync_slot(SYNC_TS).await;
        assert_eq!(on_time.compose_calls()[0].mode, SyncSlotMode::Steady);

        let mut late = Fixture::new(
            CUTOFF_MS + 1,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        late.sync_slot(SYNC_TS).await;
        assert_eq!(
            late.compose_calls(),
            vec![ComposeCall {
                parent: 5,
                timestamp: SYNC_TS,
                target: None,
                mode: SyncSlotMode::Empty,
                engine_calls: 5 * 4,
            }]
        );
        assert_eq!(late.head(), SYNC_HEIGHT);
    }

    #[tokio::test(start_paused = true)]
    async fn off_grid_head_skips_the_sync_block() {
        let mut f = Fixture::new(
            TRIGGER_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        // One second off the grid: parent + L2 lands on SYNC_TS, not SYNC_TS + 1.
        f.sync_slot(SYNC_TS + 1).await;

        assert!(f.compose_calls().is_empty());
        assert_eq!(f.sequencer.off_grid_skips, 1);
        assert_eq!(f.commits(), 5);
        assert_eq!(f.head(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn speculative_cap_holds_future_and_sync() {
        let l1_head = L1CanonicalHead::default();
        l1_head.seed(BatchRecord {
            post: PostId {
                l1_block: 1,
                l1_block_hash: B256::repeat_byte(1),
                log_index: 0,
            },
            tx_hash: B256::repeat_byte(2),
            last_l2_block: 1,
        });
        let mut f = Fixture::new(TRIGGER_MS, 3, l1_head);
        f.sync_slot(SYNC_TS).await;

        // Cursor 1, depth 3: 3 Live and 1 Future commit, then the cap holds
        // the second Future block and the Sync block.
        assert_eq!(f.commits(), 4);
        assert_eq!(f.head(), 4);
        assert!(f.compose_calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn prebuilt_release_follows_the_commit() {
        let mut f = Fixture::new(
            TRIGGER_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        *f.composer.prebuild.lock().unwrap() = Prebuild::Child;
        f.sync_slot(SYNC_TS).await;

        let hash = f.composer.released().await.unwrap();
        assert_eq!(hash, f.sequencer.committer.last_header().hash());
        let calls = f.engine.calls();
        assert_eq!(
            calls[5 * 4..],
            [
                EngineCall::NewPayload { hash },
                EngineCall::Fcu {
                    head: hash,
                    with_attrs: false
                },
                EngineCall::Released { hash },
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_parent_withholds_the_bundle() {
        let mut f = Fixture::new(
            TRIGGER_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        *f.composer.prebuild.lock().unwrap() = Prebuild::StaleChild;
        f.sync_slot(SYNC_TS).await;

        assert!(f.composer.released().await.is_err());
        assert_eq!(f.commits(), 5);
        assert_eq!(f.head(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_payload_withholds_the_bundle() {
        let mut f = Fixture::new(
            TRIGGER_MS,
            DEFAULT_MAX_SPECULATIVE_DEPTH,
            L1CanonicalHead::default(),
        );
        *f.composer.prebuild.lock().unwrap() = Prebuild::Child;
        // Five sequenced blocks pass; the prebuilt Sync block is rejected.
        for _ in 0..5 {
            f.engine.queue_new_payload(MockStatus::Valid);
        }
        f.engine.queue_new_payload(MockStatus::Invalid);
        let err = f.try_sync_slot(SYNC_TS).await.unwrap_err();

        assert!(matches!(err, DriverError::InvalidPayload(_)), "{err}");
        assert!(f.composer.released().await.is_err());
        assert_eq!(f.head(), 5);
    }
}
