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
use crate::slot::{SlotEvent, SlotKind, SyncSlotComposerHandle, SyncSlotMode};
use crate::timing::{RollupTiming, SlotComposition};

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

enum SequencingMode {
    Standalone,
    Composer {
        rollup_id: u64,
        composer: SyncSlotComposerHandle,
        max_speculative_depth: u64,
        l1_head: Arc<eez_l1::L1CanonicalHead>,
    },
}

impl fmt::Debug for SequencingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Standalone => f.write_str("Standalone"),
            Self::Composer {
                rollup_id,
                max_speculative_depth,
                ..
            } => f
                .debug_struct("Composer")
                .field("rollup_id", rollup_id)
                .field("max_speculative_depth", max_speculative_depth)
                .finish_non_exhaustive(),
        }
    }
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
            // Amsterdam-fork addition; not active for stage-1 dev chains.
            slot_number: None,
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
    /// Role-specific sequencing dependencies. Composer mode always has its
    /// Sync-slot composer and L1-confirmed head; standalone mode has neither.
    mode: SequencingMode,
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
            .field("mode", &self.mode)
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
    /// Construct an unanchored development sequencer around the binary-owned
    /// `BlockCommitter` actor.
    #[must_use]
    pub fn standalone(
        attributes: EthAttributesBuilder<ChainSpec>,
        committer: BlockCommitterHandle<T>,
        schedule_rx: mpsc::Receiver<SlotEvent>,
        timing: RollupTiming,
    ) -> Self {
        Self {
            attributes,
            schedule_rx,
            committer,
            timing,
            mode: SequencingMode::Standalone,
            off_grid_skips: 0,
        }
    }

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
            mode: SequencingMode::Composer {
                rollup_id,
                composer,
                max_speculative_depth,
                l1_head,
            },
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
            SlotEvent::Live { target_timestamp } => self.advance_live_tick(target_timestamp).await,
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

    /// Interval-mode handler: greedy backfill of Live blocks until the
    /// chain is within one L2 block-time of `target_wall`, capped at
    /// [`MAX_BLOCKS_PER_CATCHUP`](crate::MAX_BLOCKS_PER_CATCHUP) per
    /// invocation so the run-loop stays responsive to FCU refreshes
    /// and the next schedule event.
    async fn advance_live_tick(&mut self, target_wall: u64) -> DriverResult<()> {
        let l2_block_time = self.timing.l2_block_time().as_secs();
        let mut produced: u64 = 0;

        while produced < crate::MAX_BLOCKS_PER_CATCHUP {
            let last_header = self.committer.last_header();
            let gap = target_wall.saturating_sub(last_header.timestamp());
            if gap < l2_block_time {
                break;
            }
            if self.speculative_limit_paused(last_header.number()) {
                break;
            }
            match self.commit_one(SlotKind::Live, &last_header).await {
                Ok(()) => {}
                Err(err) if err.is_stale_parent() => {
                    log_stale_parent("backfill.live");
                    break;
                }
                Err(err) => return Err(err),
            }
            produced += 1;
        }

        if produced == crate::MAX_BLOCKS_PER_CATCHUP {
            event!(
                name: "eez.sequencer.backfill.yield",
                Level::INFO,
                target_timestamp = target_wall,
                last_block_timestamp = self.committer.last_header().timestamp(),
                "hit per-trigger block cap; continuing catch-up on next tick",
            );
        }
        Ok(())
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
        let Some((rollup_id, composer)) = self.composer_context() else {
            event!(
                name: "eez.sequencer.mode_mismatch",
                Level::ERROR,
                event = "live_tick",
                "standalone sequencer received an L1-anchored event",
            );
            return Ok(());
        };
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
            Err(err) if err.is_stale_parent() => {
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
        let proof_ms = u64::try_from(self.timing.proof_time().as_millis()).unwrap_or(u64::MAX);
        let slack_ms =
            u64::try_from(self.timing.submission_slack().as_millis()).unwrap_or(u64::MAX);
        let ready_ms = crate::slot::now_unix_millis().saturating_add(proof_ms);
        let deadline_ms = sync_slot_timestamp
            .saturating_mul(1_000)
            .saturating_sub(slack_ms);
        ready_ms > deadline_ms
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
        let Some((rollup_id, composer)) = self.composer_context() else {
            event!(
                name: "eez.sequencer.mode_mismatch",
                Level::ERROR,
                event = "sync_slot",
                "standalone sequencer received an L1-anchored event",
            );
            return Ok(());
        };
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
                        Err(err) if err.is_stale_parent() => {
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
                    // Catch-up: next-available L1 block (unpinned), empty.
                    .compose_sync_slot(rollup_id, parent, sync_ts, None, SyncSlotMode::Catchup)
                    .await;
                if let Some(built) = prebuilt {
                    if let Err(err) = self.commit_one_prebuilt(SlotKind::Sync, built).await {
                        if err.is_stale_parent() {
                            log_stale_parent("catchup.sync.prebuilt");
                            return Ok(());
                        }
                        return Err(err);
                    }
                    self.off_grid_skips = 0;
                } else {
                    match self.commit_one(SlotKind::Sync, &last_header).await {
                        Ok(()) => {}
                        Err(err) if err.is_stale_parent() => {
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
                        Err(err) if err.is_stale_parent() => {
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
                        Err(err) if err.is_stale_parent() => {
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
                        Err(err) if err.is_stale_parent() => {
                            log_stale_parent("slot.sync.prebuilt");
                        }
                        Err(err) => return Err(err),
                    }
                    self.off_grid_skips = 0;
                    return Ok(());
                }

                match self.commit_one(SlotKind::Sync, &last_header).await {
                    Ok(()) => {}
                    Err(err) if err.is_stale_parent() => {
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
        let (max_depth, source) = match &self.mode {
            SequencingMode::Standalone => return false,
            SequencingMode::Composer {
                max_speculative_depth,
                l1_head,
                ..
            } => (*max_speculative_depth, l1_head),
        };
        if max_depth == 0 {
            return false;
        }
        let confirmed = source.cursor();
        let speculative_depth = head_number.saturating_sub(confirmed);
        if speculative_depth >= max_depth {
            event!(
                name: "eez.sequencer.speculative.paused",
                Level::DEBUG,
                head_number,
                confirmed,
                speculative_depth,
                max_depth,
                "paused: speculative depth at cap; waiting for Deriver to catch up",
            );
            return true;
        }
        false
    }

    fn composer_context(&self) -> Option<(u64, SyncSlotComposerHandle)> {
        match &self.mode {
            SequencingMode::Standalone => None,
            SequencingMode::Composer {
                rollup_id,
                composer,
                ..
            } => Some((*rollup_id, Arc::clone(composer))),
        }
    }

    /// Commit a pre-built Sync block (from the cross-chain composer)
    /// via `commit_derived` — same engine-API tail the Deriver uses
    /// for L1-derived blocks. Holds the reconcile lock so a sync-slot
    /// commit can't race with a deriver-side reconcile.
    async fn commit_one_prebuilt(
        &mut self,
        kind: SlotKind,
        built: crate::slot::SyncSlotBlock,
    ) -> DriverResult<()> {
        // `commit_derived` doesn't take the lock itself (only
        // `commit_sequenced` does, and the Deriver already holds it via
        // `begin_reconcile`), so hold it here too.
        let _reconcile_guard = self.committer.begin_reconcile().await;
        // Stale-parent check under the lock, like commit_sequenced: the
        // Deriver (or a failed-batch recovery) can move the head between
        // compose and commit, and a prebuilt block on a stale parent
        // would silently reorg that newer work via commit_derived's FCU.
        let current_head = self.committer.last_header().hash();
        let built_parent = built.header.parent_hash();
        if current_head != built_parent {
            return Err(DriverError::stale_parent(built_parent, current_head));
        }
        let block_number = built.header.number();
        let block_hash = built.header.hash();
        let block_timestamp = built.header.timestamp();
        let _outcome = self
            .committer
            // feed_witness=true: this is a PRODUCED Sync block — emit it to the
            // prover witness-capture feed (the follower/deriver passes false).
            .commit_derived(built.payload, built.header, true)
            .await?;

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
