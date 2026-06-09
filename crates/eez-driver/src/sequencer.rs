//! Engine-API consumer that advances the chain on a 2s cadence aligned to
//! wall-clock time, with **greedy backfill** of any timestamp slots that
//! were missed due to a reorg or downtime.
//!
//! [`Sequencer::run`] is the long-running loop. Each iteration handles one
//! of:
//!
//! 1. A scheduler tick. [`Sequencer::advance`] runs in a loop, producing
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
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{BuiltPayload, PayloadTypes};
use reth_primitives_traits::{
    AlloyBlockHeader, HeaderTy, NodePrimitives, SealedHeader, SealedHeaderFor,
};
use reth_storage_api::BlockReader;
use tracing::{Level, event};

use tokio::sync::mpsc;

use crate::block_committer::BlockCommitterHandle;
use crate::error::{DriverError, DriverResult};
use crate::slot::{SlotEvent, SlotKind, SyncSlotComposerHandle};
use crate::submit::{BatchCandidate, BatchEmitter, BatchPolicy};
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

/// L1-confirmed L2 head height. `eez_l1::L1CanonicalHead`
/// implements this; the Sequencer uses it to bound speculative depth.
pub trait ConfirmedHeadSource: Send + Sync + 'static {
    /// Highest L2 block confirmed by an L1-landed batch.
    fn confirmed_head(&self) -> u64;
}

struct SpeculativeLimit {
    max_depth: u64,
    source: Arc<dyn ConfirmedHeadSource>,
}

impl fmt::Debug for SpeculativeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeculativeLimit")
            .field("max_depth", &self.max_depth)
            .finish_non_exhaustive()
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
    /// Optional speculative-depth cap. None = no limit
    /// (single-composer / follower mode). See `DEFAULT_MAX_SPECULATIVE_DEPTH`.
    speculative_limit: Option<SpeculativeLimit>,
    /// Optional [`BatchCandidate`] emitter. None on follower setups
    /// where no Composer is co-located.
    batch_emitter: Option<BatchEmitter>,
    /// Optional Sync-slot system-tx producer. When present, called
    /// before each Sync block to fetch the cross-chain system txs
    /// for the slot. Defaults to `None` — equivalent to producing
    /// empty Sync blocks (no cross-chain content).
    sync_slot_composer: Option<SyncSlotComposerHandle>,
}

impl<T, ChainSpec> fmt::Debug for Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("committer", &self.committer)
            .field("speculative_limit", &self.speculative_limit)
            .field("batch_emitter", &self.batch_emitter)
            .finish_non_exhaustive()
    }
}

impl<T, ChainSpec> Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes> + Send + Sync + 'static,
    <T::BuiltPayload as BuiltPayload>::Primitives: NodePrimitives + Send + Sync + 'static,
    SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Send,
    ChainSpec: EthChainSpec<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>
        + EthereumHardforks
        + Send
        + Sync
        + 'static,
{
    /// Constructs a sequencer by reading the current best block from
    /// Construct a sequencer: read `provider`'s best block, spawn a
    /// `BlockCommitter` seeded with that header.
    ///
    /// # Errors
    ///
    /// `provider` (lookup failure), `missing_header` (best block has no
    /// header — brief startup race).
    pub fn new<P>(
        provider: &P,
        attributes: EthAttributesBuilder<ChainSpec>,
        to_engine: ConsensusEngineHandle<T>,
        schedule_rx: mpsc::Receiver<SlotEvent>,
        payload_builder: PayloadBuilderHandle<T>,
        timing: RollupTiming,
    ) -> DriverResult<Self>
    where
        P: BlockReader<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>,
    {
        let best = provider
            .best_block_number()
            .map_err(DriverError::provider)?;
        let last_header = provider
            .sealed_header(best)
            .map_err(DriverError::provider)?
            .ok_or_else(|| DriverError::missing_header(best))?;
        let committer = BlockCommitterHandle::spawn(last_header, to_engine, payload_builder);
        Ok(Self {
            attributes,
            schedule_rx,
            committer,
            timing,
            speculative_limit: None,
            batch_emitter: None,
            sync_slot_composer: None,
        })
    }

    /// Attach a [`SyncSlotComposerHandle`]. When set, the Sequencer
    /// calls it before each Sync block to fetch cross-chain system
    /// txs. None = empty Sync blocks (no cross-chain content).
    #[must_use]
    pub fn with_sync_slot_composer(mut self, composer: SyncSlotComposerHandle) -> Self {
        self.sync_slot_composer = Some(composer);
        self
    }

    /// Cap blocks above `source`'s L1-confirmed head; `advance` pauses
    /// past `max_depth` so the Deriver gets time to replay L1 batches
    /// without state-pruning churn. Skip for single-composer /
    /// follower setups. Use `DEFAULT_MAX_SPECULATIVE_DEPTH` for the
    /// typical based-mode deployment.
    #[must_use]
    pub fn with_speculative_limit(
        mut self,
        max_depth: u64,
        source: Arc<dyn ConfirmedHeadSource>,
    ) -> Self {
        self.speculative_limit = Some(SpeculativeLimit { max_depth, source });
        self
    }

    /// Attach a [`BatchCandidate`] sender. After each committed block,
    /// the Sequencer evaluates `policy` and sends one candidate per
    /// closed window. Backpressured: if the receiver is full, the
    /// produce loop awaits — Composer falling behind slows block
    /// production rather than dropping candidates.
    #[must_use]
    pub fn with_batch_emitter(
        mut self,
        rollup_id: u64,
        policy: BatchPolicy,
        tx: mpsc::Sender<BatchCandidate>,
    ) -> Self {
        self.batch_emitter = Some(BatchEmitter::new(rollup_id, policy, tx));
        self
    }

    /// Clone-cheap handle to the underlying `BlockCommitter` actor.
    /// Other components (Deriver) push commands through the same task.
    #[must_use]
    pub fn committer(&self) -> BlockCommitterHandle<T> {
        self.committer.clone()
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
            } => self.advance_sync_slot(block_height, timestamp).await,
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
                    event!(
                        name: "eez.sequencer.stale_parent",
                        Level::DEBUG,
                        parent_hash = %last_header.hash(),
                        "deriver advanced the chain between snapshot and FCU; breaking backfill loop",
                    );
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

    /// L1-anchored handler: read current head, compute the per-trigger
    /// Live/Future/Sync split via
    /// [`RollupTiming::per_trigger_composition`], produce accordingly.
    async fn advance_sync_slot(
        &mut self,
        sync_slot_block_height: u64,
        sync_slot_timestamp: u64,
    ) -> DriverResult<()> {
        let head = self.committer.last_header().number();
        let comp = self
            .timing
            .per_trigger_composition(head, sync_slot_block_height);

        event!(
            name: "eez.sequencer.sync_slot.composition",
            Level::INFO,
            head,
            sync_slot_block_height,
            sync_slot_timestamp,
            composition = ?comp,
            "computed per-trigger slot composition",
        );

        match comp {
            SlotComposition::Idle => {}
            SlotComposition::Catchup { live } => {
                for _ in 0..live {
                    let last_header = self.committer.last_header();
                    if self.speculative_limit_paused(last_header.number()) {
                        break;
                    }
                    match self.commit_one(SlotKind::Live, &last_header).await {
                        Ok(()) => {}
                        Err(err) if err.is_stale_parent() => {
                            event!(
                                name: "eez.sequencer.stale_parent",
                                Level::DEBUG,
                                parent_hash = %last_header.hash(),
                                "deriver advanced during catchup; bailing this trigger",
                            );
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
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
                            event!(
                                name: "eez.sequencer.stale_parent",
                                Level::DEBUG,
                                parent_hash = %last_header.hash(),
                                "deriver advanced during Slot.live; bailing this trigger",
                            );
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }
                for _ in 0..future {
                    let last_header = self.committer.last_header();
                    match self.commit_one(SlotKind::Future, &last_header).await {
                        Ok(()) => {}
                        Err(err) if err.is_stale_parent() => {
                            event!(
                                name: "eez.sequencer.stale_parent",
                                Level::DEBUG,
                                parent_hash = %last_header.hash(),
                                "deriver advanced during Slot.future; bailing this trigger",
                            );
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }
                let last_header = self.committer.last_header();
                let expected_sync_ts = last_header
                    .timestamp()
                    .saturating_add(self.timing.l2_block_time().as_secs());
                if expected_sync_ts != sync_slot_timestamp {
                    event!(
                        name: "eez.sequencer.sync_slot.timestamp_mismatch",
                        Level::WARN,
                        expected = expected_sync_ts,
                        sync_slot_timestamp,
                        head = last_header.number(),
                        "sync-slot timestamp drift: parent + L2_block_time != Scheduler-supplied sync_slot_timestamp; producing with parent-derived value",
                    );
                }

                // Cross-chain content hook (S4.7): pull any pre-composed
                // system txs for this Sync slot from the umbrella's
                // composer. Empty in the common case (no cross-chain
                // content this slot). Non-empty routes through the
                // manual-block-construction path (commit_with_system_txs)
                // per the S4.3 design — not implemented yet, so we
                // assert empty until S4.8 wiring lands.
                if let (Some(composer), Some(emitter)) = (
                    self.sync_slot_composer.as_ref(),
                    self.batch_emitter.as_ref(),
                ) {
                    let system_txs = composer.compose_sync_slot(emitter.rollup_id).await;
                    assert!(
                        system_txs.is_empty(),
                        "S4.7 contract: {} cross-chain system tx(s) produced for sync slot, but \
                         commit_with_system_txs path lands in a later phase. Disable cross-chain \
                         composition or land the manual block-construction path.",
                        system_txs.len(),
                    );
                }

                match self.commit_one(SlotKind::Sync, &last_header).await {
                    Ok(()) => {}
                    Err(err) if err.is_stale_parent() => {
                        event!(
                            name: "eez.sequencer.stale_parent",
                            Level::DEBUG,
                            parent_hash = %last_header.hash(),
                            "deriver advanced during Slot.sync; bailing this trigger",
                        );
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    /// True if the speculative-depth limit is reached; logged at DEBUG.
    fn speculative_limit_paused(&self, head_number: u64) -> bool {
        let Some(limit) = &self.speculative_limit else {
            return false;
        };
        let confirmed = limit.source.confirmed_head();
        let speculative_depth = head_number.saturating_sub(confirmed);
        if speculative_depth >= limit.max_depth {
            event!(
                name: "eez.sequencer.speculative.paused",
                Level::DEBUG,
                head_number,
                confirmed,
                speculative_depth,
                max_depth = limit.max_depth,
                "paused: speculative depth at cap; waiting for Deriver to catch up",
            );
            return true;
        }
        false
    }

    /// Commit one block of the given kind at
    /// `parent.timestamp() + L2_block_time`. Logs `eez.sequencer.block.produced`
    /// and forwards a `BatchCandidate` to the emitter when the policy
    /// closes a window.
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
            "produced block {{block.number}} hash={{block.hash}} ts={{block.timestamp}} kind={{slot.kind}}",
        );

        if let Some(emitter) = self.batch_emitter.as_mut() {
            if let Some(candidate) = emitter.on_block_committed(block_number) {
                // Backpressured: if Composer is slow draining, this
                // await pauses block production rather than dropping
                // the window. Channel-closed = Composer task died;
                // log loudly so the next layer of supervision sees it.
                if let Err(err) = emitter.tx.send(candidate).await {
                    event!(
                        name: "eez.sequencer.batch_candidate.send_failed",
                        Level::ERROR,
                        rollup_id = candidate.rollup_id,
                        to_block = candidate.to_block,
                        error = %err,
                        "batch candidate channel closed; downstream Composer task is gone",
                    );
                }
            }
        }
        Ok(())
    }
}
