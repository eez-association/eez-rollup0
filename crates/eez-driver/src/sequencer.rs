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

use crate::block_committer::{BlockCommitterHandle, SequenceOutcome};
use crate::error::{DriverError, DriverResult};
use crate::scheduler::{ProposalRequest, Scheduler};

/// How often the sequencer re-publishes the current forkchoice state.
///
/// Even when no new block is being proposed, sending an FCU keeps reth's
/// engine convinced the chain is alive. Mirrors the cadence reth's own dev
/// miner uses.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// L2 block time, in seconds. Pinned at 2s per Rollup-1 spec §1.3 "no
/// skipped blocks" — each block's timestamp is exactly
/// `parent.timestamp() + BLOCK_TIME_SECS`.
const BLOCK_TIME_SECS: u64 = 2;

/// Yield bound on the per-tick backfill loop — caps blocks committed
/// in one scheduler tick before returning to the run-loop. Catch-up
/// resumes next tick. 32 blocks / 2s tick ≈ 16 blocks/s.
const MAX_BLOCKS_PER_TICK: usize = 32;

/// Bound parent-rebuild attempts that do not produce a block. Keeps one
/// scheduler tick from starving forkchoice refresh if another task keeps
/// moving the canonical parent underneath the sequencer.
const MAX_STALE_PARENT_RETRIES_PER_TICK: usize = MAX_BLOCKS_PER_TICK;

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
    scheduler: Scheduler,
    committer: BlockCommitterHandle<T>,
    /// Optional speculative-depth cap. None = no limit
    /// (single-composer / follower mode). See `DEFAULT_MAX_SPECULATIVE_DEPTH`.
    speculative_limit: Option<SpeculativeLimit>,
}

impl<T, ChainSpec> fmt::Debug for Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("committer", &self.committer)
            .field("speculative_limit", &self.speculative_limit)
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
        scheduler: Scheduler,
        payload_builder: PayloadBuilderHandle<T>,
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
            scheduler,
            committer,
            speculative_limit: None,
        })
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

    /// Clone-cheap handle to the underlying `BlockCommitter` actor.
    /// Other components (Deriver) push commands through the same task.
    #[must_use]
    pub fn committer(&self) -> BlockCommitterHandle<T> {
        self.committer.clone()
    }

    /// Runs the sequencer loop until cancellation. Errors during advance
    /// are logged and the loop continues; `committer_closed` should
    /// trigger an explicit shutdown (not yet wired).
    pub async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                req = self.scheduler.next() => {
                    if let Err(err) = self.advance(req).await {
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

    /// Greedy backfill loop. Each iteration commits one block at the
    /// next deterministic `parent.timestamp() + BLOCK_TIME_SECS` slot
    /// until the chain is within one block-time of the tick's
    /// wall-clock target. If the gap is large enough to exceed
    /// [`MAX_BLOCKS_PER_TICK`] in one tick, the loop yields and
    /// resumes on the next scheduler tick — catch-up isn't abandoned,
    /// just spread over multiple ticks so the run-loop stays
    /// responsive to canon-state notifications and FCU refreshes.
    async fn advance(&mut self, req: ProposalRequest) -> DriverResult<()> {
        let target_wall = req.target_timestamp;
        let mut produced: usize = 0;
        let mut stale_parent_retries: usize = 0;

        while produced < MAX_BLOCKS_PER_TICK {
            // Read the current canonical head from the committer per
            // iteration. The committer is the single source of truth
            // — Deriver-driven advances are visible here immediately,
            // with no `CanonStateNotification` broadcast lag.
            let last_header = self.committer.last_header();
            let parent_num = last_header.number();
            let parent_hash = last_header.hash();
            let parent_ts = last_header.timestamp();
            let gap = target_wall.saturating_sub(parent_ts);

            // Chain has reached (or passed) the tick's target — nothing
            // more to produce this tick.
            if gap < BLOCK_TIME_SECS {
                break;
            }

            // Speculative-depth limit: if we're already too far
            // ahead of the L1-confirmed cursor, pause and let the Deriver
            // catch up. Without this, the Sequencer races ahead during
            // a long timestamp-backfill and the Deriver's subsequent
            // reorgs displace blocks faster than reth's state-retention
            // window — eventually producing `no state found` on a deep
            // replay.
            if let Some(limit) = &self.speculative_limit {
                let confirmed = limit.source.confirmed_head();
                let speculative_depth = parent_num.saturating_sub(confirmed);
                if speculative_depth >= limit.max_depth {
                    event!(
                        name: "eez.sequencer.speculative.paused",
                        Level::DEBUG,
                        parent_num,
                        confirmed,
                        speculative_depth,
                        max_depth = limit.max_depth,
                        "paused: speculative depth at cap; waiting for Deriver to catch up",
                    );
                    break;
                }
            }

            let next_ts = parent_ts.saturating_add(BLOCK_TIME_SECS);
            let attrs = self.attributes.build(&last_header, next_ts);
            let timestamp = attrs.timestamp;
            let outcome = self
                .committer
                .commit_sequenced(parent_hash, parent_num, attrs)
                .await?;
            let outcome = match outcome {
                SequenceOutcome::Committed(outcome) => {
                    stale_parent_retries = 0;
                    outcome
                }
                SequenceOutcome::StaleParent {
                    expected_hash,
                    expected_number,
                    actual_hash,
                    actual_number,
                } => {
                    stale_parent_retries += 1;
                    event!(
                        name: "eez.sequencer.parent.stale",
                        Level::DEBUG,
                        expected_parent.number = expected_number,
                        expected_parent.hash = %expected_hash,
                        actual_parent.number = actual_number,
                        actual_parent.hash = %actual_hash,
                        "sequencer parent changed before commit; rebuilding attributes",
                    );
                    if stale_parent_retries >= MAX_STALE_PARENT_RETRIES_PER_TICK {
                        event!(
                            name: "eez.sequencer.parent.stale.yield",
                            Level::DEBUG,
                            retries = stale_parent_retries,
                            "stale-parent retry budget exhausted; yielding scheduler tick",
                        );
                        break;
                    }
                    continue;
                }
            };
            let block_number = outcome.header.number();
            let block_hash = outcome.header.hash();

            event!(
                name: "eez.sequencer.block.produced",
                Level::INFO,
                slot.kind = %req.kind,
                block.number = block_number,
                block.hash = %block_hash,
                block.timestamp = timestamp,
                block.is_filler = produced > 0,
                "produced block {{block.number}} hash={{block.hash}} ts={{block.timestamp}}",
            );

            produced += 1;
        }

        if produced == MAX_BLOCKS_PER_TICK {
            event!(
                name: "eez.sequencer.backfill.yield",
                Level::INFO,
                target_timestamp = target_wall,
                last_block_timestamp = self.committer.last_header().timestamp(),
                "hit per-tick block cap; continuing catch-up on next tick",
            );
        }
        Ok(())
    }
}
