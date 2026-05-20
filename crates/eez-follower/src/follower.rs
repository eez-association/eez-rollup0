//! Follower task: tracks the sequencer's chain by querying its JSON-RPC for
//! the current head hash and driving local reth via `forkchoiceUpdated`.
//!
//! Per-tick algorithm:
//!
//! 1. `eth_getBlockByNumber(latest)` against the sequencer — the sequencer is
//!    authoritative for "current head", so we don't try to derive a slot
//!    locally.
//! 2. If `null` (sequencer has no blocks yet) — skip, retry next tick.
//! 3. If the hash matches what we already FCU'd last tick — refresh and skip.
//! 4. Build the FCU triplet from L1-derived safe/finalized in [`L1View`],
//!    resolving each L2 block number to a hash via local reth.
//! 5. `VALID` → head advanced. `SYNCING` → reth still backfilling (benign).
//!    `INVALID` → log error, forget the hash so the next tick can retry.
//!
//! The safe/finalized pointers come from a separate `L1Watcher` task that
//! tails L1 `BatchPosted` events and L1's own `safe`/`finalized` block tags.
//! See `l1.rs`. Until the first batch lands on L1, `safe` and `finalized`
//! collapse to `head` — same shape reth sees at sequencer startup.

use core::fmt;
use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::ForkchoiceState;
use eez_driver::Scheduler;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_payload_primitives::PayloadTypes;
use reth_storage_api::BlockReader;
use tracing::{Level, event};

use crate::error::FollowerError;
use crate::l1::L1View;

/// Keepalive cadence — re-publish the current forkchoice state so reth's
/// engine view doesn't drift during quiet periods. Mirrors
/// `eez-driver`'s `Sequencer` at `crates/eez-driver/src/sequencer.rs:53`.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Drives a reth node by translating per-tick sequencer head-pointer lookups
/// into engine-API `forkchoiceUpdated` calls. The unsafe `head` comes from
/// the sequencer RPC; `safe` and `finalized` come from a shared [`L1View`]
/// populated by the L1 watcher task.
pub(crate) struct Follower<T, P>
where
    T: PayloadTypes,
    P: BlockReader,
{
    to_engine: ConsensusEngineHandle<T>,
    sequencer_rpc: RootProvider,
    scheduler: Scheduler,
    /// Local reth provider, used to look up L2 block hashes for the
    /// `safe`/`finalized` numbers published by the L1 watcher.
    l2_provider: Arc<P>,
    /// L1-derived safe/finalized L2 block numbers. Shared with the L1
    /// watcher task; we only read.
    l1_view: Arc<L1View>,
    /// Last head hash we successfully FCU'd. `None` before the first
    /// successful sequencer poll.
    last_head: Option<B256>,
}

impl<T, P> fmt::Debug for Follower<T, P>
where
    T: PayloadTypes,
    P: BlockReader,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Follower")
            .field("last_head", &self.last_head)
            .field("safe_l2", &self.l1_view.safe_l2_block.load(Ordering::Acquire))
            .field(
                "finalized_l2",
                &self.l1_view.finalized_l2_block.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl<T, P> Follower<T, P>
where
    T: PayloadTypes,
    P: BlockReader + Send + Sync + 'static,
{
    pub(crate) fn new(
        to_engine: ConsensusEngineHandle<T>,
        sequencer_rpc: RootProvider,
        scheduler: Scheduler,
        l2_provider: Arc<P>,
        l1_view: Arc<L1View>,
    ) -> Self {
        Self {
            to_engine,
            sequencer_rpc,
            scheduler,
            l2_provider,
            l1_view,
            last_head: None,
        }
    }

    /// Runs the follower loop until cancellation.
    ///
    /// Per-tick errors are logged and the loop continues — mirrors the
    /// sequencer's containment pattern. `last_head` is only mutated on
    /// the success path so a failed advance leaves the local view
    /// untouched for the next retry.
    pub(crate) async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                _ = self.scheduler.next() => {
                    if let Err(err) = self.advance().await {
                        event!(
                            name: "eez.follower.advance.failed",
                            Level::ERROR,
                            error = %err,
                            "advance failed: {{error}}",
                        );
                    }
                }
                _ = fcu_interval.tick() => {
                    if let Err(err) = self.refresh_forkchoice().await {
                        event!(
                            name: "eez.follower.fcu.failed",
                            Level::WARN,
                            error = %err,
                            "forkchoice refresh failed: {{error}}",
                        );
                    }
                }
            }
        }
    }

    /// Builds the FCU triplet. `head` from `last_head`; `safe`/`finalized`
    /// from `L1View` resolved against local reth. Collapses missing
    /// pointers to `head` so reth always receives a valid triplet.
    ///
    /// Returns `None` only before we've ever seen the sequencer.
    fn forkchoice_state(&self) -> Option<ForkchoiceState> {
        let head = self.last_head?;
        let safe_l2 = self.l1_view.safe_l2_block.load(Ordering::Acquire);
        let finalized_l2 = self.l1_view.finalized_l2_block.load(Ordering::Acquire);
        let safe = self.lookup_l2_hash(safe_l2).unwrap_or(head);
        let finalized = self.lookup_l2_hash(finalized_l2).unwrap_or(head);
        Some(ForkchoiceState {
            head_block_hash: head,
            safe_block_hash: safe,
            finalized_block_hash: finalized,
        })
    }

    /// Returns `Some(hash)` if local reth has block `num` and `num > 0`;
    /// `None` otherwise (caller collapses to `head`).
    fn lookup_l2_hash(&self, num: u64) -> Option<B256> {
        if num == 0 {
            return None;
        }
        self.l2_provider.block_hash(num).ok().flatten()
    }

    async fn refresh_forkchoice(&self) -> Result<(), FollowerError> {
        let Some(state) = self.forkchoice_state() else {
            return Ok(());
        };
        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| FollowerError::EngineRpc(e.to_string()))?;
        if res.is_invalid() {
            return Err(FollowerError::InvalidForkchoice(format!("{res:?}")));
        }
        Ok(())
    }

    async fn advance(&mut self) -> Result<(), FollowerError> {
        let block = self
            .sequencer_rpc
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await
            .map_err(|e| FollowerError::Rpc(e.to_string()))?;
        let Some(block) = block else {
            event!(
                name: "eez.follower.block.not_yet_produced",
                Level::DEBUG,
                "sequencer reports no latest block yet",
            );
            return Ok(());
        };

        let number = block.header.number;
        let hash = block.header.hash;

        // Duplicate of last tick? Just refresh — sequencer hasn't produced
        // a new block yet, but we want to keep reth's engine view fresh.
        if self.last_head == Some(hash) {
            return self.refresh_forkchoice().await;
        }

        // Tentatively adopt the new head so forkchoice_state() can build a
        // triplet, then roll back on INVALID.
        let prior_head = self.last_head;
        self.last_head = Some(hash);
        let state = self
            .forkchoice_state()
            .expect("last_head was just set");

        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| FollowerError::EngineRpc(e.to_string()))?;

        if res.is_invalid() {
            // Reth rejected this hash. Restore prior head so the next tick
            // retries cleanly (possibly with the same hash if it was a
            // transient reth state issue, or a new hash if a reorg happened).
            self.last_head = prior_head;
            return Err(FollowerError::InvalidForkchoice(format!("{res:?}")));
        }

        if res.is_valid() {
            event!(
                name: "eez.follower.head.advanced",
                Level::INFO,
                block.number = number,
                block.hash = %hash,
                "follower advanced to block {{block.number}} hash={{block.hash}}",
            );
        } else {
            event!(
                name: "eez.follower.head.syncing",
                Level::INFO,
                block.number = number,
                block.hash = %hash,
                "follower FCU returned SYNCING; reth backfilling via P2P to block {{block.number}} hash={{block.hash}}",
            );
        }

        Ok(())
    }
}
