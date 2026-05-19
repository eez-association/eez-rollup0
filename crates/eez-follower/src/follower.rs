//! Follower task: tracks the sequencer's chain by querying its JSON-RPC for
//! the current head hash and driving local reth via `forkchoiceUpdated`.
//!
//! Block data flows over reth's existing devp2p (eth/68) — the FCU points at
//! a head reth doesn't yet have, reth enters `SYNCING`, pipeline sync pulls
//! the missing blocks from peers (including the sequencer), and the FCU
//! eventually resolves to `VALID`. Catch-up after downtime works the same
//! way: the first FCU at startup kicks off backfill from genesis (or
//! whatever the local best block is) to the current head.
//!
//! The sequencer JSON-RPC carries only the 32-byte head hash per tick. Every
//! byte of block data, transactions, and receipts flows over P2P.
//!
//! Per-tick algorithm:
//!
//! 1. `eth_getBlockByNumber(latest)` against the sequencer — the sequencer is
//!    authoritative for "current head", so we don't try to derive a slot
//!    locally. (Wall-clock-derived slot math breaks on dev chains, where
//!    `genesis.timestamp = 0` but the sequencer's first produced block has
//!    `timestamp ≈ now`.)
//! 2. If `null` (sequencer has no blocks yet) — skip, retry next tick.
//! 3. If the hash matches what we already FCU'd last tick — refresh and skip
//!    the push so the deque doesn't accumulate duplicates.
//! 4. Push the hash to `recent_hashes`, build the forkchoice triplet
//!    (`head`, `head - 32`, `head - 64`), and call `fork_choice_updated`.
//! 5. `VALID` → head advanced. `SYNCING` → reth still backfilling (benign).
//!    `INVALID` → log error, drop the hash so the next tick can retry.
//!
//! The forkchoice math (`SAFE_LAG`, `FINALIZED_LAG`, `FORKCHOICE_HISTORY`)
//! intentionally duplicates `eez-driver`'s `Sequencer` rather than refactor
//! into a shared abstraction. Extraction can wait until a second consumer
//! signals real drift.

use core::fmt;
use std::{collections::VecDeque, time::Duration};

use alloy_primitives::B256;
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_engine::ForkchoiceState;
use alloy_rpc_types_eth::BlockNumberOrTag;
use eez_driver::Scheduler;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_payload_primitives::PayloadTypes;
use tracing::{Level, event};

use crate::error::FollowerError;

/// Mirrors `eez_driver::Sequencer`'s forkchoice tracking constants. See
/// `crates/eez-driver/src/sequencer.rs:44-46` for the rationale (2-minute
/// history at 2s block time; safe/finalized lags chosen so those pointers
/// don't move every block).
const FORKCHOICE_HISTORY: usize = 64;
const SAFE_LAG: usize = 32;
const FINALIZED_LAG: usize = 64;

/// Keepalive cadence — re-publish the current forkchoice state so reth's
/// engine view doesn't drift during quiet periods. Mirrors sequencer.rs:53.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Drives a reth node by translating per-tick sequencer head-pointer lookups
/// into engine-API `forkchoiceUpdated` calls. The actual block data is
/// transferred by reth's P2P stack as a side-effect of FCU.
pub(crate) struct Follower<T>
where
    T: PayloadTypes,
{
    to_engine: ConsensusEngineHandle<T>,
    rpc: RootProvider,
    scheduler: Scheduler,
    recent_hashes: VecDeque<B256>,
}

impl<T> fmt::Debug for Follower<T>
where
    T: PayloadTypes,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Follower")
            .field("head", &self.recent_hashes.back())
            .field("recent_hashes", &self.recent_hashes.len())
            .finish_non_exhaustive()
    }
}

impl<T> Follower<T>
where
    T: PayloadTypes,
{
    /// Constructs a follower bound to the given engine handle and sequencer
    /// RPC client.
    pub(crate) fn new(
        to_engine: ConsensusEngineHandle<T>,
        rpc: RootProvider,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            to_engine,
            rpc,
            scheduler,
            recent_hashes: VecDeque::with_capacity(FORKCHOICE_HISTORY),
        }
    }

    /// Runs the follower loop until cancellation.
    ///
    /// Per-tick errors are logged and the loop continues — mirrors the
    /// sequencer's containment pattern. State is only mutated on the
    /// success path so a failed advance leaves the local view untouched
    /// for the next retry.
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

    /// Builds the forkchoice triplet from `recent_hashes`. Returns `None`
    /// before the first successful per-tick advance — there is no head to
    /// advertise yet.
    fn forkchoice_state(&self) -> Option<ForkchoiceState> {
        let head = *self.recent_hashes.back()?;
        let safe = *self
            .recent_hashes
            .get(self.recent_hashes.len().saturating_sub(SAFE_LAG))
            .expect("len >= 1 keeps index valid under saturating_sub");
        let finalized = *self
            .recent_hashes
            .get(self.recent_hashes.len().saturating_sub(FINALIZED_LAG))
            .expect("len >= 1 keeps index valid under saturating_sub");
        Some(ForkchoiceState {
            head_block_hash: head,
            safe_block_hash: safe,
            finalized_block_hash: finalized,
        })
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
        // SYNCING is a normal state while reth backfills via P2P. Only
        // explicit INVALID should surface as an error here.
        if res.is_invalid() {
            return Err(FollowerError::InvalidForkchoice(format!("{res:?}")));
        }
        Ok(())
    }

    async fn advance(&mut self) -> Result<(), FollowerError> {
        let block = self
            .rpc
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

        // If this is a duplicate of what we already FCU'd last tick (sequencer
        // hasn't produced a new block), just refresh — don't grow the deque
        // with duplicates.
        if self.recent_hashes.back() == Some(&hash) {
            return self.refresh_forkchoice().await;
        }

        self.recent_hashes.push_back(hash);
        if self.recent_hashes.len() > FORKCHOICE_HISTORY {
            self.recent_hashes.pop_front();
        }

        let state = self.forkchoice_state().expect("just pushed a hash");

        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| FollowerError::EngineRpc(e.to_string()))?;

        if res.is_invalid() {
            // Sequencer's hash was rejected by our local reth. Drop it so
            // the next tick can retry with whatever the sequencer reports
            // then (possibly the same hash if it was a transient reth
            // state issue, or a new hash if there was a reorg).
            self.recent_hashes.pop_back();
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
