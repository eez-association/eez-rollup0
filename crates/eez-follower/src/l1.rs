//! L1 watcher: tails `EEZ.BatchPosted` events, maps each batch's L1 inclusion
//! block to the cumulative L2 block count it represents, and publishes the
//! L1-derived safe/finalized L2 block numbers via a shared [`L1View`].
//!
//! `Follower` reads `L1View` on every FCU build to drive `safe` and
//! `finalized` block hashes. The watcher itself never touches the engine —
//! the two tasks communicate only through atomics in `L1View`.
//!
//! Trust model: we trust that L1 ran `IProofSystem.verify` before emitting
//! `BatchPosted`. We decode the posted payload only to learn how many L2
//! blocks the batch represents (the L1 contract doesn't surface this
//! directly — it's implicit in the payload's `blockTxCounts.len()`).

use std::collections::VecDeque;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, U256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_eth::{Filter, TransactionTrait};
use alloy_sol_types::{SolCall, SolEvent};
use eez_prover::EezRegistry;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};
use url::Url;

use crate::error::FollowerError;

/// How often the watcher polls L1 for new `BatchPosted` events and refreshed
/// `safe`/`finalized` tags. ≈ one Chiado block.
const TICK: Duration = Duration::from_secs(5);

/// Soft cap on retained `BatchRecord`s. Memory is tiny (~24 B each) and we
/// only need enough history to answer "what's the latest batch with L1 block
/// ≤ X" for X = current L1 safe/finalized tag — but a hard cap keeps memory
/// bounded if `safe`/`finalized` ever stop advancing.
const BATCH_HISTORY_CAP: usize = 4096;

/// Don't trim batches more recent than `finalized_l1 - FINALIZED_HYSTERESIS`,
/// in case an L1 client briefly reports a regressed `finalized` tag.
const FINALIZED_HYSTERESIS: u64 = 16;

/// Shared L1-derived state. Written by [`L1Watcher`], read by `Follower`.
///
/// `safe_l2_block` / `finalized_l2_block` hold the L2 block number whose
/// hash should populate the FCU triplet's `safe`/`finalized` slots. The
/// Follower looks up the hash itself via its local reth `BlockReader`.
///
/// Value `0` means "no batch has been seen yet" (the Follower collapses
/// to `head` in that case).
#[derive(Debug, Default)]
pub(crate) struct L1View {
    pub(crate) safe_l2_block: AtomicU64,
    pub(crate) finalized_l2_block: AtomicU64,
    pub(crate) last_l1_block_seen: AtomicU64,
}

/// One past `BatchPosted` event we've observed.
#[derive(Debug, Clone, Copy)]
struct BatchRecord {
    /// L1 block the batch landed in.
    l1_block: u64,
    /// Cumulative L2 block height the contract has accepted as of and
    /// including this batch.
    l2_block_end: u64,
}

/// Required L1 config, populated from env at startup.
#[derive(Debug, Clone)]
pub(crate) struct L1Config {
    pub rpc_url: Url,
    pub eez_address: Address,
    pub rollup_id: u64,
    pub deploy_block: u64,
}

impl L1Config {
    /// Read all required `EEZ_*` env vars. Fails loudly on any missing or
    /// malformed value — the follower won't run without L1.
    pub(crate) fn from_env() -> Result<Self, FollowerError> {
        let rpc_url = Url::parse(&require_env("EEZ_L1_RPC_URL")?)
            .map_err(|e| FollowerError::L1Config(format!("EEZ_L1_RPC_URL: {e}")))?;
        let eez_address = Address::from_str(&require_env("EEZ_REGISTRY_ADDRESS")?)
            .map_err(|e| FollowerError::L1Config(format!("EEZ_REGISTRY_ADDRESS: {e}")))?;
        let rollup_id = require_env("EEZ_ROLLUP_ID")?
            .parse::<u64>()
            .map_err(|e| FollowerError::L1Config(format!("EEZ_ROLLUP_ID: {e}")))?;
        let deploy_block = require_env("EEZ_REGISTRY_DEPLOY_BLOCK")?
            .parse::<u64>()
            .map_err(|e| FollowerError::L1Config(format!("EEZ_REGISTRY_DEPLOY_BLOCK: {e}")))?;
        Ok(Self {
            rpc_url,
            eez_address,
            rollup_id,
            deploy_block,
        })
    }
}

fn require_env(name: &str) -> Result<String, FollowerError> {
    env::var(name).map_err(|_| FollowerError::L1Config(format!("{name} is required")))
}

/// L1 watcher task — tails `BatchPosted` events and publishes L1-derived
/// safe/finalized into `L1View`.
#[derive(Debug)]
pub(crate) struct L1Watcher {
    config: L1Config,
    l1_rpc: RootProvider,
    /// Last L1 block we've fully ingested. The next tick scans
    /// `[cursor + 1 ..= latest]`.
    cursor: u64,
    /// Past batches that target our rollup, in L1-block order.
    batches: VecDeque<BatchRecord>,
    view: Arc<L1View>,
}

impl L1Watcher {
    /// Build the watcher and run the one-shot bootstrap scan.
    pub(crate) async fn new(config: L1Config, view: Arc<L1View>) -> Result<Self, FollowerError> {
        let l1_rpc = RootProvider::new_http(config.rpc_url.clone());
        let mut this = Self {
            config,
            l1_rpc,
            cursor: 0,
            batches: VecDeque::with_capacity(BATCH_HISTORY_CAP.min(256)),
            view,
        };
        this.bootstrap().await?;
        Ok(this)
    }

    async fn bootstrap(&mut self) -> Result<(), FollowerError> {
        let latest = self
            .l1_rpc
            .get_block_number()
            .await
            .map_err(|e| FollowerError::L1Rpc(format!("get_block_number: {e}")))?;
        self.ingest_logs(self.config.deploy_block, latest).await?;
        self.refresh_view().await?;
        self.cursor = latest;
        self.view
            .last_l1_block_seen
            .store(latest, Ordering::Release);
        let batches = self.batches.len();
        let safe_l2 = self.view.safe_l2_block.load(Ordering::Acquire);
        let finalized_l2 = self.view.finalized_l2_block.load(Ordering::Acquire);
        event!(
            name: "eez.follower.l1.bootstrap.complete",
            Level::INFO,
            batches,
            l1_block = latest,
            safe_l2,
            finalized_l2,
            "L1 bootstrap complete: {{batches}} past batches, safe_l2={{safe_l2}} finalized_l2={{finalized_l2}}",
        );
        Ok(())
    }

    pub(crate) async fn run(mut self) {
        let mut ticker = interval_at(Instant::now() + TICK, TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = self.tick().await {
                event!(
                    name: "eez.follower.l1.tick.failed",
                    Level::WARN,
                    error = %err,
                    "L1 watcher tick failed; will retry next tick",
                );
            }
        }
    }

    async fn tick(&mut self) -> Result<(), FollowerError> {
        let latest = self
            .l1_rpc
            .get_block_number()
            .await
            .map_err(|e| FollowerError::L1Rpc(format!("get_block_number: {e}")))?;
        if latest > self.cursor {
            self.ingest_logs(self.cursor + 1, latest).await?;
            self.cursor = latest;
            self.view
                .last_l1_block_seen
                .store(latest, Ordering::Release);
        }
        self.refresh_view().await?;
        Ok(())
    }

    /// Walk `BatchPosted` events in `[from..=to]` and append `BatchRecord`s
    /// for those that target our rollup. Skips batches with malformed
    /// payloads (logged, not fatal).
    async fn ingest_logs(&mut self, from: u64, to: u64) -> Result<(), FollowerError> {
        let filter = Filter::new()
            .address(self.config.eez_address)
            .event_signature(EezRegistry::BatchPosted::SIGNATURE_HASH)
            .from_block(from)
            .to_block(to);
        let logs = self
            .l1_rpc
            .get_logs(&filter)
            .await
            .map_err(|e| FollowerError::L1Rpc(format!("get_logs: {e}")))?;
        for log in &logs {
            let l1_block = log
                .block_number
                .ok_or_else(|| FollowerError::L1Rpc("log missing block_number".into()))?;
            let tx_hash = log
                .transaction_hash
                .ok_or_else(|| FollowerError::L1Rpc("log missing transaction_hash".into()))?;
            let tx = self
                .l1_rpc
                .get_transaction_by_hash(tx_hash)
                .await
                .map_err(|e| {
                    FollowerError::L1Rpc(format!("get_transaction_by_hash({tx_hash}): {e}"))
                })?
                .ok_or_else(|| FollowerError::L1Rpc(format!("tx {tx_hash} not found")))?;
            let input = tx.inner.input();
            let decoded = EezRegistry::postAndVerifyBatchCall::abi_decode(input).map_err(|e| {
                FollowerError::L1Rpc(format!("decode postBatch({tx_hash}): {e}"))
            })?;
            // Filter to batches that target our rollup. Indexed event topic
            // is only `rollupCount` (registry-wide), so we have to decode the
            // tx to check.
            let ours = decoded
                .batch
                .rollupIdsWithProofSystems
                .iter()
                .any(|r| r.rollupId == U256::from(self.config.rollup_id));
            if !ours {
                event!(
                    name: "eez.follower.l1.batch.skipped.not_ours",
                    Level::DEBUG,
                    tx_hash = %tx_hash,
                    "BatchPosted is for a different rollup; skipping",
                );
                continue;
            }
            let batch = match eez_payload_codec::decode(decoded.batch.callData.as_ref()) {
                Ok(b) => b,
                Err(e) => {
                    event!(
                        name: "eez.follower.l1.batch.skipped.bad_payload",
                        Level::WARN,
                        tx_hash = %tx_hash,
                        error = %e,
                        "BatchPosted payload failed to decode; skipping",
                    );
                    continue;
                }
            };
            let block_count = batch.block_count() as u64;
            let prev_end = self.batches.back().map_or(0, |r| r.l2_block_end);
            let l2_block_end = prev_end + block_count;
            self.batches.push_back(BatchRecord {
                l1_block,
                l2_block_end,
            });
            event!(
                name: "eez.follower.l1.batch.observed",
                Level::INFO,
                tx_hash = %tx_hash,
                l1_block,
                block_count,
                l2_block_end,
                "observed batch at L1 block {{l1_block}}: +{{block_count}} L2 blocks, cumulative end {{l2_block_end}}",
            );
        }
        Ok(())
    }

    /// Query L1's safe/finalized block numbers, resolve to L2 block numbers
    /// via `batches`, update `L1View` atomics, and trim history.
    async fn refresh_view(&mut self) -> Result<(), FollowerError> {
        let safe_l1 = self.fetch_tag(BlockNumberOrTag::Safe).await?;
        let finalized_l1 = self.fetch_tag(BlockNumberOrTag::Finalized).await?;

        let prior_safe = self.view.safe_l2_block.load(Ordering::Acquire);
        let prior_finalized = self.view.finalized_l2_block.load(Ordering::Acquire);

        // `.max(prior)` enforces monotonicity even if our history gets trimmed
        // out from under us. L1 reorgs that move `safe`/`finalized` backwards
        // are out of scope for this PR (handled by stage-3 reorg work).
        let safe_l2 = self.l2_at_or_before(safe_l1).unwrap_or(0).max(prior_safe);
        let finalized_l2 = self
            .l2_at_or_before(finalized_l1)
            .unwrap_or(0)
            .max(prior_finalized);

        if safe_l2 != prior_safe {
            self.view.safe_l2_block.store(safe_l2, Ordering::Release);
            event!(
                name: "eez.follower.l1.safe.advanced",
                Level::INFO,
                from = prior_safe, to = safe_l2, l1_safe = safe_l1,
                "L1-derived safe L2 head advanced",
            );
        }
        if finalized_l2 != prior_finalized {
            self.view
                .finalized_l2_block
                .store(finalized_l2, Ordering::Release);
            event!(
                name: "eez.follower.l1.finalized.advanced",
                Level::INFO,
                from = prior_finalized, to = finalized_l2, l1_finalized = finalized_l1,
                "L1-derived finalized L2 head advanced",
            );
        }

        let trim_below = finalized_l1.saturating_sub(FINALIZED_HYSTERESIS);
        while let Some(front) = self.batches.front() {
            if front.l1_block < trim_below {
                self.batches.pop_front();
            } else {
                break;
            }
        }
        while self.batches.len() > BATCH_HISTORY_CAP {
            self.batches.pop_front();
        }
        Ok(())
    }

    async fn fetch_tag(&self, tag: BlockNumberOrTag) -> Result<u64, FollowerError> {
        let block = self
            .l1_rpc
            .get_block_by_number(tag)
            .await
            .map_err(|e| FollowerError::L1Rpc(format!("get_block_by_number({tag:?}): {e}")))?
            .ok_or_else(|| FollowerError::L1Rpc(format!("{tag:?} block not found")))?;
        Ok(block.header.number)
    }

    /// Latest batch whose L1 inclusion block is `<= l1`. `None` if no such
    /// batch is in our history.
    fn l2_at_or_before(&self, l1: u64) -> Option<u64> {
        self.batches
            .iter()
            .rev()
            .find(|r| r.l1_block <= l1)
            .map(|r| r.l2_block_end)
    }
}
