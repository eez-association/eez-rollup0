//! L1 watcher: tails `EEZ.BatchPosted` events, maps each batch's L1 inclusion
//! block to the cumulative L2 block count it represents, verifies the
//! sequencer-served L2 chain matches what L1 committed to (tx-equivalence),
//! and publishes the L1-derived safe/finalized L2 block numbers via a
//! shared [`L1View`].
//!
//! `Follower` reads `L1View` on every FCU build to drive `safe` and
//! `finalized` block hashes. The watcher itself never touches the engine —
//! the two tasks communicate only through atomics in `L1View`.
//!
//! Trust model: we trust L1's own `IProofSystem.verify` to gate
//! `BatchPosted` emission. *On top of that*, we tx-verify each batch's
//! decoded payload against local reth before allowing the corresponding
//! L2 block number into `L1View::safe_l2_block` / `finalized_l2_block`.
//! That closes the equivocation gap between "what the sequencer posted
//! to L1" and "what the sequencer serves to our reth P2P" — if they
//! disagree, we halt the follower instead of canonicalizing a chain L1
//! didn't sign off on.

use std::collections::VecDeque;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_eips::{BlockNumberOrTag, Encodable2718};
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_eth::{Filter, TransactionTrait};
use alloy_sol_types::{SolCall, SolEvent};
use eez_prover::EezRegistry;
use reth_primitives_traits::{Block, BlockBody};
use reth_storage_api::{BlockReader, TransactionsProvider};
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
///
/// Carries per-L2-block tx-hash expectations decoded from the batch's
/// `callData`. The verifier compares these against local reth and clears
/// the vector once the batch is fully verified, so steady-state memory is
/// just the two `u64`s.
#[derive(Debug, Clone)]
struct BatchRecord {
    /// L1 block the batch landed in.
    l1_block: u64,
    /// Cumulative L2 block height the contract has accepted as of and
    /// including this batch.
    l2_block_end: u64,
    /// Per-L2-block tx-hash expectations decoded from this batch's
    /// `callData`. Drained as the verifier matches each block against
    /// local reth. Empty once the batch is fully verified.
    expectations: Vec<BlockExpectation>,
}

/// What the L1-committed batch says block `l2_block_num` must contain.
#[derive(Debug, Clone)]
struct BlockExpectation {
    l2_block_num: u64,
    /// EIP-2718 envelope hashes of the user txs in this block, in
    /// block-internal order. Compared positionally against the same hash
    /// computed from local reth's `block_by_number(l2_block_num)`.
    tx_hashes: Vec<B256>,
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

/// L1 watcher task — tails `BatchPosted` events, verifies each batch's
/// txs match local reth, and publishes L1-derived safe/finalized into
/// `L1View`.
#[derive(Debug)]
pub(crate) struct L1Watcher<P: BlockReader> {
    config: L1Config,
    l1_rpc: RootProvider,
    /// Last L1 block we've fully ingested. The next tick scans
    /// `[cursor + 1 ..= latest]`.
    cursor: u64,
    /// Past batches that target our rollup, in L1-block order.
    batches: VecDeque<BatchRecord>,
    view: Arc<L1View>,
    /// Local reth provider, used by the verifier to look up the txs the
    /// sequencer actually served us for each L2 block number L1 committed to.
    l2_provider: Arc<P>,
    /// Highest L2 block number whose tx contents we've matched against an
    /// L1-committed batch. The atomic safe/finalized writes are capped by
    /// this value, so we never advertise a hash for a block we haven't
    /// verified — even if L1 has already finalized the batch covering it.
    verified_through_l2_block: u64,
    /// Set after a tick fails; cleared on first success. Used to demote
    /// repeat L1-outage warnings to DEBUG so a multi-minute outage doesn't
    /// drown the log.
    last_tick_failed: bool,
}

impl<P> L1Watcher<P>
where
    P: BlockReader + Send + Sync + 'static,
    <P as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Build the watcher and run the one-shot bootstrap scan.
    pub(crate) async fn new(
        config: L1Config,
        view: Arc<L1View>,
        l2_provider: Arc<P>,
    ) -> Result<Self, FollowerError> {
        let l1_rpc = RootProvider::new_http(config.rpc_url.clone());
        let mut this = Self {
            config,
            l1_rpc,
            cursor: 0,
            batches: VecDeque::with_capacity(256),
            view,
            l2_provider,
            verified_through_l2_block: 0,
            last_tick_failed: false,
        };
        this.bootstrap().await?;
        Ok(this)
    }

    async fn bootstrap(&mut self) -> Result<(), FollowerError> {
        // Verify the configured rollup_id is actually registered on the EEZ
        // before doing anything else. `rollupCounter` is the auto-generated
        // getter on EEZ.sol's `uint256 public rollupCounter` — registered
        // ids are 1..=counter. Failing here saves the historical event scan
        // and gives the operator a precise error instead of a silent
        // "follower runs but never observes anything for us".
        let registry = EezRegistry::new(self.config.eez_address, &self.l1_rpc);
        let counter = registry
            .rollupCounter()
            .call()
            .await
            .map_err(|e| FollowerError::L1Rpc(format!("rollupCounter: {e}")))?;
        if self.config.rollup_id == 0 || U256::from(self.config.rollup_id) > counter {
            return Err(FollowerError::L1Config(format!(
                "rollup_id={} not registered on EEZ at {} (rollupCounter={counter})",
                self.config.rollup_id, self.config.eez_address,
            )));
        }

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
            match self.tick().await {
                Ok(()) => {
                    if self.last_tick_failed {
                        event!(
                            name: "eez.follower.l1.recovered",
                            Level::WARN,
                            "L1 watcher recovered after prior tick failures",
                        );
                        self.last_tick_failed = false;
                    }
                }
                Err(err)
                    if err.is_permanent_batch_failure() || err.is_permanent_chain_failure() =>
                {
                    // Halt the watcher. Either a batch is unrecoverably
                    // malformed (cumulative-count tracking would drift) or
                    // the sequencer-served chain disagrees with what L1
                    // committed to (equivocation). Both are operator-only
                    // recoveries; safe/finalized stall at last-verified.
                    event!(
                        name: "eez.follower.l1.halted",
                        Level::ERROR,
                        error = %err,
                        "L1 watcher halting: permanent failure (safe/finalized will no longer advance)",
                    );
                    return;
                }
                Err(err) => {
                    if self.last_tick_failed {
                        event!(
                            name: "eez.follower.l1.tick.failed",
                            Level::DEBUG,
                            error = %err,
                            "L1 watcher tick failed; will retry next tick",
                        );
                    } else {
                        event!(
                            name: "eez.follower.l1.tick.failed",
                            Level::WARN,
                            error = %err,
                            "L1 watcher tick failed; subsequent failures will log at DEBUG until recovery",
                        );
                        self.last_tick_failed = true;
                    }
                }
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
        self.verify_pending()?;
        self.refresh_view().await?;
        Ok(())
    }

    /// Walk pending block expectations against local reth. Advances
    /// `verified_through_l2_block`; halts the watcher (via `InvalidChain`)
    /// if any L1-committed block disagrees with what the sequencer served
    /// our P2P. If local reth hasn't synced a pending block yet, we stop
    /// and retry on the next tick.
    ///
    /// TODO(reorgs): this implementation assumes both L1 and L2 history
    /// are append-only beyond this watcher's lifetime.
    ///
    /// - On an L1 reorg that drops a `BatchPosted` we already ingested,
    ///   our in-memory expectations become stale and the verifier could
    ///   halt on a sequencer chain that's actually correct on the new
    ///   canonical L1. Fix: pin each `BatchRecord` to its L1 block hash;
    ///   on tick, re-fetch the hash for any unfinalized batch and rewind
    ///   `cursor` + `verified_through_l2_block` on mismatch.
    /// - On an L2 reorg that replaces a block at or below
    ///   `verified_through_l2_block`, our "verified" status outlives the
    ///   block it was attached to. Fix: degraded mode — rewind FCU to
    ///   `head = safe = last_verified_safe_hash` and wait for the
    ///   L1-canonical chain to re-arrive over P2P.
    fn verify_pending(&mut self) -> Result<(), FollowerError> {
        for record in &mut self.batches {
            while let Some(expected) = record.expectations.first() {
                let n = expected.l2_block_num;
                let block = match self.l2_provider.block_by_number(n) {
                    Ok(Some(b)) => b,
                    Ok(None) => return Ok(()),
                    Err(e) => {
                        return Err(FollowerError::L2Provider(format!(
                            "block_by_number({n}): {e}",
                        )));
                    }
                };

                let local_hashes: Vec<B256> = block
                    .body()
                    .transactions()
                    .iter()
                    .map(|tx| keccak256(tx.encoded_2718()))
                    .collect();

                if local_hashes != expected.tx_hashes {
                    // Best-effort hash for the error payload; `block_hash`
                    // is a cheap lookup and the verifier has already
                    // succeeded in fetching the block above.
                    let block_hash = self
                        .l2_provider
                        .block_hash(n)
                        .ok()
                        .flatten()
                        .unwrap_or(B256::ZERO);
                    let detail = format!(
                        "L1-committed batch has {} tx(s) at L2 block {}, sequencer-served block has {} tx(s); positional hash compare failed",
                        expected.tx_hashes.len(),
                        n,
                        local_hashes.len(),
                    );
                    event!(
                        name: "eez.follower.l1.verify.mismatch",
                        Level::ERROR,
                        l2_block = n,
                        block_hash = %block_hash,
                        "sequencer-served L2 block disagrees with L1-committed batch — halting",
                    );
                    return Err(FollowerError::InvalidChain {
                        block_number: n,
                        block_hash,
                        detail,
                    });
                }

                self.verified_through_l2_block = n;
                record.expectations.remove(0);
            }
        }
        Ok(())
    }

    /// Walk `BatchPosted` events in `[from..=to]` and append `BatchRecord`s
    /// for those that target our rollup.
    ///
    /// Decode failures (`postAndVerifyBatchCall` ABI mismatch, or
    /// `eez_payload_codec::decode`) raise [`FollowerError::BatchMalformed`]
    /// — the watcher's `run` loop classifies that as a permanent halt
    /// rather than retrying. Silently skipping would drift our cumulative
    /// L2-block sum below the L1 contract's accepted height permanently.
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
                FollowerError::BatchMalformed {
                    l1_block,
                    tx_hash,
                    detail: format!("postAndVerifyBatch ABI decode: {e}"),
                }
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
            let batch = eez_payload_codec::decode(decoded.batch.callData.as_ref()).map_err(
                |e| FollowerError::BatchMalformed {
                    l1_block,
                    tx_hash,
                    detail: format!("payload codec: {e}"),
                },
            )?;
            let block_count = batch.block_count() as u64;
            let prev_end = self.batches.back().map_or(0, |r| r.l2_block_end);
            let l2_block_end = prev_end + block_count;
            let expectations = expectations_from_decoded(prev_end, &batch);
            self.batches.push_back(BatchRecord {
                l1_block,
                l2_block_end,
                expectations,
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

        // Cap both pointers at `verified_through_l2_block`: never advertise
        // a safe/finalized hash for an L2 block whose txs we haven't matched
        // against an L1-committed batch yet. `.max(prior)` then enforces
        // monotonicity even if our history gets trimmed out from under us.
        // L1 reorgs that move `safe`/`finalized` backwards are out of scope
        // for this PR (handled by stage-3 reorg work).
        let cap = self.verified_through_l2_block;
        let safe_l2 = self
            .l2_at_or_before(safe_l1)
            .unwrap_or(0)
            .min(cap)
            .max(prior_safe);
        let finalized_l2 = self
            .l2_at_or_before(finalized_l1)
            .unwrap_or(0)
            .min(cap)
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

        // Trim verified, L1-finalized batches. The verified-guard prevents
        // dropping a record whose expectations the verifier hasn't drained
        // yet — without it, a sufficiently old unverified batch (e.g.,
        // reth slow to backfill) could silently disappear and let a wrong
        // block slip past as "verified" by omission.
        let trim_below = finalized_l1.saturating_sub(FINALIZED_HYSTERESIS);
        while let Some(front) = self.batches.front() {
            if front.l1_block < trim_below && front.expectations.is_empty() {
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

/// Slice the codec's flat `(block_tx_counts, transactions)` into per-L2-block
/// expectations starting at `prev_end + 1`. Transactions are EIP-2718
/// envelopes; we hash each with `keccak256` for positional comparison
/// against `keccak256(tx.encoded_2718())` over local reth's block body.
fn expectations_from_decoded(
    prev_end: u64,
    batch: &eez_payload_codec::DecodedBatch,
) -> Vec<BlockExpectation> {
    let mut out = Vec::with_capacity(batch.block_count());
    let mut cursor = 0usize;
    for (i, count) in batch.block_tx_counts.iter().enumerate() {
        let n_txs = usize::from(*count);
        let slice = &batch.transactions[cursor..cursor + n_txs];
        cursor += n_txs;
        out.push(BlockExpectation {
            l2_block_num: prev_end + (i as u64) + 1,
            tx_hashes: slice.iter().map(keccak256).collect(),
        });
    }
    out
}
