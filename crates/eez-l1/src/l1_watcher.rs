//! Polls L1 for new heads, `BatchPosted` events, and finalized-head
//! advances. Emits [`L1Event`]s on a [`broadcast::Sender`] for the
//! Deriver and Composer to consume.
//!
//! The watcher never picks its own baseline: [`L1Watcher::polling`] takes
//! a block the caller already scanned (the Deriver's finalized-clamped
//! catch-up endpoint) and seeds the canonical ring with it. Every poll
//! path scans `BatchPosted` logs before advancing the ring forward or
//! emitting heads, so a failed cycle retries the identical range next
//! tick. A reorg retreat (Reorg + rewind) still precedes its scan.
//!
//! HTTP polling (reuses the `EEZ_L1_RPC_URL` read by `L1ReaderConfig`) — 2s interval, well
//! inside gnosis 5s / Ethereum mainnet 12s block times.
//!
//! Reorg detection walks back via `parent_hash` against a ring of recent
//! canonical hashes (length ≤ `reorg_max_depth`). Past
//! `reorg_max_depth`: halt with [`L1Error::ReorgTooDeep`]. Multi-block
//! gaps between polls fill forward by the same walk. Catch-up gaps
//! wider than `reorg_max_depth` verify the old tip is still canonical
//! at its height before reseeding; a mismatch is a reorg across the
//! gap — common ancestor is found against the ring and
//! [`L1Event::Reorg`] is emitted, never swallowed (invariant 7).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, Bytes};
use alloy_provider::{Provider, ProviderBuilder};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};
use url::Url;

use crate::error::{L1Error, L1Result};
use crate::scan::ScannedBatch;

/// [`L1Watcher`] polling cadence. 2s gives prompt detection without
/// burning RPC quota — half of gnosis's 5s L1 block time, a sixth of
/// Ethereum mainnet's 12s.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Every Nth poll tick we additionally call
/// `eth_getBlockByNumber("finalized")` and emit a `Finalized` event if
/// the result changed. 6 ticks × 2s = 12s — matches L1 mainnet block
/// time, fine-grained enough that consumers see finality move promptly.
const FINALIZED_REFRESH_TICKS: u64 = 6;

/// Broadcast channel capacity for [`L1Event`]s. 256 events ≈ several
/// minutes of L1 activity — plenty for any subscriber that processes
/// events promptly. Lagged subscribers see `RecvError::Lagged` and miss
/// events; downstream design must tolerate this (e.g., resync by
/// reading on-chain state).
const EVENT_BUFFER: usize = 256;

/// Attempts to read the seed block and its ancestors before the watcher gives
/// up — ~5 min at `POLL_INTERVAL`. Long enough to outlast a restarting L1,
/// short enough to surface one that stopped serving a block it just served.
const MAX_SEED_ATTEMPTS: u32 = 150;

/// Event emitted by the [`L1Watcher`].
#[derive(Debug, Clone)]
pub enum L1Event {
    /// Canonical L1 chain extended to a new head.
    NewHead {
        /// L1 block number.
        block_number: u64,
        /// L1 block hash.
        block_hash: B256,
        /// L1 block unix timestamp. Used by the Scheduler to anchor
        /// the proof-window trigger (`L1_ts + proof_window_open`).
        timestamp: u64,
    },
    /// Canonical L1 chain rewound. `common_ancestor_*` is the most
    /// recent block still in canon; everything strictly above it on
    /// the old chain is invalidated. The new canonical head follows.
    Reorg {
        common_ancestor_number: u64,
        common_ancestor_hash: B256,
        old_head_hash: B256,
        new_head_number: u64,
        new_head_hash: B256,
    },
    /// `EEZ.BatchPosted` log observed. `call_data` is the raw payload
    /// from the originating tx's `ProofSystemBatch.callData` field; the
    /// Deriver decodes via `eez-payload-codec`. The Composer typically
    /// uses only the metadata (`l1_block_number`, `tx_hash`, `submitter`).
    BatchPosted {
        l1_block_number: u64,
        l1_block_hash: B256,
        tx_hash: B256,
        /// Tx originator — the address that sent the postBatch.
        /// Composer uses this to detect external batches (based mode).
        submitter: Address,
        call_data: Bytes,
        /// The originating postBatch tx's full `postAndVerifyBatch` input,
        /// captured from the tx fetched by (block, index) during the scan.
        /// The Deriver's reconcile fallback decodes `batch.entries`
        /// from these bytes instead of re-fetching the tx by hash (which
        /// fails on a pruned or still-resyncing embedded L1).
        post_batch_input: Bytes,
        /// `true` iff the same L1 tx emitted `L2ExecutionPerformed` —
        /// the contract's state delta applied. `false` = loser
        /// (`ImmediateEntrySkipped`). Deriver reads this directly.
        state_applied: bool,
        /// Which of this batch's claimed steps L1 actually ran, attributed
        /// from the `L2ExecutionPerformed` events in the batch's own window
        /// (its postBatch tx up to the next postBatch verifying our rollup).
        /// `is_empty()` = nothing settled → Deriver skips the batch.
        settlement: crate::scan::Settlement,
        /// FIRST stateDelta's `currentState` for our rollup. Deriver
        /// compares to local STF result at `from_block - 1` to catch
        /// claimed-vs-derived divergence at the batch entry point.
        claimed_current_state: Option<B256>,
        /// LAST stateDelta's `newState` for our rollup — the claimed
        /// full-chain end. Diagnostics only; see `settlement.final_state`.
        claimed_new_state: Option<B256>,
    },
    /// L1 finalized head advanced.
    Finalized { block_number: u64, block_hash: B256 },
}

/// Configuration for the [`L1Watcher`].
#[derive(Debug, Clone)]
pub struct L1WatcherConfig {
    /// L1 RPC endpoint (HTTP / HTTPS). Uses the same `EEZ_L1_RPC_URL` as
    /// [`L1ReaderConfig`](crate::L1ReaderConfig).
    pub rpc_url: Url,
    /// Deployed `EEZ` (rollups registry) address. Used to filter
    /// `BatchPosted` log events.
    pub eez: Address,
    /// Our rollup's id. Used to filter `L2ExecutionPerformed`
    /// by topic so each `BatchPosted` can be tagged winner / loser.
    pub rollup_id: u64,
    /// Max L1 reorg depth tolerated before halting with
    /// [`L1Error::ReorgTooDeep`]. Default 62 — Ethereum's finality
    /// bound. Configurable so dev / testnet operators can tighten or
    /// loosen.
    pub reorg_max_depth: usize,
}

impl L1WatcherConfig {
    /// Read from `EEZ_*` env vars. Shares `EEZ_L1_RPC_URL` +
    /// `EEZ_REGISTRY_ADDRESS` with the other configs;
    /// `EEZ_L1_REORG_MAX_DEPTH_BLOCKS` is L1Watcher-specific (default
    /// 62).
    ///
    /// # Errors
    ///
    /// Returns [`L1Error::Config`] for any missing required var or
    /// malformed value.
    pub fn from_env() -> L1Result<Self> {
        use std::env;
        use std::str::FromStr;

        let rpc_url_raw = env::var("EEZ_L1_RPC_URL")
            .map_err(|_| L1Error::Config("EEZ_L1_RPC_URL is required (see .env.example)".into()))?;
        let rpc_url = Url::parse(&rpc_url_raw)
            .map_err(|e| L1Error::Config(format!("EEZ_L1_RPC_URL: {e}")))?;

        let eez_raw = env::var("EEZ_REGISTRY_ADDRESS").map_err(|_| {
            L1Error::Config("EEZ_REGISTRY_ADDRESS is required (see .env.example)".into())
        })?;
        let eez = Address::from_str(&eez_raw)
            .map_err(|e| L1Error::Config(format!("EEZ_REGISTRY_ADDRESS: {e}")))?;

        let rollup_id_raw = env::var("EEZ_ROLLUP_ID")
            .map_err(|_| L1Error::Config("EEZ_ROLLUP_ID is required (see .env.example)".into()))?;
        let rollup_id = rollup_id_raw
            .parse::<u64>()
            .map_err(|e| L1Error::Config(format!("EEZ_ROLLUP_ID: {e}")))?;

        let reorg_max_depth = match env::var("EEZ_L1_REORG_MAX_DEPTH_BLOCKS") {
            Ok(v) => v
                .parse::<usize>()
                .map_err(|e| L1Error::Config(format!("EEZ_L1_REORG_MAX_DEPTH_BLOCKS: {e}")))?,
            Err(env::VarError::NotPresent) => 62,
            Err(_) => {
                return Err(L1Error::Config(
                    "EEZ_L1_REORG_MAX_DEPTH_BLOCKS contains non-UTF-8 bytes".into(),
                ));
            }
        };

        if reorg_max_depth == 0 {
            return Err(L1Error::Config(
                "EEZ_L1_REORG_MAX_DEPTH_BLOCKS must be >= 1 (0 tolerates no reorg at all)".into(),
            ));
        }
        Ok(Self {
            rpc_url,
            eez,
            rollup_id,
            reorg_max_depth,
        })
    }
}

/// [`L1Watcher`] handle. Cheaply [`Clone`]able — clones share the same
/// background polling task via `Arc<Inner>`.
#[derive(Clone)]
pub struct L1Watcher {
    inner: Arc<Inner>,
}

struct Inner {
    config: L1WatcherConfig,
    event_tx: broadcast::Sender<L1Event>,
}

impl std::fmt::Debug for L1Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L1Watcher")
            .field("rpc_url", &self.inner.config.rpc_url.as_str())
            .field("eez", &self.inner.config.eez)
            .field("reorg_max_depth", &self.inner.config.reorg_max_depth)
            .field("subscribers", &self.inner.event_tx.receiver_count())
            .finish()
    }
}

impl L1Watcher {
    /// Constructs a handle. Spawns nothing — call [`Self::polling`] once
    /// all subscribers exist.
    #[must_use]
    pub fn new(config: L1WatcherConfig) -> Self {
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER);
        Self {
            inner: Arc::new(Inner { config, event_tx }),
        }
    }

    /// The polling loop, seeded at
    /// `seed_number`/`seed_hash` — a block the caller already scanned
    /// (the Deriver's finalized-clamped catch-up endpoint). The watcher
    /// owns and scans everything strictly after it. Call once, after all
    /// subscribers exist.
    pub fn polling(&self, seed_number: u64, seed_hash: B256) -> impl Future<Output = ()> + use<> {
        let runner = self.clone();
        async move {
            runner.run(seed_number, seed_hash).await;
        }
    }

    /// Returns a new receiver subscribed to all future [`L1Event`]s.
    /// Each call creates an independent receiver; events broadcast to
    /// all of them.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<L1Event> {
        self.inner.event_tx.subscribe()
    }

    async fn run(self, seed_number: u64, seed_hash: B256) {
        let provider = ProviderBuilder::new().connect_http(self.inner.config.rpc_url.clone());
        let mut state = WatcherState::new(self.inner.config.reorg_max_depth);
        // Seed fully before polling: a one-entry ring can't walk back a
        // reorg, trading a retryable error now for ReorgTooDeep later.
        let mut seed_attempts = 0_u32;
        let seed_ts = loop {
            if seed_attempts >= MAX_SEED_ATTEMPTS {
                event!(
                    name: "eez.l1_watcher.seed.exhausted",
                    Level::ERROR,
                    seed_number,
                    seed_attempts,
                    "L1 never served the seed block and its ancestors; stopping the node",
                );
                // Panic, not return: see the poll loop below. Returning here is
                // worse still — no NewHead has been emitted, so the scheduler
                // would never arm and the node would idle forever.
                panic!(
                    "L1 watcher could not seed at block {seed_number} after {seed_attempts} attempts"
                );
            }
            match fetch_block_by_hash(&provider, seed_hash).await {
                Ok(seed) => {
                    match fetch_ancestor_window(&provider, seed, self.inner.config.reorg_max_depth)
                        .await
                    {
                        Ok(window) => {
                            for (number, hash) in window {
                                state.push_canonical(number, hash);
                            }
                            break seed.timestamp;
                        }
                        Err(err) => event!(
                            name: "eez.l1_watcher.seed.ancestors_unavailable",
                            Level::WARN,
                            seed_number,
                            error = %err,
                            "seed ancestors unavailable; retrying before polling",
                        ),
                    }
                }
                Err(err) => event!(
                    name: "eez.l1_watcher.seed.unreadable",
                    Level::WARN,
                    seed_number,
                    error = %err,
                    "seed block unreadable; retrying before polling",
                ),
            }
            seed_attempts += 1;
            tokio::time::sleep(POLL_INTERVAL).await;
        };
        // The seed's NewHead arms the scheduler and is REQUIRED — without it a
        // quiet L1 never produces one.
        self.emit(L1Event::NewHead {
            block_number: seed_number,
            block_hash: seed_hash,
            timestamp: seed_ts,
        });
        let mut ticker = interval_at(Instant::now() + POLL_INTERVAL, POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut tick_count: u64 = 0;

        event!(
            name: "eez.l1_watcher.started",
            Level::INFO,
            eez = %self.inner.config.eez,
            poll_interval_secs = POLL_INTERVAL.as_secs(),
            reorg_max_depth = self.inner.config.reorg_max_depth,
            seed_number,
            ring_entries = state.recent.len(),
            "L1 watcher polling started",
        );

        loop {
            ticker.tick().await;
            tick_count += 1;
            if let Err(err) = self.poll_cycle(&provider, &mut state, tick_count).await {
                // Retrying a deterministic error is an infinite silent stall.
                // PANIC, don't return: reth's critical-task wrapper reports
                // only panics, so a return exits just as quietly.
                if err.is_terminal() {
                    event!(
                        name: "eez.l1_watcher.poll.terminal",
                        Level::ERROR,
                        error = %err,
                        tick = tick_count,
                        "L1 poll cycle failed unrecoverably; stopping the node",
                    );
                    panic!("L1 watcher stopped at tick {tick_count}: {err}");
                }
                event!(
                    name: "eez.l1_watcher.poll.failed",
                    Level::WARN,
                    error = %err,
                    tick = tick_count,
                    "L1 poll cycle failed; will retry next tick",
                );
            }
        }
    }

    /// One poll cycle. Idempotent on retry: far-behind catch-up advances
    /// the ring one scanned chunk at a time, so a transient failure
    /// costs at most one chunk and the next tick resumes from the ring
    /// tip.
    async fn poll_cycle(
        &self,
        provider: &impl Provider,
        state: &mut WatcherState,
        tick_count: u64,
    ) -> L1Result<()> {
        let latest = fetch_block_by_tag(provider, BlockNumberOrTag::Latest).await?;
        let latest_number = latest.number;
        let latest_hash = latest.hash;
        let latest_parent = latest.parent_hash;
        let (tip_number, tip_hash) = state
            .tip()
            .expect("ring is seeded at start and never emptied");
        event!(
            name: "eez.l1_watcher.poll.tick",
            Level::INFO,
            tick = tick_count,
            latest_number,
            tip = tip_number,
            "poll cycle: fetched latest L1 block",
        );

        if tip_hash == latest_hash {
            // No change since last poll.
        } else if tip_hash == latest_parent && latest_number == tip_number + 1 {
            // Normal extension by exactly one block — scan before the
            // ring advances so a failed scan retries this block.
            let scanned = self
                .fetch_batch_logs(provider, latest_number, latest_number)
                .await?;
            state.push_canonical(latest_number, latest_hash);
            self.emit(L1Event::NewHead {
                block_number: latest_number,
                block_hash: latest_hash,
                timestamp: latest.timestamp,
            });
            self.emit_scanned_batches(latest_number, latest_number, scanned);
        } else {
            // Either a reorg, a multi-block gap, or a far-behind tip.
            let (old_tip_number, old_tip_hash) = (tip_number, tip_hash);
            // Far-behind first: when the gap exceeds the ring's
            // depth, the parent-hash walk below is provably futile
            // (it cannot descend to ring heights before exhausting
            // reorg_max_depth), so skip straight to chunked
            // catch-up instead of burning the walk's RPCs per tick.
            let depth = state.reorg_max_depth as u64;
            if latest_number > old_tip_number.saturating_add(depth) {
                // "Far behind" doesn't prove the old tip is
                // still canonical — swallowing a reorg across
                // the gap is the silent fallback invariant 7
                // forbids. Verify by height first.
                let at_old_height =
                    fetch_block_by_tag(provider, BlockNumberOrTag::Number(old_tip_number)).await?;
                let reorged_across_gap = at_old_height.hash != old_tip_hash;
                if reorged_across_gap {
                    // Old tip reorged out. Find the common
                    // ancestor against the ring (bounded by
                    // ≤ reorg_max_depth); none in bounds →
                    // loud halt, not benign catch-up.
                    let common = find_common_ancestor_by_height(provider, state)
                        .await?
                        .ok_or(L1Error::ReorgTooDeep {
                            walked: state.reorg_max_depth,
                            max: state.reorg_max_depth,
                        })?;
                    event!(
                        name: "eez.l1_watcher.poll.catchup_reorg",
                        Level::WARN,
                        tick = tick_count,
                        old_tip_number,
                        old_tip_hash = %old_tip_hash,
                        hash_at_old_height = %at_old_height.hash,
                        common_ancestor_number = common.number,
                        common_ancestor_hash = %common.hash,
                        latest_number,
                        "chain reorged across catch-up gap — old tip no \
                         longer canonical; emitting Reorg and reseeding \
                         at the ancestor before chunked catch-up",
                    );
                    self.emit(L1Event::Reorg {
                        common_ancestor_number: common.number,
                        common_ancestor_hash: common.hash,
                        old_head_hash: old_tip_hash,
                        new_head_number: latest_number,
                        new_head_hash: latest_hash,
                    });
                    // Retreat so later ticks re-enter as plain catch-up.
                    // `common` is from the ring, so this KEEPS its ancestors —
                    // clearing them would leave a ring that can't walk back.
                    state.rewind_to(common.number);
                    return Ok(());
                }

                // Still-canonical catch-up: advance ONE chunk per
                // tick. Progress commits via the ring tip, so a
                // failed chunk costs nothing — the next tick
                // recomputes the same range from the tip.
                //
                // Reorg tolerance here is boundary-only: a reorg above it
                // needs no retraction, since the next chunk re-scans and
                // re-delivered batches dedup by tx hash downstream.
                let scan_from = old_tip_number + 1;
                let chunk_to = scan_from
                    .saturating_add(crate::scan::LOG_SCAN_CHUNK_BLOCKS - 1)
                    .min(latest_number);
                let boundary = if chunk_to == latest_number {
                    BlockSnapshot {
                        number: latest_number,
                        hash: latest_hash,
                        parent_hash: latest_parent,
                        timestamp: latest.timestamp,
                    }
                } else {
                    fetch_block_by_tag(provider, BlockNumberOrTag::Number(chunk_to)).await?
                };
                event!(
                    name: "eez.l1_watcher.poll.catchup",
                    Level::INFO,
                    tick = tick_count,
                    old_tip_number,
                    scan_from,
                    chunk_to,
                    latest_number,
                    reorg_max_depth = state.reorg_max_depth,
                    "tip is far behind latest — scanning one \
                     BatchPosted chunk and reseeding ring at the \
                     chunk boundary",
                );
                // Narrowing may cover LESS than `chunk_to` when the range
                // matches more logs than the provider serves; the ring must
                // then be reseeded at the block actually reached, or we'd
                // claim to have scanned blocks we never read.
                let (scanned, reached) = crate::scan::scan_batch_logs_range_adaptive(
                    provider,
                    self.inner.config.eez,
                    self.inner.config.rollup_id,
                    scan_from,
                    chunk_to,
                )
                .await?;
                let boundary = if reached == chunk_to {
                    boundary
                } else {
                    fetch_block_by_tag(provider, BlockNumberOrTag::Number(reached)).await?
                };
                // Refill whenever the boundary is within reorg reach of the
                // tip; a lone entry there can't walk back a one-block reorg.
                // Narrowing and a short final chunk both land there.
                let window = if latest_number.saturating_sub(boundary.number)
                    < state.reorg_max_depth as u64
                {
                    fetch_ancestor_window(provider, boundary, state.reorg_max_depth).await?
                } else {
                    vec![(boundary.number, boundary.hash)]
                };
                // INFO, not WARN: fires once per chunk during
                // routine catch-up progress; the reorg reseed
                // below keeps its WARN.
                event!(
                    name: "eez.l1_watcher.ring.rewind",
                    Level::INFO,
                    tick = tick_count,
                    old_tip_number,
                    old_tip_hash = %old_tip_hash,
                    new_tip_number = boundary.number,
                    new_tip_hash = %boundary.hash,
                    window_len = window.len(),
                    "reseeding ring at chunk boundary — dropping all \
                     prior ring entries",
                );
                state.rewind_to(0);
                for (number, hash) in window {
                    state.push_canonical(number, hash);
                }
                self.emit(L1Event::NewHead {
                    block_number: boundary.number,
                    block_hash: boundary.hash,
                    timestamp: boundary.timestamp,
                });
                self.emit_scanned_batches(scan_from, reached, scanned);
                return Ok(());
            }

            // Walk back via parent_hash links until we hit a hash
            // in our ring (common ancestor) or exceed
            // reorg_max_depth.
            let walked = walk_back_to_common(
                provider,
                latest_parent,
                latest_number.saturating_sub(1),
                state,
            )
            .await?;
            let Some(common) = walked else {
                // Walk-back exhausted reorg_max_depth without a
                // common ancestor while the gap is within the
                // ring's depth — genuine deep reorg → loud halt.
                return Err(L1Error::ReorgTooDeep {
                    walked: state.reorg_max_depth,
                    max: state.reorg_max_depth,
                });
            };

            let was_reorg = old_tip_number > common.number || old_tip_hash != common.hash;
            if was_reorg {
                // Reorg event first, then the ring rewind it
                // explains — every rewind below the old tip is
                // preceded by a Reorg emission.
                self.emit(L1Event::Reorg {
                    common_ancestor_number: common.number,
                    common_ancestor_hash: common.hash,
                    old_head_hash: old_tip_hash,
                    new_head_number: latest_number,
                    new_head_hash: latest_hash,
                });
                event!(
                    name: "eez.l1_watcher.ring.rewind",
                    Level::WARN,
                    tick = tick_count,
                    old_tip_number,
                    old_tip_hash = %old_tip_hash,
                    common_ancestor_number = common.number,
                    common_ancestor_hash = %common.hash,
                    new_head_number = latest_number,
                    new_head_hash = %latest_hash,
                    "reorg — rewinding ring to common ancestor",
                );
                state.rewind_to(common.number);
            }
            // Walk forward from common+1 to latest, emitting NewHead
            // for each missed block and scanning its BatchPosted
            // logs.
            let scan_from = common.number + 1;
            let (scanned, reached) = crate::scan::scan_batch_logs_range_adaptive(
                provider,
                self.inner.config.eez,
                self.inner.config.rollup_id,
                scan_from,
                latest_number,
            )
            .await?;
            // Narrowed: commit only what was read; the next tick resumes there.
            let (to, to_hash) = if reached == latest_number {
                (latest_number, latest_hash)
            } else {
                let boundary =
                    fetch_block_by_tag(provider, BlockNumberOrTag::Number(reached)).await?;
                (reached, boundary.hash)
            };
            self.fill_forward(provider, scan_from, to, to_hash, state)
                .await?;
            self.emit_scanned_batches(scan_from, to, scanned);
        }

        if tick_count.is_multiple_of(FINALIZED_REFRESH_TICKS) {
            self.refresh_finalized(provider, state).await?;
        }

        Ok(())
    }

    /// Walks blocks `from..=to` forward by hash chain (latest's
    /// ancestor at each height), emitting `NewHead` for each and
    /// extending the canonical ring buffer. `to_hash` is the known hash
    /// at height `to`; ancestors are resolved via `parent_hash` links.
    async fn fill_forward(
        &self,
        provider: &impl Provider,
        from: u64,
        to: u64,
        to_hash: B256,
        state: &mut WatcherState,
    ) -> L1Result<()> {
        if from > to {
            return Ok(());
        }
        // Collect (number, hash, timestamp) for from..=to by walking
        // back from (to, to_hash) via parent_hash. We need each
        // block's timestamp for downstream Scheduler trigger anchoring,
        // so unlike the earlier hash-only walk, fetch every step.
        let span = usize::try_from(to - from + 1).unwrap_or(usize::MAX);
        let mut collected: Vec<(u64, B256, u64)> = Vec::with_capacity(span);
        let mut cursor_hash = to_hash;
        let mut cursor_number = to;
        loop {
            let block = fetch_block_by_hash(provider, cursor_hash).await?;
            collected.push((cursor_number, cursor_hash, block.timestamp));
            if cursor_number == from {
                break;
            }
            cursor_hash = block.parent_hash;
            cursor_number = block.number.saturating_sub(1);
            if collected.len() >= span {
                break;
            }
        }
        // Emit oldest to newest.
        for (n, h, ts) in collected.iter().rev() {
            state.push_canonical(*n, *h);
            self.emit(L1Event::NewHead {
                block_number: *n,
                block_hash: *h,
                timestamp: *ts,
            });
        }
        Ok(())
    }

    /// Fetches `BatchPosted` logs in `[from, to]` without emitting —
    /// callers commit ring state only after this succeeds.
    ///
    /// Non-adaptive by design: the only caller passes a single block. Wider
    /// ranges go through [`scan_batch_logs_range_adaptive`] instead.
    async fn fetch_batch_logs(
        &self,
        provider: &impl Provider,
        from: u64,
        to: u64,
    ) -> L1Result<Vec<ScannedBatch>> {
        event!(
            name: "eez.l1_watcher.scan_batch_posted",
            Level::INFO,
            from,
            to,
            "scanning L1 range for BatchPosted logs",
        );
        crate::scan::scan_batch_logs_range(
            provider,
            self.inner.config.eez,
            self.inner.config.rollup_id,
            from,
            to,
        )
        .await
    }

    fn emit_scanned_batches(&self, from: u64, to: u64, scanned: Vec<ScannedBatch>) {
        if !scanned.is_empty() {
            event!(
                name: "eez.l1_watcher.scan_batch_posted.found",
                Level::INFO,
                from,
                to,
                count = scanned.len(),
                "emitting BatchPosted events to subscribers",
            );
        }
        for b in scanned {
            self.emit(L1Event::BatchPosted {
                l1_block_number: b.l1_block_number,
                l1_block_hash: b.l1_block_hash,
                tx_hash: b.tx_hash,
                submitter: b.submitter,
                call_data: b.call_data,
                post_batch_input: b.post_batch_input,
                state_applied: b.state_applied,
                settlement: b.settlement,
                claimed_current_state: b.claimed_current_state,
                claimed_new_state: b.claimed_new_state,
            });
        }
    }

    async fn refresh_finalized(
        &self,
        provider: &impl Provider,
        state: &mut WatcherState,
    ) -> L1Result<()> {
        // A freshly-launched embedded chiado L1 has no finalized block
        // until lighthouse delivers an FCU carrying a finalized field.
        // Treat "block(finalized) returned None" as "not yet available"
        // — don't propagate it as a poll-cycle failure.
        let finalized = match fetch_block_by_tag(provider, BlockNumberOrTag::Finalized).await {
            Ok(b) => b,
            Err(L1Error::Provider(msg)) if msg.contains("returned None") => return Ok(()),
            Err(e) => return Err(e),
        };
        if state.last_finalized_hash == Some(finalized.hash) {
            return Ok(());
        }
        state.last_finalized_hash = Some(finalized.hash);
        self.emit(L1Event::Finalized {
            block_number: finalized.number,
            block_hash: finalized.hash,
        });
        Ok(())
    }

    fn emit(&self, event: L1Event) {
        // `send` returns Err if there are no active subscribers, which
        // is fine — events were never observed and that's OK.
        let _ = self.inner.event_tx.send(event);
    }
}

/// In-memory state owned by the polling task.
struct WatcherState {
    /// Ring buffer of recent canonical L1 (number, hash) pairs. Oldest
    /// at front, newest at back. Bounded by [`Self::reorg_max_depth`].
    /// `(number, hash)` is stored as a tuple rather than just hash so
    /// `rewind_to(common.number)` can drop everything above the common
    /// ancestor without an additional RPC.
    recent: VecDeque<(u64, B256)>,
    reorg_max_depth: usize,
    last_finalized_hash: Option<B256>,
}

impl WatcherState {
    fn new(reorg_max_depth: usize) -> Self {
        // `L1WatcherConfig`'s fields are pub, so `from_env`'s rejection of 0 is
        // not the only way in. At 0 every push pops itself, emptying the ring
        // and panicking the tip lookup on the first tick.
        let reorg_max_depth = reorg_max_depth.max(1);
        Self {
            recent: VecDeque::with_capacity(reorg_max_depth),
            reorg_max_depth,
            last_finalized_hash: None,
        }
    }

    fn tip(&self) -> Option<(u64, B256)> {
        self.recent.back().copied()
    }

    fn push_canonical(&mut self, number: u64, hash: B256) {
        self.recent.push_back((number, hash));
        while self.recent.len() > self.reorg_max_depth {
            self.recent.pop_front();
        }
    }

    /// Drop ring entries with number > `keep_up_to_number`. Used after
    /// a reorg to align the ring with the new canonical chain before
    /// `fill_forward` extends it again.
    fn rewind_to(&mut self, keep_up_to_number: u64) {
        while let Some(&(n, _)) = self.recent.back() {
            if n > keep_up_to_number {
                self.recent.pop_back();
            } else {
                break;
            }
        }
    }

    fn lookup_hash(&self, hash: B256) -> Option<u64> {
        self.recent
            .iter()
            .find(|(_, h)| *h == hash)
            .map(|(n, _)| *n)
    }
}

#[derive(Debug, Clone, Copy)]
struct CommonAncestor {
    number: u64,
    hash: B256,
}

/// Walks the chain backward from `(start_parent_hash, start_parent_number)`
/// following `parent_hash` pointers until either:
///   - the cursor hash matches a hash already in the watcher's ring
///     (that's the common ancestor); returns `Some`.
///   - we exceed [`WatcherState::reorg_max_depth`] hops; returns `None`
///     (the caller maps this to [`L1Error::ReorgTooDeep`]).
async fn walk_back_to_common(
    provider: &impl Provider,
    mut cursor_hash: B256,
    mut cursor_number: u64,
    state: &WatcherState,
) -> L1Result<Option<CommonAncestor>> {
    for _ in 0..state.reorg_max_depth {
        if let Some(known_number) = state.lookup_hash(cursor_hash) {
            // Ring stores numbers — use the ring's number, which is
            // authoritative (matches the cursor by hash equality).
            debug_assert_eq!(known_number, cursor_number);
            return Ok(Some(CommonAncestor {
                number: cursor_number,
                hash: cursor_hash,
            }));
        }
        if cursor_number == 0 {
            // Reached genesis without finding common ancestor. Treat
            // as too-deep so the operator can investigate.
            return Ok(None);
        }
        let block = fetch_block_by_hash(provider, cursor_hash).await?;
        cursor_hash = block.parent_hash;
        cursor_number = block.number.saturating_sub(1);
    }
    Ok(None)
}

/// Parent-linked `(number, hash)` window ending at `boundary` (inclusive),
/// oldest → newest, ≤ `depth` entries. Collects fully before the caller
/// mutates the ring, so a fetch failure retries the identical chunk.
async fn fetch_ancestor_window(
    provider: &impl Provider,
    boundary: BlockSnapshot,
    depth: usize,
) -> L1Result<Vec<(u64, B256)>> {
    let mut collected: Vec<(u64, B256)> = Vec::with_capacity(depth);
    collected.push((boundary.number, boundary.hash));
    let mut parent_hash = boundary.parent_hash;
    let mut number = boundary.number;
    while collected.len() < depth && number > 0 {
        let block = fetch_block_by_hash(provider, parent_hash).await?;
        number = block.number;
        parent_hash = block.parent_hash;
        collected.push((number, block.hash));
    }
    collected.reverse();
    Ok(collected)
}

/// Finds the most recent ring entry whose hash still matches the
/// provider's canonical block at that height. Compares newest first,
/// fetching by NUMBER (not hash) so the answer reflects the provider's
/// CURRENT canonical chain — used when a catch-up gap is too wide for
/// the parent-hash walk in [`walk_back_to_common`] to reach the ring.
/// Bounded by the ring length (≤ `reorg_max_depth`). Returns `None` if
/// every ring entry was reorged out (caller maps that to
/// [`L1Error::ReorgTooDeep`]).
async fn find_common_ancestor_by_height(
    provider: &impl Provider,
    state: &WatcherState,
) -> L1Result<Option<CommonAncestor>> {
    for &(number, hash) in state.recent.iter().rev() {
        let canonical = fetch_block_by_tag(provider, BlockNumberOrTag::Number(number)).await?;
        if canonical.hash == hash {
            return Ok(Some(CommonAncestor { number, hash }));
        }
    }
    Ok(None)
}

/// Minimal block-header snapshot the watcher needs. Avoids the full
/// `alloy_rpc_types_eth::Block` type at every call site.
#[derive(Debug, Clone, Copy)]
struct BlockSnapshot {
    number: u64,
    hash: B256,
    parent_hash: B256,
    /// Unix timestamp from `block.header.timestamp`. Used by downstream
    /// Schedulers to anchor proof-window triggers.
    timestamp: u64,
}

async fn fetch_block_by_tag(
    provider: &impl Provider,
    tag: BlockNumberOrTag,
) -> L1Result<BlockSnapshot> {
    let block = provider
        .get_block_by_number(tag)
        .await
        .map_err(|e| L1Error::Provider(format!("get_block({tag:?}): {e}")))?
        .ok_or_else(|| L1Error::Provider(format!("block({tag:?}) returned None")))?;
    Ok(BlockSnapshot {
        number: block.header.number,
        hash: block.header.hash,
        parent_hash: block.header.inner.parent_hash,
        timestamp: block.header.inner.timestamp,
    })
}

async fn fetch_block_by_hash(provider: &impl Provider, hash: B256) -> L1Result<BlockSnapshot> {
    let block = provider
        .get_block_by_hash(hash)
        .await
        .map_err(|e| L1Error::Provider(format!("get_block_by_hash({hash}): {e}")))?
        .ok_or_else(|| L1Error::Provider(format!("block({hash}) returned None")))?;
    Ok(BlockSnapshot {
        number: block.header.number,
        hash: block.header.hash,
        parent_hash: block.header.inner.parent_hash,
        timestamp: block.header.inner.timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use alloy_transport::mock::Asserter;

    fn test_watcher() -> (L1Watcher, broadcast::Receiver<L1Event>) {
        let (event_tx, rx) = broadcast::channel(256);
        let watcher = L1Watcher {
            inner: std::sync::Arc::new(Inner {
                config: L1WatcherConfig {
                    rpc_url: "http://127.0.0.1:0".parse().expect("static url"),
                    eez: alloy_primitives::Address::ZERO,
                    rollup_id: 1,
                    reorg_max_depth: 3,
                },
                event_tx,
            }),
        };
        (watcher, rx)
    }

    fn mock_block(number: u64, hash: B256, parent: B256, ts: u64) -> alloy_rpc_types_eth::Block {
        let mut b: alloy_rpc_types_eth::Block = alloy_rpc_types_eth::Block::default();
        b.header.hash = hash;
        b.header.inner.number = number;
        b.header.inner.parent_hash = parent;
        b.header.inner.timestamp = ts;
        b
    }

    /// Far-behind catch-up advances the ring ONE chunk per poll cycle and
    /// emits NewHead at the chunk boundary — not at the far target.
    #[tokio::test]
    async fn far_behind_catch_up_steps_one_chunk_per_tick() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA);
        let latest_hash = B256::with_last_byte(0xBB);
        let latest_parent = B256::with_last_byte(0xB0);
        let boundary_hash = B256::with_last_byte(0xCC);
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        // ── Tick 1: latest=200_000, tip=10 → far behind. Expect ONE chunk
        // [11, 100_010] then ring reseed at the boundary.
        // Call order: latest, block #10 (canonical check), block
        // #100_010 (chunk boundary), 2× get_logs. The gap exceeds the
        // ring depth, so no parent-hash walk-back runs.
        asserter.push_success(&mock_block(200_000, latest_hash, latest_parent, 5_000));
        asserter.push_success(&mock_block(10, tip_hash, B256::with_last_byte(9), 10));
        asserter.push_success(&mock_block(
            100_010,
            boundary_hash,
            B256::with_last_byte(4),
            3_000,
        ));
        asserter.push_success(&serde_json::json!([])); // BatchPosted logs
        asserter.push_success(&serde_json::json!([])); // winner logs

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect("chunk step succeeds");

        assert_eq!(
            state.tip(),
            Some((100_010, boundary_hash)),
            "ring reseeds at chunk boundary"
        );
        assert_eq!(
            state.recent.len(),
            1,
            "mid-catch-up boundary stays single-entry — out of real reorg reach"
        );
        match rx.try_recv().expect("one event emitted") {
            L1Event::NewHead {
                block_number,
                block_hash,
                timestamp,
            } => {
                assert_eq!(block_number, 100_010);
                assert_eq!(block_hash, boundary_hash);
                assert_eq!(timestamp, 3_000);
            }
            other => panic!("expected NewHead at boundary, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "no further events on tick 1");

        // ── Tick 2: same latest → final chunk [100_011, 200_000]. Boundary
        // == latest (shortcut path, no extra boundary fetch); at the tip
        // the ring refills an ancestor window (2 extra by-hash fetches).
        let ancestor1_hash = B256::with_last_byte(0xA1);
        let ancestor2_hash = B256::with_last_byte(0xA2);
        asserter.push_success(&mock_block(200_000, latest_hash, latest_parent, 5_000));
        asserter.push_success(&mock_block(
            100_010,
            boundary_hash,
            B256::with_last_byte(4),
            3_000,
        ));
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([]));
        // Ancestor window: parent of latest, then grandparent.
        asserter.push_success(&mock_block(199_999, latest_parent, ancestor2_hash, 4_999));
        asserter.push_success(&mock_block(199_998, ancestor2_hash, ancestor1_hash, 4_998));

        watcher
            .poll_cycle(&provider, &mut state, 2)
            .await
            .expect("final chunk succeeds");

        assert_eq!(
            state.tip(),
            Some((200_000, latest_hash)),
            "ring reaches latest"
        );
        assert_eq!(
            state.recent.len(),
            3,
            "final at-tip boundary refills a full ancestor window"
        );
        assert_eq!(state.lookup_hash(latest_parent), Some(199_999));
        match rx.try_recv().expect("one event emitted") {
            L1Event::NewHead {
                block_number,
                block_hash,
                ..
            } => {
                assert_eq!(block_number, 200_000);
                assert_eq!(block_hash, latest_hash);
            }
            other => panic!("expected NewHead at latest, got {other:?}"),
        }
    }

    /// A one-block reorg right after far-behind catch-up completes must
    /// find its ancestor in the refilled ring — Reorg, not ReorgTooDeep.
    #[tokio::test]
    async fn shallow_reorg_after_far_behind_recovers() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA);
        let latest_hash = B256::with_last_byte(0xBB);
        let latest_parent = B256::with_last_byte(0xB0);
        let grandparent_hash = B256::with_last_byte(0xA2);
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        // ── Tick 1: gap (10 → 50_000) is far behind but fits in one
        // LOG_SCAN_CHUNK_BLOCKS chunk, so the boundary lands directly on
        // the live tip and the ring refills a 3-entry ancestor window.
        asserter.push_success(&mock_block(50_000, latest_hash, latest_parent, 5_000));
        asserter.push_success(&mock_block(10, tip_hash, B256::with_last_byte(9), 10)); // at_old_height check
        asserter.push_success(&serde_json::json!([])); // BatchPosted logs
        asserter.push_success(&serde_json::json!([])); // winner logs
        asserter.push_success(&mock_block(49_999, latest_parent, grandparent_hash, 4_999));
        asserter.push_success(&mock_block(
            49_998,
            grandparent_hash,
            B256::with_last_byte(0xA1),
            4_998,
        ));

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect("catch-up chunk succeeds");

        assert_eq!(
            state.recent.len(),
            3,
            "final boundary refills a full window"
        );
        assert_eq!(state.tip(), Some((50_000, latest_hash)));
        assert_eq!(
            state.lookup_hash(latest_parent),
            Some(49_999),
            "one hop back is in the ring"
        );
        match rx.try_recv().expect("NewHead at boundary") {
            L1Event::NewHead { block_number, .. } => assert_eq!(block_number, 50_000),
            other => panic!("expected NewHead, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "nothing else on tick 1");

        // ── Tick 2: a one-block reorg replaces the tip — same height,
        // new hash, parent = the ancestor the window just seeded. This
        // must recover via Reorg, not ReorgTooDeep.
        let replacement_hash = B256::with_last_byte(0xDD);
        asserter.push_success(&mock_block(50_000, replacement_hash, latest_parent, 5_001));
        asserter.push_success(&serde_json::json!([])); // BatchPosted logs
        asserter.push_success(&serde_json::json!([])); // winner logs
        asserter.push_success(&mock_block(50_000, replacement_hash, latest_parent, 5_001)); // fill_forward by-hash

        watcher
            .poll_cycle(&provider, &mut state, 2)
            .await
            .expect("shallow reorg recovers, not ReorgTooDeep");

        match rx.try_recv().expect("Reorg event") {
            L1Event::Reorg {
                common_ancestor_number,
                common_ancestor_hash,
                old_head_hash,
                new_head_number,
                new_head_hash,
            } => {
                assert_eq!(common_ancestor_number, 49_999);
                assert_eq!(common_ancestor_hash, latest_parent);
                assert_eq!(old_head_hash, latest_hash);
                assert_eq!(new_head_number, 50_000);
                assert_eq!(new_head_hash, replacement_hash);
            }
            other => panic!("expected Reorg, got {other:?}"),
        }
        match rx.try_recv().expect("NewHead after reorg") {
            L1Event::NewHead {
                block_number,
                block_hash,
                ..
            } => {
                assert_eq!(block_number, 50_000);
                assert_eq!(block_hash, replacement_hash);
            }
            other => panic!("expected NewHead, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "nothing else emitted");
        assert_eq!(state.tip(), Some((50_000, replacement_hash)));
    }

    /// A failed chunk scan advances nothing: ring tip unchanged, zero
    /// events emitted. The next tick retries the same range for free.
    #[tokio::test]
    async fn far_behind_chunk_failure_advances_nothing() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA);
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        asserter.push_success(&mock_block(
            200_000,
            B256::with_last_byte(0xBB),
            B256::with_last_byte(0xB0),
            5_000,
        ));
        asserter.push_success(&mock_block(10, tip_hash, B256::with_last_byte(9), 10));
        asserter.push_success(&mock_block(
            100_010,
            B256::with_last_byte(0xCC),
            B256::with_last_byte(4),
            3_000,
        ));
        asserter.push_failure_msg("injected: range scan failed"); // first get_logs

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect_err("injected failure must propagate");

        assert_eq!(
            state.tip(),
            Some((10, tip_hash)),
            "tip unchanged on failure"
        );
        assert!(rx.try_recv().is_err(), "no events leaked on failure");
    }

    /// Old tip reorged out across the gap: the reorg tick emits exactly
    /// one Reorg, reseeds the ring at the common ancestor, and scans
    /// nothing — chunking resumes from the ancestor on later ticks.
    #[tokio::test]
    async fn far_behind_reorged_gap_emits_reorg_and_reseeds_at_ancestor() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let stale_tip_hash = B256::with_last_byte(0xAA);
        let ancestor_hash = B256::with_last_byte(0x99);
        let replaced_hash = B256::with_last_byte(0xDD);
        let older_hash = B256::with_last_byte(0x88);
        let mut state = WatcherState::new(3);
        state.push_canonical(8, older_hash);
        state.push_canonical(9, ancestor_hash);
        state.push_canonical(10, stale_tip_hash);

        // latest, block #10 (≠ ring → reorged), then
        // find_common_ancestor_by_height: #10 (mismatch), #9 (match).
        asserter.push_success(&mock_block(
            200_000,
            B256::with_last_byte(0xBB),
            B256::with_last_byte(0xB0),
            5_000,
        ));
        asserter.push_success(&mock_block(10, replaced_hash, B256::with_last_byte(9), 10));
        asserter.push_success(&mock_block(10, replaced_hash, B256::with_last_byte(9), 10));
        asserter.push_success(&mock_block(9, ancestor_hash, B256::with_last_byte(8), 9));

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect("reorg tick succeeds");

        assert_eq!(
            state.tip(),
            Some((9, ancestor_hash)),
            "ring reseeds at ancestor"
        );
        // Retreat KEEPS ancestors below `common`; clearing them would leave a
        // ring that can't walk back the next reorg.
        assert_eq!(state.lookup_hash(older_hash), Some(8), "ancestor 8 dropped");
        match rx.try_recv().expect("one event emitted") {
            L1Event::Reorg {
                common_ancestor_number,
                common_ancestor_hash,
                old_head_hash,
                ..
            } => {
                assert_eq!(common_ancestor_number, 9);
                assert_eq!(common_ancestor_hash, ancestor_hash);
                assert_eq!(old_head_hash, stale_tip_hash);
            }
            other => panic!("expected Reorg, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "reorg tick emits nothing else");
    }

    /// +1 path: a failed log scan must leave the ring untouched so the
    /// identical range is retried next tick, not skipped forever.
    #[tokio::test]
    async fn extension_scan_failure_leaves_tip_then_retry_succeeds() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA);
        let next_hash = B256::with_last_byte(0xBB);
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        // Tick 1: latest = 11 (extension by one). Scan runs before the
        // ring commits, so an injected failure must leave the tip alone.
        asserter.push_success(&mock_block(11, next_hash, tip_hash, 100));
        asserter.push_failure_msg("injected: log scan failed"); // first get_logs

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect_err("injected failure must propagate");

        assert_eq!(
            state.tip(),
            Some((10, tip_hash)),
            "tip unchanged on failure"
        );
        assert!(rx.try_recv().is_err(), "no events leaked on failure");

        // Tick 2: identical range retried — this time the scan succeeds.
        asserter.push_success(&mock_block(11, next_hash, tip_hash, 100));
        asserter.push_success(&serde_json::json!([])); // BatchPosted logs
        asserter.push_success(&serde_json::json!([])); // winner logs

        watcher
            .poll_cycle(&provider, &mut state, 2)
            .await
            .expect("retry succeeds");

        assert_eq!(state.tip(), Some((11, next_hash)), "ring advances on retry");
        match rx.try_recv().expect("one event emitted") {
            L1Event::NewHead {
                block_number,
                block_hash,
                ..
            } => {
                assert_eq!(block_number, 11);
                assert_eq!(block_hash, next_hash);
            }
            other => panic!("expected NewHead, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "nothing else emitted on retry");
    }

    /// Walk-back path: a scan failure after a common ancestor is found
    /// must not advance the ring — retry re-walks and re-scans the
    /// identical range.
    #[tokio::test]
    async fn walk_back_scan_failure_leaves_tip_then_retry_succeeds() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA); // height 10
        let h11 = B256::with_last_byte(0x11); // height 11, parent = tip_hash
        let h12 = B256::with_last_byte(0x12); // height 12 (latest), parent = h11
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        // Tick 1: latest = 12. walk_back_to_common fetches block 11 by
        // hash, whose parent (tip_hash) matches the ring at height 10 —
        // common ancestor found, no reorg. Scan over [11, 12] fails.
        asserter.push_success(&mock_block(12, h12, h11, 200));
        asserter.push_success(&mock_block(11, h11, tip_hash, 110)); // walk-back fetch
        asserter.push_failure_msg("injected: log scan failed"); // first get_logs

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect_err("injected failure must propagate");

        assert_eq!(
            state.tip(),
            Some((10, tip_hash)),
            "tip unchanged on failure"
        );
        assert!(rx.try_recv().is_err(), "no events leaked on failure");

        // Tick 2: identical range retried — walk-back, scan, then
        // fill_forward's own block-by-hash fetches (h12, then h11).
        asserter.push_success(&mock_block(12, h12, h11, 200));
        asserter.push_success(&mock_block(11, h11, tip_hash, 110)); // walk-back fetch
        asserter.push_success(&serde_json::json!([])); // BatchPosted logs
        asserter.push_success(&serde_json::json!([])); // winner logs
        asserter.push_success(&mock_block(12, h12, h11, 200)); // fill_forward: by-hash h12
        asserter.push_success(&mock_block(11, h11, tip_hash, 110)); // fill_forward: by-hash h11

        watcher
            .poll_cycle(&provider, &mut state, 2)
            .await
            .expect("retry succeeds");

        assert_eq!(state.tip(), Some((12, h12)), "ring reaches latest");
        match rx.try_recv().expect("first event") {
            L1Event::NewHead { block_number, .. } => assert_eq!(block_number, 11),
            other => panic!("expected NewHead(11), got {other:?}"),
        }
        match rx.try_recv().expect("second event") {
            L1Event::NewHead { block_number, .. } => assert_eq!(block_number, 12),
            other => panic!("expected NewHead(12), got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "nothing else emitted on retry");
    }

    /// Walk-back with a NARROWED (not failed) scan: the ring and NewHeads
    /// must stop at `reached`, never `latest` — the tail is next tick's.
    #[tokio::test]
    async fn walk_back_narrowed_scan_reseeds_ring_at_reached_not_latest() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let tip_hash = B256::with_last_byte(0xAA); // height 10
        let h11 = B256::with_last_byte(0x11); // height 11, parent = tip_hash
        let h12 = B256::with_last_byte(0x12); // height 12, parent = h11
        let h13 = B256::with_last_byte(0x13); // height 13 (latest), parent = h12
        let mut state = WatcherState::new(3);
        state.push_canonical(10, tip_hash);

        // gap == reorg_max_depth → walk-back, not far-behind. Common ancestor
        // is 10, no reorg. The [11, 13] scan is too wide and halves to [11,12].
        asserter.push_success(&mock_block(13, h13, h12, 1_300));
        asserter.push_success(&mock_block(12, h12, h11, 1_200)); // walk-back fetch
        asserter.push_success(&mock_block(11, h11, tip_hash, 1_100)); // walk-back fetch
        asserter.push_failure_msg("query returned more than 10000 results"); // get_logs [11,13]
        asserter.push_success(&serde_json::json!([])); // get_logs [11,12] BatchPosted
        asserter.push_success(&serde_json::json!([])); // get_logs [11,12] winners
        asserter.push_success(&mock_block(12, h12, h11, 1_200)); // boundary fetch (reached != latest)
        asserter.push_success(&mock_block(12, h12, h11, 1_200)); // fill_forward: by-hash h12
        asserter.push_success(&mock_block(11, h11, tip_hash, 1_100)); // fill_forward: by-hash h11

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect("narrowed scan must not propagate — reseeding at reached is the remedy");

        assert_eq!(
            state.tip(),
            Some((12, h12)),
            "ring reseeds at the NARROWED boundary, not latest (13)"
        );
        assert_eq!(state.recent.len(), 3);
        match rx.try_recv().expect("first event") {
            L1Event::NewHead { block_number, .. } => assert_eq!(block_number, 11),
            other => panic!("expected NewHead(11), got {other:?}"),
        }
        match rx.try_recv().expect("second event") {
            L1Event::NewHead { block_number, .. } => assert_eq!(block_number, 12),
            other => panic!("expected NewHead(12), got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "NewHeads must stop at the narrowed boundary — none for 13"
        );
    }

    /// A seeded tip that still matches `latest` is a no-op cycle: no
    /// scan, no events, ring untouched.
    #[tokio::test]
    async fn seeded_tip_unchanged_emits_nothing() {
        let (watcher, mut rx) = test_watcher();
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let hash = B256::with_last_byte(0x64);
        let mut state = WatcherState::new(3);
        state.push_canonical(100, hash);

        asserter.push_success(&mock_block(100, hash, B256::with_last_byte(0x63), 1_000));

        watcher
            .poll_cycle(&provider, &mut state, 1)
            .await
            .expect("no-op cycle succeeds");

        assert_eq!(state.tip(), Some((100, hash)), "tip unchanged");
        assert!(rx.try_recv().is_err(), "no events on unchanged tip");
    }

    #[test]
    fn watcher_state_seeds_empty() {
        let state = WatcherState::new(64);
        assert!(state.tip().is_none());
        assert!(state.lookup_hash(B256::ZERO).is_none());
    }

    #[test]
    fn watcher_state_pushes_and_caps() {
        let mut state = WatcherState::new(3);
        for i in 0..5u64 {
            state.push_canonical(
                i,
                B256::with_last_byte(u8::try_from(i).expect("test index fits in u8")),
            );
        }
        assert_eq!(state.recent.len(), 3);
        assert_eq!(state.tip(), Some((4, B256::with_last_byte(4))));
        // Oldest two entries got dropped.
        assert!(state.lookup_hash(B256::with_last_byte(0)).is_none());
        assert!(state.lookup_hash(B256::with_last_byte(1)).is_none());
        assert_eq!(state.lookup_hash(B256::with_last_byte(2)), Some(2));
    }

    #[test]
    fn watcher_state_rewinds() {
        let mut state = WatcherState::new(10);
        for i in 0..8u64 {
            state.push_canonical(
                i,
                B256::with_last_byte(u8::try_from(i).expect("test index fits in u8")),
            );
        }
        state.rewind_to(4);
        assert_eq!(state.tip(), Some((4, B256::with_last_byte(4))));
        assert!(state.lookup_hash(B256::with_last_byte(5)).is_none());
        assert_eq!(state.lookup_hash(B256::with_last_byte(4)), Some(4));
    }

    /// Depth 0 is constructible (pub fields) and would empty the ring.
    #[test]
    fn zero_reorg_depth_still_keeps_a_usable_ring() {
        let mut state = WatcherState::new(0);
        state.push_canonical(9, B256::with_last_byte(9));
        assert_eq!(state.tip(), Some((9, B256::with_last_byte(9))));
    }

    /// Deterministic errors stop the loop; retryable ones must not.
    #[test]
    fn only_deterministic_errors_are_terminal() {
        let depth = L1Error::ReorgTooDeep {
            walked: 62,
            max: 62,
        };
        let incomplete = L1Error::SourceIncomplete {
            block: 1,
            tx_hash: B256::ZERO,
            detail: "warming up".into(),
        };
        assert!(L1Error::Decode("bad abi".into()).is_terminal());
        assert!(depth.is_terminal());
        // A malformed RPC response is retryable — re-requesting can succeed.
        assert!(!L1Error::Provider("log missing block_hash".into()).is_terminal());
        assert!(!incomplete.is_terminal());
    }

    #[test]
    fn watcher_state_lookup_by_hash() {
        let mut state = WatcherState::new(10);
        state.push_canonical(7, B256::with_last_byte(7));
        state.push_canonical(8, B256::with_last_byte(8));
        assert_eq!(state.lookup_hash(B256::with_last_byte(8)), Some(8));
        assert_eq!(state.lookup_hash(B256::with_last_byte(7)), Some(7));
        assert_eq!(state.lookup_hash(B256::with_last_byte(99)), None);
    }
}
