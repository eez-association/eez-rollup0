//! Polls L1 for new heads, `BatchPosted` events, and finalized-head
//! advances. Emits [`L1Event`]s on a [`broadcast::Sender`] for the
//! Deriver and Composer to consume.
//!
//! HTTP polling (reuses `SubmitterConfig::rpc_url`) — 2s interval, well
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
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};
use url::Url;

use crate::error::{L1Error, L1Result};
use crate::submitter::{BatchLogChunks, ScannedBatch};

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
        /// `rollupCount` indexed param from the event.
        rollup_count: U256,
        call_data: Bytes,
        /// `true` iff the same L1 tx emitted `L2ExecutionPerformed` —
        /// the contract's state delta applied. `false` = loser
        /// (`ImmediateEntrySkipped`). Deriver reads this directly.
        state_applied: bool,
        /// `L2ExecutionPerformed` events for our rollupId in the
        /// postBatch's L1 block (counted per-block — deferred entries
        /// emit from the bundled user_txs, not the postBatch tx). 0 =
        /// nothing settled → Deriver skips the batch. Under partial
        /// consumption (a reverting user_tx leaves its entry and the
        /// rest unconsumed) this is the applied prefix: leading
        /// immediate + consumed deferred entries.
        settled_count: usize,
        /// LAST applied entry's `newState` (log order) — L1's ACTUAL
        /// stored root after this batch (a prefix endpoint under partial
        /// consumption). The Deriver validates its replay endpoint
        /// against THIS, never the claimed full-chain end.
        settled_final_state: Option<B256>,
        /// FIRST stateDelta's `currentState` for our rollup. Deriver
        /// compares to local STF result at `from_block - 1` to catch
        /// claimed-vs-derived divergence at the batch entry point.
        claimed_current_state: Option<B256>,
        /// LAST stateDelta's `newState` for our rollup — the claimed
        /// full-chain end. Diagnostics only; see `settled_final_state`.
        claimed_new_state: Option<B256>,
    },
    /// L1 finalized head advanced.
    Finalized { block_number: u64, block_hash: B256 },
}

/// Configuration for the [`L1Watcher`].
#[derive(Debug, Clone)]
pub struct L1WatcherConfig {
    /// L1 RPC endpoint (HTTP / HTTPS). Reuse
    /// [`SubmitterConfig::rpc_url`](crate::SubmitterConfig::rpc_url).
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
    /// Spawns the polling task on the current tokio runtime and returns
    /// a handle.
    #[must_use]
    pub fn spawn(config: L1WatcherConfig) -> Self {
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER);
        let watcher = Self {
            inner: Arc::new(Inner { config, event_tx }),
        };
        let runner = watcher.clone();
        tokio::spawn(async move {
            runner.run().await;
        });
        watcher
    }

    /// Returns a new receiver subscribed to all future [`L1Event`]s.
    /// Each call creates an independent receiver; events broadcast to
    /// all of them.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<L1Event> {
        self.inner.event_tx.subscribe()
    }

    async fn run(self) {
        let provider = ProviderBuilder::new().connect_http(self.inner.config.rpc_url.clone());
        let mut state = WatcherState::new(self.inner.config.reorg_max_depth);
        let mut ticker = interval_at(Instant::now() + POLL_INTERVAL, POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut tick_count: u64 = 0;

        event!(
            name: "eez.l1_watcher.started",
            Level::INFO,
            eez = %self.inner.config.eez,
            poll_interval_secs = POLL_INTERVAL.as_secs(),
            reorg_max_depth = self.inner.config.reorg_max_depth,
            "L1 watcher polling started",
        );

        loop {
            ticker.tick().await;
            tick_count += 1;
            if let Err(err) = self.poll_cycle(&provider, &mut state, tick_count).await {
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

    /// One poll cycle. Far-behind catch-up keeps its chunk cursor in
    /// [`WatcherState`], so a transient scan failure retries the failed
    /// tail instead of restarting from the stale ring tip.
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
        event!(
            name: "eez.l1_watcher.poll.tick",
            Level::INFO,
            tick = tick_count,
            latest_number,
            tip = ?state.tip().map(|(n, _)| n),
            "poll cycle: fetched latest L1 block",
        );

        if state.pending_catch_up.is_some()
            && self
                .resume_pending_catch_up(provider, state, tick_count)
                .await?
        {
            return Ok(());
        }

        match state.tip() {
            // First poll — seed the ring with the latest, emit NewHead.
            None => {
                event!(
                    name: "eez.l1_watcher.poll.seed",
                    Level::INFO,
                    tick = tick_count,
                    latest_number,
                    "first poll: seeding canonical ring at latest L1 block",
                );
                state.push_canonical(latest_number, latest_hash);
                self.emit(L1Event::NewHead {
                    block_number: latest_number,
                    block_hash: latest_hash,
                    timestamp: latest.timestamp,
                });
                self.scan_batch_posted(provider, latest_number, latest_number, latest_hash)
                    .await?;
            }
            // No change since last poll.
            Some((_, tip_hash)) if tip_hash == latest_hash => {}
            // Normal extension by exactly one block.
            Some((tip_number, tip_hash))
                if tip_hash == latest_parent && latest_number == tip_number + 1 =>
            {
                state.push_canonical(latest_number, latest_hash);
                self.emit(L1Event::NewHead {
                    block_number: latest_number,
                    block_hash: latest_hash,
                    timestamp: latest.timestamp,
                });
                self.scan_batch_posted(provider, latest_number, latest_number, latest_hash)
                    .await?;
            }
            // Either a reorg or a multi-block gap. Walk back via
            // parent_hash links until we hit a hash in our ring (common
            // ancestor) or exceed reorg_max_depth.
            Some(_) => {
                let walked = walk_back_to_common(
                    provider,
                    latest_parent,
                    latest_number.saturating_sub(1),
                    state,
                )
                .await?;
                let (old_tip_number, old_tip_hash) =
                    state.tip().expect("tip is Some in this branch");

                let Some(common) = walked else {
                    // Walk-back exhausted depth without a common
                    // ancestor. Either the tip is far enough behind
                    // latest that the walk never reached it (benign
                    // catch-up) or it's within reorg_max_depth and
                    // still unmatched (genuine deep reorg → halt).
                    let depth = state.reorg_max_depth as u64;
                    if latest_number > old_tip_number.saturating_add(depth) {
                        // "Far behind" doesn't prove the old tip is
                        // still canonical — swallowing a reorg across
                        // the gap is the silent fallback invariant 7
                        // forbids. Verify by height first.
                        let at_old_height =
                            fetch_block_by_tag(provider, BlockNumberOrTag::Number(old_tip_number))
                                .await?;
                        let reorged_across_gap = at_old_height.hash != old_tip_hash;
                        let (scan_from, pending_reorg) = if reorged_across_gap {
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
                                 longer canonical; scanning BatchPosted before \
                                 emitting Reorg and reseeding",
                            );
                            (
                                common.number + 1,
                                Some(PendingReorg {
                                    common_ancestor_number: common.number,
                                    common_ancestor_hash: common.hash,
                                    old_head_hash: old_tip_hash,
                                    new_head_number: latest_number,
                                    new_head_hash: latest_hash,
                                }),
                            )
                        } else {
                            event!(
                                name: "eez.l1_watcher.poll.catchup",
                                Level::INFO,
                                tick = tick_count,
                                old_tip_number,
                                latest_number,
                                reorg_max_depth = state.reorg_max_depth,
                                "tip is far behind latest beyond reorg_max_depth \
                                 and still canonical — treating as catch-up, \
                                 scanning BatchPosted and reseeding ring at \
                                 latest",
                            );
                            (old_tip_number + 1, None)
                        };
                        event!(
                            name: "eez.l1_watcher.catch_up_scan.started",
                            Level::INFO,
                            tick = tick_count,
                            scan_from,
                            scan_to = latest_number,
                            old_tip_number,
                            old_tip_hash = %old_tip_hash,
                            reorged_across_gap,
                            "starting resumable far-behind BatchPosted scan",
                        );
                        state.pending_catch_up = Some(PendingCatchUpScan {
                            old_tip_number,
                            old_tip_hash,
                            scan_from,
                            target_number: latest_number,
                            target_hash: latest_hash,
                            target_timestamp: latest.timestamp,
                            reorged_across_gap,
                            pending_reorg,
                            chunks: BatchLogChunks::new(scan_from, latest_number),
                            scanned: Vec::new(),
                        });
                        self.resume_pending_catch_up(provider, state, tick_count)
                            .await?;
                        return Ok(());
                    }
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
                self.fill_forward(provider, scan_from, latest_number, latest_hash, state)
                    .await?;
                self.scan_batch_posted(provider, scan_from, latest_number, latest_hash)
                    .await?;
            }
        }

        if tick_count % FINALIZED_REFRESH_TICKS == 0 {
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

    /// Fetches `BatchPosted` logs in `[from, to]` via the shared
    /// chunked scanner and emits one [`L1Event::BatchPosted`] per log.
    async fn scan_batch_posted(
        &self,
        provider: &impl Provider,
        from: u64,
        to: u64,
        _to_hash: B256,
    ) -> L1Result<()> {
        event!(
            name: "eez.l1_watcher.scan_batch_posted",
            Level::INFO,
            from,
            to,
            "scanning L1 range for BatchPosted logs",
        );
        let mut chunks = BatchLogChunks::new(from, to);
        while let Some(scanned) = self
            .scan_next_batch_log_chunk(provider, &mut chunks)
            .await?
        {
            self.emit_scanned_batches(from, to, scanned);
        }
        Ok(())
    }

    async fn resume_pending_catch_up(
        &self,
        provider: &impl Provider,
        state: &mut WatcherState,
        tick_count: u64,
    ) -> L1Result<bool> {
        let Some(pending) = state.pending_catch_up.as_ref() else {
            return Ok(false);
        };
        if !catch_up_target_is_canonical(provider, pending.target_number, pending.target_hash)
            .await?
        {
            let pending = state
                .pending_catch_up
                .take()
                .expect("pending catch-up existed above");
            event!(
                name: "eez.l1_watcher.catch_up_scan.target_stale",
                Level::WARN,
                tick = tick_count,
                target_number = pending.target_number,
                target_hash = %pending.target_hash,
                "catch-up scan target is no longer canonical; discarding scan progress",
            );
            return Ok(false);
        }

        loop {
            let scanned = {
                let pending = state
                    .pending_catch_up
                    .as_mut()
                    .expect("pending catch-up exists while resuming");
                self.scan_next_batch_log_chunk(provider, &mut pending.chunks)
                    .await?
            };
            let Some(mut scanned) = scanned else {
                break;
            };
            let pending = state
                .pending_catch_up
                .as_mut()
                .expect("pending catch-up exists while collecting");
            if !scanned.is_empty() {
                event!(
                    name: "eez.l1_watcher.catch_up_scan.chunk_found",
                    Level::INFO,
                    tick = tick_count,
                    target_number = pending.target_number,
                    count = scanned.len(),
                    "queued BatchPosted events from catch-up scan chunk",
                );
            }
            pending.scanned.append(&mut scanned);
        }

        let pending = state
            .pending_catch_up
            .take()
            .expect("pending catch-up exists after scan completion");
        if !catch_up_target_is_canonical(provider, pending.target_number, pending.target_hash)
            .await?
        {
            event!(
                name: "eez.l1_watcher.catch_up_scan.target_stale",
                Level::WARN,
                tick = tick_count,
                target_number = pending.target_number,
                target_hash = %pending.target_hash,
                "catch-up scan target changed before completion; discarding scanned events",
            );
            return Ok(false);
        }

        if let Some(reorg) = pending.pending_reorg {
            self.emit(L1Event::Reorg {
                common_ancestor_number: reorg.common_ancestor_number,
                common_ancestor_hash: reorg.common_ancestor_hash,
                old_head_hash: reorg.old_head_hash,
                new_head_number: reorg.new_head_number,
                new_head_hash: reorg.new_head_hash,
            });
        }
        self.emit_scanned_batches(pending.scan_from, pending.target_number, pending.scanned);

        event!(
            name: "eez.l1_watcher.ring.rewind",
            Level::WARN,
            tick = tick_count,
            old_tip_number = pending.old_tip_number,
            old_tip_hash = %pending.old_tip_hash,
            new_tip_number = pending.target_number,
            new_tip_hash = %pending.target_hash,
            reorged_across_gap = pending.reorged_across_gap,
            "reseeding ring at latest — dropping all prior ring entries",
        );
        state.rewind_to(0);
        state.push_canonical(pending.target_number, pending.target_hash);
        self.emit(L1Event::NewHead {
            block_number: pending.target_number,
            block_hash: pending.target_hash,
            timestamp: pending.target_timestamp,
        });
        Ok(true)
    }

    async fn scan_next_batch_log_chunk(
        &self,
        provider: &impl Provider,
        chunks: &mut BatchLogChunks,
    ) -> L1Result<Option<Vec<ScannedBatch>>> {
        crate::submitter::scan_next_batch_log_chunk(
            provider,
            self.inner.config.eez,
            self.inner.config.rollup_id,
            chunks,
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
                rollup_count: b.rollup_count,
                call_data: b.call_data,
                state_applied: b.state_applied,
                settled_count: b.settled_count,
                settled_final_state: b.settled_final_state,
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
    pending_catch_up: Option<PendingCatchUpScan>,
}

impl WatcherState {
    fn new(reorg_max_depth: usize) -> Self {
        Self {
            recent: VecDeque::with_capacity(reorg_max_depth),
            reorg_max_depth,
            last_finalized_hash: None,
            pending_catch_up: None,
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

#[derive(Debug)]
struct PendingCatchUpScan {
    old_tip_number: u64,
    old_tip_hash: B256,
    scan_from: u64,
    target_number: u64,
    target_hash: B256,
    target_timestamp: u64,
    reorged_across_gap: bool,
    pending_reorg: Option<PendingReorg>,
    chunks: BatchLogChunks,
    scanned: Vec<ScannedBatch>,
}

#[derive(Debug, Clone, Copy)]
struct PendingReorg {
    common_ancestor_number: u64,
    common_ancestor_hash: B256,
    old_head_hash: B256,
    new_head_number: u64,
    new_head_hash: B256,
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

async fn catch_up_target_is_canonical(
    provider: &impl Provider,
    number: u64,
    hash: B256,
) -> L1Result<bool> {
    Ok(
        fetch_block_by_tag(provider, BlockNumberOrTag::Number(number))
            .await?
            .hash
            == hash,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
