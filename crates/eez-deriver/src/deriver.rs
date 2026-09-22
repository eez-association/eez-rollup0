//! L1-event-driven L2 consensus: replays `BatchPosted` events into
//! local reth, advances safe/finalized via [`BlockCommitterHandle`],
//! and maintains a per-batch index so [`L1Event::Reorg`] retreats
//! the safe head and [`L1Event::Finalized`] advances finalized.
//!
//! STF-replay pattern adapted from `based-rollup`'s `build_derived_block`
//! at `/root/sync-rollups-composer/crates/based-rollup/src/driver/protocol_txs.rs:453`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_eips::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, B256, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use eez_driver::{BUILDER_GAS_LIMIT, BlockCommitterHandle, DeriveOutcome};
use eez_evm::EezEvmConfig;
use eez_l1::{BatchRecord, L1CanonicalHead, L1Event, L1Reader, ScannedBatch};
use eez_primitives::EezTxEnvelope as TransactionSigned;
use eez_primitives::engine::EezEngineTypes;
use eez_protocol::outbound_gate::OutboundCallObservation;
use reth_chainspec::{ChainSpec, EthereumHardforks};
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes, execute::BlockBuilder};
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody, SealedHeader, SignedTransaction};
use reth_provider::StateProviderFactory;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{BlockReader, ReceiptProvider, TransactionsProvider};
use revm::database::State;
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::error::{DeriverError, DeriverResult};

/// Per-block header inputs the composer chose, carried in the DA span. Read
/// from the payload, not assumed — a constant diverges the moment the composer
/// picks something else.
#[derive(Debug, Clone)]
pub struct BlockHeaderInputs {
    pub beneficiary: Address,
    pub extra_data: Bytes,
}

impl BlockHeaderInputs {
    /// The inputs the span carries for its `index`-th block.
    fn from_span(span: &eez_payload_codec::DecodedSpan, index: usize) -> DeriverResult<Self> {
        let (Some(beneficiary), Some(extra_data)) =
            (span.beneficiaries.get(index), span.extra_data.get(index))
        else {
            return Err(DeriverError::l2_provider(format!(
                "DA span has no header inputs for block index {index} (span covers {} blocks)",
                span.block_count(),
            )));
        };
        Ok(Self {
            beneficiary: Address::from(*beneficiary),
            extra_data: Bytes::copy_from_slice(extra_data),
        })
    }
}

/// Watcher seed: the finalized block, kept inside the range this scan read.
/// Both bounds have wedged boot in the field. Separate fn so they stay
/// unit-testable without a provider.
fn choose_seed(floor: u64, end: u64, finalized: Option<u64>) -> u64 {
    // `floor.min(end)` is load-bearing: `clamp` PANICS when min > max, which a
    // rewound L1 can produce.
    finalized.unwrap_or(floor).clamp(floor.min(end), end)
}

/// L1-derived L2 consensus engine. Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct Deriver<L2>
where
    L2: BlockReader,
{
    inner: Arc<Inner<L2>>,
}

struct Inner<L2>
where
    L2: BlockReader,
{
    committer: BlockCommitterHandle<EezEngineTypes>,
    l2_provider: Arc<L2>,
    l1_reader: L1Reader,
    evm_config: EezEvmConfig,
    /// Chainspec-aware deriver
    chain_spec: Arc<ChainSpec>,
    /// L2 block time in seconds — `execute_block` derives each block's
    /// timestamp from its parent's. Must match the sequencer's cadence
    /// (`RollupTiming::l2_block_time_ms`); a mismatch yields different
    /// block hashes for byte-identical txs/state (composer↔follower
    /// divergence).
    l2_block_time_secs: u64,
    deploy_block: u64,
    /// Shared canonical-head state — cursor + per-batch index +
    /// `finalized_l2`. The Deriver is the sole writer; the Composer
    /// reads `cursor()` to compute the next batch's `from_block`.
    l1_head: Arc<L1CanonicalHead>,
    /// L2 block number currently reth `safe` head points at. Mirrors
    /// what we last passed to [`BlockCommitterHandle::advance_safe_finalized`];
    /// used to compute the FCU when advancing finalized without
    /// disturbing safe (and vice versa).
    safe_l2_block: AtomicU64,
    /// Cross-chain system-tx reconstruction config. `Some` enables
    /// L1-entries → L2 system-tx prepending in `reconcile_batch_blocks`;
    /// `None` falls back to pure-user-tx STF. See [`Deriver::new`] docs.
    system_tx_cfg: Option<eez_protocol::system_tx::SystemTxContext>,
    /// L2 datadir holding the boot checkpoint. `None` disables it, which is
    /// the old behaviour: boot rescans from the deploy block.
    checkpoint_dir: Option<PathBuf>,
}

impl<L2> std::fmt::Debug for Deriver<L2>
where
    L2: BlockReader,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deriver")
            .field("cursor", &self.inner.l1_head.cursor())
            .field(
                "safe_l2_block",
                &self.inner.safe_l2_block.load(Ordering::Acquire),
            )
            .field("finalized_l2_block", &self.inner.l1_head.finalized_l2())
            .field("committer", &self.inner.committer)
            .finish_non_exhaustive()
    }
}

impl<L2> Deriver<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Send
        + Sync
        + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
    <L2 as ReceiptProvider>::Receipt: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
{
    /// Builds a deriver. Cursor + per-batch index are populated lazily
    /// by `catch_up_to`, which walks historical `BatchPosted` events
    /// applying the same linearity check live events get — so losers
    /// (competing batches whose `currentState` no longer matches the
    /// cursor) don't pollute the index.
    ///
    /// `system_tx_cfg = Some(_)` enables the cross-chain STF path: the
    /// deriver reconstructs the same native system transactions
    /// the composer produced (from the postBatch's `entries[]` /
    /// `l2_entries[]`) and prepends them to the batch's Sync block, so
    /// local replay is byte-identical. `None` is the pure-user-tx STF.
    pub fn new(
        committer: BlockCommitterHandle<EezEngineTypes>,
        l2_provider: Arc<L2>,
        l1_reader: L1Reader,
        chain_spec: Arc<ChainSpec>,
        l2_block_time_secs: u64,
        deploy_block: u64,
        l1_head: Arc<L1CanonicalHead>,
        system_tx_cfg: Option<eez_protocol::system_tx::SystemTxContext>,
        checkpoint_dir: Option<PathBuf>,
    ) -> Self {
        let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            inner: Arc::new(Inner {
                committer,
                l2_provider,
                l1_reader,
                evm_config,
                chain_spec,
                l2_block_time_secs,
                deploy_block,
                l1_head,
                safe_l2_block: AtomicU64::new(0),
                system_tx_cfg,
                checkpoint_dir,
            }),
        }
    }

    /// Record the last indexed batch so the next boot resumes there. Best
    /// effort: a write failure costs a slow boot, never correctness.
    fn save_checkpoint(&self) {
        let Some(dir) = self.inner.checkpoint_dir.as_deref() else {
            return;
        };
        let Some(tail) = self.inner.l1_head.last_indexed() else {
            return;
        };
        // A resume seeds only this batch's hash, so an earlier batch in the same
        // block would replay with the cursor past it; a slow boot beats a wrong one.
        if self.inner.l1_head.count_at_l1_block(tail.l1_block) > 1 {
            event!(
                name: "eez.deriver.checkpoint.multi_batch_block",
                Level::DEBUG,
                l1_block = tail.l1_block,
                "L1 block holds more than one indexed batch; skipping the boot checkpoint",
            );
            return;
        }
        let l2_block_hash = match self.l2_hash_at(tail.last_l2_block) {
            Ok(root) => root,
            Err(err) => {
                event!(
                    name: "eez.deriver.checkpoint.no_local_root",
                    Level::WARN,
                    l2_block = tail.last_l2_block,
                    error = %err,
                    "no local root at the indexed tip; skipping the boot checkpoint",
                );
                return;
            }
        };
        let checkpoint = crate::checkpoint::ReconcileCheckpoint {
            l1_block: tail.l1_block,
            l1_block_hash: tail.l1_block_hash,
            tx_hash: tail.tx_hash,
            l2_cursor: tail.last_l2_block,
            l2_block_hash,
        };
        if let Err(err) = checkpoint.save(dir) {
            event!(
                name: "eez.deriver.checkpoint.write_failed",
                Level::WARN,
                dir = %dir.display(),
                error = %err,
                "boot checkpoint not written; the next boot rescans from the deploy block",
            );
        }
    }

    /// Bounded wait for the L1 head to reach `block`. Best effort: on timeout
    /// the caller reads whatever L1 serves and the usual validation decides.
    async fn await_l1_head(&self, block: u64) {
        const POLL: Duration = Duration::from_secs(2);
        const POLLS: u32 = 30;
        for _ in 0..POLLS {
            match self.inner.l1_reader.readiness().await {
                Ok(state) if state.head_block_number >= block => return,
                _ => tokio::time::sleep(POLL).await,
            }
        }
        event!(
            name: "eez.deriver.checkpoint.l1_behind",
            Level::WARN,
            l1_block = block,
            "L1 head still below the checkpoint block; validating anyway",
        );
    }

    /// Checkpoint to seed the boot scan from, or `None` to rescan from the
    /// deploy block. [`ReconcileCheckpoint::usable_with`] decides.
    async fn checkpoint_seed(&self) -> Option<crate::checkpoint::ReconcileCheckpoint> {
        let checkpoint =
            crate::checkpoint::ReconcileCheckpoint::load(self.inner.checkpoint_dir.as_deref()?)?;
        // The checkpoint names the tip we last indexed. A restart brings the L1
        // back with us, so for a few seconds its head sits below that block and
        // every hash read is `None` — discarding the checkpoint then is a
        // verdict about timing, not canonicality. Once the head is level, a
        // missing or different hash is real reorg evidence and still rejects.
        self.await_l1_head(checkpoint.l1_block).await;
        let canonical = self
            .inner
            .l1_reader
            .canonical_l1_hash(checkpoint.l1_block)
            .await
            .ok()
            .flatten();
        let local_hash = self.l2_hash_at(checkpoint.l2_cursor).ok();
        if let Err(reason) = checkpoint.usable_with(canonical, local_hash) {
            event!(
                name: "eez.deriver.checkpoint.rejected",
                Level::WARN,
                l1_block = checkpoint.l1_block,
                l2_cursor = checkpoint.l2_cursor,
                canonical = ?canonical,
                local_hash = ?local_hash,
                reason,
                "boot checkpoint rejected; rescanning from the deploy block",
            );
            return None;
        }
        event!(
            name: "eez.deriver.checkpoint.seeded",
            Level::INFO,
            l1_block = checkpoint.l1_block,
            l2_cursor = checkpoint.l2_cursor,
            "boot seeded from checkpoint; scanning forward instead of from the deploy block",
        );
        Some(checkpoint)
    }

    /// Current cursor — highest L2 block confirmed by any L1-landed
    /// batch. Reads through the shared [`L1CanonicalHead`].
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.inner.l1_head.cursor()
    }

    /// Reorg-aware catch-up from the latest canonical L1 batch already
    /// indexed locally, or from the registry deploy block if the index is
    /// empty. Scans historical `BatchPosted` events in chunks, replaying
    /// non-matching L2 blocks and populating `L1CanonicalHead`.
    ///
    /// # Errors
    ///
    /// `l1_scan` (scan failure), `l2_provider` (lookup failure),
    /// `local_diverged` (replay failure), `committer_closed`.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub async fn catch_up(&self) -> DeriverResult<()> {
        self.catch_up_inner().await.map(|_| ())
    }

    /// [`Self::catch_up`], additionally returning the `L1Watcher::polling`
    /// seed: the finalized block, kept inside the range this scan read so
    /// the seed is immutable and always servable. Boot-only.
    ///
    /// # Errors
    ///
    /// As [`Self::catch_up`]; additionally `SourceIncomplete` while the
    /// L1 source cannot yet serve the seed block's hash.
    pub async fn catch_up_with_seed(&self) -> DeriverResult<(u64, B256)> {
        let end = self.catch_up_inner().await?;
        // NOT canonicality-probed: at boot this tail is a batch THIS scan just
        // found, with a hash straight from `get_logs`. Cross-checked below.
        let indexed_tail = self.inner.l1_head.last_indexed();
        let floor = indexed_tail
            .as_ref()
            .map_or_else(|| self.inner.deploy_block.saturating_sub(1), |t| t.l1_block);
        let finalized = self
            .inner
            .l1_reader
            .finalized_block()
            .await
            .map_err(DeriverError::l1_scan)?;
        // No finality yet (a chain younger than two epochs) → the floor, which
        // this scan read and the watcher's ancestor backfill makes reorg-safe.
        let seed = choose_seed(floor, end, finalized.map(|(n, _)| n));
        let canonical = self
            .inner
            .l1_reader
            .canonical_l1_hash(seed)
            .await
            .map_err(DeriverError::l1_scan)?
            .ok_or_else(|| {
                // Not served yet, or rewound mid-scan — retryable, not fatal.
                DeriverError::l1_scan(eez_l1::L1Error::SourceIncomplete {
                    block: seed,
                    tx_hash: B256::ZERO,
                    detail: "catch-up seed block not served by the L1 source yet".into(),
                })
            })?;
        // A batch this scan indexed at the seed height sitting on another fork
        // means the chain moved under us: retry so `revalidate_index_tail` drops
        // it. (A finalized seed needs no such check — it cannot reorg.)
        if indexed_tail.is_some_and(|t| t.l1_block == seed && t.l1_block_hash != canonical) {
            return Err(DeriverError::l1_scan(eez_l1::L1Error::SourceIncomplete {
                block: seed,
                tx_hash: B256::ZERO,
                detail: "catch-up seed block reorged during the scan; retry".into(),
            }));
        }
        event!(
            name: "eez.deriver.catch_up.seed",
            Level::INFO,
            floor,
            scan_end = end,
            finalized = ?finalized.map(|(n, _)| n),
            seed,
            seed_hash = %canonical,
            "watcher seed chosen",
        );
        Ok((seed, canonical))
    }

    /// Shared body of [`Self::catch_up`] / [`Self::catch_up_with_seed`].
    /// Returns the inclusive L1 block covered: the scan's endpoint, or the
    /// tip when the range was empty (nothing below it can hold our events).
    async fn catch_up_inner(&self) -> DeriverResult<u64> {
        let _guard = self.inner.committer.begin_reconcile().await;
        let old_cursor = self.inner.l1_head.cursor();
        let anchor = self.revalidate_index_tail().await?;
        let cursor = self.inner.l1_head.cursor();
        if cursor < old_cursor {
            self.retreat_l2_to_cursor(cursor).await?;
        }
        match anchor {
            Some(anchor_l1_block) => self.sync_batches_inner(anchor_l1_block, cursor).await,
            // Every boot has an empty index. The checkpoint keeps the scan
            // short (`docs/issues/deep-reverify-cost.md`).
            None => match self.checkpoint_seed().await {
                Some(seed) => {
                    self.inner.l1_head.append(BatchRecord {
                        l1_block: seed.l1_block,
                        l1_block_hash: seed.l1_block_hash,
                        tx_hash: seed.tx_hash,
                        last_l2_block: seed.l2_cursor,
                    });
                    self.sync_batches_inner(seed.l1_block, seed.l2_cursor).await
                }
                None => self.sync_batches_inner(self.inner.deploy_block, 0).await,
            },
        }
    }

    /// Phase 1 of [`Self::catch_up`]: walk the index tail backward,
    /// dropping batches whose recorded L1 hash is no longer canonical.
    /// Returns the highest still-canonical batch's L1 block (the rescan
    /// lower bound), or `None` if the index is empty. Caller holds the
    /// reconcile lock.
    async fn revalidate_index_tail(&self) -> DeriverResult<Option<u64>> {
        while let Some(tail) = self.inner.l1_head.last_indexed() {
            let canonical = self
                .inner
                .l1_reader
                .canonical_l1_hash(tail.l1_block)
                .await
                .map_err(DeriverError::l1_scan)?;
            // `None` is not proof of a reorg — retreating would unwind L2 to
            // genesis, so retry. A head ABOVE this block means pruned/rewound,
            // which retrying won't fix; report it so that is diagnosable.
            let Some(canonical) = canonical else {
                let head = self
                    .inner
                    .l1_reader
                    .readiness()
                    .await
                    .map(|r| r.head_block_number)
                    .ok();
                return Err(DeriverError::l1_scan(eez_l1::L1Error::SourceIncomplete {
                    block: tail.l1_block,
                    tx_hash: tail.tx_hash,
                    detail: format!(
                        "indexed batch's L1 block not served; cannot judge canonicality \
                         (source head: {head:?} — above this block means pruned or rewound, \
                         not lagging)"
                    ),
                }));
            };
            if canonical == tail.l1_block_hash {
                return Ok(Some(tail.l1_block));
            }
            let old_cursor = self.inner.l1_head.cursor();
            let (new_cursor, _new_finalized, dropped) = self
                .inner
                .l1_head
                .retreat_on_l1_reorg(tail.l1_block.saturating_sub(1));
            event!(
                name: "eez.deriver.l1.reorg.retreated",
                Level::WARN,
                event_name = "eez.deriver.l1.reorg.retreated",
                l1_block = tail.l1_block,
                indexed_hash = %tail.l1_block_hash,
                canonical_hash = %canonical,
                old_cursor,
                new_cursor,
                dropped_batches = dropped,
                "L1 reorg rolled out confirmed batches; L2 safe cursor retreated",
            );
        }
        Ok(None)
    }

    /// Scan `BatchPosted` from `from_l1_block`, reconciling and committing
    /// each successful L1 chunk before fetching the next. If a later chunk
    /// reports an incomplete source, the next catch-up retry can resume from
    /// the latest canonical batch already indexed in [`L1CanonicalHead`].
    /// Returns the inclusive L1 block the scan covered through.
    async fn sync_batches_inner(
        &self,
        from_l1_block: u64,
        cumulative_start: u64,
    ) -> DeriverResult<u64> {
        let local_head = self
            .inner
            .l2_provider
            .best_block_number()
            .map_err(DeriverError::l2_provider)?;
        let mut chunks = self
            .inner
            .l1_reader
            .batch_log_chunks(from_l1_block)
            .await
            .map_err(DeriverError::l1_scan)?;
        let to_l1_block = chunks.to_block();
        event!(
            name: "eez.deriver.catch_up.start",
            Level::INFO,
            local_head,
            from_l1_block,
            to_l1_block,
            cumulative_start,
            "starting batch scan to populate L1CanonicalHead and reconcile L2 chain",
        );

        if chunks.is_empty() {
            event!(
                name: "eez.deriver.catch_up.noop",
                Level::DEBUG,
                cursor = cumulative_start,
                "scan completed without replaying any blocks",
            );
            return Ok(to_l1_block);
        }

        let mut cumulative_l2 = cumulative_start;
        let mut total_replayed: u64 = 0;
        while let Some(scanned_batches) = self
            .inner
            .l1_reader
            .next_batch_log_chunk(&mut chunks)
            .await
            .map_err(DeriverError::l1_scan)?
        {
            total_replayed += self
                .reconcile_scanned_batches(&scanned_batches, &mut cumulative_l2)
                .await?;
        }

        if total_replayed > 0 {
            event!(
                name: "eez.deriver.catch_up.done",
                Level::INFO,
                local_head,
                replayed = total_replayed,
                cursor = cumulative_l2,
                "catch-up replay complete",
            );
        } else {
            event!(
                name: "eez.deriver.catch_up.noop",
                Level::DEBUG,
                cursor = cumulative_l2,
                "scan completed without replaying any blocks",
            );
        }
        Ok(to_l1_block)
    }

    async fn reconcile_scanned_batches(
        &self,
        scanned_batches: &[ScannedBatch],
        cumulative_l2: &mut u64,
    ) -> DeriverResult<u64> {
        let known_tx_hashes = self.inner.l1_head.known_tx_hashes();
        let mut new_batches: Vec<BatchRecord> = Vec::new();
        let mut total_replayed: u64 = 0;
        for batch in scanned_batches {
            let Some(container) =
                self.decode_our_payload(batch.call_data.as_ref(), batch.l1_block_number)?
            else {
                continue;
            };
            let decoded = &container.span;

            // `settled_count == 0` = nothing applied on L1 (the claimed
            // roots are phantoms). Skip the whole reconcile — no
            // cursor advance, no replay, no state check; the composer's
            // next slot re-attempts over the same range.
            if batch.settlement.is_empty() {
                event!(
                    name: "eez.deriver.catch_up.batch.unsettled",
                    Level::DEBUG,
                    l1_block_number = batch.l1_block_number,
                    tx_hash = %batch.tx_hash,
                    "catch_up: postBatch's L1 block has no L2ExecutionPerformed for our rollup; skipping (re-attempt expected)",
                );
                continue;
            }

            // Already indexed — processed by an earlier sync; its L2
            // range is accounted for in `cumulative_start`.
            if known_tx_hashes.contains(&batch.tx_hash) {
                continue;
            }

            let resumed = batch.settlement.resumed();
            // Anchor on the batch's own start, not our cursor: a competing
            // composer's batch can cover a range that begins below it.
            let anchor = match batch.settlement.entry_state {
                Some(entry_state) => self
                    .batch_anchor(*cumulative_l2, entry_state, decoded.block_count() as u64)?
                    .unwrap_or(*cumulative_l2),
                None => *cumulative_l2,
            };
            let (batch_first_l2, batch_last_l2) =
                batch_l2_range(anchor, resumed, decoded.block_count() as u64);

            // Cursor guard (as in on_batch_posted): a root this batch cannot leave at
            // the cursor means the scan is off L1, and replaying would fork us alone.
            if let Some(entry_state) = batch.settlement.entry_state {
                let local_root = self.l2_hash_at(anchor)?;
                if !cursor_root_accepted(local_root, entry_state, batch.settlement.final_state) {
                    event!(
                        name: "eez.deriver.catch_up.cursor.misaligned",
                        Level::ERROR,
                        l1_block_number = batch.l1_block_number,
                        tx_hash = %batch.tx_hash,
                        cumulative_l2 = *cumulative_l2,
                        anchor,
                        local_root = %local_root,
                        entry_state = %entry_state,
                        final_state = ?batch.settlement.final_state,
                        applied_first = ?batch.settlement.first_applied(),
                        "local state root at the scan cursor is neither the root the batch's applied run started from nor its settled endpoint; refusing to replay",
                    );
                    return Err(DeriverError::local_diverged(batch_first_l2));
                }
            }

            let (replayed_here, scan_sync_hash) = self
                .reconcile_batch_blocks(
                    batch_first_l2,
                    decoded,
                    &container.actions,
                    batch.l1_block_number,
                    batch.tx_hash,
                    &batch.settlement,
                )
                .await?;
            total_replayed += replayed_here;

            // Catch drift now, not at a live event. Both ends must be what L1
            // ACTUALLY ran, not the claimed endpoints. Pre-check skipped when
            // resumed: the cursor-alignment guard above already checked it, at
            // `cumulative_l2` — `batch_first_l2 - 1` would check the wrong
            // height now that a resumed batch doesn't advance past it.
            let settled_end = self.check_claimed_state(
                if resumed {
                    None
                } else {
                    batch.settlement.entry_state.or(batch.claimed_current_state)
                },
                batch.settlement.final_state.or(batch.claimed_new_state),
                anchor,
                batch_first_l2,
                batch_last_l2,
                scan_sync_hash,
                batch.l1_block_number,
                batch.tx_hash,
            )?;
            // Follow L1's real endpoint. A partial settlement stops early, and
            // the composer must not anchor past L1's stored root.
            let end = settled_end.unwrap_or(batch_last_l2);
            new_batches.push(BatchRecord {
                l1_block: batch.l1_block_number,
                l1_block_hash: batch.l1_block_hash,
                tx_hash: batch.tx_hash,
                last_l2_block: end,
            });
            *cumulative_l2 = end;
        }

        // Index every batch we walked (de-duped against startup
        // entries) so subsequent live `BatchPosted` events for any
        // of them are skipped as already-processed.
        if !new_batches.is_empty() {
            self.inner.l1_head.append_many(new_batches);
            self.save_checkpoint();
        }

        // Advance safe once per chunk, not per batch: two batches in one L1 block
        // rewrite the same L2 height, so a per-batch hash gets orphaned.
        let old_safe_l2 = self.inner.safe_l2_block.load(Ordering::Acquire);
        if *cumulative_l2 > old_safe_l2 {
            let safe_header = self.l2_sealed_header_at(*cumulative_l2)?;
            let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
            self.inner
                .committer
                .advance_safe_finalized(safe_header, finalized_hash)
                .await?;
            self.inner
                .safe_l2_block
                .store(*cumulative_l2, Ordering::Release);
        }

        Ok(total_replayed)
    }

    /// STF-replay `raw_txs` on top of `parent_block_number`. Timestamp
    /// is `parent.timestamp + 2s` (Rollup-1 spec §1.3).
    ///
    /// # Errors
    ///
    /// `l2_provider` (parent lookup / state / builder failure),
    /// `local_diverged` (tx decode / recover / execute failure).
    pub fn execute_block(
        &self,
        parent_block_number: u64,
        raw_txs: &[Vec<u8>],
        header: &BlockHeaderInputs,
    ) -> DeriverResult<(ExecutionData, SealedHeader<alloy_consensus::Header>)> {
        // Diagnostic: log parent context before touching reth so we can
        // pinpoint failing `state_by_block_hash` lookups.
        let local_best = self
            .inner
            .l2_provider
            .best_block_number()
            .map_err(DeriverError::l2_provider)?;
        event!(
            name: "eez.deriver.execute_block.start",
            Level::DEBUG,
            parent_block_number,
            local_best,
            tx_count = raw_txs.len(),
            "execute_block: looking up parent header",
        );

        let parent_header = self
            .inner
            .l2_provider
            .sealed_header(parent_block_number)
            .map_err(|e| {
                event!(
                    name: "eez.deriver.execute_block.parent_lookup_failed",
                    Level::ERROR,
                    parent_block_number,
                    local_best,
                    error = %e,
                    "sealed_header() failed",
                );
                DeriverError::l2_provider(e)
            })?
            .ok_or_else(|| {
                event!(
                    name: "eez.deriver.execute_block.parent_missing",
                    Level::ERROR,
                    parent_block_number,
                    local_best,
                    "sealed_header() returned None — parent header is not in canonical chain",
                );
                DeriverError::l2_provider(format!(
                    "local L2 header at parent block {parent_block_number} missing"
                ))
            })?;

        let parent_hash = parent_header.hash();
        let timestamp = parent_header
            .timestamp()
            .saturating_add(self.inner.l2_block_time_secs);

        let state_provider = self
            .inner
            .l2_provider
            .state_by_block_hash(parent_hash)
            .map_err(|e| {
                // reth has the *header* for this block (sealed_header
                // succeeded above) but refuses to give us its state.
                // Capture as much context as possible.
                event!(
                    name: "eez.deriver.execute_block.no_state",
                    Level::ERROR,
                    parent_block_number,
                    parent_hash = %parent_hash,
                    local_best,
                    parent_timestamp = parent_header.timestamp(),
                    error = %e,
                    "state_by_block_hash() failed for a header that sealed_header() returned successfully — likely reth state retention timing under reorg churn",
                );
                DeriverError::l2_provider(e)
            })?;
        let state_db = StateProviderDatabase::new(state_provider.as_ref());
        let mut db = State::builder()
            .with_database(state_db)
            .with_bundle_update()
            .build();

        // Chainspec-aware deriver
        // prevents STF mismatches w.r.t. payload builder
        let chain_spec = &self.inner.chain_spec;
        let attributes = NextBlockEnvAttributes {
            timestamp,
            suggested_fee_recipient: header.beneficiary,
            prev_randao: B256::ZERO,
            gas_limit: BUILDER_GAS_LIMIT,
            parent_beacon_block_root: chain_spec
                .is_cancun_active_at_timestamp(timestamp)
                .then_some(B256::ZERO),
            withdrawals: chain_spec
                .is_shanghai_active_at_timestamp(timestamp)
                .then(alloy_eips::eip4895::Withdrawals::default),
            extra_data: header.extra_data.clone(),
            slot_number: None,
        };

        let mut builder = self
            .inner
            .evm_config
            .builder_for_next_block(&mut db, &parent_header, attributes)
            .map_err(|e| {
                DeriverError::l2_provider(format!("builder_for_next_block failed: {e}"))
            })?;

        builder
            .apply_pre_execution_changes()
            .map_err(|e| DeriverError::l2_provider(format!("pre-execution changes failed: {e}")))?;

        for (tx_idx, tx_bytes) in raw_txs.iter().enumerate() {
            let tx = TransactionSigned::decode_2718(&mut tx_bytes.as_slice()).map_err(|e| {
                DeriverError::local_diverged_with_msg(
                    parent_block_number + 1,
                    &format!("decode tx #{tx_idx}: {e}"),
                )
            })?;
            let recovered = SignedTransaction::try_into_recovered(tx).map_err(|_| {
                DeriverError::local_diverged_with_msg(
                    parent_block_number + 1,
                    &format!("could not recover signer for tx #{tx_idx}"),
                )
            })?;
            builder.execute_transaction(recovered).map_err(|e| {
                DeriverError::local_diverged_with_msg(
                    parent_block_number + 1,
                    &format!("execute tx #{tx_idx}: {e}"),
                )
            })?;
        }

        let outcome = builder
            .finish(state_provider.as_ref(), None)
            .map_err(|e| DeriverError::l2_provider(format!("block builder finish failed: {e}")))?;

        let sealed_block = outcome.block.sealed_block().clone();
        let sealed_header = sealed_block.sealed_header().clone();
        let execution_data = <EezEngineTypes as PayloadTypes>::block_to_payload(sealed_block, None);
        Ok((execution_data, sealed_header))
    }

    /// Build + commit one L1-derived L2 block via STF replay.
    ///
    /// # Errors
    ///
    /// Forwards [`Self::execute_block`] errors plus
    /// [`DeriverError::is_invalid_forkchoice`] /
    /// [`DeriverError::is_committer_closed`] from the
    /// committer-side submission.
    pub async fn replay_block(
        &self,
        parent_block_number: u64,
        raw_txs: &[Vec<u8>],
        header_inputs: &BlockHeaderInputs,
    ) -> DeriverResult<DeriveOutcome> {
        let (payload, header) = self.execute_block(parent_block_number, raw_txs, header_inputs)?;
        // feed_witness=false: follower / L1-reconcile re-derive — the producer
        // already fed this block to the prover witness capture; don't double-feed.
        Ok(self
            .inner
            .committer
            .commit_derived(payload, header, false)
            .await?)
    }

    /// Decode a posted DA payload, or `None` when it is not ours.
    ///
    /// Skipped rather than fatal: we read one `ChainOperation`, so a legitimate
    /// multi-rollup batch reads as foreign, and halting would hand a peer a
    /// stop. Unchecked outside cross-chain mode, which configures no rollup id.
    fn decode_our_payload(
        &self,
        call_data: &[u8],
        l1_block_number: u64,
    ) -> DeriverResult<Option<eez_payload_codec::DecodedContainer>> {
        let container = eez_payload_codec::decode_container(call_data)?;
        let ours = self.inner.system_tx_cfg.as_ref().map(|c| c.this_rollup_id);
        if ours.is_some_and(|ours| container.rollup_id != ours) {
            event!(
                name: "eez.deriver.foreign_rollup_payload",
                Level::WARN,
                l1_block_number,
                payload_rollup = container.rollup_id,
                ours = ?ours,
                "DA payload carries another rollup's operations; skipping",
            );
            return Ok(None);
        }
        Ok(Some(container))
    }

    /// Runs the deriver loop, processing each event on `rx` until the
    /// stream closes. `rx` must be subscribed before the `L1Watcher`
    /// starts so no event predates it.
    pub async fn run(self, mut rx: broadcast::Receiver<L1Event>) {
        // Defensive re-anchor: a cheap no-op in the normal boot order;
        // load-bearing for any caller that subscribed rx late.
        if let Err(err) = self.catch_up().await {
            event!(
                name: "eez.deriver.resync.failed",
                Level::ERROR,
                event_name = "eez.deriver.resync.failed",
                error = %err,
                "post-subscribe resync failed; deriver may have a gap",
            );
        }

        event!(
            name: "eez.deriver.started",
            Level::INFO,
            cursor = self.cursor(),
            "deriver loop started",
        );
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Err(err) = self.handle_event(event).await {
                        if err.is_committer_closed() {
                            event!(
                                name: "eez.deriver.committer.closed",
                                Level::ERROR,
                                event_name = "eez.deriver.committer.closed",
                                error = %err,
                                "block committer gone; deriver exiting",
                            );
                            return;
                        }
                        // A dropped event leaves `last_indexed_l2` behind
                        // L1, so later batches replay at the wrong heights —
                        // re-anchor from L1 first.
                        event!(
                            name: "eez.deriver.event.failed",
                            Level::WARN,
                            error = %err,
                            "deriver failed to handle event; resyncing from L1",
                        );
                        if !self.try_recover().await {
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    event!(
                        name: "eez.deriver.l1_events.lagged",
                        Level::WARN,
                        skipped,
                        "L1 event stream lagged; resyncing from L1",
                    );
                    if !self.try_recover().await {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    event!(
                        name: "eez.deriver.l1_events.closed",
                        Level::ERROR,
                        "L1 event stream closed; deriver exiting",
                    );
                    return;
                }
            }
        }
    }

    /// Post-failure recovery via [`Self::catch_up`]. Returns `false` only
    /// when the committer is gone and the loop must exit; a failed resync is
    /// logged and retried at the next L1 event.
    async fn try_recover(&self) -> bool {
        match self.catch_up().await {
            Ok(()) => {
                event!(
                    name: "eez.deriver.resync.recovered",
                    Level::INFO,
                    cursor = self.cursor(),
                    "resync complete; cursor re-anchored to L1",
                );
                true
            }
            Err(err) if err.is_committer_closed() => {
                event!(
                    name: "eez.deriver.committer.closed",
                    Level::ERROR,
                    event_name = "eez.deriver.committer.closed",
                    error = %err,
                    "block committer gone; deriver exiting",
                );
                false
            }
            Err(err) => {
                event!(
                    name: "eez.deriver.resync.failed",
                    Level::ERROR,
                    event_name = "eez.deriver.resync.failed",
                    error = %err,
                    "resync failed; will retry after the next L1 event",
                );
                true
            }
        }
    }

    async fn handle_event(&self, event: L1Event) -> DeriverResult<()> {
        match event {
            L1Event::BatchPosted {
                l1_block_number,
                l1_block_hash,
                tx_hash,
                submitter,
                call_data,
                state_applied,
                settlement,
                claimed_current_state,
                claimed_new_state,
                last_in_l1_block,
            } => {
                self.on_batch_posted(
                    l1_block_number,
                    l1_block_hash,
                    tx_hash,
                    submitter,
                    call_data,
                    state_applied,
                    settlement,
                    claimed_current_state,
                    claimed_new_state,
                    last_in_l1_block,
                )
                .await
            }
            L1Event::NewHead { .. } => Ok(()),
            L1Event::Reorg {
                common_ancestor_number,
                old_head_hash,
                new_head_hash,
                new_head_number,
                ..
            } => {
                self.on_l1_reorg(
                    common_ancestor_number,
                    old_head_hash,
                    new_head_number,
                    new_head_hash,
                )
                .await
            }
            L1Event::Finalized { block_number, .. } => self.on_l1_finalized(block_number).await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn on_batch_posted(
        &self,
        l1_block_number: u64,
        l1_block_hash: B256,
        tx_hash: B256,
        submitter: Address,
        call_data: Bytes,
        state_applied: bool,
        settlement: eez_l1::Settlement,
        claimed_current_state: Option<B256>,
        claimed_new_state: Option<B256>,
        last_in_l1_block: bool,
    ) -> DeriverResult<()> {
        let Some(container) = self.decode_our_payload(call_data.as_ref(), l1_block_number)? else {
            return self.flush_deferred_safe(last_in_l1_block).await;
        };
        let decoded = &container.span;
        let block_count = decoded.block_count() as u64;
        if block_count == 0 {
            return self.flush_deferred_safe(last_in_l1_block).await;
        }

        // Dedup by tx_hash — already-indexed = already processed by
        // catch_up; this is a stale live event.
        if self.inner.l1_head.contains_l1_tx(&tx_hash) {
            event!(
                name: "eez.deriver.batch_posted.skipped",
                Level::DEBUG,
                l1_block_number,
                tx_hash = %tx_hash,
                "live event for an already-indexed batch; skipping",
            );
            return self.flush_deferred_safe(last_in_l1_block).await;
        }

        // L1-finality gate: no `L2ExecutionPerformed` for our rollupId
        // means the bundle didn't settle — L1's stored root didn't
        // advance and the claimed roots are phantoms. Skip; the
        // composer's next slot re-attempts. Without this gate the
        // deriver would replay the unsettled range, reorg L2, then fail
        // `check_claimed_state` as a spurious divergence.
        if settlement.is_empty() {
            event!(
                name: "eez.deriver.batch.unsettled",
                Level::INFO,
                l1_block_number,
                tx_hash = %tx_hash,
                submitter = %submitter,
                "postBatch's L1 block has no L2ExecutionPerformed for our rollup; bundle didn't settle, skipping (re-attempt expected)",
            );
            return self.flush_deferred_safe(last_in_l1_block).await;
        }

        event!(
            name: "eez.deriver.batch_posted.entered",
            Level::INFO,
            l1_block_number,
            tx_hash = %tx_hash,
            state_applied,
            "on_batch_posted entered; awaiting reconcile lock",
        );
        // Acquire lock to prevent sequencing during multi-block derivation
        let _guard = self.inner.committer.begin_reconcile().await;
        event!(
            name: "eez.deriver.batch_posted.lock_acquired",
            Level::INFO,
            l1_block_number,
            tx_hash = %tx_hash,
            state_applied,
            "reconcile lock acquired",
        );

        // `state_applied` only catches the IMMEDIATE-entry path, where
        // `_applyStateUpdates` fires in the postBatch tx itself. In the
        // DEFERRED-entry path (our setter / deposit flow) it fires later
        // inside the user_tx calling `executeCrossChainCall` — a
        // different tx hash in the same L1 block — so the batch-log scanner
        // reports `state_applied=false`. The `settled_count` gate above
        // already confirmed something settled, so we still process.
        if !state_applied {
            event!(
                name: "eez.deriver.batch.deferred_path",
                Level::INFO,
                l1_block_number,
                tx_hash = %tx_hash,
                submitter = %submitter,
                "no L2ExecutionPerformed in postBatch tx — deferred-entry flow; bundled user_tx settled the state",
            );
        }

        let last_indexed_l2 = self.inner.l1_head.last_indexed_l2();
        let resumed = settlement.resumed();
        // Anchor on the batch's own start, not our cursor: a competing
        // composer's batch can cover a range that begins below it.
        let anchor = match settlement.entry_state {
            Some(entry_state) => self
                .batch_anchor(last_indexed_l2, entry_state, block_count)?
                .unwrap_or(last_indexed_l2),
            None => last_indexed_l2,
        };
        let (from_block, to_block) = batch_l2_range(anchor, resumed, block_count);

        // Cursor guard: a root this batch cannot leave at the cursor means the local
        // index is off L1 (e.g. a dropped event); let the run-loop resync re-anchor.
        if let Some(entry_state) = settlement.entry_state {
            let local_root = self.l2_hash_at(anchor)?;
            if !cursor_root_accepted(local_root, entry_state, settlement.final_state) {
                event!(
                    name: "eez.deriver.cursor.misaligned",
                    Level::ERROR,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    last_indexed_l2,
                    anchor,
                    local_root = %local_root,
                    entry_state = %entry_state,
                    final_state = ?settlement.final_state,
                    applied_first = ?settlement.first_applied(),
                    "local state root at cursor is neither the root the batch's applied run started from nor its settled endpoint; resync required",
                );
                return Err(DeriverError::local_diverged(from_block));
            }
        }

        // Per-block reconciliation: skip blocks whose tx lists already
        // match the batch, and STF-replay the rest (reth fork-switches
        // as needed).
        let (replayed, sync_block_hash) = self
            .reconcile_batch_blocks(
                from_block,
                decoded,
                &container.actions,
                l1_block_number,
                tx_hash,
                &settlement,
            )
            .await?;
        event!(
            name: "eez.deriver.reconcile.done",
            Level::DEBUG,
            l1_block_number,
            tx_hash = %tx_hash,
            from_block,
            to_block,
            replayed,
            "per-block reconciliation complete (pre-divergence check)",
        );

        // Both ends are what L1 ACTUALLY ran, never the claimed chain's endpoints.
        // Pre-check skipped when resumed: the cursor-alignment guard above
        // already checked it, at `last_indexed_l2` — `from_block - 1` would
        // check the wrong height now that a resumed batch doesn't advance past it.
        let settled_end = self.check_claimed_state(
            if resumed {
                None
            } else {
                settlement.entry_state.or(claimed_current_state)
            },
            settlement.final_state.or(claimed_new_state),
            anchor,
            from_block,
            to_block,
            sync_block_hash,
            l1_block_number,
            tx_hash,
        )?;

        let l1_settled_commitment = settlement.final_state.unwrap_or_default();
        // Index first: the safe advance reads this cursor, and a replayed batch
        // must stay indexed even if the FCU fails. Use L1's real endpoint.
        self.inner.l1_head.append(BatchRecord {
            l1_block: l1_block_number,
            l1_block_hash,
            tx_hash,
            last_l2_block: settled_end.unwrap_or(to_block),
        });
        // After the index, so the checkpoint never names a batch the index does
        // not hold; the blocks it points at are already committed locally.
        self.save_checkpoint();

        // Safe moves once per L1 block, at its last batch: a resumed batch rewrites
        // the height its same-block predecessor settled, orphaning that safe hash.
        if !last_in_l1_block {
            event!(
                name: "eez.deriver.safe.deferred",
                Level::DEBUG,
                from_block,
                to_block,
                l1_block_number,
                tx_hash = %tx_hash,
                "more batches in this L1 block; safe advance waits for its last one",
            );
            return Ok(());
        }
        let Some(new_safe_hash) = self.sync_safe_to_cursor().await? else {
            return Ok(());
        };
        event!(
            name: "eez.deriver.safe.advanced",
            Level::INFO,
            event_name = "eez.deriver.safe.advanced",
            from_block,
            to_block,
            applied_entries = settlement.applied().len(),
            l1_settled_commitment = %l1_settled_commitment,
            l1_block_number,
            tx_hash = %tx_hash,
            submitter = %submitter,
            new_safe_hash = %new_safe_hash,
            "advanced L2 safe head from L1-confirmed batch",
        );
        Ok(())
    }

    /// Runs the safe advance a skipped last batch would have done, else safe
    /// stalls until the next posted batch.
    async fn flush_deferred_safe(&self, last_in_l1_block: bool) -> DeriverResult<()> {
        if last_in_l1_block {
            self.sync_safe_to_cursor().await?;
        }
        Ok(())
    }

    /// Moves `safe` to the L1-confirmed cursor, re-reading the header so a height
    /// a resume rewrote gives its current hash. Only L1 finality moves finalized.
    async fn sync_safe_to_cursor(&self) -> DeriverResult<Option<B256>> {
        let cursor = self.inner.l1_head.last_indexed_l2();
        if cursor <= self.inner.safe_l2_block.load(Ordering::Acquire) {
            return Ok(None);
        }
        let safe_header = self.l2_sealed_header_at(cursor)?;
        let safe_hash = safe_header.hash();
        let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
        self.inner
            .committer
            .advance_safe_finalized(safe_header, finalized_hash)
            .await?;
        self.inner.safe_l2_block.store(cursor, Ordering::Release);
        Ok(Some(safe_hash))
    }

    async fn on_l1_reorg(
        &self,
        common_ancestor_number: u64,
        old_head_hash: B256,
        new_head_number: u64,
        new_head_hash: B256,
    ) -> DeriverResult<()> {
        // Delivered Reorg events already carry the surviving canonical L1
        // ancestor; hash-tail auditing is reserved for missed reorgs.
        let _guard = self.inner.committer.begin_reconcile().await;

        let old_cursor = self.inner.l1_head.cursor();
        let (new_cursor, _new_finalized, dropped) = self
            .inner
            .l1_head
            .retreat_on_l1_reorg(common_ancestor_number);
        if new_cursor >= old_cursor {
            event!(
                name: "eez.deriver.l1.reorg.noop",
                Level::WARN,
                event_name = "eez.deriver.l1.reorg.noop",
                common_ancestor_number,
                old_head_hash = %old_head_hash,
                new_head_number,
                new_head_hash = %new_head_hash,
                dropped_batches = dropped,
                "L1 reorg reported above indexed batches; no L2 retreat needed",
            );
            return Ok(());
        }

        let new_safe_hash = self.retreat_l2_to_cursor(new_cursor).await?;

        event!(
            name: "eez.deriver.l1.reorg.retreated",
            Level::WARN,
            event_name = "eez.deriver.l1.reorg.retreated",
            common_ancestor_number,
            old_head_hash = %old_head_hash,
            new_head_number,
            new_head_hash = %new_head_hash,
            old_cursor,
            new_cursor,
            dropped_batches = dropped,
            new_safe_hash = %new_safe_hash,
            "L1 reorg rolled out confirmed batches; L2 head retreated to the surviving safe cursor",
        );
        Ok(())
    }

    /// Retreat reth's safe/finalized anchors and canonical head to the
    /// L1-derived cursor. Caller holds the reconcile lock so the Sequencer
    /// can't extend the branch between the two forkchoice updates.
    async fn retreat_l2_to_cursor(&self, cursor: u64) -> DeriverResult<B256> {
        let safe_header = self.l2_sealed_header_at(cursor)?;
        let safe_hash = safe_header.hash();
        let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;

        // Order matters: retreat safe/finalized first while the old head is
        // still canonical, then roll head back and repair the parent mirror.
        self.inner
            .committer
            .advance_safe_finalized(safe_header.clone(), finalized_hash)
            .await?;
        self.inner.committer.reorg_to(safe_header).await?;
        self.inner.safe_l2_block.store(cursor, Ordering::Release);
        Ok(safe_hash)
    }

    async fn on_l1_finalized(&self, l1_finalized_block: u64) -> DeriverResult<()> {
        // Find highest batch with l1_block <= l1_finalized_block.
        // That batch's last_l2_block is the new L2 finalized head.
        let new_finalized = self
            .inner
            .l1_head
            .highest_l2_at_or_below_l1(l1_finalized_block)
            .unwrap_or(0);
        let old_finalized = self.inner.l1_head.finalized_l2();
        if new_finalized <= old_finalized {
            return Ok(());
        }
        // Bound by safe — finalized never exceeds safe.
        let current_safe = self.inner.safe_l2_block.load(Ordering::Acquire);
        let bounded = new_finalized.min(current_safe);
        if bounded <= old_finalized {
            return Ok(());
        }

        let safe_header = self.l2_sealed_header_at(current_safe)?;
        let finalized_hash = self.l2_hash_at(bounded)?;
        self.inner
            .committer
            .advance_safe_finalized(safe_header, finalized_hash)
            .await?;
        self.inner.l1_head.set_finalized_l2(bounded);
        event!(
            name: "eez.deriver.finalized.advanced",
            Level::INFO,
            event_name = "eez.deriver.finalized.advanced",
            l1_finalized_block,
            l2_finalized = bounded,
            "advanced L2 finalized head from L1 finality",
        );
        Ok(())
    }

    fn l2_sealed_header_at(
        &self,
        l2_block: u64,
    ) -> DeriverResult<SealedHeader<alloy_consensus::Header>> {
        self.inner
            .l2_provider
            .sealed_header(l2_block)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("local L2 header at {l2_block} missing"))
            })
    }

    fn l2_hash_at(&self, l2_block: u64) -> DeriverResult<B256> {
        Ok(self.l2_sealed_header_at(l2_block)?.hash())
    }

    /// Number of the block a commitment names, or `None` when we do not hold
    /// it. A hash identifies one block, so this is an index read.
    fn block_at(&self, commitment: B256) -> DeriverResult<Option<u64>> {
        self.inner
            .l2_provider
            .block_number(commitment)
            .map_err(DeriverError::l2_provider)
    }

    /// Block whose hash is `entry_state`: where this batch's run starts. A
    /// competing composer's batch can begin below our cursor, so the answer is
    /// bounded by the range the batch could legally cover, not pinned to it.
    fn batch_anchor(
        &self,
        cursor: u64,
        entry_state: B256,
        window: u64,
    ) -> DeriverResult<Option<u64>> {
        let anchor = in_settleable_range(
            self.block_at(entry_state)?,
            cursor.saturating_sub(window),
            cursor,
        );
        // Overlapping range: a competing composer's batch starts below our cursor.
        if let Some(anchor) = anchor
            && anchor != cursor
        {
            event!(
                name: "eez.deriver.batch.anchored_below_cursor",
                Level::INFO,
                cursor,
                anchor,
                "batch anchored below the cursor; its range overlaps ours",
            );
        }
        Ok(anchor)
    }

    /// Per-block reconciliation against a decoded batch beginning at
    /// `from_block`: for each block, skip if local reth already holds the
    /// same tx list, otherwise STF-replay it (reth fork-switches via
    /// `newPayload` + head-FCU). Once any block in the batch is replayed,
    /// replay every later block too so matching tx lists are rebuilt on the
    /// new ancestry. Returns the count of blocks replayed.
    ///
    /// NOTE: not transactional. If a replay fails partway, earlier blocks
    /// are already committed to reth's canonical chain, leaving local L2
    /// in a half-state. The per-block `eez.deriver.reconcile.block` log
    /// records progress so a failure shows how far the loop got.
    /// [open: roll the canonical head back to the pre-loop snapshot on
    /// failure.]
    async fn reconcile_batch_blocks(
        &self,
        from_block: u64,
        decoded: &eez_payload_codec::DecodedSpan,
        actions: &[eez_payload_codec::Action],
        l1_block_number: u64,
        tx_hash: B256,
        settlement: &eez_l1::Settlement,
    ) -> DeriverResult<(u64, Option<B256>)> {
        // Cross-chain path (skipped when `system_tx_cfg` is `None`):
        // reconstruct the system txs the composer produced from the DA action
        // manifest, which is their only published form.
        event!(
            name: "eez.deriver.reconcile.start",
            Level::INFO,
            l1_block_number,
            tx_hash = %tx_hash,
            from_block,
            block_count = decoded.block_count(),
            actions = actions.len(),
            cross_chain = self.inner.system_tx_cfg.is_some(),
            "reconcile_batch_blocks entered",
        );
        // Reconstruct the Sync block's FULL tx list (system txs interleaved with
        // their user txs) via the SAME builder the composer uses → byte-identical
        // by construction. `None` when no cross-chain cfg (loop uses user txs
        // verbatim).
        //
        // Producing entries are ordered `[anchor, outbound…, inbound…]`:
        // `postAndVerifyBatch` drains the leading `proxyEntryHash==0` run (anchor +
        // outbound) inline, then consumes the deferred inbound ones (`EEZ.sol:387`).
        // `Settlement::producing_slice` says WHICH of those L1 actually ran, read
        // off the settled roots — the anchor cannot be assumed to have run, since
        // a competing same-block batch can supply its hop.
        //
        // `gate_outbound`: outbound entries L1 paid, stashed for the post-replay
        // gate (empty in the pure-user-tx path → no-op).
        let mut gate_outbound: Vec<eez_protocol::abi::ExecutionEntrySol> = Vec::new();
        let sync_block_txs: Option<Vec<Vec<u8>>> = match self.inner.system_tx_cfg.as_ref() {
            Some(cfg) => {
                // The DA publishes actions, not entries: an inbound entry's
                // on-chain form binds its call only via `proxyEntryHash`, so
                // this is the only published copy of what to deliver.
                let mut entries = actions
                    .iter()
                    .enumerate()
                    .map(|(i, action)| {
                        eez_protocol::entries::manifest::entry_from_action(
                            action,
                            eez_protocol::RollupId(cfg.this_rollup_id),
                        )
                        .map_err(|e| {
                            DeriverError::l2_provider(format!(
                                "rebuild entry from DA action[{i}] for tx {tx_hash}: {e}"
                            ))
                        })
                    })
                    .collect::<DeriverResult<Vec<_>>>()?;
                event!(
                    name: "eez.deriver.reconcile.actions",
                    Level::INFO,
                    tx_hash = %tx_hash,
                    actions = entries.len(),
                    "rebuilt execution entries from the DA action manifest",
                );
                // Drop non-producing entries (the anchor immediate signs no system
                // tx), then split by direction: `proxyEntryHash == 0` = outbound
                // settlement, `!= 0` = inbound delivery. `partition` keeps each
                // side's order, preserving `[outbound…, inbound…]`.
                entries.retain(|e| !e.l2ToL1Calls.is_empty());
                let (outbound, inbound): (Vec<_>, Vec<_>) = entries
                    .into_iter()
                    .partition(|e| e.proxyEntryHash == alloy_primitives::B256::ZERO);
                // Captured before drain/truncate: all originally-claimed entries,
                // settled or not, were paired 1:1 with Sync-block user txs.
                let original_outbound_len = outbound.len();

                let original_inbound_len = inbound.len();

                // The Sync block is the LAST block of the range; its user txs are
                // the tail of `decoded.transactions`. Pair the i-th outbound entry
                // with the i-th of those (composer drain == splice == DA order).
                let last_count = decoded
                    .block_tx_counts
                    .last()
                    .copied()
                    .map(|c| c as usize)
                    .unwrap_or(0);
                let sync_user_start = decoded.transactions.len().saturating_sub(last_count);
                let sync_user_txs: Vec<Bytes> = decoded.transactions[sync_user_start..]
                    .iter()
                    .map(|t| Bytes::from(t.clone()))
                    .collect();
                // Outbound entries pair POSITIONALLY with the Sync block's user
                // txs, so a skipped OR unconsumed outbound entry must drop its user
                // tx too, else every pair shifts by one. Neither ever lands on this
                // chain as a bare tx: a skipped entry's tx already landed under the
                // competing batch that consumed it; an unconsumed one is rolled back
                // by the composer's own recovery (rich Sync blocks reorg out on
                // partial settlement) and retried later. Checked against the
                // pre-truncation count so the unconsumed tail is covered too.
                if sync_user_txs.len() < original_outbound_len {
                    return Err(DeriverError::local_diverged_with_msg(
                        from_block,
                        &format!(
                            "outbound entries ({original_outbound_len}) exceed Sync-block user txs ({})",
                            sync_user_txs.len(),
                        ),
                    ));
                }
                // Each outbound entry takes its OWN ordinal's user tx, so a
                // skipped entry never shifts the pairing of the rest.
                let slots = select_applied_slots(settlement, &outbound, &inbound, &sync_user_txs)
                    .map_err(|e| DeriverError::local_diverged_with_msg(from_block, &e))?;
                if slots.outbound_paired.len() < original_outbound_len
                    || slots.inbound.len() < original_inbound_len
                {
                    event!(
                        name: "eez.deriver.reconcile.partial_consumption",
                        Level::WARN,
                        event_name = "eez.deriver.reconcile.partial_consumption",
                        tx_hash = %tx_hash,
                        outbound = original_outbound_len,
                        inbound = original_inbound_len,
                        outbound_applied = slots.outbound_paired.len(),
                        inbound_applied = slots.inbound.len(),
                        applied = ?settlement.applied_indices(),
                        "L1 ran only part of this batch; rebuilding exactly what ran",
                    );
                }
                let outbound_paired = slots.outbound_paired;
                // `inbound` stays the FULL claimed list: the skipped-prefix nonce
                // count below indexes it. Applied deliveries are separate.
                let applied_inbound = slots.inbound;

                // Stash for the post-replay gate — it needs the
                // `CrossChainCallExecuted` events, observable only after replay.
                gate_outbound = outbound_paired.iter().map(|(e, _)| e.clone()).collect();

                let mut starting_nonce = self.system_address_nonce_at(from_block - 1)?;
                // A resume starts mid-block: the rival's system txs already sit
                // here, so count the skipped prefix from the first applied role.
                let (outbound_skip, inbound_skip) =
                    match settlement.applied().first().map(|a| a.role) {
                        Some(eez_l1::EntryRole::Outbound { ordinal }) => (ordinal, 0),
                        // Every outbound entry precedes every inbound one.
                        Some(eez_l1::EntryRole::Inbound { ordinal }) => {
                            (original_outbound_len, ordinal)
                        }
                        _ => (0, 0),
                    };
                if outbound_skip > 0 || inbound_skip > 0 {
                    let skipped_paired: Vec<(eez_protocol::abi::ExecutionEntrySol, Bytes)> =
                        outbound[..outbound_skip]
                            .iter()
                            .cloned()
                            .zip(sync_user_txs[..outbound_skip].iter().cloned())
                            .collect();
                    let prefix_pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
                        &skipped_paired,
                        &inbound[..inbound_skip.min(inbound.len())],
                        cfg,
                        starting_nonce,
                    )
                    .map_err(|e| {
                        DeriverError::l2_provider(format!(
                            "build_cross_chain_sync_pairs(skipped prefix, tx={tx_hash}): {e}"
                        ))
                    })?;
                    starting_nonce = starting_nonce
                        .checked_add(prefix_pairs.len() as u64)
                        .ok_or_else(|| {
                            DeriverError::l2_provider(format!(
                                "SYSTEM_ADDRESS nonce overflow over the skipped prefix (tx={tx_hash})"
                            ))
                        })?;
                }
                let pairs = eez_protocol::system_tx::build_cross_chain_sync_pairs(
                    &outbound_paired,
                    &applied_inbound,
                    cfg,
                    starting_nonce,
                )
                .map_err(|e| {
                    DeriverError::l2_provider(format!(
                        "build_cross_chain_sync_pairs(tx={tx_hash}): {e}"
                    ))
                })?;
                // The interleaved list IS the Sync block's system + outbound-user
                // txs; append any remaining (non-cross-chain) user txs after it.
                let mut full: Vec<Vec<u8>> =
                    eez_protocol::system_tx::interleave_sync_block_txs(&pairs)
                        .into_iter()
                        .map(|b| b.to_vec())
                        .collect();
                for t in &decoded.transactions[sync_user_start + original_outbound_len..] {
                    full.push(t.clone());
                }
                event!(
                    name: "eez.deriver.reconcile.sync_block_built",
                    Level::INFO,
                    event_name = "eez.deriver.reconcile.sync_block_built",
                    tx_hash = %tx_hash,
                    sync_height = from_block + decoded.block_tx_counts.len().saturating_sub(1) as u64,
                    outbound = outbound_paired.len(),
                    inbound = inbound.len(),
                    sync_block_txs = full.len(),
                    starting_nonce,
                    "rebuilt Sync block",
                );
                Some(full)
            }
            None => None,
        };

        // Hash of the Sync block replayed this pass; the gate must read by hash.
        let mut sync_block_hash: Option<B256> = None;
        let stale_boundary = !local_batch_boundary_matches(&self.inner.l2_provider, from_block)?;
        let mut replayed: u64 = 0;
        let last_index = decoded.block_tx_counts.len().saturating_sub(1);
        let resumed = settlement.resumed();
        if resumed {
            // The competing batch already committed this Sync block; only the
            // entries this batch settled are new. Append them to its EXISTING
            // content rather than a fresh block — see `batch_l2_range`.
            if stale_boundary {
                return Err(DeriverError::local_diverged_with_msg(
                    from_block,
                    "resumed batch's Sync block is missing or reorged; cannot append its \
                     settled entries without the existing content",
                ));
            }
            // Which Sync block these entries land in (issue #121).
            event!(
                name: "eez.deriver.resumed.placement",
                Level::INFO,
                event_name = "eez.deriver.resumed.placement",
                l1_block_number,
                tx_hash = %tx_hash,
                from_block,
                applied = ?settlement.applied_indices(),
                entry_state = ?settlement.entry_state,
                claimed_block_count = decoded.block_count(),
                "resumed batch placement: appending settled entries to Sync block {from_block}",
            );
            let existing_txs: Vec<Vec<u8>> = self
                .inner
                .l2_provider
                .block_by_number(from_block)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!("local L2 block at {from_block} missing"))
                })?
                .body()
                .transactions()
                .iter()
                .map(Encodable2718::encoded_2718)
                .collect();
            let new_content = sync_block_txs.clone().unwrap_or_default();
            // The block exists (`stale_boundary` above refused otherwise), so its
            // hash answers whether a previous pass already appended these entries.
            let replay_txs = resume_replay_txs(
                self.l2_hash_at(from_block)?,
                settlement.final_state,
                &existing_txs,
                &new_content,
            );
            event!(
                name: "eez.deriver.reconcile.block",
                Level::DEBUG,
                l1_block_number,
                tx_hash = %tx_hash,
                l2_block = from_block,
                action = if replay_txs.is_some() { "replay" } else { "skip" },
                tx_count = replay_txs.as_ref().map_or(existing_txs.len(), Vec::len),
                resumed_mid_chain = true,
                "appending settled entries to the existing Sync block",
            );
            if let Some(block_txs) = replay_txs {
                // INFO, not debug: a resumed batch rewriting a Sync block is
                // rare and consequential — the rewritten height briefly has a
                // sibling reth can drop — so it should be visible without
                // raising the log level, and observable by tests.
                event!(
                    name: "eez.deriver.resumed.appended",
                    Level::INFO,
                    event_name = "eez.deriver.resumed.appended",
                    l2_block = from_block,
                    tx_count = block_txs.len(),
                    appended = new_content.len(),
                    "rebuilding the existing Sync block with this batch's settled entries",
                );
                // Rewriting a height at or below `safe` orphans the stored safe hash
                // and the engine rejects the FCU, so retreat safe to the parent.
                if self.inner.safe_l2_block.load(Ordering::Acquire) >= from_block {
                    // Safe cannot retreat below finalized (finalized ahead of safe is
                    // an invalid forkchoice), so refuse loudly here (invariant 7).
                    let finalized_l2 = self.inner.l1_head.finalized_l2();
                    if from_block <= finalized_l2 {
                        return Err(DeriverError::local_diverged_with_msg(
                            from_block,
                            &format!(
                                "resumed batch would rewrite Sync block {from_block} at or below \
                                 the L1-finalized L2 head {finalized_l2}"
                            ),
                        ));
                    }
                    let parent = self.l2_sealed_header_at(from_block - 1)?;
                    // Only safe retreats; finalized tracks L1 finality, which a
                    // resume never reaches.
                    let finalized_hash = self.l2_hash_at(finalized_l2)?;
                    self.inner
                        .committer
                        .advance_safe_finalized(parent, finalized_hash)
                        .await?;
                    self.inner
                        .safe_l2_block
                        .store(from_block - 1, Ordering::Release);
                    event!(
                        name: "eez.deriver.safe.retreated_for_resume",
                        Level::WARN,
                        event_name = "eez.deriver.safe.retreated_for_resume",
                        l2_block = from_block,
                        "safe retreated to the parent for a same-height resumed replacement (finalized unchanged)",
                    );
                }
                let outcome = self
                    .replay_block(
                        from_block - 1,
                        &block_txs,
                        &BlockHeaderInputs::from_span(decoded, last_index)?,
                    )
                    .await?;
                // No later block follows this one to make it canonical, so
                // reading by number can still return the old block. Only an
                // issue here; a forward replay is extended by the next commit.
                sync_block_hash = Some(outcome.block_hash);
                replayed = 1;
            }
        } else if range_already_derived(
            stale_boundary,
            self.inner
                .l2_provider
                .sealed_header(from_block + last_index as u64)
                .map_err(DeriverError::l2_provider)?
                .map(|header| header.hash()),
            settlement.final_state,
        ) {
            event!(
                name: "eez.deriver.reconcile.range_skipped",
                Level::DEBUG,
                l1_block_number,
                tx_hash = %tx_hash,
                from_block,
                to_block = from_block + last_index as u64,
                "terminal block is L1's settled commitment, so the whole range is already derived",
            );
        } else {
            let mut suffix_replay = SuffixReplay::new(stale_boundary);
            let mut tx_offset = 0usize;
            for (i, count) in decoded.block_tx_counts.iter().enumerate() {
                let l2_block = from_block + i as u64;
                let count_usize = *count as usize;
                let user_txs = &decoded.transactions[tx_offset..tx_offset + count_usize];
                tx_offset += count_usize;
                // Per Rollup-1 §1.3 + §13.4.23 the composer always sets
                // `to_block = sync_slot_block`, so the Sync block is the
                // LAST block of every batch's range. Prepend system txs
                // there; earlier blocks stay user-tx-only.
                let is_sync_block = i == last_index;
                // The Sync block's full tx list (system + outbound-user, interleaved,
                // plus trailing non-cc user txs) was pre-built above; every other
                // block is its user txs verbatim.
                let block_txs: Vec<Vec<u8>> = match (is_sync_block, sync_block_txs.as_ref()) {
                    (true, Some(full)) => full.clone(),
                    _ => user_txs.to_vec(),
                };
                // Skip only identical blocks: a needless rewrite wedges reth.
                // Txs alone do not identify one; the header inputs seal in too.
                let header_inputs = BlockHeaderInputs::from_span(decoded, i)?;
                let local_matches =
                    local_block_matches(&self.inner.l2_provider, l2_block, &block_txs)?
                        && local_header_inputs_match(
                            &self.inner.l2_provider,
                            l2_block,
                            &header_inputs,
                        )?;
                let should_replay = suffix_replay.required(local_matches);
                event!(
                    name: "eez.deriver.reconcile.block",
                    Level::DEBUG,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    l2_block,
                    action = if should_replay { "replay" } else { "skip" },
                    tx_count = block_txs.len(),
                    replayed_so_far = replayed,
                    "reconciling batch block",
                );
                if !should_replay {
                    continue;
                }
                let outcome = self
                    .replay_block(l2_block - 1, &block_txs, &header_inputs)
                    .await?;
                if is_sync_block {
                    sync_block_hash = Some(outcome.block_hash);
                }
                replayed += 1;
            }
        }

        // Outbound authorization gate (trace binding): every OUTBOUND settlement
        // entry L1 paid must match a real `CrossChainCallExecuted` event this Sync
        // block emitted on re-execution — proof a signed DA tx actually made that
        // L2->L1 call at ANY depth (EOA or wrapper). A phantom has no match. Runs
        // post-replay (events exist only after commit); no-op with no outbound.
        // See `eez_protocol::outbound_gate` + docs/OUTBOUND-VIA-WRAPPER-GATE.md.
        if !gate_outbound.is_empty() {
            let cfg = self
                .inner
                .system_tx_cfg
                .as_ref()
                .expect("gate_outbound only populated under system_tx_cfg = Some");
            let to_block = if resumed {
                from_block
            } else {
                from_block + last_index as u64
            };
            // By hash after a replay: the number still resolves to the superseded
            // block until persistence catches up.
            let source: alloy_eips::BlockHashOrNumber =
                sync_block_hash.map_or_else(|| to_block.into(), Into::into);
            let observed = self.observed_outbound_calls(source, cfg.eezl2_address)?;
            eez_protocol::outbound_gate::verify_outbound_authorized(
                &gate_outbound,
                &observed,
                cfg.this_rollup_id,
            )
            .map_err(|e| {
                DeriverError::local_diverged_with_msg(
                    from_block,
                    &format!("outbound authorization gate failed (tx={tx_hash}): {e}"),
                )
            })?;
        }
        Ok((replayed, sync_block_hash))
    }

    /// Outbound calls emitted by the L2 manager in `block`.
    ///
    /// # Errors
    /// [`DeriverError::l2_provider`] if the block's receipts are missing locally.
    fn observed_outbound_calls(
        &self,
        block: alloy_eips::BlockHashOrNumber,
        eez_l2: Address,
    ) -> DeriverResult<Vec<OutboundCallObservation>> {
        let receipts = self
            .inner
            .l2_provider
            .receipts_by_block(block)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("local receipts for Sync block {block} missing"))
            })?;
        Ok(extract_outbound_call_observations(&receipts, eez_l2))
    }

    /// SYSTEM_ADDRESS account nonce at the L2 parent block. Both
    /// composer and deriver query this at the same block hash; reth
    /// is deterministic so they read identical values, which makes
    /// the native system-tx hashes byte-equal.
    fn system_address_nonce_at(&self, parent_block_number: u64) -> DeriverResult<u64> {
        let parent_header = self
            .inner
            .l2_provider
            .sealed_header(parent_block_number)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!(
                    "local L2 header at parent {parent_block_number} missing"
                ))
            })?;
        let state = self
            .inner
            .l2_provider
            .state_by_block_hash(parent_header.hash())
            .map_err(DeriverError::l2_provider)?;
        let system_address = eez_primitives::SYSTEM_ADDRESS;
        Ok(state
            .account_nonce(&system_address)
            .map_err(DeriverError::l2_provider)?
            .unwrap_or(0))
    }

    /// Loud-fail if the batch's claimed state-root chain disagrees with
    /// our STF's actual L2 roots at the batch boundaries:
    ///
    /// - `claimed_current_state` (first state update's `currentState`) vs the
    ///   local root at `from_block - 1`.
    /// - `claimed_new_state` (last state update's `newState`) vs the local
    ///   root at `to_block`.
    ///
    /// Both ends are checked — the composer chains deltas across entries, so
    /// checking one would let a crafted chain pass. Halting here surfaces a
    /// mismatch at its origin rather than at our next post's
    /// `StateRootMismatch`.
    ///
    /// `entry_root` is [`eez_l1::Settlement::entry_state`], not the claimed chain
    /// head — the claimed head would contradict the cursor guard on a mid-chain resume.
    fn check_claimed_state(
        &self,
        entry_root: Option<B256>,
        claimed_new_state: Option<B256>,
        // Block the applied run started from. The endpoint is in
        // `[anchor, to_block]`, anchor included.
        anchor: u64,
        from_block: u64,
        to_block: u64,
        // Hash of the block replayed at `to_block`. Needed only when a
        // resumed batch rewrote the tip; see `sync_block_hash`.
        to_block_hash: Option<B256>,
        l1_block_number: u64,
        tx_hash: B256,
    ) -> DeriverResult<Option<u64>> {
        if let Some(claimed_curr) = entry_root {
            let pre = from_block.saturating_sub(1);
            let local_pre = self
                .inner
                .l2_provider
                .sealed_header(pre)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!("local L2 header at {pre} missing"))
                })?
                .hash();
            if local_pre != claimed_curr {
                event!(
                    name: "eez.deriver.state.diverged_pre",
                    Level::ERROR,
                    event_name = "eez.deriver.state.diverged_pre",
                    l1_block_number,
                    tx_hash = %tx_hash,
                    pre_block = pre,
                    local_root = %local_pre,
                    claimed = %claimed_curr,
                    "local L2 state root at from_block-1 differs from the root the batch's applied run started from",
                );
                return Err(DeriverError::local_diverged(pre));
            }
        }
        // A partial settlement stops early, so find the block carrying the
        // settled root rather than assuming `to_block`. It becomes the cursor.
        if let Some(claimed_new) = claimed_new_state {
            // L1 can only apply a PREFIX of the claimed chain, so the settled
            // endpoint is at or below the range end — never above our tip.
            let tip = self.inner.committer.last_header().number();
            if to_block > tip {
                event!(
                    name: "eez.deriver.state.endpoint_above_tip",
                    Level::ERROR,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    to_block,
                    tip,
                    "batch claims a range ending above the local tip; replay did not reach it",
                );
                return Err(DeriverError::local_diverged(to_block));
            }
            // `from_block` is `anchor + 1` when fresh and `anchor` when
            // resumed, so the settleable range starts at the anchor either way.
            if let Some(settled_end) =
                in_settleable_range(self.block_at(claimed_new)?, anchor, to_block)
            {
                if settled_end != to_block {
                    event!(
                        name: "eez.deriver.state.settled_prefix",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        to_block,
                        settled_end,
                        "L1 settled a prefix; the cursor follows its endpoint, not the claimed range end",
                    );
                }
                return Ok(Some(settled_end));
            }
            // A resumed batch rewrote the tip, so reading by number can still
            // return the superseded block; the replayed hash is authoritative.
            // When we have it, it IS the answer — no lookup needed.
            let local_post = match to_block_hash {
                Some(hash) => hash,
                None => self
                    .inner
                    .l2_provider
                    .sealed_header(to_block)
                    .map_err(DeriverError::l2_provider)?
                    .ok_or_else(|| {
                        DeriverError::l2_provider(format!("local L2 header at {to_block} missing"))
                    })?
                    .hash(),
            };
            if local_post != claimed_new {
                event!(
                    name: "eez.deriver.state.diverged_post",
                    Level::ERROR,
                    event_name = "eez.deriver.state.diverged_post",
                    l1_block_number,
                    tx_hash = %tx_hash,
                    to_block,
                    local_root = %local_post,
                    claimed = %claimed_new,
                    "local L2 state root at to_block differs from batch's claimed newState",
                );
                return Err(DeriverError::local_diverged(to_block));
            }
            return Ok(Some(to_block));
        }
        Ok(None)
    }
}

/// A commitment names this batch's endpoint only inside the range it can settle;
/// outside it the hash is some other block we hold, which is divergence.
const fn in_settleable_range(found: Option<u64>, low: u64, high: u64) -> Option<u64> {
    match found {
        Some(block) if block >= low && block <= high => Some(block),
        _ => None,
    }
}

/// L2 heights a batch's settled part covers. A resumed batch reuses the Sync
/// block at `cumulative_l2`; a fresh height would re-apply EIP-2935/4788.
const fn batch_l2_range(cumulative_l2: u64, resumed: bool, claimed_block_count: u64) -> (u64, u64) {
    if resumed {
        (cumulative_l2, cumulative_l2)
    } else {
        (cumulative_l2 + 1, cumulative_l2 + claimed_block_count)
    }
}

/// Takes `entry_state` (batch not applied yet) or `final_state` (already applied,
/// as a re-scan sees). Re-derivation is idempotent; any other root is divergence.
fn cursor_root_accepted(local_root: B256, entry_state: B256, final_state: Option<B256>) -> bool {
    local_root == entry_state || final_state == Some(local_root)
}

/// `true` iff the first local block in a batch is anchored to the current
/// local parent. `false` if the block is missing or sits on stale ancestry.
/// Whether the local block's header inputs are the ones the DA names.
///
/// Matching transactions is not enough: these seal into the header hash too,
/// so a block differing in them is a different block nothing else catches.
fn local_header_inputs_match<L2>(
    l2_provider: &Arc<L2>,
    block_number: u64,
    expected: &BlockHeaderInputs,
) -> DeriverResult<bool>
where
    L2: BlockReader<Header = alloy_consensus::Header>,
{
    let Some(header) = l2_provider
        .sealed_header(block_number)
        .map_err(DeriverError::l2_provider)?
    else {
        return Ok(false);
    };
    Ok(header_inputs_match(&header, expected))
}

/// The comparison itself, split out so it is testable without a provider.
fn header_inputs_match(header: &alloy_consensus::Header, expected: &BlockHeaderInputs) -> bool {
    header.beneficiary == expected.beneficiary && header.extra_data == expected.extra_data
}

#[cfg(test)]
mod header_inputs_tests {
    use super::{BlockHeaderInputs, header_inputs_match};
    use alloy_primitives::{Address, Bytes};

    /// A block whose txs match but whose header inputs do not is a DIFFERENT
    /// block, and the skip paths are the only guard against treating it as one.
    #[test]
    fn header_inputs_decide_block_identity_beyond_transactions() {
        let expected = BlockHeaderInputs {
            beneficiary: Address::with_last_byte(0xAA),
            extra_data: Bytes::from_static(b"eez"),
        };
        let mut header = alloy_consensus::Header {
            beneficiary: expected.beneficiary,
            extra_data: expected.extra_data.clone(),
            ..Default::default()
        };
        assert!(header_inputs_match(&header, &expected));

        header.beneficiary = Address::with_last_byte(0xBB);
        assert!(!header_inputs_match(&header, &expected), "beneficiary");

        header.beneficiary = expected.beneficiary;
        header.extra_data = Bytes::from_static(b"other");
        assert!(!header_inputs_match(&header, &expected), "extraData");
    }
}

fn local_batch_boundary_matches<L2>(l2_provider: &Arc<L2>, from_block: u64) -> DeriverResult<bool>
where
    L2: BlockReader<Header = alloy_consensus::Header>,
{
    let Some(local_block) = l2_provider
        .block_by_number(from_block)
        .map_err(DeriverError::l2_provider)?
    else {
        return Ok(false);
    };
    let parent_block = from_block.checked_sub(1).ok_or_else(|| {
        DeriverError::l2_provider("cannot reconcile a batch starting at genesis block")
    })?;
    let expected_parent_hash = l2_provider
        .sealed_header(parent_block)
        .map_err(DeriverError::l2_provider)?
        .ok_or_else(|| {
            DeriverError::l2_provider(format!("local L2 header at {parent_block} missing"))
        })?
        .hash();

    Ok(local_block.header().parent_hash == expected_parent_hash)
}

/// `true` iff local reth has a block at `block_number` whose tx list
/// matches `expected_txs`. `false` if the block is missing or has
/// different txs — caller's signal to STF-replay this slot.
fn local_block_matches<L2>(
    l2_provider: &Arc<L2>,
    block_number: u64,
    expected_txs: &[Vec<u8>],
) -> DeriverResult<bool>
where
    L2: BlockReader,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    let Some(local_block) = l2_provider
        .block_by_number(block_number)
        .map_err(DeriverError::l2_provider)?
    else {
        return Ok(false);
    };
    let local_txs: Vec<Vec<u8>> = local_block
        .body()
        .transactions()
        .iter()
        .map(Encodable2718::encoded_2718)
        .collect();
    if local_txs.len() != expected_txs.len() {
        return Ok(false);
    }
    Ok(local_txs
        .iter()
        .zip(expected_txs.iter())
        .all(|(l, e)| l == e))
}

/// Once one block in a batch must be replayed, every descendant in that batch
/// must be rebuilt on the new parent even when its transaction list matches.
#[derive(Debug, Clone, Copy)]
struct SuffixReplay {
    active: bool,
}

impl SuffixReplay {
    const fn new(stale_boundary: bool) -> Self {
        Self {
            active: stale_boundary,
        }
    }

    const fn required(&mut self, local_matches: bool) -> bool {
        self.active |= !local_matches;
        self.active
    }
}

/// `true` iff the batch's range is already derived, decided from its terminal
/// block alone.
///
/// L1 commits the settled block's hash, and a block hash contains its parent's,
/// so a match at the terminal proves every block beneath it — transactions,
/// beneficiary, `extraData`, timestamp, and any header field that becomes
/// DA-carried later. On a mismatch the whole range is rebuilt: the entries
/// commit only to the endpoints, so there is no per-height value to localise a
/// divergence with.
///
/// `stale_boundary` stays a precondition because a superseded sibling can still
/// answer a read by number; requiring the range's first block to link to its
/// local parent rules that out.
fn range_already_derived(
    stale_boundary: bool,
    local_terminal: Option<B256>,
    settled: Option<B256>,
) -> bool {
    match (stale_boundary, local_terminal, settled) {
        (false, Some(local), Some(settled)) => local == settled,
        _ => false,
    }
}

/// Transactions to replay a resumed batch's Sync block with, or `None` when
/// local already holds L1's settled endpoint.
///
/// A resumed batch appends only the entries L1 settled to a Sync block a
/// competing batch already committed, so the decision is whether a previous
/// pass already appended them. Comparing the local block's hash against L1's
/// settled commitment settles that outright.
///
/// The returned list drops any copy of `new_content` the existing block already
/// ends with before appending: a block can carry this content and still hash
/// differently (any header field differing is enough), and appending then would
/// duplicate it.
fn resume_replay_txs(
    local_hash: B256,
    settled: Option<B256>,
    existing_txs: &[Vec<u8>],
    new_content: &[Vec<u8>],
) -> Option<Vec<Vec<u8>>> {
    if settled == Some(local_hash) {
        return None;
    }
    let base = if new_content.is_empty() {
        existing_txs
    } else {
        existing_txs
            .strip_suffix(new_content)
            .unwrap_or(existing_txs)
    };
    let mut txs = base.to_vec();
    txs.extend_from_slice(new_content);
    Some(txs)
}

/// The DA slots L1 actually applied, addressed by role rather than by offset.
#[derive(Debug, Default)]
struct AppliedSlots {
    /// Applied outbound entries, each with the Sync-block user tx at its own
    /// ordinal.
    outbound_paired: Vec<(eez_protocol::abi::ExecutionEntrySol, Bytes)>,
    /// Applied inbound delivery entries, in application order.
    inbound: Vec<eez_protocol::abi::ExecutionEntrySol>,
}

/// Select the DA entries L1 applied, by the role it reported for each. Selects
/// rather than truncates, so a non-contiguous applied set is fine.
///
/// # Errors
/// A role naming a slot the DA does not carry — divergence, so it is loud.
fn select_applied_slots(
    settlement: &eez_l1::Settlement,
    outbound: &[eez_protocol::abi::ExecutionEntrySol],
    inbound: &[eez_protocol::abi::ExecutionEntrySol],
    sync_user_txs: &[Bytes],
) -> Result<AppliedSlots, String> {
    let mut slots = AppliedSlots::default();
    for applied in settlement.applied() {
        match applied.role {
            // State-only: moves the commitment, signs no system tx.
            eez_l1::EntryRole::Anchor => {}
            eez_l1::EntryRole::Outbound { ordinal } => {
                let (Some(entry), Some(user_tx)) =
                    (outbound.get(ordinal), sync_user_txs.get(ordinal))
                else {
                    return Err(format!(
                        "L1 applied outbound entry {} (ordinal {ordinal}) but the DA carries {} \
                         outbound entries and {} Sync-block user txs",
                        applied.entry_index,
                        outbound.len(),
                        sync_user_txs.len(),
                    ));
                };
                slots.outbound_paired.push((entry.clone(), user_tx.clone()));
            }
            eez_l1::EntryRole::Inbound { ordinal } => {
                let Some(entry) = inbound.get(ordinal) else {
                    return Err(format!(
                        "L1 applied inbound entry {} (ordinal {ordinal}) but the DA carries {} \
                         inbound entries",
                        applied.entry_index,
                        inbound.len(),
                    ));
                };
                slots.inbound.push(entry.clone());
            }
        }
    }
    Ok(slots)
}

/// Decode outbound-call events emitted by the configured L2 manager.
fn extract_outbound_call_observations<R>(
    receipts: &[R],
    eez_l2: Address,
) -> Vec<OutboundCallObservation>
where
    R: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
{
    let logs: Vec<alloy_primitives::Log> = receipts
        .iter()
        .flat_map(alloy_consensus::TxReceipt::logs)
        .cloned()
        .collect();
    eez_protocol::outbound_gate::observations_from_logs(&logs, eez_l2)
}

#[cfg(test)]
mod suffix_replay_tests {
    use super::SuffixReplay;

    fn decisions(stale_boundary: bool, local_matches: &[bool]) -> Vec<bool> {
        let mut suffix = SuffixReplay::new(stale_boundary);
        local_matches
            .iter()
            .map(|matches| suffix.required(*matches))
            .collect()
    }

    #[test]
    fn mismatch_replays_every_later_descendant() {
        assert_eq!(
            decisions(false, &[true, false, true, true]),
            [false, true, true, true],
        );
    }

    #[test]
    fn stale_boundary_replays_the_complete_batch() {
        assert_eq!(decisions(true, &[true, true, true]), [true, true, true]);
    }
}

#[cfg(test)]
mod reconcile_decision_tests {
    use super::{range_already_derived, resume_replay_txs};
    use alloy_primitives::B256;

    fn tx(byte: u8) -> Vec<u8> {
        vec![byte]
    }

    /// The terminal block's hash decides the whole range: a block hash contains
    /// its parent's, so a match proves every block beneath it.
    #[test]
    fn a_range_is_derived_only_when_its_terminal_is_l1s_settled_block() {
        let settled = B256::repeat_byte(0xAA);
        let other = B256::repeat_byte(0xBB);

        assert!(range_already_derived(false, Some(settled), Some(settled)));

        assert!(
            !range_already_derived(false, Some(other), Some(settled)),
            "a different terminal block must be rebuilt"
        );
        assert!(
            !range_already_derived(true, Some(settled), Some(settled)),
            "a stale boundary means a read by number can return a superseded sibling"
        );
        assert!(
            !range_already_derived(false, None, Some(settled)),
            "a range whose terminal is not local yet cannot already be derived"
        );
        assert!(
            !range_already_derived(false, None, None),
            "two absent values must not compare equal"
        );
        assert!(
            !range_already_derived(false, Some(settled), None),
            "without a settled endpoint there is nothing to match against"
        );
    }

    #[test]
    fn a_resumed_batch_skips_only_when_local_is_already_l1s_settled_block() {
        let settled = B256::repeat_byte(0xAA);
        let existing = [tx(1), tx(2)];
        let new_content = [tx(3)];

        assert_eq!(
            resume_replay_txs(settled, Some(settled), &existing, &new_content),
            None,
            "local already holds the settled endpoint"
        );
        assert_eq!(
            resume_replay_txs(
                B256::repeat_byte(0xBB),
                Some(settled),
                &existing,
                &new_content
            ),
            Some(vec![tx(1), tx(2), tx(3)]),
            "not yet appended, so append"
        );
    }

    /// A block can already carry this content and still hash differently — any
    /// header field differing is enough. Appending again would duplicate it.
    #[test]
    fn a_resumed_replay_never_duplicates_content_the_block_already_ends_with() {
        let new_content = [tx(3), tx(4)];
        let already_appended = [tx(1), tx(2), tx(3), tx(4)];

        assert_eq!(
            resume_replay_txs(
                B256::repeat_byte(0xBB),
                Some(B256::repeat_byte(0xAA)),
                &already_appended,
                &new_content
            ),
            Some(vec![tx(1), tx(2), tx(3), tx(4)]),
            "the suffix is rebuilt in place, not appended a second time"
        );
    }

    /// Without a settled endpoint there is nothing to prove applied-ness with,
    /// so replay is the only safe answer.
    #[test]
    fn a_resumed_batch_with_no_settled_endpoint_replays() {
        assert_eq!(
            resume_replay_txs(B256::repeat_byte(0xAA), None, &[tx(1)], &[tx(2)]),
            Some(vec![tx(1), tx(2)])
        );
    }

    /// No reconstructed content plus a hash that is not the settled endpoint is a
    /// real divergence. Rebuilding the block unchanged lets the endpoint check
    /// downstream report it instead of silently skipping.
    #[test]
    fn a_resumed_batch_with_no_new_content_rebuilds_rather_than_skipping() {
        let existing = [tx(1), tx(2)];
        assert_eq!(
            resume_replay_txs(
                B256::repeat_byte(0xBB),
                Some(B256::repeat_byte(0xAA)),
                &existing,
                &[]
            ),
            Some(vec![tx(1), tx(2)])
        );
    }
}

#[cfg(test)]
mod applied_selection_tests {
    //! Two composers post into ONE L1 block: `pb1` claims `A→B` and lands, so
    //! `pb2`'s leading entries are refused as redundant and L1 resumes
    //! MID-CHAIN inside it. The deriver must rebuild exactly what ran.

    use super::{AppliedSlots, select_applied_slots};
    use alloy_primitives::{Address, B256, Bytes};
    use eez_l1::{AppliedEntry, EntryRole, Settlement};

    fn producing_entry(tag: u8) -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
            proxyEntryHash: B256::repeat_byte(tag),
            l2ToL1Calls: vec![eez_protocol::abi::L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: Address::ZERO,
                sourceRollupId: 1,
                targetAddress: Address::ZERO,
                value: alloy_primitives::U256::ZERO,
                data: Bytes::new(),
            }],
            ..Default::default()
        }
    }

    fn user_tx(tag: u8) -> Bytes {
        Bytes::from(vec![tag])
    }

    /// `applied` is a list of (entry_index, role).
    fn settlement_of(applied: &[(usize, EntryRole)]) -> Settlement {
        Settlement::new(
            applied
                .iter()
                .map(|&(entry_index, role)| AppliedEntry { entry_index, role })
                .collect(),
            None,
            None,
        )
    }

    fn slots(applied: &[(usize, EntryRole)]) -> AppliedSlots {
        let outbound = vec![producing_entry(0x01), producing_entry(0x02)];
        let inbound = vec![producing_entry(0xB1), producing_entry(0xB2)];
        let txs = vec![user_tx(0xA), user_tx(0xB)];
        select_applied_slots(&settlement_of(applied), &outbound, &inbound, &txs)
            .expect("roles address the DA")
    }

    /// A partial settlement rebuilds only the entries that ran. The unconsumed
    /// tail is absent, not truncated-to-length.
    #[test]
    fn stopping_short_selects_only_what_ran() {
        let s = slots(&[
            (0, EntryRole::Anchor),
            (1, EntryRole::Outbound { ordinal: 0 }),
        ]);
        assert_eq!(s.outbound_paired.len(), 1);
        assert_eq!(s.outbound_paired[0].1, user_tx(0xA));
        assert!(
            s.inbound.is_empty(),
            "an unconsumed delivery is not rebuilt"
        );
    }

    /// The anchor moves the commitment but signs no system tx, so an
    /// anchor-only settlement rebuilds nothing.
    #[test]
    fn anchor_only_settlement_rebuilds_nothing() {
        let s = slots(&[(0, EntryRole::Anchor)]);
        assert!(s.outbound_paired.is_empty() && s.inbound.is_empty());
    }

    /// An outbound entry takes the user tx at its OWN ordinal; offset-based
    /// selection would shift every pair after a skip.
    #[test]
    fn outbound_pairing_follows_the_original_ordinal() {
        let s = slots(&[(2, EntryRole::Outbound { ordinal: 1 })]);
        assert_eq!(s.outbound_paired.len(), 1);
        assert_eq!(
            s.outbound_paired[0].1,
            user_tx(0xB),
            "ordinal 1 keeps the second user tx even when ordinal 0 was skipped",
        );
    }

    /// A hole — an applied set that is not one contiguous run, which a peer's
    /// branching batch produces — is selected around rather than refused.
    #[test]
    fn a_hole_in_the_applied_set_is_selected_around() {
        let s = slots(&[
            (1, EntryRole::Outbound { ordinal: 0 }),
            (4, EntryRole::Inbound { ordinal: 1 }),
        ]);
        assert_eq!(s.outbound_paired.len(), 1);
        assert_eq!(s.outbound_paired[0].1, user_tx(0xA));
        assert_eq!(
            s.inbound.len(),
            1,
            "the entry after the hole still rebuilds"
        );
    }

    /// A role naming a slot the DA lacks means the readings disagree — loud,
    /// because rebuilding a different block than L1 settled is divergence.
    #[test]
    fn a_role_beyond_the_da_is_loud() {
        let outbound = vec![producing_entry(0x01)];
        let txs = vec![user_tx(0xA)];
        let error = select_applied_slots(
            &settlement_of(&[(1, EntryRole::Outbound { ordinal: 5 })]),
            &outbound,
            &[],
            &txs,
        )
        .expect_err("a role beyond the DA must not be clamped");
        assert!(error.contains("ordinal 5"), "got {error}");
    }

    /// A resumed batch's `entry_state` is the rival's endpoint; the cursor guard
    /// and `check_claimed_state` must agree on it.
    #[test]
    fn entry_state_on_a_resume_is_not_the_claimed_chain_head() {
        let (a, b) = (B256::repeat_byte(0x0A), B256::repeat_byte(0x0B));
        let claimed_head = Some(a);
        let resumed = Settlement::new(
            vec![AppliedEntry {
                entry_index: 1,
                role: EntryRole::Outbound { ordinal: 0 },
            }],
            None,
            Some(b),
        );
        assert!(resumed.resumed());
        assert_eq!(resumed.entry_state.or(claimed_head), Some(b));
        assert_ne!(resumed.entry_state.or(claimed_head), claimed_head);
    }
}

#[cfg(test)]
mod outbound_wiring_tests {
    //! Wiring + attack-surface tests for the outbound authorization path: the
    //! event extraction ([`extract_outbound_call_observations`]) and its composition
    //! with [`eez_protocol::outbound_gate::verify_outbound_authorized`]. The pure gate
    //! logic is unit-tested in `eez-protocol`; here we exercise the DERIVER-side wiring
    //! — the address + event-signature filters that decide which events authorize
    //! — and the accept/reject decisions on synthetic receipts.

    use super::extract_outbound_call_observations;
    use alloy_consensus::Receipt;
    use alloy_primitives::{Address, B256, Bytes, Log, U256, address};
    use alloy_sol_types::SolEvent;
    use eez_protocol::RollupId;
    use eez_protocol::abi::eez_l2_events::CrossChainCallExecuted;
    use eez_protocol::abi::{ExecutionEntrySol, L2ToL1CallSol};
    use eez_protocol::action::{CallHashInput, l2_outbound_call_hash};
    use eez_protocol::outbound_gate::{OutboundCallObservation, verify_outbound_authorized};

    const EEZL2: Address = address!("4200000000000000000000000000000000000007");
    const OTHER: Address = address!("00000000000000000000000000000000deadbeef");
    const L2_RID: u64 = 1;

    /// A canonical `CrossChainCallExecuted` log from `addr`.
    fn cc_log(addr: Address, call_hash: B256) -> Log {
        Log {
            address: addr,
            data: CrossChainCallExecuted {
                crossChainCallHash: call_hash,
                proxy: Address::ZERO,
                sourceAddress: Address::ZERO,
                callData: Bytes::new(),
                value: U256::ZERO,
                callGas: 0,
            }
            .encode_log_data(),
        }
    }

    fn receipt(logs: Vec<Log>) -> Receipt {
        Receipt {
            status: true.into(),
            cumulative_gas_used: 0,
            logs,
        }
    }

    fn outbound_call(source: Address, target: Address, value: u64, data: &[u8]) -> L2ToL1CallSol {
        L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: source,
            sourceRollupId: L2_RID,
            targetAddress: target,
            value: U256::from(value),
            data: Bytes::from(data.to_vec()),
        }
    }

    fn outbound_entry(call: L2ToL1CallSol) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateUpdates: Vec::new(),
            proxyEntryHash: B256::ZERO, // outbound immediate
            l2ToL1Calls: vec![call],
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: L2_RID,
            success: true,
            returnData: Bytes::new(),
        }
    }

    /// The topic1 `EEZL2` emits for `call` on this L2 (`targetRollupId` =
    /// MAINNET(0), `sourceRollupId` = `L2_RID`) — what the gate recomputes.
    fn call_hash(call: &L2ToL1CallSol) -> B256 {
        l2_outbound_call_hash(
            CallHashInput {
                call_mode: eez_protocol::CallMode::Mutable,
                source_address: call.sourceAddress,
                source_rollup_id: RollupId(L2_RID),
                target_address: call.targetAddress,
                target_rollup_id: RollupId::MAINNET,
                value: call.value,
                data: &call.data,
            },
            0,
        )
    }

    fn eoa() -> Address {
        address!("00000000000000000000000000000000000000aa")
    }
    fn l1_target() -> Address {
        address!("dc64a140aa3e981100a9beca4e685f962f0cf6c9")
    }

    // ── extraction filters ──────────────────────────────────────────────

    #[test]
    fn extract_picks_eezl2_events_and_preserves_multiset() {
        let h1 = B256::repeat_byte(0x11);
        let h2 = B256::repeat_byte(0x22);
        // Empty receipts (reverted txs) and topicless logs carry no hash — ignored.
        let bare = Log::new_unchecked(EEZL2, Vec::new(), Bytes::new());
        let receipts = vec![
            receipt(vec![cc_log(EEZL2, h1), cc_log(EEZL2, h2)]),
            receipt(vec![cc_log(EEZL2, h1)]), // duplicate h1 → multiset keeps both
            receipt(vec![]),                  // reverted tx → no logs
            receipt(vec![bare]),              // topicless log → no hash
        ];
        assert_eq!(
            extract_outbound_call_observations(&receipts, EEZL2),
            vec![
                OutboundCallObservation::new(h1, 0),
                OutboundCallObservation::new(h2, 0),
                OutboundCallObservation::new(h1, 0),
            ]
        );
    }

    // ── extraction ∘ gate: accept + attack rejections ───────────────────

    #[test]
    fn wiring_accepts_contract_source_wrapper() {
        // Outbound-via-wrapper end to end through extraction: source is a CONTRACT.
        let wrapper = address!("cccccccccccccccccccccccccccccccccccccccc");
        let call = outbound_call(wrapper, l1_target(), 42, &[0xab]);
        let receipts = vec![receipt(vec![cc_log(EEZL2, call_hash(&call))])];
        let observed = extract_outbound_call_observations(&receipts, EEZL2);
        assert!(
            verify_outbound_authorized(&[outbound_entry(call)], &observed, L2_RID).is_ok(),
            "a contract-initiated (wrapper) outbound must be accepted"
        );
    }

    #[test]
    fn wiring_rejects_spoofed_foreign_event() {
        // ATTACK: the only event with the matching hash is emitted by a foreign
        // address; extraction drops it, so the gate sees a phantom.
        let call = outbound_call(eoa(), l1_target(), 7, &[0x12]);
        let receipts = vec![receipt(vec![cc_log(OTHER, call_hash(&call))])];
        let observed = extract_outbound_call_observations(&receipts, EEZL2);
        assert!(verify_outbound_authorized(&[outbound_entry(call)], &observed, L2_RID).is_err());
    }

    #[test]
    fn wiring_rejects_double_count() {
        // ATTACK: two identical settlement entries, one real event → the second is
        // unmatched (multiset consumption).
        let call = outbound_call(eoa(), l1_target(), 7, &[0x12]);
        let entries = vec![outbound_entry(call.clone()), outbound_entry(call.clone())];
        let one = vec![receipt(vec![cc_log(EEZL2, call_hash(&call))])];
        assert!(
            verify_outbound_authorized(
                &entries,
                &extract_outbound_call_observations(&one, EEZL2),
                L2_RID,
            )
            .is_err()
        );
        // …but two events authorize both.
        let two = vec![receipt(vec![
            cc_log(EEZL2, call_hash(&call)),
            cc_log(EEZL2, call_hash(&call)),
        ])];
        assert!(
            verify_outbound_authorized(
                &entries,
                &extract_outbound_call_observations(&two, EEZL2),
                L2_RID,
            )
            .is_ok()
        );
    }
}

#[cfg(test)]
mod anchor_range_tests {
    //! Two composers on one rollup post OVERLAPPING ranges: B's cursor sits a
    //! block behind A's, so B's batch starts below A's cursor (live 2026-08-24).
    //!
    //! Locating a commitment is an index read; what still matters is the RANGE a
    //! batch may settle in.

    use super::{batch_l2_range, in_settleable_range};

    const ANCHOR: u64 = 174_192;

    /// The live stall: B's batch covered 174192..174197 while A's cursor was
    /// 174192, so B's run started one block below it.
    #[test]
    fn a_batch_may_anchor_below_the_cursor() {
        let block_count = 6_u64;
        assert_eq!(
            in_settleable_range(Some(174_191), ANCHOR.saturating_sub(block_count), ANCHOR),
            Some(174_191),
        );
    }

    /// The anchor is a legitimate endpoint; its predecessor is not, because that
    /// block predates the run.
    #[test]
    fn the_range_includes_the_anchor_and_excludes_what_precedes_it() {
        for count in [1_u64, 6] {
            let (_, to) = batch_l2_range(ANCHOR, false, count);
            assert_eq!(
                in_settleable_range(Some(ANCHOR), ANCHOR, to),
                Some(ANCHOR),
                "count={count}: the anchor must stay reachable",
            );
            assert_eq!(
                in_settleable_range(Some(ANCHOR - 1), ANCHOR, to),
                None,
                "count={count}: a block predating the run is not an endpoint",
            );
        }
    }

    /// L1 can stop at any entry, so every block the batch claims is a possible
    /// endpoint — and nothing above the claimed end is.
    #[test]
    fn every_claimed_block_is_a_valid_endpoint() {
        let (from, to) = batch_l2_range(ANCHOR, false, 6);
        for endpoint in from..=to {
            assert_eq!(
                in_settleable_range(Some(endpoint), ANCHOR, to),
                Some(endpoint)
            );
        }
        assert_eq!(in_settleable_range(Some(to + 1), ANCHOR, to), None);
    }

    /// A resumed batch collapses onto the anchor, so that block is its only
    /// valid endpoint.
    #[test]
    fn a_resumed_batch_settles_only_at_its_own_sync_block() {
        let (from, to) = batch_l2_range(ANCHOR, true, 10);
        assert_eq!((from, to), (ANCHOR, ANCHOR));
        assert_eq!(in_settleable_range(Some(ANCHOR), ANCHOR, to), Some(ANCHOR));
        assert_eq!(in_settleable_range(Some(ANCHOR - 1), ANCHOR, to), None);
    }

    /// A commitment naming a block we do not hold is not an endpoint; the caller
    /// falls through to the divergence check.
    #[test]
    fn a_commitment_we_do_not_hold_is_not_an_endpoint() {
        assert_eq!(in_settleable_range(None, ANCHOR, ANCHOR + 6), None);
    }
}

#[cfg(test)]
mod cursor_guard_tests {
    //! A re-scanned resume hits the guard twice: cursor at `entry_state` first,
    //! at `final_state` after. Calling the second divergence rolls back good state.

    use super::cursor_root_accepted;
    use alloy_primitives::B256;

    const ENTRY: B256 = B256::repeat_byte(0x0B);
    const FINAL: B256 = B256::repeat_byte(0x0C);
    const OTHER: B256 = B256::repeat_byte(0xFF);

    #[test]
    fn entry_state_passes_before_the_append() {
        assert!(cursor_root_accepted(ENTRY, ENTRY, Some(FINAL)));
    }

    #[test]
    fn settled_endpoint_passes_after_the_append() {
        assert!(cursor_root_accepted(FINAL, ENTRY, Some(FINAL)));
    }

    #[test]
    fn a_third_root_stays_loud() {
        assert!(!cursor_root_accepted(OTHER, ENTRY, Some(FINAL)));
        // No settled endpoint reported: only `entry_state` can clear the guard.
        assert!(!cursor_root_accepted(OTHER, ENTRY, None));
        assert!(!cursor_root_accepted(FINAL, ENTRY, None));
    }
}

#[cfg(test)]
mod seed_tests {
    use super::choose_seed;

    /// The seed must never leave `[floor, end]` — both bounds have been wrong
    /// in the field, wedging boot each time.
    #[test]
    fn choose_seed_stays_within_the_scanned_range() {
        assert_eq!(choose_seed(500, 1000, Some(968)), 968); // finality lags the tip
        assert_eq!(choose_seed(990, 1000, Some(900)), 990); // finalized below floor
        assert_eq!(choose_seed(500, 1000, Some(1010)), 1000); // finality past scan
        // Chain too young to finalize: the floor is what the scan read.
        assert_eq!(choose_seed(500, 1000, None), 500);
        // Floor above the endpoint (L1 rewound under an indexed batch).
        assert_eq!(choose_seed(999, 40, Some(20)), 40);
        assert_eq!(choose_seed(0, 0, None), 0);
    }
}
