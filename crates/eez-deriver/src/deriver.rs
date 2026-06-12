//! L1-event-driven L2 consensus: replays `BatchPosted` events into
//! local reth, advances safe/finalized via [`BlockCommitterHandle`],
//! and maintains a per-batch index so [`L1Event::Reorg`] retreats
//! the safe head and [`L1Event::Finalized`] advances finalized.
//!
//! STF-replay pattern adapted from `based-rollup`'s `build_derived_block`
//! at `/root/sync-rollups-composer/crates/based-rollup/src/driver/protocol_txs.rs:453`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_eips::{Decodable2718, Encodable2718};
use alloy_primitives::{Address, B256, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use eez_driver::{BUILDER_EXTRA_DATA, BUILDER_GAS_LIMIT, BlockCommitterHandle, DeriveOutcome};
use eez_l1::{BatchRecord, L1CanonicalHead, L1Event, L1Watcher, Submitter};
use reth_chainspec::{ChainSpec, EthereumHardforks};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes, execute::BlockBuilder};
use reth_evm_ethereum::EthEvmConfig;
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody, SealedHeader, SignedTransaction};
use reth_provider::StateProviderFactory;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{BlockReader, TransactionsProvider};
use revm::database::State;
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::error::{DeriverError, DeriverResult};

/// L2 block time, in seconds. Pinned at 2s per Rollup-1 spec §1.3.
/// Used by [`Deriver::execute_block`] to derive each block's
/// timestamp from its parent's deterministically.
const BLOCK_TIME_SECS: u64 = 2;

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
    l1_watcher: L1Watcher,
    committer: BlockCommitterHandle<EthEngineTypes>,
    l2_provider: Arc<L2>,
    submitter: Submitter,
    evm_config: EthEvmConfig,
    /// Chainspec-aware deriver
    chain_spec: Arc<ChainSpec>,
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
            .field("l1_watcher", &self.inner.l1_watcher)
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
{
    /// Builds a deriver. Cursor + per-batch index are populated lazily
    /// by `catch_up_to`, which walks historical `BatchPosted` events
    /// applying the same linearity check live events get — so losers
    /// (competing batches whose `currentState` no longer matches the
    /// cursor) don't pollute the index.
    pub fn new(
        l1_watcher: L1Watcher,
        committer: BlockCommitterHandle<EthEngineTypes>,
        l2_provider: Arc<L2>,
        submitter: Submitter,
        chain_spec: Arc<ChainSpec>,
        deploy_block: u64,
        l1_head: Arc<L1CanonicalHead>,
    ) -> Self {
        let evm_config = EthEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            inner: Arc::new(Inner {
                l1_watcher,
                committer,
                l2_provider,
                submitter,
                evm_config,
                chain_spec,
                deploy_block,
                l1_head,
                safe_l2_block: AtomicU64::new(0),
            }),
        }
    }

    /// Current cursor — highest L2 block confirmed by any L1-landed
    /// batch. Reads through the shared [`L1CanonicalHead`].
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.inner.l1_head.cursor()
    }

    /// Sync local state with L1's full confirmed batch history from the
    /// registry deploy block: walks past `BatchPosted` in tx-order,
    /// skips losers via `state_applied`, force-replays every block of
    /// every winning batch (during boot the deriver is the authority on
    /// L2 chain state — skips would trust whatever's locally canonical,
    /// which can be a Sequencer-race-produced block), and populates
    /// `L1CanonicalHead`.
    ///
    /// # Errors
    ///
    /// `l2_provider` (lookup / scan failure), `local_diverged` (replay
    /// failure), `committer_closed`.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub async fn catch_up(&self) -> DeriverResult<()> {
        self.sync_batches(self.inner.deploy_block, 0, true).await
    }

    /// Reorg-aware re-sync — the single recovery path for every way the
    /// event stream can betray us: a failed `BatchPosted`, a lagged
    /// (dropped) event, a live `L1Event::Reorg`, or a reorg the deriver
    /// never heard about (broadcast lag, or the boot window before
    /// [`Self::run`] subscribes).
    ///
    /// Two phases:
    /// 1. [`Self::revalidate_index_tail`] drops indexed batches whose
    ///    L1 block is no longer canonical, retreating the cursor — the
    ///    same effect a delivered Reorg event would have had.
    /// 2. [`Self::sync_batches`] re-scans L1 from the surviving anchor
    ///    (full history when nothing survives) and reconciles whatever
    ///    isn't indexed yet, then moves reth's safe head to match —
    ///    forward after new batches, backward after a retreat.
    ///
    /// Without phase 1, a forward-only rescan trusts a poisoned index:
    /// it stacks new-chain batches on top of rolled-out ones at the
    /// wrong L2 heights and replays blocks that exist on no other node.
    ///
    /// # Errors
    ///
    /// Same as [`Self::catch_up`].
    async fn resync(&self) -> DeriverResult<()> {
        match self.revalidate_index_tail().await? {
            Some(anchor_l1_block) => {
                self.sync_batches(anchor_l1_block, self.inner.l1_head.cursor(), false)
                    .await
            }
            None => self.sync_batches(self.inner.deploy_block, 0, true).await,
        }
    }

    /// Phase 1 of [`Self::resync`]: walk the index tail backward,
    /// dropping batches whose recorded L1 block hash is no longer the
    /// canonical hash at that height. Returns the L1 block of the
    /// highest still-canonical indexed batch — the lower bound for the
    /// forward rescan — or `None` if the index is (or became) empty.
    async fn revalidate_index_tail(&self) -> DeriverResult<Option<u64>> {
        while let Some(tail) = self.inner.l1_head.last_indexed() {
            let canonical = self
                .inner
                .submitter
                .canonical_l1_hash(tail.l1_block)
                .await
                .map_err(|e| DeriverError::l2_provider(format!("L1 canonicality probe: {e}")))?;
            if canonical == Some(tail.l1_block_hash) {
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
                l1_block = tail.l1_block,
                indexed_hash = %tail.l1_block_hash,
                canonical_hash = ?canonical,
                old_cursor,
                new_cursor,
                dropped_batches = dropped,
                "L1 reorg rolled out confirmed batches; L2 safe cursor retreated",
            );
        }
        Ok(None)
    }

    /// Shared body of [`Self::catch_up`] / [`Self::resync`]: scan
    /// `BatchPosted` from `from_l1_block`, reconcile each not-yet-
    /// indexed winner with L2-range accounting starting at
    /// `cumulative_start` (the L2 block confirmed just before the scan
    /// window), then advance the safe/finalized anchors. Batches whose
    /// tx hash is already indexed are skipped entirely — their L2
    /// ranges are covered by `cumulative_start`.
    async fn sync_batches(
        &self,
        from_l1_block: u64,
        cumulative_start: u64,
        force_replay: bool,
    ) -> DeriverResult<()> {
        // Acquire lock to prevent sequencing during the sync
        let _guard = self.inner.committer.begin_reconcile().await;
        let local_head = self
            .inner
            .l2_provider
            .best_block_number()
            .map_err(DeriverError::l2_provider)?;
        event!(
            name: "eez.deriver.catch_up.start",
            Level::INFO,
            local_head,
            from_l1_block,
            cumulative_start,
            force_replay,
            "starting batch scan to populate L1CanonicalHead and reconcile L2 chain",
        );

        let historical = self
            .inner
            .submitter
            .scan_batches(from_l1_block)
            .await
            .map_err(|e| DeriverError::l2_provider(format!("catch-up scan: {e}")))?;

        let known_tx_hashes = self.inner.l1_head.known_tx_hashes();
        let mut new_batches: Vec<BatchRecord> = Vec::new();
        let mut cumulative_l2: u64 = cumulative_start;
        let mut total_replayed: u64 = 0;
        for batch in &historical {
            // Skip losers — no `L2ExecutionPerformed`, state didn't
            // move on L1, cursor stays put.
            if !batch.state_applied {
                event!(
                    name: "eez.deriver.catch_up.batch.lost_race",
                    Level::INFO,
                    l1_block_number = batch.l1_block_number,
                    tx_hash = %batch.tx_hash,
                    "catch_up: batch lost the race on L1; skipping",
                );
                continue;
            }

            // Already indexed — processed by an earlier sync; its L2
            // range is accounted for in `cumulative_start`.
            if known_tx_hashes.contains(&batch.tx_hash) {
                continue;
            }

            let decoded = eez_payload_codec::decode(batch.call_data.as_ref())?;
            let batch_first_l2 = cumulative_l2 + 1;
            let batch_last_l2 = cumulative_l2 + decoded.block_count() as u64;

            total_replayed += self
                .reconcile_batch_blocks(
                    batch_first_l2,
                    &decoded,
                    batch.l1_block_number,
                    batch.tx_hash,
                    force_replay,
                )
                .await?;

            new_batches.push(BatchRecord {
                l1_block: batch.l1_block_number,
                l1_block_hash: batch.l1_block_hash,
                tx_hash: batch.tx_hash,
                last_l2_block: batch_last_l2,
            });

            // Catch claimed-vs-derived drift now, during the sync,
            // rather than waiting for a live event.
            self.check_claimed_state(
                batch.claimed_new_state,
                batch_last_l2,
                batch.l1_block_number,
                batch.tx_hash,
            )?;
            cumulative_l2 = batch_last_l2;
        }

        // Index every batch we walked (de-duped against startup
        // entries) so subsequent live `BatchPosted` events for any
        // of them are skipped as already-processed.
        if !new_batches.is_empty() {
            self.inner.l1_head.append_many(new_batches);
        }

        // Move reth's safe head to whatever L1 has confirmed — forward
        // after a bulk replay, *backward* after the resync's anchor
        // walk dropped rolled-out batches. Live on_batch_posted
        // advances safe on each new event; here we reconcile the safe
        // head after a bulk pass so RPC clients see the right safe head
        // before the next live event lands.
        if cumulative_l2 != self.inner.safe_l2_block.load(Ordering::Acquire) {
            let safe_header = self.l2_sealed_header_at(cumulative_l2)?;
            let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
            self.inner
                .committer
                .advance_safe_finalized(safe_header, finalized_hash)
                .await?;
            self.inner
                .safe_l2_block
                .store(cumulative_l2, Ordering::Release);
        }

        // Cursor + index now reflect L1; the Composer may build batches.
        self.inner.l1_head.mark_initialized();

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
        Ok(())
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
        let timestamp = parent_header.timestamp().saturating_add(BLOCK_TIME_SECS);

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
            suggested_fee_recipient: Address::ZERO,
            prev_randao: B256::ZERO,
            gas_limit: BUILDER_GAS_LIMIT,
            parent_beacon_block_root: chain_spec
                .is_cancun_active_at_timestamp(timestamp)
                .then_some(B256::ZERO),
            withdrawals: chain_spec
                .is_shanghai_active_at_timestamp(timestamp)
                .then(alloy_eips::eip4895::Withdrawals::default),
            extra_data: Bytes::from_static(BUILDER_EXTRA_DATA),
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
        let execution_data = <EthEngineTypes as PayloadTypes>::block_to_payload(sealed_block, None);
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
    ) -> DeriverResult<DeriveOutcome> {
        let (payload, header) = self.execute_block(parent_block_number, raw_txs)?;
        Ok(self.inner.committer.commit_derived(payload, header).await?)
    }

    /// Runs the deriver loop. Subscribes to the `L1Watcher`'s event
    /// broadcast and processes each event until the stream closes.
    pub async fn run(self) {
        // Subscribe FIRST. broadcast::channel only delivers events
        // fired after the subscription; any event fired before is lost
        // to this receiver. Subscribing here means events fired during
        // the resync below are queued in the broadcast buffer and
        // delivered to us once we enter the recv() loop.
        let mut rx = self.inner.l1_watcher.subscribe();

        // Resync: re-anchor against L1 to cover the window between
        // main.rs's boot-time `catch_up` and the subscription above.
        // Batches that landed in that window are visible neither in
        // the boot scan nor in live events; a reorg in that window is
        // worse — it invalidates batches the boot scan already
        // indexed, and no Reorg event for it will ever arrive. The
        // reorg-aware resync handles both.
        if let Err(err) = self.resync().await {
            event!(
                name: "eez.deriver.resync.failed",
                Level::ERROR,
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
                                error = %err,
                                "block committer gone; deriver exiting",
                            );
                            return;
                        }
                        // A dropped event would leave `last_indexed_l2`
                        // permanently behind L1 — every later batch
                        // would replay at the wrong heights. Re-anchor
                        // from L1 before processing further events.
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

    /// Post-failure recovery: bounded [`Self::resync`] from the last
    /// indexed batch. Returns `false` iff the committer is gone and the
    /// event loop must exit. A failed resync is logged and retried
    /// after the next L1 event — events keep arriving on every batch,
    /// so recovery is re-attempted at batch cadence.
    async fn try_recover(&self) -> bool {
        match self.resync().await {
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
                    error = %err,
                    "block committer gone; deriver exiting",
                );
                false
            }
            Err(err) => {
                event!(
                    name: "eez.deriver.resync.failed",
                    Level::ERROR,
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
                claimed_current_state,
                claimed_new_state,
                ..
            } => {
                self.on_batch_posted(
                    l1_block_number,
                    l1_block_hash,
                    tx_hash,
                    submitter,
                    call_data,
                    state_applied,
                    claimed_current_state,
                    claimed_new_state,
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
        claimed_current_state: Option<B256>,
        claimed_new_state: Option<B256>,
    ) -> DeriverResult<()> {
        let decoded = eez_payload_codec::decode(call_data.as_ref())?;
        let block_count = decoded.block_count() as u64;
        if block_count == 0 {
            return Ok(());
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
            return Ok(());
        }

        // Acquire lock to prevent sequencing during multi-block derivation
        let _guard = self.inner.committer.begin_reconcile().await;

        // Skip losers — L1's `_applyStateDeltas` (EEZ.sol:967) decides
        // the winner; `state_applied` mirrors `L2ExecutionPerformed`.
        if !state_applied {
            event!(
                name: "eez.deriver.batch.lost_race",
                Level::INFO,
                l1_block_number,
                tx_hash = %tx_hash,
                submitter = %submitter,
                "batch lost the race on L1 (no L2ExecutionPerformed emitted — competing batch's state delta won); skipping",
            );
            return Ok(());
        }

        // from_block is the next L2 block after the highest indexed
        // batch — the shared L1CanonicalHead is the source of truth.
        let last_indexed_l2 = self.inner.l1_head.last_indexed_l2();
        let from_block = last_indexed_l2 + 1;
        let to_block = last_indexed_l2 + block_count;

        // Cursor-alignment guard: a winning batch's claimed
        // `currentState` must equal our state root at the cursor
        // height. A mismatch means the local index is misaligned with
        // L1 (e.g., an earlier event was dropped) — replaying this
        // batch's txs at our cursor heights would commit blocks that
        // exist on no other node. Bail out; the run-loop resync
        // re-anchors the cursor from L1.
        if let Some(claimed_current) = claimed_current_state {
            let local_root = self
                .inner
                .l2_provider
                .sealed_header(last_indexed_l2)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!(
                        "local L2 header at {last_indexed_l2} missing"
                    ))
                })?
                .state_root();
            if local_root != claimed_current {
                event!(
                    name: "eez.deriver.cursor.misaligned",
                    Level::ERROR,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    last_indexed_l2,
                    local_root = %local_root,
                    claimed_current = %claimed_current,
                    "batch currentState does not match local state root at cursor; resync required",
                );
                return Err(DeriverError::local_diverged(from_block));
            }
        }

        // Per-block reconciliation: skip blocks whose tx lists already
        // match the batch, and STF-replay the rest (reth fork-switches
        // as needed).
        let replayed = self
            .reconcile_batch_blocks(from_block, &decoded, l1_block_number, tx_hash, false)
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

        self.check_claimed_state(claimed_new_state, to_block, l1_block_number, tx_hash)?;

        let new_safe_header = self.l2_sealed_header_at(to_block)?;
        let new_safe_hash = new_safe_header.hash();

        // Advance safe; keep finalized where it is (only L1 finality
        // moves it).
        let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
        self.inner
            .committer
            .advance_safe_finalized(new_safe_header, finalized_hash)
            .await?;

        self.inner.safe_l2_block.store(to_block, Ordering::Release);
        self.inner.l1_head.append(BatchRecord {
            l1_block: l1_block_number,
            l1_block_hash,
            tx_hash,
            last_l2_block: to_block,
        });

        event!(
            name: "eez.deriver.safe.advanced",
            Level::INFO,
            from_block,
            to_block,
            l1_block_number,
            tx_hash = %tx_hash,
            submitter = %submitter,
            new_safe_hash = %new_safe_hash,
            "advanced L2 safe head from L1-confirmed batch",
        );
        Ok(())
    }

    async fn on_l1_reorg(
        &self,
        common_ancestor_number: u64,
        old_head_hash: B256,
        new_head_number: u64,
        new_head_hash: B256,
    ) -> DeriverResult<()> {
        // The reorg-aware resync re-derives the retreat point itself
        // (anchor walk against canonical L1), so a delivered Reorg
        // event and one lost to broadcast lag take the same path. The
        // walk also retreats reth's safe head (via sync_batches) and
        // picks up the new chain's batches in the same pass — the
        // watcher's follow-up BatchPosted events dedup by tx hash.
        event!(
            name: "eez.deriver.l1.reorg",
            Level::WARN,
            common_ancestor_number,
            old_head_hash = %old_head_hash,
            new_head_number,
            new_head_hash = %new_head_hash,
            "L1 reorg reported; re-anchoring the batch index from canonical L1",
        );
        self.resync().await
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
        decoded: &eez_payload_codec::DecodedBatch,
        l1_block_number: u64,
        tx_hash: B256,
        force_replay: bool,
    ) -> DeriverResult<u64> {
        let mut tx_offset = 0usize;
        let mut replayed: u64 = 0;
        let force_replay =
            force_replay || !local_batch_boundary_matches(&self.inner.l2_provider, from_block)?;
        for (i, count) in decoded.block_tx_counts.iter().enumerate() {
            let l2_block = from_block + i as u64;
            let count_usize = usize::from(*count);
            let block_txs = &decoded.transactions[tx_offset..tx_offset + count_usize];
            tx_offset += count_usize;
            let matched = if force_replay {
                false
            } else {
                local_block_matches(&self.inner.l2_provider, l2_block, block_txs)?
            };
            let should_replay = force_replay || replayed > 0 || !matched;
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
            self.replay_block(l2_block - 1, block_txs).await?;
            replayed += 1;
        }
        Ok(replayed)
    }

    /// Loud-fail if the batch's claimed `newState` disagrees with our
    /// STF's state root at `to_block`. No-op when the batch carries no
    /// claim for our rollup.
    ///
    /// Under the mock prover, `verify` can't enforce linearity, so a
    /// dishonest composer could land a wrong `newState` that L1 accepts.
    /// Halting here surfaces the mismatch where it originates instead of
    /// later, when our own next post reverts with `StateRootMismatch`.
    fn check_claimed_state(
        &self,
        claimed_new_state: Option<B256>,
        to_block: u64,
        l1_block_number: u64,
        tx_hash: B256,
    ) -> DeriverResult<()> {
        let Some(claimed) = claimed_new_state else {
            return Ok(());
        };
        let local_root = self
            .inner
            .l2_provider
            .sealed_header(to_block)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("local L2 header at {to_block} missing"))
            })?
            .state_root();
        if local_root != claimed {
            event!(
                name: "eez.deriver.state.diverged",
                Level::ERROR,
                l1_block_number,
                tx_hash = %tx_hash,
                to_block,
                local_root = %local_root,
                claimed = %claimed,
                "local L2 state root differs from the batch's claimed newState",
            );
            return Err(DeriverError::local_diverged(to_block));
        }
        Ok(())
    }
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

/// `true` iff the first local block in a batch is anchored to the current
/// local parent. `false` if the block is missing or sits on stale ancestry.
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
