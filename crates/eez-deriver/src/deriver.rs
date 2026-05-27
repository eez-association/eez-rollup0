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
use eez_driver::{BlockCommitterHandle, DeriveOutcome, ForkchoiceOutcome};
use eez_l1::{
    BatchRecord, HistoricalBatch, L1CanonicalHead, L1Event, L1Watcher, L2BlockRef, Submitter,
};
use reth_chainspec::ChainSpec;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes, execute::BlockBuilder};
use reth_evm_ethereum::EthEvmConfig;
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody, SealedHeader, SignedTransaction};
use reth_provider::StateProviderFactory;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{BlockReader, HeaderProvider, TransactionsProvider};
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
    deploy_block: u64,
    /// Shared canonical-head state — cursor + per-batch index +
    /// `finalized_l2`. The Deriver is the sole writer; the Composer
    /// reads `cursor()` to compute the next batch's `from_block`.
    /// W3.16 — replaces the previous duplicate cursor/batches state.
    l1_head: Arc<L1CanonicalHead>,
    /// L2 block number currently reth `safe` head points at. Mirrors
    /// what we last passed to [`BlockCommitterHandle::advance_safe_finalized`];
    /// used to compute the FCU when advancing finalized without
    /// disturbing safe (and vice versa).
    safe_l2_block: AtomicU64,
}

struct ReconciledBlock {
    header: SealedHeader<alloy_consensus::Header>,
    already_canonical: bool,
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
        let evm_config = EthEvmConfig::new(chain_spec);
        Self {
            inner: Arc::new(Inner {
                l1_watcher,
                committer,
                l2_provider,
                submitter,
                evm_config,
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

    /// Sync local state with L1's confirmed batch history: walks past
    /// `BatchPosted` in tx-order, skips losers via `state_applied`,
    /// STF-replays non-matching L2 blocks, populates `L1CanonicalHead`.
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
        let local_head = self
            .inner
            .l2_provider
            .best_block_number()
            .map_err(DeriverError::l2_provider)?;
        event!(
            name: "eez.deriver.catch_up.start",
            Level::INFO,
            local_head,
            "starting historical batch scan to populate L1CanonicalHead and reconcile L2 chain",
        );

        let historical = self
            .inner
            .submitter
            .scan_batches(self.inner.deploy_block)
            .await
            .map_err(|e| DeriverError::l2_provider(format!("catch-up scan: {e}")))?;

        let known_tx_hashes = self.inner.l1_head.known_tx_hashes();
        let mut new_batches: Vec<BatchRecord> = Vec::new();
        let mut cumulative_l2: u64 = 0;
        let mut last_replayed: Option<u64> = None;
        for batch in &historical {
            let decoded = eez_payload_codec::decode(batch.call_data.as_ref())?;

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

            let batch_first_l2 = cumulative_l2 + 1;
            let batch_last_l2 = cumulative_l2 + decoded.block_count() as u64;

            // Same per-block reconciliation as on_batch_posted:
            // replay any block whose local content doesn't match the
            // batch's. Lets startup catch-up handle the case where a
            // prior local Sequencer produced blocks that L1 doesn't
            // agree with (e.g., the previous proposing run lost a race
            // and never reorged its local chain).
            let mut tx_offset = 0usize;
            let mut batch_end_header = None;
            for (i, count) in decoded.block_tx_counts.iter().enumerate() {
                let l2_block = batch_first_l2 + i as u64;
                let count_usize = usize::from(*count);
                let block_txs = &decoded.transactions[tx_offset..tx_offset + count_usize];
                tx_offset += count_usize;

                let reconciled = self.reconcile_l1_block(l2_block, block_txs).await?;
                let matched = reconciled.already_canonical;
                event!(
                    name: "eez.deriver.catch_up.block",
                    Level::DEBUG,
                    l1_block_number = batch.l1_block_number,
                    tx_hash = %batch.tx_hash,
                    l2_block,
                    action = if matched { "skip" } else { "replay" },
                    tx_count = block_txs.len(),
                    "catch_up_to: reconciling batch block",
                );
                batch_end_header = Some(reconciled.header);
                if !matched {
                    last_replayed = Some(l2_block);
                }
            }
            let batch_end_header = batch_end_header.ok_or_else(|| {
                DeriverError::l2_provider(format!(
                    "batch {} decoded to no L2 blocks",
                    batch.tx_hash
                ))
            })?;
            let batch_end_ref = L2BlockRef {
                number: batch_last_l2,
                hash: batch_end_header.hash(),
            };
            if !known_tx_hashes.contains(&batch.tx_hash) {
                new_batches.push(BatchRecord {
                    l1_block: batch.l1_block_number,
                    l1_block_hash: batch.l1_block_hash,
                    tx_hash: batch.tx_hash,
                    last_l2: batch_end_ref,
                });
            }
            // Same divergence check as on_batch_posted — once the block
            // is locally present, compare its STF-produced state root
            // to the batch's claimed `newState`. Catches drift early
            // (during startup catch-up) instead of waiting for a live
            // event later.
            if let Some(claimed) = batch.claimed_new_state {
                let local_root = batch_end_header.state_root();
                if local_root != claimed {
                    event!(
                        name: "eez.deriver.state.diverged",
                        Level::ERROR,
                        l1_block_number = batch.l1_block_number,
                        tx_hash = %batch.tx_hash,
                        to_block = batch_last_l2,
                        local_root = %local_root,
                        claimed = %claimed,
                        "during catch-up: local L2 state root differs from batch's claimed newState",
                    );
                    return Err(DeriverError::local_diverged(batch_last_l2));
                }
            }
            cumulative_l2 = batch_last_l2;
        }

        // Index every batch we walked (de-duped against startup
        // entries) so subsequent live `BatchPosted` events for any
        // of them are skipped as already-processed.
        if !new_batches.is_empty() {
            self.inner.l1_head.append_many(new_batches);
        }

        // Advance reth's safe head to whatever L1 has confirmed. Live
        // on_batch_posted advances safe on each new event; here we
        // catch the safe head up after a bulk replay so RPC clients
        // see the right safe head before the next live event lands.
        if cumulative_l2 > self.inner.safe_l2_block.load(Ordering::Acquire) {
            let safe_hash = self.l2_ref_or_hash_at(cumulative_l2)?;
            let finalized_hash = self.l2_ref_or_hash_at(self.inner.l1_head.finalized_l2())?;
            self.advance_safe_finalized_or_recover(
                L2BlockRef {
                    number: cumulative_l2,
                    hash: safe_hash,
                },
                finalized_hash,
            )
            .await?;
            self.inner
                .safe_l2_block
                .store(cumulative_l2, Ordering::Release);
        }

        if let Some(last) = last_replayed {
            event!(
                name: "eez.deriver.catch_up.done",
                Level::INFO,
                local_head,
                replayed_through = last,
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
        // W3.18 diagnostic: log parent context before touching reth so
        // we can pinpoint failing `state_by_block_hash` lookups.
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
                // W3.18 root-cause hunting: this is the specific error
                // the user has been chasing — reth has the *header* for
                // this block (sealed_header succeeded above) but is
                // refusing to give us its state. Capture as much
                // context as possible.
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

        let attributes = NextBlockEnvAttributes {
            timestamp,
            suggested_fee_recipient: Address::ZERO,
            prev_randao: B256::ZERO,
            gas_limit: parent_header.gas_limit(),
            parent_beacon_block_root: Some(B256::ZERO),
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
            extra_data: Bytes::default(),
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

    async fn reconcile_l1_block(
        &self,
        l2_block: u64,
        raw_txs: &[Vec<u8>],
    ) -> DeriverResult<ReconciledBlock> {
        if let Some(header) = self.local_block_matches(l2_block, raw_txs)? {
            return Ok(ReconciledBlock {
                header,
                already_canonical: true,
            });
        }

        let parent_block_number = l2_block.checked_sub(1).ok_or_else(|| {
            DeriverError::l2_provider("cannot reconcile L2 genesis as a derived block")
        })?;
        let (payload, header) = self.execute_block(parent_block_number, raw_txs)?;
        self.inner
            .committer
            .commit_derived(payload, header.clone())
            .await?;
        Ok(ReconciledBlock {
            header,
            already_canonical: false,
        })
    }

    async fn advance_safe_finalized_or_recover(
        &self,
        safe: L2BlockRef,
        finalized_hash: B256,
    ) -> DeriverResult<()> {
        match self
            .inner
            .committer
            .advance_safe_finalized(safe.hash, finalized_hash)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.is_invalid_forkchoice_state() => {
                event!(
                    name: "eez.deriver.safe.inconsistent",
                    Level::WARN,
                    l2_safe = safe.number,
                    safe_hash = %safe.hash,
                    finalized_hash = %finalized_hash,
                    error = %err,
                    "L1-derived safe/finalized hashes are incompatible with the current head; recovering head to safe",
                );
                let safe_header = self
                    .inner
                    .l2_provider
                    .sealed_header_by_hash(safe.hash)
                    .map_err(DeriverError::l2_provider)?
                    .ok_or_else(|| {
                        DeriverError::l2_provider(format!(
                            "L1-derived safe header {} missing locally",
                            safe.hash
                        ))
                    })?;
                if safe_header.number() != safe.number {
                    return Err(DeriverError::l2_provider(format!(
                        "L1-derived safe hash {} resolved to block {}, expected {}",
                        safe.hash,
                        safe_header.number(),
                        safe.number
                    )));
                }

                match self
                    .inner
                    .committer
                    .recover_head_to_safe(safe_header, Some(finalized_hash))
                    .await?
                {
                    ForkchoiceOutcome::Valid => {
                        event!(
                            name: "eez.deriver.safe.recovered",
                            Level::WARN,
                            l2_safe = safe.number,
                            safe_hash = %safe.hash,
                            finalized_hash = %finalized_hash,
                            "recovered forkchoice head to L1-derived safe block",
                        );
                        Ok(())
                    }
                    ForkchoiceOutcome::Syncing => Err(DeriverError::invalid_forkchoice(format!(
                        "recovery FCU for L1-derived safe {} returned SYNCING",
                        safe.hash
                    ))),
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the local canonical header iff the local block's ordered
    /// tx list matches the L1-posted tx list. This is a staged
    /// equivalence check: it preserves PR #5's cheap happy path while
    /// still letting the follower store hash-bearing L1 checkpoints.
    fn local_block_matches(
        &self,
        l2_block: u64,
        expected_txs: &[Vec<u8>],
    ) -> DeriverResult<Option<SealedHeader<alloy_consensus::Header>>> {
        let Some(local_block) = self
            .inner
            .l2_provider
            .block_by_number(l2_block)
            .map_err(DeriverError::l2_provider)?
        else {
            return Ok(None);
        };

        let local_txs: Vec<Vec<u8>> = local_block
            .body()
            .transactions()
            .iter()
            .map(Encodable2718::encoded_2718)
            .collect();
        if local_txs.len() != expected_txs.len() {
            return Ok(None);
        }

        if local_txs
            .iter()
            .zip(expected_txs.iter())
            .all(|(l, e)| l == e)
        {
            let header = self
                .inner
                .l2_provider
                .sealed_header(l2_block)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!(
                        "local L2 header at matched block {l2_block} missing"
                    ))
                })?;
            Ok(Some(header))
        } else {
            Ok(None)
        }
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

        // Resync: re-scan L1 history to pick up batches that landed
        // between main.rs's boot-time `catch_up` and the subscription
        // above. Without this, those batches are visible neither in
        // the boot scan nor in live events, and a subsequent
        // `BatchPosted` event for a batch above the gap arrives with
        // its tx-list anchored at an L2 height we never materialised
        // — failing in `execute_block` with a missing parent.
        if let Err(err) = self.catch_up().await {
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
                        event!(
                            name: "eez.deriver.event.failed",
                            Level::WARN,
                            error = %err,
                            "deriver failed to handle event; continuing",
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    event!(
                        name: "eez.deriver.l1_events.lagged",
                        Level::WARN,
                        skipped,
                        "L1 event stream lagged; cursor may be stale until next batch",
                    );
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
                self.on_batch_posted(HistoricalBatch {
                    l1_block_number,
                    l1_block_hash,
                    tx_hash,
                    submitter,
                    call_data,
                    state_applied,
                    claimed_current_state,
                    claimed_new_state,
                })
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

    async fn on_batch_posted(&self, batch: HistoricalBatch) -> DeriverResult<()> {
        let decoded = eez_payload_codec::decode(batch.call_data.as_ref())?;
        let block_count = decoded.block_count() as u64;
        if block_count == 0 {
            return Ok(());
        }

        // Dedup by tx_hash — already-indexed = already processed by
        // catch_up; this is a stale live event.
        if self.inner.l1_head.contains_l1_tx(&batch.tx_hash) {
            event!(
                name: "eez.deriver.batch_posted.skipped",
                Level::DEBUG,
                l1_block_number = batch.l1_block_number,
                tx_hash = %batch.tx_hash,
                "live event for an already-indexed batch; skipping",
            );
            return Ok(());
        }

        // Skip losers — L1's `_applyStateDeltas` (EEZ.sol:967) decides
        // the winner; `state_applied` mirrors `L2ExecutionPerformed`.
        if !batch.state_applied {
            event!(
                name: "eez.deriver.batch.lost_race",
                Level::INFO,
                l1_block_number = batch.l1_block_number,
                tx_hash = %batch.tx_hash,
                submitter = %batch.submitter,
                "batch lost the race on L1 (no L2ExecutionPerformed emitted — competing batch's state delta won); skipping",
            );
            return Ok(());
        }

        // from_block is the next L2 block after the highest indexed
        // batch — the shared L1CanonicalHead is the source of truth.
        let last_indexed_l2 = self.inner.l1_head.last_indexed_l2();
        let from_block = last_indexed_l2 + 1;
        let to_block = last_indexed_l2 + block_count;

        // Per-block reconciliation. For each block in the batch:
        //   * local matches (same tx list)   → skip (chain already canonical here)
        //   * local missing                  → replay (we were lagging)
        //   * local has different tx list    → replay; reth handles the L2
        //     reorg transparently via `newPayload + head-FCU` on the new
        //     fork. This is W3.15 — the based-mode path where competing
        //     composers produce locally-different blocks at the same
        //     height and whoever lands first becomes canonical for both.
        //
        // W3.18 diagnostic: the loop is NOT transactional today — partial
        // replays before a mid-loop failure leave reth's canonical chain
        // in a half-state. Per-iteration logging shows exactly which
        // block we got to before a panic / error, so we know how much
        // state mutation already happened on failure.
        let mut tx_offset = 0usize;
        let mut replayed_count: u64 = 0;
        let mut batch_end_header = None;
        for (i, count) in decoded.block_tx_counts.iter().enumerate() {
            let l2_block = from_block + i as u64;
            let count_usize = usize::from(*count);
            let block_txs = &decoded.transactions[tx_offset..tx_offset + count_usize];
            tx_offset += count_usize;
            let reconciled = self.reconcile_l1_block(l2_block, block_txs).await?;
            let matched = reconciled.already_canonical;
            event!(
                name: "eez.deriver.reconcile.block",
                Level::DEBUG,
                l1_block_number = batch.l1_block_number,
                tx_hash = %batch.tx_hash,
                l2_block,
                action = if matched { "skip" } else { "replay" },
                tx_count = block_txs.len(),
                replayed_so_far = replayed_count,
                "reconciling batch block",
            );
            batch_end_header = Some(reconciled.header);
            // If replay fails, the error propagates out. Subsequent logs
            // (`execute_block.parent_missing`, `execute_block.no_state`,
            // etc.) show why; the `replayed_so_far` value above is what
            // we'd want to roll back if we made the loop transactional.
            if !matched {
                replayed_count += 1;
            }
        }
        event!(
            name: "eez.deriver.reconcile.done",
            Level::DEBUG,
            l1_block_number = batch.l1_block_number,
            tx_hash = %batch.tx_hash,
            from_block,
            to_block,
            replayed = replayed_count,
            "per-block reconciliation complete (pre-divergence check)",
        );

        let new_safe_header = batch_end_header.ok_or_else(|| {
            DeriverError::l2_provider(format!("batch {} decoded to no L2 blocks", batch.tx_hash))
        })?;

        // Divergence detection (mock-prover regime): the on-chain
        // `StateDelta.newState` should equal our STF's state root at
        // `to_block`. With a real zk prover this is enforced
        // cryptographically; with the mock, a buggy / dishonest
        // composer could claim a wrong root and L1 would accept it.
        // Then our next post's `currentState` (local L2 state root at
        // `posted_through`) will mismatch L1's stored root and we'll
        // revert with `StateRootMismatch`. Better to halt this batch
        // now so the operator sees the loud failure here, where the
        // mismatch originated.
        if let Some(claimed) = batch.claimed_new_state {
            let local_root = new_safe_header.state_root();
            if local_root != claimed {
                event!(
                    name: "eez.deriver.state.diverged",
                    Level::ERROR,
                    l1_block_number = batch.l1_block_number,
                    tx_hash = %batch.tx_hash,
                    submitter = %batch.submitter,
                    to_block,
                    local_root = %local_root,
                    claimed = %claimed,
                    "local L2 state root differs from the batch's claimed newState; refusing to advance safe",
                );
                return Err(DeriverError::local_diverged(to_block));
            }
        }

        let new_safe_hash = new_safe_header.hash();

        // Advance safe; keep finalized where it is (only L1 finality
        // moves it).
        let finalized_hash = self.l2_ref_or_hash_at(self.inner.l1_head.finalized_l2())?;
        self.advance_safe_finalized_or_recover(
            L2BlockRef {
                number: to_block,
                hash: new_safe_hash,
            },
            finalized_hash,
        )
        .await?;

        self.inner.safe_l2_block.store(to_block, Ordering::Release);
        self.inner.l1_head.append(BatchRecord {
            l1_block: batch.l1_block_number,
            l1_block_hash: batch.l1_block_hash,
            tx_hash: batch.tx_hash,
            last_l2: L2BlockRef {
                number: to_block,
                hash: new_safe_hash,
            },
        });

        event!(
            name: "eez.deriver.safe.advanced",
            Level::INFO,
            from_block,
            to_block,
            l1_block_number = batch.l1_block_number,
            tx_hash = %batch.tx_hash,
            submitter = %batch.submitter,
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
        // Walk batch index: anything with l1_block > common_ancestor
        // was rolled out. Find the highest still-canonical batch — that
        // batch's last_l2_block is the new (retreated) safe cursor.
        // If no batch is still canonical, cursor goes to 0. The shared
        // L1CanonicalHead handles the cursor + finalized retreats
        // atomically; we just propagate to reth's safe head below.
        let old_cursor = self.inner.l1_head.cursor();
        let (new_cursor_ref, new_finalized_ref, dropped) = self
            .inner
            .l1_head
            .retreat_on_l1_reorg(common_ancestor_number);
        let new_cursor = new_cursor_ref.map_or(0, |r| r.number);
        if new_cursor >= old_cursor {
            // L1 reorg happened above where our batches live; nothing
            // for us to retreat.
            event!(
                name: "eez.deriver.l1.reorg.noop",
                Level::DEBUG,
                common_ancestor_number,
                old_head_hash = %old_head_hash,
                new_head_number,
                new_head_hash = %new_head_hash,
                "L1 reorg above our batches; no L2 retreat needed",
            );
            return Ok(());
        }

        // Compute the new safe head's L2 hash.
        let new_safe_ref = match new_cursor_ref {
            Some(r) => r,
            None => L2BlockRef {
                number: 0,
                hash: self.l2_hash_at(0)?,
            },
        };

        // Finalized was already bounded inside retreat_on_l1_reorg.
        let new_finalized_hash =
            new_finalized_ref.map_or_else(|| self.l2_hash_at(0), |r| Ok(r.hash))?;

        self.advance_safe_finalized_or_recover(new_safe_ref, new_finalized_hash)
            .await?;

        self.inner
            .safe_l2_block
            .store(new_cursor, Ordering::Release);

        event!(
            name: "eez.deriver.l1.reorg.retreated",
            Level::WARN,
            common_ancestor_number,
            old_head_hash = %old_head_hash,
            new_head_number,
            new_head_hash = %new_head_hash,
            old_cursor,
            new_cursor,
            dropped_batches = dropped,
            "L1 reorg rolled out confirmed batches; L2 safe head retreated",
        );
        Ok(())
    }

    async fn on_l1_finalized(&self, l1_finalized_block: u64) -> DeriverResult<()> {
        // Find highest batch with l1_block <= l1_finalized_block.
        // That batch's last_l2_block is the new L2 finalized head.
        let Some(new_finalized_ref) = self
            .inner
            .l1_head
            .highest_l2_ref_at_or_below_l1(l1_finalized_block)
        else {
            return Ok(());
        };
        let new_finalized = new_finalized_ref.number;
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
        let bounded_ref = if bounded == new_finalized_ref.number {
            new_finalized_ref
        } else {
            self.inner
                .l1_head
                .cursor_ref()
                .filter(|r| r.number == bounded)
                .or_else(|| {
                    self.inner
                        .l1_head
                        .highest_l2_ref_at_or_below_l1(l1_finalized_block)
                        .filter(|r| r.number == bounded)
                })
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!(
                        "no finalized checkpoint hash for L2 block {bounded}"
                    ))
                })?
        };

        let safe_hash = self.l2_ref_or_hash_at(current_safe)?;
        let finalized_hash = bounded_ref.hash;
        self.advance_safe_finalized_or_recover(
            L2BlockRef {
                number: current_safe,
                hash: safe_hash,
            },
            finalized_hash,
        )
        .await?;
        self.inner.l1_head.set_finalized_ref(Some(bounded_ref));
        event!(
            name: "eez.deriver.finalized.advanced",
            Level::INFO,
            l1_finalized_block,
            l2_finalized = bounded,
            "advanced L2 finalized head from L1 finality",
        );
        Ok(())
    }

    fn l2_hash_at(&self, l2_block: u64) -> DeriverResult<B256> {
        Ok(self
            .inner
            .l2_provider
            .sealed_header(l2_block)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("local L2 header at {l2_block} missing"))
            })?
            .hash())
    }

    fn l2_ref_or_hash_at(&self, l2_block: u64) -> DeriverResult<B256> {
        if l2_block == 0 {
            return self.l2_hash_at(0);
        }
        if let Some(cursor) = self.inner.l1_head.cursor_ref()
            && cursor.number == l2_block
        {
            return Ok(cursor.hash);
        }
        if let Some(finalized) = self.inner.l1_head.finalized_ref()
            && finalized.number == l2_block
        {
            return Ok(finalized.hash);
        }
        self.l2_hash_at(l2_block)
    }
}
