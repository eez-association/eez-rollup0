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
    system_tx_cfg: Option<eez_evm::system_tx::SystemTxContext>,
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
    ///
    /// `system_tx_cfg = Some(_)` enables the cross-chain STF path: the
    /// deriver reconstructs the same `SYSTEM_ADDRESS`-signed system txs
    /// the composer produced (from the postBatch's `entries[]` /
    /// `l2_entries[]`) and prepends them to the batch's Sync block, so
    /// local replay is byte-identical. `None` is the pure-user-tx STF.
    pub fn new(
        l1_watcher: L1Watcher,
        committer: BlockCommitterHandle<EthEngineTypes>,
        l2_provider: Arc<L2>,
        submitter: Submitter,
        chain_spec: Arc<ChainSpec>,
        l2_block_time_secs: u64,
        deploy_block: u64,
        l1_head: Arc<L1CanonicalHead>,
        system_tx_cfg: Option<eez_evm::system_tx::SystemTxContext>,
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
                l2_block_time_secs,
                deploy_block,
                l1_head,
                safe_l2_block: AtomicU64::new(0),
                system_tx_cfg,
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
        // Acquire lock to prevent sequencing during catch-up
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
        let mut total_replayed: u64 = 0;
        for batch in &historical {
            let decoded = eez_payload_codec::decode(batch.call_data.as_ref())?;

            // `settled_count == 0` = nothing applied on L1 (the claimed
            // roots are phantoms). Skip the whole reconcile — no
            // cursor advance, no replay, no state check; the composer's
            // next slot re-attempts over the same range.
            if batch.settled_count == 0 {
                event!(
                    name: "eez.deriver.catch_up.batch.unsettled",
                    Level::DEBUG,
                    l1_block_number = batch.l1_block_number,
                    tx_hash = %batch.tx_hash,
                    "catch_up: postBatch's L1 block has no L2ExecutionPerformed for our rollup; skipping (re-attempt expected)",
                );
                continue;
            }

            let batch_first_l2 = cumulative_l2 + 1;
            let batch_last_l2 = cumulative_l2 + decoded.block_count() as u64;

            // Do NOT unconditionally force-replay: re-executing a block reth
            // ALREADY holds canonically re-canonicalizes it via a head-FCU,
            // which for an earlier block walks reth's head BACKWARD (e.g. 85→1)
            // — wiping the bulk-catch-up chain and stranding the L1-derived
            // FOLLOWER (committer head resets to ~2, then it can't read its own
            // head to derive forward). `local_block_matches` already replays
            // exactly the divergent suffix: a Sequencer-race block has a
            // different tx-list → `matched=false` → replayed (and `replayed>0`
            // cascades the rebuild to every later block on the new ancestry), so
            // correctness is preserved without re-deriving already-correct
            // blocks. `consumed_count` bounds the inbound system-tx
            // reconstruction to the L1-consumed prefix (partial-consumption rule).
            total_replayed += self
                .reconcile_batch_blocks(
                    batch_first_l2,
                    &decoded,
                    batch.l1_block_number,
                    batch.tx_hash,
                    batch.consumed_count,
                    false,
                )
                .await?;

            if !known_tx_hashes.contains(&batch.tx_hash) {
                new_batches.push(BatchRecord {
                    l1_block: batch.l1_block_number,
                    tx_hash: batch.tx_hash,
                    last_l2_block: batch_last_l2,
                });
            }

            // Catch claimed-vs-derived drift now, during startup, rather
            // than waiting for a live event. The endpoint is what L1
            // ACTUALLY stored (`settled_final_state`), which under
            // partial consumption is a prefix root of the claimed
            // chain, not its end.
            self.check_claimed_state(
                batch.claimed_current_state,
                batch.settled_final_state.or(batch.claimed_new_state),
                batch_first_l2,
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

        // Advance reth's safe head to whatever L1 has confirmed. Live
        // on_batch_posted advances safe on each new event; here we
        // catch the safe head up after a bulk replay so RPC clients
        // see the right safe head before the next live event lands.
        if cumulative_l2 > self.inner.safe_l2_block.load(Ordering::Acquire) {
            let safe_hash = self.l2_hash_at(cumulative_l2)?;
            let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
            self.inner
                .committer
                .advance_safe_finalized(safe_hash, finalized_hash)
                .await?;
            self.inner
                .safe_l2_block
                .store(cumulative_l2, Ordering::Release);
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

        // `sealed_header_or_head` falls back to the committer's in-memory
        // canonical head when the DB-scoped provider lacks the (freshly-derived,
        // unpersisted) parent — see its doc for why the follower needs this.
        let parent_header = self.sealed_header_or_head(parent_block_number)?.ok_or_else(|| {
            event!(
                name: "eez.deriver.execute_block.parent_missing",
                Level::ERROR,
                parent_block_number,
                local_best,
                "parent header is neither in the DB nor the committer's canonical head",
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
                // DIAGNOSTIC (phase-1 divergence probe): surface the revm
                // rejection that local_diverged_with_msg otherwise discards.
                event!(
                    name: "eez.deriver.execute_block.tx_rejected",
                    Level::ERROR,
                    block = parent_block_number + 1,
                    tx_idx,
                    error = %e,
                    "execute_transaction REJECTED a re-derived tx",
                );
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
        // Follower / L1-reconcile re-derive — do NOT feed the prover witness task
        // (feed_witness = false); the producer already fed this block at
        // production time. Avoids double-feeding the same block_number.
        let outcome = self
            .inner
            .committer
            .commit_derived(payload, header, false)
            .await?;
        Ok(outcome)
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
                tx_hash,
                submitter,
                call_data,
                state_applied,
                settled_count,
                consumed_count,
                settled_final_state,
                claimed_current_state,
                claimed_new_state,
                ..
            } => {
                self.on_batch_posted(
                    l1_block_number,
                    tx_hash,
                    submitter,
                    call_data,
                    state_applied,
                    settled_count,
                    consumed_count,
                    settled_final_state,
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

    async fn on_batch_posted(
        &self,
        l1_block_number: u64,
        tx_hash: B256,
        submitter: Address,
        call_data: Bytes,
        state_applied: bool,
        settled_count: usize,
        consumed_count: usize,
        settled_final_state: Option<B256>,
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

        // L1-finality gate: no `L2ExecutionPerformed` for our rollupId
        // means the bundle didn't settle — L1's stored root didn't
        // advance and the claimed roots are phantoms. Skip; the
        // composer's next slot re-attempts. Without this gate the
        // deriver would replay the unsettled range, reorg L2, then fail
        // `check_claimed_state` as a spurious divergence.
        if settled_count == 0 {
            event!(
                name: "eez.deriver.batch.unsettled",
                Level::INFO,
                l1_block_number,
                tx_hash = %tx_hash,
                submitter = %submitter,
                "postBatch's L1 block has no L2ExecutionPerformed for our rollup; bundle didn't settle, skipping (re-attempt expected)",
            );
            return Ok(());
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
        // `_applyStateDeltas` fires in the postBatch tx itself. In the
        // DEFERRED-entry path (our setter / deposit flow) it fires later
        // inside the user_tx calling `executeCrossChainCall` — a
        // different tx hash in the same L1 block — so `scan_batch_logs`
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

        // from_block is the next L2 block after the highest indexed
        // batch — the shared L1CanonicalHead is the source of truth.
        let last_indexed_l2 = self.inner.l1_head.last_indexed_l2();
        let from_block = last_indexed_l2 + 1;
        let to_block = last_indexed_l2 + block_count;

        // Per-block reconciliation: skip blocks whose tx lists already
        // match the batch, and STF-replay the rest (reth fork-switches
        // as needed).
        let replayed = self
            .reconcile_batch_blocks(
                from_block,
                &decoded,
                l1_block_number,
                tx_hash,
                consumed_count,
                false,
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

        // Endpoint = what L1 ACTUALLY stored (`settled_final_state`),
        // which under partial consumption is a prefix root of the
        // claimed chain — never the claimed full-chain end.
        self.check_claimed_state(
            claimed_current_state,
            settled_final_state.or(claimed_new_state),
            from_block,
            to_block,
            l1_block_number,
            tx_hash,
        )?;

        let new_safe_hash = self.l2_hash_at(to_block)?;

        // Advance safe; keep finalized where it is (only L1 finality
        // moves it).
        let finalized_hash = self.l2_hash_at(self.inner.l1_head.finalized_l2())?;
        self.inner
            .committer
            .advance_safe_finalized(new_safe_hash, finalized_hash)
            .await?;

        self.inner.safe_l2_block.store(to_block, Ordering::Release);
        self.inner.l1_head.append(BatchRecord {
            l1_block: l1_block_number,
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
        // Hold the reconcile lock across the retreat: the two engine
        // commands below (advance_safe_finalized then reorg_to) must be
        // atomic to the Sequencer, else it could slip a commit_sequenced
        // between them and extend off the about-to-be-orphaned head.
        let _guard = self.inner.committer.begin_reconcile().await;

        // Walk batch index: anything with l1_block > common_ancestor
        // was rolled out. Find the highest still-canonical batch — that
        // batch's last_l2_block is the new (retreated) safe cursor.
        // If no batch is still canonical, cursor goes to 0. The shared
        // L1CanonicalHead handles the cursor + finalized retreats
        // atomically; we just propagate to reth's safe head below.
        let old_cursor = self.inner.l1_head.cursor();
        let (new_cursor, _new_finalized, dropped) = self
            .inner
            .l1_head
            .retreat_on_l1_reorg(common_ancestor_number);
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

        // Resolve the L2 sealed header at the retreated cursor — this
        // is the new safe AND the new canonical head target. Both
        // engine-API calls below need this header (advance_safe_finalized
        // takes the hash; reorg_to takes the full sealed header).
        let new_safe_header = self
            .inner
            .l2_provider
            .sealed_header(new_cursor)
            .map_err(DeriverError::l2_provider)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!(
                    "local L2 header at retreated cursor {new_cursor} missing"
                ))
            })?;
        let new_safe_hash = new_safe_header.hash();

        // Finalized was already bounded inside retreat_on_l1_reorg.
        let new_finalized = self.inner.l1_head.finalized_l2();
        let new_finalized_hash = self.l2_hash_at(new_finalized)?;

        // Order matters: retreat SAFE+FINALIZED first (FCU with the old
        // head still set — accepted because new safe is its ancestor),
        // then HEAD (FCU with head == safe, rolling the canonical head
        // back past new_cursor). Reversed, the head-FCU would carry the
        // OLD safe hash, now off the canonical chain — reth rejects it
        // as `invalid_forkchoice`.
        self.inner
            .committer
            .advance_safe_finalized(new_safe_hash, new_finalized_hash)
            .await?;
        self.inner.committer.reorg_to(new_safe_header).await?;

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
            "L1 reorg rolled out confirmed batches; L2 head retreated to cursor's L2 hash",
        );
        Ok(())
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

        let safe_hash = self.l2_hash_at(current_safe)?;
        let finalized_hash = self.l2_hash_at(bounded)?;
        self.inner
            .committer
            .advance_safe_finalized(safe_hash, finalized_hash)
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

    /// Resolve the sealed header at `number`, falling back to the committer's
    /// in-memory canonical head when the DB-scoped provider doesn't have it.
    ///
    /// A freshly-derived bulk-catch-up head lives in reth's in-memory canonical
    /// window (unpersisted until the head advances past it), which
    /// `l2_provider.sealed_header()` (DB-scoped) cannot see — the committer
    /// mirrors that head on every commit. Without this fallback the L1-derived
    /// FOLLOWER DEADLOCKS: it can't read its own freshly-derived catch-up head
    /// to derive the next block, and the head never persists because it can't
    /// advance past itself. (based-rollup never hit this — it derived
    /// incrementally, giving each block time to persist before the next.)
    /// Returns `None` only when `number` is neither in the DB nor the head.
    fn sealed_header_or_head(
        &self,
        number: u64,
    ) -> DeriverResult<Option<SealedHeader<alloy_consensus::Header>>> {
        if let Some(h) = self
            .inner
            .l2_provider
            .sealed_header(number)
            .map_err(DeriverError::l2_provider)?
        {
            return Ok(Some(h));
        }
        let head = self.inner.committer.last_header();
        if head.number() == number {
            return Ok(Some(head));
        }
        event!(
            name: "eez.deriver.sealed_header_or_head.miss",
            Level::DEBUG,
            requested = number,
            committer_head = head.number(),
            "header not in DB and != committer canonical head",
        );
        Ok(None)
    }

    fn l2_hash_at(&self, l2_block: u64) -> DeriverResult<B256> {
        Ok(self
            .sealed_header_or_head(l2_block)?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("local L2 header at {l2_block} missing"))
            })?
            .hash())
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
        consumed_count: usize,
        force_replay: bool,
    ) -> DeriverResult<u64> {
        // Cross-chain path (skipped when `system_tx_cfg` is `None`):
        // reconstruct the system txs the composer produced, from either
        // codec branch:
        //
        // - `decoded.l2_entries` non-empty: the L2-shape entries travel
        //   in the payload directly (value-bearing batches).
        // - empty: fall back to the on-chain `batch.entries[]` (L1
        //   entries) — for value-free calls L1 and L2 shapes coincide.
        event!(
            name: "eez.deriver.reconcile.start",
            Level::INFO,
            l1_block_number,
            tx_hash = %tx_hash,
            from_block,
            block_count = decoded.block_count(),
            l2_entries_len = decoded.l2_entries.len(),
            cross_chain = self.inner.system_tx_cfg.is_some(),
            "reconcile_batch_blocks entered",
        );
        // A4 outbound follower gate: the batch's OUTBOUND settlement entries,
        // captured out of the cross-chain arm below so the Sync-block replay can
        // cross-check them against the re-executed CrossChainCallExecuted logs.
        // A4 outbound gate state, carried out of the `system_txs` match: the
        // (outbound immediate entry, its paired SIGNED Sync-block user tx) pairs
        // + this L2's rollup id. The gate authorizes each outbound settlement
        // against the user tx the composer drained — NOT a re-executed log (the
        // outbound user tx reverts in plain re-execution, emitting none). See
        // `eez_evm::outbound_gate`.
        let mut gate_outbound: Vec<(eez_evm::types::ExecutionEntrySol, Bytes)> = Vec::new();
        let mut gate_l2_rollup_id: u64 = 0;
        let system_txs = match self.inner.system_tx_cfg.as_ref() {
            Some(cfg) => {
                let entries = if decoded.l2_entries.is_empty() {
                    event!(
                        name: "eez.deriver.reconcile.fetch_entries",
                        Level::INFO,
                        tx_hash = %tx_hash,
                        "fetching postBatch entries via L1 RPC (codec v1 fallback)",
                    );
                    self.fetch_post_batch_entries(tx_hash, l1_block_number).await?
                } else {
                    use alloy_sol_types::SolValue as _;
                    let mut out = Vec::with_capacity(decoded.l2_entries.len());
                    for (i, raw) in decoded.l2_entries.iter().enumerate() {
                        let entry =
                            eez_evm::types::ExecutionEntrySol::abi_decode(raw).map_err(|e| {
                                DeriverError::l2_provider(format!(
                                    "decode l2_entries[{i}] for tx {tx_hash}: {e}"
                                ))
                            })?;
                        out.push(entry);
                    }
                    out
                };
                // Partial-consumption truncation: the deferred FIFO
                // consumes as a PREFIX, so only the consumed prefix's
                // system txs may execute on L2 — rebuilding all of them
                // would put L2 permanently ahead of L1's stored root.
                // `consumed_count` = the AUTHORITATIVE number of inbound
                // deferred entries consumed (the `ExecutionConsumed` event
                // count for our rollupId, EEZ.sol:903), counted DIRECTLY —
                // never derived as `settled_count - (1 + outbound)`, which
                // deflates by one per skipped outbound immediate
                // (`ImmediateEntrySkipped`) and would wrongly drop an
                // inbound delivery. Mirrors based-rollup's per-entry
                // ExecutionConsumed signal.
                // Partition the L1 entries by DIRECTION (discriminate on
                // proxyEntryHash, NOT l2ToL1Calls-emptiness — an outbound entry
                // has non-empty l2ToL1Calls and would be mis-lowered into a wrong
                // executeIncomingCrossChainCall by build_inbound_system_txs, R1):
                //   - OUTBOUND immediate: proxyEntryHash==0 + non-empty l2ToL1Calls
                //   - INBOUND deferred:   proxyEntryHash != 0 + non-empty l2ToL1Calls
                // The leading anchor (proxyEntryHash==0 + EMPTY l2ToL1Calls) signs
                // no system tx → dropped by the emptiness filter.
                let (outbound_entries, mut inbound_deferred): (
                    Vec<eez_evm::types::ExecutionEntrySol>,
                    Vec<eez_evm::types::ExecutionEntrySol>,
                ) = entries
                    .into_iter()
                    .filter(|e| !e.l2ToL1Calls.is_empty())
                    .partition(|e| e.proxyEntryHash == B256::ZERO);

                // Carry this L2's rollup id out for the Sync-block A4 gate below
                // (the pairs themselves are carried after `outbound_paired` is
                // built — they need the Sync-block user txs).
                gate_l2_rollup_id = cfg.this_rollup_id;

                // Partial-consumption truncation for the INBOUND deferred FIFO.
                // `consumed_count` is the ExecutionConsumed event count for our
                // rollupId = exactly the number of inbound deferred entries
                // consumed on L1 (the anchor + outbound immediates emit
                // L2ExecutionPerformed, NOT ExecutionConsumed, so they're already
                // excluded). Robust to a skipped outbound immediate — independent
                // of outbound_entries.len() entirely.
                let consumed_deferred = consumed_count;
                if inbound_deferred.len() > consumed_deferred {
                    event!(
                        name: "eez.deriver.reconcile.partial_consumption",
                        Level::WARN,
                        tx_hash = %tx_hash,
                        entries = inbound_deferred.len(),
                        consumed_deferred,
                        "L1 consumed only a prefix of the batch's deferred entries; truncating system-tx reconstruction to match",
                    );
                    inbound_deferred.truncate(consumed_deferred);
                }

                let starting_nonce = self.system_address_nonce_at(from_block - 1)?;

                // The Sync block's user txs (the LAST L2 block of this batch's
                // range, Rollup-1 §1.3) — the K outbound `executeCrossChainCall`
                // users that pair with the K outbound loads. The deriver pairs
                // POSITIONALLY (i-th outbound entry ↔ i-th user tx); the composer's
                // drain==splice==DA order guarantees the match.
                let sync_user_count = decoded
                    .block_tx_counts
                    .last()
                    .map_or(0, |c| usize::from(*c));
                let user_start = decoded.transactions.len().saturating_sub(sync_user_count);
                let outbound_paired: Vec<(eez_evm::types::ExecutionEntrySol, Bytes)> =
                    outbound_entries
                        .iter()
                        .cloned()
                        .zip(
                            decoded.transactions[user_start..]
                                .iter()
                                .map(|t| Bytes::from(t.clone())),
                        )
                        .collect();

                // Carry the (outbound entry, signed user tx) pairs out for the
                // A4 gate — the positional pairing the composer's
                // drain==splice==DA order guarantees.
                gate_outbound = outbound_paired.clone();

                // THE canonical builder — the SAME function the composer calls
                // (eez_evm::system_tx), so the Sync block's system txs (two-phase
                // SYSTEM_ADDRESS nonces: outbound loads N.., inbound N+K..) AND the
                // interleaved order [load,user,…,deliveries] are byte-identical BY
                // CONSTRUCTION. (Replaces the deriver's own system-first concat,
                // which drifted from the composer's interleave and was L2-invalid
                // for a mixed slot — the inbound delivery's loadExecutionTable
                // self-clean wiped the outbound table before its user tx consumed
                // it. A2b.)
                let pairs = eez_evm::system_tx::build_cross_chain_sync_pairs(
                    &outbound_paired,
                    &inbound_deferred,
                    cfg,
                    starting_nonce,
                )
                .map_err(|e| {
                    DeriverError::l2_provider(format!(
                        "build_cross_chain_sync_pairs(tx={tx_hash}): {e}"
                    ))
                })?;

                event!(
                    name: "eez.deriver.reconcile.system_txs_built",
                    Level::INFO,
                    tx_hash = %tx_hash,
                    sys_tx_count = pairs.len(),
                    outbound = outbound_entries.len(),
                    inbound = inbound_deferred.len(),
                    starting_nonce,
                    "built outbound load + inbound delivery system txs",
                );
                // The COMPLETE interleaved Sync-block tx list (loads + their user
                // txs + deliveries) — the SAME bytes the composer commits.
                eez_evm::system_tx::interleave_sync_block_txs(&pairs)
            }
            None => Vec::new(),
        };

        // A4 — outbound L2->L1 follower gate (HARD). Authorize every outbound
        // settlement entry against its paired, SIGNED Sync-block user tx (the one
        // the composer drained + committed to DA) BEFORE replaying/committing any
        // block of this batch. A phantom withdrawal — an entry with no real user
        // tx behind it — cannot satisfy the signer/value/data/proxy-target binds:
        // a composer can't forge a user's ECDSA signature. The deriver-side mirror
        // of the prover's A3 gate (the SAME shared `eez_evm::outbound_gate` check,
        // so the follower + prover can't drift); defense-in-depth for a follower
        // that re-derives WITHOUT verifying the proof. Runs ONCE, before the loop,
        // so an unauthorized batch is refused (return Err -> no phantom block is
        // committed) rather than rewound after the fact.
        if !gate_outbound.is_empty() {
            let (g_entries, g_txs): (Vec<eez_evm::types::ExecutionEntrySol>, Vec<Bytes>) =
                gate_outbound.iter().cloned().unzip();
            if let Err(e) = eez_evm::outbound_gate::verify_outbound_authorized(
                &g_entries,
                &g_txs,
                gate_l2_rollup_id,
            ) {
                event!(
                    name: "eez.deriver.reconcile.outbound_gate",
                    Level::ERROR,
                    tx_hash = %tx_hash,
                    from_block,
                    error = %e,
                    "A4 outbound gate REJECTED: an outbound settlement entry is not authorized by a signed user tx — refusing to derive this batch (phantom withdrawal)",
                );
                return Err(DeriverError::local_diverged(from_block));
            }
        }

        let mut tx_offset = 0usize;
        let mut replayed: u64 = 0;
        let force_replay =
            force_replay || !local_batch_boundary_matches(&self.inner.l2_provider, from_block)?;
        let last_index = decoded.block_tx_counts.len().saturating_sub(1);
        for (i, count) in decoded.block_tx_counts.iter().enumerate() {
            let l2_block = from_block + i as u64;
            let count_usize = usize::from(*count);
            let user_txs = &decoded.transactions[tx_offset..tx_offset + count_usize];
            tx_offset += count_usize;
            // Per Rollup-1 §1.3 + §13.4.23 the composer always sets
            // `to_block = sync_slot_block`, so the Sync block is the
            // LAST block of every batch's range. Prepend system txs
            // there; earlier blocks stay user-tx-only.
            let is_sync_block = i == last_index;
            let block_txs: Vec<Vec<u8>> = if is_sync_block && !system_txs.is_empty() {
                // `system_txs` is now the COMPLETE interleaved Sync-block tx list
                // (loads + their user txs + deliveries) built canonically via
                // build_cross_chain_sync_pairs → interleave_sync_block_txs — the
                // user txs are ALREADY in it; do NOT append user_txs again.
                system_txs.iter().map(|b| b.to_vec()).collect()
            } else {
                user_txs.to_vec()
            };
            let matched = if force_replay {
                false
            } else {
                local_block_matches(&self.inner.l2_provider, l2_block, &block_txs)?
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
                system_tx_count = if is_sync_block { system_txs.len() } else { 0 },
                replayed_so_far = replayed,
                "reconciling batch block",
            );
            if !should_replay {
                continue;
            }
            self.replay_block(l2_block - 1, &block_txs).await?;
            replayed += 1;
        }
        Ok(replayed)
    }

    /// Fetch a postBatch tx's `entries[]` directly from L1 (used by the
    /// codec-v1 fallback when `decoded.l2_entries` is empty).
    ///
    /// # Errors
    ///
    /// Returns [`DeriverError::l2_provider`] (reused as a transport
    /// error bucket) on RPC failure or ABI decode failure.
    async fn fetch_post_batch_entries(
        &self,
        tx_hash: B256,
        l1_block_number: u64,
    ) -> DeriverResult<Vec<eez_evm::types::ExecutionEntrySol>> {
        use alloy_consensus::Transaction as _;
        use alloy_eips::BlockNumberOrTag;
        use alloy_provider::{Provider as _, ProviderBuilder};
        use alloy_sol_types::SolCall;

        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(self.inner.submitter.rpc_url());
        // Fetch the postBatch tx from its L1 BLOCK (full-tx list), NOT via
        // `eth_getTransactionByHash`. A snapshot-synced / pruned L1 — e.g. the
        // embedded reth booted from a `--minimal` snapshot — has the BLOCK but
        // not the historical `TransactionLookup` index, so a by-hash lookup
        // returns None even though the block carries the tx. The block is always
        // available (the `L1Watcher` already scanned it to surface this batch),
        // so deriving the postBatch calldata from the block makes catch-up
        // robust to a pruned/snapshot L1 with NO archive-node dependency — the
        // core property that lets a wiped node re-derive purely from L1.
        let block = provider
            .get_block_by_number(BlockNumberOrTag::Number(l1_block_number))
            .full()
            .await
            .map_err(|e| {
                DeriverError::l2_provider(format!("get_block({l1_block_number}): {e}"))
            })?
            .ok_or_else(|| {
                DeriverError::l2_provider(format!("L1 block {l1_block_number} not found"))
            })?;
        let tx = block
            .transactions
            .txns()
            .find(|t| *t.inner.tx_hash() == tx_hash)
            .ok_or_else(|| {
                DeriverError::l2_provider(format!(
                    "postBatch tx {tx_hash} not in L1 block {l1_block_number}"
                ))
            })?;
        let input = tx.inner.input();
        let decoded = eez_evm::types::postAndVerifyBatchCall::abi_decode(input)
            .map_err(|e| DeriverError::l2_provider(format!("decode postBatch({tx_hash}): {e}")))?;
        Ok(decoded.batch.entries)
    }

    /// SYSTEM_ADDRESS account nonce at the L2 parent block. Both
    /// composer and deriver query this at the same block hash; reth
    /// is deterministic so they read identical values, which makes
    /// the signed system-tx hashes byte-equal.
    fn system_address_nonce_at(&self, parent_block_number: u64) -> DeriverResult<u64> {
        let Some(cfg) = self.inner.system_tx_cfg.as_ref() else {
            return Ok(0);
        };
        let parent_header = self.sealed_header_or_head(parent_block_number)?.ok_or_else(|| {
            DeriverError::l2_provider(format!(
                "local L2 header at parent {parent_block_number} missing"
            ))
        })?;
        let state = self
            .inner
            .l2_provider
            .state_by_block_hash(parent_header.hash())
            .map_err(DeriverError::l2_provider)?;
        let system_address = cfg.system_signer.address();
        Ok(state
            .account_nonce(&system_address)
            .map_err(DeriverError::l2_provider)?
            .unwrap_or(0))
    }

    /// Loud-fail if the batch's claimed state-root chain disagrees with
    /// our STF's actual L2 roots at the batch boundaries:
    ///
    /// - `claimed_current_state` (first stateDelta.currentState) vs the
    ///   local root at `from_block - 1`.
    /// - `claimed_new_state` (last stateDelta.newState) vs the local
    ///   root at `to_block`.
    ///
    /// Both ends are checked — the composer chains deltas across
    /// entries, so checking one would let a crafted chain pass. Matters
    /// under the mock prover, which can't enforce linearity; halting
    /// here surfaces the mismatch at its origin rather than at our next
    /// post's `StateRootMismatch`.
    fn check_claimed_state(
        &self,
        claimed_current_state: Option<B256>,
        claimed_new_state: Option<B256>,
        from_block: u64,
        to_block: u64,
        l1_block_number: u64,
        tx_hash: B256,
    ) -> DeriverResult<()> {
        if let Some(claimed_curr) = claimed_current_state {
            let pre = from_block.saturating_sub(1);
            let local_pre = self
                .inner
                .l2_provider
                .sealed_header(pre)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!("local L2 header at {pre} missing"))
                })?
                .state_root();
            if local_pre != claimed_curr {
                event!(
                    name: "eez.deriver.state.diverged_pre",
                    Level::ERROR,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    pre_block = pre,
                    local_root = %local_pre,
                    claimed = %claimed_curr,
                    "local L2 state root at from_block-1 differs from batch's claimed currentState",
                );
                return Err(DeriverError::local_diverged(pre));
            }
        }
        if let Some(claimed_new) = claimed_new_state {
            let local_post = self
                .inner
                .l2_provider
                .sealed_header(to_block)
                .map_err(DeriverError::l2_provider)?
                .ok_or_else(|| {
                    DeriverError::l2_provider(format!("local L2 header at {to_block} missing"))
                })?
                .state_root();
            if local_post != claimed_new {
                event!(
                    name: "eez.deriver.state.diverged_post",
                    Level::ERROR,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    to_block,
                    local_root = %local_post,
                    claimed = %claimed_new,
                    "local L2 state root at to_block differs from batch's claimed newState",
                );
                return Err(DeriverError::local_diverged(to_block));
            }
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
