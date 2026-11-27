//! Recovery: what L1's verdict on a bundle does to the chain and the held transactions.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes};
use eez_l1::{
    BundleTarget, DropReason, L1Error, L1Event, RelayReply, SendOutcome, SentBundle, Submitter,
};
use reth_primitives_traits::AlloyBlockHeader;
use reth_storage_api::{BlockReader, StateProviderFactory, TransactionsProvider};
use tokio::sync::broadcast;
use tracing::{Level, event};

use super::nonces::{
    inbound_source_nonces_for_drain, outbound_source_nonces_for_drain, partition_stale,
};
use super::{Composer, Inner};
use crate::held_pool::HeldTx;
use crate::in_flight::{SettlementFailureSource, recover_settlement_failure};
use crate::ingress::Direction;
use crate::local::build_sync_block;
use crate::optimistic::OptimisticallyIncluded;
use crate::rollup::RollupState;

/// Whether [`Composer::recover_failed_batch`] moved the head, so the caller knows
/// whether its slot still owes a block.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryOutcome {
    /// The empty sibling landed, so the caller's slot parent is stale.
    HeadMoved,
    /// Nothing was committed (bail, stale verdict, or nothing to roll back).
    HeadUnchanged,
}

/// Dispose of a recovered batch's txs: a spent nonce (`landed`) releases the
/// tx, anything else returns it to the pool.
fn dispose_recovered_txs(
    rollup_id: u64,
    pool: &crate::HeldPool,
    sync_height: u64,
    txs: Vec<crate::HeldTx>,
    landed: &[alloy_primitives::TxHash],
    charge_attempt: bool,
) {
    // Releasing or evicting a tx can come before reth re-adds it from the
    // dropped block, which would mine it as a plain L2 call.
    pool.refuse(txs.iter().map(|tx| tx.hash), sync_height);
    let mut keep: Vec<crate::HeldTx> = Vec::with_capacity(txs.len());
    let mut release: Vec<crate::HeldTx> = Vec::new();
    let mut dropped = 0usize;
    for tx in txs {
        if landed.contains(&tx.hash) {
            dropped += 1;
            release.push(tx.clone());
            event!(
                name: "eez.composer.recovery.nonce_burned",
                Level::WARN,
                event_name = "eez.composer.recovery.nonce_burned",
                rollup_id,
                tx_hash = %tx.hash,
                "user_tx nonce already spent; not re-queueing (a reverted one must be resubmitted)",
            );
        } else {
            keep.push(tx);
        }
    }
    pool.release_in_flight_batch(&release);
    let (re_pushed, settlement_evicted) = if charge_attempt {
        let recovery =
            recover_settlement_failure(pool, rollup_id, keep, SettlementFailureSource::Relay);
        (recovery.requeued, recovery.evicted)
    } else {
        let re_pushed = keep.len();
        pool.push_front_batch(keep);
        (re_pushed, 0)
    };
    dropped += settlement_evicted;
    if re_pushed > 0 || dropped > 0 {
        event!(
            name: "eez.composer.recovery.re_pushed",
            Level::INFO,
            rollup_id,
            sync_height,
            re_pushed,
            dropped,
            "batch recovered; user_txs re-queued (front) for the next Sync slot",
        );
    }
}

/// Split a partially settled batch's txs with the drain's stale test: a tx is
/// spent once its sender's nonce has moved past it. Also names the tx where L1
/// stopped: the first unspent outbound tx, else the first inbound tx if unspent
/// (a later inbound tx may only be stuck behind one that reverted and burned).
fn partial_disposition(
    txs: &[HeldTx],
    nonces: &HashMap<(Address, Direction), u64>,
) -> (
    Vec<alloy_primitives::TxHash>,
    Option<alloy_primitives::TxHash>,
) {
    let (unspent, spent) = partition_stale(txs.to_vec(), nonces);
    let culprit = unspent
        .iter()
        .find(|tx| tx.direction == Direction::Outbound)
        .or_else(|| {
            txs.iter()
                .find(|tx| tx.direction == Direction::Inbound)
                .filter(|first| unspent.iter().any(|tx| tx.hash == first.hash))
        })
        .map(|tx| tx.hash);
    (spent.into_iter().map(|tx| tx.hash).collect(), culprit)
}

impl<L2> Inner<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Diagnostic-only handler for L1 events. State (cursor + reorg
    /// retreats) lives in the shared `L1CanonicalHead` written by the
    /// Deriver; here we just log own-vs-external batch attribution +
    /// flag the `expect_external_batches=false` violation when in
    /// sequenced mode.
    pub(super) fn on_l1_event(
        &self,
        event: &Result<L1Event, broadcast::error::RecvError>,
        our_address: alloy_primitives::Address,
    ) {
        match event {
            Ok(L1Event::BatchPosted { post, .. }) => {
                let l1_block_number = post.id.l1_block;
                let tx_hash = post.tx_hash;
                let submitter = post.sender;
                let is_ours = submitter == our_address;
                if is_ours {
                    event!(
                        name: "eez.composer.batch.confirmed",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        "our batch landed on L1",
                    );
                } else {
                    // Only posts listing our rollup arrive here, so this one is
                    // a peer's. Log level by whether any rollup expects external
                    // batches.
                    let any_expects_external = self
                        .rollups
                        .values()
                        .any(|r| r.config.expect_external_batches);
                    if any_expects_external {
                        event!(
                            name: "eez.composer.batch.external",
                            Level::INFO,
                            l1_block_number,
                            tx_hash = %tx_hash,
                            submitter = %submitter,
                            "external batch landed (based mode)",
                        );
                    } else {
                        event!(
                            name: "eez.composer.batch.external.unexpected",
                            Level::ERROR,
                            l1_block_number,
                            tx_hash = %tx_hash,
                            submitter = %submitter,
                            "external batch landed in sequenced-mode rollup — someone else is sequencing our chain",
                        );
                    }
                }
            }
            Ok(L1Event::Reorg {
                common_ancestor_number,
                ..
            }) => {
                // L1 rolled out blocks. Any optimistic batch above the
                // retreated cursor lost its L1 backing — recover its
                // txs into the held pool for re-composition. The
                // Deriver independently retreats the cursor and the L2
                // head; `highest_l2_at_or_below_l1` reads the same
                // batch index it prunes, so this is order-independent.
                for (rollup_id, rollup) in &self.rollups {
                    let new_cursor = rollup
                        .l1_head
                        .highest_l2_at_or_below_l1(*common_ancestor_number)
                        .unwrap_or(0);
                    let txs = rollup.optimistic.take_rolled_out(new_cursor);
                    if txs.is_empty() {
                        continue;
                    }
                    event!(
                        name: "eez.composer.optimistic.l1_reorg_recovery",
                        Level::WARN,
                        rollup_id,
                        common_ancestor = common_ancestor_number,
                        new_cursor,
                        tx_count = txs.len(),
                        "L1 reorg rolled out optimistic batches; re-queueing their user_txs",
                    );
                    rollup.held_pool.push_front_batch(txs);
                }
            }
            Ok(L1Event::Finalized { block_number, .. }) => {
                self.audit_finalized_batches(*block_number);
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                event!(
                    name: "eez.composer.l1_events.lagged",
                    Level::WARN,
                    skipped,
                    "L1 event stream lagged",
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                event!(
                    name: "eez.composer.l1_events.closed",
                    Level::ERROR,
                    "L1 event stream closed",
                );
            }
        }
    }

    /// Finality audit: check each rollup's settled batches at or below the
    /// finalized L1 block before they are discarded.
    fn audit_finalized_batches(&self, block_number: u64) {
        // Settled batches at or below L1 finality can never be
        // rolled out — but "Settled" is the observer's/cursor's
        // belief. Finality audit backstop: before discarding
        // each ledger entry, verify its postBatch receipt still
        // exists on L1. A missing receipt means a reorg rolled
        // the batch out and the L1Watcher missed it. The receipt checks
        // are async while `on_l1_event` is synchronous, so the audit
        // runs in a spawned task per rollup.
        for (rollup_id, rollup) in &self.rollups {
            let Some(fin_l2) = rollup.l1_head.highest_l2_at_or_below_l1(block_number) else {
                continue;
            };
            let finalized = rollup.optimistic.take_finalized(fin_l2);
            if finalized.is_empty() {
                continue;
            }
            let rollup_id = *rollup_id;
            let submitter = self.submitter.clone();
            let held_pool = rollup.held_pool.clone();
            tokio::spawn(async move {
                for (sync_height, post_batch_hash, txs) in finalized {
                    match submitter.receipt_exists(post_batch_hash).await {
                        Ok(true) => {
                            held_pool.release_in_flight_batch(&txs);
                            event!(
                                name: "eez.composer.finality_audit.ok",
                                Level::DEBUG,
                                rollup_id,
                                sync_height,
                                post_batch_hash = %post_batch_hash,
                                "finalized batch audited: postBatch receipt present; ledger entry dropped",
                            );
                        }
                        Ok(false) => {
                            // Detection + tx recovery only —
                            // repairing the L2 history is the
                            // Deriver's job.
                            event!(
                                name: "eez.composer.finality_audit.rolled_out",
                                Level::ERROR,
                                rollup_id,
                                sync_height,
                                post_batch_hash = %post_batch_hash,
                                tx_count = txs.len(),
                                "finalized batch has NO postBatch receipt on L1 — reorg rolled it out unobserved; re-queueing its user_txs (front)",
                            );
                            held_pool.push_front_batch(txs);
                        }
                        Err(err) => {
                            // Inconclusive: re-queueing txs that
                            // actually landed would burn nonces
                            // and poison the next bundle's simulation,
                            // so an inconclusive audit does not recover
                            // transactions speculatively.
                            event!(
                                name: "eez.composer.finality_audit.check_failed",
                                Level::ERROR,
                                rollup_id,
                                sync_height,
                                post_batch_hash = %post_batch_hash,
                                error = %err,
                                "finality-audit receipt lookup failed; ledger entry dropped UNAUDITED",
                            );
                            held_pool.release_in_flight_batch(&txs);
                        }
                    }
                }
            });
        }
    }
}

impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Send
        + Sync
        + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Release the in-flight transactions of batches the cursor has confirmed,
    /// then run [`Self::recover_batches`].
    pub(super) async fn reconcile_slot_batches(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
    ) -> Option<RecoveryOutcome> {
        let newly_cursor_confirmed = rollup.optimistic.resolve_below_cursor(cursor);
        if !newly_cursor_confirmed.is_empty() {
            rollup
                .held_pool
                .release_in_flight_batch(&newly_cursor_confirmed);
        }
        self.recover_batches(rollup_id, rollup, cursor).await
    }

    /// Dispose of a partially settled batch, then recover a failed one.
    /// `Some` when a failed batch was recovered, with whether the head moved.
    pub(super) async fn recover_batches(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
    ) -> Option<RecoveryOutcome> {
        // ── Failure recovery (Sync slot or per-tick hook) ────────────
        // The observer task only RECORDS verdicts; the destructive
        // recovery happens here, serialized with the Sequencer's own
        // commits (the slot loop is sequential — no commit can race
        // this) and with the Deriver (reconcile lock). By now the
        // failed Sync block has either committed (head ≥ height →
        // reorg it out) or permanently didn't (stale-parent bail —
        // nothing to roll back).
        if let Some(partial) = rollup.optimistic.partial_to_dispose(cursor) {
            // No rollback: the height is canonical, so this owes the slot
            // nothing beyond the tx disposition and the slot proceeds normally.
            self.recover_partial_batch(rollup_id, rollup, partial).await;
        }
        let failed = rollup.optimistic.take_failed_for_recovery(cursor)?;
        Some(self.recover_failed_batch(rollup_id, rollup, failed).await)
    }

    /// How many L2 blocks L1 has yet to record. Flat is healthy; monotonic
    /// growth is what every settlement stall looks like.
    pub(super) fn log_settlement_backlog(
        &self,
        rollup_id: u64,
        cursor: u64,
        sync_height: u64,
        gate_blocked: bool,
    ) {
        let backlog = sync_height.saturating_sub(cursor);
        let max_blocks = self.inner.emission.max_blocks;
        if backlog > max_blocks.saturating_mul(2) {
            event!(
                name: "eez.composer.settlement.backlog",
                Level::WARN,
                rollup_id,
                cursor,
                sync_height,
                backlog,
                gate_blocked,
                max_blocks,
                "settlement backlog past 2x the batch cap; L1 is falling behind L2",
            );
        } else {
            event!(
                name: "eez.composer.settlement.backlog",
                Level::INFO,
                rollup_id,
                cursor,
                sync_height,
                backlog,
                gate_blocked,
                "settlement backlog",
            );
        }
    }

    /// Sweep a partially settled batch once the Deriver has rebuilt it: the
    /// height stays canonical, so only the tx disposition is owed.
    ///
    /// A tx is spent once its sender's nonce has moved past it. Spent txs are
    /// released and the rest re-queue; the tx where L1 stopped, if known, alone
    /// is charged an attempt, since the ones behind it did nothing wrong. If any
    /// nonce cannot be read the batch waits for the next tick.
    async fn recover_partial_batch(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        partial: crate::optimistic::FailedBatch,
    ) {
        let sync_height = partial.sync_height;
        let Some(l1_tip) = rollup.l1_head.last_indexed() else {
            return;
        };
        let l2_head = self.inner.committer.last_header().hash();
        // A guess would drop a tx that must return, or re-queue one whose nonce
        // is gone and lose it on a later reorg.
        let Some(nonces) = self
            .source_nonces(
                rollup_id,
                rollup,
                &partial.txs,
                l1_tip.post.l1_block_hash,
                l2_head,
            )
            .await
        else {
            return;
        };
        // Keep slow nonce RPCs outside the reconcile guard. Deriver changes
        // the index and head under it, so validate the exact snapshots before
        // claiming the ledger entry or mutating the pool. A height check alone
        // misses a retreat followed by a same-height replacement.
        let _guard = self.inner.committer.begin_reconcile().await;
        if rollup.l1_head.cursor() < sync_height
            || rollup.l1_head.last_indexed().map(|tip| tip.post) != Some(l1_tip.post)
            || self.inner.committer.last_header().hash() != l2_head
        {
            event!(
                Level::INFO,
                rollup_id,
                sync_height,
                "partial settlement snapshot recheck failed; retaining txs for retry",
            );
            return; // Retain every tx for reorg recovery or a fresh disposition.
        }
        let (spent, culprit) = partial_disposition(&partial.txs, &nonces);
        // The cursor may cover a rival's batch. Nonces still decide what was
        // consumed, but only our own canonical post can justify a retry charge.
        let own_post = rollup.l1_head.contains_l1_tx(partial.post_batch_hash);
        let culprit = culprit.filter(|_| own_post);
        // Only spent txs can be stranded by a reorg; the re-queued ones are the
        // pool's again.
        let stranded: Vec<crate::HeldTx> = partial
            .txs
            .iter()
            .filter(|tx| spent.contains(&tx.hash))
            .cloned()
            .collect();
        // A reorg during the reads may have taken the batch and re-queued its
        // txs. Claim it before touching the pool.
        if !rollup
            .optimistic
            .claim_partial(sync_height, partial.post_batch_hash, stranded)
        {
            event!(
                Level::INFO,
                rollup_id,
                sync_height,
                "partial settlement left the ledger during disposition; leaving its txs alone",
            );
            return;
        }
        event!(
            name: "eez.composer.recovery.settled_partial",
            Level::INFO,
            event_name = "eez.composer.recovery.settled_partial",
            rollup_id,
            sync_height,
            own_post,
            txs = partial.txs.len(),
            spent = spent.len(),
            culprit = ?culprit,
            "partial settlement: spent txs are released, the rest re-queue",
        );
        let (charged, rest): (Vec<_>, Vec<_>) = partial
            .txs
            .into_iter()
            .partition(|tx| culprit == Some(tx.hash));
        let pool = rollup.held_pool.as_ref();
        dispose_recovered_txs(rollup_id, pool, sync_height, rest, &spent, false);
        dispose_recovered_txs(rollup_id, pool, sync_height, charged, &[], true);
    }

    /// Read each sender's source nonce at the supplied L1 and L2 block hashes.
    /// The caller must revalidate these snapshots under the reconcile guard
    /// before applying disposition. Returns `None` unless every sender was read.
    async fn source_nonces(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        txs: &[HeldTx],
        l1_block: B256,
        head: B256,
    ) -> Option<HashMap<(Address, Direction), u64>> {
        let mut nonces = match rollup.l2_provider.state_by_block_hash(head) {
            Ok(state) => outbound_source_nonces_for_drain(state.as_ref(), rollup_id, txs),
            Err(error) => {
                event!(
                    name: "eez.composer.recovery.l2_head_unreadable",
                    Level::WARN,
                    rollup_id,
                    head = %head,
                    error = %error,
                    "L2 head state unreadable; partial settlement disposition waits",
                );
                return None;
            }
        };
        nonces.extend(
            inbound_source_nonces_for_drain(
                &self.inner.cross_chain.exec_ctx,
                rollup_id,
                txs,
                alloy_eips::BlockId::hash(l1_block),
            )
            .await,
        );
        txs.iter()
            .all(|tx| nonces.contains_key(&(tx.sender, tx.direction)))
            .then_some(nonces)
    }

    /// Reorg a landed failed batch out, substitute an empty sibling (the shape
    /// a deriver rebuilds from L1), re-push its txs minus burned nonces.
    async fn recover_failed_batch(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        failed: crate::optimistic::FailedBatch,
    ) -> RecoveryOutcome {
        let sync_height = failed.sync_height;
        let pool = rollup.held_pool.as_ref();
        // Set when the sibling commit moves the head.
        let mut outcome = RecoveryOutcome::HeadUnchanged;
        let committer = &self.inner.committer;
        let landed = match self.recovered_tx_receipts(&failed.txs).await {
            Ok(landed) => landed,
            Err((tx_hash, err)) => {
                event!(
                    name: "eez.composer.recovery.receipt_check_failed",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    tx_hash = %tx_hash,
                    error = %err,
                    "user receipt lookup failed; retaining failed batch for retry",
                );
                rollup.optimistic.reinsert_failed(failed);
                return RecoveryOutcome::HeadUnchanged;
            }
        };
        // Reorg only if the failed block (or descendants) actually
        // became canonical. Rich Sync blocks carry unsettled
        // cross-chain effects; empty (minimal-path) blocks are
        // L1-independent and need no rollback.
        if !failed.txs.is_empty() {
            let guard = committer.begin_reconcile().await;
            // Cursor re-check UNDER the lock: the Deriver may have
            // canonicalized this very batch from L1 between the
            // observer's verdict and this slot (a false-failure
            // verdict, or the bundle landing later than observed).
            // Rolling back an L1-confirmed block would start a
            // recovery-vs-deriver war — the deriver restores it, we
            // roll it back again, and every postBatch in between
            // anchors against the wrong root.
            if rollup.l1_head.cursor() >= sync_height {
                // The cursor proves the height settled, not that these txs ran: a
                // rival or partial batch may have settled it. Back in the ledger
                // below the cursor, the entry goes to nonce disposition.
                rollup.optimistic.reinsert_failed(failed);
                event!(
                    name: "eez.composer.recovery.stale_verdict",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    cursor = rollup.l1_head.cursor(),
                    "failure verdict is stale — Deriver confirmed the height from L1; its txs go to nonce disposition",
                );
                return RecoveryOutcome::HeadUnchanged;
            }
            let head = committer.last_header();
            // Replace only the block that failed: another can hold its height after
            // an L1-reorg retreat, or when its commit bailed after the batch began.
            let replaces = head.number() >= sync_height
                && match rollup.l2_provider.block_hash(sync_height) {
                    Ok(hash) => hash == Some(failed.block),
                    Err(_) => {
                        rollup.optimistic.reinsert_failed(failed);
                        return RecoveryOutcome::HeadUnchanged;
                    }
                };
            if !replaces {
                event!(
                    name: "eez.composer.recovery.not_committed",
                    Level::INFO,
                    rollup_id,
                    sync_height,
                    head = head.number(),
                    "failed Sync block is not canonical; nothing to roll back",
                );
            }
            // Still under the reconcile guard. Committed while the failed branch is
            // canonical, the sibling is a reorg, so reth's pool takes back its pure
            // L2 txs. Head elsewhere → nothing to substitute.
            if replaces || head.hash() == failed.parent.hash() {
                outcome = self
                    .replace_failed_sync_block(rollup_id, rollup, &failed, &guard)
                    .await;
            }
            // The failed block stays canonical until a sibling replaces it, so keep
            // the entry Failed for the next slot. A rewind would lose the pure L2 txs.
            if replaces && matches!(outcome, RecoveryOutcome::HeadUnchanged) {
                rollup.optimistic.reinsert_failed(failed);
                return RecoveryOutcome::HeadUnchanged;
            }
        }
        dispose_recovered_txs(
            rollup_id,
            pool,
            sync_height,
            failed.txs,
            &landed,
            // A drop the txs caused counts toward poison-eviction; a skipped L1
            // slot or relay transport failure does not.
            !failed.slot_skipped,
        );
        outcome
    }

    /// Commit an empty sibling in place of the failed Sync block.
    async fn replace_failed_sync_block(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        failed: &crate::optimistic::FailedBatch,
        _reconcile: &tokio::sync::OwnedMutexGuard<()>,
    ) -> RecoveryOutcome {
        let sync_height = failed.sync_height;
        let committer = &self.inner.committer;
        let timestamp = failed
            .parent
            .timestamp()
            .saturating_add(self.inner.emission.timing.l2_block_time().as_secs());
        match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            &failed.parent,
            timestamp,
            rollup.config.fee_recipient,
            &[],
        ) {
            // feed_witness=true: a PRODUCED block — later historical
            // chunks need its witness in the store.
            Ok(built) => match committer
                .commit_derived(built.payload, built.header, true)
                .await
            {
                Ok(_) => {
                    event!(
                        name: "eez.composer.recovery.substituted",
                        Level::WARN,
                        event_name = "eez.composer.recovery.substituted",
                        rollup_id,
                        sync_height,
                        timestamp,
                        "failed Sync block substituted with its empty sibling",
                    );
                    return RecoveryOutcome::HeadMoved;
                }
                Err(err) => event!(
                    name: "eez.composer.recovery.substitute_failed",
                    Level::ERROR,
                    rollup_id,
                    sync_height,
                    error = %err,
                    "empty-sibling commit failed",
                ),
            },
            Err(err) => event!(
                name: "eez.composer.recovery.substitute_failed",
                Level::ERROR,
                rollup_id,
                sync_height,
                error = %err,
                "empty-sibling build failed",
            ),
        }
        RecoveryOutcome::HeadUnchanged
    }

    /// Hashes of the failed batch's transactions that landed on L1; the
    /// first receipt lookup error, with its transaction, stops the scan.
    async fn recovered_tx_receipts(&self, txs: &[HeldTx]) -> Result<Vec<B256>, (B256, L1Error)> {
        let mut landed = Vec::new();
        for tx in txs {
            if self
                .inner
                .submitter
                .receipt_exists(tx.hash)
                .await
                .map_err(|err| (tx.hash, err))?
            {
                landed.push(tx.hash);
            }
        }
        Ok(landed)
    }
}

/// What the observer records for one bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BundleVerdict {
    /// L1 reached the claimed endpoint.
    Settled,
    /// L1 kept part of the batch; the height stays canonical.
    SettledPartial,
    /// L1 ran nothing of the batch. `charge_attempt` counts an attempt toward eviction.
    Failed { charge_attempt: bool },
    /// The post landed in `l1_block` but its settlement is unread; the cursor decides.
    Unresolved { l1_block: u64 },
}

/// The verdict L1's outcome gives. The relay's answer never decides it: only a
/// bundle the relay accepted is charged, and a skipped pinned slot or
/// `waive_charge` (the target block's timestamp differs from the pin, or the
/// node sent the bundle late) waives that.
pub(super) fn bundle_verdict(
    sent: &SentBundle,
    expected_final_root: B256,
    waive_charge: bool,
) -> BundleVerdict {
    let charge_attempt = sent.reply == RelayReply::Accepted && !waive_charge;
    match &sent.outcome {
        SendOutcome::Included { settlement, .. }
            if settlement.final_state == Some(expected_final_root) =>
        {
            BundleVerdict::Settled
        }
        // The anchor names a candidate at THIS height, so any applied entry keeps
        // the height canonical; only an empty settlement leaves it unratified.
        SendOutcome::Included { settlement, .. } if !settlement.is_empty() => {
            BundleVerdict::SettledPartial
        }
        SendOutcome::Dropped {
            reason: DropReason::PinnedSlotSkipped,
            ..
        } => BundleVerdict::Failed {
            charge_attempt: false,
        },
        SendOutcome::Included { .. } | SendOutcome::Dropped { .. } => {
            BundleVerdict::Failed { charge_attempt }
        }
        SendOutcome::IncludedUnread { l1_block, .. } => BundleVerdict::Unresolved {
            l1_block: *l1_block,
        },
    }
}

/// Reads of the target block's timestamp before its charge is waived.
const PIN_READ_ATTEMPTS: u32 = 3;

/// Whether L1 block `block` is missing or has a timestamp other than `pin`.
/// A read that keeps failing counts as a mismatch, which waives the charge.
async fn pin_mismatch(submitter: &Submitter, block: u64, pin: u64) -> bool {
    for attempt in 1..=PIN_READ_ATTEMPTS {
        match submitter.block_timestamp(block).await {
            Ok(timestamp) => return timestamp != Some(pin),
            Err(_) if attempt < PIN_READ_ATTEMPTS => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(_) => {}
        }
    }
    true
}

/// Background bundle observer — verdict recording ONLY. Marks the ledger
/// entry Settled or Failed; never mutates chain state. The destructive
/// Recovery for failed entries runs at the next Sync slot through
/// `Composer::recover_failed_batch`, serialized with Sequencer commits.
///
/// "Settled" requires an `L2ExecutionPerformed` in the inclusion block
/// whose `newRoot` equals `expected_final_root` (the built Sync block's
/// root) — the leading immediate advancing L1 partway doesn't count.
/// `on_dispatch` runs when the bundle goes out. A bundle `sent_late` by the
/// node charges nothing.
pub(super) async fn observe_bundle_outcome(
    rollup_id: u64,
    sync_height: u64,
    bundle: Vec<Bytes>,
    // The verdict names this bundle, so it cannot land on a batch rebuilt at
    // the same height.
    post_batch_hash: alloy_primitives::TxHash,
    expected_final_root: B256,
    optimistic: Arc<OptimisticallyIncluded>,
    submitter: Submitter,
    target: BundleTarget,
    on_dispatch: impl FnOnce(),
    sent_late: bool,
) {
    let sent = match submitter.send_bundle(&bundle, target, on_dispatch).await {
        Ok(sent) => sent,
        Err(err) => {
            // Nothing reached the relay, so it says nothing about the txs.
            event!(
                name: "eez.composer.bundle.not_sent",
                Level::ERROR,
                rollup_id,
                sync_height,
                error = %err,
                "bundle was not sent; re-queueing transactions without charging an attempt",
            );
            optimistic.mark_failed(sync_height, post_batch_hash, true);
            return;
        }
    };
    let mut verdict = bundle_verdict(&sent, expected_final_root, sent_late);
    // Only a charge can be waived, and only an Exact target carries a pin.
    if let (
        BundleVerdict::Failed {
            charge_attempt: true,
        },
        BundleTarget::Exact { block, timestamp },
    ) = (verdict, target)
        && pin_mismatch(&submitter, block, timestamp).await
    {
        verdict = bundle_verdict(&sent, expected_final_root, true);
    }
    log_bundle_verdict(rollup_id, sync_height, &sent, verdict);
    match verdict {
        BundleVerdict::Settled => optimistic.mark_settled(sync_height, post_batch_hash),
        // L1 kept part of the batch, so this height stays canonical and
        // recovery owes only the tail disposition — no rollback.
        BundleVerdict::SettledPartial => {
            optimistic.mark_settled_partial(sync_height, post_batch_hash)
        }
        // L1 ran nothing of this batch, so the height is unratified and the
        // reorg path is correct.
        BundleVerdict::Failed { charge_attempt } => {
            optimistic.mark_failed(sync_height, post_batch_hash, !charge_attempt)
        }
        // The post landed, so the cursor resolves it unless it reverted or an L1 reorg drops it.
        BundleVerdict::Unresolved { l1_block } => {
            watch_unresolved(
                rollup_id,
                sync_height,
                post_batch_hash,
                l1_block,
                &optimistic,
                &submitter,
            )
            .await;
        }
    }
}

/// How often an unresolved post is checked.
const UNRESOLVED_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Wait while the batch is pending. A post L1 applied nothing of before the
/// cursor covered it fails without a charge: its receipt reverted, or its
/// receipt is gone and `l1_block`, where it landed, reads without it. A
/// successful receipt waits for the cursor; anything unread is read again.
pub(super) async fn watch_unresolved(
    rollup_id: u64,
    sync_height: u64,
    post_batch_hash: alloy_primitives::TxHash,
    l1_block: u64,
    optimistic: &OptimisticallyIncluded,
    submitter: &Submitter,
) {
    while optimistic.is_pending(sync_height, post_batch_hash) {
        let failed = match submitter.receipt_status(post_batch_hash).await {
            // Back from the mempool after a reorg, it can land again and revert.
            Ok(Some(false)) => Some("the postBatch reverted on L1"),
            // A lagging provider answers no receipt too, but it cannot serve the block.
            Ok(None) => matches!(
                submitter.block_has_tx(l1_block, post_batch_hash).await,
                Ok(Some(false))
            )
            .then_some("the postBatch is no longer on L1"),
            Ok(Some(true)) | Err(_) => None,
        };
        if let Some(reason) = failed {
            event!(
                name: "eez.composer.bundle.unresolved_failed",
                Level::WARN,
                rollup_id,
                sync_height,
                post_batch_hash = %post_batch_hash,
                reason,
                "unresolved postBatch applied nothing on L1; failing the batch without charging an attempt",
            );
            optimistic.mark_failed(sync_height, post_batch_hash, true);
            return;
        }
        tokio::time::sleep(UNRESOLVED_POLL_INTERVAL).await;
    }
}

fn log_bundle_verdict(rollup_id: u64, sync_height: u64, sent: &SentBundle, verdict: BundleVerdict) {
    let settled = verdict == BundleVerdict::Settled;
    let (reply, outcome) = (&sent.reply, &sent.outcome);
    match (verdict, outcome) {
        // Settled partially: the height stays canonical and the Deriver rebuilds the
        // block L1 named; only the unconsumed tail is owed a disposition.
        (BundleVerdict::SettledPartial, SendOutcome::Included { settlement, .. }) => event!(
            name: "eez.composer.bundle.observed",
            Level::WARN,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            applied_any = true,
            applied = ?settlement.applied_indices(),
            outcome = ?outcome,
            relay_reply = ?reply,
            "postBatch settled partially; L1 kept part of this batch",
        ),
        (BundleVerdict::Unresolved { .. }, _) => event!(
            name: "eez.composer.bundle.unresolved",
            Level::ERROR,
            rollup_id,
            sync_height,
            outcome = ?outcome,
            relay_reply = ?reply,
            "postBatch landed but its settlement is unreadable; leaving the batch pending for the cursor",
        ),
        (BundleVerdict::Failed { .. }, SendOutcome::Included { .. }) => event!(
            name: "eez.composer.bundle.observed",
            Level::ERROR,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?outcome,
            relay_reply = ?reply,
            "postBatch was included without the expected state transition; possible Composer/prover/settlement-contract disagreement",
        ),
        (_, SendOutcome::Dropped { .. }) => event!(
            name: "eez.composer.bundle.observed",
            Level::WARN,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?outcome,
            relay_reply = ?reply,
            "bundle was dropped before settlement; re-queueing transactions for recovery",
        ),
        _ => event!(
            name: "eez.composer.bundle.observed",
            Level::INFO,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?outcome,
            relay_reply = ?reply,
            "bundle outcome observed",
        ),
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::tests::held;
    use crate::in_flight::MAX_BUNDLE_ATTEMPTS;
    use alloy_primitives::TxHash;

    /// Every relay answer, L1 outcome and pin reading. The verdict differs from
    /// a table where the relay's answer decides only in the rows listed.
    #[test]
    fn relay_reply_only_waives_the_charge() {
        use eez_l1::scan::{AppliedEntry, EntryRole, Settlement};
        let root = B256::repeat_byte(0xaa);
        let applied = |final_state| {
            Settlement::new(
                vec![AppliedEntry {
                    entry_index: 0,
                    role: EntryRole::Anchor,
                }],
                Some(final_state),
                None,
                false,
            )
        };
        let included = |settlement| SendOutcome::Included {
            tx_hash: TxHash::ZERO,
            l1_block: 7,
            settlement,
        };
        let dropped = |reason| SendOutcome::Dropped {
            tx_hash: TxHash::ZERO,
            target_block: 7,
            reason,
        };
        let outcomes = [
            ("settled", included(applied(root))),
            ("partial", included(applied(B256::repeat_byte(0xbb)))),
            ("nothing applied", included(Settlement::NONE)),
            ("slot skipped", dropped(DropReason::PinnedSlotSkipped)),
            ("slot excluded", dropped(DropReason::PinnedSlotExcluded)),
            ("target passed", dropped(DropReason::TargetPassed)),
            (
                "unread",
                SendOutcome::IncludedUnread {
                    tx_hash: TxHash::ZERO,
                    l1_block: 7,
                    error: String::new(),
                },
            ),
        ];
        let replies = [
            RelayReply::Accepted,
            RelayReply::Rejected(String::new()),
            RelayReply::Unanswered(String::new()),
        ];
        // The contrasting table, where the relay decides: one that did not accept
        // fails the batch uncharged, and an unread settlement fails instead of `Unresolved`.
        let relay_decides = |reply: &RelayReply, outcome: &SendOutcome, pin_mismatch: bool| match (
            reply, outcome,
        ) {
            (RelayReply::Rejected(_) | RelayReply::Unanswered(_), _) => BundleVerdict::Failed {
                charge_attempt: false,
            },
            (_, SendOutcome::Included { settlement, .. })
                if settlement.final_state == Some(root) =>
            {
                BundleVerdict::Settled
            }
            (_, SendOutcome::Included { settlement, .. }) if !settlement.is_empty() => {
                BundleVerdict::SettledPartial
            }
            _ => BundleVerdict::Failed {
                charge_attempt: !pin_mismatch,
            },
        };
        for reply in &replies {
            for (case, outcome) in &outcomes {
                for pin_mismatch in [false, true] {
                    // A skipped slot always mismatches its pin, an excluded one never.
                    let reason = match outcome {
                        SendOutcome::Dropped { reason, .. } => Some(*reason),
                        _ => None,
                    };
                    if (reason == Some(DropReason::PinnedSlotSkipped) && !pin_mismatch)
                        || (reason == Some(DropReason::PinnedSlotExcluded) && pin_mismatch)
                    {
                        continue;
                    }
                    let sent = SentBundle {
                        reply: reply.clone(),
                        outcome: outcome.clone(),
                    };
                    let got = bundle_verdict(&sent, root, pin_mismatch);
                    let want = match (reply, *case) {
                        // L1 decides, whatever the relay answered.
                        (RelayReply::Rejected(_) | RelayReply::Unanswered(_), "settled") => {
                            BundleVerdict::Settled
                        }
                        (RelayReply::Rejected(_) | RelayReply::Unanswered(_), "partial") => {
                            BundleVerdict::SettledPartial
                        }
                        // A landed post is never a failure.
                        (_, "unread") => BundleVerdict::Unresolved { l1_block: 7 },
                        _ => relay_decides(reply, outcome, pin_mismatch),
                    };
                    assert_eq!(got, want, "{reply:?} {case} pin_mismatch={pin_mismatch}");
                }
            }
        }
    }

    /// The safety property: a spent nonce is released and never re-queued; an
    /// unspent one must come back.
    #[test]
    fn a_partial_settlement_releases_what_landed_and_requeues_only_the_tail() {
        let pool = crate::HeldPool::new();
        let sender = Address::repeat_byte(0x11);
        for (nonce, byte) in [(1u64, 0xA1u8), (2, 0xA2), (3, 0xA3)] {
            pool.push_contiguous(held(sender, Direction::Inbound, nonce, byte), 1)
                .unwrap();
        }
        let drained = pool.pop_n(3);
        assert_eq!(drained.len(), 3);

        // L1 consumed the first two entries; the third never ran.
        let landed = [TxHash::repeat_byte(0xA1), TxHash::repeat_byte(0xA2)];
        dispose_recovered_txs(1, &pool, 100, drained, &landed, false);

        let queued = pool.pop_all();
        assert_eq!(
            queued.iter().map(|t| t.hash).collect::<Vec<_>>(),
            vec![TxHash::repeat_byte(0xA3)],
            "only the unconsumed tail is re-queued; burned nonces are not",
        );
        assert_eq!(
            queued[0].attempts, 0,
            "an uncharged disposition leaves attempts alone",
        );
    }

    /// A tx is spent once its sender's nonce moves past it, in either
    /// direction; of the rest, the first outbound one is where L1 stopped.
    #[test]
    fn partial_disposition_splits_by_nonce_and_names_the_first_unspent() {
        let (a, b) = (Address::repeat_byte(0x0A), Address::repeat_byte(0x0B));
        let kept_out = held(a, Direction::Outbound, 5, 0x01);
        let cut_out = held(a, Direction::Outbound, 6, 0x02);
        let included_in = held(b, Direction::Inbound, 3, 0x03);
        let dropped_in = held(b, Direction::Inbound, 4, 0x04);
        // Pool order puts an inbound tx first; batch order still leads with outbound.
        let txs = [
            included_in.clone(),
            kept_out.clone(),
            cut_out.clone(),
            dropped_in,
        ];
        let nonces = HashMap::from([((a, Direction::Outbound), 6), ((b, Direction::Inbound), 4)]);

        let (spent, culprit) = partial_disposition(&txs, &nonces);
        assert_eq!(spent, vec![included_in.hash, kept_out.hash]);
        assert_eq!(culprit, Some(cut_out.hash));
    }

    /// With every outbound tx kept, the cut is at the first unspent inbound
    /// one — a reverting tx the builder dropped instead of including.
    #[test]
    fn partial_disposition_falls_back_to_the_first_unspent_inbound() {
        let (a, b) = (Address::repeat_byte(0x0A), Address::repeat_byte(0x0B));
        let kept_out = held(a, Direction::Outbound, 5, 0x01);
        let dropped_in = held(b, Direction::Inbound, 4, 0x04);
        let behind_in = held(b, Direction::Inbound, 5, 0x05);
        let txs = [kept_out, dropped_in.clone(), behind_in];
        let nonces = HashMap::from([((a, Direction::Outbound), 6), ((b, Direction::Inbound), 4)]);

        let (spent, culprit) = partial_disposition(&txs, &nonces);
        assert_eq!(spent.len(), 1, "only the kept outbound tx");
        assert_eq!(culprit, Some(dropped_in.hash), "not the tx behind it");
    }

    /// The first inbound tx failed but was included, so its nonce burned; the
    /// unspent one behind it only got stuck, and must not be charged for it.
    #[test]
    fn partial_disposition_spares_an_inbound_tx_stuck_behind_a_burned_one() {
        let b = Address::repeat_byte(0x0B);
        let reverted_in = held(b, Direction::Inbound, 3, 0x03);
        let stuck_in = held(b, Direction::Inbound, 4, 0x04);
        let txs = [reverted_in.clone(), stuck_in];
        let nonces = HashMap::from([((b, Direction::Inbound), 4)]);

        let (spent, culprit) = partial_disposition(&txs, &nonces);
        assert_eq!(spent, vec![reverted_in.hash]);
        assert_eq!(
            culprit, None,
            "the burned tx already paid; nobody else is charged"
        );
    }

    /// When every tx is spent, as on a builder that includes reverting txs,
    /// nothing re-queues and nobody is charged.
    #[test]
    fn partial_disposition_charges_nobody_when_every_nonce_is_spent() {
        let a = Address::repeat_byte(0x0A);
        let txs = [
            held(a, Direction::Inbound, 1, 0x01),
            held(a, Direction::Inbound, 2, 0x02),
        ];
        let nonces = HashMap::from([((a, Direction::Inbound), 3)]);

        let (spent, culprit) = partial_disposition(&txs, &nonces);
        assert_eq!(spent.len(), 2);
        assert_eq!(culprit, None);
    }

    /// A burned nonce must release its reservation, else the resubmission is
    /// blocked. Probed with a different hash, since re-pushing one is idempotent.
    #[test]
    fn a_burned_nonce_releases_its_in_flight_reservation() {
        let pool = crate::HeldPool::new();
        let sender = Address::repeat_byte(0x22);
        pool.push_contiguous(held(sender, Direction::Inbound, 1, 0xB1), 1)
            .unwrap();
        let drained = pool.pop_n(1);

        let resubmission = held(sender, Direction::Inbound, 1, 0xB2);
        assert!(
            pool.push_contiguous(resubmission.clone(), 1).is_err(),
            "an in-flight reservation must block the nonce before disposition",
        );

        dispose_recovered_txs(1, &pool, 100, drained, &[TxHash::repeat_byte(0xB1)], false);
        pool.push_contiguous(resubmission, 1)
            .expect("the nonce must be free once the burned tx releases its reservation");
        assert_eq!(pool.len(), 1);
    }

    /// The eviction backstop still applies where the bundle genuinely failed and
    /// we cannot tell whose fault it was.
    #[test]
    fn a_charged_attempt_still_evicts_at_the_cap() {
        let pool = crate::HeldPool::new();
        let sender = Address::repeat_byte(0x44);
        let mut tx = held(sender, Direction::Inbound, 1, 0xD1);
        tx.attempts = MAX_BUNDLE_ATTEMPTS - 1;
        pool.push_contiguous(tx, 1).unwrap();
        let drained = pool.pop_n(1);

        dispose_recovered_txs(1, &pool, 100, drained, &[], true);
        assert_eq!(pool.len(), 0, "at MAX_BUNDLE_ATTEMPTS the tx is evicted");
        assert!(
            pool.holds(&TxHash::repeat_byte(0xD1)),
            "an evicted tx stays refused, so reth cannot re-add it from the dropped block",
        );
    }
}
