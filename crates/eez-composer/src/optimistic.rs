//! Per-rollup ledger of cross-chain txs whose Sync block committed to L2
//! *before* their L1 bundle settled.
//!
//! Optimistic composition commits the rich Sync block immediately and
//! observes the L1 bundle in the background; between commit and
//! settlement the drained user txs live here, keyed by Sync-block L2
//! height. Three resolution paths:
//!
//! - **Settled**: retained until L1 finality so a reorg can still
//!   recover the txs ([`OptimisticallyIncluded::take_rolled_out`]); at
//!   finality the composer audits the postBatch receipt before dropping
//!   ([`OptimisticallyIncluded::take_finalized`]) — a missed-reorg
//!   backstop (invariant 7).
//! - **Failed**: the observer only marks it
//!   ([`OptimisticallyIncluded::mark_failed`]); recovery (reorg to the parent,
//!   substitute an empty sibling, re-push unburned txs) runs at the next slot
//!   event, serialized against the Sequencer and Deriver to close the TOCTOU
//!   ([`OptimisticallyIncluded::take_failed_for_recovery`]).
//! - **Cursor-confirmed**: the Deriver's cursor passing the Sync height
//!   proves settlement independently of the observer
//!   ([`OptimisticallyIncluded::resolve_below_cursor`]); the
//!   one-in-flight gate ([`OptimisticallyIncluded::blocking_height`])
//!   holds emission until then.
//!
//! Keyed by L2 heights; the L1⇄L2 map is the Deriver-owned
//! `L1CanonicalHead` batch index.

use std::collections::BTreeMap;
use std::sync::Mutex;

use alloy_primitives::TxHash;
use reth_primitives_traits::SealedHeader;

use crate::held_pool::HeldTx;

/// A failed entry extracted for slot-context recovery.
///
/// "Failed" here always means the bundle did not land (postBatch had no
/// receipt by its target L1 block). Under strict all-or-nothing bundles
/// a reverting tx is excluded → the whole bundle drops, so the outcome
/// can't distinguish "relay bad luck" from "a tx would revert" — both
/// look like a drop. Poison is therefore caught earlier, at compose
/// time (a tx whose chained simulation deterministically fails is
/// evicted before it can ever enter a bundle); a drop that reaches
/// recovery is treated as bad luck and re-queued, with
/// [`MAX_BUNDLE_ATTEMPTS`](crate::composer::MAX_BUNDLE_ATTEMPTS) as a
/// backstop for poison the compose-time sim view missed. The same bound covers
/// proof failures and compose-time funding deferrals, so one counter follows a
/// transaction across every bounded retry path.
#[derive(Debug)]
pub struct FailedBatch {
    /// L2 height of the optimistically-committed Sync block.
    pub sync_height: u64,
    /// Hash of the batch's postBatch L1 tx (keccak of the raw
    /// EIP-2718 envelope) — preserved across reinsert so the finality
    /// audit can still locate the receipt.
    pub post_batch_hash: TxHash,
    /// The Sync block's parent — the reorg target if the block landed.
    pub parent: SealedHeader<alloy_consensus::Header>,
    /// The user txs whose effects the block carried.
    pub txs: Vec<HeldTx>,
    /// Drop not attributable to the txs (skipped L1 slot, relay transport
    /// failure) — recovery re-queues without counting toward poison-eviction.
    pub slot_skipped: bool,
}

/// Resolution state of one optimistically-committed Sync block's batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolution {
    /// Bundle submitted; outcome not yet known.
    Pending,
    /// L1 settled (observer verdict or Deriver cursor confirmation).
    /// Retained until L1 finality for reorg recovery.
    Settled,
    /// Observer verdict: the bundle didn't land. Awaiting slot-context
    /// recovery (reorg + re-push) via
    /// [`OptimisticallyIncluded::take_failed_for_recovery`]. The
    /// Deriver's cursor reaching this height overrides to Settled — the
    /// cursor is the stronger oracle.
    Failed,
    /// Observer verdict: L1 kept part of the batch. The height stays canonical and
    /// nothing rolls back; txs with a spent nonce are released, the rest
    /// re-queue.
    SettledPartial,
}

#[derive(Debug)]
struct InFlight {
    txs: Vec<HeldTx>,
    /// Hash of the bundle's postBatch L1 tx — the finality audit
    /// checks its receipt before the entry is dropped.
    post_batch_hash: TxHash,
    parent: SealedHeader<alloy_consensus::Header>,
    resolution: Resolution,
    /// Whether the Deriver cursor has confirmed this entry. This is
    /// independent from `resolution`: the observer can mark an entry
    /// Settled first, while cursor confirmation owns the held pool's
    /// one-time in-flight cleanup.
    cursor_confirmed: bool,
    /// Set by `mark_failed` when the drop wasn't the bundled txs' fault.
    slot_skipped: bool,
}

/// Ledger of in-flight and settled-but-unfinalized optimistic batches.
/// One per cross-chain rollup, alongside its `HeldPool`.
#[derive(Debug, Default)]
pub struct OptimisticallyIncluded {
    by_sync_height: Mutex<BTreeMap<u64, InFlight>>,
}

impl OptimisticallyIncluded {
    /// Empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a freshly-submitted batch: the Sync block at
    /// `sync_height` carries `txs`' cross-chain effects and its bundle
    /// — whose postBatch L1 tx hashes to `post_batch_hash` — is now in
    /// flight. Replaces any stale entry at the same height (a
    /// failed-then-reorged slot rebuilds at the same height).
    pub fn begin(
        &self,
        sync_height: u64,
        post_batch_hash: TxHash,
        parent: SealedHeader<alloy_consensus::Header>,
        txs: Vec<HeldTx>,
    ) {
        let mut map = self.by_sync_height.lock().unwrap();
        map.insert(
            sync_height,
            InFlight {
                txs,
                post_batch_hash,
                parent,
                resolution: Resolution::Pending,
                cursor_confirmed: false,
                slot_skipped: false,
            },
        );
    }

    /// The lowest entry height above `cursor`, if any — the previous
    /// postBatch is not yet DERIVER-confirmed and the composer must
    /// skip emission this slot. Counts Pending, Settled, AND Failed
    /// entries: an observer settle-verdict is NOT enough to re-open
    /// the gate — only the Deriver's cursor passing the height proves
    /// the next bundle would read a fresh `posted` cursor. (Failed
    /// entries are extracted by `take_failed_for_recovery` before this
    /// check; one still present means recovery didn't complete — stay
    /// blocked.) Call after [`Self::resolve_below_cursor`].
    #[must_use]
    pub fn blocking_height(&self, cursor: u64) -> Option<u64> {
        let map = self.by_sync_height.lock().unwrap();
        map.range(cursor + 1..).next().map(|(h, _)| *h)
    }

    /// Resolve entries the Deriver's cursor covers; the cursor outranks any
    /// observer verdict, including Failed. It confirms the height, not which txs
    /// ran, so it releases txs only for a full-settle verdict and sends every
    /// other entry to nonce disposition, which is what finds an unconsumed tail.
    pub fn resolve_below_cursor(&self, cursor: u64) -> Vec<HeldTx> {
        let mut map = self.by_sync_height.lock().unwrap();
        let mut newly_cursor_confirmed = Vec::new();
        for (_, entry) in map.range_mut(..=cursor) {
            if matches!(entry.resolution, Resolution::Pending | Resolution::Failed) {
                entry.resolution = Resolution::SettledPartial;
            }
            if entry.resolution == Resolution::Settled && !entry.cursor_confirmed {
                newly_cursor_confirmed.extend(entry.txs.iter().cloned());
                entry.cursor_confirmed = true;
            }
        }
        newly_cursor_confirmed
    }

    /// L1 kept part of the batch. Unlike [`Self::mark_failed`] the height is not rolled
    /// back; recovery only disposes of the transactions.
    pub fn mark_settled_partial(&self, sync_height: u64, post_batch_hash: TxHash) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height)
            && entry.post_batch_hash == post_batch_hash
            && entry.resolution == Resolution::Pending
        {
            entry.resolution = Resolution::SettledPartial;
        }
    }

    /// The next entry at or below `cursor` awaiting tx disposition: the Deriver
    /// has then rebuilt the block, so L2 nonces show which outbound txs it kept. The caller must NOT reorg: the height is
    /// canonical.
    ///
    /// The entry stays until [`Self::finish_partial`], so a disposition that
    /// cannot read nonces yet simply retries on the next tick.
    #[must_use]
    pub fn partial_to_dispose(&self, cursor: u64) -> Option<FailedBatch> {
        let map = self.by_sync_height.lock().unwrap();
        let (&h, entry) = map
            .range(..=cursor)
            .find(|(_, e)| e.resolution == Resolution::SettledPartial)?;
        Some(FailedBatch {
            sync_height: h,
            post_batch_hash: entry.post_batch_hash,
            parent: entry.parent.clone(),
            // Copied, not drained: disposition awaits nonce reads, and a reorg
            // in that window must still find every tx here to recover. The full
            // set is right then, since the reorg also undoes their spent nonces.
            txs: entry.txs.clone(),
            // A partial settlement is not a drop: the entries that ran, ran.
            slot_skipped: false,
        })
    }

    /// Settle a disposed entry, keeping only its spent txs: only those can be
    /// stranded by a reorg, and the re-queued ones now belong to the pool.
    ///
    /// `post_batch_hash` names the bundle disposed: a rebuild can replace the
    /// entry at this height meanwhile, and must keep its own txs.
    pub fn finish_partial(&self, sync_height: u64, post_batch_hash: TxHash, spent: Vec<HeldTx>) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height)
            && entry.post_batch_hash == post_batch_hash
        {
            entry.txs = spent;
            entry.resolution = Resolution::Settled;
            entry.cursor_confirmed = true;
        }
    }

    /// Observer verdict: the bundle settled on L1. Entry is retained
    /// (Settled) until [`Self::take_finalized`].
    ///
    /// Every verdict names its bundle: a slot can rebuild at the same height
    /// while an old observer still runs, and its verdict must not land on the
    /// new batch.
    pub fn mark_settled(&self, sync_height: u64, post_batch_hash: TxHash) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height)
            && entry.post_batch_hash == post_batch_hash
        {
            entry.resolution = Resolution::Settled;
        }
    }

    /// Observer verdict: the bundle failed (dropped, or landed without
    /// reaching the claimed final state root). Marks the entry Failed;
    /// the actual recovery (L2 reorg + re-push) happens in slot
    /// context via [`Self::take_failed_for_recovery`] — the observer
    /// task never mutates chain state. No-op if the entry is already
    /// Settled (cursor confirmation wins) or gone. See
    /// [`FailedBatch::slot_skipped`] for the flag.
    pub fn mark_failed(&self, sync_height: u64, post_batch_hash: TxHash, slot_skipped: bool) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height)
            && entry.post_batch_hash == post_batch_hash
            && entry.resolution == Resolution::Pending
        {
            entry.resolution = Resolution::Failed;
            entry.slot_skipped = slot_skipped;
        }
    }

    /// Extract the Failed entry above `cursor`, if any, for slot-
    /// context recovery (at most one can exist — the gate blocks new
    /// emission while any entry is unresolved). The caller performs
    /// the reorg + re-push; on a recovery error it re-inserts via
    /// [`Self::reinsert_failed`] so the gate stays closed and the next
    /// slot retries.
    #[must_use]
    pub fn take_failed_for_recovery(&self, cursor: u64) -> Option<FailedBatch> {
        let mut map = self.by_sync_height.lock().unwrap();
        let h = map
            .range(cursor + 1..)
            .find(|(_, e)| e.resolution == Resolution::Failed)
            .map(|(h, _)| *h)?;
        let entry = map.remove(&h)?;
        Some(FailedBatch {
            sync_height: h,
            post_batch_hash: entry.post_batch_hash,
            parent: entry.parent,
            txs: entry.txs,
            slot_skipped: entry.slot_skipped,
        })
    }

    /// Put a failed entry back after an unsuccessful recovery attempt.
    pub fn reinsert_failed(&self, batch: FailedBatch) {
        let mut map = self.by_sync_height.lock().unwrap();
        map.insert(
            batch.sync_height,
            InFlight {
                txs: batch.txs,
                post_batch_hash: batch.post_batch_hash,
                parent: batch.parent,
                resolution: Resolution::Failed,
                cursor_confirmed: false,
                slot_skipped: batch.slot_skipped,
            },
        );
    }

    /// L1 reorg rolled out every SETTLED batch above `new_l2_cursor`
    /// (the retreated Deriver cursor). Removes and returns their txs
    /// for re-queueing. Pending entries stay — an L1 reorg does not
    /// invalidate an in-flight bundle targeting a future L1 block (its
    /// observer will resolve it); Failed entries stay for the slot
    /// recovery path. A partial settlement not yet swept rolls out too:
    /// left behind, it would sit above the cursor and hold the gate shut.
    #[must_use]
    pub fn take_rolled_out(&self, new_l2_cursor: u64) -> Vec<HeldTx> {
        let mut map = self.by_sync_height.lock().unwrap();
        let heights: Vec<u64> = map
            .range(new_l2_cursor + 1..)
            .filter(|(_, e)| {
                matches!(
                    e.resolution,
                    Resolution::Settled | Resolution::SettledPartial
                )
            })
            .map(|(h, _)| *h)
            .collect();
        heights
            .into_iter()
            .filter_map(|h| map.remove(&h))
            .flat_map(|e| e.txs)
            .collect()
    }

    /// L1 finality reached `finalized_l2`: extract the Settled entries
    /// at or below it as `(sync_height, post_batch_hash, txs)` for the
    /// caller's finality audit. The entries are removed — but NOT
    /// dropped blind: the caller verifies each postBatch receipt still
    /// exists on L1 before discarding, catching the
    /// L1Watcher-missed-a-reorg case where a rolled-out batch would
    /// otherwise leave phantom effects on L2. Pending and Failed
    /// entries stay (their own resolution paths own them).
    #[must_use]
    pub fn take_finalized(&self, finalized_l2: u64) -> Vec<(u64, TxHash, Vec<HeldTx>)> {
        let mut map = self.by_sync_height.lock().unwrap();
        let heights: Vec<u64> = map
            .range(..=finalized_l2)
            .filter(|(_, e)| e.resolution == Resolution::Settled)
            .map(|(h, _)| *h)
            .collect();
        heights
            .into_iter()
            .filter_map(|h| map.remove(&h).map(|e| (h, e.post_batch_hash, e.txs)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, Bytes, keccak256};
    use proptest::prelude::*;

    fn tx(tag: u8) -> HeldTx {
        let raw = Bytes::from(vec![tag; 4]);
        let hash = keccak256(raw.as_ref());
        HeldTx {
            raw_tx: raw,
            hash,
            attempts: 0,
            max_fee_per_gas: u128::from(tag),
            priority_fee_per_gas: u128::from(tag),
            sender: alloy_primitives::Address::repeat_byte(tag),
            nonce: u64::from(tag),
            direction: crate::ingress::Direction::Inbound,
        }
    }

    fn hdr() -> SealedHeader<alloy_consensus::Header> {
        SealedHeader::new(alloy_consensus::Header::default(), B256::ZERO)
    }

    /// Dummy postBatch tx hash, distinguishable per batch.
    fn pb_hash(tag: u8) -> TxHash {
        TxHash::repeat_byte(tag)
    }

    /// The cursor confirms the height, not which txs ran. When it arrives before
    /// any verdict, the batch still goes to disposition, or an unconsumed tail
    /// never returns to the pool.
    #[test]
    fn a_cursor_that_confirms_first_hands_the_batch_to_disposition() {
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(10, pb_hash(0xA1), hdr(), vec![tx(1), tx(2)]);

        // Deriver wins the race; the observer's verdict comes too late to matter.
        assert!(
            ledger.resolve_below_cursor(10).is_empty(),
            "nothing is released blind"
        );
        ledger.mark_settled_partial(10, pb_hash(0xA1));

        let partial = ledger
            .partial_to_dispose(10)
            .expect("the confirmed batch must reach disposition");
        assert_eq!(partial.sync_height, 10);
        assert_eq!(
            partial.txs.len(),
            2,
            "the tail must be available for disposal"
        );
    }

    /// A reorg undoes the spent nonces disposition trusted, so those txs stay
    /// recoverable here. The re-queued ones do not: the pool owns them now.
    #[test]
    fn a_reorg_recovers_the_burned_txs_and_leaves_the_requeued_ones_alone() {
        let ledger = OptimisticallyIncluded::new();
        let (burned, requeued) = (tx(3), tx(4));
        ledger.begin(20, pb_hash(0xB2), hdr(), vec![burned.clone(), requeued]);
        ledger.mark_settled_partial(20, pb_hash(0xB2));
        let disposed = ledger.partial_to_dispose(20).expect("partial settlement");
        assert_eq!(disposed.txs.len(), 2, "disposition sees the whole bundle");

        // Disposition found tx(3)'s nonce spent and re-queued tx(4).
        ledger.finish_partial(20, pb_hash(0xB2), vec![burned.clone()]);

        let rolled = ledger.take_rolled_out(19);
        assert_eq!(
            rolled.len(),
            1,
            "only the burned tx is the ledger's to recover"
        );
        assert_eq!(rolled[0].hash, burned.hash);
    }

    /// A slot can rebuild at the same height; a disposition still running for
    /// the old bundle must not overwrite the new bundle's txs.
    #[test]
    fn finish_partial_ignores_a_rebuilt_height() {
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(30, pb_hash(0xC1), hdr(), vec![tx(5)]);
        ledger.mark_settled_partial(30, pb_hash(0xC1));
        let old = ledger.partial_to_dispose(30).expect("partial settlement");

        // The slot rebuilds at the same height with a different bundle.
        ledger.begin(30, pb_hash(0xC2), hdr(), vec![tx(6)]);
        ledger.finish_partial(30, old.post_batch_hash, vec![tx(5)]);

        ledger.mark_settled(30, pb_hash(0xC2));
        let rolled = ledger.take_rolled_out(29);
        assert_eq!(rolled.len(), 1);
        assert_eq!(
            rolled[0].hash,
            tx(6).hash,
            "the rebuilt bundle's tx survives"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(192))]

        /// Model arbitrary optimistic begin/observe/confirm/recover sequences.
        /// The public gate permits at most one unresolved batch, cursor
        /// confirmation releases each tx once, and rollback is always
        /// represented by an explicit failed-batch extraction.
        #[test]
        fn optimistic_ledger_state_machine_preserves_invariants(
            commands in proptest::collection::vec((0u8..7, any::<bool>()), 1..128),
        ) {
            let ledger = OptimisticallyIncluded::new();
            let mut cursor = 0u64;
            let mut next_tag = 1u8;
            let mut released = std::collections::HashSet::<TxHash>::new();
            let mut explicit_rollbacks = 0usize;

            for (command, flag) in commands {
                let blocking = ledger.blocking_height(cursor);
                match command {
                    0 if blocking.is_none() => {
                        let height = cursor.saturating_add(1 + u64::from(next_tag % 3));
                        ledger.begin(height, pb_hash(next_tag), hdr(), vec![tx(next_tag)]);
                        next_tag = next_tag.wrapping_add(1);
                    }
                    1 => {
                        if let Some(height) = blocking {
                            let batch = ledger.by_sync_height.lock().unwrap()[&height].post_batch_hash;
                            ledger.mark_settled(height, batch);
                        }
                    }
                    2 => {
                        if let Some(height) = blocking {
                            let batch = ledger.by_sync_height.lock().unwrap()[&height].post_batch_hash;
                            ledger.mark_failed(height, batch, flag);
                        }
                    }
                    3 => {
                        if let Some(height) = blocking {
                            cursor = height;
                            for tx in ledger.resolve_below_cursor(cursor) {
                                prop_assert!(released.insert(tx.hash), "tx released twice");
                            }
                            // Disposition finds every nonce spent in this model.
                            while let Some(batch) = ledger.partial_to_dispose(cursor) {
                                for tx in &batch.txs {
                                    prop_assert!(released.insert(tx.hash), "tx released twice");
                                }
                                ledger.finish_partial(batch.sync_height, batch.post_batch_hash, batch.txs);
                            }
                        }
                    }
                    4 => {
                        if let Some(failed) = ledger.take_failed_for_recovery(cursor) {
                            if flag {
                                ledger.reinsert_failed(failed);
                            } else {
                                explicit_rollbacks += 1;
                            }
                        }
                    }
                    5 => {
                        let _ = ledger.take_finalized(cursor);
                    }
                    _ => {
                        let rolled_back = ledger.take_rolled_out(cursor);
                        explicit_rollbacks += usize::from(!rolled_back.is_empty());
                    }
                }

                let map = ledger.by_sync_height.lock().unwrap();
                let unresolved = map
                    .range(cursor.saturating_add(1)..)
                    .filter(|(_, entry)| entry.resolution != Resolution::Settled)
                    .count();
                prop_assert!(unresolved <= 1, "more than one optimistic batch is unresolved");
                let expected_blocking = map.range(cursor.saturating_add(1)..).next().map(|(h, _)| *h);
                drop(map);
                prop_assert_eq!(ledger.blocking_height(cursor), expected_blocking);
            }

            // Keep this assertion non-vacuous in shrunk cases without requiring
            // a rollback on every generated trace.
            prop_assert!(explicit_rollbacks <= 128);
        }
    }

    #[test]
    fn gate_blocks_until_cursor_passes_even_when_settled() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        assert_eq!(pool.blocking_height(5), Some(10));
        // Observer settle-verdict does NOT re-open the gate…
        pool.mark_settled(10, pb_hash(0xa));
        assert_eq!(pool.blocking_height(5), Some(10));
        // …only the Deriver's cursor passing the height does.
        assert_eq!(pool.blocking_height(10), None);
    }

    /// The cursor confirms the HEIGHT only; promoting a partial settlement to Settled here
    /// would release the tail's reservations and lose those transactions.
    #[test]
    fn a_partial_settlement_is_not_released_by_the_cursor() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_settled_partial(10, pb_hash(0xa));

        assert!(
            pool.resolve_below_cursor(10).is_empty(),
            "a partial settlement owes its tail a disposition; the cursor must not release it",
        );
        let swept = pool
            .partial_to_dispose(10)
            .expect("the partial settlement is still there to sweep");
        assert_eq!(swept.sync_height, 10);
        assert_eq!(swept.txs.len(), 2);
        assert!(
            !swept.slot_skipped,
            "a partial settlement is not a drop: the entries that ran, ran",
        );
        assert!(
            pool.partial_to_dispose(10).is_some(),
            "a read that fails leaves it for the next tick"
        );
        pool.finish_partial(10, pb_hash(0xa), Vec::new());
        assert!(pool.partial_to_dispose(10).is_none(), "swept exactly once");
    }

    /// A partial settlement keeps its height — the Deriver rebuilds the block L1
    /// named — so it must never surface on the reorg path.
    #[test]
    fn a_partial_settlement_is_never_recovered_as_a_failure() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled_partial(10, pb_hash(0xa));
        assert!(
            pool.take_failed_for_recovery(0).is_none(),
            "a canonical height must not be reorged out",
        );
    }

    /// An observer's full-settle verdict stands: a later partial one for the
    /// same bundle is ignored.
    #[test]
    fn a_partial_verdict_cannot_override_a_settled_entry() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled(10, pb_hash(0xa));
        pool.mark_settled_partial(10, pb_hash(0xa));
        assert!(pool.partial_to_dispose(10).is_none());
    }

    /// An L1 reorg can land before the slot sweeps a partial verdict. The entry
    /// must roll out with its block, or it holds the gate shut for good.
    #[test]
    fn an_unswept_partial_settlement_rolls_out_with_its_l1_block() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_settled_partial(10, pb_hash(0xa));

        let rolled = pool.take_rolled_out(9);
        assert_eq!(
            rolled.len(),
            2,
            "the whole block rolled back, so every tx returns"
        );
        assert_eq!(
            pool.blocking_height(9),
            None,
            "nothing left to hold the gate"
        );
        assert!(pool.partial_to_dispose(10).is_none());
    }

    /// A slot can rebuild at a height while the old bundle's observer still
    /// runs. Its verdict names the old bundle, so the new batch is untouched.
    #[test]
    fn a_verdict_for_a_replaced_batch_is_ignored() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.begin(10, pb_hash(0xb), hdr(), vec![tx(2)]);

        pool.mark_failed(10, pb_hash(0xa), false);
        pool.mark_settled(10, pb_hash(0xa));
        pool.mark_settled_partial(10, pb_hash(0xa));
        assert!(pool.take_failed_for_recovery(0).is_none());
        assert!(pool.partial_to_dispose(10).is_none());

        // Still Pending, so the new bundle's own verdict applies.
        pool.mark_failed(10, pb_hash(0xb), false);
        let failed = pool
            .take_failed_for_recovery(0)
            .expect("the new batch's verdict");
        assert_eq!(failed.post_batch_hash, pb_hash(0xb));
    }

    /// Disposition reads L2 nonces, which show the kept outbound txs only once
    /// the Deriver has rebuilt the block, so the sweep waits for the cursor.
    #[test]
    fn a_partial_settlement_waits_for_the_deriver() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled_partial(10, pb_hash(0xa));

        assert!(pool.partial_to_dispose(9).is_none(), "not derived yet");
        assert_eq!(
            pool.blocking_height(9),
            Some(10),
            "the gate stays shut meanwhile"
        );
        assert!(pool.partial_to_dispose(10).is_some());
    }

    #[test]
    fn failed_recovery_extracts_once_and_unblocks() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_failed(10, pb_hash(0xa), false);
        // Failed entry still blocks until recovered.
        assert_eq!(pool.blocking_height(0), Some(10));
        let batch = pool.take_failed_for_recovery(0).expect("failed entry");
        assert_eq!(batch.sync_height, 10);
        assert_eq!(batch.post_batch_hash, pb_hash(0xa));
        assert_eq!(batch.txs.len(), 2);
        assert!(pool.take_failed_for_recovery(0).is_none());
        assert_eq!(pool.blocking_height(0), None);
        // Recovery error path: reinsert keeps the gate closed.
        pool.reinsert_failed(batch);
        assert_eq!(pool.blocking_height(0), Some(10));
    }

    #[test]
    fn reinserted_failure_preserves_nonce_reservations() {
        let held_pool = crate::HeldPool::new();
        let original = tx(1);
        held_pool.push_contiguous(original.clone(), 1).unwrap();
        let reserved = held_pool.pop_n(1);

        let optimistic = OptimisticallyIncluded::new();
        optimistic.begin(10, pb_hash(0xa), hdr(), reserved);
        optimistic.mark_failed(10, pb_hash(0xa), false);
        let failed = optimistic.take_failed_for_recovery(0).unwrap();
        optimistic.reinsert_failed(failed);

        let mut replacement = original;
        replacement.hash = TxHash::repeat_byte(0xff);
        replacement.raw_tx = alloy_primitives::Bytes::from(vec![0xff; 4]);
        assert!(held_pool.push_contiguous(replacement, 1).is_err());
        assert_eq!(optimistic.blocking_height(0), Some(10));
    }

    #[test]
    fn failed_recovery_propagates_slot_skipped() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        // A skipped-slot drop isn't the tx's fault; the flag must reach
        // recovery so the caller requeues without poison-eviction.
        pool.mark_failed(10, pb_hash(0xa), true);
        let batch = pool.take_failed_for_recovery(0).expect("failed entry");
        assert!(batch.slot_skipped);
        assert_eq!(batch.txs.len(), 2);
    }

    #[test]
    fn cursor_resolution_overrides_false_failure() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_failed(10, pb_hash(0xa), false);
        // Deriver confirmed the batch — the false-negative verdict is overridden,
        // and nonces, not the verdict, decide what happens to its txs.
        assert!(pool.resolve_below_cursor(10).is_empty());
        assert!(pool.take_failed_for_recovery(0).is_none());
        assert!(pool.partial_to_dispose(10).is_some());
        assert_eq!(pool.blocking_height(10), None);
    }

    #[test]
    fn cursor_resolution_releases_observer_settled_once() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_settled(10, pb_hash(0xa));

        let released = pool.resolve_below_cursor(10);
        assert_eq!(released.len(), 2);
        assert!(pool.resolve_below_cursor(10).is_empty());
    }

    #[test]
    fn rolled_out_recovers_only_settled() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled(10, pb_hash(0xa));
        pool.begin(15, pb_hash(0xb), hdr(), vec![tx(2)]); // Pending — in-flight bundle
        let recovered = pool.take_rolled_out(8);
        // Only the settled batch's txs come back; the pending bundle's
        // observer will resolve it.
        assert_eq!(recovered.len(), 1);
        assert_eq!(pool.blocking_height(8), Some(15));
    }

    #[test]
    fn finalize_extracts_settled_for_audit() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled(10, pb_hash(0xa));
        pool.begin(15, pb_hash(0xb), hdr(), vec![tx(2)]); // Pending — stays
        let finalized = pool.take_finalized(12);
        // The settled entry is returned with its postBatch hash so the
        // caller can audit the receipt; the pending entry stays.
        assert_eq!(finalized.len(), 1);
        let (height, post_batch_hash, txs) = &finalized[0];
        assert_eq!(*height, 10);
        assert_eq!(*post_batch_hash, pb_hash(0xa));
        assert_eq!(txs.len(), 1);
        assert!(pool.take_finalized(12).is_empty());
        assert!(pool.take_rolled_out(0).is_empty());
        assert_eq!(pool.blocking_height(12), Some(15));
    }
}
