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
/// proof failures, so one counter follows a transaction across both paths.
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

    /// Flip Pending AND Failed entries at or below the Deriver's
    /// cursor to Settled — the cursor only advances past a batch after
    /// `check_claimed_state` accepted it, which is a stronger
    /// settlement proof than the observer's log scan. A Failed verdict
    /// is overridden here: a false-negative observation must not undo
    /// a batch the Deriver confirmed. Returns the txs newly resolved so
    /// the held pool can release their in-flight nonce reservations.
    pub fn resolve_below_cursor(&self, cursor: u64) -> Vec<HeldTx> {
        let mut map = self.by_sync_height.lock().unwrap();
        let mut newly_cursor_confirmed = Vec::new();
        for (_, entry) in map.range_mut(..=cursor) {
            if entry.resolution != Resolution::Settled {
                entry.resolution = Resolution::Settled;
            }
            if !entry.cursor_confirmed {
                newly_cursor_confirmed.extend(entry.txs.iter().cloned());
                entry.cursor_confirmed = true;
            }
        }
        newly_cursor_confirmed
    }

    /// Observer verdict: the bundle settled on L1. Entry is retained
    /// (Settled) until [`Self::take_finalized`].
    pub fn mark_settled(&self, sync_height: u64) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height) {
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
    pub fn mark_failed(&self, sync_height: u64, slot_skipped: bool) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height)
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
    /// recovery path.
    #[must_use]
    pub fn take_rolled_out(&self, new_l2_cursor: u64) -> Vec<HeldTx> {
        let mut map = self.by_sync_height.lock().unwrap();
        let heights: Vec<u64> = map
            .range(new_l2_cursor + 1..)
            .filter(|(_, e)| e.resolution == Resolution::Settled)
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
                            ledger.mark_settled(height);
                        }
                    }
                    2 => {
                        if let Some(height) = blocking {
                            ledger.mark_failed(height, flag);
                        }
                    }
                    3 => {
                        if let Some(height) = blocking {
                            cursor = height;
                            for tx in ledger.resolve_below_cursor(cursor) {
                                prop_assert!(released.insert(tx.hash), "tx released twice");
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
                    .values()
                    .filter(|entry| entry.resolution != Resolution::Settled)
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
        pool.mark_settled(10);
        assert_eq!(pool.blocking_height(5), Some(10));
        // …only the Deriver's cursor passing the height does.
        assert_eq!(pool.blocking_height(10), None);
    }

    #[test]
    fn failed_recovery_extracts_once_and_unblocks() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_failed(10, false);
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
        optimistic.mark_failed(10, false);
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
        pool.mark_failed(10, true);
        let batch = pool.take_failed_for_recovery(0).expect("failed entry");
        assert!(batch.slot_skipped);
        assert_eq!(batch.txs.len(), 2);
    }

    #[test]
    fn cursor_resolution_overrides_false_failure() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_failed(10, false);
        // Deriver confirmed the batch — false-negative verdict overridden.
        let released = pool.resolve_below_cursor(10);
        assert_eq!(released.len(), 1);
        assert!(pool.take_failed_for_recovery(0).is_none());
        assert_eq!(pool.blocking_height(10), None);
    }

    #[test]
    fn cursor_resolution_releases_observer_settled_once() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1), tx(2)]);
        pool.mark_settled(10);

        let released = pool.resolve_below_cursor(10);
        assert_eq!(released.len(), 2);
        assert!(pool.resolve_below_cursor(10).is_empty());
    }

    #[test]
    fn rolled_out_recovers_only_settled() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled(10);
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
        pool.mark_settled(10);
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

    #[test]
    fn resolved_gate_allows_the_next_sync_slot() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_settled(10);

        // The relay verdict alone is deliberately insufficient.
        assert_eq!(pool.blocking_height(9), Some(10));
        assert_eq!(pool.resolve_below_cursor(10).len(), 1);
        assert_eq!(pool.blocking_height(10), None);

        // A subsequent slot can take the one-in-flight reservation.
        pool.begin(11, pb_hash(0xb), hdr(), vec![tx(2)]);
        assert_eq!(pool.blocking_height(10), Some(11));
    }

    #[test]
    fn consecutive_failed_batches_leave_no_stale_gate_or_transactions() {
        let pool = OptimisticallyIncluded::new();

        for (height, tag) in [(10, 1), (11, 2)] {
            pool.begin(height, pb_hash(tag), hdr(), vec![tx(tag)]);
            pool.mark_failed(height, false);
            let failed = pool
                .take_failed_for_recovery(height - 1)
                .expect("failed batch must be recovered exactly once");
            assert_eq!(failed.sync_height, height);
            assert_eq!(failed.txs.len(), 1);
            assert!(pool.take_failed_for_recovery(height - 1).is_none());
            assert_eq!(pool.blocking_height(height - 1), None);
        }

        assert!(pool.by_sync_height.lock().unwrap().is_empty());
    }

    #[test]
    fn peer_consuming_the_same_held_transaction_reconciles_each_ledger_once() {
        // Both composers selected the same raw user transaction but signed
        // different postBatch envelopes. The peer's canonical batch consumes the
        // shared entry, so each local ledger releases its reservation exactly once.
        let shared = tx(1);
        let composer_a = OptimisticallyIncluded::new();
        let composer_b = OptimisticallyIncluded::new();
        composer_a.begin(10, pb_hash(0xa), hdr(), vec![shared.clone()]);
        composer_b.begin(10, pb_hash(0xb), hdr(), vec![shared]);

        composer_b.mark_settled(10);
        let winner_release = composer_b.resolve_below_cursor(10);
        let peer_release = composer_a.resolve_below_cursor(10);
        assert_eq!(winner_release.len(), 1);
        assert_eq!(peer_release.len(), 1);
        assert_eq!(winner_release[0].hash, peer_release[0].hash);
        assert!(composer_a.resolve_below_cursor(10).is_empty());
        assert!(composer_b.resolve_below_cursor(10).is_empty());
        assert!(composer_a.take_failed_for_recovery(0).is_none());
        assert!(composer_b.take_failed_for_recovery(0).is_none());
    }

    #[test]
    fn rival_post_batch_at_same_height_does_not_resolve_local_entry() {
        use eez_l1::{BatchRecord, L1CanonicalHead};

        let local_hash = pb_hash(0xa);
        let rival_hash = pb_hash(0xb);
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(10, local_hash, hdr(), vec![tx(1)]);

        // Model the only signal the composer currently consumes: the deriver
        // indexed a peer's batch and advanced the shared cursor to our height.
        let canonical = L1CanonicalHead::default();
        canonical.append(BatchRecord {
            l1_block: 100,
            l1_block_hash: B256::repeat_byte(0xc),
            tx_hash: rival_hash,
            last_l2_block: 10,
        });
        assert!(!canonical.contains_l1_tx(&local_hash));

        let released = ledger.resolve_below_cursor(canonical.cursor());
        assert!(
            released.is_empty(),
            "a peer's canonical batch must not release our held transaction"
        );
        let map = ledger.by_sync_height.lock().unwrap();
        let local = map.get(&10).expect("local entry remains owned");
        assert_eq!(local.resolution, Resolution::Pending);
        assert!(!local.cursor_confirmed);
    }

    #[test]
    fn rival_cursor_does_not_override_failure_when_observer_finishes_first() {
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        ledger.mark_failed(10, false);

        let released = ledger.resolve_below_cursor(10);
        assert!(
            released.is_empty(),
            "unrelated cursor advancement must not convert Failed to Settled"
        );
        let map = ledger.by_sync_height.lock().unwrap();
        assert_eq!(map.get(&10).unwrap().resolution, Resolution::Failed);
    }

    #[test]
    fn late_failure_remains_recoverable_after_rival_cursor_advances() {
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);

        let released = ledger.resolve_below_cursor(10);
        ledger.mark_failed(10, false);

        assert!(
            released.is_empty(),
            "peer confirmation must not release the local reservation"
        );
        let map = ledger.by_sync_height.lock().unwrap();
        assert_eq!(map.get(&10).unwrap().resolution, Resolution::Failed);
        assert!(!map.get(&10).unwrap().cursor_confirmed);
    }

    #[test]
    #[ignore = "known defect: take_finalized removes ownership before an RPC audit completes"]
    fn inconclusive_finality_audit_keeps_the_entry_for_retry() {
        let ledger = OptimisticallyIncluded::new();
        ledger.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        ledger.mark_settled(10);

        // The caller took the candidate, then its receipt RPC returned Err.
        // With no conclusive present/absent result, the next audit must still
        // be able to claim exactly the same entry.
        let first_attempt = ledger.take_finalized(10);
        assert_eq!(first_attempt.len(), 1);
        drop(first_attempt);

        let retry = ledger.take_finalized(10);
        assert_eq!(
            retry.len(),
            1,
            "an inconclusive audit must retain ownership"
        );
        assert_eq!(retry[0].0, 10);
        assert_eq!(retry[0].1, pb_hash(0xa));
        assert_eq!(retry[0].2.len(), 1);
    }
}
