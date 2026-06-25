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
//!   ([`OptimisticallyIncluded::mark_failed`]); the destructive recovery
//!   (reorg to the Sync block's parent, re-push unburned txs) runs at
//!   the next Sync slot ([`OptimisticallyIncluded::take_failed_for_recovery`]),
//!   serialized against the Sequencer and Deriver to close the
//!   observer-vs-commit TOCTOU.
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
/// time (a tx whose `simulate_and_resolve` deterministically fails is
/// evicted before it can ever enter a bundle); a drop that reaches
/// recovery is treated as bad luck and re-queued, with
/// [`MAX_BUNDLE_ATTEMPTS`](crate::composer::MAX_BUNDLE_ATTEMPTS) as a
/// backstop for poison the compose-time sim view missed.
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
            },
        );
    }

    /// Update the postBatch tx hash of an existing entry. The deferred
    /// post (real proof system) registers the gate entry SYNCHRONOUSLY at
    /// slot time — before the proof arrives, so before the L1 tx can be
    /// signed — with a placeholder hash; once the attestation lands and
    /// the tx is signed, the dispatch task fills the real hash here so the
    /// finality audit (`take_finalized`) can still locate the receipt.
    /// No-op if the entry is gone (e.g. already recovered).
    pub fn set_post_batch_hash(&self, sync_height: u64, post_batch_hash: TxHash) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height) {
            entry.post_batch_hash = post_batch_hash;
        }
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
    /// a batch the Deriver confirmed.
    pub fn resolve_below_cursor(&self, cursor: u64) {
        let mut map = self.by_sync_height.lock().unwrap();
        for (_, entry) in map.range_mut(..=cursor) {
            if entry.resolution != Resolution::Settled {
                entry.resolution = Resolution::Settled;
            }
        }
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
    /// Settled (cursor confirmation wins) or gone.
    pub fn mark_failed(&self, sync_height: u64) {
        let mut map = self.by_sync_height.lock().unwrap();
        if let Some(entry) = map.get_mut(&sync_height) {
            if entry.resolution == Resolution::Pending {
                entry.resolution = Resolution::Failed;
            }
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

    fn tx(tag: u8) -> HeldTx {
        let raw = Bytes::from(vec![tag; 4]);
        let hash = keccak256(raw.as_ref());
        HeldTx {
            raw_tx: raw,
            hash,
            attempts: 0,
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
        pool.mark_failed(10);
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
    fn cursor_resolution_overrides_false_failure() {
        let pool = OptimisticallyIncluded::new();
        pool.begin(10, pb_hash(0xa), hdr(), vec![tx(1)]);
        pool.mark_failed(10);
        // Deriver confirmed the batch — false-negative verdict overridden.
        pool.resolve_below_cursor(10);
        assert!(pool.take_failed_for_recovery(0).is_none());
        assert_eq!(pool.blocking_height(10), None);
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
    fn deferred_gate_closes_at_slot_time_and_hash_is_filled_on_sign() {
        let pool = OptimisticallyIncluded::new();
        // Deferred post, slot time: register the gate with a PLACEHOLDER hash
        // (the proof hasn't arrived, so the L1 tx isn't signed yet).
        pool.begin(10, TxHash::ZERO, hdr(), vec![tx(1)]);
        // The one-in-flight gate is closed IMMEDIATELY — the next Sync slot
        // must skip emission even though the post hasn't been signed/sent.
        assert_eq!(pool.blocking_height(5), Some(10));
        // Proof arrives + tx signed: the dispatch task fills the real hash.
        pool.set_post_batch_hash(10, pb_hash(0xa));
        // Observer settles; the finality audit gets the REAL hash, not ZERO.
        pool.mark_settled(10);
        let finalized = pool.take_finalized(10);
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].1, pb_hash(0xa));
        // set_post_batch_hash on an absent entry is a no-op (no resurrection).
        pool.set_post_batch_hash(10, pb_hash(0xb));
        assert!(pool.take_finalized(10).is_empty());
    }

    #[test]
    fn deferred_timeout_marks_failed_for_recovery() {
        let pool = OptimisticallyIncluded::new();
        // Slot time: gate registered with survivors + a placeholder hash.
        pool.begin(10, TxHash::ZERO, hdr(), vec![tx(1), tx(2)]);
        // Proof never arrives within budget → the deferred task marks the
        // pre-registered entry Failed (it does NOT call begin again).
        pool.mark_failed(10);
        // The gate stays closed until the next slot recovers it…
        assert_eq!(pool.blocking_height(0), Some(10));
        let batch = pool.take_failed_for_recovery(0).expect("failed entry");
        assert_eq!(batch.sync_height, 10);
        // …re-queueing the survivors held in the ledger (not dropped)…
        assert_eq!(batch.txs.len(), 2);
        // …and the placeholder hash survives (a never-sent tx has no receipt).
        assert_eq!(batch.post_batch_hash, TxHash::ZERO);
        assert_eq!(pool.blocking_height(0), None);
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
}
