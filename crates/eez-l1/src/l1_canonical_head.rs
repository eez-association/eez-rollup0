//! Shared L1-confirmed L2 head: cursor + per-tx batch index + finalized.
//! Deriver writes, Composer reads `cursor()` for next batch's
//! `from_block`. Indexed per-L1-tx (not per-L1-block) so competing
//! batches in the same L1 block don't shadow each other. See plan
//! §10 W3.16 + W3.18.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::B256;
use eez_driver::ConfirmedHeadSource;

/// L2 block identity derived from an L1 batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2BlockRef {
    pub number: u64,
    pub hash: B256,
}

/// One winning `BatchPosted` event in the per-tx index. Walked by the
/// L1-reorg and L1-finality handlers. Losers aren't recorded.
#[derive(Debug, Clone, Copy)]
pub struct BatchRecord {
    pub l1_block: u64,
    pub l1_block_hash: B256,
    pub tx_hash: B256,
    pub last_l2: L2BlockRef,
}

/// Shared canonical-head state. See module docs.
#[derive(Debug, Default)]
pub struct L1CanonicalHead {
    /// Highest L2 block any L1-landed batch confirmed.
    cursor: AtomicU64,
    /// L2 block number reth's `finalized` head was last advanced to via
    /// `BlockCommitterHandle::advance_safe_finalized`. Kept as an atomic
    /// for cheap telemetry; the hash-bearing mirror lives in
    /// `finalized_ref`.
    finalized_l2: AtomicU64,
    /// Hash-bearing finalized checkpoint. `None` means genesis/no
    /// finalized batch yet.
    finalized_ref: Mutex<Option<L2BlockRef>>,
    /// Per-tx batch index appended on each accepted `BatchPosted` event.
    /// Walked by the L1-reorg handler to drop entries above the new
    /// common ancestor, and by the L1-finality handler to find the
    /// highest L2 block confirmed by an L1-finalized block.
    batches: Mutex<Vec<BatchRecord>>,
}

impl L1CanonicalHead {
    /// Highest L2 block confirmed on L1 so far.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Acquire)
    }

    /// L2 block number currently mirrored as reth's `finalized` head.
    pub fn finalized_l2(&self) -> u64 {
        self.finalized_l2.load(Ordering::Acquire)
    }

    /// Hash-bearing finalized checkpoint, if one has been set.
    ///
    /// # Panics
    ///
    /// If the `finalized_ref` mutex is poisoned.
    pub fn finalized_ref(&self) -> Option<L2BlockRef> {
        *self.finalized_ref.lock().unwrap()
    }

    /// Hash-bearing cursor, if any L1 batch has been indexed.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn cursor_ref(&self) -> Option<L2BlockRef> {
        self.batches.lock().unwrap().last().map(|b| b.last_l2)
    }

    /// Highest indexed `last_l2_block`, or 0 if empty.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn last_indexed_l2(&self) -> u64 {
        self.batches
            .lock()
            .unwrap()
            .last()
            .map_or(0, |b| b.last_l2.number)
    }

    /// `true` iff `tx_hash` is already indexed. Used by the Deriver
    /// to dedup live `BatchPosted` events against the catch-up scan.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn contains_l1_tx(&self, tx_hash: &B256) -> bool {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .any(|b| &b.tx_hash == tx_hash)
    }

    /// Snapshot of indexed tx hashes. Used by `catch_up` to skip
    /// already-counted batches.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn known_tx_hashes(&self) -> HashSet<B256> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.tx_hash)
            .collect()
    }

    /// Append a `BatchRecord` and bump cursor. No-op if `tx_hash`
    /// is already indexed.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn append(&self, record: BatchRecord) {
        let mut batches = self.batches.lock().unwrap();
        if batches.iter().any(|b| b.tx_hash == record.tx_hash) {
            return;
        }
        batches.push(record);
        if record.last_l2.number > self.cursor.load(Ordering::Acquire) {
            self.cursor.store(record.last_l2.number, Ordering::Release);
        }
    }

    /// Append many records in L1 tx-position order; skip already-
    /// indexed `tx_hash`es. Used by `catch_up` for bulk replay.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn append_many(&self, records: impl IntoIterator<Item = BatchRecord>) {
        let mut batches = self.batches.lock().unwrap();
        let known: HashSet<B256> = batches.iter().map(|b| b.tx_hash).collect();
        let mut max_l2 = self.cursor.load(Ordering::Acquire);
        for r in records {
            if !known.contains(&r.tx_hash) {
                batches.push(r);
                if r.last_l2.number > max_l2 {
                    max_l2 = r.last_l2.number;
                }
            }
        }
        self.cursor.store(max_l2, Ordering::Release);
    }

    /// Drop indexed batches above `common_ancestor_number`; retreat
    /// `cursor` (and finalized if it would cross cursor).
    /// Returns `(new_cursor, new_finalized, dropped_count)`.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn retreat_on_l1_reorg(
        &self,
        common_ancestor_number: u64,
    ) -> (Option<L2BlockRef>, Option<L2BlockRef>, usize) {
        let mut batches = self.batches.lock().unwrap();
        let original_len = batches.len();
        batches.retain(|b| b.l1_block <= common_ancestor_number);
        let dropped = original_len - batches.len();
        let new_cursor = batches.last().map(|b| b.last_l2);
        self.cursor
            .store(new_cursor.map_or(0, |r| r.number), Ordering::Release);

        let mut finalized_ref = self.finalized_ref.lock().unwrap();
        if finalized_ref
            .is_some_and(|finalized| finalized.number > new_cursor.map_or(0, |r| r.number))
        {
            *finalized_ref = new_cursor;
        }
        self.finalized_l2
            .store((*finalized_ref).map_or(0, |r| r.number), Ordering::Release);
        (new_cursor, *finalized_ref, dropped)
    }

    /// Highest hash-bearing L2 checkpoint of any batch with
    /// `l1_block <= bound`. Used to map an L1-finalized block to its
    /// L2 head.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn highest_l2_ref_at_or_below_l1(&self, l1_block_bound: u64) -> Option<L2BlockRef> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|b| b.l1_block <= l1_block_bound)
            .map(|b| b.last_l2)
    }

    /// Update the hash-bearing finalized mirror. Caller is responsible
    /// for keeping it `<= cursor()`.
    ///
    /// # Panics
    ///
    /// If the `finalized_ref` mutex is poisoned.
    pub fn set_finalized_ref(&self, finalized: Option<L2BlockRef>) {
        *self.finalized_ref.lock().unwrap() = finalized;
        self.finalized_l2
            .store(finalized.map_or(0, |r| r.number), Ordering::Release);
    }
}

impl ConfirmedHeadSource for L1CanonicalHead {
    fn confirmed_head(&self) -> u64 {
        self.cursor()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(l1_block: u64, tx_byte: u8, last_l2_block: u64) -> BatchRecord {
        BatchRecord {
            l1_block,
            l1_block_hash: B256::with_last_byte(0x11),
            tx_hash: B256::with_last_byte(tx_byte),
            last_l2: L2BlockRef {
                number: last_l2_block,
                hash: B256::with_last_byte(tx_byte),
            },
        }
    }

    #[test]
    fn append_dedups_by_tx_hash_not_l1_block() {
        let head = L1CanonicalHead::default();
        head.append(record(100, 0xaa, 50));
        head.append(record(100, 0xbb, 60)); // same L1 block, different tx — must be kept
        head.append(record(100, 0xaa, 70)); // duplicate tx_hash — dropped
        assert_eq!(head.cursor(), 60);
        assert_eq!(head.last_indexed_l2(), 60);
        assert!(head.contains_l1_tx(&B256::with_last_byte(0xaa)));
        assert!(head.contains_l1_tx(&B256::with_last_byte(0xbb)));
    }

    #[test]
    fn append_many_dedups_and_advances_cursor() {
        let head = L1CanonicalHead::default();
        head.append(record(100, 0xaa, 50));
        head.append_many([
            record(100, 0xaa, 999), // dup tx_hash — ignored
            record(100, 0xbb, 60),  // same L1 block, new tx — kept
            record(101, 0xcc, 70),  // new L1 block — kept
        ]);
        assert_eq!(head.cursor(), 70);
        assert_eq!(head.known_tx_hashes().len(), 3);
    }

    #[test]
    fn retreat_drops_entries_above_common_ancestor() {
        let head = L1CanonicalHead::default();
        head.append_many([
            record(100, 0xaa, 50),
            record(100, 0xbb, 60), // same L1 block as 0xaa
            record(101, 0xcc, 70),
            record(102, 0xdd, 80),
        ]);
        head.set_finalized_ref(Some(L2BlockRef {
            number: 60,
            hash: B256::with_last_byte(0xbb),
        }));
        let (new_cursor, new_finalized, dropped) = head.retreat_on_l1_reorg(100);
        assert_eq!(new_cursor.map(|r| r.number), Some(60)); // both batches at L1=100 survive
        assert_eq!(new_finalized.map(|r| r.number), Some(60));
        assert_eq!(dropped, 2);
    }

    #[test]
    fn highest_l2_ref_at_or_below_l1_returns_max_within_bound() {
        let head = L1CanonicalHead::default();
        head.append_many([
            record(100, 0xaa, 50),
            record(100, 0xbb, 60),
            record(101, 0xcc, 70),
            record(102, 0xdd, 80),
        ]);
        let at_100 = head.highest_l2_ref_at_or_below_l1(100).unwrap();
        assert_eq!(at_100.number, 60);
        assert_eq!(at_100.hash, B256::with_last_byte(0xbb));
        let at_101 = head.highest_l2_ref_at_or_below_l1(101).unwrap();
        assert_eq!(at_101.number, 70);
        assert_eq!(at_101.hash, B256::with_last_byte(0xcc));
        assert!(head.highest_l2_ref_at_or_below_l1(99).is_none());
    }

    #[test]
    fn finalized_ref_tracks_hash_bearing_checkpoint() {
        let head = L1CanonicalHead::default();
        head.append_many([record(100, 0xaa, 50), record(101, 0xbb, 60)]);
        let checkpoint = L2BlockRef {
            number: 60,
            hash: B256::with_last_byte(0xbb),
        };
        head.set_finalized_ref(Some(checkpoint));

        assert_eq!(head.finalized_ref(), Some(checkpoint));
        assert_eq!(head.finalized_l2(), 60);
    }
}
