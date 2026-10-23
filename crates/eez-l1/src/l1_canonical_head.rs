//! Shared L1-confirmed L2 head: cursor + per-post batch index + finalized.
//! Deriver writes, Composer reads `cursor()` for next batch's
//! `from_block`. Indexed per post ([`PostId`]) in L1 order, so posts
//! sharing an L1 block or a tx don't shadow each other.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::B256;
use tracing::{Level, event};

use crate::scan::PostId;

/// One winning `BatchPosted` event in the per-post index. Walked by the
/// L1-reorg and L1-finality handlers. Losers aren't recorded.
#[derive(Debug, Clone, Copy)]
pub struct BatchRecord {
    /// The post's `BatchPosted` log. Its block hash lets the Deriver's resync
    /// verify the record is still canonical — survives a missed `L1Event::Reorg`.
    pub post: PostId,
    pub tx_hash: B256,
    pub last_l2_block: u64,
}

/// Shared canonical-head state. See module docs.
#[derive(Debug, Default)]
pub struct L1CanonicalHead {
    /// Highest L2 block any L1-landed batch confirmed.
    cursor: AtomicU64,
    /// L2 block number reth's `finalized` head was last advanced to via
    /// `BlockCommitterHandle::advance_safe_finalized`. Used to bound
    /// subsequent advance/retreat calls so finalized never crosses safe.
    finalized_l2: AtomicU64,
    /// Per-post batch index appended in L1 order on each accepted `BatchPosted` event.
    /// Walked by the L1-reorg handler to drop entries above the new
    /// common ancestor, and by the L1-finality handler to find the
    /// highest L2 block confirmed by an L1-finalized block.
    batches: Mutex<Vec<BatchRecord>>,
    /// The seed's post, if the index was seeded. Finality can't reorg, so a
    /// retreat below it is refused.
    floor: Mutex<Option<PostId>>,
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

    /// Start an empty index at `record`: cursor and finalized become its L2
    /// block, and its post becomes the floor.
    ///
    /// # Panics
    ///
    /// If the index is not empty or a mutex is poisoned.
    pub fn seed(&self, record: BatchRecord) {
        let mut batches = self.batches.lock().unwrap();
        assert!(batches.is_empty(), "seed requires an empty index");
        batches.push(record);
        self.cursor.store(record.last_l2_block, Ordering::Release);
        self.finalized_l2
            .store(record.last_l2_block, Ordering::Release);
        *self.floor.lock().unwrap() = Some(record.post);
    }

    /// The seed's post, or `None` if the index was never seeded.
    #[cfg(test)]
    fn floor(&self) -> Option<PostId> {
        *self.floor.lock().unwrap()
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
            .map_or(0, |b| b.last_l2_block)
    }

    /// The most recently indexed batch, or `None` if empty. Anchor for the
    /// Deriver's resync: its `post.id.l1_block_hash` is canonicality-checked and
    /// its `post.id.l1_block` is the scan's lower bound.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn last_indexed(&self) -> Option<BatchRecord> {
        self.batches.lock().unwrap().last().copied()
    }

    /// `true` iff `id` is at or before the indexed tail. Posts arrive in L1
    /// order, so such a post was already seen: indexed, or applied nothing.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn is_processed(&self, id: PostId) -> bool {
        self.batches
            .lock()
            .unwrap()
            .last()
            .is_some_and(|tail| at_or_before(&id, &tail.post))
    }

    /// Append a `BatchRecord` and bump cursor. A record at or before the tail
    /// is ignored and logged at ERROR: a caller skipped [`Self::is_processed`].
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn append(&self, record: BatchRecord) {
        let mut batches = self.batches.lock().unwrap();
        if !push_in_order(&mut batches, record) {
            return;
        }
        if record.last_l2_block > self.cursor.load(Ordering::Acquire) {
            self.cursor.store(record.last_l2_block, Ordering::Release);
        }
    }

    /// Append many records in L1 order, each checked as [`Self::append`]
    /// does. Used by `catch_up` for bulk replay.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn append_many(&self, records: impl IntoIterator<Item = BatchRecord>) {
        let mut batches = self.batches.lock().unwrap();
        let mut max_l2 = self.cursor.load(Ordering::Acquire);
        for record in records {
            if push_in_order(&mut batches, record) {
                max_l2 = max_l2.max(record.last_l2_block);
            }
        }
        self.cursor.store(max_l2, Ordering::Release);
    }

    /// Drop indexed batches above `common_ancestor_number`; retreat
    /// `cursor` (and `finalized_l2` if it would cross cursor).
    /// Returns `(new_cursor, new_finalized, dropped_count)`, or `Err(floor)`
    /// with the index untouched when the ancestor is below the floor's block.
    ///
    /// # Errors
    ///
    /// The floor [`PostId`] when `common_ancestor_number` is below it.
    ///
    /// # Panics
    ///
    /// If a mutex is poisoned.
    pub fn retreat_on_l1_reorg(
        &self,
        common_ancestor_number: u64,
    ) -> Result<(u64, u64, usize), PostId> {
        let mut batches = self.batches.lock().unwrap();
        if let Some(floor) = *self.floor.lock().unwrap()
            && common_ancestor_number < floor.l1_block
        {
            return Err(floor);
        }
        let original_len = batches.len();
        batches.retain(|b| b.post.l1_block <= common_ancestor_number);
        let dropped = original_len - batches.len();
        let new_cursor = batches.last().map_or(0, |b| b.last_l2_block);
        self.cursor.store(new_cursor, Ordering::Release);
        let old_finalized = self.finalized_l2.load(Ordering::Acquire);
        let new_finalized = old_finalized.min(new_cursor);
        if new_finalized != old_finalized {
            self.finalized_l2.store(new_finalized, Ordering::Release);
        }
        Ok((new_cursor, new_finalized, dropped))
    }

    /// Highest `last_l2_block` of any batch with `l1_block ≤ bound`.
    /// Used to map an L1-finalized block to its L2 head.
    ///
    /// # Panics
    ///
    /// If the `batches` mutex is poisoned.
    pub fn highest_l2_at_or_below_l1(&self, l1_block_bound: u64) -> Option<u64> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|b| b.post.l1_block <= l1_block_bound)
            .map(|b| b.last_l2_block)
    }

    /// Update the `finalized_l2` mirror. Caller is responsible for
    /// keeping it `<= cursor()`.
    pub fn set_finalized_l2(&self, l2_block: u64) {
        self.finalized_l2.store(l2_block, Ordering::Release);
    }
}

/// At or before `tail` on its chain: the tail's height on another fork is a
/// different block. Reorg handling drops records before a new fork's posts arrive.
fn at_or_before(id: &PostId, tail: &PostId) -> bool {
    id.position() <= tail.position()
        && (id.l1_block < tail.l1_block || id.l1_block_hash == tail.l1_block_hash)
}

/// Push `record` unless it is at or before the tail. Returns whether it was pushed.
fn push_in_order(batches: &mut Vec<BatchRecord>, record: BatchRecord) -> bool {
    if let Some(tail) = batches.last()
        && at_or_before(&record.post, &tail.post)
    {
        event!(
            name: "eez.l1_canonical_head.append.out_of_order",
            Level::ERROR,
            post = ?record.post,
            tail = ?tail.post,
            "post at or before the indexed tail; ignoring it",
        );
        return false;
    }
    batches.push(record);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(l1_block: u64, log_index: u64) -> PostId {
        PostId {
            l1_block,
            l1_block_hash: B256::with_last_byte(u8::try_from(l1_block).expect("test block fits")),
            log_index,
        }
    }

    /// Every record shares one tx hash: identity is the position, not the tx.
    fn record(l1_block: u64, log_index: u64, last_l2_block: u64) -> BatchRecord {
        BatchRecord {
            post: post(l1_block, log_index),
            tx_hash: B256::ZERO,
            last_l2_block,
        }
    }

    /// Another fork at the tail's height is a different block: nothing in it was seen.
    #[test]
    fn is_processed_compares_positions_against_the_tail() {
        let head = L1CanonicalHead::default();
        assert!(!head.is_processed(post(100, 0)), "empty index");
        head.append(record(100, 5, 50));
        let other_fork = PostId {
            l1_block_hash: B256::repeat_byte(0xF0),
            ..post(100, 2)
        };
        for id in [post(99, 40), post(100, 2), post(100, 5)] {
            assert!(head.is_processed(id), "{id:?}");
        }
        for id in [post(100, 6), post(101, 0), other_fork] {
            assert!(!head.is_processed(id), "{id:?}");
        }
    }

    #[test]
    fn append_keeps_two_posts_of_one_tx_and_ignores_out_of_order() {
        let head = L1CanonicalHead::default();
        head.append(record(100, 0xaa, 50));
        head.append(record(100, 0xbb, 60)); // same tx, later log — must be kept
        head.append(record(100, 0xaa, 70)); // the tail again — ignored
        head.append(record(99, 0xcc, 80)); // earlier block — ignored
        assert_eq!(head.cursor(), 60);
        assert_eq!(head.last_indexed_l2(), 60);
        assert!(head.is_processed(post(100, 0xaa)));
        assert!(head.is_processed(post(100, 0xbb)));
    }

    #[test]
    fn append_many_dedups_and_advances_cursor() {
        let head = L1CanonicalHead::default();
        head.append(record(100, 0xaa, 50));
        head.append_many([
            record(100, 0xaa, 999), // the tail again — ignored
            record(100, 0xbb, 60),  // same L1 block, later log — kept
            record(101, 0xcc, 70),  // new L1 block — kept
        ]);
        assert_eq!(head.cursor(), 70);
        // Retreating past 101 leaves the same-block later-log record as the tail.
        head.retreat_on_l1_reorg(100).unwrap();
        assert_eq!(head.last_indexed().map(|r| r.post), Some(post(100, 0xbb)));
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
        head.set_finalized_l2(60);
        let (new_cursor, new_finalized, dropped) = head.retreat_on_l1_reorg(100).unwrap();
        assert_eq!(new_cursor, 60); // both batches at L1=100 survive
        assert_eq!(new_finalized, 60);
        assert_eq!(dropped, 2);
        assert!(head.is_processed(post(100, 0xbb)));
        assert!(!head.is_processed(post(101, 0xcc)), "reopened");
    }

    #[test]
    fn highest_l2_at_or_below_l1_returns_max_within_bound() {
        let head = L1CanonicalHead::default();
        head.append_many([
            record(100, 0xaa, 50),
            record(100, 0xbb, 60),
            record(101, 0xcc, 70),
            record(102, 0xdd, 80),
        ]);
        assert_eq!(head.highest_l2_at_or_below_l1(100), Some(60));
        assert_eq!(head.highest_l2_at_or_below_l1(101), Some(70));
        assert_eq!(head.highest_l2_at_or_below_l1(99), None);
    }

    #[test]
    fn seed_marks_its_block_processed() {
        let head = L1CanonicalHead::default();
        head.seed(record(100, u64::MAX, 40));
        assert!(head.is_processed(post(100, 7)), "same block, later log");
        assert!(!head.is_processed(post(101, 0)));
        assert_eq!(head.cursor(), 40);
        assert_eq!(head.finalized_l2(), 40);
        assert_eq!(head.last_indexed_l2(), 40);
        assert_eq!(head.highest_l2_at_or_below_l1(100), Some(40));
        assert_eq!(head.floor(), Some(post(100, u64::MAX)));
    }

    #[test]
    fn retreat_below_floor_is_refused() {
        let head = L1CanonicalHead::default();
        head.seed(record(100, u64::MAX, 40));
        head.append(record(105, 1, 50));
        assert_eq!(head.retreat_on_l1_reorg(103), Ok((40, 40, 1)));
        assert_eq!(head.retreat_on_l1_reorg(99), Err(post(100, u64::MAX)));
        assert_eq!(head.cursor(), 40);
        assert_eq!(
            head.last_indexed().map(|r| r.post),
            Some(post(100, u64::MAX))
        );
    }
}
