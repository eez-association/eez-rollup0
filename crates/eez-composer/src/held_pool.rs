//! Per-rollup pool of cross-chain transactions awaiting Sync-slot
//! composition.
//!
//! Cross-chain txs (detected at `TxIngress` time per §5.4.5) sit here
//! between submission and the next Sync slot, where they're drained,
//! handed to the umbrella's
//! [`eez_evm_inspector::EvmComposer`] for `simulate_and_resolve`, and the
//! resulting `system_txs` get bundled into the Sync block.
//!
//! Held-pool drain semantics:
//!
//! - [`HeldPool::pop_all`] — empties the pool. Called by the umbrella
//!   on Sync-slot trigger after the composer's batch is built and the
//!   bundle is queued for submission. Cross-chain reorg recovery
//!   (re-injecting pre-composed txs after L1 reorg or bundle drop)
//!   replays into the pool via [`HeldPool::push`].
//! - [`HeldPool::drain_matching`] — removes txs whose hashes appear
//!   in `consumed`. Called when an external composer's batch lands
//!   that included some of our held txs (the case-(c) recovery in
//!   §5.4.9 — those txs are already on-chain, don't re-compose them).
//!
//! Concurrency: `Mutex<VecDeque>` keeps the implementation simple.
//! Held-pool operations are infrequent (per sync slot, per L1 event)
//! and never hot-path; a finer-grained lock split is premature.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use alloy_primitives::{Bytes, TxHash};

/// A cross-chain transaction held between submission and Sync-slot
/// composition.
#[derive(Debug, Clone)]
pub struct HeldTx {
    /// RLP-encoded source transaction (signed envelope). Handed to
    /// `EvmComposer::simulate_and_resolve` verbatim.
    pub raw_tx: Bytes,
    /// Cached hash of the signed envelope. Indexed by
    /// [`HeldPool::by_hash`] for [`HeldPool::drain_matching`] lookups
    /// — avoids recomputing on every external-batch landing.
    pub hash: TxHash,
}

/// Per-rollup pool of held cross-chain transactions.
///
/// Stored as `Option<HeldPool>` on
/// [`RollupState`](crate::RollupState): `None` for rollups that don't
/// participate in cross-chain composition (entry-only deployments or
/// follower-only deployments without cross-chain content from this
/// chain).
#[derive(Debug, Default)]
pub struct HeldPool {
    /// Insertion-ordered queue. The composer drains in FIFO order so
    /// sync-slot inclusion order matches submission order.
    txs: Mutex<VecDeque<HeldTx>>,
    /// `tx_hash → index in `txs`` lookup for `drain_matching`. Kept
    /// in sync with `txs` mutations.
    by_hash: Mutex<HashMap<TxHash, usize>>,
}

impl HeldPool {
    /// Empty pool — no held txs.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `tx` to the pool. Idempotent on duplicate hash (later
    /// `push` of an already-held hash is a no-op).
    pub fn push(&self, tx: HeldTx) {
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        if by_hash.contains_key(&tx.hash) {
            return;
        }
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        by_hash.insert(tx.hash, txs.len());
        txs.push_back(tx);
    }

    /// Drain every tx whose hash appears in `consumed`. Returns the
    /// drained set in original-insertion order. Used by case-(c)
    /// recovery in §5.4.9: when an external composer's batch lands
    /// that included some of our held txs, those txs are already
    /// on-chain and must not be re-composed.
    pub fn drain_matching(&self, consumed: &[TxHash]) -> Vec<HeldTx> {
        if consumed.is_empty() {
            return Vec::new();
        }
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        let consumed_set: HashSet<&TxHash> = consumed.iter().collect();
        let mut drained = Vec::new();
        let mut kept = VecDeque::with_capacity(txs.len());
        for tx in txs.drain(..) {
            if consumed_set.contains(&tx.hash) {
                drained.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        *txs = kept;
        // Rebuild the index — keys shifted by the partition.
        by_hash.clear();
        for (i, tx) in txs.iter().enumerate() {
            by_hash.insert(tx.hash, i);
        }
        drained
    }

    /// Drain every held tx. Used on Sync-slot trigger when the
    /// composer is about to produce system_txs from them. Empty after
    /// this call; recovery paths re-`push` if needed.
    pub fn pop_all(&self) -> Vec<HeldTx> {
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        let drained: Vec<HeldTx> = txs.drain(..).collect();
        by_hash.clear();
        drained
    }

    /// Number of currently-held txs.
    pub fn len(&self) -> usize {
        self.txs.lock().expect("held_pool txs poisoned").len()
    }

    /// True iff the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn tx(byte: u8) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![byte; 32]),
            hash: TxHash::from(B256::repeat_byte(byte)),
        }
    }

    #[test]
    fn push_then_pop_returns_fifo() {
        let pool = HeldPool::new();
        pool.push(tx(1));
        pool.push(tx(2));
        pool.push(tx(3));
        let drained = pool.pop_all();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].hash, TxHash::from(B256::repeat_byte(1)));
        assert_eq!(drained[2].hash, TxHash::from(B256::repeat_byte(3)));
        assert!(pool.is_empty());
    }

    #[test]
    fn push_dedupes_by_hash() {
        let pool = HeldPool::new();
        pool.push(tx(1));
        pool.push(tx(1));
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn drain_matching_removes_only_listed() {
        let pool = HeldPool::new();
        pool.push(tx(1));
        pool.push(tx(2));
        pool.push(tx(3));
        let drained = pool.drain_matching(&[TxHash::from(B256::repeat_byte(2))]);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].hash, TxHash::from(B256::repeat_byte(2)));
        assert_eq!(pool.len(), 2);
        let rest = pool.pop_all();
        assert_eq!(rest[0].hash, TxHash::from(B256::repeat_byte(1)));
        assert_eq!(rest[1].hash, TxHash::from(B256::repeat_byte(3)));
    }

    #[test]
    fn drain_matching_empty_is_noop() {
        let pool = HeldPool::new();
        pool.push(tx(1));
        let drained = pool.drain_matching(&[]);
        assert!(drained.is_empty());
        assert_eq!(pool.len(), 1);
    }
}
