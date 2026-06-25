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

use alloy_primitives::{Address, Bytes, TxHash};

use crate::ingress::Direction;

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
    /// Failed-bundle attempts so far. Bundles are strict
    /// all-or-nothing, so a deterministically-reverting tx fails every
    /// bundle it joins — and from outside, a "tx reverts in builder
    /// simulation" drop is indistinguishable from a "no builder slot"
    /// drop. Recovery increments this on each re-queue and EVICTS the
    /// tx (loud WARN, user resubmits) at
    /// [`MAX_BUNDLE_ATTEMPTS`](crate::composer::MAX_BUNDLE_ATTEMPTS),
    /// so one poison tx can't fail every postBatch forever.
    pub attempts: u32,
    /// Recovered L1 sender. Together with `nonce`, lets the pool and
    /// the eviction path keep each sender's nonce chain CONTIGUOUS:
    /// ingress rejects out-of-sequence submissions, and evicting a tx
    /// cascades to the sender's higher nonces (they can never land
    /// once the gap exists — bundling them only poisons bundles).
    pub sender: Address,
    /// The tx's L1 nonce.
    pub nonce: u64,
    /// Cross-chain direction (which ingress signal matched). Today
    /// always [`Direction::Inbound`] — the outbound (L2→L1) path is not
    /// yet wired. A later step keys the per-sender nonce contiguity by
    /// `(sender, direction)` so the L1-context (inbound) and L2-context
    /// (outbound) nonce chains for the same EOA stay independent.
    pub direction: Direction,
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

    /// Re-queue recovered txs at the FRONT of the pool, preserving
    /// their relative order. Recovery from a failed bundle must put
    /// the txs AHEAD of anything submitted since — appending to the
    /// tail re-orders user transactions across retries (setValue(2)
    /// executing before the retried setValue(1)). Duplicate hashes are
    /// skipped.
    pub fn push_front_batch(&self, recovered: Vec<HeldTx>) {
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        for tx in recovered.into_iter().rev() {
            if by_hash.contains_key(&tx.hash) {
                continue;
            }
            by_hash.insert(tx.hash, 0);
            txs.push_front(tx);
        }
        // Rebuild the index — every position shifted.
        by_hash.clear();
        for (i, tx) in txs.iter().enumerate() {
            by_hash.insert(tx.hash, i);
        }
    }

    /// Number of held txs from `sender` in `direction`. With the
    /// contiguity invariant (ingress validation + cascade eviction), the
    /// sender's next valid nonce IN THAT DIRECTION = on-chain nonce +
    /// this count. Keyed by `(sender, direction)` so the inbound
    /// (L1-context) and outbound (L2-context) nonce chains of one EOA
    /// stay independent.
    #[must_use]
    pub fn held_count_for(&self, sender: Address, direction: Direction) -> usize {
        self.txs
            .lock()
            .expect("held_pool txs poisoned")
            .iter()
            .filter(|t| t.sender == sender && t.direction == direction)
            .count()
    }

    /// Remove and return every held tx from `sender` in `direction`
    /// with a nonce strictly above `nonce`. Called when a tx of that
    /// sender is evicted: the higher nonces are gapped — invalid until
    /// the gap fills, which eviction guarantees never happens — so
    /// leaving them queued only poisons future bundles. Keyed by
    /// `direction`: an evicted outbound (L2-nonce) tx must NOT cascade
    /// onto the same EOA's inbound (L1-nonce) chain, an independent
    /// sequence.
    #[must_use]
    pub fn drain_sender_above(
        &self,
        sender: Address,
        direction: Direction,
        nonce: u64,
    ) -> Vec<HeldTx> {
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        let mut drained = Vec::new();
        let mut kept = VecDeque::with_capacity(txs.len());
        for tx in txs.drain(..) {
            if tx.sender == sender && tx.direction == direction && tx.nonce > nonce {
                drained.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        *txs = kept;
        by_hash.clear();
        for (i, tx) in txs.iter().enumerate() {
            by_hash.insert(tx.hash, i);
        }
        drained
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

    /// Drain up to `max` held txs (FIFO). Empirical fact about the
    /// rbuilder-chiado relay: bundles containing more than ~3 user_txs
    /// often have a subset silently dropped at inclusion time even
    /// when the bundle itself lands. Capping bundle size keeps the
    /// per-bundle inclusion atomic at the cost of more Sync slots to
    /// drain a large pool.
    pub fn pop_n(&self, max: usize) -> Vec<HeldTx> {
        let mut txs = self.txs.lock().expect("held_pool txs poisoned");
        let mut by_hash = self.by_hash.lock().expect("held_pool by_hash poisoned");
        let n = max.min(txs.len());
        let drained: Vec<HeldTx> = txs.drain(..n).collect();
        // Rebuild index for the txs that remain.
        by_hash.clear();
        for (i, tx) in txs.iter().enumerate() {
            by_hash.insert(tx.hash, i);
        }
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
            attempts: 0,
            sender: Address::repeat_byte(byte),
            nonce: u64::from(byte),
            direction: Direction::Inbound,
        }
    }

    /// R8: the per-sender nonce contiguity is keyed by `(sender,
    /// direction)`. Inbound (L1-context) and outbound (L2-context) nonce
    /// chains of the SAME EOA are independent — counting and cascade
    /// eviction in one direction must not touch the other.
    #[test]
    fn contiguity_is_isolated_per_direction() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xEE);
        let mk = |tag: u8, nonce: u64, direction: Direction| HeldTx {
            raw_tx: Bytes::from(vec![tag; 4]),
            hash: TxHash::from(B256::repeat_byte(tag)),
            attempts: 0,
            sender,
            nonce,
            direction,
        };
        // Same EOA: 2 inbound (nonce 5, 6) + 2 outbound (nonce 5, 6).
        pool.push(mk(1, 5, Direction::Inbound));
        pool.push(mk(2, 6, Direction::Inbound));
        pool.push(mk(3, 5, Direction::Outbound));
        pool.push(mk(4, 6, Direction::Outbound));

        // Counting is per-direction.
        assert_eq!(pool.held_count_for(sender, Direction::Inbound), 2);
        assert_eq!(pool.held_count_for(sender, Direction::Outbound), 2);

        // Evicting outbound above nonce 5 drains ONLY the outbound
        // nonce-6 tx; the inbound chain is untouched.
        let drained = pool.drain_sender_above(sender, Direction::Outbound, 5);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].nonce, 6);
        assert_eq!(drained[0].direction, Direction::Outbound);
        assert_eq!(
            pool.held_count_for(sender, Direction::Inbound),
            2,
            "inbound chain must be untouched by an outbound eviction",
        );
        assert_eq!(pool.held_count_for(sender, Direction::Outbound), 1);
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
