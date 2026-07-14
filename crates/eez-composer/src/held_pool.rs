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
//! - [`HeldPool::pop_all`] — empties the queued pool and reserves the
//!   drained nonces as in-flight. Called by the umbrella on Sync-slot
//!   trigger after the composer's batch is built and the bundle is
//!   queued for submission. Cross-chain reorg recovery (re-injecting
//!   pre-composed txs after L1 reorg or bundle drop) replays into the
//!   pool via [`HeldPool::push_front_batch`].
//! - [`HeldPool::drain_matching`] — removes txs whose hashes appear
//!   in `consumed`. Called when an external composer's batch lands
//!   that included some of our held txs (the case-(c) recovery in
//!   §5.4.9 — those txs are already on-chain, don't re-compose them).
//!
//! Concurrency: one `Mutex<PoolState>` keeps queue membership, hash
//! dedupe, and in-flight nonce reservations atomic. Held-pool operations
//! are infrequent (per sync slot, per L1 event) and never hot-path.

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
    /// Cached hash of the signed envelope. Indexed for
    /// [`HeldPool::drain_matching`] lookups — avoids recomputing on
    /// every external-batch landing.
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
    /// The tx's nonce, in the sender's chain for this `direction`
    /// (inbound: the originating chain's nonce; outbound: this L2's).
    pub nonce: u64,
    /// Cross-chain axis. Inbound and outbound txs of the same EOA keep
    /// INDEPENDENT nonce chains, so `held_count_for` /
    /// `drain_sender_above` are keyed on `(sender, direction)`.
    pub direction: Direction,
}

/// Outcome of inserting a held tx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldInsert {
    /// The tx was newly inserted into the queued pool.
    Inserted,
    /// The tx hash was already queued or in flight; no state changed.
    Duplicate,
}

/// A nonce-contiguity admission failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceAdmissionError {
    pub expected: u64,
    pub on_chain: u64,
    pub reserved: u64,
}

#[derive(Debug, Default)]
struct PoolState {
    txs: VecDeque<HeldTx>,
    by_hash: HashMap<TxHash, usize>,
    in_flight_hashes: HashSet<TxHash>,
    in_flight_counts: HashMap<(Address, Direction), usize>,
}

impl PoolState {
    fn rebuild_index(&mut self) {
        self.by_hash.clear();
        for (i, tx) in self.txs.iter().enumerate() {
            self.by_hash.insert(tx.hash, i);
        }
    }

    fn queued_count_for(&self, sender: Address, direction: Direction) -> usize {
        self.txs
            .iter()
            .filter(|t| t.sender == sender && t.direction == direction)
            .count()
    }

    fn reserved_count_for(&self, sender: Address, direction: Direction) -> usize {
        self.queued_count_for(sender, direction)
            + self
                .in_flight_counts
                .get(&(sender, direction))
                .copied()
                .unwrap_or(0)
    }

    fn contains_hash(&self, hash: TxHash) -> bool {
        self.by_hash.contains_key(&hash) || self.in_flight_hashes.contains(&hash)
    }

    fn push_back(&mut self, tx: HeldTx) -> HoldInsert {
        if self.contains_hash(tx.hash) {
            return HoldInsert::Duplicate;
        }
        self.by_hash.insert(tx.hash, self.txs.len());
        self.txs.push_back(tx);
        HoldInsert::Inserted
    }

    fn push_front_recovered(&mut self, tx: HeldTx) -> HoldInsert {
        self.release_in_flight(&tx);
        if self.by_hash.contains_key(&tx.hash) {
            return HoldInsert::Duplicate;
        }
        self.txs.push_front(tx);
        self.rebuild_index();
        HoldInsert::Inserted
    }

    fn mark_in_flight(&mut self, tx: &HeldTx) {
        if self.in_flight_hashes.insert(tx.hash) {
            *self
                .in_flight_counts
                .entry((tx.sender, tx.direction))
                .or_insert(0) += 1;
        }
    }

    fn release_in_flight(&mut self, tx: &HeldTx) {
        if !self.in_flight_hashes.remove(&tx.hash) {
            return;
        }
        let key = (tx.sender, tx.direction);
        if let Some(count) = self.in_flight_counts.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight_counts.remove(&key);
            }
        }
    }
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
    state: Mutex<PoolState>,
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
        self.state
            .lock()
            .expect("held_pool mutex poisoned")
            .push_back(tx);
    }

    /// Append `tx` only if it is contiguous with queued + in-flight txs for
    /// `(sender, direction)`. Duplicate hashes are accepted as idempotent.
    pub fn push_contiguous(
        &self,
        tx: HeldTx,
        on_chain: u64,
    ) -> Result<HoldInsert, NonceAdmissionError> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        if state.contains_hash(tx.hash) {
            return Ok(HoldInsert::Duplicate);
        }
        let reserved = state.reserved_count_for(tx.sender, tx.direction) as u64;
        let expected = on_chain.saturating_add(reserved);
        if tx.nonce != expected {
            return Err(NonceAdmissionError {
                expected,
                on_chain,
                reserved,
            });
        }
        Ok(state.push_back(tx))
    }

    /// Re-queue recovered txs at the FRONT of the pool, preserving
    /// their relative order. Recovery from a failed bundle must put
    /// the txs AHEAD of anything submitted since — appending to the
    /// tail re-orders user transactions across retries (setValue(2)
    /// executing before the retried setValue(1)). Duplicate hashes are
    /// skipped.
    pub fn push_front_batch(&self, recovered: Vec<HeldTx>) {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        for tx in recovered.into_iter().rev() {
            state.push_front_recovered(tx);
        }
    }

    /// Number of queued txs from `sender` on the `direction` nonce chain.
    #[must_use]
    pub fn held_count_for(&self, sender: Address, direction: Direction) -> usize {
        self.state
            .lock()
            .expect("held_pool mutex poisoned")
            .queued_count_for(sender, direction)
    }

    /// Number of queued plus in-flight txs from `sender` on the `direction`
    /// nonce chain.
    #[must_use]
    pub fn reserved_count_for(&self, sender: Address, direction: Direction) -> usize {
        self.state
            .lock()
            .expect("held_pool mutex poisoned")
            .reserved_count_for(sender, direction)
    }

    /// Release txs that were drained into an optimistic bundle and are now
    /// known not to need nonce reservation anymore.
    pub fn release_in_flight_batch(&self, txs: &[HeldTx]) {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        for tx in txs {
            state.release_in_flight(tx);
        }
    }

    /// Remove and return every held tx from `sender` on the `direction` nonce
    /// chain with a nonce strictly above `nonce`. Called on eviction: the higher
    /// nonces are now gapped (eviction guarantees the gap never fills), so leaving
    /// them queued only poisons future bundles. Keyed on direction so evicting an
    /// outbound tx never drains the sender's independent inbound chain.
    #[must_use]
    pub fn drain_sender_above(
        &self,
        sender: Address,
        direction: Direction,
        nonce: u64,
    ) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let mut drained = Vec::new();
        let mut kept = VecDeque::with_capacity(state.txs.len());
        for tx in state.txs.drain(..) {
            if tx.sender == sender && tx.direction == direction && tx.nonce > nonce {
                drained.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        state.txs = kept;
        state.rebuild_index();
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
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let consumed_set: HashSet<&TxHash> = consumed.iter().collect();
        let mut drained = Vec::new();
        let mut kept = VecDeque::with_capacity(state.txs.len());
        for tx in state.txs.drain(..) {
            if consumed_set.contains(&tx.hash) {
                drained.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        state.txs = kept;
        state.rebuild_index();
        drained
    }

    /// Drain every held tx. Used on Sync-slot trigger when the
    /// composer is about to produce system_txs from them. Empty after
    /// this call; recovery paths re-`push` if needed.
    pub fn pop_all(&self) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let drained: Vec<HeldTx> = state.txs.drain(..).collect();
        state.by_hash.clear();
        for tx in &drained {
            state.mark_in_flight(tx);
        }
        drained
    }

    /// Drain up to `max` held txs with per-sender fairness. Empirical fact about the
    /// rbuilder-chiado relay: bundles containing more than ~3 user_txs
    /// often have a subset silently dropped at inclusion time even
    /// when the bundle itself lands. Capping bundle size keeps the
    /// per-bundle inclusion atomic at the cost of more Sync slots to
    /// drain a large pool.
    pub fn pop_n(&self, max: usize) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let mut drained = Vec::new();
        while drained.len() < max && !state.txs.is_empty() {
            let mut seen = HashSet::new();
            let mut kept = VecDeque::with_capacity(state.txs.len());
            let start_len = state.txs.len();
            let mut made_progress = false;
            for _ in 0..start_len {
                let tx = state.txs.pop_front().expect("bounded by start_len");
                let key = (tx.sender, tx.direction);
                if drained.len() < max && seen.insert(key) {
                    state.mark_in_flight(&tx);
                    drained.push(tx);
                    made_progress = true;
                } else {
                    kept.push_back(tx);
                }
            }
            state.txs = kept;
            if !made_progress {
                break;
            }
        }
        state.rebuild_index();
        drained
    }

    /// Number of currently-held txs.
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("held_pool mutex poisoned")
            .txs
            .len()
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
        tx_dir(byte, Direction::Inbound)
    }

    fn tx_dir(byte: u8, direction: Direction) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![byte; 32]),
            hash: TxHash::from(B256::repeat_byte(byte)),
            attempts: 0,
            sender: Address::repeat_byte(byte),
            nonce: u64::from(byte),
            direction,
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
    fn contiguity_is_isolated_per_direction() {
        // Same EOA, two independent nonce chains. Counting / draining one
        // direction must never touch the other.
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(7);
        let mk = |nonce: u64, dir: Direction, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: dir,
        };
        pool.push(mk(0, Direction::Inbound, 1));
        pool.push(mk(1, Direction::Inbound, 2));
        pool.push(mk(0, Direction::Outbound, 3));
        pool.push(mk(1, Direction::Outbound, 4));

        assert_eq!(pool.held_count_for(sender, Direction::Inbound), 2);
        assert_eq!(pool.held_count_for(sender, Direction::Outbound), 2);

        // Evict outbound above nonce 0 → drops only the outbound nonce-1 tx.
        let drained = pool.drain_sender_above(sender, Direction::Outbound, 0);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].nonce, 1);
        assert_eq!(drained[0].direction, Direction::Outbound);
        assert_eq!(
            pool.held_count_for(sender, Direction::Inbound),
            2,
            "inbound untouched"
        );
        assert_eq!(pool.held_count_for(sender, Direction::Outbound), 1);
    }

    #[test]
    fn drain_matching_empty_is_noop() {
        let pool = HeldPool::new();
        pool.push(tx(1));
        let drained = pool.drain_matching(&[]);
        assert!(drained.is_empty());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn contiguous_push_is_atomic_against_duplicate_nonce() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(9);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: Direction::Inbound,
        };

        assert_eq!(
            pool.push_contiguous(mk(0, 1), 0).unwrap(),
            HoldInsert::Inserted
        );
        let err = pool.push_contiguous(mk(0, 2), 0).unwrap_err();
        assert_eq!(err.expected, 1);
        assert_eq!(err.on_chain, 0);
        assert_eq!(err.reserved, 1);
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn contiguous_push_counts_in_flight_reservations() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xa);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: Direction::Inbound,
        };

        pool.push_contiguous(mk(0, 1), 0).unwrap();
        let drained = pool.pop_n(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(pool.held_count_for(sender, Direction::Inbound), 0);
        assert_eq!(pool.reserved_count_for(sender, Direction::Inbound), 1);

        pool.push_contiguous(mk(1, 2), 0).unwrap();
        assert_eq!(pool.reserved_count_for(sender, Direction::Inbound), 2);
        pool.release_in_flight_batch(&drained);
        assert_eq!(pool.reserved_count_for(sender, Direction::Inbound), 1);
    }

    #[test]
    fn pop_n_is_fair_across_sender_direction_chains() {
        let pool = HeldPool::new();
        let sender_a = Address::repeat_byte(0xa);
        let sender_b = Address::repeat_byte(0xb);
        let mk = |sender: Address, nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: Direction::Inbound,
        };

        pool.push(mk(sender_a, 0, 1));
        pool.push(mk(sender_a, 1, 2));
        pool.push(mk(sender_a, 2, 3));
        pool.push(mk(sender_b, 0, 4));

        let drained = pool.pop_n(3);
        let hashes: Vec<_> = drained.iter().map(|tx| tx.hash).collect();
        assert_eq!(
            hashes,
            vec![
                TxHash::from(B256::repeat_byte(1)),
                TxHash::from(B256::repeat_byte(4)),
                TxHash::from(B256::repeat_byte(2)),
            ]
        );
    }
}
