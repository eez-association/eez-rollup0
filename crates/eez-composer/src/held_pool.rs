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
}

#[derive(Debug, Default)]
struct PoolState {
    txs: VecDeque<HeldTx>,
    by_hash: HashMap<TxHash, usize>,
    in_flight: HashMap<TxHash, (Address, Direction, u64)>,
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

    fn next_expected_nonce(
        &self,
        sender: Address,
        direction: Direction,
        on_chain: u64,
    ) -> Option<u64> {
        let reserved: HashSet<u64> = self
            .txs
            .iter()
            .filter(|tx| tx.sender == sender && tx.direction == direction)
            .map(|tx| tx.nonce)
            .chain(
                self.in_flight
                    .values()
                    .filter(|(s, d, _)| *s == sender && *d == direction)
                    .map(|(_, _, nonce)| *nonce),
            )
            .collect();
        let mut expected = on_chain;
        while reserved.contains(&expected) {
            expected = expected.checked_add(1)?;
        }
        Some(expected)
    }

    fn contains_hash(&self, hash: TxHash) -> bool {
        self.by_hash.contains_key(&hash) || self.in_flight.contains_key(&hash)
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
        self.in_flight
            .entry(tx.hash)
            .or_insert((tx.sender, tx.direction, tx.nonce));
    }

    fn release_in_flight(&mut self, tx: &HeldTx) {
        self.in_flight.remove(&tx.hash);
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

    /// Append `tx` to the pool without source-chain nonce validation.
    /// Idempotent on duplicate hash. Production ingress supplies a provider
    /// and uses [`Self::push_contiguous`]; this path supports dev/no-RPC use.
    pub fn push(&self, tx: HeldTx) {
        self.state
            .lock()
            .expect("held_pool mutex poisoned")
            .push_back(tx);
    }

    /// Append `tx` only if it extends the contiguous queued + in-flight nonce
    /// chain for `(sender, direction)`. Reservations below `on_chain` have
    /// already landed and do not advance the expected nonce a second time.
    /// Duplicate hashes are accepted as idempotent.
    pub fn push_contiguous(
        &self,
        tx: HeldTx,
        on_chain: u64,
    ) -> Result<HoldInsert, NonceAdmissionError> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        if state.contains_hash(tx.hash) {
            return Ok(HoldInsert::Duplicate);
        }
        let Some(expected) = state.next_expected_nonce(tx.sender, tx.direction, on_chain) else {
            return Err(NonceAdmissionError {
                expected: u64::MAX,
                on_chain,
            });
        };
        if tx.nonce != expected {
            return Err(NonceAdmissionError { expected, on_chain });
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

    /// Release txs that were drained into an optimistic bundle and are now
    /// known not to need nonce reservation anymore.
    pub fn release_in_flight_batch(&self, txs: &[HeldTx]) {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        for tx in txs {
            state.release_in_flight(tx);
        }
    }

    /// Atomically evict every queued and in-flight reservation at or above
    /// `nonce` in one sender/direction chain. Returns queued transactions that
    /// were removed; callers already own any matching in-flight transactions.
    #[must_use]
    pub fn evict_nonce_chain_from(
        &self,
        sender: Address,
        direction: Direction,
        nonce: u64,
    ) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let mut evicted = Vec::new();
        let mut kept = VecDeque::with_capacity(state.txs.len());
        for tx in state.txs.drain(..) {
            if tx.sender == sender && tx.direction == direction && tx.nonce >= nonce {
                evicted.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        state.txs = kept;
        state
            .in_flight
            .retain(|_, (s, d, n)| !(*s == sender && *d == direction && *n >= nonce));
        state.rebuild_index();
        evicted
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

    fn insert(pool: &HeldPool, tx: HeldTx, on_chain: u64) {
        assert_eq!(
            pool.push_contiguous(tx, on_chain).unwrap(),
            HoldInsert::Inserted
        );
    }

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
        insert(&pool, tx(1), 1);
        insert(&pool, tx(2), 2);
        insert(&pool, tx(3), 3);
        let drained = pool.pop_all();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].hash, TxHash::from(B256::repeat_byte(1)));
        assert_eq!(drained[2].hash, TxHash::from(B256::repeat_byte(3)));
        assert!(pool.is_empty());
    }

    #[test]
    fn push_dedupes_by_hash() {
        let pool = HeldPool::new();
        insert(&pool, tx(1), 1);
        assert_eq!(
            pool.push_contiguous(tx(1), 1).unwrap(),
            HoldInsert::Duplicate
        );
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn drain_matching_removes_only_listed() {
        let pool = HeldPool::new();
        insert(&pool, tx(1), 1);
        insert(&pool, tx(2), 2);
        insert(&pool, tx(3), 3);
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
        // Same EOA, two independent nonce chains. Admission / draining one
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
        insert(&pool, mk(0, Direction::Inbound, 1), 0);
        insert(&pool, mk(1, Direction::Inbound, 2), 0);
        insert(&pool, mk(0, Direction::Outbound, 3), 0);
        insert(&pool, mk(1, Direction::Outbound, 4), 0);

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
        insert(&pool, tx(1), 1);
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
        assert_eq!(
            err,
            NonceAdmissionError {
                expected: 1,
                on_chain: 0
            }
        );
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn contiguous_push_handles_landed_but_not_derived_reservation() {
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

        // Before landing, nonce 0 is supplied by the in-flight reservation.
        assert_eq!(
            pool.push_contiguous(mk(1, 2), 0).unwrap(),
            HoldInsert::Inserted
        );

        // Once nonce 0 lands, the source-chain nonce advances to 1 while the
        // reservation remains until Deriver confirmation. It must not be
        // counted a second time; queued nonce 1 makes nonce 2 the next value.
        assert_eq!(
            pool.push_contiguous(mk(2, 3), 1).unwrap(),
            HoldInsert::Inserted
        );
        let err = pool.push_contiguous(mk(4, 4), 1).unwrap_err();
        assert_eq!(
            err,
            NonceAdmissionError {
                expected: 3,
                on_chain: 1
            }
        );

        pool.release_in_flight_batch(&drained);
    }

    #[test]
    fn contiguous_push_fills_an_existing_reserved_gap() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xb);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: Direction::Outbound,
        };

        pool.push(mk(2, 1));
        assert_eq!(
            pool.push_contiguous(mk(1, 2), 1).unwrap(),
            HoldInsert::Inserted
        );
    }

    #[test]
    fn contiguous_push_caps_exhausted_nonce_space_at_max() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xd);
        let mk = |h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce: u64::MAX,
            direction: Direction::Inbound,
        };

        insert(&pool, mk(1), u64::MAX);
        assert_eq!(
            pool.push_contiguous(mk(2), u64::MAX).unwrap_err(),
            NonceAdmissionError {
                expected: u64::MAX,
                on_chain: u64::MAX
            }
        );
    }

    #[test]
    fn nonce_chain_eviction_removes_queued_and_in_flight_suffix_atomically() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xe);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            sender,
            nonce,
            direction: Direction::Inbound,
        };
        for nonce in 0..=3 {
            insert(&pool, mk(nonce, nonce as u8 + 1), 0);
        }
        let drained = pool.pop_n(3);
        assert_eq!(
            drained.iter().map(|tx| tx.nonce).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        let queued = pool.evict_nonce_chain_from(sender, Direction::Inbound, 1);

        assert_eq!(
            queued.iter().map(|tx| tx.nonce).collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(
            pool.push_contiguous(mk(1, 5), 0).unwrap(),
            HoldInsert::Inserted
        );
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

        insert(&pool, mk(sender_a, 0, 1), 0);
        insert(&pool, mk(sender_a, 1, 2), 0);
        insert(&pool, mk(sender_a, 2, 3), 0);
        insert(&pool, mk(sender_b, 0, 4), 0);

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
