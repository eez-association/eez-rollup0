//! Per-rollup pool of cross-chain transactions awaiting Sync-slot
//! composition.
//!
//! Cross-chain txs (detected at `TxIngress` time per §5.4.5) sit here
//! between submission and the next Sync slot, where they're drained,
//! handed to the umbrella's [`crate::Composer`] for simulation, and the
//! resulting `system_txs` get bundled into the Sync block.
//!
//! Held-pool drain semantics:
//!
//! - [`HeldPool::pop_n`] / [`HeldPool::pop_all`] — drain queued transactions
//!   and reserve their nonces as in-flight. Called by the umbrella on Sync-slot
//!   trigger after the composer's batch is built and the bundle is
//!   queued for submission. Cross-chain reorg recovery (re-injecting
//!   pre-composed txs after L1 reorg or bundle drop) replays into the
//!   pool via [`HeldPool::push_front_batch`].
//!
//! Concurrency: one `Mutex<PoolState>` keeps queue membership, hash
//! dedupe, and in-flight nonce reservations atomic. Held-pool operations
//! are infrequent (per sync slot, per L1 event) and never hot-path.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use alloy_primitives::{Address, Bytes, TxHash};
use tracing::{Level, event};

use crate::ingress::Direction;

/// A cross-chain transaction held between submission and Sync-slot
/// composition.
#[derive(Debug, Clone)]
pub struct HeldTx {
    /// RLP-encoded source transaction (signed envelope). Handed to
    /// `EvmComposer::simulate_and_resolve` verbatim.
    pub raw_tx: Bytes,
    /// Cached hash of the signed envelope, used for queued/in-flight dedupe.
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
    /// independent nonce chains, keyed on `(sender, direction)`.
    pub direction: Direction,
    /// Declared `max_fee_per_gas` (legacy: `gas_price`). The pool is FIFO,
    /// so this buys no priority — it only enforces the replacement bump.
    pub max_fee_per_gas: u128,
    /// Effective priority fee (legacy/2930: `gas_price`, which is both cap
    /// and tip). Replacements must bump it alongside the cap (geth rule).
    pub priority_fee_per_gas: u128,
}

/// Minimum bump (percent) a queued-nonce replacement must offer on BOTH
/// `max_fee_per_gas` and the priority fee, each over its replaced value
/// (geth's price-bump rule). The cap bump bounds churn via the ingress
/// balance check; the tip bump keeps a replacement a real better offer.
pub const REPLACEMENT_TX_COST_PERCENT: u128 = 10;

/// A pool admission failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// Nonce is burned, reserved in-flight, or not the next unreserved slot.
    Nonce { expected: u64, on_chain: u64 },
    /// Queued-nonce replacement did not bump both fees by the required
    /// percent. `required_*` values are what the replacement had to offer.
    UnderpricedReplacement {
        offered_max_fee: u128,
        required_max_fee: u128,
        offered_priority_fee: u128,
        required_priority_fee: u128,
    },
}

/// Smallest fee a replacement must offer over `replaced`: ≥10% more,
/// always at least +1 (bars equal-fee churn).
fn min_replacement_fee(replaced: u128) -> u128 {
    let required_increase = (replaced.saturating_mul(REPLACEMENT_TX_COST_PERCENT) / 100).max(1);
    replaced.saturating_add(required_increase)
}

/// All pool state under one lock, so queue membership, hash dedupe and
/// nonce reservations can never disagree with each other.
#[derive(Debug, Default)]
struct PoolState {
    /// Queued txs awaiting the next Sync-slot drain, in FIFO order.
    txs: VecDeque<HeldTx>,
    /// Hashes of the queued txs — O(1) duplicate-submission check.
    by_hash: HashSet<TxHash>,
    /// Txs drained into a bundle whose outcome is not yet known
    /// (`hash → (sender, direction, nonce)`). The nonce stays reserved —
    /// still counted by admission, not replaceable — until the bundle
    /// settles (release) or fails (recovery re-queues the tx).
    /// Invariant: every drained tx must eventually be released, re-queued,
    /// or evicted by its caller; a forgotten reservation bricks its nonce
    /// slot (admission forever expects past it).
    in_flight: HashMap<TxHash, (Address, Direction, u64)>,
}

impl PoolState {
    /// Next nonce that keeps the `(sender, direction)` chain contiguous:
    /// one past the highest queued-or-in-flight reservation, or `on_chain`
    /// if there is none. Reservations below `on_chain` already landed and
    /// are ignored. `None` on nonce overflow (chain is at `u64::MAX`).
    fn next_expected_nonce(
        &self,
        sender: Address,
        direction: Direction,
        on_chain: u64,
    ) -> Option<u64> {
        let highest_reserved = self
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
            .filter(|nonce| *nonce >= on_chain)
            .max();
        match highest_reserved {
            Some(nonce) => nonce.checked_add(1),
            None => Some(on_chain),
        }
    }

    /// Move a recovered tx from in-flight back to the queue front,
    /// skipping the re-queue if the hash is already queued (idempotent).
    fn push_front_recovered(&mut self, tx: HeldTx) {
        self.release_in_flight(&tx);
        if self.by_hash.insert(tx.hash) {
            self.txs.push_front(tx);
        }
    }

    /// Reserve a drained tx's nonce until its bundle resolves.
    fn mark_in_flight(&mut self, tx: &HeldTx) {
        self.in_flight
            .entry(tx.hash)
            .or_insert((tx.sender, tx.direction, tx.nonce));
    }

    /// Drop a tx's nonce reservation (bundle settled, tx evicted, or
    /// re-queued by recovery).
    fn release_in_flight(&mut self, tx: &HeldTx) {
        self.in_flight.remove(&tx.hash);
    }
}

/// Per-rollup pool of held cross-chain transactions.
///
/// Every [`RollupState`](crate::RollupState) owned by a composer has one;
/// follower and development binaries do not construct `RollupState`.
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

    /// Admit `tx` into the contiguous queued + in-flight nonce chain for
    /// `(sender, direction)`. A new hash at an existing queued nonce replaces
    /// that transaction in place, if it bumps `max_fee_per_gas` by
    /// [`REPLACEMENT_TX_COST_PERCENT`]. In-flight nonces cannot be replaced.
    /// Reservations below `on_chain` have already landed and do not advance
    /// the expected nonce a second time. Duplicate hashes are idempotent.
    pub fn push_contiguous(&self, tx: HeldTx, on_chain: u64) -> Result<(), AdmissionError> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        if state.by_hash.contains(&tx.hash) || state.in_flight.contains_key(&tx.hash) {
            return Ok(());
        }

        let target_sender = tx.sender;
        let target_direction = tx.direction;
        let target_nonce = tx.nonce;
        let same_chain = |sender: Address, direction: Direction| {
            sender == target_sender && direction == target_direction
        };
        let queued_match = state.txs.iter().position(|queued| {
            same_chain(queued.sender, queued.direction) && queued.nonce == target_nonce
        });

        if target_nonce < on_chain {
            let expected = state
                .next_expected_nonce(target_sender, target_direction, on_chain)
                .unwrap_or(u64::MAX);
            return Err(AdmissionError::Nonce { expected, on_chain });
        }

        if let Some(queued_idx) = queued_match {
            // Geth price-bump rule: BOTH fees must bump over the replaced tx.
            let required_max_fee = min_replacement_fee(state.txs[queued_idx].max_fee_per_gas);
            let required_priority_fee =
                min_replacement_fee(state.txs[queued_idx].priority_fee_per_gas);
            if tx.max_fee_per_gas < required_max_fee
                || tx.priority_fee_per_gas < required_priority_fee
            {
                return Err(AdmissionError::UnderpricedReplacement {
                    offered_max_fee: tx.max_fee_per_gas,
                    required_max_fee,
                    offered_priority_fee: tx.priority_fee_per_gas,
                    required_priority_fee,
                });
            }
            let replacement_hash = tx.hash;
            let previous_hash = state.txs[queued_idx].hash;
            state.txs[queued_idx] = tx;
            state.by_hash.remove(&previous_hash);
            state.by_hash.insert(replacement_hash);
            return Ok(());
        }

        if state.in_flight.values().any(|(sender, direction, nonce)| {
            same_chain(*sender, *direction) && *nonce == target_nonce
        }) {
            return Err(AdmissionError::Nonce {
                expected: state
                    .next_expected_nonce(target_sender, target_direction, on_chain)
                    .unwrap_or(u64::MAX),
                on_chain,
            });
        }

        let Some(expected) = state.next_expected_nonce(target_sender, target_direction, on_chain)
        else {
            return Err(AdmissionError::Nonce {
                expected: u64::MAX,
                on_chain,
            });
        };
        if target_nonce != expected {
            return Err(AdmissionError::Nonce { expected, on_chain });
        }
        state.by_hash.insert(tx.hash);
        state.txs.push_back(tx);
        Ok(())
    }

    /// Re-queue recovered txs at the front, unless a newer queued replacement
    /// owns that nonce; then discard the recovered root and suffix.
    pub fn push_front_batch(&self, recovered: Vec<HeldTx>) {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let mut replacement_roots: HashMap<(Address, Direction), (u64, TxHash)> = HashMap::new();
        for tx in &recovered {
            let replacement = state.txs.iter().find(|queued| {
                queued.sender == tx.sender
                    && queued.direction == tx.direction
                    && queued.nonce == tx.nonce
                    && queued.hash != tx.hash
            });
            if let Some(replacement) = replacement {
                replacement_roots
                    .entry((tx.sender, tx.direction))
                    .and_modify(|(nonce, hash)| {
                        if tx.nonce < *nonce {
                            *nonce = tx.nonce;
                            *hash = replacement.hash;
                        }
                    })
                    .or_insert((tx.nonce, replacement.hash));
            }
        }

        for tx in recovered.into_iter().rev() {
            if let Some(&(conflict_nonce, replacement_hash)) =
                replacement_roots.get(&(tx.sender, tx.direction))
                && tx.nonce >= conflict_nonce
            {
                state.release_in_flight(&tx);
                event!(
                    name: "eez.held_pool.recovery_conflict",
                    Level::WARN,
                    recovered_hash = %tx.hash,
                    replacement_hash = %replacement_hash,
                    sender = %tx.sender,
                    direction = ?tx.direction,
                    nonce = tx.nonce,
                    conflict_nonce,
                    "newer queued replacement wins; discarding recovered nonce-chain suffix",
                );
                continue;
            }
            state.push_front_recovered(tx);
        }
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
    pub fn evict_chain_at_or_above(
        &self,
        sender: Address,
        direction: Direction,
        nonce: u64,
    ) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let mut evicted = Vec::new();
        let queued = std::mem::take(&mut state.txs);
        let mut kept = VecDeque::with_capacity(queued.len());
        for tx in queued {
            if tx.sender == sender && tx.direction == direction && tx.nonce >= nonce {
                state.by_hash.remove(&tx.hash);
                evicted.push(tx);
            } else {
                kept.push_back(tx);
            }
        }
        state.txs = kept;
        state
            .in_flight
            .retain(|_, (s, d, n)| !(*s == sender && *d == direction && *n >= nonce));
        evicted
    }

    /// Drain every queued transaction and reserve its nonce as in-flight.
    /// Recovery paths can return the batch with [`Self::push_front_batch`].
    pub fn pop_all(&self) -> Vec<HeldTx> {
        self.pop_n(usize::MAX)
    }

    /// Drain up to `max` held txs (FIFO). A bundle is all-or-nothing, so one
    /// un-includable tx drops the whole bundle; capping bounds how many good txs
    /// that takes down, at the cost of more Sync slots to drain a large pool.
    pub fn pop_n(&self, max: usize) -> Vec<HeldTx> {
        let mut state = self.state.lock().expect("held_pool mutex poisoned");
        let count = max.min(state.txs.len());
        let drained: Vec<_> = state.txs.drain(..count).collect();
        for tx in &drained {
            state.by_hash.remove(&tx.hash);
            state.mark_in_flight(tx);
        }
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
        pool.push_contiguous(tx, on_chain).unwrap();
    }

    fn tx(byte: u8) -> HeldTx {
        tx_dir(byte, Direction::Inbound)
    }

    fn tx_dir(byte: u8, direction: Direction) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![byte; 32]),
            hash: TxHash::from(B256::repeat_byte(byte)),
            attempts: 0,
            max_fee_per_gas: u128::from(byte),
            priority_fee_per_gas: u128::from(byte),
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
        let original = tx(1);
        insert(&pool, original.clone(), 1);
        pool.push_contiguous(original.clone(), 1).unwrap();
        assert_eq!(pool.len(), 1);

        let drained = pool.pop_all();
        pool.push_contiguous(original.clone(), 1).unwrap();
        let mut replacement = original;
        replacement.hash = TxHash::from(B256::repeat_byte(9));
        assert_eq!(
            pool.push_contiguous(replacement, 1).unwrap_err(),
            AdmissionError::Nonce {
                expected: 2,
                on_chain: 1
            }
        );
        pool.release_in_flight_batch(&drained);
    }

    #[test]
    fn underpriced_replacement_is_rejected() {
        let pool = HeldPool::new();
        let mut original = tx(1);
        original.max_fee_per_gas = 100;
        original.priority_fee_per_gas = 10;
        insert(&pool, original, 1);

        // Cap bumped but tip not (geth rule: BOTH must bump).
        let mut replacement = tx(1);
        replacement.hash = TxHash::from(B256::repeat_byte(9));
        replacement.max_fee_per_gas = 110;
        replacement.priority_fee_per_gas = 10;
        assert_eq!(
            pool.push_contiguous(replacement.clone(), 1).unwrap_err(),
            AdmissionError::UnderpricedReplacement {
                offered_max_fee: 110,
                required_max_fee: 110,
                offered_priority_fee: 10,
                required_priority_fee: 11
            }
        );

        // Tip bumped but cap not.
        replacement.max_fee_per_gas = 109;
        replacement.priority_fee_per_gas = 11;
        assert!(pool.push_contiguous(replacement.clone(), 1).is_err());

        // Both bumped → replaces.
        replacement.max_fee_per_gas = 110;
        pool.push_contiguous(replacement.clone(), 1).unwrap();
        assert_eq!(pool.pop_all()[0].hash, replacement.hash);
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
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce,
            direction: dir,
        };
        insert(&pool, mk(0, Direction::Inbound, 1), 0);
        insert(&pool, mk(1, Direction::Inbound, 2), 0);
        insert(&pool, mk(0, Direction::Outbound, 3), 0);
        insert(&pool, mk(1, Direction::Outbound, 4), 0);

        let evicted = pool.evict_chain_at_or_above(sender, Direction::Outbound, 1);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].nonce, 1);
        assert_eq!(evicted[0].direction, Direction::Outbound);

        let remaining = pool.pop_n(usize::MAX);
        assert_eq!(
            remaining
                .iter()
                .filter(|tx| tx.direction == Direction::Inbound)
                .count(),
            2,
            "inbound chain must remain untouched"
        );
        assert!(
            remaining
                .iter()
                .any(|tx| { tx.direction == Direction::Outbound && tx.nonce == 0 })
        );
    }

    #[test]
    fn queued_nonce_replacement_preserves_position_and_suffix() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(9);
        let other_sender = Address::repeat_byte(8);
        let mk = |sender: Address, nonce: u64, direction: Direction, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce,
            direction,
        };

        insert(&pool, mk(sender, 0, Direction::Inbound, 1), 0);
        insert(&pool, mk(other_sender, 0, Direction::Inbound, 10), 0);
        insert(&pool, mk(sender, 1, Direction::Inbound, 2), 0);
        insert(&pool, mk(sender, 0, Direction::Outbound, 20), 0);
        insert(&pool, mk(sender, 2, Direction::Inbound, 3), 0);
        insert(&pool, mk(other_sender, 1, Direction::Inbound, 11), 0);
        insert(&pool, mk(sender, 1, Direction::Outbound, 21), 0);

        pool.push_contiguous(mk(sender, 1, Direction::Inbound, 9), 0)
            .unwrap();

        let drained = pool.pop_all();
        assert!(drained.iter().any(|tx| {
            tx.sender == sender
                && tx.direction == Direction::Outbound
                && tx.nonce == 1
                && tx.hash == TxHash::from(B256::repeat_byte(21))
        }));
        let hashes: Vec<_> = drained.iter().map(|tx| tx.hash).collect();
        assert_eq!(
            hashes,
            vec![
                TxHash::from(B256::repeat_byte(1)),
                TxHash::from(B256::repeat_byte(10)),
                TxHash::from(B256::repeat_byte(9)),
                TxHash::from(B256::repeat_byte(20)),
                TxHash::from(B256::repeat_byte(3)),
                TxHash::from(B256::repeat_byte(11)),
                TxHash::from(B256::repeat_byte(21)),
            ]
        );
    }

    #[test]
    fn queued_replacement_allows_lower_in_flight_but_not_in_flight_nonce() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(9);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce,
            direction: Direction::Inbound,
        };
        for nonce in 0..=2 {
            insert(&pool, mk(nonce, nonce as u8 + 1), 0);
        }
        let drained = pool.pop_n(1);
        assert_eq!(drained[0].nonce, 0);

        pool.push_contiguous(mk(1, 9), 0).unwrap();
        assert_eq!(
            pool.push_contiguous(mk(0, 8), 0).unwrap_err(),
            AdmissionError::Nonce {
                expected: 3,
                on_chain: 0
            }
        );
        pool.push_contiguous(mk(2, 7), 0).unwrap();

        let queued = pool.pop_all();
        assert_eq!(queued.len(), 2);
        assert_eq!(queued[0].hash, TxHash::from(B256::repeat_byte(9)));
        assert_eq!(queued[1].hash, TxHash::from(B256::repeat_byte(7)));
    }

    #[test]
    fn contiguous_push_handles_landed_but_not_derived_reservation() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xa);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce,
            direction: Direction::Inbound,
        };

        pool.push_contiguous(mk(0, 1), 0).unwrap();
        let drained = pool.pop_n(1);
        assert_eq!(drained.len(), 1);
        assert!(pool.is_empty());

        // Before landing, nonce 0 is supplied by the in-flight reservation.
        pool.push_contiguous(mk(1, 2), 0).unwrap();

        // Once nonce 0 lands, the source-chain nonce advances to 1 while the
        // reservation remains until Deriver confirmation. It must not be
        // counted a second time; queued nonce 1 makes nonce 2 the next value.
        pool.push_contiguous(mk(2, 3), 1).unwrap();
        let err = pool.push_contiguous(mk(4, 4), 1).unwrap_err();
        assert_eq!(
            err,
            AdmissionError::Nonce {
                expected: 3,
                on_chain: 1
            }
        );

        pool.release_in_flight_batch(&drained);
    }

    #[test]
    fn max_nonce_can_be_replaced_while_queued_but_not_in_flight() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xd);
        let mk = |h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce: u64::MAX,
            direction: Direction::Inbound,
        };

        insert(&pool, mk(1), u64::MAX);
        pool.push_contiguous(mk(2), u64::MAX).unwrap();
        let drained = pool.pop_all();
        assert_eq!(drained[0].hash, TxHash::from(B256::repeat_byte(2)));
        assert_eq!(
            pool.push_contiguous(mk(3), u64::MAX).unwrap_err(),
            AdmissionError::Nonce {
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
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
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

        let queued = pool.evict_chain_at_or_above(sender, Direction::Inbound, 1);

        assert_eq!(
            queued.iter().map(|tx| tx.nonce).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(
            !pool
                .state
                .lock()
                .unwrap()
                .in_flight
                .values()
                .any(|(s, d, nonce)| { *s == sender && *d == Direction::Inbound && *nonce >= 1 }),
            "inclusive eviction must release the root's own reservation"
        );
        pool.push_contiguous(mk(1, 5), 0).unwrap();
    }

    #[test]
    fn recovery_preserves_newer_replacement_and_discards_old_suffix() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xf);
        let mk = |nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
            sender,
            nonce,
            direction: Direction::Inbound,
        };
        let old_root = mk(0, 1);
        let old_suffix = mk(1, 2);
        insert(&pool, old_root.clone(), 0);
        insert(&pool, old_suffix.clone(), 0);
        let old_batch = pool.pop_n(2);

        pool.release_in_flight_batch(&old_batch);
        let replacement = mk(0, 9);
        insert(&pool, replacement.clone(), 0);
        pool.push_front_batch(old_batch);

        let state = pool.state.lock().unwrap();
        assert_eq!(state.txs.len(), 1);
        assert_eq!(state.txs[0].hash, replacement.hash);
        assert!(!state.in_flight.contains_key(&old_root.hash));
        assert!(!state.in_flight.contains_key(&old_suffix.hash));
    }

    #[test]
    fn same_hash_recovery_is_idempotent() {
        let pool = HeldPool::new();
        let original = tx(1);
        insert(&pool, original.clone(), 1);
        let drained = pool.pop_n(1);

        pool.push_front_batch(drained.clone());
        pool.push_front_batch(drained);

        let state = pool.state.lock().unwrap();
        assert_eq!(state.txs.len(), 1);
        assert_eq!(state.txs[0].hash, original.hash);
        assert!(state.in_flight.is_empty());
    }

    #[test]
    fn pop_n_drains_fifo_prefix() {
        let pool = HeldPool::new();
        let sender_a = Address::repeat_byte(0xa);
        let sender_b = Address::repeat_byte(0xb);
        let mk = |sender: Address, nonce: u64, h: u8| HeldTx {
            raw_tx: Bytes::from(vec![h; 4]),
            hash: TxHash::from(B256::repeat_byte(h)),
            attempts: 0,
            max_fee_per_gas: u128::from(h),
            priority_fee_per_gas: u128::from(h),
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
                TxHash::from(B256::repeat_byte(2)),
                TxHash::from(B256::repeat_byte(3)),
            ]
        );
        assert_eq!(pool.pop_all()[0].hash, TxHash::from(B256::repeat_byte(4)));
    }
}
