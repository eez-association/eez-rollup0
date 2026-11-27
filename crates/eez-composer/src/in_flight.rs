//! Drained transactions until their fate is decided: the bodies of each
//! in-flight batch, and the pool work that releases them, re-queues them, or
//! charges them an attempt and evicts them at the bound.
//!
//! The optimistic ledger keeps only a [`BatchTx`](crate::BatchTx) per
//! transaction. Recovery decides each one's fate and hands it over as a
//! [`BatchDisposition`]; [`dispose`] carries it out here.

use std::collections::HashMap;
use std::sync::Mutex;

use alloy_primitives::{Address, B256, TxHash};
use eez_prover::{ProverError, RetryableProverError};
use tracing::{Level, event};

use crate::disposition::{BatchDisposition, TxDisposition};
use crate::held_pool::{HeldPool, HeldTx};
use crate::ingress::Direction;

/// Failed inclusion attempts before a held user_tx is evicted as probable
/// poison. This covers funding deferrals, relay drops, and proof requests that
/// still fail after their retry episode. After this many failures, the
/// transaction and its nonce-dependent suffix are evicted so they cannot block
/// the FIFO queue indefinitely.
///
/// Drain-time simulations are chained per chain in canonical order
/// (`compose_crosschain` over the slot's L1 state and the Sync block under
/// construction — `docs/CHAINED-INTERSTATE-DESIGN.md`), so a co-bundled
/// prerequisite is already visible when its dependant composes. What this bound
/// backstops is the residual: L1 state that moves between compose time and the
/// bundle's inclusion block.
pub const MAX_BUNDLE_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub(crate) enum SettlementFailureSource {
    Prover,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SettlementFailureOutcome {
    pub(crate) requeued: usize,
    pub(crate) evicted: usize,
}

/// Count one failed settlement episode for every candidate, evict transactions
/// that reach the bound, and cascade each eviction through its sender/direction
/// nonce suffix. Survivors retain their attempt count and FIFO order.
pub(crate) fn recover_settlement_failure(
    pool: &HeldPool,
    rollup_id: u64,
    txs: Vec<HeldTx>,
    source: SettlementFailureSource,
) -> SettlementFailureOutcome {
    let mut retry = Vec::with_capacity(txs.len());
    let mut evicted = Vec::new();
    let mut evicted_chains: Vec<(Address, Direction, u64)> = Vec::new();

    for mut tx in txs {
        tx.attempts = tx.attempts.saturating_add(1);
        if tx.attempts >= MAX_BUNDLE_ATTEMPTS {
            evicted_chains.push((tx.sender, tx.direction, tx.nonce));
            event!(
                name: "eez.composer.recovery.poison_evicted",
                Level::ERROR,
                event_name = "eez.composer.recovery.poison_evicted",
                rollup_id,
                source = ?source,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                direction = ?tx.direction,
                nonce = tx.nonce,
                attempts = tx.attempts,
                "potentially valid user_tx evicted after MAX_BUNDLE_ATTEMPTS failed settlement episodes to preserve chain liveness; repeated component disagreement requires investigation; resubmit required",
            );
            evicted.push(tx);
        } else {
            retry.push(tx);
        }
    }

    // Without the missing nonce, later transactions cannot execute and would
    // keep invalidating batches that include them.
    for (sender, direction, nonce) in evicted_chains {
        retry.retain(|tx| {
            let cascade =
                tx.sender == sender && tx.direction == direction && tx.nonce > nonce;
            if cascade {
                event!(
                    name: "eez.composer.recovery.nonce_chain_evicted",
                    Level::ERROR,
                    event_name = "eez.composer.recovery.nonce_chain_evicted",
                    rollup_id,
                    source = ?source,
                    tx_hash = %tx.hash,
                    sender = %tx.sender,
                    direction = ?tx.direction,
                    nonce = tx.nonce,
                    gap_at = nonce,
                    "potentially valid same-sender tx depends on an evicted nonce; evicting nonce suffix for liveness (resubmit in order)",
                );
                evicted.push(tx.clone());
            }
            !cascade
        });
        for tx in pool.evict_chain_at_or_above(sender, direction, nonce) {
            event!(
                name: "eez.composer.recovery.nonce_chain_evicted",
                Level::ERROR,
                event_name = "eez.composer.recovery.nonce_chain_evicted",
                rollup_id,
                source = ?source,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                direction = ?tx.direction,
                nonce = tx.nonce,
                gap_at = nonce,
                "potentially valid pooled tx depends on an evicted nonce; evicting nonce suffix for liveness (resubmit in order)",
            );
            evicted.push(tx);
        }
    }

    let outcome = SettlementFailureOutcome {
        requeued: retry.len(),
        evicted: evicted.len(),
    };
    pool.release_in_flight_batch(&evicted);
    pool.push_front_batch(retry);
    outcome
}

pub(crate) fn recover_prover_failure(
    pool: &HeldPool,
    rollup_id: u64,
    txs: Vec<HeldTx>,
    error: &ProverError,
) -> SettlementFailureOutcome {
    if error.retryable_kind() == Some(RetryableProverError::Unavailable) {
        // An unavailable backend has not rejected these candidates. Preserve
        // earlier charged attempts so an outage cannot reset the eviction bound.
        let requeued = txs.len();
        pool.push_front_batch(txs);
        return SettlementFailureOutcome {
            requeued,
            evicted: 0,
        };
    }
    recover_settlement_failure(pool, rollup_id, txs, SettlementFailureSource::Prover)
}

/// The bodies of each in-flight batch's transactions, keyed by batch id (the
/// postBatch hash). One per cross-chain rollup, alongside its `HeldPool`.
#[derive(Debug, Default)]
pub struct InFlightBatches {
    by_batch: Mutex<HashMap<B256, Vec<HeldTx>>>,
}

impl InFlightBatches {
    /// Empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold the bodies of `txs` while batch `batch_id` is in flight.
    pub(crate) fn begin(&self, batch_id: B256, txs: Vec<HeldTx>) {
        let mut by_batch = self
            .by_batch
            .lock()
            .expect("in-flight batches mutex poisoned");
        by_batch.insert(batch_id, txs);
    }

    /// The bodies `disposition` names, in its order, with their fates, and the
    /// named txs that have no body here. A body leaves the table unless it is
    /// released under `keep_released`; a batch with no body left leaves too.
    fn take(
        &self,
        disposition: &BatchDisposition,
    ) -> (Vec<(HeldTx, TxDisposition)>, Vec<(TxHash, TxDisposition)>) {
        let mut by_batch = self
            .by_batch
            .lock()
            .expect("in-flight batches mutex poisoned");
        let Some(bodies) = by_batch.get_mut(&disposition.batch_id) else {
            return (Vec::new(), disposition.txs.clone());
        };
        let mut taken = Vec::with_capacity(disposition.txs.len());
        let mut missing = Vec::new();
        for &(hash, fate) in &disposition.txs {
            let Some(at) = bodies.iter().position(|tx| tx.hash == hash) else {
                missing.push((hash, fate));
                continue;
            };
            let body = if fate == TxDisposition::Release && disposition.keep_released {
                bodies[at].clone()
            } else {
                bodies.remove(at)
            };
            taken.push((body, fate));
        }
        if bodies.is_empty() {
            by_batch.remove(&disposition.batch_id);
        }
        (taken, missing)
    }
}

/// Carry out recovery's decisions on the held pool in one pass: refuse each
/// batch's txs at its `refuse_at`, release the `Release` txs, re-queue the
/// `Requeue` txs at the front in one push, then charge the `Charge` txs. A
/// `Forget` tx only loses its body.
///
/// A pass that refuses reports the txs it released (`nonce_burned`) and its
/// totals (`re_pushed`); recovery hands such a pass one batch. A `Release`
/// needs only the tx's hash. A `Requeue` or `Charge` tx with no body here (an
/// unknown or already disposed batch) is left alone.
pub(crate) fn dispose(
    rollup_id: u64,
    pool: &HeldPool,
    batches: &InFlightBatches,
    dispositions: Vec<BatchDisposition>,
) {
    let mut refused_at = None;
    let mut dropped = 0usize;
    let mut release = Vec::new();
    let mut requeue = Vec::new();
    let mut charge = Vec::new();
    for disposition in &dispositions {
        if let Some(sync_height) = disposition.refuse_at {
            // Releasing or evicting a tx can come before reth re-adds it from the
            // dropped block, which would mine it as a plain L2 call.
            pool.refuse(disposition.txs.iter().map(|(hash, _)| *hash), sync_height);
            refused_at = Some(sync_height);
            for (hash, _) in disposition
                .txs
                .iter()
                .filter(|(_, fate)| *fate == TxDisposition::Release)
            {
                dropped += 1;
                event!(
                    name: "eez.composer.recovery.nonce_burned",
                    Level::WARN,
                    event_name = "eez.composer.recovery.nonce_burned",
                    rollup_id,
                    tx_hash = %hash,
                    "user_tx nonce already spent; not re-queueing (a reverted one must be resubmitted)",
                );
            }
        }
        let (taken, missing) = batches.take(disposition);
        let mut unknown = 0usize;
        for (hash, fate) in missing {
            match fate {
                TxDisposition::Release => release.push(hash),
                TxDisposition::Requeue | TxDisposition::Charge => unknown += 1,
                TxDisposition::Forget => {}
            }
        }
        if unknown > 0 {
            event!(
                name: "eez.composer.recovery.unknown_txs",
                Level::WARN,
                event_name = "eez.composer.recovery.unknown_txs",
                rollup_id,
                batch_id = %disposition.batch_id,
                unknown,
                "disposition names txs with no in-flight body; leaving them alone",
            );
        }
        for (tx, fate) in taken {
            match fate {
                TxDisposition::Release => release.push(tx.hash),
                TxDisposition::Requeue => requeue.push(tx),
                TxDisposition::Charge => charge.push(tx),
                TxDisposition::Forget => {}
            }
        }
    }
    pool.release_by_hash(release);
    let requeued = requeue.len();
    pool.push_front_batch(requeue);
    let charged =
        recover_settlement_failure(pool, rollup_id, charge, SettlementFailureSource::Relay);
    let re_pushed = requeued + charged.requeued;
    dropped += charged.evicted;
    if let Some(sync_height) = refused_at
        && (re_pushed > 0 || dropped > 0)
    {
        event!(
            name: "eez.composer.recovery.re_pushed",
            Level::INFO,
            rollup_id,
            sync_height,
            re_pushed,
            dropped,
            "batch recovered; user_txs re-queued (front) for the next Sync slot",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::tests::held;
    use TxDisposition::{Charge, Release, Requeue};

    const BATCH: B256 = B256::repeat_byte(0xb1);

    /// Admit `txs` and drain them into batch `batch_id`, as the drain and the
    /// rich-batch begin do. Nonces start at zero per sender.
    fn begin(pool: &HeldPool, batches: &InFlightBatches, batch_id: B256, txs: &[&HeldTx]) {
        for tx in txs {
            pool.push_contiguous((*tx).clone(), 0).unwrap();
        }
        batches.begin(batch_id, pool.pop_n(txs.len()));
    }

    fn decided(
        batch_id: B256,
        refuse_at: Option<u64>,
        txs: &[(&HeldTx, TxDisposition)],
        keep_released: bool,
    ) -> BatchDisposition {
        BatchDisposition {
            batch_id,
            refuse_at,
            txs: txs.iter().map(|(tx, fate)| (tx.hash, *fate)).collect(),
            keep_released,
        }
    }

    /// The bodies the table holds for `batch_id`, in order.
    fn bodies(batches: &InFlightBatches, batch_id: B256) -> Option<Vec<TxHash>> {
        let by_batch = batches.by_batch.lock().unwrap();
        by_batch
            .get(&batch_id)
            .map(|txs| txs.iter().map(|tx| tx.hash).collect())
    }

    /// The pool keys a reservation by hash, so a release needs no body.
    #[test]
    fn a_release_with_no_body_still_frees_the_reservation() {
        let pool = HeldPool::new();
        let batches = InFlightBatches::new();
        let tx = held(Address::repeat_byte(1), Direction::Inbound, 0, 0x01);
        pool.push_contiguous(tx.clone(), 0).unwrap();
        let _reserved = pool.pop_all();

        dispose(
            1,
            &pool,
            &batches,
            vec![decided(BATCH, None, &[(&tx, Release)], false)],
        );

        assert!(!pool.holds(&tx.hash));
        pool.push_contiguous(held(tx.sender, Direction::Inbound, 0, 0x02), 0)
            .expect("the nonce is free again");
    }

    #[test]
    fn the_table_gives_up_exactly_the_disposed_bodies() {
        let pool = HeldPool::new();
        let batches = InFlightBatches::new();
        let (a, b, c) = (
            held(Address::repeat_byte(1), Direction::Inbound, 0, 0x01),
            held(Address::repeat_byte(2), Direction::Inbound, 0, 0x02),
            held(Address::repeat_byte(3), Direction::Inbound, 0, 0x03),
        );
        begin(&pool, &batches, BATCH, &[&a, &b, &c]);
        assert_eq!(bodies(&batches, BATCH), Some(vec![a.hash, b.hash, c.hash]));

        dispose(
            1,
            &pool,
            &batches,
            vec![decided(BATCH, None, &[(&a, Requeue)], false)],
        );
        assert_eq!(bodies(&batches, BATCH), Some(vec![b.hash, c.hash]));

        // Released under keep_released: the ledger still holds it for a reorg.
        dispose(
            1,
            &pool,
            &batches,
            vec![decided(BATCH, None, &[(&b, Release)], true)],
        );
        assert_eq!(bodies(&batches, BATCH), Some(vec![b.hash, c.hash]));

        let last = [(&b, Release), (&c, Charge)];
        dispose(1, &pool, &batches, vec![decided(BATCH, None, &last, false)]);
        assert_eq!(bodies(&batches, BATCH), None);
    }

    #[test]
    fn settlement_failures_increment_once_per_episode_and_preserve_fifo() {
        let pool = HeldPool::new();
        let first = held(Address::repeat_byte(0xa), Direction::Inbound, 0, 1);
        let second = held(Address::repeat_byte(0xb), Direction::Inbound, 0, 2);
        pool.push_contiguous(first.clone(), 0).unwrap();
        pool.push_contiguous(second.clone(), 0).unwrap();

        let first_failure =
            recover_settlement_failure(&pool, 1, pool.pop_all(), SettlementFailureSource::Prover);
        assert_eq!(
            first_failure,
            SettlementFailureOutcome {
                requeued: 2,
                evicted: 0,
            }
        );
        let after_first = pool.pop_all();
        assert_eq!(
            after_first.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![first.hash, second.hash]
        );
        assert!(after_first.iter().all(|tx| tx.attempts == 1));

        let second_failure =
            recover_settlement_failure(&pool, 1, after_first, SettlementFailureSource::Relay);
        assert_eq!(
            second_failure,
            SettlementFailureOutcome {
                requeued: 2,
                evicted: 0,
            }
        );
        let after_second = pool.pop_all();
        assert_eq!(
            after_second.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![first.hash, second.hash]
        );
        assert!(after_second.iter().all(|tx| tx.attempts == 2));
        pool.release_in_flight_batch(&after_second);
    }

    #[test]
    fn prover_outage_preserves_candidates_and_prior_attempts_past_the_eviction_limit() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xa);
        let mut root = held(sender, Direction::Inbound, 0, 1);
        root.attempts = MAX_BUNDLE_ATTEMPTS - 1;
        let suffix = held(sender, Direction::Inbound, 1, 2);
        let independent = held(Address::repeat_byte(0xb), Direction::Outbound, 0, 3);
        pool.push_contiguous(root.clone(), 0).unwrap();
        pool.push_contiguous(suffix.clone(), 0).unwrap();
        pool.push_contiguous(independent.clone(), 0).unwrap();
        let unavailable = ProverError::Retryable {
            kind: RetryableProverError::Unavailable,
            message: "connection refused".to_owned(),
        };

        for _ in 0..=MAX_BUNDLE_ATTEMPTS {
            let recovery = recover_prover_failure(&pool, 1, pool.pop_all(), &unavailable);
            assert_eq!(
                recovery,
                SettlementFailureOutcome {
                    requeued: 3,
                    evicted: 0,
                },
                "an outage must not consume candidate attempts or evict a nonce suffix",
            );
        }
        let remaining = pool.pop_all();
        assert_eq!(
            remaining.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![root.hash, suffix.hash, independent.hash],
            "recovery must preserve candidates and FIFO order",
        );
        assert_eq!(
            remaining.iter().map(|tx| tx.attempts).collect::<Vec<_>>(),
            vec![MAX_BUNDLE_ATTEMPTS - 1, 0, 0],
            "an outage must neither charge nor reset earlier attempts",
        );
        assert!(
            pool.push_contiguous(held(sender, Direction::Inbound, 0, 4), 0)
                .is_err(),
            "the original nonce reservation must remain held through the outage",
        );

        let recovery = recover_prover_failure(
            &pool,
            1,
            remaining,
            &ProverError::Backend("invalid proof".to_owned()),
        );
        assert_eq!(
            recovery,
            SettlementFailureOutcome {
                requeued: 1,
                evicted: 2,
            },
            "the next charged failure must still evict the root and its nonce suffix",
        );
        let remaining = pool.pop_all();
        assert_eq!(remaining[0].hash, independent.hash);
        assert_eq!(remaining[0].attempts, 1);
        pool.release_in_flight_batch(&remaining);
    }

    #[test]
    fn other_retryable_prover_errors_still_charge_candidate_attempts() {
        for kind in [
            RetryableProverError::DeadlineExceeded,
            RetryableProverError::Aborted,
        ] {
            let pool = HeldPool::new();
            let mut tx = held(Address::repeat_byte(0xa), Direction::Inbound, 0, 1);
            tx.attempts = MAX_BUNDLE_ATTEMPTS - 1;
            pool.push_contiguous(tx, 0).unwrap();
            let error = ProverError::Retryable {
                kind,
                message: "attempt failed".to_owned(),
            };

            let recovery = recover_prover_failure(&pool, 1, pool.pop_all(), &error);
            assert_eq!(
                recovery,
                SettlementFailureOutcome {
                    requeued: 0,
                    evicted: 1,
                },
                "{kind} must retain the existing eviction bound",
            );
            assert!(pool.is_empty());
        }
    }

    #[test]
    fn settlement_failure_limit_evicts_current_and_queued_nonce_suffix() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xa);
        let mut root = held(sender, Direction::Inbound, 0, 1);
        root.attempts = MAX_BUNDLE_ATTEMPTS - 1;
        let in_flight_suffix = held(sender, Direction::Inbound, 1, 2);
        let queued_suffix = held(sender, Direction::Inbound, 2, 3);
        let opposite_direction = held(sender, Direction::Outbound, 0, 4);
        let independent = held(Address::repeat_byte(0xb), Direction::Inbound, 0, 5);
        pool.push_contiguous(root, 0).unwrap();
        pool.push_contiguous(in_flight_suffix, 0).unwrap();
        pool.push_contiguous(queued_suffix, 0).unwrap();
        let failed = pool.pop_n(2);
        pool.push_contiguous(opposite_direction.clone(), 0).unwrap();
        pool.push_contiguous(independent.clone(), 0).unwrap();

        let recovery =
            recover_settlement_failure(&pool, 1, failed, SettlementFailureSource::Prover);
        assert_eq!(
            recovery,
            SettlementFailureOutcome {
                requeued: 0,
                evicted: 3,
            }
        );
        let remaining = pool.pop_all();
        assert_eq!(
            remaining.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![opposite_direction.hash, independent.hash]
        );
        pool.release_in_flight_batch(&remaining);
    }
}
