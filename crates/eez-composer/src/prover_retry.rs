//! Composer-specific retry policy and actionable proof-failure recovery.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::{B256, Bytes};
use alloy_sol_types::SolValue as _;
use eez_driver::RollupTiming;
use eez_l1::BundleTarget;
use eez_prover::{
    ActionableProverFailure, Prover, ProverError, ProvingContext, RetryableProverError,
};
use reth_primitives_traits::SignedTransaction;
use tokio::time::{Instant, sleep_until, timeout};
use tracing::{Level, event};

use crate::{Direction, HeldTx};

const INITIAL_PROVER_RETRY_BACKOFF_MS: u64 = 100;
const MAX_PROVER_RETRY_BACKOFF_MS: u64 = 1_000;

/// Resolve a request-validated proof failure to the held transaction that
/// produced it.
pub(crate) fn actionable_held_tx<'a>(
    failure: ActionableProverFailure,
    survivors: &'a [HeldTx],
    outbound_entry_count: usize,
    inbound_compositions: &[(eez_protocol::Composition, B256)],
) -> Option<&'a HeldTx> {
    let (direction, held_hash) = match failure {
        ActionableProverFailure::Outbound {
            transaction_hash, ..
        } => (Direction::Outbound, transaction_hash),
        ActionableProverFailure::Inbound { entry_index, .. } => {
            // PostBatch order is `[anchor | outbound... | inbound...]`.
            let mut inbound_index = entry_index.checked_sub(1 + outbound_entry_count)?;
            let mut owner = None;
            for (composition, held_hash) in inbound_compositions {
                let entry_count = composition.source.batch.entries.len();
                if inbound_index < entry_count {
                    owner = Some(*held_hash);
                    break;
                }
                inbound_index -= entry_count;
            }
            (Direction::Inbound, owner?)
        }
    };
    survivors
        .iter()
        .find(|tx| tx.direction == direction && tx.hash == held_hash)
}

pub(crate) fn partition_retryable(
    survivors: Vec<HeldTx>,
    poison: &HeldTx,
) -> (Vec<HeldTx>, Vec<HeldTx>) {
    survivors.into_iter().partition(|tx| {
        tx.sender != poison.sender || tx.direction != poison.direction || tx.nonce < poison.nonce
    })
}

/// Verify both references in an actionable failure against the exact request
/// before allowing Composer state to change.
///
/// An honest, version-compatible prover answering the current request should
/// always pass this gate. Failure means the details are malformed, stale,
/// cross-request, malicious, or disagree with Composer's request encoding; in
/// every case the hint remains non-actionable. The caller may count the request
/// as an opaque prover rejection, but must not use the hint to select pool entries.
pub(crate) fn validate_actionable_prover_failure(
    failure: ActionableProverFailure,
    batch: &eez_protocol::EvmBatch,
    sync_block: Option<&reth_primitives_traits::RecoveredBlock<reth_ethereum_primitives::Block>>,
) -> Result<(), String> {
    match failure {
        ActionableProverFailure::Outbound {
            transaction_index,
            transaction_hash,
        } => {
            let block =
                sync_block.ok_or("outbound proof failure has no in-memory terminal Sync block")?;
            let transaction = block
                .body()
                .transactions()
                .nth(transaction_index)
                .ok_or_else(|| {
                    format!(
                        "outbound proof failure transaction index {transaction_index} is out of range"
                    )
                })?;
            let actual = transaction.recalculate_hash();
            if actual != transaction_hash {
                return Err(format!(
                    "outbound proof failure hash {transaction_hash} does not match transaction \
                     {transaction_index} hash {actual}"
                ));
            }
        }
        ActionableProverFailure::Inbound {
            entry_index,
            entry_hash,
        } => {
            let entry = batch.entries.get(entry_index).ok_or_else(|| {
                format!("inbound proof failure entry index {entry_index} is out of range")
            })?;
            let actual = alloy_primitives::keccak256(entry.abi_encode());
            if actual != entry_hash {
                return Err(format!(
                    "inbound proof failure hash {entry_hash} does not match entry {entry_index} \
                     hash {actual}"
                ));
            }
        }
    }
    Ok(())
}

fn unix_time_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Latest wall-clock millisecond at which a full proof attempt may start.
///
/// The Sequencer applies the same budget before entering the Composer. Repeating
/// it here closes the race introduced by retry backoff: the proof budget is
/// reserved before the relay-submission slack, so a retry cannot consume either.
fn proof_start_cutoff_ms(timing: RollupTiming, target: BundleTarget) -> Option<u64> {
    let BundleTarget::Exact { timestamp, .. } = target else {
        return None;
    };
    let proof_ms = u64::try_from(timing.proof_time().as_millis()).unwrap_or(u64::MAX);
    let slack_ms = u64::try_from(timing.submission_slack().as_millis()).unwrap_or(u64::MAX);
    Some(
        timestamp
            .saturating_mul(1_000)
            .saturating_sub(slack_ms)
            .saturating_sub(proof_ms),
    )
}

/// Exponential backoff with ±20% wall-clock jitter and a fixed cap.
fn prover_retry_backoff(failed_attempt: u32) -> Duration {
    let jitter_seed = u64::from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    );
    prover_retry_backoff_with_seed(failed_attempt, jitter_seed)
}

fn prover_retry_backoff_with_seed(failed_attempt: u32, jitter_seed: u64) -> Duration {
    let shift = failed_attempt.saturating_sub(1).min(31);
    let base_ms = (INITIAL_PROVER_RETRY_BACKOFF_MS << shift).min(MAX_PROVER_RETRY_BACKOFF_MS);
    let jitter = base_ms / 5;
    let offset = jitter_seed % (jitter * 2 + 1);
    Duration::from_millis(base_ms - jitter + offset)
}

fn retry_budget(
    timing: RollupTiming,
    target: BundleTarget,
    now_ms: u64,
) -> Result<Duration, ProverError> {
    match proof_start_cutoff_ms(timing, target) {
        Some(cutoff_ms) if now_ms > cutoff_ms => Err(ProverError::Retryable {
            kind: RetryableProverError::DeadlineExceeded,
            message: "Composer proof-attempt cutoff reached".to_owned(),
        }),
        Some(cutoff_ms) => Ok(Duration::from_millis(cutoff_ms - now_ms)),
        None => Ok(timing.proof_time()),
    }
}

/// Run one complete proving operation with the Composer profile's closed
/// retryable-error allowlist. Every attempt is independently bounded by the
/// configured proof-time budget.
pub(crate) async fn prove_with_retry(
    prover: &dyn Prover,
    ctx: ProvingContext,
    timing: RollupTiming,
    target: BundleTarget,
) -> Result<Bytes, ProverError> {
    prove_with_retry_at(prover, ctx, timing, target, unix_time_millis()).await
}

async fn prove_with_retry_at(
    prover: &dyn Prover,
    ctx: ProvingContext,
    timing: RollupTiming,
    target: BundleTarget,
    now_ms: u64,
) -> Result<Bytes, ProverError> {
    let budget = retry_budget(timing, target, now_ms)?;
    let retry_deadline = Instant::now() + budget;
    let mut attempt = 1_u32;

    loop {
        let attempt_timeout = match target {
            BundleTarget::NextBlock => retry_deadline.saturating_duration_since(Instant::now()),
            BundleTarget::Exact { .. } => timing.proof_time(),
        };
        let result = match timeout(attempt_timeout, prover.prove(ctx.clone())).await {
            Ok(result) => result,
            Err(_) => Err(ProverError::Retryable {
                kind: RetryableProverError::DeadlineExceeded,
                message: format!("Composer proof attempt exceeded its {attempt_timeout:?} budget"),
            }),
        };
        let error = match result {
            Ok(proof) => return Ok(proof),
            Err(error) => error,
        };
        let Some(kind) = error.retryable_kind() else {
            return Err(error);
        };

        let wake_at = Instant::now() + prover_retry_backoff(attempt);
        if wake_at >= retry_deadline {
            event!(
                name: "eez.composer.prover.retry_cutoff",
                Level::WARN,
                attempt,
                ?kind,
                ?target,
                "not retrying proof because its retry budget is exhausted",
            );
            return Err(error);
        }

        event!(
            name: "eez.composer.prover.retry",
            Level::WARN,
            attempt,
            next_attempt = attempt + 1,
            ?kind,
            ?target,
            "retrying complete proof request after transient failure",
        );
        sleep_until(wake_at).await;
        // The planned wake was safe, but the runtime may resume this task late.
        if Instant::now() >= retry_deadline {
            event!(
                name: "eez.composer.prover.retry_cutoff",
                Level::WARN,
                attempt,
                ?kind,
                ?target,
                "proof retry budget exhausted while waiting",
            );
            return Err(error);
        }
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_consensus::SignableTransaction as _;
    use alloy_primitives::{Address, TxHash};
    use async_trait::async_trait;

    use super::*;

    fn held(sender: Address, direction: Direction, nonce: u64, hash_byte: u8) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![hash_byte; 4]),
            hash: TxHash::repeat_byte(hash_byte),
            attempts: 0,
            max_fee_per_gas: u128::from(hash_byte),
            priority_fee_per_gas: u128::from(hash_byte),
            sender,
            nonce,
            direction,
        }
    }

    #[test]
    fn actionable_failures_resolve_direction_specific_held_identities() {
        fn composition(entry_count: usize) -> eez_protocol::Composition {
            eez_protocol::Composition {
                source: eez_protocol::SourceComposition {
                    rollup_id: eez_protocol::RollupId(1),
                    batch: eez_protocol::EvmBatch {
                        entries: vec![Default::default(); entry_count],
                        ..Default::default()
                    },
                },
                targets: Vec::new(),
            }
        }

        let sender = Address::repeat_byte(0xc);
        let outbound = held(sender, Direction::Outbound, 1, 1);
        let inbound = held(sender, Direction::Inbound, 1, 2);
        let second_inbound = held(Address::repeat_byte(0xd), Direction::Inbound, 0, 3);
        let survivors = vec![outbound.clone(), inbound.clone(), second_inbound.clone()];
        let inbound_compositions = vec![
            (composition(2), inbound.hash),
            (composition(1), second_inbound.hash),
        ];

        let resolved = actionable_held_tx(
            ActionableProverFailure::Outbound {
                transaction_index: 3,
                transaction_hash: outbound.hash,
            },
            &survivors,
            2,
            &inbound_compositions,
        )
        .unwrap();
        assert_eq!(resolved.hash, outbound.hash);

        // [anchor, outbound 0, outbound 1, inbound 0, inbound 1, inbound 2]
        let resolved = actionable_held_tx(
            ActionableProverFailure::Inbound {
                entry_index: 4,
                entry_hash: B256::repeat_byte(0xee),
            },
            &survivors,
            2,
            &inbound_compositions,
        )
        .unwrap();
        assert_eq!(resolved.hash, inbound.hash);

        assert!(
            actionable_held_tx(
                ActionableProverFailure::Inbound {
                    entry_index: 2,
                    entry_hash: B256::repeat_byte(0xee),
                },
                &survivors,
                2,
                &inbound_compositions,
            )
            .is_none()
        );
    }

    #[test]
    fn proof_ejection_removes_only_the_same_direction_nonce_suffix() {
        let sender = Address::repeat_byte(0xc);
        let poison = held(sender, Direction::Inbound, 2, 2);
        let survivors = vec![
            held(sender, Direction::Inbound, 1, 1),
            poison.clone(),
            held(sender, Direction::Inbound, 3, 3),
            held(sender, Direction::Outbound, 3, 4),
            held(Address::repeat_byte(0xd), Direction::Inbound, 3, 5),
        ];

        let (retry, evicted) = partition_retryable(survivors, &poison);

        assert_eq!(
            retry.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![
                TxHash::repeat_byte(1),
                TxHash::repeat_byte(4),
                TxHash::repeat_byte(5),
            ]
        );
        assert_eq!(
            evicted.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![TxHash::repeat_byte(2), TxHash::repeat_byte(3)]
        );
    }

    #[test]
    fn actionable_references_must_match_the_exact_proving_request() {
        let transaction: reth_ethereum_primitives::TransactionSigned =
            alloy_consensus::TxLegacy::default()
                .into_signed(alloy_primitives::Signature::test_signature())
                .into();
        let transaction_hash = transaction.recalculate_hash();
        let body: reth_ethereum_primitives::BlockBody = alloy_consensus::BlockBody {
            transactions: vec![transaction],
            ..Default::default()
        };
        let block = reth_primitives_traits::RecoveredBlock::new_unhashed(
            reth_ethereum_primitives::Block::new(Default::default(), body),
            vec![Address::ZERO],
        );

        let mut batch = eez_protocol::EvmBatch::default();
        batch
            .entries
            .push(eez_protocol::abi::ExecutionEntrySol::default());
        let entry_hash = alloy_primitives::keccak256(batch.entries[0].abi_encode());

        assert!(
            validate_actionable_prover_failure(
                ActionableProverFailure::Outbound {
                    transaction_index: 0,
                    transaction_hash,
                },
                &batch,
                Some(&block),
            )
            .is_ok()
        );
        assert!(
            validate_actionable_prover_failure(
                ActionableProverFailure::Inbound {
                    entry_index: 0,
                    entry_hash,
                },
                &batch,
                Some(&block),
            )
            .is_ok()
        );
        assert!(
            validate_actionable_prover_failure(
                ActionableProverFailure::Outbound {
                    transaction_index: 0,
                    transaction_hash: B256::repeat_byte(0xff),
                },
                &batch,
                Some(&block),
            )
            .is_err()
        );
        assert!(
            validate_actionable_prover_failure(
                ActionableProverFailure::Inbound {
                    entry_index: 1,
                    entry_hash,
                },
                &batch,
                Some(&block),
            )
            .is_err()
        );
    }

    #[derive(Debug)]
    struct ScriptedProver {
        outcomes: Mutex<VecDeque<Result<Bytes, ProverError>>>,
        calls: AtomicUsize,
    }

    impl ScriptedProver {
        fn new(outcomes: Vec<Result<Bytes, ProverError>>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes.into()),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl Prover for ScriptedProver {
        async fn prove(&self, _ctx: ProvingContext) -> Result<Bytes, ProverError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted prover ran out of outcomes")
        }

        fn vkey(&self) -> B256 {
            B256::ZERO
        }
    }

    #[derive(Debug, Default)]
    struct TimeoutProver {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Prover for TimeoutProver {
        async fn prove(&self, _ctx: ProvingContext) -> Result<Bytes, ProverError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::future::pending().await
        }

        fn vkey(&self) -> B256 {
            B256::ZERO
        }
    }

    #[derive(Debug, Default)]
    struct AlwaysRetryableProver {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Prover for AlwaysRetryableProver {
        async fn prove(&self, _ctx: ProvingContext) -> Result<Bytes, ProverError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(retryable(RetryableProverError::Unavailable))
        }

        fn vkey(&self) -> B256 {
            B256::ZERO
        }
    }

    #[derive(Debug, Default)]
    struct DelayedFailureThenStallProver {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Prover for DelayedFailureThenStallProver {
        async fn prove(&self, _ctx: ProvingContext) -> Result<Bytes, ProverError> {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(300)).await;
                return Err(retryable(RetryableProverError::Unavailable));
            }
            std::future::pending().await
        }

        fn vkey(&self) -> B256 {
            B256::ZERO
        }
    }

    fn retryable(kind: RetryableProverError) -> ProverError {
        ProverError::Retryable {
            kind,
            message: "transient test failure".to_owned(),
        }
    }

    fn test_timing() -> RollupTiming {
        RollupTiming::new(2_000, 1_000, 500, 100)
    }

    const TEST_NOW_MS: u64 = 1_700_000_000_000;

    #[tokio::test(start_paused = true)]
    async fn retryable_prover_error_retries_the_complete_operation() {
        let prover = ScriptedProver::new(vec![
            Err(retryable(RetryableProverError::Unavailable)),
            Ok(Bytes::from_static(b"proof")),
        ]);

        let proof = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap();

        assert_eq!(proof, Bytes::from_static(b"proof"));
        assert_eq!(prover.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn non_retryable_prover_error_is_attempted_once() {
        let prover = ScriptedProver::new(vec![Err(ProverError::Backend(
            "fatal test failure".to_owned(),
        ))]);

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ProverError::Backend(_)));
        assert_eq!(prover.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn actionable_prover_error_is_returned_without_retrying_same_request() {
        let failure = ActionableProverFailure::Outbound {
            transaction_index: 3,
            transaction_hash: B256::repeat_byte(0xaa),
        };
        let prover = ScriptedProver::new(vec![Err(ProverError::Actionable {
            failure,
            message: "outbound transaction failed validation".to_owned(),
        })]);

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert_eq!(error.actionable_failure(), Some(failure));
        assert_eq!(prover.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_slot_cutoff_starts_no_proof_attempt() {
        let prover = ScriptedProver::new(vec![Ok(Bytes::from_static(b"unexpected"))]);

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::Exact {
                block: 1,
                timestamp: 1,
            },
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::DeadlineExceeded)
        );
        assert_eq!(prover.calls(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_attempt_exhausts_the_next_block_retry_window() {
        let prover = TimeoutProver::default();

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::DeadlineExceeded)
        );
        assert_eq!(prover.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn next_block_retry_attempt_uses_only_the_remaining_budget() {
        let timing = RollupTiming::new(3_000, 1_000, 1_000, 100);
        let prover = DelayedFailureThenStallProver::default();
        let started = Instant::now();

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            timing,
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::DeadlineExceeded)
        );
        assert_eq!(prover.calls.load(Ordering::Relaxed), 2);
        assert_eq!(Instant::now().duration_since(started), timing.proof_time());
    }

    #[tokio::test(start_paused = true)]
    async fn next_block_retries_until_its_total_wait_budget_is_exhausted() {
        let timing = RollupTiming::new(5_000, 1_000, 2_000, 100);
        let prover = AlwaysRetryableProver::default();
        let started = Instant::now();

        let error = prove_with_retry_at(
            &prover,
            ProvingContext::default(),
            timing,
            BundleTarget::NextBlock,
            TEST_NOW_MS,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::Unavailable)
        );
        let elapsed = Instant::now().duration_since(started);
        assert!(
            elapsed >= Duration::from_millis(INITIAL_PROVER_RETRY_BACKOFF_MS * 4 / 5),
            "retrying should include a real backoff rather than hot-looping"
        );
        assert!(elapsed < timing.proof_time());
        assert!(
            prover.calls.load(Ordering::Relaxed) > 3,
            "the retry window should outlive the old three-attempt cap"
        );
    }

    #[test]
    fn backoff_stays_within_twenty_percent_of_its_capped_exponential_base() {
        for attempt in 1_u32..=40 {
            let shift = attempt.saturating_sub(1).min(31);
            let base_ms =
                (INITIAL_PROVER_RETRY_BACKOFF_MS << shift).min(MAX_PROVER_RETRY_BACKOFF_MS);
            for seed in [0, 1, u64::MAX / 2, u64::MAX] {
                let delay_ms =
                    u64::try_from(prover_retry_backoff_with_seed(attempt, seed).as_millis())
                        .unwrap();
                assert!(
                    delay_ms >= base_ms * 4 / 5,
                    "attempt {attempt}, seed {seed}"
                );
                assert!(
                    delay_ms <= base_ms * 6 / 5,
                    "attempt {attempt}, seed {seed}"
                );
            }
        }
    }

    #[test]
    fn proof_cutoff_reserves_proof_time_and_submission_slack() {
        let timing = RollupTiming::new(12_000, 2_000, 4_000, 1_500);
        assert_eq!(
            proof_start_cutoff_ms(
                timing,
                BundleTarget::Exact {
                    block: 7,
                    timestamp: 100,
                }
            ),
            Some(94_500)
        );
        assert_eq!(proof_start_cutoff_ms(timing, BundleTarget::NextBlock), None);
    }
}
