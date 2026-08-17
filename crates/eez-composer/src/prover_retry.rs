//! Composer-specific retry policy for complete proving operations.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::Bytes;
use eez_driver::RollupTiming;
use eez_l1::BundleTarget;
use eez_prover::{Prover, ProverError, ProvingContext, RetryableProverError};
use tokio::time::{sleep, timeout};
use tracing::{Level, event};

/// A complete proving operation gets one initial attempt and at most two
/// retries. The slot deadline remains the stronger bound for pinned batches.
const MAX_PROVER_ATTEMPTS: u32 = 3;
const INITIAL_PROVER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
const MAX_PROVER_RETRY_BACKOFF: Duration = Duration::from_secs(1);

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

fn unix_time_millis() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Exponential backoff with ±20% wall-clock jitter and a fixed cap.
fn prover_retry_backoff(failed_attempt: u32) -> Duration {
    let shift = failed_attempt.saturating_sub(1).min(31);
    let base_ms = u64::try_from(INITIAL_PROVER_RETRY_BACKOFF.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(1u64 << shift)
        .min(u64::try_from(MAX_PROVER_RETRY_BACKOFF.as_millis()).unwrap_or(u64::MAX));
    let jitter = base_ms / 5;
    let width = jitter.saturating_mul(2).saturating_add(1);
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let offset = u64::from(seed) % width;
    Duration::from_millis(base_ms.saturating_sub(jitter).saturating_add(offset))
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
    let cutoff_ms = proof_start_cutoff_ms(timing, target);
    for attempt in 1..=MAX_PROVER_ATTEMPTS {
        if cutoff_ms.is_some_and(|cutoff| unix_time_millis() > cutoff) {
            return Err(ProverError::Retryable {
                kind: RetryableProverError::DeadlineExceeded,
                message: "Composer proof-attempt cutoff reached".to_owned(),
            });
        }

        let result = match timeout(timing.proof_time(), prover.prove(ctx.clone())).await {
            Ok(result) => result,
            Err(_) => Err(ProverError::Retryable {
                kind: RetryableProverError::DeadlineExceeded,
                message: format!(
                    "Composer proof attempt exceeded its {:?} budget",
                    timing.proof_time()
                ),
            }),
        };
        let error = match result {
            Ok(proof) => return Ok(proof),
            Err(error) => error,
        };
        let Some(kind) = error.retryable_kind() else {
            return Err(error);
        };
        if attempt == MAX_PROVER_ATTEMPTS {
            return Err(error);
        }

        let delay = prover_retry_backoff(attempt);
        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        if cutoff_ms.is_some_and(|cutoff| unix_time_millis().saturating_add(delay_ms) > cutoff) {
            event!(
                name: "eez.composer.prover.retry_cutoff",
                Level::WARN,
                attempt,
                ?kind,
                delay_ms,
                cutoff_ms,
                "not retrying proof because the next complete attempt would miss the settlement cutoff",
            );
            return Err(error);
        }
        event!(
            name: "eez.composer.prover.retry",
            Level::WARN,
            attempt,
            next_attempt = attempt + 1,
            ?kind,
            delay_ms,
            "retrying complete proof request after transient failure",
        );
        sleep(delay).await;
    }
    unreachable!("MAX_PROVER_ATTEMPTS is nonzero")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_primitives::B256;
    use async_trait::async_trait;

    use super::*;

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

    fn retryable(kind: RetryableProverError) -> ProverError {
        ProverError::Retryable {
            kind,
            message: "transient test failure".to_owned(),
        }
    }

    fn test_timing() -> RollupTiming {
        RollupTiming::new(2_000, 1_000, 500, 100)
    }

    #[tokio::test]
    async fn retryable_prover_error_retries_the_complete_operation() {
        let prover = ScriptedProver::new(vec![
            Err(retryable(RetryableProverError::Unavailable)),
            Ok(Bytes::from_static(b"proof")),
        ]);

        let proof = prove_with_retry(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
        )
        .await
        .unwrap();

        assert_eq!(proof, Bytes::from_static(b"proof"));
        assert_eq!(prover.calls(), 2);
    }

    #[tokio::test]
    async fn non_retryable_prover_error_is_attempted_once() {
        let prover = ScriptedProver::new(vec![Err(ProverError::Backend(
            "fatal test failure".to_owned(),
        ))]);

        let error = prove_with_retry(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::NextBlock,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ProverError::Backend(_)));
        assert_eq!(prover.calls(), 1);
    }

    #[tokio::test]
    async fn expired_slot_cutoff_starts_no_proof_attempt() {
        let prover = ScriptedProver::new(vec![Ok(Bytes::from_static(b"unexpected"))]);

        let error = prove_with_retry(
            &prover,
            ProvingContext::default(),
            test_timing(),
            BundleTarget::Exact {
                block: 1,
                timestamp: 1,
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::DeadlineExceeded)
        );
        assert_eq!(prover.calls(), 0);
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
