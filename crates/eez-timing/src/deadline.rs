//! Deadlines for a Sync slot's proof, as pure functions of the time.

use std::time::Duration;

use crate::timing::RollupTiming;

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// True if a bundle for the Sync block at `sync_ts` (unix seconds) cannot
/// reach the relay by `sync_ts·1000 − slack` when the trigger runs at
/// `now_ms`. The proof is ready at `now_ms + proof`.
#[must_use]
pub fn misses_l1_slot(timing: RollupTiming, now_ms: u64, sync_ts: u64) -> bool {
    let ready_ms = now_ms.saturating_add(millis(timing.proof_time()));
    let deadline_ms = sync_ts
        .saturating_mul(1_000)
        .saturating_sub(millis(timing.submission_slack()));
    ready_ms > deadline_ms
}

/// Latest unix millisecond at which a full proof for a bundle pinned to
/// `timestamp` (unix seconds) may start: `timestamp·1000 − slack − proof`.
#[must_use]
pub fn proof_start_cutoff_ms(timing: RollupTiming, timestamp: u64) -> u64 {
    timestamp
        .saturating_mul(1_000)
        .saturating_sub(millis(timing.submission_slack()))
        .saturating_sub(millis(timing.proof_time()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYNC_TS: u64 = 1_700_000_012;

    #[test]
    fn lateness_boundary() {
        // proof 4000, slack 1500: the deadline is SYNC_TS·1000 − 1500.
        let t = RollupTiming::new(12_000, 2_000, 4_000, 1_500);
        let last_on_time = SYNC_TS * 1_000 - 1_500 - 4_000;
        assert!(!misses_l1_slot(t, last_on_time, SYNC_TS));
        assert!(misses_l1_slot(t, last_on_time + 1, SYNC_TS));
    }

    #[test]
    fn cutoff_boundary() {
        let t = RollupTiming::new(12_000, 2_000, 4_000, 1_500);
        let cutoff = proof_start_cutoff_ms(t, SYNC_TS);
        assert_eq!(cutoff, SYNC_TS * 1_000 - 1_500 - 4_000);
        // The cutoff is the last start the lateness check accepts.
        assert!(!misses_l1_slot(t, cutoff, SYNC_TS));
        assert!(misses_l1_slot(t, cutoff + 1, SYNC_TS));
    }
}
