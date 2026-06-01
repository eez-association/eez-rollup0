//! Per-rollup wall-clock timing configuration.
//!
//! [`RollupTiming`] is a `Copy` value type carrying four input fields:
//!
//! - `l1_block_time_ms` — L1 cadence (12 000 mainnet, 5 000 chiado).
//! - `l2_block_time_ms` — L2 cadence (2 000 mainnet target, 2 500
//!   chiado-slow, 1 000 chiado-fast).
//! - `proof_time_ms` — worst-case prover wall-clock budget.
//! - `submission_slack_ms` — relay propagation buffer (default 100 ms).
//!
//! Derived helpers ([`Self::k`], [`Self::future_count`], [`Self::live_count`],
//! [`Self::per_trigger_composition`]) compute the per-sync-slot block layout
//! deterministically from these four fields. No I/O, no allocation; the
//! Sequencer calls them once per trigger.
//!
//! Validation ([`Self::validate`]) refuses any of: non-integer K, K < 2,
//! `proof_time + slack ≥ l1_block_time`, `proof_time > (K-1) * l2_block_time`,
//! or any zero field. Hard error at startup per `invariant 7` — the
//! Sequencer refuses to start on misconfig rather than producing surprising
//! block patterns.
//!
//! Per-rollup placement: stored inside each `RollupState` in the
//! `eez-composer` umbrella. Different L2s on the same composer can run
//! different timings.
//!
//! # Worked examples
//!
//! | Deployment | L1 (ms) | L2 (ms) | proof (ms) | K | future_count | live_count |
//! |---|---|---|---|---|---|---|
//! | mainnet    | 12 000  | 2 000   | 4 000      | 6 | 1            | 4          |
//! | chiado-slow| 5 000   | 2 500   | 2 500      | 2 | 0            | 1          |
//! | chiado-fast| 5 000   | 1 000   | 2 000      | 5 | 1            | 3          |

use std::env;
use std::num::ParseIntError;
use std::time::Duration;

use crate::error::{DriverError, DriverResult};

/// Cap on Live blocks produced per catchup trigger. Aligned with
/// `MAX_BLOCKS_PER_BATCH` on the Composer side so one trigger's
/// catchup output maps to exactly one postBatch submission. Drop to
/// 100 if 300 turns out to produce calldata-gas pressure in practice.
pub const MAX_BLOCKS_PER_CATCHUP: u64 = 300;

const ENV_L1_BLOCK_TIME_MS: &str = "EEZ_L1_BLOCK_TIME_MS";
const ENV_L2_BLOCK_TIME_MS: &str = "EEZ_L2_BLOCK_TIME_MS";
const ENV_PROOF_TIME_MS: &str = "EEZ_PROOF_TIME_MS";
const ENV_SUBMISSION_SLACK_MS: &str = "EEZ_SUBMISSION_SLACK_MS";

const DEFAULT_SUBMISSION_SLACK_MS: u32 = 100;

/// Per-rollup timing configuration. Cheap `Copy` value type (four
/// `u32` fields).
//
// `_ms` postfixes are intentional: each field carries milliseconds at
// the raw-arithmetic layer (env parsing + integer validation in
// `validate()`). The `Duration` accessors below — `l1_block_time()`,
// `l2_block_time()`, etc — are the typed view. Dropping `_ms` would
// lose the unit at the integer boundary and is more confusing than the
// repetition is worth.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RollupTiming {
    l1_block_time_ms: u32,
    l2_block_time_ms: u32,
    proof_time_ms: u32,
    submission_slack_ms: u32,
}

impl RollupTiming {
    /// Construct from raw values. Does not validate — call
    /// [`Self::validate`] before relying on derived methods, or use
    /// [`Self::from_env`] which validates as part of loading.
    #[must_use]
    pub const fn new(
        l1_block_time_ms: u32,
        l2_block_time_ms: u32,
        proof_time_ms: u32,
        submission_slack_ms: u32,
    ) -> Self {
        Self {
            l1_block_time_ms,
            l2_block_time_ms,
            proof_time_ms,
            submission_slack_ms,
        }
    }

    /// Read fields from `EEZ_L1_BLOCK_TIME_MS`, `EEZ_L2_BLOCK_TIME_MS`,
    /// `EEZ_PROOF_TIME_MS`, `EEZ_SUBMISSION_SLACK_MS` (the slack
    /// defaults to 100 ms if unset). Runs [`Self::validate`] on the
    /// result.
    ///
    /// # Errors
    ///
    /// Returns [`DriverError::is_timing_config`] for any missing
    /// required var, malformed value, or validation failure.
    pub fn from_env() -> DriverResult<Self> {
        let t = Self::new(
            parse_env(ENV_L1_BLOCK_TIME_MS)?,
            parse_env(ENV_L2_BLOCK_TIME_MS)?,
            parse_env(ENV_PROOF_TIME_MS)?,
            parse_env_or(ENV_SUBMISSION_SLACK_MS, DEFAULT_SUBMISSION_SLACK_MS)?,
        );
        t.validate()?;
        Ok(t)
    }

    /// Standalone-dev default: mainnet-shaped (L1=12s, L2=2s, proof=4s,
    /// slack=100ms). Used when no L1 stack is configured — only the
    /// `l2_block_time()` accessor is meaningful in that path; the
    /// other fields exist for type completeness. Production deployments
    /// MUST use [`Self::from_env`] so misconfig is loud.
    #[must_use]
    pub const fn standalone_default() -> Self {
        Self::new(12_000, 2_000, 4_000, 100)
    }

    /// Verify the invariants from §5.4.4. Hard error on violation;
    /// `eez-node` startup refuses to spawn the Sequencer rather than
    /// producing surprising block patterns from a misconfigured set.
    ///
    /// # Errors
    ///
    /// Returns [`DriverError::is_timing_config`] for any of:
    /// - any zero field
    /// - `L1_block_time % L2_block_time != 0` (K not integer)
    /// - `K < 2` (Sync slot must be distinct from surrounding Lives)
    /// - `proof_time + submission_slack >= L1_block_time`
    /// - `proof_time > (K - 1) * L2_block_time` (no room for Future
    ///   blocks before Sync)
    pub fn validate(&self) -> DriverResult<()> {
        if self.l1_block_time_ms == 0 || self.l2_block_time_ms == 0 || self.proof_time_ms == 0 {
            return Err(DriverError::timing_config(
                "all timing fields (l1_block_time, l2_block_time, proof_time) must be > 0",
            ));
        }
        if self.l1_block_time_ms % self.l2_block_time_ms != 0 {
            return Err(DriverError::timing_config(format!(
                "L1 block time ({} ms) must be an integer multiple of L2 block time ({} ms); K must be integer",
                self.l1_block_time_ms, self.l2_block_time_ms,
            )));
        }
        let k = self.k();
        if k < 2 {
            return Err(DriverError::timing_config(format!(
                "K must be >= 2 (got {k}); Sync slot must be distinct from surrounding Live blocks"
            )));
        }
        if self.proof_time_ms.saturating_add(self.submission_slack_ms) >= self.l1_block_time_ms {
            return Err(DriverError::timing_config(format!(
                "proof_time ({} ms) + submission_slack ({} ms) must be < L1 block time ({} ms)",
                self.proof_time_ms, self.submission_slack_ms, self.l1_block_time_ms,
            )));
        }
        let max_proof = (k - 1) * self.l2_block_time_ms;
        if self.proof_time_ms > max_proof {
            return Err(DriverError::timing_config(format!(
                "proof_time ({} ms) must be <= (K-1) * L2 block time ({} * {} = {} ms); else there is no room for Future blocks before Sync",
                self.proof_time_ms,
                k - 1,
                self.l2_block_time_ms,
                max_proof,
            )));
        }
        Ok(())
    }

    /// L1 block time as a [`Duration`].
    #[must_use]
    pub const fn l1_block_time(self) -> Duration {
        Duration::from_millis(self.l1_block_time_ms as u64)
    }

    /// L2 block time as a [`Duration`].
    #[must_use]
    pub const fn l2_block_time(self) -> Duration {
        Duration::from_millis(self.l2_block_time_ms as u64)
    }

    /// Prover wall-clock budget as a [`Duration`].
    #[must_use]
    pub const fn proof_time(self) -> Duration {
        Duration::from_millis(self.proof_time_ms as u64)
    }

    /// Relay propagation buffer as a [`Duration`].
    #[must_use]
    pub const fn submission_slack(self) -> Duration {
        Duration::from_millis(self.submission_slack_ms as u64)
    }

    /// L2 blocks per sync slot. `K = L1_block_time / L2_block_time`.
    #[must_use]
    pub const fn k(self) -> u32 {
        self.l1_block_time_ms / self.l2_block_time_ms
    }

    /// Future blocks per slot (proof-window padding ahead of Sync).
    #[must_use]
    pub const fn future_count(self) -> u32 {
        (self.proof_time_ms / self.l2_block_time_ms).saturating_sub(1)
    }

    /// Live blocks per slot — the L2 blocks already produced on
    /// wall-clock cadence by the time the trigger fires.
    #[must_use]
    pub const fn live_count(self) -> u32 {
        self.k() - self.future_count() - 1
    }

    /// Wall-clock offset from "L1 block N landed" at which the
    /// Scheduler should fire so the prover has `proof_time` budget.
    #[must_use]
    pub const fn proof_window_open(self) -> Duration {
        Duration::from_millis((self.l1_block_time_ms - self.proof_time_ms) as u64)
    }

    /// Wall-clock offset from "L1 block N landed" by which the
    /// postBatch bundle must reach the relay
    /// (`L1_block_time - submission_slack`).
    #[must_use]
    pub const fn submission_deadline(self) -> Duration {
        Duration::from_millis((self.l1_block_time_ms - self.submission_slack_ms) as u64)
    }

    /// Decide what to produce at a sync-slot trigger given the current
    /// L2 head and the target sync-slot block height.
    ///
    /// Catchup vs. steady-state split: if closing the slot in this
    /// trigger would require more than `K` blocks, drop into Catchup
    /// mode (Live-only, capped at [`MAX_BLOCKS_PER_CATCHUP`]); else
    /// produce the slot suffix (Live + Future + 1 Sync).
    ///
    /// Returns [`SlotComposition::Idle`] if the head is already at or
    /// past `sync_slot_block` — a trigger should not produce that
    /// outcome under normal flow; it indicates a late-firing trigger
    /// or a recent reorg that overshot.
    #[must_use]
    pub fn per_trigger_composition(self, head_block: u64, sync_slot_block: u64) -> SlotComposition {
        if head_block >= sync_slot_block {
            return SlotComposition::Idle;
        }

        let future = u64::from(self.future_count());
        let k = u64::from(self.k());
        let live_region_end = sync_slot_block.saturating_sub(future + 1);

        if head_block < live_region_end {
            // Below Live region. Steady-state if we can close the
            // slot this trigger; Catchup otherwise.
            let live_to_produce = live_region_end - head_block;
            // K - (future + 1) = max Live blocks per steady-state slot.
            if live_to_produce <= k.saturating_sub(future + 1) {
                SlotComposition::Slot {
                    live: live_to_produce,
                    future,
                }
            } else {
                SlotComposition::Catchup {
                    live: live_to_produce.min(MAX_BLOCKS_PER_CATCHUP),
                }
            }
        } else {
            // Inside the Future region already — produce the remainder
            // of the slot suffix. This happens if a previous trigger
            // overshot.
            let in_future = head_block - live_region_end;
            let remaining_future = future.saturating_sub(in_future);
            SlotComposition::Slot {
                live: 0,
                future: remaining_future,
            }
        }
    }
}

/// Output of [`RollupTiming::per_trigger_composition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotComposition {
    /// Already at or past the target sync slot. No blocks to produce
    /// this trigger.
    Idle,
    /// Catchup: too far behind to close the slot. Produce `live` Live
    /// blocks only; Future + Sync are deferred to the next trigger.
    Catchup { live: u64 },
    /// Steady-state: produce `live` Live blocks + `future` Future
    /// blocks + 1 Sync block (always implicit when this variant is
    /// returned).
    Slot { live: u64, future: u64 },
}

fn parse_env(name: &str) -> DriverResult<u32> {
    let raw =
        env::var(name).map_err(|_| DriverError::timing_config(format!("{name} is required")))?;
    raw.parse::<u32>()
        .map_err(|e: ParseIntError| DriverError::timing_config(format!("{name}: {e}")))
}

fn parse_env_or(name: &str, default: u32) -> DriverResult<u32> {
    match env::var(name) {
        Ok(v) => v
            .parse::<u32>()
            .map_err(|e: ParseIntError| DriverError::timing_config(format!("{name}: {e}"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(DriverError::timing_config(format!(
            "{name} contains non-UTF-8 bytes"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mainnet() -> RollupTiming {
        RollupTiming::new(12_000, 2_000, 4_000, 100)
    }

    fn chiado_slow() -> RollupTiming {
        RollupTiming::new(5_000, 2_500, 2_500, 100)
    }

    fn chiado_fast() -> RollupTiming {
        RollupTiming::new(5_000, 1_000, 2_000, 100)
    }

    #[test]
    fn mainnet_validates() {
        mainnet().validate().expect("mainnet config is valid");
    }

    #[test]
    fn mainnet_derived() {
        let t = mainnet();
        assert_eq!(t.k(), 6);
        assert_eq!(t.future_count(), 1);
        assert_eq!(t.live_count(), 4);
        assert_eq!(t.proof_window_open(), Duration::from_millis(8_000));
        assert_eq!(t.submission_deadline(), Duration::from_millis(11_900));
    }

    #[test]
    fn chiado_slow_validates() {
        chiado_slow().validate().expect("chiado-slow valid");
    }

    #[test]
    fn chiado_slow_derived() {
        let t = chiado_slow();
        assert_eq!(t.k(), 2);
        assert_eq!(t.future_count(), 0);
        assert_eq!(t.live_count(), 1);
    }

    #[test]
    fn chiado_fast_validates() {
        chiado_fast().validate().expect("chiado-fast valid");
    }

    #[test]
    fn chiado_fast_derived() {
        let t = chiado_fast();
        assert_eq!(t.k(), 5);
        assert_eq!(t.future_count(), 1);
        assert_eq!(t.live_count(), 3);
    }

    #[test]
    fn non_integer_k_rejected() {
        // L1=12s, L2=5s → K = 12/5 = 2.4 (non-integer).
        let err = RollupTiming::new(12_000, 5_000, 2_000, 100)
            .validate()
            .expect_err("non-integer K should be refused");
        assert!(format!("{err}").contains("integer multiple"));
    }

    #[test]
    fn k_less_than_2_rejected() {
        // L1=2s, L2=2s → K=1.
        let err = RollupTiming::new(2_000, 2_000, 500, 100)
            .validate()
            .expect_err("K=1 should be refused");
        assert!(format!("{err}").contains("K must be >= 2"));
    }

    #[test]
    fn proof_plus_slack_must_fit_l1_block_time() {
        let err = RollupTiming::new(12_000, 2_000, 10_000, 3_000)
            .validate()
            .expect_err("proof+slack >= L1 should be refused");
        assert!(format!("{err}").contains("must be <"));
    }

    #[test]
    fn proof_time_must_fit_future_region() {
        // L1=4s, L2=2s, proof=3s, slack=100. K=2, (K-1)*L2 = 2s.
        // Passes proof+slack check (3100 < 4000) but trips
        // proof > (K-1)*L2 (3000 > 2000).
        let err = RollupTiming::new(4_000, 2_000, 3_000, 100)
            .validate()
            .expect_err("proof > (K-1)*L2 should be refused");
        assert!(format!("{err}").contains("Future blocks"));
    }

    #[test]
    fn zero_fields_rejected() {
        RollupTiming::new(0, 2_000, 4_000, 100)
            .validate()
            .expect_err("zero L1 refused");
        RollupTiming::new(12_000, 0, 4_000, 100)
            .validate()
            .expect_err("zero L2 refused");
        RollupTiming::new(12_000, 2_000, 0, 100)
            .validate()
            .expect_err("zero proof refused");
    }

    // --- per_trigger_composition ---

    #[test]
    fn mainnet_steady_state_at_live_region_end() {
        // Sync slot at block 6. live_region_end = 6 - (1+1) = 4.
        // Head = 4 → produce 0 Live + 1 Future + 1 Sync.
        let c = mainnet().per_trigger_composition(4, 6);
        assert_eq!(c, SlotComposition::Slot { live: 0, future: 1 });
    }

    #[test]
    fn mainnet_steady_state_one_live_behind() {
        let c = mainnet().per_trigger_composition(3, 6);
        assert_eq!(c, SlotComposition::Slot { live: 1, future: 1 });
    }

    #[test]
    fn mainnet_steady_state_start_of_slot() {
        // Head = 0 → 4 Live + 1 Future + 1 Sync = 6 blocks (= K).
        let c = mainnet().per_trigger_composition(0, 6);
        assert_eq!(c, SlotComposition::Slot { live: 4, future: 1 });
    }

    #[test]
    fn mainnet_catchup_one_slot_behind() {
        // Sync at 12, head at 0. live_region_end = 10. live_to_produce
        // = 10 > 4 → Catchup. Live blocks = 10 (under cap).
        let c = mainnet().per_trigger_composition(0, 12);
        assert_eq!(c, SlotComposition::Catchup { live: 10 });
    }

    #[test]
    fn mainnet_catchup_clamped_to_cap() {
        // Way behind: sync at 320, head at 0. live_region_end = 318
        // → live_to_produce = 318 → capped at MAX_BLOCKS_PER_CATCHUP = 300.
        let c = mainnet().per_trigger_composition(0, 320);
        assert_eq!(
            c,
            SlotComposition::Catchup {
                live: MAX_BLOCKS_PER_CATCHUP
            }
        );
    }

    #[test]
    fn chiado_slow_steady_state_start_of_slot() {
        // K=2, future_count=0. Sync at 2, head at 0. live_region_end=1.
        // live_to_produce=1, K-(future+1)=1. 1<=1 → steady-state.
        // Slot { live: 1, future: 0 } — 1 Live + 1 Sync.
        let c = chiado_slow().per_trigger_composition(0, 2);
        assert_eq!(c, SlotComposition::Slot { live: 1, future: 0 });
    }

    #[test]
    fn chiado_fast_steady_state_start_of_slot() {
        // K=5, future_count=1. Sync at 5, head at 0. live_region_end=3,
        // live_to_produce=3, K-(future+1)=3. 3<=3 → steady-state.
        // Slot { live: 3, future: 1 } — 3 Live + 1 Future + 1 Sync.
        let c = chiado_fast().per_trigger_composition(0, 5);
        assert_eq!(c, SlotComposition::Slot { live: 3, future: 1 });
    }

    #[test]
    fn idle_when_head_at_or_past_sync_slot() {
        assert_eq!(
            mainnet().per_trigger_composition(6, 6),
            SlotComposition::Idle
        );
        assert_eq!(
            mainnet().per_trigger_composition(7, 6),
            SlotComposition::Idle
        );
    }

    #[test]
    fn inside_future_region_produces_remainder() {
        // Mainnet: sync at 6, future_count=1. live_region_end=4.
        // Head=5 (1 block into Future region) → 0 Future remaining,
        // just Sync. Slot { live: 0, future: 0 }.
        let c = mainnet().per_trigger_composition(5, 6);
        assert_eq!(c, SlotComposition::Slot { live: 0, future: 0 });
    }
}
