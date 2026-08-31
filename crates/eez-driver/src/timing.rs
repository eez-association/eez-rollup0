//! Per-rollup wall-clock timing configuration.
//!
//! [`RollupTiming`] is a `Copy` value type carrying four input fields:
//!
//! - `l1_block_time_ms` — L1 cadence (12 000 mainnet, 5 000 chiado).
//! - `l2_block_time_ms` — L2 cadence (2 000 mainnet target, 2 500
//!   chiado-slow, 1 000 chiado-fast).
//! - `proof_time_ms` — worst-case prover wall-clock budget.
//! - `submission_slack_ms` — how early the proven bundle must reach the
//!   relay before the next L1 slot. With `proof_time`, sizes the Future
//!   region (see [`RollupTiming::future_count`]). Default 100 ms.
//!
//! Derived helpers ([`RollupTiming::k`], [`RollupTiming::future_count`],
//! [`RollupTiming::live_count`], [`RollupTiming::per_trigger_composition`])
//! compute the per-sync-slot block layout deterministically from these four
//! fields. No I/O, no allocation; the Sequencer calls them once per trigger.
//!
//! Validation ([`RollupTiming::validate`]) refuses any of: non-integer K, K < 2,
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
//! `future = ceil((proof + slack) / L2) − 1`, `live = K − future − 1`,
//! `trigger = L1 − (future + 1)·L2`:
//!
//! | Deployment  | L1 (ms) | L2 (ms) | proof | slack | K | future | live | trigger (ms) |
//! |---|---|---|---|---|---|---|---|---|
//! | mainnet     | 12 000  | 2 000   | 4 000 | 1 500 | 6 | 2      | 3    | 6 000        |
//! | chiado      | 5 000   | 1 000   | 200   | 1 700 | 5 | 1      | 3    | 3 000        |
//! | chiado-slow | 4 000   | 2 000   | 1 500 | 100   | 2 | 0      | 1    | 2 000        |

use std::env;
use std::num::ParseIntError;
use std::time::Duration;

use crate::error::{DriverError, DriverResult};

/// Cap on Live blocks produced per catchup trigger. Aligned with
/// [`MAX_BLOCKS_PER_BATCH`] on the Composer side so one trigger's
/// catchup output maps to exactly one postBatch submission. Drop to
/// 100 if 300 turns out to produce calldata-gas pressure in practice.
pub const MAX_BLOCKS_PER_CATCHUP: u64 = 300;

/// Cap on one postBatch's SETTLEMENT range, where [`MAX_BLOCKS_PER_CATCHUP`] caps
/// production. `EEZ_MAX_BLOCKS_PER_BATCH` overrides it, rounded down to a multiple
/// of K. A chunk shrinks no further than K blocks, so calldata heavier than
/// `EEZ_MAX_POSTBATCH_GAS` allows over K blocks cannot settle at any cap.
pub const MAX_BLOCKS_PER_BATCH: u64 = MAX_BLOCKS_PER_CATCHUP;

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
    /// Returns [`DriverError`] for any missing required variable, malformed
    /// value, or validation failure.
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

    /// Verify the invariants from §5.4.4. Hard error on violation;
    /// `eez-node` startup refuses to spawn the Sequencer rather than
    /// producing surprising block patterns from a misconfigured set.
    ///
    /// # Errors
    ///
    /// Returns [`DriverError`] for any of:
    /// - any zero field
    /// - `L2_block_time % 1000 != 0` — L2 block time must be whole
    ///   seconds. Block timestamps are unix integer-seconds and the
    ///   Scheduler computes `block_height = (sync_slot_ts - genesis_ts)
    ///   / l2_block_secs`; sub-second cadence truncates and leaves the
    ///   Sequencer perpetually behind the target.
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
        if !self.l2_block_time_ms.is_multiple_of(1000) {
            return Err(DriverError::timing_config(format!(
                "L2 block time ({} ms) must be a whole number of seconds; \
                 unix block timestamps are integer-second and the sync-slot \
                 block-height arithmetic relies on it",
                self.l2_block_time_ms,
            )));
        }
        if !self.l1_block_time_ms.is_multiple_of(self.l2_block_time_ms) {
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
        let max_budget = (k - 1) * self.l2_block_time_ms;
        if self.proof_time_ms.saturating_add(self.submission_slack_ms) > max_budget {
            return Err(DriverError::timing_config(format!(
                "proof_time ({} ms) + submission_slack ({} ms) must be <= (K-1) * L2 block time ({} * {} = {} ms); else there is no room for Future blocks before Sync",
                self.proof_time_ms,
                self.submission_slack_ms,
                k - 1,
                self.l2_block_time_ms,
                max_budget,
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

    /// Relay-submission slack as a [`Duration`].
    #[must_use]
    pub const fn submission_slack(self) -> Duration {
        Duration::from_millis(self.submission_slack_ms as u64)
    }

    /// L2 blocks per sync slot. `K = L1_block_time / L2_block_time`.
    #[must_use]
    pub const fn k(self) -> u32 {
        self.l1_block_time_ms / self.l2_block_time_ms
    }

    /// Future blocks per slot: `ceil((proof_time + submission_slack) / l2) − 1`.
    /// Sized so the proven bundle reaches the relay `submission_slack` before
    /// the next L1 slot, letting the postBatch target the immediate next block.
    #[must_use]
    pub const fn future_count(self) -> u32 {
        let budget_ms = self.proof_time_ms.saturating_add(self.submission_slack_ms);
        // Manual ceil — div_ceil isn't const on the pinned toolchain.
        let blocks = budget_ms
            .saturating_add(self.l2_block_time_ms)
            .saturating_sub(1)
            / self.l2_block_time_ms;
        blocks.saturating_sub(1)
    }

    /// Live blocks per slot — the L2 blocks already produced on
    /// wall-clock cadence by the time the trigger fires.
    #[must_use]
    pub const fn live_count(self) -> u32 {
        self.k() - self.future_count() - 1
    }

    /// Wall-clock offset after the anchor L1 block at which the trigger
    /// fires — the end of the Live region, `l1 − (future + 1)·l2`. Leaves
    /// the prover `proof_time` and the relay `submission_slack` before the
    /// next slot. Always whole seconds, so `.as_secs()` is lossless.
    #[must_use]
    pub const fn proof_window_open(self) -> Duration {
        let reserved_ms = (self.future_count() + 1).saturating_mul(self.l2_block_time_ms);
        Duration::from_millis(self.l1_block_time_ms.saturating_sub(reserved_ms) as u64)
    }

    /// Wall-clock offset from "L1 block N landed" by which the
    /// postBatch bundle must reach the relay
    /// (`L1_block_time - submission_slack`).
    #[must_use]
    pub const fn submission_deadline(self) -> Duration {
        Duration::from_millis((self.l1_block_time_ms - self.submission_slack_ms) as u64)
    }

    /// Decide what to produce at a sync-slot trigger given the current
    /// L2 head, the target sync-slot block height, and the per-trigger
    /// catchup budget.
    ///
    /// `max_catchup_blocks` bounds a Catchup chunk to the K-grid; the
    /// Sequencer always passes [`MAX_BLOCKS_PER_CATCHUP`] (catchup does not
    /// enforce the speculative cap — that's steady-only).
    ///
    /// Catchup if closing the slot needs > `K` blocks, else the slot suffix
    /// (Live + Future + 1 Sync). [`SlotComposition::Idle`] when head ≥
    /// `sync_slot_block`, or the budget can't reach a grid height above head.
    #[must_use]
    pub fn per_trigger_composition(
        self,
        head_block: u64,
        sync_slot_block: u64,
        max_catchup_blocks: u64,
    ) -> SlotComposition {
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
                // Over-catch: snap the terminal to the K-grid (step back from
                // the target by whole K's until it fits) so the deriver
                // re-derives it — robust to a genesis off the slot boundary.
                let gap = sync_slot_block - head_block;
                let steps_back = gap.saturating_sub(max_catchup_blocks).div_ceil(k);
                let snap = steps_back.saturating_mul(k);
                if snap >= sync_slot_block || sync_slot_block - snap <= head_block {
                    // Budget can't reach a grid height above head; wait.
                    SlotComposition::Idle
                } else {
                    SlotComposition::Catchup {
                        live: sync_slot_block - snap - head_block - 1,
                    }
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

    /// Terminal for a bounded settlement chunk: the largest height
    /// `<= cursor + cap` sharing `sync_height`'s K-residue (the deriver rebuilds
    /// a terminal by position). `None` if none lies strictly between the two.
    #[must_use]
    pub fn historical_chunk_boundary(self, cursor: u64, sync_height: u64, cap: u64) -> Option<u64> {
        let k = u64::from(self.k());
        let over = sync_height.saturating_sub(cursor.saturating_add(cap));
        let boundary = sync_height.checked_sub(over.div_ceil(k).saturating_mul(k))?;
        (boundary > cursor && boundary < sync_height).then_some(boundary)
    }
}

/// Output of [`RollupTiming::per_trigger_composition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotComposition {
    /// Already at or past the target sync slot. No blocks to produce
    /// this trigger.
    Idle,
    /// Catchup: `live` Live blocks then a terminal Sync block at the
    /// grid-snapped height `head + live + 1`. No Future blocks; the Sync
    /// block is structural-only (empty) — see [`SyncSlotMode`](crate::SyncSlotMode).
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
        // proof+slack = 5500 → future = 2, live = 3, trigger = +6s.
        RollupTiming::new(12_000, 2_000, 4_000, 1_500)
    }

    // K=2 devnet timing. Whole-second L2 only — sub-second cadence
    // trips `.as_secs()` truncation in the Scheduler (see `validate`).
    fn chiado_slow() -> RollupTiming {
        // proof+slack = 1600 → future = 0, live = 1.
        RollupTiming::new(4_000, 2_000, 1_500, 100)
    }

    // Real chiado deployment: proof+slack = 1900 → future = 1, live = 3,
    // trigger = +3s, so the postBatch targets the immediate next L1 block.
    fn chiado_fast() -> RollupTiming {
        RollupTiming::new(5_000, 1_000, 200, 1_700)
    }

    #[test]
    fn mainnet_validates() {
        mainnet().validate().expect("mainnet config is valid");
    }

    #[test]
    fn mainnet_derived() {
        let t = mainnet();
        assert_eq!(t.k(), 6);
        assert_eq!(t.future_count(), 2);
        assert_eq!(t.live_count(), 3);
        assert_eq!(t.proof_window_open(), Duration::from_secs(6));
        assert_eq!(t.submission_deadline(), Duration::from_millis(10_500));
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
        // +3s trigger: 200ms prove finishes 1.7s before the +5s slot.
        assert_eq!(t.proof_window_open(), Duration::from_secs(3));
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
    fn sub_second_l2_rejected() {
        // L1=5s, L2=2.5s — K is integer but the L1-anchored Scheduler
        // truncates `.as_secs()` and lands perpetually in Catchup.
        let err = RollupTiming::new(5_000, 2_500, 2_000, 100)
            .validate()
            .expect_err("sub-second L2 cadence should be refused");
        assert!(format!("{err}").contains("whole number of seconds"));
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
        // Passes the proof+slack < L1 check (3100 < 4000) but trips
        // proof+slack > (K-1)*L2 (3100 > 2000) → no room for Future blocks.
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
        // future=2 → live_region_end = 6 - (2+1) = 3. Head = 3 (live
        // region full) → 0 Live + 2 Future + 1 Sync.
        let c = mainnet().per_trigger_composition(3, 6, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 0, future: 2 });
    }

    #[test]
    fn mainnet_steady_state_one_live_behind() {
        // future=2, live_region_end=3. Head=2 → 1 Live left + 2 Future + Sync.
        let c = mainnet().per_trigger_composition(2, 6, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 1, future: 2 });
    }

    #[test]
    fn mainnet_steady_state_start_of_slot() {
        // Head = 0 → 3 Live + 2 Future + 1 Sync = 6 blocks (= K).
        let c = mainnet().per_trigger_composition(0, 6, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 3, future: 2 });
    }

    #[test]
    fn mainnet_catchup_within_cap_lands_on_target() {
        // Sync at 12 (on-grid, K=6), head at 0. gap=12 <= cap, so the
        // catchup closes exactly at sync_slot: 11 Live + 1 Sync = 12
        // blocks, terminal at 0 + 11 + 1 = 12 (on grid). Next trigger
        // sees gap=K -> Slot mode.
        let c = mainnet().per_trigger_composition(0, 12, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Catchup { live: 11 });
    }

    #[test]
    fn mainnet_catchup_clamped_to_cap_snaps_to_grid() {
        // Way behind: sync at 318 (=6*53, on-grid), head at 0. gap=318 >
        // cap=300, so the terminal snaps back to the largest grid height
        // that fits the cap: 318 - ceil((318-300)/6)*6 = 318 - 18 = 300.
        // 299 Live + 1 Sync = 300 blocks (= cap), terminal at 300 (=6*50).
        let head = 0u64;
        let c = mainnet().per_trigger_composition(head, 318, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Catchup { live: 299 });
        // Terminal is on-grid and the trigger fits the cap.
        let SlotComposition::Catchup { live } = c else {
            panic!("expected Catchup");
        };
        let terminal = head + live + 1;
        let total_blocks = live + 1; // Live blocks + the Sync terminal
        assert_eq!(terminal % u64::from(mainnet().k()), 0, "terminal on grid");
        assert!(
            total_blocks <= MAX_BLOCKS_PER_CATCHUP,
            "fits per-trigger cap"
        );
    }

    #[test]
    fn catchup_terminal_always_on_grid_when_far_behind() {
        // For a range of far-behind heads, the catchup terminal must land
        // on the same K-grid as the (on-grid) sync_slot_block.
        let t = mainnet();
        let k = u64::from(t.k());
        let sync_slot = 6_000u64; // on-grid (multiple of K=6)
        for head in [0u64, 1, 5, 100, 137, 5_000, 5_699] {
            let SlotComposition::Catchup { live } =
                t.per_trigger_composition(head, sync_slot, MAX_BLOCKS_PER_CATCHUP)
            else {
                continue; // close enough for Slot mode
            };
            let terminal = head + live + 1;
            assert_eq!(
                terminal % k,
                0,
                "terminal {terminal} off-grid (head={head})"
            );
            assert!(terminal > head, "terminal must advance");
            assert!(
                terminal - head <= MAX_BLOCKS_PER_CATCHUP,
                "trigger exceeds cap (head={head})"
            );
            assert!(terminal <= sync_slot, "terminal overshot target");
        }
    }

    #[test]
    fn catchup_grid_snap_respects_genesis_offset() {
        // If genesis isn't on an L1 slot boundary, sync_slot_block carries
        // a residue != 0 mod K. Stepping back from the target keeps the
        // terminal on the SAME (offset) grid, not the 0-mod-K grid.
        let t = mainnet();
        let k = u64::from(t.k());
        let head = 0u64;
        let sync_slot = 317u64; // residue 5 mod 6 (offset grid)
        let SlotComposition::Catchup { live } =
            t.per_trigger_composition(head, sync_slot, MAX_BLOCKS_PER_CATCHUP)
        else {
            panic!("expected Catchup for gap > cap");
        };
        let terminal = head + live + 1;
        assert_eq!(
            terminal % k,
            sync_slot % k,
            "terminal must share the target's grid residue"
        );
        assert_eq!(
            (sync_slot - terminal) % k,
            0,
            "stepped back whole K-multiples"
        );
    }

    #[test]
    fn catchup_budget_bounds_chunk_to_speculative_room() {
        // based-mode: a small budget (the speculative window) caps the
        // chunk well below MAX_BLOCKS_PER_CATCHUP. head=0, sync=6000,
        // budget=64 -> terminal snaps to the largest grid height <= 64.
        let t = mainnet();
        let k = u64::from(t.k());
        let SlotComposition::Catchup { live } = t.per_trigger_composition(0, 6_000, 64) else {
            panic!("expected Catchup");
        };
        let terminal = live + 1;
        assert!(terminal <= 64, "chunk {terminal} exceeds budget 64");
        assert_eq!(terminal % k, 0, "terminal off-grid");
        assert!(terminal > 0, "must advance");
    }

    #[test]
    fn catchup_idle_when_budget_too_small() {
        // Speculative window nearly full: budget 4 < K=6, so no grid
        // height sits above head -> Idle (wait for L1 to confirm).
        let t = mainnet();
        assert_eq!(
            t.per_trigger_composition(60, 6_000, 4),
            SlotComposition::Idle
        );
    }

    #[test]
    fn catchup_chunk_never_exceeds_budget() {
        // The chunk (live + Sync) must fit the budget for any budget >= K,
        // so the terminal is reachable before the speculative pause trips.
        let t = mainnet();
        for budget in [6u64, 7, 64, 128, 299, 300] {
            if let SlotComposition::Catchup { live } = t.per_trigger_composition(0, 10_000, budget)
            {
                let chunk = live + 1; // Live blocks + the Sync terminal
                assert!(chunk <= budget, "chunk {chunk} > budget {budget}");
            }
        }
    }

    #[test]
    fn catchup_converges_to_slot_mode_next_trigger() {
        // After over-catch head = sync_slot = 12. Next trigger advances
        // sync_slot by K to 18 → gap=6 fits Slot mode (live=3, future=2).
        let head_after_catchup = 12u64;
        let sync_slot_after_trigger = 18u64;
        let c = mainnet().per_trigger_composition(
            head_after_catchup,
            sync_slot_after_trigger,
            MAX_BLOCKS_PER_CATCHUP,
        );
        assert_eq!(c, SlotComposition::Slot { live: 3, future: 2 });
    }

    #[test]
    fn chiado_slow_steady_state_start_of_slot() {
        // K=2, future_count=0. Sync at 2, head at 0. live_region_end=1.
        // live_to_produce=1, K-(future+1)=1. 1<=1 → steady-state.
        // Slot { live: 1, future: 0 } — 1 Live + 1 Sync.
        let c = chiado_slow().per_trigger_composition(0, 2, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 1, future: 0 });
    }

    #[test]
    fn chiado_fast_steady_state_start_of_slot() {
        // K=5, future_count=1. Sync at 5, head at 0. live_region_end=3,
        // live_to_produce=3, K-(future+1)=3. 3<=3 → steady-state.
        // Slot { live: 3, future: 1 } — 3 Live + 1 Future + 1 Sync.
        let c = chiado_fast().per_trigger_composition(0, 5, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 3, future: 1 });
    }

    #[test]
    fn idle_when_head_at_or_past_sync_slot() {
        assert_eq!(
            mainnet().per_trigger_composition(6, 6, MAX_BLOCKS_PER_CATCHUP),
            SlotComposition::Idle
        );
        assert_eq!(
            mainnet().per_trigger_composition(7, 6, MAX_BLOCKS_PER_CATCHUP),
            SlotComposition::Idle
        );
    }

    // --- historical_chunk_boundary (settlement dual of the grid snap) ---

    #[test]
    fn historical_boundary_is_past_on_grid_and_within_cap() {
        // 6_005 is the genesis-offset case: sync heights carry a residue != 0
        // mod K, and stepping back whole K's must keep that OFFSET grid.
        let t = mainnet();
        let k = u64::from(t.k());
        for sync_height in [6_000u64, 6_005] {
            for cursor in [0u64, 1, 7, 137, 5_000] {
                let boundary = t
                    .historical_chunk_boundary(cursor, sync_height, MAX_BLOCKS_PER_BATCH)
                    .expect("backlog exceeds the cap for every cursor here");
                assert_eq!(
                    boundary % k,
                    sync_height % k,
                    "boundary {boundary} off-grid (cursor={cursor})"
                );
                assert!(boundary > cursor && boundary < sync_height);
                assert!(boundary - cursor <= MAX_BLOCKS_PER_BATCH);
            }
        }
    }

    #[test]
    fn historical_boundary_none_when_backlog_fits_cap() {
        // Steady emission handles this — no historical chunk.
        let t = mainnet();
        assert_eq!(t.historical_chunk_boundary(100, 400, 300), None);
        assert_eq!(t.historical_chunk_boundary(100, 100, 300), None);
    }

    #[test]
    fn historical_boundary_none_when_no_grid_height_fits_the_cap() {
        // K=6, grid ≡ 0 mod 6. From cursor 97 the next grid height is 102 —
        // five blocks up, out of reach for a cap of 3, so nothing is emitted.
        let t = mainnet();
        assert_eq!(t.historical_chunk_boundary(97, 6_000, 3), None);
        // A cap of at least K always reaches one: the step-back lands within
        // K of `cursor + cap`. This is why the composer clamps the cap to K.
        assert_eq!(t.historical_chunk_boundary(97, 6_000, 6), Some(102));
    }

    #[test]
    fn inside_future_region_produces_remainder() {
        // Mainnet: sync at 6, future_count=2. live_region_end=3.
        // Head=5 (2 blocks into the Future region) → 0 Future remaining,
        // just Sync. Slot { live: 0, future: 0 }.
        let c = mainnet().per_trigger_composition(5, 6, MAX_BLOCKS_PER_CATCHUP);
        assert_eq!(c, SlotComposition::Slot { live: 0, future: 0 });
    }
}
