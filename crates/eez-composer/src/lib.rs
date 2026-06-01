//! Composer umbrella. Owns the per-rollup block-production + batch-
//! submission stack: `HashMap<RollupId, RollupState>` (Sequencer +
//! Scheduler + [`RollupTiming`](eez_driver::RollupTiming) per rollup)
//! plus the shared Aggregator + Submitter + Prover.
//!
//! The composer is the "produce + post" half of an eez node. The
//! "follow" half — reth + Deriver — lives outside the umbrella and
//! runs in follower-mode nodes that have no umbrella at all.
//!
//! # Where it fits
//!
//! ```text
//! eez-driver    Sequencer / Scheduler / BlockCommitter primitives.
//!                ↑
//! eez-l1        L1Watcher / L1CanonicalHead / Submitter primitives.
//!                ↑
//! eez-prover    Prover trait + impls.
//!                ↑
//! eez-composer  THIS crate. Composer umbrella struct + RollupState +
//!               Aggregator. Drives Sequencer per-rollup, builds + posts
//!               multi-rollup-ready batches.
//!                ↑
//! eez-node      Binary. Decides follower vs composer mode at startup.
//! ```
//!
//! # Stage 4 status
//!
//! - S4.2 (current): crate skeleton. `Composer` + `RollupState` + `Aggregator` move in from `eez-l1::Composer` later in this phase.
//! - S4.7: `HeldPool` per rollup; cross-chain handlers; `eez-evm` inspector integration.
//!
//! See `docs/plans/IMPLEMENTATION.md` §5.4.8 (umbrella architecture) +
//! §5.4.11 (work order).

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
