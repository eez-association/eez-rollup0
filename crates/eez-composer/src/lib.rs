//! Composer umbrella. Owns the per-rollup block-production + batch-
//! submission stack: `HashMap<RollupId, RollupState>` (timing,
//! L2 provider, L1-confirmed cursor per rollup) plus the shared
//! Aggregator + Submitter + Prover + `L1Watcher`.
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
//! eez-composer  THIS crate. Composer umbrella + RollupState +
//!               L1-anchored Scheduler. Drives Sequencer per-rollup,
//!               builds + posts batches, hosts the cross-chain
//!               composition logic (S4.7+).
//!                ↑
//! eez-composer  Binary. Wires the complete composer stack at startup.
//! ```
//!
//! # Stage 4 status
//!
//! - S4.2 (this commit): umbrella + L1-anchored Scheduler in place;
//!   `eez-l1::Composer` retires; logic moves here.
//! - S4.7: per-rollup `HeldPool`; cross-chain handlers; `eez-protocol`
//!   inspector integration.
//!
//! See `docs/plans/IMPLEMENTATION.md` §5.4.8 (umbrella architecture) +
//! §5.4.11 (work order).

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod composer;
pub mod held_pool;
pub mod ingress;
pub mod local;
pub mod optimistic;
mod prover_retry;
pub mod rollup;

#[doc(inline)]
pub use composer::{Composer, CrossChainExecCtx};
#[doc(inline)]
pub use held_pool::{AdmissionError, HeldPool, HeldTx};
#[doc(inline)]
pub use ingress::Direction;

#[doc(inline)]
pub use local::{BuildError, BuiltSyncBlock, GnosisL1Adapter, LocalChainClient, build_sync_block};
pub use optimistic::OptimisticallyIncluded;
#[doc(inline)]
pub use rollup::{RollupConfig, RollupState};
