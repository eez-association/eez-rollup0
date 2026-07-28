//! Composer umbrella. Owns the per-rollup block-production + batch-
//! submission stack.
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
//! eez-node      Binary. Decides follower vs composer mode at startup.
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod composer;
pub mod held_pool;
pub mod ingress;
pub mod local;
pub mod optimistic;
pub mod rollup;

#[doc(inline)]
pub use composer::{Composer, CrossChainExecCtx};
#[doc(inline)]
pub use held_pool::{HeldPool, HeldTx};
#[doc(inline)]
pub use ingress::Direction;

#[doc(inline)]
pub use local::{build_sync_block, BuildError, BuiltSyncBlock, GnosisL1Adapter, LocalChainClient};
pub use optimistic::OptimisticallyIncluded;
#[doc(inline)]
pub use rollup::{RollupConfig, RollupState};
