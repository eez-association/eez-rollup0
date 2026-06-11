//! L1 anchor for eez-rollup0.
//!
//! Two-layer surface:
//!
//! - [`Submitter`] is the thin L1-interaction primitive: send
//!   `postAndVerifyBatch` and scan past `BatchPosted` events. No state,
//!   no orchestration.
//! - [`Composer`] owns the orchestration: holds the in-memory
//!   `posted_through` cursor seeded from a startup scan via the
//!   Submitter, ticks on interval, detects the L2-head gap, builds the
//!   stage-2 batch payload, asks the injected [`Prover`](eez_prover::Prover)
//!   for a proof, packs it, and hands the assembled batch to the
//!   Submitter to send.
//!
//! Stateless after startup — no on-disk cursor; the L1 contract's event
//! log is the source of truth. Stage 3's Deriver will own the L1-event-
//! driven catch-up + reorg path.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod composer;
pub mod config;
pub mod error;
pub mod l1_canonical_head;
pub mod l1_watcher;
pub mod submitter;

#[doc(inline)]
pub use composer::Composer;
#[doc(inline)]
pub use config::{ComposerConfig, SubmitterConfig, registry_deploy_block_from_env};
#[doc(inline)]
pub use error::{L1Error, L1Result};
#[doc(inline)]
pub use l1_canonical_head::{BatchRecord, L1CanonicalHead};
#[doc(inline)]
pub use l1_watcher::{L1Event, L1Watcher, L1WatcherConfig};
#[doc(inline)]
pub use submitter::{BundleTarget, HistoricalBatch, SendOutcome, Submitter};
