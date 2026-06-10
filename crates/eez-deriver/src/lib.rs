//! L1-derived L2 consensus: subscribes to [`L1Event`](eez_l1::L1Event)s,
//! decodes posted batches, and drives the
//! [`BlockCommitterHandle`](eez_driver::BlockCommitterHandle) to keep
//! local L2 in sync with L1. Owns:
//!
//! - Safe-head advancement from `BatchPosted` (decode + tx-list match → FCU safe)
//! - Catch-up via STF replay for empty-datadir / behind-head starts
//! - Reorg walk-back from `L1Event::Reorg` (drops stale batches, retreats safe)
//! - L2-finality mapping from `L1Event::Finalized` (advances reth finalized)
//!
//! Multi-L2 fan-out (per-rollup `Deriver` sharing one `L1Watcher`) is the
//! next layer up.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod deriver;
pub mod error;

#[doc(inline)]
pub use deriver::Deriver;
#[doc(inline)]
pub use error::{DeriverError, DeriverResult};
