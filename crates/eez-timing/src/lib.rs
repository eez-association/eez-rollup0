//! Slot timing for an EEZ rollup, with no reth dependency.
//!
//! - [`timing`]: [`RollupTiming`], the per-rollup timing config, and the
//!   per-trigger Live / Future / Sync split.
//! - [`scheduler`]: [`SlotKind`] and [`SlotEvent`], the protocol between the
//!   scheduler and the Sequencer.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod scheduler;
pub mod timing;

#[doc(inline)]
pub use scheduler::{SlotEvent, SlotKind};
#[doc(inline)]
pub use timing::{
    MAX_BLOCKS_PER_BATCH, MAX_BLOCKS_PER_CATCHUP, RollupTiming, SlotComposition, TimingError,
};
