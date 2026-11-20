//! Slot timing for an EEZ rollup, with no reth dependency.
//!
//! - [`timing`]: [`RollupTiming`], the per-rollup timing config, and the
//!   per-trigger Live / Future / Sync split.
//! - [`clock`]: the injected [`Clock`] that every slot-time read goes through.
//! - [`deadline`]: the lateness check and the proof-start cutoff.
//! - [`scheduler`]: [`SlotKind`] and [`SlotEvent`], the protocol between the
//!   scheduler and the Sequencer, and [`SlotScheduler`], the scheduler's
//!   sans-IO core.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod clock;
pub mod deadline;
pub mod scheduler;
pub mod timing;

#[doc(inline)]
pub use clock::{Clock, SharedClock, SystemClock, TokioClock};
#[doc(inline)]
pub use scheduler::{ArmedTrigger, SlotEvent, SlotKind, SlotScheduler};
#[doc(inline)]
pub use timing::{
    MAX_BLOCKS_PER_BATCH, MAX_BLOCKS_PER_CATCHUP, RollupTiming, SlotComposition, TimingError,
};
