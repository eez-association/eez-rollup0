//! Engine-API consumer that drives a reth node to produce blocks on a tick.
//!
//! `eez-driver` is the "consensus driver" half of the eez node: it owns the
//! schedule of when blocks are produced, builds payload attributes, and pushes
//! the resulting blocks into reth via the engine API (`forkchoiceUpdated` +
//! `newPayload`). The other half — block execution, storage, networking, RPC —
//! lives inside reth and is reached only through the engine-API handles.
//!
//! This is the same shape of integration that op-node uses for OP Stack; the
//! engine-API client is reth's stable contract for "consensus that isn't a
//! real `PoS` CL."
//!
//! ## Surface (Stage 4)
//!
//! - [`slot`] — `SlotKind`, `SlotEvent`, and [`spawn_l1_anchored`]. Defines
//!   the protocol between Schedulers and Sequencers.
//! - [`Sequencer`] consumes [`SlotEvent`]s, builds payload attributes,
//!   drives reth's engine, tracks head.
//! - [`RollupTiming`] + [`SlotComposition`] — per-rollup wall-clock
//!   timing config and per-trigger Live/Future/Sync split.
//!
//! L1-anchored scheduling consumes an [`eez_l1::L1HeadStream`] through
//! [`spawn_l1_anchored`]; higher-level composition remains in `eez-composer`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

/// Shared by `Deriver::execute_block` and reth's payload builder (via
/// `eez_node_common::EezPayloadBuilder`). Single source of truth — no
/// CLI flag for either path to drift from.
pub const BUILDER_EXTRA_DATA: &[u8] = &[];
pub const BUILDER_GAS_LIMIT: u64 = 30_000_000;

pub mod block_committer;
pub mod error;
pub mod sequencer;
pub mod slot;
pub mod timing;
#[cfg(feature = "witness")]
pub mod witness;

#[doc(inline)]
pub use block_committer::{BlockCommitterHandle, CommitOutcome, DeriveOutcome, ForkchoiceOutcome};
#[doc(inline)]
pub use error::{DriverError, DriverResult};
#[doc(inline)]
pub use sequencer::{DEFAULT_MAX_SPECULATIVE_DEPTH, EthAttributesBuilder, Sequencer};
#[doc(inline)]
pub use slot::{
    ParentContext, SlotEvent, SlotKind, SyncSlotBlock, SyncSlotComposer, SyncSlotComposerHandle,
    SyncSlotMode, spawn_l1_anchored,
};
#[doc(inline)]
pub use timing::{MAX_BLOCKS_PER_BATCH, MAX_BLOCKS_PER_CATCHUP, RollupTiming, SlotComposition};
