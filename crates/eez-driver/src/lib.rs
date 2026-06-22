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
//! - [`slot`] — `SlotKind`, `SlotEvent`, `L1HeadSource`, plus
//!   [`spawn_interval`] (standalone-mode) and [`spawn_l1_anchored`]
//!   (production). Defines the protocol between Schedulers and
//!   Sequencers.
//! - [`Sequencer`] consumes [`SlotEvent`]s, builds payload attributes,
//!   drives reth's engine, tracks head, emits [`BatchCandidate`]s.
//! - [`RollupTiming`] + [`SlotComposition`] — per-rollup wall-clock
//!   timing config and per-trigger Live/Future/Sync split.
//! - [`submit`] — `BatchCandidate` schema and emission policy.
//!
//! Composition with `eez-l1` + `eez-composer`: this crate has zero L1
//! dependencies; L1 wiring crosses the [`L1HeadSource`] trait boundary
//! (impl lives in `eez-l1::L1HeadStream`).

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

/// Shared by `Deriver::execute_block` and reth's payload builder (via
/// `eez_node::payload::EezPayloadBuilder`). Single source of truth — no
/// CLI flag for either path to drift from.
pub const BUILDER_EXTRA_DATA: &[u8] = &[];
pub const BUILDER_GAS_LIMIT: u64 = 30_000_000;

pub mod block_committer;
pub mod error;
pub mod sequencer;
pub mod slot;
pub mod submit;
pub mod timing;

#[doc(inline)]
pub use block_committer::{BlockCommitterHandle, CommitOutcome, DeriveOutcome, ForkchoiceOutcome};
#[doc(inline)]
pub use error::{DriverError, DriverResult};
#[doc(inline)]
pub use sequencer::{ConfirmedHeadSource, EthAttributesBuilder, Sequencer};
#[doc(inline)]
pub use slot::{
    L1HeadInfo, L1HeadSource, NoCrossChainContent, ParentContext, SlotEvent, SlotKind,
    SyncSlotBlock, SyncSlotComposer, SyncSlotComposerHandle, spawn_interval, spawn_l1_anchored,
};
#[doc(inline)]
pub use submit::{BatchCandidate, BatchPolicy};
#[doc(inline)]
pub use timing::{MAX_BLOCKS_PER_CATCHUP, RollupTiming, SlotComposition};
