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
//! Stage 1 surface (this crate today):
//!
//! - [`Scheduler`] fires a [`ProposalRequest`] on a fixed wall-clock interval.
//! - [`Sequencer`] consumes those requests, builds payload attributes, drives
//!   reth's engine, and tracks the head.
//!
//! Later stages will add: L1-triggered safe-head updates and reorgs (stage 3),
//! batch pre-building with future timestamps (stage 4), and composer-driven
//! cross-chain block contents (stage 4). The Stage-1 module layout already
//! places those seams where they belong — the [`Scheduler`] is the entry
//! point for non-tick events, and the [`Sequencer`] is where head state lives.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod block_committer;
pub mod error;
pub mod scheduler;
pub mod sequencer;

#[doc(inline)]
pub use block_committer::{BlockCommitterHandle, CommitOutcome, DeriveOutcome, ForkchoiceOutcome};
#[doc(inline)]
pub use error::{DriverError, DriverResult};
#[doc(inline)]
pub use scheduler::{ProposalRequest, Scheduler, SlotKind};
#[doc(inline)]
pub use sequencer::{ConfirmedHeadSource, EthAttributesBuilder, Sequencer};
