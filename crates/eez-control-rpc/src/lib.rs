//! Wire contract for the composer → prover feed.
//!
//! The composer streams one [`v1::ControlEvent`] per committed L2 block
//! via the `control.v1.ControlFeed` service. Each event is self-contained:
//! the block identity, the execution witness + block RLP (which the composer
//! pulls from reth's `eez_executionWitness`), and (for Sync blocks and
//! Normal-shaped settling blocks) the cross-chain composition. One block →
//! one event; the prover does not join.
//!
//! Only generated types + tonic stubs live here.

mod generated;

/// Tonic-generated protobuf module for the `control.v1` package.
pub use generated::v1;
