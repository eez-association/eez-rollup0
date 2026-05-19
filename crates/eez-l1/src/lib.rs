//! L1 anchor for eez-rollup0 — submits L2 batches to `EEZ.sol` on L1.
//!
//! Stage 2 surface:
//!
//! - [`SubmitterConfig::from_env`] reads `EEZ_*` env vars (see `.env.example`).
//! - [`Submitter::run`] is a tokio task: every `interval`, scan past `BatchPosted`
//!   events to recover the last L2 block we posted, read the local L2 head, and
//!   if the L2 has advanced, build an empty
//!   `ProofSystemBatchPerVerificationEntries` carrying our payload bytes in
//!   `callData`, compute the public-inputs hash exactly as `EEZ` does, ask the
//!   injected [`Prover`](eez_prover::Prover) for a proof, and submit
//!   `postAndVerifyBatch`. Stateless by design — no on-disk cursor; the
//!   L1 contract's event log is the source of truth.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod config;
pub mod error;
pub mod submitter;

#[doc(inline)]
pub use config::SubmitterConfig;
#[doc(inline)]
pub use error::{L1Error, L1Result};
#[doc(inline)]
pub use submitter::Submitter;
