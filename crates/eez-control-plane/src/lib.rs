//! Composer↔prover control plane.
//!
//! Owns the control-feed / proof-sink / prover-dispatch tonic services
//! that connect the composer's block-production loop to an out-of-process
//! prover, the [`PostedWindows`](posted_windows) ledger tracking which
//! posted batches are still awaiting attestation, and the `postBatch`
//! control-message builder.
//!
//! These modules are self-contained: they depend only on the control-plane
//! wire contract (`eez-control-rpc`) and the EVM ABI/batch types
//! (`eez-evm`). They do not reach back into the composer umbrella — the
//! composer wires them in, not the other way round.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod control_feed;
pub mod post_batch_msg;
pub mod posted_windows;
pub mod proof_sink;
pub mod prover_dispatch;
