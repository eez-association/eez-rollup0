//! Re-export of the tonic-generated module tree for `prove.v1`.
//!
//! Contents:
//! * `ProveChunk`, `ProveHeader`, `PostBatch`, `BlockWitness`,
//!   `ExecutionWitness`, `ProveResponse` message structs.
//! * `prover_server::{Prover, ProverServer}` — the async trait `eez-proverd`
//!   implements + its tonic service adapter.
//! * `prover_client::ProverClient` — the composer side (`eez-prover-client`).
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/prove.v1.rs"));
}
