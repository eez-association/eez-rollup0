//! Re-export of the tonic-generated module tree for `control.v1`.
//!
//! Contents:
//! * `ControlEvent`, `Composition`, `PostBatch`, `ExecutionWitness`,
//!   `SubscribeRequest`, `SlotProof`, `SubmitAck` message structs.
//! * `control_feed_server::{ControlFeed, ControlFeedServer}` — the async
//!   trait the composer implements + its tonic service adapter.
//! * `control_feed_client::ControlFeedClient` — the prover side.
//! * `proof_sink_server` / `proof_sink_client` — the return path.
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/control.v1.rs"));
}
