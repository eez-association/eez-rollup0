//! Wire contract for the composer-controlled `Prove` RPC.
//!
//! The composer (client, `eez-prover-client`) dials `eez-proof-signer` and
//! streams one posted settlement window — a
//! [`v1::ProveHeader`] then one [`v1::BlockWitness`] per block — via the
//! `prove.v1.Prover` service. The prover re-executes, gates, ECDSA-signs the
//! recomputed `publicInputsHash`, and returns a [`v1::ProveResponse`]. One
//! request/response: no feed, no dispatch, no sink.
//!
//! Only generated types + tonic stubs live here — no `Prover`-trait coupling.

mod generated;

/// Tonic-generated protobuf module for the `prove.v1` package.
pub use generated::v1;

/// Max size of a single `Prove` gRPC message, applied to BOTH the client
/// (encode) and server (decode).
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024 * 1024;
