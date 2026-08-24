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

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost::Message;

    use super::MAX_MESSAGE_BYTES;
    use super::v1::{ProveChunk, ProveHeader, ProveResponse, prove_chunk};

    #[test]
    fn prove_header_round_trips_through_protobuf() {
        let chunk = ProveChunk {
            kind: Some(prove_chunk::Kind::Header(ProveHeader {
                rollup_id: 7,
                from_block: 10,
                to_block: 12,
                post_batch: None,
            })),
        };

        let encoded = chunk.encode_to_vec();
        assert!(encoded.len() < MAX_MESSAGE_BYTES);
        assert_eq!(ProveChunk::decode(encoded.as_slice()).unwrap(), chunk);
    }

    #[test]
    fn response_round_trips_and_truncated_payload_is_rejected() {
        let response = ProveResponse {
            public_inputs_hash: vec![0x11; 32],
            signature: vec![0x22; 65],
        };
        let encoded = response.encode_to_vec();

        assert_eq!(ProveResponse::decode(encoded.as_slice()).unwrap(), response);
        assert!(ProveResponse::decode(&encoded[..encoded.len() - 1]).is_err());
    }

    proptest! {
        // Proto decode of arbitrary RPC bytes must never panic.
        #[test]
        fn arbitrary_wire_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = ProveChunk::decode(bytes.as_slice());
            let _ = ProveResponse::decode(bytes.as_slice());
        }
    }
}
