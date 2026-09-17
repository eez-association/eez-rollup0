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

use prost::Message;

/// Max size of a single `Prove` gRPC message, applied to BOTH the client
/// (encode) and server (decode).
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024 * 1024;

/// Encode one typed [`v1::ProveFailure`] for the gRPC status-details field.
#[must_use]
pub fn encode_prove_failure(failure: &v1::ProveFailure) -> Vec<u8> {
    failure.encode_to_vec()
}

/// Decode a typed [`v1::ProveFailure`] from the gRPC status-details field.
///
/// # Errors
///
/// Returns [`prost::DecodeError`] when the details are not a valid
/// `prove.v1.ProveFailure` protobuf payload.
pub fn decode_prove_failure(details: &[u8]) -> Result<v1::ProveFailure, prost::DecodeError> {
    v1::ProveFailure::decode(details)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use prost::Message;

    use super::v1::{
        OutboundFailure, ProveChunk, ProveFailure, ProveHeader, ProveResponse, prove_chunk,
        prove_failure,
    };
    use super::{MAX_MESSAGE_BYTES, decode_prove_failure, encode_prove_failure};

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

    #[test]
    fn prove_failure_details_round_trip() {
        let failure = ProveFailure {
            actionable_failure: Some(prove_failure::ActionableFailure::Outbound(
                OutboundFailure {
                    transaction_index: 3,
                    transaction_hash: vec![0x44; 32],
                },
            )),
        };

        assert_eq!(
            decode_prove_failure(&encode_prove_failure(&failure)).unwrap(),
            failure
        );
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
