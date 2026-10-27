//! Streaming proof-signing wire contract: Begin, Block, Finalize and session recovery.
//! Generated types and tonic stubs; execution and settlement live in the prover crates.

mod generated;

/// Tonic-generated protobuf module for the `prove.v1` package.
pub use generated::v1;

use prost::Message;

/// Max size of a single `ProveStream` gRPC message, applied to BOTH the client
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
        Begin, Cancel, Cancelled, ClientFrame, Ready, Rejected, Rewind, ServerFrame, Validated,
        client_frame, server_frame,
    };
    use super::v1::{OutboundFailure, ProveFailure, prove_failure};
    use super::{decode_prove_failure, encode_prove_failure};

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

    #[test]
    fn streaming_frames_round_trip_with_correlated_session_and_request() {
        let begin = ClientFrame {
            session_id: Vec::new(),
            request_id: 17,
            epoch: 0,
            kind: Some(client_frame::Kind::Begin(Begin {
                rollup_id: 7,
                anchor_number: 100,
                anchor_hash: vec![0x11; 32],
                anchor_state_root: vec![0x22; 32],
            })),
        };
        assert_eq!(
            ClientFrame::decode(begin.encode_to_vec().as_slice()).unwrap(),
            begin
        );

        let session_id = vec![0x33; 32];
        let ready = ServerFrame {
            session_id: session_id.clone(),
            request_id: begin.request_id,
            epoch: 1,
            kind: Some(server_frame::Kind::Ready(Ready {
                validated_through: 100,
                validated_hash: vec![0x11; 32],
            })),
        };
        assert_eq!(
            ServerFrame::decode(ready.encode_to_vec().as_slice()).unwrap(),
            ready
        );

        let acknowledged = ServerFrame {
            session_id: session_id.clone(),
            request_id: 18,
            epoch: 1,
            kind: Some(server_frame::Kind::Validated(Validated {
                number: 101,
                hash: vec![0x44; 32],
                post_state_root: vec![0x55; 32],
                reused: true,
            })),
        };
        assert_eq!(
            ServerFrame::decode(acknowledged.encode_to_vec().as_slice()).unwrap(),
            acknowledged
        );

        let rejected = ServerFrame {
            session_id: session_id.clone(),
            request_id: 19,
            epoch: 1,
            kind: Some(server_frame::Kind::Rejected(Rejected {
                code: 14,
                message: "retry from the validated cursor".to_owned(),
                details: Vec::new(),
                validated_through: 101,
                validated_hash: vec![0x44; 32],
            })),
        };
        assert_eq!(
            ServerFrame::decode(rejected.encode_to_vec().as_slice()).unwrap(),
            rejected
        );

        let rewind = ClientFrame {
            session_id: session_id.clone(),
            request_id: 20,
            epoch: 1,
            kind: Some(client_frame::Kind::Rewind(Rewind {
                ancestor_hash: vec![0x11; 32],
            })),
        };
        assert_eq!(
            ClientFrame::decode(rewind.encode_to_vec().as_slice()).unwrap(),
            rewind
        );

        let rewound = ServerFrame {
            session_id,
            request_id: rewind.request_id,
            epoch: 2,
            kind: Some(server_frame::Kind::Ready(Ready {
                validated_through: 100,
                validated_hash: vec![0x11; 32],
            })),
        };
        assert_eq!(
            ServerFrame::decode(rewound.encode_to_vec().as_slice()).unwrap(),
            rewound
        );

        let cancel = ClientFrame {
            session_id: vec![0x33; 32],
            request_id: 21,
            epoch: 2,
            kind: Some(client_frame::Kind::Cancel(Cancel {})),
        };
        assert_eq!(
            ClientFrame::decode(cancel.encode_to_vec().as_slice()).unwrap(),
            cancel
        );

        let cancelled = ServerFrame {
            session_id: cancel.session_id.clone(),
            request_id: cancel.request_id,
            epoch: cancel.epoch,
            kind: Some(server_frame::Kind::Cancelled(Cancelled {})),
        };
        assert_eq!(
            ServerFrame::decode(cancelled.encode_to_vec().as_slice()).unwrap(),
            cancelled
        );
    }

    proptest! {
        // Proto decode of arbitrary RPC bytes must never panic.
        #[test]
        fn arbitrary_wire_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = ClientFrame::decode(bytes.as_slice());
            let _ = ServerFrame::decode(bytes.as_slice());
        }
    }
}
