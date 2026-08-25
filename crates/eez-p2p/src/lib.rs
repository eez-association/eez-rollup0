//! Signed unsafe-block protocol shared by composers and followers.
//!
//! The wire message is `signature || body`, where `signature` is the 65-byte
//! Ethereum `r || s || y_parity` signature and `body` is the SSZ-encoded
//! Engine API V4 request tuple:
//!
//! ```text
//! (ExecutionPayloadV3, blob_versioned_hashes, parent_beacon_block_root,
//!  execution_requests)
//! ```
//!
//! Keeping the body identical to Reth's SSZ `engine_newPayloadV4` request
//! avoids a second fork-specific payload representation at the trust boundary.

mod network;

pub use network::{NetworkConfig, NetworkEvent, NetworkHandle, NetworkService};

use alloy_eips::eip7685::{EMPTY_REQUESTS_HASH, Requests, RequestsOrHash};
use alloy_primitives::{Address, B256, Signature, U256, keccak256};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
    ExecutionPayloadV3, PraguePayloadFields,
};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use ssz::{Decode, Encode};

/// Byte length of an Ethereum recoverable signature.
pub const SIGNATURE_LEN: usize = 65;

/// Protocol version tracks the Engine API request shape carried by the topic.
pub const PROTOCOL_VERSION: u8 = 4;

/// Returns the chain-scoped GossipSub topic for signed unsafe blocks.
#[must_use]
pub fn blocks_topic(chain_id: u64) -> String {
    format!("/eez/{chain_id}/{PROTOCOL_VERSION}/blocks")
}

/// Errors at the signed unsafe-block trust boundary.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Message cannot contain a complete signature and body.
    #[error("signed block is too short: got {actual} bytes, need more than {SIGNATURE_LEN}")]
    MessageTooShort { actual: usize },
    /// The execution payload is not the Cancun/Prague payload carried by V4.
    #[error("only ExecutionPayloadV3 is supported on the V4 unsafe-block topic")]
    UnsupportedPayload,
    /// Required Cancun fields are absent.
    #[error("payload is missing Cancun sidecar fields")]
    MissingCancunFields,
    /// The actual Prague requests are absent; a header-only requests hash is insufficient.
    #[error("payload is missing the Prague execution-request list")]
    MissingExecutionRequests,
    /// SSZ decoding failed.
    #[error("invalid V4 execution payload SSZ: {0}")]
    InvalidSsz(String),
    /// Signature parsing failed.
    #[error("invalid sequencer signature: {0}")]
    InvalidSignature(#[from] alloy_primitives::SignatureError),
    /// The wire format requires the raw y-parity byte, not legacy 27/28 notation.
    #[error("invalid sequencer signature parity: expected 0 or 1, got {0}")]
    InvalidSignatureParity(u8),
    /// Non-canonical ECDSA signatures are rejected.
    #[error("sequencer signature has a non-canonical high-s value")]
    HighSignatureS,
    /// Public-key recovery failed.
    #[error("could not recover sequencer address: {0}")]
    SignatureRecovery(alloy_primitives::SignatureError),
    /// The signature is valid but not from the configured unsafe-block signer.
    #[error("unauthorized unsafe-block signer: recovered {recovered}, expected {expected}")]
    UnauthorizedSigner {
        recovered: Address,
        expected: Address,
    },
    /// Local signing failed.
    #[error("could not sign unsafe block: {0}")]
    Signing(String),
    /// The payload cannot be converted back into a block.
    #[error("invalid execution payload: {0}")]
    InvalidPayload(String),
    /// Payload fields do not reproduce its claimed block hash.
    #[error("execution payload block hash mismatch: claimed {claimed}, computed {computed}")]
    BlockHashMismatch { claimed: B256, computed: B256 },
}

/// Encode complete Engine API V4 input as the signed message body.
pub fn encode_body(data: &ExecutionData) -> Result<Vec<u8>, ProtocolError> {
    let ExecutionPayload::V3(payload) = &data.payload else {
        return Err(ProtocolError::UnsupportedPayload);
    };
    let cancun = data
        .sidecar
        .cancun()
        .ok_or(ProtocolError::MissingCancunFields)?;
    let prague = data
        .sidecar
        .prague()
        .ok_or(ProtocolError::MissingExecutionRequests)?;
    let empty_requests = Requests::default();
    let requests = match &prague.requests {
        RequestsOrHash::Requests(requests) => requests,
        RequestsOrHash::Hash(hash) if *hash == EMPTY_REQUESTS_HASH => &empty_requests,
        RequestsOrHash::Hash(_) => return Err(ProtocolError::MissingExecutionRequests),
    };

    Ok((
        payload.clone(),
        cancun.versioned_hashes.clone(),
        cancun.parent_beacon_block_root,
        requests.iter().cloned().collect::<Vec<_>>(),
    )
        .as_ssz_bytes())
}

/// Decode an Engine API V4 body and reject a payload whose header hash does not match its fields.
pub fn decode_body(body: &[u8]) -> Result<ExecutionData, ProtocolError> {
    let (payload, versioned_hashes, parent_beacon_block_root, execution_requests) =
        <(
            ExecutionPayloadV3,
            Vec<B256>,
            B256,
            Vec<alloy_primitives::Bytes>,
        )>::from_ssz_bytes(body)
        .map_err(|error| ProtocolError::InvalidSsz(format!("{error:?}")))?;
    let claimed = payload.payload_inner.payload_inner.block_hash;
    let sidecar = ExecutionPayloadSidecar::v4(
        CancunPayloadFields::new(parent_beacon_block_root, versioned_hashes),
        PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(execution_requests))),
    );
    let data = ExecutionData::new(payload.into(), sidecar);
    let computed = data
        .clone()
        .into_block_raw()
        .map_err(|error| ProtocolError::InvalidPayload(error.to_string()))?
        .hash_slow();
    if computed != claimed {
        return Err(ProtocolError::BlockHashMismatch { claimed, computed });
    }
    Ok(data)
}

/// OP-style domain-separated signing digest for an unsafe payload body.
///
/// The digest is `keccak256(bytes32(0) || uint256(chain_id) || keccak256(body))`.
#[must_use]
pub fn signing_hash(chain_id: u64, body: &[u8]) -> B256 {
    let mut input = [0u8; 96];
    input[32..64].copy_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    input[64..].copy_from_slice(keccak256(body).as_slice());
    keccak256(input)
}

/// Sign a complete execution payload for publication.
pub fn sign_payload(
    data: &ExecutionData,
    chain_id: u64,
    signer: &PrivateKeySigner,
) -> Result<Vec<u8>, ProtocolError> {
    let body = encode_body(data)?;
    let signature = signer
        .sign_hash_sync(&signing_hash(chain_id, &body))
        .map_err(|error| ProtocolError::Signing(error.to_string()))?;
    let mut message = Vec::with_capacity(SIGNATURE_LEN + body.len());
    message.extend_from_slice(&signature.as_rsy());
    message.extend_from_slice(&body);
    Ok(message)
}

/// Authenticate and decode a signed unsafe payload.
pub fn verify_payload(
    message: &[u8],
    chain_id: u64,
    authorized_signer: Address,
) -> Result<ExecutionData, ProtocolError> {
    if message.len() <= SIGNATURE_LEN {
        return Err(ProtocolError::MessageTooShort {
            actual: message.len(),
        });
    }
    let (signature, body) = message.split_at(SIGNATURE_LEN);
    if signature[64] > 1 {
        return Err(ProtocolError::InvalidSignatureParity(signature[64]));
    }
    let signature = Signature::try_from(signature)?;
    if signature.normalize_s().is_some() {
        return Err(ProtocolError::HighSignatureS);
    }
    let recovered = signature
        .recover_address_from_prehash(&signing_hash(chain_id, body))
        .map_err(ProtocolError::SignatureRecovery)?;
    if recovered != authorized_signer {
        return Err(ProtocolError::UnauthorizedSigner {
            recovered,
            expected: authorized_signer,
        });
    }
    decode_body(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Block, BlockBody, Header, TxEnvelope};
    use alloy_eips::eip4895::Withdrawals;
    use alloy_primitives::Bloom;

    fn test_payload() -> ExecutionData {
        let body = BlockBody::<TxEnvelope> {
            withdrawals: Some(Withdrawals::default()),
            ..BlockBody::default()
        };
        let block = Block::new(
            Header {
                parent_hash: B256::repeat_byte(0x11),
                beneficiary: Address::repeat_byte(0x22),
                state_root: B256::repeat_byte(0x33),
                receipts_root: B256::repeat_byte(0x44),
                logs_bloom: Bloom::ZERO,
                mix_hash: B256::repeat_byte(0x55),
                number: 7,
                gas_limit: 30_000_000,
                gas_used: 0,
                timestamp: 1_700_000_000,
                base_fee_per_gas: Some(1_000_000_000),
                withdrawals_root: body.calculate_withdrawals_root(),
                blob_gas_used: Some(0),
                excess_blob_gas: Some(0),
                parent_beacon_block_root: Some(B256::repeat_byte(0x66)),
                requests_hash: Some(EMPTY_REQUESTS_HASH),
                ..Header::default()
            },
            body,
        );
        let payload = ExecutionPayloadV3::from_block_slow(&block);
        ExecutionData::new(
            payload.into(),
            ExecutionPayloadSidecar::v4(
                CancunPayloadFields::new(B256::repeat_byte(0x66), Vec::new()),
                PraguePayloadFields::new(Requests::default()),
            ),
        )
    }

    fn test_signer(last_byte: u8) -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::with_last_byte(last_byte)).unwrap()
    }

    #[test]
    fn v4_body_roundtrip_preserves_engine_input() {
        let original = test_payload();
        let decoded = decode_body(&encode_body(&original).unwrap()).unwrap();

        assert_eq!(decoded.block_hash(), original.block_hash());
        assert_eq!(decoded.parent_hash(), original.parent_hash());
        assert_eq!(
            decoded.parent_beacon_block_root(),
            original.parent_beacon_block_root()
        );
        assert_eq!(decoded.sidecar.versioned_hashes(), Some(&Vec::new()));
        assert_eq!(decoded.sidecar.requests(), Some(&Requests::default()));
    }

    #[test]
    fn encoder_only_normalizes_the_canonical_empty_requests_hash() {
        let mut payload = test_payload();
        payload.sidecar = ExecutionPayloadSidecar::v4(
            payload.sidecar.cancun().unwrap().clone(),
            PraguePayloadFields::new(EMPTY_REQUESTS_HASH),
        );
        assert!(encode_body(&payload).is_ok());

        payload.sidecar = ExecutionPayloadSidecar::v4(
            payload.sidecar.cancun().unwrap().clone(),
            PraguePayloadFields::new(B256::repeat_byte(0x77)),
        );
        assert!(matches!(
            encode_body(&payload),
            Err(ProtocolError::MissingExecutionRequests)
        ));
    }

    #[test]
    fn signed_payload_is_chain_bound_and_authorized() {
        let payload = test_payload();
        let signer = test_signer(1);
        let message = sign_payload(&payload, 1234, &signer).unwrap();

        let decoded = verify_payload(&message, 1234, signer.address()).unwrap();
        assert_eq!(decoded.block_hash(), payload.block_hash());
        assert!(matches!(
            verify_payload(&message, 1235, signer.address()),
            Err(ProtocolError::UnauthorizedSigner { .. })
        ));
        assert!(matches!(
            verify_payload(&message, 1234, test_signer(2).address()),
            Err(ProtocolError::UnauthorizedSigner { .. })
        ));
    }

    #[test]
    fn signed_payload_rejects_body_mutation() {
        let payload = test_payload();
        let signer = test_signer(1);
        let mut message = sign_payload(&payload, 1234, &signer).unwrap();
        *message.last_mut().unwrap() ^= 1;

        assert!(verify_payload(&message, 1234, signer.address()).is_err());
    }

    #[test]
    fn verifier_rejects_legacy_signature_parity_encoding() {
        let payload = test_payload();
        let signer = test_signer(1);
        let mut message = sign_payload(&payload, 1234, &signer).unwrap();
        message[64] += 27;

        assert!(matches!(
            verify_payload(&message, 1234, signer.address()),
            Err(ProtocolError::InvalidSignatureParity(27 | 28))
        ));
    }

    #[test]
    fn decoder_rejects_claimed_block_hash_mismatch() {
        let mut payload = test_payload();
        let ExecutionPayload::V3(inner) = &mut payload.payload else {
            unreachable!()
        };
        inner.payload_inner.payload_inner.block_hash = B256::ZERO;

        assert!(matches!(
            decode_body(&encode_body(&payload).unwrap()),
            Err(ProtocolError::BlockHashMismatch { .. })
        ));
    }

    #[test]
    fn topic_is_chain_scoped_and_versioned() {
        assert_eq!(blocks_topic(1234), "/eez/1234/4/blocks");
    }
}
