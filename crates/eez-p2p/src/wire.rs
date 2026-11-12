use alloy_primitives::{B256, Bytes};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use eez_seq_sig::{Envelope, HeadClaim};
use sha2::{Digest, Sha256};

/// Cap on the uncompressed message, checked before allocating.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Bytes of the Snappy length preamble; four 7-bit groups hold any length up to the cap.
const MAX_PREAMBLE_LEN: usize = 4;
const _: () = assert!(MAX_MESSAGE_SIZE < 1 << (7 * MAX_PREAMBLE_LEN));

/// Ethereum consensus's message-id domains, for data that does and doesn't decompress.
const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
const MESSAGE_DOMAIN_INVALID_SNAPPY: [u8; 4] = [0, 0, 0, 0];

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("message of {0} bytes exceeds the cap")]
    TooLarge(usize),
    #[error("snappy: {0}")]
    Snappy(String),
    #[error("rlp: {0}")]
    Rlp(String),
}

#[derive(RlpEncodable, RlpDecodable)]
struct WireHead {
    rollup_id: u64,
    number: u64,
    block_hash: B256,
    parent_hash: B256,
    epoch_ms: u64,
    counter: u64,
}

#[derive(RlpEncodable, RlpDecodable)]
struct WireEnvelope {
    head: WireHead,
    signature: Bytes,
    block: Bytes,
}

/// Snappy-compressed RLP of the envelope.
pub fn encode(envelope: &Envelope) -> Result<Vec<u8>, WireError> {
    let h = &envelope.head;
    let wire = WireEnvelope {
        head: WireHead {
            rollup_id: h.rollup_id,
            number: h.number,
            block_hash: h.block_hash,
            parent_hash: h.parent_hash,
            epoch_ms: h.epoch_ms,
            counter: h.counter,
        },
        signature: envelope.signature.clone(),
        block: envelope.block.clone(),
    };
    let mut rlp = Vec::with_capacity(wire.length());
    wire.encode(&mut rlp);
    if rlp.len() > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(rlp.len()));
    }
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&rlp)
        .map_err(|e| WireError::Snappy(e.to_string()))?;
    if compressed.len() > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(compressed.len()));
    }
    Ok(compressed)
}

/// Inverse of [`encode`]; never panics on malformed input.
pub fn decode(data: &[u8]) -> Result<Envelope, WireError> {
    let rlp = decompress(data)?;
    let mut buf = rlp.as_slice();
    let w = WireEnvelope::decode(&mut buf).map_err(|e| WireError::Rlp(e.to_string()))?;
    if !buf.is_empty() {
        return Err(WireError::Rlp("trailing bytes".into()));
    }
    Ok(Envelope {
        head: HeadClaim {
            rollup_id: w.head.rollup_id,
            number: w.head.number,
            block_hash: w.head.block_hash,
            parent_hash: w.head.parent_hash,
            epoch_ms: w.head.epoch_ms,
            counter: w.head.counter,
        },
        signature: w.signature,
        block: w.block,
    })
}

/// Gossipsub message id, as in Ethereum consensus since Altair. It hashes the
/// decompressed bytes, so every Snappy encoding of one envelope has the same id.
pub fn message_id(topic: &str, data: &[u8]) -> [u8; 20] {
    let rlp = decompress(data);
    let (domain, payload) = match &rlp {
        Ok(rlp) => (MESSAGE_DOMAIN_VALID_SNAPPY, rlp.as_slice()),
        Err(_) => (MESSAGE_DOMAIN_INVALID_SNAPPY, data),
    };
    let digest = Sha256::new()
        .chain_update(domain)
        .chain_update((topic.len() as u64).to_le_bytes())
        .chain_update(topic)
        .chain_update(payload)
        .finalize();
    let mut id = [0; 20];
    id.copy_from_slice(&digest[..20]);
    id
}

/// The RLP inside a message, after the preamble and size checks.
fn decompress(data: &[u8]) -> Result<Vec<u8>, WireError> {
    uncompressed_len(data)?;
    snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|e| WireError::Snappy(e.to_string()))
}

/// The uncompressed length from the Snappy preamble, a little-endian base-128 varint.
/// Only its minimal encoding is accepted; snap alone also takes padded ones.
fn uncompressed_len(data: &[u8]) -> Result<usize, WireError> {
    let mut len = 0;
    for (i, &byte) in data.iter().take(MAX_PREAMBLE_LEN).enumerate() {
        len |= usize::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            if i > 0 && byte == 0 {
                return Err(WireError::Snappy("non-minimal length preamble".into()));
            }
            if len > MAX_MESSAGE_SIZE {
                return Err(WireError::TooLarge(len));
            }
            return Ok(len);
        }
    }
    Err(WireError::Snappy(
        "truncated or oversized length preamble".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(block: Bytes) -> Envelope {
        Envelope {
            head: HeadClaim {
                rollup_id: 1,
                number: 9,
                block_hash: B256::repeat_byte(1),
                parent_hash: B256::repeat_byte(2),
                epoch_ms: 100,
                counter: 3,
            },
            signature: Bytes::from(vec![5u8; 65]),
            block,
        }
    }

    #[test]
    fn wire_round_trips() {
        let e = envelope(Bytes::from(vec![0xc0, 1, 2, 3]));
        assert_eq!(decode(&encode(&e).unwrap()).unwrap(), e);
    }

    #[test]
    fn layout_is_head_signature_block() {
        let rlp = snap::raw::Decoder::new()
            .decompress_vec(&encode(&envelope(Bytes::from(vec![0xc0]))).unwrap())
            .unwrap();
        let outer = alloy_rlp::Header::decode(&mut rlp.as_slice()).unwrap();
        assert!(outer.list);
        let mut payload = &rlp[rlp.len() - outer.payload_length..];
        let head = alloy_rlp::Header::decode(&mut payload).unwrap();
        assert!(head.list);
        payload = &payload[head.payload_length..];
        assert_eq!(Bytes::decode(&mut payload).unwrap().len(), 65);
        assert_eq!(
            Bytes::decode(&mut payload).unwrap(),
            Bytes::from(vec![0xc0])
        );
        assert!(payload.is_empty());
    }

    /// The test vector in `docs/sequencer-p2p-spec.md`; change both together.
    #[test]
    fn spec_vector() {
        use alloy_consensus::{Header, constants::EMPTY_ROOT_HASH};
        use alloy_primitives::{address, b256, hex};
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;
        use eez_seq_sig::HeadDomain;

        let key: PrivateKeySigner =
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();
        let domain = HeadDomain {
            l2_chain_id: 1337,
            registry: address!("5FbDB2315678afecb367f032d93F642f64180aa3"),
            rollup_id: 1,
        };
        // An empty block in the shape every fork at genesis gives: Prague fields, no withdrawals.
        let block = eez_primitives::Block {
            header: Header {
                parent_hash: B256::repeat_byte(0xaa),
                beneficiary: key.address(),
                state_root: B256::repeat_byte(0xcc),
                number: 7,
                gas_limit: 30_000_000,
                timestamp: 1_700_000_000,
                base_fee_per_gas: Some(7),
                withdrawals_root: Some(EMPTY_ROOT_HASH),
                blob_gas_used: Some(0),
                excess_blob_gas: Some(0),
                parent_beacon_block_root: Some(B256::ZERO),
                // SHA-256 of nothing: no execution-layer requests.
                requests_hash: Some(b256!(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                )),
                ..Default::default()
            },
            body: eez_primitives::BlockBody {
                withdrawals: Some(Default::default()),
                ..Default::default()
            },
        };
        let block_rlp = alloy_rlp::encode(&block);
        assert_eq!(block_rlp.len(), 612);
        // The header list, then empty transactions, ommers and withdrawals.
        assert!(block_rlp.starts_with(&hex!("f90261f9025b")));
        assert!(block_rlp.ends_with(&hex!("c0c0c0")));
        let head = HeadClaim {
            rollup_id: 1,
            number: 7,
            block_hash: block.header.hash_slow(),
            parent_hash: B256::repeat_byte(0xaa),
            epoch_ms: 1_700_000_000_000,
            counter: 1,
        };
        assert_eq!(
            head.block_hash,
            b256!("d122d407ef1f4b4722b48ba4dd225fa5cba50a2b95a1265585060f0464dcce25")
        );
        assert_eq!(
            key.address(),
            address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
        );
        assert_eq!(
            domain.eip712().separator(),
            b256!("7bcd13c41deec50e6183399b45bf4a4676869087cef8d68623fe7ff72b7f8f9c")
        );
        let hash = head.signing_hash(&domain);
        assert_eq!(
            hash,
            b256!("14fb14f7072872b45edee4f4136fb40bce291cc9baacd928b65157800681e090")
        );
        let signature = key.sign_hash_sync(&hash).unwrap().as_bytes();
        assert_eq!(
            signature,
            hex!(
                "b20d4045602cae51e6f1643093554ef17df9768a187ff79de73bce7eb5d3ac24"
                "615548d39c40635eaa905b70dbde2c4c41a37339564e7026e16600a0e65a2932"
                "1b"
            )
        );
        let envelope = Envelope {
            head,
            signature: Bytes::from(signature.to_vec()),
            block: Bytes::from(block_rlp.clone()),
        };
        assert!(envelope.clone().verify(&domain, key.address()).is_ok());
        let data = encode(&envelope).unwrap();
        let rlp = snap::raw::Decoder::new().decompress_vec(&data).unwrap();
        let prefix = hex!(
            "f902f8f84c0107"
            "a0d122d407ef1f4b4722b48ba4dd225fa5cba50a2b95a1265585060f0464dcce25"
            "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "86018bcfe5680001"
            "b841b20d4045602cae51e6f1643093554ef17df9768a187ff79de73bce7eb5d3ac24"
            "615548d39c40635eaa905b70dbde2c4c41a37339564e7026e16600a0e65a29321b"
            "b90264"
        );
        assert_eq!(rlp, [&prefix[..], &block_rlp].concat());
        let topic = crate::topic(domain.l2_chain_id, domain.rollup_id);
        assert_eq!(topic, "/eez/1337/1/heads/0");
        assert_eq!(
            message_id(&topic, &data),
            hex!("81d7c145eb07b6c067870afb26ccda4aabdff8e7")
        );
    }

    #[test]
    fn padded_preamble_is_rejected() {
        let data = encode(&envelope(Bytes::from(vec![0xc0]))).unwrap();
        // A zero continuation byte pads the varint without changing its value.
        let end = data.iter().position(|b| b & 0x80 == 0).unwrap() + 1;
        let mut padded = data[..end].to_vec();
        padded[end - 1] |= 0x80;
        padded.push(0);
        padded.extend_from_slice(&data[end..]);
        assert!(snap::raw::Decoder::new().decompress_vec(&padded).is_ok());
        assert!(matches!(decode(&padded), Err(WireError::Snappy(_))));
        for truncated in [&[][..], &[0x94], &[0xff; 4]] {
            assert!(matches!(decode(truncated), Err(WireError::Snappy(_))));
        }
    }

    #[test]
    fn every_snappy_encoding_has_one_id() {
        let topic = "/eez/1/1/heads/0";
        let data = encode(&envelope(Bytes::from(vec![0xc0]))).unwrap();
        let rlp = snap::raw::Decoder::new().decompress_vec(&data).unwrap();
        // The same RLP as one Snappy literal: the same preamble, tag 60 << 2, then the
        // length minus one in one byte.
        let preamble = data.iter().position(|b| b & 0x80 == 0).unwrap() + 1;
        let mut literal = data[..preamble].to_vec();
        literal.extend([60 << 2, u8::try_from(rlp.len() - 1).unwrap()]);
        literal.extend_from_slice(&rlp);
        assert_ne!(literal, data);
        assert_eq!(decode(&literal).unwrap(), decode(&data).unwrap());
        assert_eq!(message_id(topic, &literal), message_id(topic, &data));

        // Data that fails the Snappy checks is hashed as is, under the other domain.
        let invalid = &data[1..];
        assert!(decode(invalid).is_err());
        let expected = Sha256::new()
            .chain_update(MESSAGE_DOMAIN_INVALID_SNAPPY)
            .chain_update(16u64.to_le_bytes())
            .chain_update(topic)
            .chain_update(invalid)
            .finalize();
        assert_eq!(message_id(topic, invalid), expected[..20]);
    }

    #[test]
    fn oversize_is_rejected() {
        let big = envelope(Bytes::from(vec![0u8; MAX_MESSAGE_SIZE + 1]));
        assert!(matches!(encode(&big), Err(WireError::TooLarge(_))));
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&vec![0u8; MAX_MESSAGE_SIZE + 1])
            .unwrap();
        assert!(matches!(decode(&compressed), Err(WireError::TooLarge(_))));
    }
}
