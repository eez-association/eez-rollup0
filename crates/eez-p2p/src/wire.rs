use alloy_primitives::{B256, Bytes};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use eez_seq_sig::{Envelope, HeadClaim};

/// Cap on the uncompressed message, checked before allocating.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

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
    // Empty means no block.
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
        block: envelope.block.clone().unwrap_or_default(),
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
    let len = snap::raw::decompress_len(data).map_err(|e| WireError::Snappy(e.to_string()))?;
    if len > MAX_MESSAGE_SIZE {
        return Err(WireError::TooLarge(len));
    }
    let rlp = snap::raw::Decoder::new()
        .decompress_vec(data)
        .map_err(|e| WireError::Snappy(e.to_string()))?;
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
        block: (!w.block.is_empty()).then_some(w.block),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(block: Option<Bytes>) -> Envelope {
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
        for block in [None, Some(Bytes::from(vec![0xc0, 1, 2, 3]))] {
            let e = envelope(block);
            assert_eq!(decode(&encode(&e).unwrap()).unwrap(), e);
        }
    }

    #[test]
    fn layout_is_head_signature_block() {
        let rlp = snap::raw::Decoder::new()
            .decompress_vec(&encode(&envelope(None)).unwrap())
            .unwrap();
        let outer = alloy_rlp::Header::decode(&mut rlp.as_slice()).unwrap();
        assert!(outer.list);
        let mut payload = &rlp[rlp.len() - outer.payload_length..];
        let head = alloy_rlp::Header::decode(&mut payload).unwrap();
        assert!(head.list);
        payload = &payload[head.payload_length..];
        assert_eq!(Bytes::decode(&mut payload).unwrap().len(), 65);
        assert_eq!(Bytes::decode(&mut payload).unwrap().len(), 0);
        assert!(payload.is_empty());
    }

    /// The test vector in `docs/sequencer-p2p-spec.md`; change both together.
    #[test]
    fn spec_vector() {
        use alloy_primitives::{address, b256, hex};
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;
        use eez_seq_sig::HeadDomain;
        use sha2::{Digest, Sha256};

        let key: PrivateKeySigner =
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();
        let domain = HeadDomain {
            l2_chain_id: 1337,
            registry: address!("5FbDB2315678afecb367f032d93F642f64180aa3"),
            rollup_id: 1,
        };
        let head = HeadClaim {
            rollup_id: 1,
            number: 7,
            block_hash: B256::repeat_byte(0xbb),
            parent_hash: B256::repeat_byte(0xaa),
            epoch_ms: 1_700_000_000_000,
            counter: 1,
        };
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
            b256!("876ed57393bba41ad8986fcdf6d080462b817110409e235077e7d1544f83247e")
        );
        let signature = key.sign_hash_sync(&hash).unwrap().as_bytes();
        assert_eq!(
            signature,
            hex!(
                "5814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e"
                "3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa658"
                "1b"
            )
        );
        let data = encode(&Envelope {
            head,
            signature: Bytes::from(signature.to_vec()),
            block: None,
        })
        .unwrap();
        let rlp = snap::raw::Decoder::new().decompress_vec(&data).unwrap();
        assert_eq!(
            rlp,
            hex!(
                "f892f84c0107"
                "a0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                "86018bcfe5680001"
                "b8415814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e"
                "3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa6581b"
                "80"
            )
        );
        assert_eq!(
            data,
            hex!(
                "94011cf892f84c0107a0bb7a010004a0aa7a0100f04b86018bcfe5680001b841"
                "5814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e"
                "3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa658"
                "1b80"
            )
        );
        assert_eq!(
            Sha256::digest(&data).as_slice(),
            hex!("65cd4ff00b3116504c276cbe89085cab1f24b74956fb5cd6f3a621b4f7fbac57")
        );
    }

    #[test]
    fn oversize_is_rejected() {
        let big = envelope(Some(Bytes::from(vec![0u8; MAX_MESSAGE_SIZE + 1])));
        assert!(matches!(encode(&big), Err(WireError::TooLarge(_))));
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&vec![0u8; MAX_MESSAGE_SIZE + 1])
            .unwrap();
        assert!(matches!(decode(&compressed), Err(WireError::TooLarge(_))));
    }
}
