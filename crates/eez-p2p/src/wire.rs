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
