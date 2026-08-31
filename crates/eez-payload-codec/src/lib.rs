//! Batch payload encode/decode per Rollup-1 spec §8.1.
//!
//! ```text
//!   payload := tagByte ‖ rlp([
//!     blockTxCounts,    # list of uint16, length == toBlock - fromBlock + 1
//!                       # entry i = number of user txs in block (fromBlock + i)
//!                       # 0 means empty block
//!     transactions,     # list of bytes, block-major order
//!                       # the first blockTxCounts[0] entries belong to block fromBlock,
//!                       # the next blockTxCounts[1] entries to block fromBlock+1, etc.
//!                       # each entry is a standard RLP-encoded EVM tx
//!     l2_entries,       # list of bytes — ABI-encoded L2-shape
//!                       # ExecutionEntry the L2 system tx
//!                       # (executeIncomingCrossChainCall) consumes.
//!                       # Empty for arbitrary-call batches; populated
//!                       # for value-bearing (deposit/withdrawal) ones.
//!   ])
//!
//!   tagByte:
//!     0x00  the current (and only) format
//! ```
//!
//! Single format on the wire today; the one-byte tag prefix leaves
//! room to add another later without breaking the decoder.
//!
//! `fromBlock` and `toBlock` are **not** in this encoding. Callers supply the
//! absolute block range from their surrounding protocol context; this codec
//! only preserves the number of covered blocks and each block's transaction
//! count.
//!
//! Decode invariants enforced (§8.3):
//!  - `sum(blockTxCounts) == len(transactions)`
//!  - `blockTxCounts[i]` fits in `u16`
//!
//! The codec does **not** validate per-tx fields (§8.4 — that's STF-level).

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use alloy_rlp::{Decodable, Encodable};

/// A raw EIP-2718 signed transaction. Opaque to this crate.
pub type RawTx = Vec<u8>;

/// Tag byte for the current (and only) calldata format. The one-byte
/// prefix lets a future format swap add a new tag without ambiguity;
/// [`decode`] dispatches on it.
pub const TAG_CALLDATA: u8 = 0x00;

/// Convenience [`Result`] alias.
pub type CodecResult<T> = Result<T, CodecError>;

/// Error returned by encode / decode.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Payload was empty or missing the tag byte.
    #[error("payload too short ({0} bytes)")]
    TooShort(usize),
    /// Tag byte didn't match the current format.
    #[error("unsupported tag byte 0x{0:02x}; expected 0x{TAG_CALLDATA:02x}")]
    UnsupportedTag(u8),
    /// RLP decoding failed.
    #[error("rlp decode failed: {0}")]
    Rlp(#[from] alloy_rlp::Error),
    /// A `blockTxCounts[i]` didn't fit in `u16`.
    #[error("block tx count {0} at index {1} exceeds u16::MAX")]
    BlockTxCountOverflow(u64, usize),
    /// `sum(blockTxCounts) != transactions.len()`.
    #[error("tx-count mismatch: sum(blockTxCounts) = {expected}, transactions.len() = {got}")]
    TxCountMismatch { expected: u64, got: u64 },
}

/// A decoded batch payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBatch {
    /// Per-block user-tx counts. Length = number of L2 blocks covered.
    pub block_tx_counts: Vec<u16>,
    /// Flat list of raw EIP-2718 signed transactions in block-major order.
    pub transactions: Vec<RawTx>,
    /// L2-shape `ExecutionEntry` bytes (ABI-encoded). Empty for
    /// arbitrary-call batches; populated for value-bearing ones
    /// (deposits/withdrawals), where the L1 settlement entry does not
    /// contain the exact L2 execution-table shape followers must rebuild.
    pub l2_entries: Vec<Vec<u8>>,
}

impl DecodedBatch {
    /// Number of L2 blocks covered.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.block_tx_counts.len()
    }
}

/// Encode the calldata payload: per-block tx vectors plus L2-shape
/// entries.
///
/// `blocks[i]` is the list of raw signed transactions for the i-th L2
/// block in the range. `l2_entries[i]` is one ABI-encoded
/// `ExecutionEntrySol`; pass `&[]` for arbitrary-call batches that
/// don't need follower-side L2 system-tx reconstruction.
///
/// # Errors
///
/// - [`CodecError::BlockTxCountOverflow`] if any block has > `u16::MAX` txs.
pub fn encode(blocks: &[Vec<RawTx>], l2_entries: &[Vec<u8>]) -> CodecResult<Vec<u8>> {
    let mut block_tx_counts: Vec<u16> = Vec::with_capacity(blocks.len());
    for (i, b) in blocks.iter().enumerate() {
        let n = u64::try_from(b.len()).unwrap_or(u64::MAX);
        let cnt = u16::try_from(b.len()).map_err(|_| CodecError::BlockTxCountOverflow(n, i))?;
        block_tx_counts.push(cnt);
    }
    let transactions: Vec<RawTx> = blocks.iter().flatten().cloned().collect();
    let body = Body {
        block_tx_counts,
        transactions,
        l2_entries: l2_entries.to_vec(),
    };
    let mut buf = Vec::with_capacity(1 + body.length());
    buf.push(TAG_CALLDATA);
    body.encode(&mut buf);
    Ok(buf)
}

/// Decode a calldata payload.
///
/// # Errors
///
/// - [`CodecError::TooShort`] if `payload` is empty.
/// - [`CodecError::UnsupportedTag`] if the leading byte isn't
///   [`TAG_CALLDATA`].
/// - [`CodecError::Rlp`] if the RLP body is malformed.
/// - [`CodecError::TxCountMismatch`] if `sum(blockTxCounts) != transactions.len()`.
pub fn decode(payload: &[u8]) -> CodecResult<DecodedBatch> {
    let Some((&tag, rest)) = payload.split_first() else {
        return Err(CodecError::TooShort(0));
    };
    if tag != TAG_CALLDATA {
        return Err(CodecError::UnsupportedTag(tag));
    }
    let body = Body::decode(&mut &rest[..])?;
    let expected: u64 = body.block_tx_counts.iter().map(|n| u64::from(*n)).sum();
    let got = body.transactions.len() as u64;
    if expected != got {
        return Err(CodecError::TxCountMismatch { expected, got });
    }
    Ok(DecodedBatch {
        block_tx_counts: body.block_tx_counts,
        transactions: body.transactions,
        l2_entries: body.l2_entries,
    })
}

#[derive(Debug, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
struct Body {
    block_tx_counts: Vec<u16>,
    transactions: Vec<RawTx>,
    l2_entries: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn tx(byte: u8, n: usize) -> RawTx {
        vec![byte; n]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Every supported payload shape round-trips without losing empty
        /// blocks, transaction boundaries, or L2 sidecar entries.
        #[test]
        fn arbitrary_payload_round_trips(
            blocks in proptest::collection::vec(
                proptest::collection::vec(
                    proptest::collection::vec(any::<u8>(), 0..128),
                    0..8,
                ),
                0..12,
            ),
            l2_entries in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..256),
                0..12,
            ),
        ) {
            let encoded = encode(&blocks, &l2_entries).expect("generated block sizes fit u16");
            let decoded = decode(&encoded).expect("encoded payload must decode");
            let expected_counts = blocks.iter().map(|block| block.len() as u16).collect::<Vec<_>>();
            let expected_txs = blocks.iter().flatten().cloned().collect::<Vec<_>>();

            prop_assert_eq!(decoded.block_tx_counts, expected_counts);
            prop_assert_eq!(decoded.transactions, expected_txs);
            prop_assert_eq!(decoded.l2_entries, l2_entries);
        }

        /// Arbitrary hostile input is a total operation. A malformed payload
        /// may return any documented error, but it must never panic.
        #[test]
        fn arbitrary_bytes_never_panic(
            bytes in proptest::collection::vec(any::<u8>(), 0..4096),
        ) {
            let outcome = std::panic::catch_unwind(|| decode(&bytes));
            prop_assert!(outcome.is_ok(), "decode panicked for {} bytes", bytes.len());
        }
    }

    /// Wire bytes are consensus-facing. These vectors deliberately use opaque
    /// transaction and L2-entry bytes: the codec must preserve their boundaries
    /// without interpreting either payload.
    #[test]
    fn payload_codec_golden_vectors_cover_all_wire_shapes() {
        struct Vector {
            name: &'static str,
            blocks: Vec<Vec<RawTx>>,
            l2_entries: Vec<Vec<u8>>,
            encoded_hex: &'static str,
        }

        let vectors = [
            Vector {
                name: "empty range",
                blocks: vec![],
                l2_entries: vec![],
                encoded_hex: "00c3c0c0c0",
            },
            Vector {
                name: "one empty block, no L2 entries",
                blocks: vec![vec![]],
                l2_entries: vec![],
                encoded_hex: "00c4c180c0c0",
            },
            Vector {
                name: "mixed blocks and value-bearing L2 entry sidecars",
                blocks: vec![
                    vec![vec![0x01]],
                    vec![],
                    vec![vec![0x82, 0xab, 0xcd], vec![0x7f]],
                ],
                l2_entries: vec![vec![], vec![0xde, 0xad, 0xbe, 0xef], vec![0x00]],
                encoded_hex: "00d3c3018002c6018382abcd7fc78084deadbeef00",
            },
        ];

        for vector in vectors {
            let expected = alloy_primitives::hex::decode(vector.encoded_hex).unwrap();
            let encoded = encode(&vector.blocks, &vector.l2_entries).unwrap();
            assert_eq!(encoded, expected, "{} encoding drifted", vector.name);

            let decoded = decode(&expected).unwrap();
            assert_eq!(
                decoded.block_tx_counts,
                vector
                    .blocks
                    .iter()
                    .map(|block| u16::try_from(block.len()).unwrap())
                    .collect::<Vec<_>>(),
                "{} block boundaries drifted",
                vector.name,
            );
            assert_eq!(
                decoded.transactions,
                vector.blocks.into_iter().flatten().collect::<Vec<_>>(),
                "{} transaction bytes drifted",
                vector.name,
            );
            assert_eq!(
                decoded.l2_entries, vector.l2_entries,
                "{} L2 entry bytes drifted",
                vector.name,
            );
        }
    }

    #[test]
    fn roundtrip_single_block_with_two_txs() {
        let blocks = vec![vec![tx(0xa1, 64), tx(0xa2, 32)]];
        let bytes = encode(&blocks, &[]).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.block_tx_counts, vec![2u16]);
        assert_eq!(decoded.transactions.len(), 2);
        assert!(decoded.l2_entries.is_empty());
    }

    #[test]
    fn roundtrip_multi_block_with_empties() {
        let blocks = vec![
            vec![tx(0x01, 8)],
            vec![],
            vec![tx(0x02, 16), tx(0x03, 16), tx(0x04, 16)],
            vec![],
            vec![tx(0x05, 24)],
        ];
        let bytes = encode(&blocks, &[]).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.block_tx_counts, vec![1, 0, 3, 0, 1]);
        assert_eq!(decoded.transactions.len(), 5);
        assert_eq!(decoded.block_count(), 5);
    }

    #[test]
    fn tag_byte_is_first_byte() {
        let bytes = encode(&[vec![]], &[]).unwrap();
        assert_eq!(bytes[0], TAG_CALLDATA);
    }

    #[test]
    fn block_overflow_rejected_on_encode() {
        let blocks = vec![vec![tx(0, 1); usize::from(u16::MAX) + 1]];
        let err = encode(&blocks, &[]).unwrap_err();
        assert!(matches!(err, CodecError::BlockTxCountOverflow(_, 0)));
    }

    #[test]
    fn too_short_rejected_on_decode() {
        assert!(matches!(decode(&[]).unwrap_err(), CodecError::TooShort(_)));
    }

    #[test]
    fn unsupported_tag_rejected_on_decode() {
        assert!(matches!(
            decode(&[0xff]).unwrap_err(),
            CodecError::UnsupportedTag(0xff)
        ));
    }

    #[test]
    fn corrupt_rlp_rejected_on_decode() {
        assert!(matches!(
            decode(&[TAG_CALLDATA, 0x99, 0x99]).unwrap_err(),
            CodecError::Rlp(_)
        ));
    }

    #[test]
    fn roundtrip_with_l2_entries() {
        // Value-bearing batch: L2 entries survive the round-trip
        // alongside per-block tx data.
        let blocks = vec![vec![], vec![tx(0xa1, 32)], vec![]];
        let l2_entries: Vec<Vec<u8>> = vec![
            vec![0xde, 0xad, 0xbe, 0xef],
            vec![0xca, 0xfe, 0xba, 0xbe, 0x00, 0x11, 0x22, 0x33],
        ];
        let bytes = encode(&blocks, &l2_entries).unwrap();
        assert_eq!(bytes[0], TAG_CALLDATA);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded.block_tx_counts, vec![0u16, 1u16, 0u16]);
        assert_eq!(decoded.transactions.len(), 1);
        assert_eq!(decoded.l2_entries, l2_entries);
    }
}
