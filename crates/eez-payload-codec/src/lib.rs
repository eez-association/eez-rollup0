//! Rollup0 DA payload — `native_block_span_v0` (spec Appendix G).
//!
//! ```text
//!   operations := 0x00 ‖ NativeBlockSpan
//!
//!   NativeBlockSpan {
//!     block_count                              # uvarint32, > 0
//!     pure_transaction_counts[block_count]     # uvarint32, 0 permitted
//!     beneficiary_runs                         # until sum == block_count
//!     extra_data_runs                          # until sum == block_count
//!     pure_transaction_lengths[T]              # uvarint32, non-zero
//!     pure_transaction_bytes                   # sum(lengths) bytes, exactly
//!   }
//!
//!   BeneficiaryRun { run_length: uvarint32 > 0, beneficiary: [u8; 20] }
//!   ExtraDataRun   { run_length: uvarint32 > 0, len: u8 ≤ 32, bytes: [u8; len] }
//! ```
//!
//! Columnar, not record-per-block: the count vector defines every block
//! boundary (a zero keeps an empty block in place), and like fields sit
//! adjacent so a later payload version can compress them.
//!
//! The span carries only the PURE-L2 prefix. Protocol-derived transactions are
//! reconstructed from the authenticated L1 action data, never carried here.
//! Block numbers and timestamps are omitted — they follow from the settled
//! parent and the fixed block cadence.
//!
//! V0 applies no compression: every byte after the version belongs to a span
//! field, and a body that looks like a zlib or Brotli header is decoded as span
//! fields anyway. A compressed format takes a different version byte.
//!
//! The version byte is a Rollup0-local namespace, independent of the EEZ
//! stream's own version. An empty payload or any other first byte is invalid —
//! a decoder never guesses a format or treats an unknown version as empty.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod container;

pub use container::{Action, DecodedContainer, STREAM_VERSION, decode_container, encode_container};

/// A raw EIP-2718 signed transaction, exactly as it enters the block's
/// transaction trie. Opaque to this crate: the span never reserializes or
/// field-decomposes it, so a future transaction type needs no codec change.
pub type RawTx = Vec<u8>;

/// Payload version selecting `native_block_span_v0`.
pub const PAYLOAD_VERSION_V0: u8 = 0x00;

/// Protocol cap on a block's `extraData`.
pub const MAX_EXTRA_DATA: usize = 32;

/// Largest `uvarint32` encoding, in bytes.
const MAX_UVARINT32_BYTES: usize = 5;

/// Convenience [`Result`] alias.
pub type CodecResult<T> = Result<T, CodecError>;

/// One L2 block's span-carried inputs.
///
/// The composer picks `beneficiary` and `extra_data` per block, so both are
/// carried rather than assumed; everything else about the block follows from
/// the settled parent and execution.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpanBlock {
    /// The block's exact beneficiary. The zero address is valid.
    pub beneficiary: [u8; 20],
    /// The block's exact `extraData`, 0 to [`MAX_EXTRA_DATA`] bytes.
    pub extra_data: Vec<u8>,
    /// Pure-L2 transactions, in block order. Empty encodes an empty block.
    pub transactions: Vec<RawTx>,
}

/// Error returned by encode / decode.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// Payload was empty, so it carries no version byte.
    #[error("payload is empty")]
    Empty,
    /// A version byte did not select a known format. Raised by the stream
    /// (expects [`STREAM_VERSION`]) and by the span (expects
    /// [`PAYLOAD_VERSION_V0`]), so it reports what it read rather than
    /// claiming one layer's expectation for the other's.
    #[error("unsupported version byte 0x{0:02x}")]
    UnsupportedVersion(u8),
    /// Body ended inside a field.
    #[error("payload truncated while reading {0}")]
    Truncated(&'static str),
    /// A varint was non-canonical: over-long, padded, or out of range.
    #[error("non-canonical uvarint32 while reading {0}")]
    NonCanonicalVarint(&'static str),
    /// `block_count` was zero — a span covers a non-empty range.
    #[error("block_count is zero")]
    EmptySpan,
    /// A count or length exceeded what the remaining bytes could encode.
    #[error("{what} = {value} exceeds the {remaining} body bytes left to encode it")]
    ImplausibleCount {
        /// Field that failed the bound.
        what: &'static str,
        /// The declared value.
        value: u64,
        /// Body bytes still unread when it was declared.
        remaining: u64,
    },
    /// A run length was zero, or the runs over/undershot `block_count`.
    #[error("{0} runs do not cover exactly block_count")]
    RunCoverage(&'static str),
    /// Two adjacent runs held the same value, so the encoding is not maximal.
    #[error("adjacent {0} runs repeat a value; runs must be maximal")]
    NonMaximalRun(&'static str),
    /// An `extraData` value exceeded [`MAX_EXTRA_DATA`].
    #[error("extra_data length {0} exceeds {MAX_EXTRA_DATA}")]
    ExtraDataTooLong(u8),
    /// A transaction length was zero.
    #[error("transaction {0} has zero length")]
    EmptyTransaction(usize),
    /// `sum(pure_transaction_lengths)` did not consume the body exactly.
    #[error("transaction bytes: {expected} declared, {got} remain")]
    TxByteMismatch {
        /// Sum of the declared lengths.
        expected: u64,
        /// Bytes actually left in the body.
        got: u64,
    },
    /// Bytes remained after the last transaction.
    #[error("{0} trailing bytes after the span")]
    TrailingBytes(usize),
    /// An action's `value` exceeded 32 bytes.
    #[error("action value length {0} exceeds 32")]
    ValueTooLong(usize),
    /// An action's `value` carried a leading zero byte.
    #[error("action value is not minimal big-endian")]
    NonMinimalValue,
    /// A message did not appear where the grammar requires one.
    #[error("expected message type {expected} at this position")]
    UnexpectedMessage {
        /// The type byte the grammar required.
        expected: u8,
    },
    /// A message type byte is not assigned by the format.
    #[error("unknown message type {0}")]
    UnknownMessage(u8),
    /// Rollup0 requires every bracket's `tx_data` to be empty.
    #[error("InitiateCrossChainTransaction.tx_data must be empty")]
    NonEmptyTxData,
    /// A value did not fit the `uvarint32` domain on encode.
    #[error("{what} = {value} exceeds u32")]
    ValueTooLarge {
        /// Field that overflowed.
        what: &'static str,
        /// The offending value.
        value: u64,
    },
}

/// Encode `blocks` as a versioned `native_block_span_v0` payload.
///
/// # Errors
///
/// - [`CodecError::EmptySpan`] if `blocks` is empty.
/// - [`CodecError::ExtraDataTooLong`] if a block's `extra_data` exceeds
///   [`MAX_EXTRA_DATA`].
/// - [`CodecError::ValueTooLarge`] if a count or transaction length exceeds
///   `u32`.
pub fn encode(blocks: &[SpanBlock]) -> CodecResult<Vec<u8>> {
    if blocks.is_empty() {
        return Err(CodecError::EmptySpan);
    }
    let mut out = vec![PAYLOAD_VERSION_V0];
    put_uvarint32(&mut out, count_u32("block_count", blocks.len())?);
    for block in blocks {
        put_uvarint32(
            &mut out,
            count_u32("pure_transaction_counts", block.transactions.len())?,
        );
    }

    // Maximal runs: a repeated beneficiary over a long catch-up range costs one
    // address and one length.
    for (run_length, block) in runs(blocks, |b| &b.beneficiary) {
        put_uvarint32(&mut out, run_length);
        out.extend_from_slice(&block.beneficiary);
    }
    for (run_length, block) in runs(blocks, |b| &b.extra_data) {
        let len = u8::try_from(block.extra_data.len())
            .map_err(|_| CodecError::ExtraDataTooLong(u8::MAX))?;
        if usize::from(len) > MAX_EXTRA_DATA {
            return Err(CodecError::ExtraDataTooLong(len));
        }
        put_uvarint32(&mut out, run_length);
        out.push(len);
        out.extend_from_slice(&block.extra_data);
    }

    let transactions = blocks.iter().flat_map(|b| &b.transactions);
    for tx in transactions.clone() {
        put_uvarint32(&mut out, count_u32("pure_transaction_lengths", tx.len())?);
    }
    for tx in transactions {
        out.extend_from_slice(tx);
    }
    Ok(out)
}

/// A decoded span, kept columnar: derivation walks the flat transaction
/// sequence and slices it with [`Self::block_tx_counts`], so the bytes are
/// never copied into per-block vectors.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodedSpan {
    /// Pure-L2 transaction count per block. Length = number of blocks.
    pub block_tx_counts: Vec<u32>,
    /// Each block's beneficiary, one per block.
    pub beneficiaries: Vec<[u8; 20]>,
    /// Each block's `extraData`, one per block.
    pub extra_data: Vec<Vec<u8>>,
    /// Every pure-L2 transaction, in block-major order.
    pub transactions: Vec<RawTx>,
}

impl DecodedSpan {
    /// Number of L2 blocks the span covers.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.block_tx_counts.len()
    }
}

/// Decode a versioned `native_block_span_v0` payload.
///
/// Every column is checked against the bytes that remain before anything is
/// allocated for it, and the body must be consumed exactly — a span with
/// trailing bytes is invalid rather than truncated silently.
///
/// # Errors
///
/// [`CodecError`] describes the first rule the payload broke.
pub fn decode(payload: &[u8]) -> CodecResult<DecodedSpan> {
    let Some((&version, body)) = payload.split_first() else {
        return Err(CodecError::Empty);
    };
    if version != PAYLOAD_VERSION_V0 {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let mut cur = Cursor::new(body);

    let block_count = cur.uvarint32("block_count")?;
    if block_count == 0 {
        return Err(CodecError::EmptySpan);
    }
    // Every declared block costs at least its own count varint byte, so this
    // rejects an impossible span before allocating a count-sized vector.
    cur.check_plausible("block_count", u64::from(block_count), 1)?;

    let mut counts = Vec::with_capacity(block_count as usize);
    let mut total_txs: u64 = 0;
    for _ in 0..block_count {
        let count = cur.uvarint32("pure_transaction_counts")?;
        total_txs =
            total_txs
                .checked_add(u64::from(count))
                .ok_or(CodecError::ImplausibleCount {
                    what: "pure_transaction_counts",
                    value: u64::from(count),
                    remaining: cur.remaining() as u64,
                })?;
        counts.push(count);
    }

    let beneficiaries = decode_runs(&mut cur, block_count, "beneficiary", |cur| {
        cur.take_array::<20>("beneficiary")
    })?;
    let extra_data = decode_runs(&mut cur, block_count, "extra_data", |cur| {
        let len = cur.byte("extra_data_length")?;
        if usize::from(len) > MAX_EXTRA_DATA {
            return Err(CodecError::ExtraDataTooLong(len));
        }
        Ok(cur.take("extra_data", usize::from(len))?.to_vec())
    })?;

    // Each transaction needs at least one length byte and, since zero length is
    // invalid, at least one content byte.
    cur.check_plausible("pure_transaction_counts", total_txs, 2)?;
    let mut lengths = Vec::with_capacity(total_txs as usize);
    let mut total_bytes: u64 = 0;
    for i in 0..total_txs {
        let len = cur.uvarint32("pure_transaction_lengths")?;
        if len == 0 {
            return Err(CodecError::EmptyTransaction(i as usize));
        }
        total_bytes =
            total_bytes
                .checked_add(u64::from(len))
                .ok_or(CodecError::TxByteMismatch {
                    expected: u64::MAX,
                    got: cur.remaining() as u64,
                })?;
        lengths.push(len as usize);
    }
    if total_bytes != cur.remaining() as u64 {
        return Err(CodecError::TxByteMismatch {
            expected: total_bytes,
            got: cur.remaining() as u64,
        });
    }

    let mut transactions = Vec::with_capacity(lengths.len());
    for len in lengths {
        transactions.push(cur.take("pure_transaction_bytes", len)?.to_vec());
    }
    if cur.remaining() != 0 {
        return Err(CodecError::TrailingBytes(cur.remaining()));
    }
    Ok(DecodedSpan {
        block_tx_counts: counts,
        beneficiaries,
        extra_data,
        transactions,
    })
}

/// Maximal runs of `blocks` by the key `of`, as `(run_length, first block)`.
fn runs<'a, K: PartialEq + ?Sized + 'a>(
    blocks: &'a [SpanBlock],
    of: impl Fn(&'a SpanBlock) -> &'a K,
) -> Vec<(u32, &'a SpanBlock)> {
    let mut out: Vec<(u32, &SpanBlock)> = Vec::new();
    for block in blocks {
        match out.last_mut() {
            Some((run_length, first)) if of(first) == of(block) => *run_length += 1,
            _ => out.push((1, block)),
        }
    }
    out
}

/// Expand a run column into one value per block, enforcing positive lengths,
/// exact coverage of `block_count`, and maximal (non-repeating) runs.
fn decode_runs<T: PartialEq + Clone>(
    cur: &mut Cursor<'_>,
    block_count: u32,
    what: &'static str,
    mut value: impl FnMut(&mut Cursor<'_>) -> CodecResult<T>,
) -> CodecResult<Vec<T>> {
    let mut out: Vec<T> = Vec::with_capacity(block_count as usize);
    let mut previous: Option<T> = None;
    while (out.len() as u32) < block_count {
        let run_length = cur.uvarint32(what)?;
        if run_length == 0 {
            return Err(CodecError::RunCoverage(what));
        }
        let value = value(cur)?;
        if previous.as_ref() == Some(&value) {
            return Err(CodecError::NonMaximalRun(what));
        }
        // Overshoot is a coverage failure, not a truncation.
        if out.len() as u64 + u64::from(run_length) > u64::from(block_count) {
            return Err(CodecError::RunCoverage(what));
        }
        out.extend(std::iter::repeat_n(value.clone(), run_length as usize));
        previous = Some(value);
    }
    Ok(out)
}

/// Reader over the span body, tracking how much is left so a declared count can
/// be checked before it is trusted.
pub(crate) struct Cursor<'a> {
    body: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) const fn new(body: &'a [u8]) -> Self {
        Self { body, at: 0 }
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.body.len() - self.at
    }

    pub(crate) fn take(&mut self, what: &'static str, len: usize) -> CodecResult<&'a [u8]> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(CodecError::Truncated(what))?;
        let out = self
            .body
            .get(self.at..end)
            .ok_or(CodecError::Truncated(what))?;
        self.at = end;
        Ok(out)
    }

    pub(crate) fn take_array<const N: usize>(
        &mut self,
        what: &'static str,
    ) -> CodecResult<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(what, N)?);
        Ok(out)
    }

    pub(crate) fn byte(&mut self, what: &'static str) -> CodecResult<u8> {
        Ok(self.take(what, 1)?[0])
    }

    /// Read one shortest-form protobuf-style `uvarint32`.
    ///
    /// Rejects a truncated continuation, more than five bytes, a fifth byte
    /// above `0x0f`, and a multi-byte encoding whose final group is zero —
    /// so every value has exactly one encoding.
    pub(crate) fn uvarint32(&mut self, what: &'static str) -> CodecResult<u32> {
        let mut value: u64 = 0;
        for i in 0..MAX_UVARINT32_BYTES {
            let byte = self.byte(what)?;
            let group = u64::from(byte & 0x7f);
            if i == MAX_UVARINT32_BYTES - 1 && group > 0x0f {
                return Err(CodecError::NonCanonicalVarint(what));
            }
            value |= group << (7 * i);
            if byte & 0x80 == 0 {
                // A multi-byte encoding whose last group is zero is padded.
                if i > 0 && group == 0 {
                    return Err(CodecError::NonCanonicalVarint(what));
                }
                return u32::try_from(value).map_err(|_| CodecError::NonCanonicalVarint(what));
            }
        }
        Err(CodecError::NonCanonicalVarint(what))
    }

    /// Reject a declared count that the remaining bytes cannot encode, before
    /// allocating for it. `per_element` is the minimum bytes each element costs.
    pub(crate) fn check_plausible(
        &self,
        what: &'static str,
        value: u64,
        per_element: u64,
    ) -> CodecResult<()> {
        let remaining = self.remaining() as u64;
        if value > remaining / per_element {
            return Err(CodecError::ImplausibleCount {
                what,
                value,
                remaining,
            });
        }
        Ok(())
    }
}

pub(crate) fn put_uvarint32(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn count_u32(what: &'static str, value: usize) -> CodecResult<u32> {
    u32::try_from(value).map_err(|_| CodecError::ValueTooLarge {
        what,
        value: value as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(byte: u8, n: usize) -> RawTx {
        vec![byte; n]
    }

    fn block(beneficiary: u8, extra: &[u8], txs: Vec<RawTx>) -> SpanBlock {
        SpanBlock {
            beneficiary: [beneficiary; 20],
            extra_data: extra.to_vec(),
            transactions: txs,
        }
    }

    /// Decode back into the per-block shape the encoder took, so a round-trip
    /// test compares like with like.
    fn decode_blocks(payload: &[u8]) -> CodecResult<Vec<SpanBlock>> {
        let mut span = decode(payload)?;
        let mut txs = std::mem::take(&mut span.transactions).into_iter();
        Ok((0..span.block_count())
            .map(|i| SpanBlock {
                beneficiary: span.beneficiaries[i],
                extra_data: span.extra_data[i].clone(),
                transactions: txs
                    .by_ref()
                    .take(span.block_tx_counts[i] as usize)
                    .collect(),
            })
            .collect())
    }

    #[test]
    fn round_trips_a_span_with_empty_blocks() {
        // A zero count keeps an empty block in place rather than omitting it.
        let blocks = vec![
            block(0xaa, b"eez", vec![tx(0x01, 8)]),
            block(0xaa, b"eez", vec![]),
            block(0xaa, b"eez", vec![tx(0x02, 16), tx(0x03, 16)]),
        ];
        let encoded = encode(&blocks).unwrap();
        assert_eq!(encoded[0], PAYLOAD_VERSION_V0);
        assert_eq!(decode_blocks(&encoded).unwrap(), blocks);
    }

    #[test]
    fn round_trips_per_block_beneficiary_and_extra_data() {
        // Both vary per block, which is exactly why the span carries them.
        let blocks = vec![
            block(0x01, b"", vec![]),
            block(0x02, &[0xff; MAX_EXTRA_DATA], vec![tx(0x09, 4)]),
            block(0x02, b"tail", vec![]),
        ];
        assert_eq!(decode_blocks(&encode(&blocks).unwrap()).unwrap(), blocks);
    }

    /// A repeated value costs one run, not one copy per block.
    #[test]
    fn repeated_values_collapse_into_one_run() {
        let flat: Vec<SpanBlock> = (0..64).map(|_| block(0xaa, b"eez", vec![])).collect();
        let varying: Vec<SpanBlock> = (0..64u8).map(|i| block(i, b"eez", vec![])).collect();
        let flat_len = encode(&flat).unwrap().len();
        assert!(
            flat_len < encode(&varying).unwrap().len(),
            "run-length encoding must pay off for a repeated beneficiary",
        );
        // version + block_count + 64 zero counts + one 21-byte beneficiary run
        // + one 3-byte extra-data run header + "eez".
        assert_eq!(flat_len, 1 + 1 + 64 + (1 + 20) + (1 + 1 + 3));
    }

    #[test]
    fn an_empty_payload_and_an_unknown_version_are_rejected() {
        assert_eq!(decode(&[]).unwrap_err(), CodecError::Empty);
        assert_eq!(
            decode(&[0x01]).unwrap_err(),
            CodecError::UnsupportedVersion(0x01),
        );
        // A body that looks compressed is still decoded as span fields.
        assert!(matches!(
            decode(&[PAYLOAD_VERSION_V0, 0x78, 0x9c]).unwrap_err(),
            CodecError::ImplausibleCount { .. } | CodecError::Truncated(_),
        ));
    }

    #[test]
    fn a_zero_block_count_is_rejected() {
        assert_eq!(encode(&[]).unwrap_err(), CodecError::EmptySpan);
        assert_eq!(
            decode(&[PAYLOAD_VERSION_V0, 0x00]).unwrap_err(),
            CodecError::EmptySpan
        );
    }

    /// Every value has exactly one encoding: padded, over-long, and
    /// out-of-range varints are all invalid.
    #[test]
    fn non_canonical_varints_are_rejected() {
        for (case, body) in [
            ("padded two-byte zero group", vec![0x81, 0x00]),
            (
                "six-byte continuation",
                vec![0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
            ),
            ("fifth byte over 0x0f", vec![0x80, 0x80, 0x80, 0x80, 0x10]),
        ] {
            let payload = [vec![PAYLOAD_VERSION_V0], body].concat();
            assert_eq!(
                decode(&payload).unwrap_err(),
                CodecError::NonCanonicalVarint("block_count"),
                "{case}",
            );
        }
    }

    /// Runs must be maximal, so a decoded column has exactly one encoding.
    #[test]
    fn adjacent_runs_repeating_a_value_are_rejected() {
        let mut payload = vec![PAYLOAD_VERSION_V0, 0x02, 0x00, 0x00];
        payload.push(0x01); // beneficiary run of 1
        payload.extend_from_slice(&[0xaa; 20]);
        payload.push(0x01); // a second run with the SAME beneficiary
        payload.extend_from_slice(&[0xaa; 20]);
        assert_eq!(
            decode(&payload).unwrap_err(),
            CodecError::NonMaximalRun("beneficiary"),
        );
    }

    #[test]
    fn runs_must_cover_block_count_exactly() {
        let mut payload = vec![PAYLOAD_VERSION_V0, 0x02, 0x00, 0x00];
        payload.push(0x03); // a run of 3 over a 2-block span
        payload.extend_from_slice(&[0xaa; 20]);
        assert_eq!(
            decode(&payload).unwrap_err(),
            CodecError::RunCoverage("beneficiary"),
        );

        let mut zero_run = vec![PAYLOAD_VERSION_V0, 0x01, 0x00];
        zero_run.push(0x00); // a zero-length run
        zero_run.extend_from_slice(&[0xaa; 20]);
        assert_eq!(
            decode(&zero_run).unwrap_err(),
            CodecError::RunCoverage("beneficiary"),
        );
    }

    #[test]
    fn extra_data_over_the_protocol_cap_is_rejected() {
        let too_long = vec![block(0x01, &[0x11; MAX_EXTRA_DATA + 1], vec![])];
        assert_eq!(
            encode(&too_long).unwrap_err(),
            CodecError::ExtraDataTooLong(33),
        );
    }

    /// The length column must consume the body exactly — no zero-length
    /// transaction, no unassigned tail.
    #[test]
    fn the_transaction_columns_must_balance() {
        let blocks = vec![block(0xaa, b"", vec![tx(0x01, 3)])];
        let encoded = encode(&blocks).unwrap();

        let mut trailing = encoded.clone();
        trailing.push(0x00);
        assert_eq!(
            decode(&trailing).unwrap_err(),
            CodecError::TxByteMismatch {
                expected: 3,
                got: 4
            },
        );

        let truncated = &encoded[..encoded.len() - 1];
        assert_eq!(
            decode(truncated).unwrap_err(),
            CodecError::TxByteMismatch {
                expected: 3,
                got: 2
            },
        );

        // Zero length is invalid: it would make a transaction unsliceable.
        let mut zero_len = encoded.clone();
        let len_at = zero_len.len() - 4;
        zero_len[len_at] = 0x00;
        assert_eq!(
            decode(&zero_len).unwrap_err(),
            CodecError::EmptyTransaction(0),
        );
    }

    /// A count larger than the bytes that could encode it is refused before
    /// anything is allocated for it.
    #[test]
    fn an_implausible_count_is_refused_before_allocation() {
        // block_count = 2^28 with a two-byte body left.
        let payload = [PAYLOAD_VERSION_V0, 0x80, 0x80, 0x80, 0x80, 0x01];
        assert!(matches!(
            decode(&payload).unwrap_err(),
            CodecError::ImplausibleCount {
                what: "block_count",
                ..
            },
        ));

        // One block declaring 2^28 transactions, with nothing left to hold them.
        let payload = [
            vec![PAYLOAD_VERSION_V0, 0x01],
            vec![0x80, 0x80, 0x80, 0x80, 0x01],
            vec![0x01],
            vec![0xaa; 20],
            vec![0x01, 0x00],
        ]
        .concat();
        assert!(matches!(
            decode(&payload).unwrap_err(),
            CodecError::ImplausibleCount {
                what: "pure_transaction_counts",
                ..
            },
        ));
    }

    #[test]
    fn transaction_bytes_survive_verbatim() {
        // The span never reserializes a transaction: bytes in, same bytes out.
        let raw: RawTx = vec![0x02, 0xf8, 0x6c, 0x01, 0x80, 0x84, 0x3b, 0x9a, 0xca, 0x00];
        let blocks = vec![block(0x00, b"", vec![raw.clone()])];
        let decoded = decode(&encode(&blocks).unwrap()).unwrap();
        assert_eq!(decoded.transactions[0], raw);
    }

    /// Normative codec vectors from spec Appendix D.4.2. The encoding is
    /// canonical, so each valid vector must both decode to its stated fields
    /// and re-encode to the exact published bytes.
    mod spec_vectors {
        use super::*;
        use alloy_primitives::hex;

        /// D.4.2 Vector 1 — six empty blocks, 31 bytes.
        #[test]
        fn vector_1_six_empty_blocks() {
            let bytes =
                hex::decode("00060000000000000600000000000000000000000000000000000000000600")
                    .unwrap();
            assert_eq!(bytes.len(), 31);

            let span = decode(&bytes).unwrap();
            assert_eq!(span.block_tx_counts, vec![0; 6]);
            assert_eq!(span.beneficiaries, vec![[0u8; 20]; 6]);
            assert_eq!(span.extra_data, vec![Vec::<u8>::new(); 6]);
            assert!(span.transactions.is_empty());

            let blocks = vec![SpanBlock::default(); 6];
            assert_eq!(
                encode(&blocks).unwrap(),
                bytes,
                "must re-encode canonically"
            );
        }

        /// D.4.2 Vector 2 — one EIP-1559 transaction, multiple metadata runs.
        #[test]
        fn vector_2_one_transaction_and_multiple_runs() {
            let bytes = hex::decode(
                "00060100000000000211111111111111111111111111111111111111110422222222222222222222222222222222222222220101aa0202bbcc03006f02f86c820539808405f5e100843b9aca0082520894000000000000000000000000000000000000dead8080c001a04849ec4d7eed2e9eb1da330cdb0a22cdb4c1e32d682556dcc2b491ea5d747dfda05e38e68ce47b2ae70e6b36de8f1e1413df4dbc73a30610675f0a8e8c1505e7a7",
            )
            .unwrap();
            assert_eq!(bytes.len(), 171);

            let span = decode(&bytes).unwrap();
            assert_eq!(span.block_tx_counts, vec![1, 0, 0, 0, 0, 0]);
            assert_eq!(
                span.beneficiaries,
                [[[0x11u8; 20]; 2].as_slice(), [[0x22u8; 20]; 4].as_slice()].concat(),
            );
            assert_eq!(
                span.extra_data,
                vec![
                    vec![0xaa],
                    vec![0xbb, 0xcc],
                    vec![0xbb, 0xcc],
                    vec![],
                    vec![],
                    vec![],
                ],
            );
            assert_eq!(span.transactions.len(), 1);
            assert_eq!(span.transactions[0].len(), 111);
            assert_eq!(span.transactions[0][0], 0x02, "EIP-1559 typed transaction");

            // Re-encode from the per-block shape the vector describes.
            let mut blocks = vec![SpanBlock::default(); 6];
            for (i, b) in blocks.iter_mut().enumerate() {
                b.beneficiary = if i < 2 { [0x11; 20] } else { [0x22; 20] };
                b.extra_data = match i {
                    0 => vec![0xaa],
                    1 | 2 => vec![0xbb, 0xcc],
                    _ => vec![],
                };
            }
            blocks[0].transactions = vec![span.transactions[0].clone()];
            assert_eq!(
                encode(&blocks).unwrap(),
                bytes,
                "must re-encode canonically"
            );
        }

        /// D.4.2 Vectors 3-7 — every one must be rejected, and for the stated
        /// reason.
        #[test]
        fn vectors_3_to_7_are_rejected() {
            for (vector, payload, want) in [
                ("3: empty payload", "", CodecError::Empty),
                (
                    "4: unknown version does not fall back to V0",
                    "01",
                    CodecError::UnsupportedVersion(0x01),
                ),
                (
                    "5: non-shortest block count",
                    "008600",
                    CodecError::NonCanonicalVarint("block_count"),
                ),
                (
                    "6: block count with no bytes left to encode it",
                    "0006",
                    CodecError::ImplausibleCount {
                        what: "block_count",
                        value: 6,
                        remaining: 0,
                    },
                ),
                (
                    "7: trailing byte after the last column",
                    "0006000000000000060000000000000000000000000000000000000000060000",
                    CodecError::TxByteMismatch {
                        expected: 0,
                        got: 1,
                    },
                ),
            ] {
                let bytes = hex::decode(payload).unwrap();
                assert_eq!(decode(&bytes).unwrap_err(), want, "vector {vector}");
            }
        }
    }
}
