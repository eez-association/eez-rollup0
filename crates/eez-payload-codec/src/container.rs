//! The EEZ message stream carried in `batch.callData`.
//!
//! `eez-core-protocol/docs/blobs/BLOB_FORMAT_SPEC.md` defines one continuous
//! stream: the batch's blobs in order, with `callData` appended after the last
//! blob. We publish no blobs yet, so `callData` IS the whole stream — the same
//! format with an empty blob portion, not a framing of our own.
//!
//! ```text
//!   stream := 00                                       # protocol version, never a message
//!           ‖ ChainOperation(rollup_id, operations)      # type 2
//!           ‖ ( Initiate ‖ Call ‖ Return ‖ Finish ) *   # one bracket per action
//! ```
//!
//! `CloseBlobStream` (type 1) ends the blob portion and is invalid inside the
//! `callData` tail, so a zero-blob stream never emits one.
//!
//! Wire conventions (§1.1): scalars little-endian fixed-width, `address` 20
//! bytes, `u256` 32 bytes little-endian, every `bytes` field length-prefixed
//! with a protobuf varint.
//!
//! `operations` is opaque to EEZ — "everything about how `operations` is
//! structured is up to the chain" — so Rollup0's `0x00 ‖ native_block_span_v0`
//! rides inside it verbatim. Nothing here extends it: the span's start comes
//! from the settled parent EEZ names, not from a field of our own.
//!
//! INTERIM CALLDATA PROFILE, not full blob-format compliance: §2.1 and §5
//! cond. 2 want `CloseBlobStream` in the blob portion, cond. 3 forbids it in
//! the tail, and a zero-blob stream has nowhere legal to put it. Open with the
//! spec owner.

use crate::{CodecError, CodecResult, Cursor, DecodedSpan, SpanBlock};

/// First byte of the stream: the EEZ protocol version (§6). Never a message.
pub const STREAM_VERSION: u8 = 0x00;

const MSG_CHAIN_OPERATION: u8 = 2;
const MSG_INITIATE: u8 = 3;
const MSG_CALL: u8 = 4;
const MSG_RETURN_SUCCESS: u8 = 6;
const MSG_RETURN_FAIL: u8 = 7;
const MSG_FINISH: u8 = 10;

/// Width of a `u256` field, in bytes.
const VALUE_BYTES: usize = 32;

/// One cross-chain action, in its semantic form.
///
/// On the wire this is `InitiateCrossChainTransaction` ‖ `Call` ‖
/// `ReturnSuccess`/`ReturnFail` ‖ `FinishCrossChainTransaction`. A `Call`'s
/// `from_chain` is never encoded — it is the executing chain (§1.2) — so it is
/// recovered from the enclosing `Initiate`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Action {
    /// EEZ network the call originates on; the `Initiate`'s `chain_id`.
    pub source_rollup_id: u64,
    /// EEZ network the call targets; the `Call`'s `to_chain`.
    pub target_rollup_id: u64,
    /// Caller on the source network.
    pub source_address: [u8; 20],
    /// Callee on the target network.
    pub target_address: [u8; 20],
    /// Value moved with the call. Big-endian here, little-endian on the wire.
    pub value: [u8; VALUE_BYTES],
    /// Gas limit forwarded to the call.
    pub gas: u64,
    /// The call's exact calldata.
    pub data: Vec<u8>,
    /// `false` selects `ReturnFail` over `ReturnSuccess`.
    pub success: bool,
    /// Return data on success, revert data on failure.
    pub return_data: Vec<u8>,
}

/// A decoded stream: the block span and the actions that settle with it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodedContainer {
    /// Whose operations this stream carries. EEZ's `chain_id` IS the rollup
    /// id (§1.2 folds it into the call hash as `sourceRollupId`). Anything
    /// deriving from `span` must check it.
    pub rollup_id: u64,
    /// The Rollup0 block span.
    pub span: DecodedSpan,
    /// The action manifest, in bracket order, aligned with `entries[]`.
    pub actions: Vec<Action>,
}

/// Encode the span and its actions as an EEZ message stream.
///
/// # Errors
///
/// - [`CodecError::EmptySpan`] if `blocks` is empty.
/// - [`CodecError::ValueTooLarge`] if a `bytes` field exceeds `u32`.
pub fn encode_container(
    rollup_id: u64,
    blocks: &[SpanBlock],
    actions: &[Action],
) -> CodecResult<Vec<u8>> {
    let operations = crate::encode(blocks)?;

    let mut out = vec![STREAM_VERSION];
    out.push(MSG_CHAIN_OPERATION);
    out.extend_from_slice(&rollup_id.to_le_bytes());
    put_bytes(&mut out, &operations)?;

    for action in actions {
        // tx_data is empty: Rollup0 identifies an action by (settlement
        // context, manifest index, call hash), never by a replaceable carrier
        // transaction.
        out.push(MSG_INITIATE);
        out.extend_from_slice(&action.source_rollup_id.to_le_bytes());
        put_bytes(&mut out, &[])?;

        out.push(MSG_CALL);
        out.extend_from_slice(&action.target_rollup_id.to_le_bytes());
        out.extend_from_slice(&action.source_address);
        out.extend_from_slice(&action.target_address);
        out.extend_from_slice(&reversed(&action.value));
        out.extend_from_slice(&action.gas.to_le_bytes());
        put_bytes(&mut out, &action.data)?;

        out.push(if action.success {
            MSG_RETURN_SUCCESS
        } else {
            MSG_RETURN_FAIL
        });
        put_bytes(&mut out, &action.return_data)?;

        out.push(MSG_FINISH);
    }
    Ok(out)
}

/// Decode an EEZ message stream from `batch.callData`.
///
/// A malformed stream is rejected whole (§5): an unknown version, an unknown
/// message type, a truncated field or an unclosed bracket fails the payload,
/// not just the offending suffix.
///
/// # Errors
///
/// [`CodecError`] describes the first rule the stream broke.
pub fn decode_container(payload: &[u8]) -> CodecResult<DecodedContainer> {
    let Some((&version, body)) = payload.split_first() else {
        return Err(CodecError::Empty);
    };
    if version != STREAM_VERSION {
        return Err(CodecError::UnsupportedVersion(version));
    }
    let mut cur = Cursor::new(body);

    // Exactly one Rollup0 ChainOperation opens the stream; a second is invalid
    // rather than concatenated with the first.
    expect_message(&mut cur, MSG_CHAIN_OPERATION)?;
    let rollup_id = u64::from_le_bytes(cur.take_array::<8>("chain_id")?);
    let span = crate::decode(take_bytes(&mut cur, "operations")?)?;

    let mut actions = Vec::new();
    while cur.remaining() != 0 {
        actions.push(decode_action(&mut cur)?);
    }
    Ok(DecodedContainer {
        rollup_id,
        span,
        actions,
    })
}

fn decode_action(cur: &mut Cursor<'_>) -> CodecResult<Action> {
    expect_message(cur, MSG_INITIATE)?;
    let source_rollup_id = u64::from_le_bytes(cur.take_array::<8>("initiate chain_id")?);
    // Generic EEZ leaves `tx_data` chain-defined; Rollup0 V1 requires empty.
    if !take_bytes(cur, "tx_data")?.is_empty() {
        return Err(CodecError::NonEmptyTransactionData);
    }

    expect_message(cur, MSG_CALL)?;
    let target_rollup_id = u64::from_le_bytes(cur.take_array::<8>("to_chain")?);
    let source_address = cur.take_array::<20>("from_address")?;
    let target_address = cur.take_array::<20>("to_address")?;
    let value = reversed(&cur.take_array::<VALUE_BYTES>("value")?);
    let gas = u64::from_le_bytes(cur.take_array::<8>("gas")?);
    let data = take_bytes(cur, "data")?.to_vec();

    let success = match cur.byte("return message")? {
        MSG_RETURN_SUCCESS => true,
        MSG_RETURN_FAIL => false,
        other => return Err(CodecError::UnknownMessage(other)),
    };
    let return_data = take_bytes(cur, "return_data")?.to_vec();
    expect_message(cur, MSG_FINISH)?;

    Ok(Action {
        source_rollup_id,
        target_rollup_id,
        source_address,
        target_address,
        value,
        gas,
        data,
        success,
        return_data,
    })
}

fn expect_message(cur: &mut Cursor<'_>, expected: u8) -> CodecResult<()> {
    if cur.byte("message type")? != expected {
        return Err(CodecError::UnexpectedMessage { expected });
    }
    Ok(())
}

/// A `bytes` field: protobuf varint length, then exactly that many bytes.
fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> CodecResult<()> {
    let len = u32::try_from(value.len()).map_err(|_| CodecError::ValueTooLarge {
        what: "bytes length",
        value: value.len() as u64,
    })?;
    put_varint(out, u64::from(len));
    out.extend_from_slice(value);
    Ok(())
}

fn take_bytes<'a>(cur: &mut Cursor<'a>, what: &'static str) -> CodecResult<&'a [u8]> {
    let len =
        usize::try_from(varint(cur, what)?).map_err(|_| CodecError::NonCanonicalVarint(what))?;
    cur.take(what, len)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// A `bytes` length prefix: a `u32` in one to five bytes (§1.1). Non-minimal
/// encodings stay valid (§5 cond. 8); anything outside that domain does not.
fn varint(cur: &mut Cursor<'_>, what: &'static str) -> CodecResult<u32> {
    let mut value: u64 = 0;
    for i in 0..crate::MAX_UVARINT32_BYTES {
        let byte = cur.byte(what)?;
        let group = u64::from(byte & 0x7f);
        if i == crate::MAX_UVARINT32_BYTES - 1 && group > 0x0f {
            return Err(CodecError::NonCanonicalVarint(what));
        }
        value |= group << (7 * i);
        if byte & 0x80 == 0 {
            return u32::try_from(value).map_err(|_| CodecError::NonCanonicalVarint(what));
        }
    }
    Err(CodecError::NonCanonicalVarint(what))
}

/// The wire carries `u256` little-endian; we hold it big-endian.
fn reversed(value: &[u8; VALUE_BYTES]) -> [u8; VALUE_BYTES] {
    let mut out = *value;
    out.reverse();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROLLUP: u64 = 1;

    fn action(success: bool) -> Action {
        let mut value = [0u8; VALUE_BYTES];
        value[31] = 0x2a;
        Action {
            source_rollup_id: 0,
            target_rollup_id: 1,
            source_address: [0xaa; 20],
            target_address: [0xbb; 20],
            value,
            gas: 21_000,
            data: vec![0x51, 0xdd, 0x0a, 0xf6],
            success,
            return_data: vec![0x01],
        }
    }

    fn span() -> Vec<SpanBlock> {
        vec![
            SpanBlock {
                beneficiary: [0x11; 20],
                extra_data: vec![],
                transactions: vec![vec![0x02, 0xf8, 0x6c]],
            },
            SpanBlock::default(),
        ]
    }

    #[test]
    fn round_trips_span_and_actions() {
        let actions = vec![action(true), action(false)];
        let encoded = encode_container(ROLLUP, &span(), &actions).unwrap();
        let decoded = decode_container(&encoded).unwrap();
        assert_eq!(decoded.span.block_tx_counts, vec![1, 0]);
        assert_eq!(decoded.actions, actions);
        assert_eq!(decoded.rollup_id, ROLLUP);
    }

    #[test]
    fn round_trips_an_empty_manifest() {
        let encoded = encode_container(ROLLUP, &span(), &[]).unwrap();
        assert!(decode_container(&encoded).unwrap().actions.is_empty());
    }

    /// The stream opens exactly as the spec defines: protocol version byte,
    /// then a ChainOperation whose chain_id is little-endian.
    #[test]
    fn the_stream_opens_as_the_spec_defines() {
        let encoded = encode_container(ROLLUP, &span(), &[]).unwrap();
        assert_eq!(encoded[0], STREAM_VERSION, "protocol version");
        assert_eq!(encoded[1], MSG_CHAIN_OPERATION, "ChainOperation is type 2");
        assert_eq!(
            &encoded[2..10],
            &ROLLUP.to_le_bytes(),
            "chain_id little-endian"
        );
    }

    /// The span rides verbatim inside `operations`, so it lifts out unchanged
    /// when the transport moves to blobs.
    #[test]
    fn operations_carries_the_span_verbatim() {
        let blocks = span();
        let encoded = encode_container(ROLLUP, &blocks, &[]).unwrap();
        let raw_span = crate::encode(&blocks).unwrap();
        assert_eq!(
            raw_span[0],
            crate::PAYLOAD_VERSION_V0,
            "published span version"
        );
        assert!(
            encoded.windows(raw_span.len()).any(|w| w == raw_span),
            "the stream must carry the span byte-for-byte",
        );
    }

    /// Outside the `u32` / five-byte domain (§1.1) no conforming decoder
    /// accepts it, unlike a merely non-minimal prefix.
    #[test]
    fn a_length_prefix_outside_the_u32_domain_is_rejected() {
        let encoded = encode_container(ROLLUP, &span(), &[]).unwrap();
        let at = 1 + 1 + 8;

        let mut over_long = encoded.clone();
        over_long.splice(at..=at, [0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
        assert_eq!(
            decode_container(&over_long).unwrap_err(),
            CodecError::NonCanonicalVarint("operations"),
        );

        let mut overflow = encoded.clone();
        overflow.splice(at..=at, [0x80, 0x80, 0x80, 0x80, 0x10]);
        assert_eq!(
            decode_container(&overflow).unwrap_err(),
            CodecError::NonCanonicalVarint("operations"),
        );
    }

    /// §5 cond. 8: a padded length prefix is valid EEZ, so a peer emitting
    /// one must not be rejected.
    #[test]
    fn a_padded_length_prefix_is_accepted() {
        let encoded = encode_container(ROLLUP, &span(), &[]).unwrap();
        // 00 ‖ 02 ‖ chain_id(8) ‖ operations length
        let at = 1 + 1 + 8;
        assert!(encoded[at] < 0x80, "fixture's length must fit one byte");
        let mut padded = encoded.clone();
        padded.splice(at..=at, [encoded[at] | 0x80, 0x00]);

        assert_eq!(
            decode_container(&padded).unwrap().span,
            decode_container(&encoded).unwrap().span,
        );
    }

    /// A stream whose first byte is not a known protocol version is rejected
    /// whole, never parsed under this format.
    #[test]
    fn an_unknown_stream_version_is_rejected() {
        let mut forged = encode_container(ROLLUP, &span(), &[]).unwrap();
        forged[0] = 0x01;
        assert_eq!(
            decode_container(&forged).unwrap_err(),
            CodecError::UnsupportedVersion(0x01),
        );
        assert_eq!(decode_container(&[]).unwrap_err(), CodecError::Empty);
    }

    /// The old RLP payload also began with 0x00, but its next byte is an RLP
    /// list header rather than a ChainOperation.
    #[test]
    fn an_old_rlp_payload_is_rejected() {
        let old = [vec![0x00u8], vec![0xc8, 0x83, 0x01, 0x02, 0x03]].concat();
        assert_eq!(
            decode_container(&old).unwrap_err(),
            CodecError::UnexpectedMessage {
                expected: MSG_CHAIN_OPERATION,
            },
        );
    }

    /// Value is little-endian on the wire and big-endian in memory.
    /// Offset of the first bracket: the stream prefix is byte-identical with
    /// and without actions, so encoding an empty manifest measures it. Scanning
    /// for a type byte would collide with the same value inside the span.
    fn first_bracket_offset() -> usize {
        encode_container(ROLLUP, &span(), &[]).unwrap().len()
    }

    #[test]
    fn value_is_little_endian_on_the_wire() {
        let a = action(true);
        let encoded = encode_container(ROLLUP, &span(), std::slice::from_ref(&a)).unwrap();
        // Initiate: type(1) | chain_id(8) | tx_data len(1) = 10 bytes.
        // Call: type(1) | to_chain(8) | from(20) | to(20) → value at +49.
        let call = first_bracket_offset() + 10;
        assert_eq!(encoded[call], MSG_CALL, "Call follows Initiate");
        assert_eq!(encoded[call + 49], 0x2a, "least-significant byte first");
        assert_eq!(encoded[call + 49 + 31], 0x00, "most-significant byte last");
        assert_eq!(
            decode_container(&encoded).unwrap().actions[0].value,
            a.value
        );
    }

    /// Rollup0 V1 rejects the whole container when `tx_data` is non-empty.
    #[test]
    fn a_non_empty_tx_data_is_rejected() {
        let clean = encode_container(ROLLUP, &span(), &[action(true)]).unwrap();
        let mut forged = clean.clone();
        let at = first_bracket_offset();
        assert_eq!(forged[at], MSG_INITIATE);
        forged[at + 9] = 0x01; // tx_data length 0 -> 1
        forged.insert(at + 10, 0xff);

        assert_eq!(
            decode_container(&forged).unwrap_err(),
            CodecError::NonEmptyTransactionData,
        );
    }

    #[test]
    fn multibyte_tx_data_is_rejected() {
        let mut forged = encode_container(ROLLUP, &span(), &[action(true)]).unwrap();
        let at = first_bracket_offset();
        forged[at + 9] = 0x02;
        forged.splice(at + 10..at + 10, [0xde, 0xad]);

        assert_eq!(
            decode_container(&forged).unwrap_err(),
            CodecError::NonEmptyTransactionData,
        );
    }

    #[test]
    fn truncated_tx_data_is_rejected_as_truncated() {
        let mut forged = encode_container(ROLLUP, &span(), &[action(true)]).unwrap();
        let at = first_bracket_offset();
        forged[at + 9] = 0x02;
        forged.truncate(at + 10);

        assert_eq!(
            decode_container(&forged).unwrap_err(),
            CodecError::Truncated("tx_data"),
        );
    }

    #[test]
    fn non_empty_tx_data_in_a_later_action_rejects_the_whole_container() {
        let first = action(true);
        let second = action(false);
        let one_action_len = encode_container(ROLLUP, &span(), std::slice::from_ref(&first))
            .unwrap()
            .len();
        let mut forged = encode_container(ROLLUP, &span(), &[first, second]).unwrap();
        assert_eq!(forged[one_action_len], MSG_INITIATE);
        forged[one_action_len + 9] = 0x01;
        forged.insert(one_action_len + 10, 0xff);

        assert_eq!(
            decode_container(&forged).unwrap_err(),
            CodecError::NonEmptyTransactionData,
        );
    }

    #[test]
    fn every_truncated_action_prefix_is_rejected() {
        let encoded = encode_container(ROLLUP, &span(), &[action(true)]).unwrap();
        let action_at = first_bracket_offset();
        for cut in action_at + 1..encoded.len() {
            assert!(
                decode_container(&encoded[..cut]).is_err(),
                "action prefix ending at {cut} unexpectedly decoded",
            );
        }
    }

    #[test]
    fn action_field_boundaries_and_extremes_round_trip() {
        let action = Action {
            source_rollup_id: u64::MAX,
            target_rollup_id: u64::MAX,
            source_address: [0x00; 20],
            target_address: [0xff; 20],
            value: [0xff; VALUE_BYTES],
            gas: u64::MAX,
            data: vec![0x5a; 128],
            success: false,
            return_data: vec![0xa5; 128],
        };
        let encoded = encode_container(ROLLUP, &span(), std::slice::from_ref(&action)).unwrap();
        assert_eq!(decode_container(&encoded).unwrap().actions, vec![action]);
    }

    /// A bracket must be complete: truncating anywhere fails the whole stream.
    #[test]
    fn a_truncated_stream_is_rejected() {
        let encoded = encode_container(ROLLUP, &span(), &[action(true)]).unwrap();
        for cut in [encoded.len() - 1, encoded.len() - 6, encoded.len() / 2] {
            assert!(
                decode_container(&encoded[..cut]).is_err(),
                "truncation at {cut} must be rejected",
            );
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn action_strategy() -> impl Strategy<Value = Action> {
            (
                any::<u64>(),
                any::<u64>(),
                any::<[u8; 20]>(),
                any::<[u8; 20]>(),
                any::<[u8; VALUE_BYTES]>(),
                any::<u64>(),
                proptest::collection::vec(any::<u8>(), 0..128),
                any::<bool>(),
                proptest::collection::vec(any::<u8>(), 0..128),
            )
                .prop_map(
                    |(
                        source_rollup_id,
                        target_rollup_id,
                        source_address,
                        target_address,
                        value,
                        gas,
                        data,
                        success,
                        return_data,
                    )| Action {
                        source_rollup_id,
                        target_rollup_id,
                        source_address,
                        target_address,
                        value,
                        gas,
                        data,
                        success,
                        return_data,
                    },
                )
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            #[test]
            fn arbitrary_valid_action_manifests_round_trip_exactly(
                rollup_id in any::<u64>(),
                actions in proptest::collection::vec(action_strategy(), 0..8),
            ) {
                let encoded = encode_container(rollup_id, &span(), &actions)
                    .expect("bounded generated manifest encodes");
                let decoded = decode_container(&encoded)
                    .expect("encoded manifest decodes");
                prop_assert_eq!(decoded.rollup_id, rollup_id);
                prop_assert_eq!(&decoded.actions, &actions);
                prop_assert_eq!(
                    encode_container(decoded.rollup_id, &span(), &decoded.actions)
                        .expect("decoded manifest re-encodes"),
                    encoded,
                );
            }
        }
    }
}
