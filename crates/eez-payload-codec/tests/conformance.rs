//! Machine-readable Rollup0 payload conformance vectors.
//!
//! These tests intentionally load frozen external fixtures instead of building
//! their expected bytes with the production encoder. That makes the fixture a
//! compatibility boundary that other implementations can consume.

use std::{collections::BTreeSet, fs, path::PathBuf};

use eez_payload_codec::{
    Action, CodecError, SpanBlock, decode, decode_container, encode, encode_container,
};
use serde::Deserialize;

const SCHEMA_VERSION: u64 = 1;
const SPEC_COMMIT: &str = "7c99bcf923e0ee0f867549a750d1ab1373bf845d";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureFile {
    schema_version: u64,
    spec_commit: String,
    valid: Vec<ValidFixture>,
    invalid: Vec<InvalidFixture>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidFixture {
    name: String,
    operations: String,
    block_tx_counts: Vec<u32>,
    beneficiaries: Vec<String>,
    extra_data: Vec<String>,
    transactions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct InvalidFixture {
    name: String,
    operations: String,
    error: String,
}

fn load() -> FixtureFile {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/payload_v0_conformance.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let fixtures: FixtureFile =
        serde_json::from_str(&raw).expect("valid payload conformance fixture JSON");
    assert_eq!(fixtures.schema_version, SCHEMA_VERSION, "fixture schema");
    assert_eq!(fixtures.spec_commit, SPEC_COMMIT, "fixture spec revision");
    fixtures
}

fn bytes(value: &str) -> Vec<u8> {
    let value = value
        .strip_prefix("0x")
        .unwrap_or_else(|| panic!("fixture byte string must start with 0x: {value}"));
    hex::decode(value).unwrap_or_else(|error| panic!("invalid fixture hex 0x{value}: {error}"))
}

fn address(value: &str) -> [u8; 20] {
    bytes(value)
        .try_into()
        .unwrap_or_else(|_| panic!("fixture beneficiary must be exactly 20 bytes: {value}"))
}

fn error_class(error: &CodecError) -> &'static str {
    match error {
        CodecError::Empty => "EMPTY",
        CodecError::UnsupportedVersion(_) => "UNSUPPORTED_VERSION",
        CodecError::Truncated(_) => "TRUNCATED",
        CodecError::NonCanonicalVarint(_) => "NON_CANONICAL_VARINT",
        CodecError::EmptySpan => "EMPTY_SPAN",
        CodecError::ImplausibleCount { .. } => "IMPLAUSIBLE_COUNT",
        CodecError::RunCoverage(_) => "RUN_COVERAGE",
        CodecError::NonMaximalRun(_) => "NON_MAXIMAL_RUN",
        CodecError::ExtraDataTooLong(_) => "EXTRA_DATA_TOO_LONG",
        CodecError::EmptyTransaction(_) => "EMPTY_TRANSACTION",
        CodecError::TxByteMismatch { .. } => "TRANSACTION_BYTES_MISMATCH",
        CodecError::TrailingBytes(_) => "TRAILING_BYTES",
        CodecError::ValueTooLong(_) => "VALUE_TOO_LONG",
        CodecError::NonMinimalValue => "NON_MINIMAL_VALUE",
        CodecError::UnexpectedMessage { .. } => "UNEXPECTED_MESSAGE",
        CodecError::UnknownMessage(_) => "UNKNOWN_MESSAGE",
        CodecError::NonEmptyTransactionData => "NON_EMPTY_TRANSACTION_DATA",
        CodecError::ValueTooLarge { .. } => "VALUE_TOO_LARGE",
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContainerFixtureFile {
    schema_version: u64,
    spec_commit: String,
    valid: Vec<ValidContainerFixture>,
    invalid: Vec<InvalidFixture>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidContainerFixture {
    name: String,
    container: String,
    rollup_id: u64,
    block_tx_counts: Vec<u32>,
    actions: Vec<ActionFixture>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActionFixture {
    source_rollup_id: u64,
    target_rollup_id: u64,
    source_address: String,
    target_address: String,
    value: String,
    gas: u64,
    data: String,
    success: bool,
    return_data: String,
}

fn load_containers() -> ContainerFixtureFile {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/action_manifest_v1_conformance.json");
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let fixtures: ContainerFixtureFile =
        serde_json::from_str(&raw).expect("valid action-manifest conformance fixture JSON");
    assert_eq!(fixtures.schema_version, SCHEMA_VERSION, "fixture schema");
    assert_eq!(fixtures.spec_commit, SPEC_COMMIT, "fixture spec revision");
    fixtures
}

fn action(fixture: &ActionFixture) -> Action {
    Action {
        source_rollup_id: fixture.source_rollup_id,
        target_rollup_id: fixture.target_rollup_id,
        source_address: address(&fixture.source_address),
        target_address: address(&fixture.target_address),
        value: bytes(&fixture.value).try_into().unwrap_or_else(|_| {
            panic!("fixture value must be exactly 32 bytes: {}", fixture.value)
        }),
        gas: fixture.gas,
        data: bytes(&fixture.data),
        success: fixture.success,
        return_data: bytes(&fixture.return_data),
    }
}

#[test]
fn action_manifest_vectors_decode_and_reencode_exactly() {
    for fixture in load_containers().valid {
        let encoded = bytes(&fixture.container);
        let decoded = decode_container(&encoded)
            .unwrap_or_else(|error| panic!("{} should decode: {error}", fixture.name));
        let actions = fixture.actions.iter().map(action).collect::<Vec<_>>();

        assert_eq!(
            decoded.rollup_id, fixture.rollup_id,
            "{} rollup",
            fixture.name
        );
        assert_eq!(
            decoded.span.block_tx_counts, fixture.block_tx_counts,
            "{} block boundaries",
            fixture.name
        );
        assert_eq!(decoded.actions, actions, "{} manifest", fixture.name);

        let mut transaction_offset = 0;
        let blocks = decoded
            .span
            .block_tx_counts
            .iter()
            .enumerate()
            .map(|(index, count)| {
                let end = transaction_offset + *count as usize;
                let block = SpanBlock {
                    beneficiary: decoded.span.beneficiaries[index],
                    extra_data: decoded.span.extra_data[index].clone(),
                    transactions: decoded.span.transactions[transaction_offset..end].to_vec(),
                };
                transaction_offset = end;
                block
            })
            .collect::<Vec<_>>();
        assert_eq!(transaction_offset, decoded.span.transactions.len());
        assert_eq!(
            encode_container(fixture.rollup_id, &blocks, &actions)
                .expect("decoded container re-encodes"),
            encoded,
            "{} must have one canonical encoding",
            fixture.name
        );
    }
}

#[test]
fn invalid_action_manifest_vectors_fail_closed() {
    for fixture in load_containers().invalid {
        let error = match decode_container(&bytes(&fixture.operations)) {
            Ok(value) => panic!("{} unexpectedly decoded as {value:?}", fixture.name),
            Err(error) => error,
        };
        assert_eq!(
            error_class(&error),
            fixture.error,
            "{} rejection class ({error})",
            fixture.name
        );
    }
}

#[test]
fn action_manifest_grammar_rejects_each_invalid_boundary() {
    let fixture = load_containers()
        .valid
        .into_iter()
        .find(|fixture| fixture.name == "one-successful-action")
        .expect("one-action grammar fixture");
    let canonical = bytes(&fixture.container);
    let action_at = 42;
    assert_eq!(canonical[action_at], 3, "Initiate message");
    assert_eq!(canonical[action_at + 10], 4, "Call message");

    let mut wrong_call = canonical.clone();
    wrong_call[action_at + 10] = 3;
    assert!(matches!(
        decode_container(&wrong_call),
        Err(CodecError::UnexpectedMessage { .. })
    ));

    let mut unknown_return = canonical.clone();
    unknown_return[action_at + 104] = 0xff;
    assert_eq!(
        decode_container(&unknown_return).unwrap_err(),
        CodecError::UnknownMessage(0xff),
    );

    let mut wrong_finish = canonical.clone();
    wrong_finish[action_at + 107] = 3;
    assert!(matches!(
        decode_container(&wrong_finish),
        Err(CodecError::UnexpectedMessage { .. })
    ));

    let mut multibyte_tx_data = canonical.clone();
    multibyte_tx_data[action_at + 9] = 2;
    multibyte_tx_data.splice(action_at + 10..action_at + 10, [0xde, 0xad]);
    assert_eq!(
        decode_container(&multibyte_tx_data).unwrap_err(),
        CodecError::NonEmptyTransactionData,
    );

    let mut truncated_tx_data = canonical;
    truncated_tx_data[action_at + 9] = 2;
    truncated_tx_data.truncate(action_at + 10);
    assert_eq!(
        decode_container(&truncated_tx_data).unwrap_err(),
        CodecError::Truncated("tx_data"),
    );
}

#[test]
fn frozen_span_vectors_cover_every_reachable_rejection_class() {
    let covered = load()
        .invalid
        .into_iter()
        .map(|fixture| fixture.error)
        .collect::<BTreeSet<_>>();
    let required = [
        "EMPTY",
        "UNSUPPORTED_VERSION",
        "TRUNCATED",
        "NON_CANONICAL_VARINT",
        "EMPTY_SPAN",
        "IMPLAUSIBLE_COUNT",
        "RUN_COVERAGE",
        "NON_MAXIMAL_RUN",
        "EXTRA_DATA_TOO_LONG",
        "EMPTY_TRANSACTION",
        "TRANSACTION_BYTES_MISMATCH",
    ];
    for class in required {
        assert!(covered.contains(class), "missing fixture for {class}");
    }
}

#[test]
fn canonical_vectors_decode_and_reencode_exactly() {
    for fixture in load().valid {
        let encoded = bytes(&fixture.operations);
        let decoded = decode(&encoded)
            .unwrap_or_else(|error| panic!("{} should decode: {error}", fixture.name));

        assert_eq!(
            decoded.block_tx_counts, fixture.block_tx_counts,
            "{} transaction counts",
            fixture.name
        );
        assert_eq!(
            decoded.beneficiaries,
            fixture
                .beneficiaries
                .iter()
                .map(|value| address(value))
                .collect::<Vec<_>>(),
            "{} beneficiaries",
            fixture.name
        );
        assert_eq!(
            decoded.extra_data,
            fixture
                .extra_data
                .iter()
                .map(|value| bytes(value))
                .collect::<Vec<_>>(),
            "{} extraData",
            fixture.name
        );
        assert_eq!(
            decoded.transactions,
            fixture
                .transactions
                .iter()
                .map(|value| bytes(value))
                .collect::<Vec<_>>(),
            "{} transactions",
            fixture.name
        );

        let mut transaction_offset = 0;
        let blocks = decoded
            .block_tx_counts
            .iter()
            .enumerate()
            .map(|(block_index, count)| {
                let end = transaction_offset + *count as usize;
                let block = SpanBlock {
                    beneficiary: decoded.beneficiaries[block_index],
                    extra_data: decoded.extra_data[block_index].clone(),
                    transactions: decoded.transactions[transaction_offset..end].to_vec(),
                };
                transaction_offset = end;
                block
            })
            .collect::<Vec<_>>();
        assert_eq!(transaction_offset, decoded.transactions.len());
        assert_eq!(
            encode(&blocks).expect("decoded fixture re-encodes"),
            encoded,
            "{} must have one canonical encoding",
            fixture.name
        );
    }
}

#[test]
fn invalid_vectors_are_rejected_by_stable_error_class() {
    for fixture in load().invalid {
        let error = match decode(&bytes(&fixture.operations)) {
            Ok(value) => panic!("{} unexpectedly decoded as {value:?}", fixture.name),
            Err(error) => error,
        };
        assert_eq!(
            error_class(&error),
            fixture.error,
            "{} rejection class ({error})",
            fixture.name
        );
    }
}
