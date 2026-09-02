//! Captured protocol regressions owned by the shared pipeline.

use crate::settlement;

fn fixture_hex(value: &str) -> Vec<u8> {
    let value = value.trim();
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).unwrap()
}

fn fixture(dir: &str, name: &str) -> String {
    let path = format!("{}/tests/fixtures/{dir}/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read fixture {path}: {error}"))
}

#[test]
fn captured_legacy_inbound_calldata_is_rejected_by_target_abi() {
    let post_batch: serde_json::Value =
        serde_json::from_str(&fixture("fresh-chain-inbound-2175", "postbatch.json")).unwrap();
    let calldata = fixture_hex(post_batch["abi_calldata"].as_str().unwrap());

    assert_eq!(&calldata[..4], &[0x8b, 0x1a, 0x09, 0x5a]);
    assert!(matches!(
        settlement::decode_canonical_post_batch(calldata),
        Err(settlement::PostBatchDecodeError::InvalidAbi { .. })
    ));
}
