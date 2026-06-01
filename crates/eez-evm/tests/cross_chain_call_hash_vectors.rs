//! Cross-chain call hash vector tests (invariant 5 byte-equality lock).
//!
//! Loads the JSON fixture at
//! `tests/fixtures/cross_chain_call_hash_vectors.json`
//! (regenerated via `scripts/regen-action-hash-vectors.sh`, which runs
//! the Foundry script `contracts/script/GenActionHashVectors.s.sol`)
//! and asserts that the Rust
//! [`crosschain_evm::action::cross_chain_call_hash`] function produces
//! byte-identical hashes for every vector. The hash bytes are
//! unchanged from the prior protocol (the 6-field `abi.encode`
//! preimage is identical); only the function name rotated to mirror
//! the on-chain `EEZ.computeCrossChainCallHash` rename.
//!
//! Vectors cover:
//! - `all_zero` — all-zero inputs (smoke).
//! - `minimal_call` / `with_value` — single-field perturbations.
//! - `swap_target_source` — non-zero rollup ids on both sides.
//! - `long_data` — realistic ERC-20 `transfer` calldata, 1 ETH value.
//! - `high_rollup_ids` — `u64::MAX` / `u64::MAX - 1` rollup ids.
//! - `max_value_no_data` — `uint256::MAX` value, empty data.

use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::{hex, Address, Bytes, B256, U256};
use crosschain_evm::action::cross_chain_call_hash;
use crosschain_protocol::RollupId;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Vector {
    name: String,
    #[serde(rename = "targetRollupId")]
    target_rollup_id: String,
    #[serde(rename = "targetAddress")]
    target_address: String,
    value: String,
    data: String,
    #[serde(rename = "sourceAddress")]
    source_address: String,
    #[serde(rename = "sourceRollupId")]
    source_rollup_id: String,
    #[serde(rename = "expectedHash")]
    expected_hash: String,
}

fn load_vectors() -> Vec<Vector> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cross_chain_call_hash_vectors.json");
    let raw =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    serde_json::from_str(&raw).expect("valid cross_chain_call_hash_vectors.json")
}

fn parse_u64_rollup(s: &str) -> RollupId {
    // Foundry emits rollup ids in decimal form; parse as u256 then
    // narrow to u64. RollupId is a u64 newtype on the Rust side.
    let v = U256::from_str(s).expect("uint256");
    RollupId(u64::try_from(v).expect("rollup id fits in u64"))
}

fn parse_data(s: &str) -> Bytes {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    if stripped.is_empty() {
        Bytes::new()
    } else {
        Bytes::from(hex::decode(stripped).expect("hex data"))
    }
}

#[test]
fn all_vectors_match() {
    let vectors = load_vectors();
    assert!(
        vectors.len() >= 7,
        "expected at least 7 vectors, got {}",
        vectors.len()
    );

    for v in vectors {
        let target_rollup_id = parse_u64_rollup(&v.target_rollup_id);
        let source_rollup_id = parse_u64_rollup(&v.source_rollup_id);
        let target_address: Address = v.target_address.parse().expect("target address");
        let source_address: Address = v.source_address.parse().expect("source address");
        let value = U256::from_str(&v.value).expect("uint256 value");
        let data = parse_data(&v.data);
        let expected: B256 = v.expected_hash.parse().expect("expected hash");

        let actual = cross_chain_call_hash(
            target_rollup_id,
            target_address,
            value,
            &data,
            source_address,
            source_rollup_id,
        );
        assert_eq!(
            actual, expected,
            "vector `{}`: cross_chain_call_hash mismatch (Rust vs Foundry)",
            v.name
        );
    }
}
