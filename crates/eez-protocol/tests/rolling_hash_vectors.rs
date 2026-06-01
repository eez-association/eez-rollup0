//! Foundry-grounded byte-equality tests for the two rolling-hash
//! accumulators (`EntryRollingHash`, `StaticCallRollingHash`).
//!
//! Mirror of
//! `crates/crosschain-evm/tests/cross_chain_call_hash_vectors.rs` —
//! reads the JSON fixture at
//! `tests/fixtures/rolling_hash_vectors.json` (regenerated via
//! `scripts/regen-rolling-hash-vectors.sh`, which runs the Foundry
//! script `contracts/script/GenRollingHashVectors.s.sol`) and asserts
//! that replaying each vector's ops in Rust produces a hash byte-
//! identical to Foundry's.
//!
//! Vector replay scripts live in this file rather than in the JSON to
//! keep the JSON narrow (one expected hash per vector); the script
//! must match the Solidity ground truth, which is enforced by these
//! tests being run alongside the regen.

use std::fs;
use std::path::PathBuf;

use crosschain_protocol::rolling_hash::{EntryRollingHash, StaticCallRollingHash};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct VectorFile {
    entry: Vec<NamedVector>,
    #[serde(rename = "static")]
    static_subcall: Vec<NamedVector>,
}

#[derive(Debug, Deserialize)]
struct NamedVector {
    name: String,
    #[serde(rename = "expectedHash")]
    expected_hash: String,
}

fn load_vectors() -> VectorFile {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rolling_hash_vectors.json");
    let raw =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    serde_json::from_str(&raw).expect("valid rolling_hash_vectors.json")
}

fn parse_hash(s: &str) -> [u8; 32] {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(stripped).expect("hex hash");
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

// ── Entry replay scripts ──────────────────────────────────────

fn replay_entry(name: &str) -> [u8; 32] {
    let mut h = EntryRollingHash::new();
    match name {
        "empty" => {}
        "single_success" => {
            h.call_begin(1);
            h.call_end(1, true, &hex::decode("AABBCC").unwrap());
        }
        "single_failure" => {
            h.call_begin(1);
            h.call_end(1, false, &hex::decode("DEADBEEF").unwrap());
        }
        "nested_action" => {
            h.call_begin(1);
            h.nested_begin(1);
            h.nested_end(1);
            h.call_end(1, true, b"");
        }
        "restore_after_inner_span" => {
            h.call_begin(1);
            let snap = h.current();
            // Inner span: would advance the hash, then ContextResult
            // restoration would write the snapshot back. We don't
            // need to actually advance + revert here — the restore
            // primitive replaces the value directly. (The unit test
            // `restore_overwrites_state` covers the advance-and-restore
            // case to lock the semantics; this vector validates the
            // Foundry ground truth for the snapshot path the on-chain
            // contract takes.)
            h.restore(snap);
            h.call_end(1, true, b"");
        }
        other => panic!("unknown entry vector `{other}`"),
    }
    h.current()
}

fn replay_static(name: &str) -> [u8; 32] {
    let mut h = StaticCallRollingHash::new();
    match name {
        "zero_calls" => {}
        "one_success_call" => {
            h.append(true, &hex::decode("AABBCC").unwrap());
        }
        "two_mixed_calls" => {
            h.append(true, &hex::decode("AABB").unwrap());
            h.append(false, &hex::decode("DEADBEEF").unwrap());
        }
        "three_mixed_with_empty" => {
            h.append(true, b"");
            h.append(true, &[0x01]);
            h.append(false, &[0xFF]);
        }
        other => panic!("unknown static vector `{other}`"),
    }
    h.current()
}

#[test]
fn entry_vectors_match_foundry() {
    let vectors = load_vectors();
    assert_eq!(vectors.entry.len(), 5, "expected 5 entry vectors");
    for v in vectors.entry {
        let actual = replay_entry(&v.name);
        let expected = parse_hash(&v.expected_hash);
        assert_eq!(
            actual, expected,
            "entry vector `{}`: Rust vs Foundry mismatch",
            v.name
        );
    }
}

#[test]
fn static_subcall_vectors_match_foundry() {
    let vectors = load_vectors();
    assert_eq!(vectors.static_subcall.len(), 4, "expected 4 static vectors");
    for v in vectors.static_subcall {
        let actual = replay_static(&v.name);
        let expected = parse_hash(&v.expected_hash);
        assert_eq!(
            actual, expected,
            "static vector `{}`: Rust vs Foundry mismatch",
            v.name
        );
    }
}
