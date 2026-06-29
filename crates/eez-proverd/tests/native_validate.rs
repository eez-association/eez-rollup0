//! Integration: the ZisK `native-validate` stateless validator accepts a REAL
//! captured window end-to-end — the prover-side state-root re-execution path
//! `validate_window` drives in production (proverd `main.rs`), exercised here
//! over the committed `tests/fixtures/` window WITHOUT a live composer.
//!
//! This is the first committed test of the native-validate integration itself
//! (the unit tests cover the gates that consume its output, not the binary run).
//! It regression-guards: the staged-block/witness file contract, the chain
//! config, and the `✓ WINDOW VALID` + JSON-summary parse.
//!
//! GATED: skips (passes) unless both `EEZ_VALIDATOR_BIN` (the native-validate
//! path) and `EEZ_CHAIN_CONFIG` (the L2 chain-config JSON) are set, since CI has
//! no ZisK toolchain. Locally:
//!   EEZ_VALIDATOR_BIN=/home/ubuntu/zisk-eth-client/target/release/native-validate \
//!   EEZ_CHAIN_CONFIG=configs/l2-chainconfig.json \
//!   cargo test -p eez-proverd --test native_validate -- --nocapture
//!
//! NOTE on direction: the committed fixture (`block-13`) is an inbound/settlement
//! window (0 txs). An OUTBOUND window (a Sync block with loadExecutionTable + the
//! user proxy-call tx) re-executes the SAME reth STF native-validate wraps, and
//! is already proven re-derivable by the follower (`e2e_value_outbound` /
//! `e2e_value_outbound_k2` converge), with the (2d) outbound gate covered
//! separately (`outbound_gate.rs` + proverd unit tests). Capturing an outbound
//! window fixture (via the composer control feed) would let this same harness
//! run native-validate over outbound txs directly.

use std::process::Command;

/// Stage a fixture's `block-<n>.rlp` + `witness-<n>.json` as the window's
/// 0-indexed pair `validate_window` writes, run `native-validate <cfg> --dir`,
/// and return its `(stdout, stderr)` — native-validate prints the JSON summary
/// to stdout (proverd parses it from there) and progress (`✓ WINDOW VALID`) to
/// stderr.
fn run_native_validate(
    validator_bin: &str,
    chain_config: &str,
    fixture_n: u64,
) -> (String, String) {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let fixtures = format!("{manifest}/tests/fixtures");
    let dir = std::env::temp_dir().join(format!("eez-nv-test-{fixture_n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir staging dir");

    // The window is one block, staged at index 0 (native-validate reads
    // block-0.rlp / witness-0.json — the per-window indices proverd writes).
    std::fs::copy(
        format!("{fixtures}/block-{fixture_n}.rlp"),
        dir.join("block-0.rlp"),
    )
    .expect("stage block rlp");
    std::fs::copy(
        format!("{fixtures}/witness-{fixture_n}.json"),
        dir.join("witness-0.json"),
    )
    .expect("stage witness json");

    let out = Command::new(validator_bin)
        .arg(chain_config)
        .arg("--dir")
        .arg(&dir)
        .output()
        .expect("spawn native-validate");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "native-validate REJECTED the window (exit {:?}). stdout:\n{stdout}\nstderr:\n{stderr}",
        out.status.code(),
    );
    (stdout, stderr)
}

#[test]
fn native_validate_accepts_the_captured_window() {
    let (Ok(validator_bin), Ok(chain_config)) = (
        std::env::var("EEZ_VALIDATOR_BIN"),
        std::env::var("EEZ_CHAIN_CONFIG"),
    ) else {
        eprintln!(
            "SKIP native_validate: set EEZ_VALIDATOR_BIN + EEZ_CHAIN_CONFIG to run (no ZisK toolchain in CI)"
        );
        return;
    };
    if !std::path::Path::new(&validator_bin).exists() {
        eprintln!("SKIP native_validate: EEZ_VALIDATOR_BIN={validator_bin} does not exist");
        return;
    }

    let (stdout, stderr) = run_native_validate(&validator_bin, &chain_config, 13);

    // It must re-execute the window statelessly (progress on stderr) and emit the
    // JSON summary on stdout with a final state root (the facts proverd's
    // settlement gates consume — parsed exactly as proverd's `validate_window`).
    assert!(
        stderr.contains("WINDOW VALID"),
        "expected `WINDOW VALID` progress on stderr:\n{stderr}"
    );
    let summary = stdout
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with('{'))
        .expect("native-validate must print a JSON summary line on stdout");
    let j: serde_json::Value =
        serde_json::from_str(summary.trim()).expect("summary line must be JSON");
    assert!(
        j["final_state_root"]
            .as_str()
            .is_some_and(|s| s.starts_with("0x")),
        "summary must carry a final_state_root: {summary}"
    );
    assert!(
        j["blocks"].as_array().is_some_and(|b| !b.is_empty()),
        "summary must carry the re-executed blocks: {summary}"
    );
    eprintln!("✓ native-validate accepted the captured window: {summary}");
}
