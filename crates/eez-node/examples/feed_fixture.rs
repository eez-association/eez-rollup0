//! Throwaway harness: serve ONE `ControlEvent` from disk fixtures to drive
//! eez-proverd end-to-end without the full composer. Reads
//! `<dir>/block-<n>.rlp` + `<dir>/witness-<n>.json`, and if
//! `<dir>/postbatch-<n>.json` exists, attaches it as the SETTLING composition
//! (so the daemon exercises the publicInputsHash + settlement-chain gates).
//! Serves `control.v1.ControlFeed` on 127.0.0.1:50051.
//!
//! Usage: cargo run -p eez-node --example feed_fixture -- [dir] [n]
//! Default: the embedded settling fixture (eez-proverd/tests/fixtures, block 13).

use std::sync::Arc;

use alloy_primitives::B256;
use eez_composer::control_feed::{ControlFeedSvc, ControlPublisher};
use eez_composer::posted_windows::{PostedWindow, PostedWindows};
use eez_composer::proof_sink::ProofSinkSvc;
use eez_composer::prover_dispatch::ProverDispatchSvc;
use eez_control_rpc::v1::{
    control_feed_server::ControlFeedServer, proof_sink_server::ProofSinkServer,
    prover_dispatch_server::ProverDispatchServer, Composition, ControlEvent, ExecutionWitness,
    PostBatch,
};

/// 32-byte B256 from a fixture byte vec (pad/truncate defensively).
fn b256_of(v: &[u8]) -> B256 {
    let mut b = [0u8; 32];
    let n = v.len().min(32);
    b[..n].copy_from_slice(&v[..n]);
    B256::from(b)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init(); // so the ProofSink's verify log is visible
    let dir = std::env::args().nth(1).unwrap_or_else(|| {
        concat!(env!("CARGO_MANIFEST_DIR"), "/../eez-proverd/tests/fixtures").to_string()
    });
    let n: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(13);

    let block = std::fs::read(format!("{dir}/block-{n}.rlp"))?;
    let wjson: serde_json::Value =
        serde_json::from_slice(&std::fs::read(format!("{dir}/witness-{n}.json"))?)?;
    let field = |k: &str| -> eyre::Result<Vec<Vec<u8>>> {
        wjson[k]
            .as_array()
            .ok_or_else(|| eyre::eyre!("witness.{k} not an array"))?
            .iter()
            .map(|v| {
                let s = v.as_str().ok_or_else(|| eyre::eyre!("non-string in witness.{k}"))?;
                Ok(alloy_primitives::hex::decode(s.trim_start_matches("0x"))?)
            })
            .collect()
    };
    let witness = ExecutionWitness {
        state: field("state")?,
        codes: field("codes")?,
        keys: field("keys")?,
        headers: field("headers")?,
    };

    // Optional settling composition from postbatch-<n>.json (a captured PostBatch).
    let (block_hash, composition) = match std::fs::read(format!("{dir}/postbatch-{n}.json")) {
        Ok(raw) => {
            let p: serde_json::Value = serde_json::from_slice(&raw)?;
            let dec = |k: &str| -> eyre::Result<Vec<u8>> {
                Ok(alloy_primitives::hex::decode(
                    p[k].as_str().unwrap_or("").trim_start_matches("0x"),
                )?)
            };
            let pb = PostBatch {
                abi_calldata: dec("abi_calldata")?,
                rollup_id: p["rollup_id"].as_u64().unwrap_or(0),
                current_state: dec("current_state")?,
                new_state: dec("new_state")?,
                entry_count: u32::try_from(p["entry_count"].as_u64().unwrap_or(0)).unwrap_or(0),
                public_inputs_hash: dec("public_inputs_hash")?,
                l1_block_hash: dec("l1_block_hash")?,
            };
            (dec("block_hash")?, Some(Composition { post_batch: Some(pb) }))
        }
        Err(_) => (vec![0u8; 32], None),
    };

    let settling = composition.is_some();

    // Composer-driven dispatch (Phase 3): a one-window PostedWindows ledger for
    // the fixture's settling block, served via the REAL ProverDispatchSvc. A
    // driven prover (EEZ_COMPOSER_DRIVEN=1) receives the directive
    // `[n..n]` and verifies that whole window. `from_block == to_block == n` is a
    // single-block batch (posted = n-1, sync_height = n); current_state /
    // public_inputs_hash come from the captured PostBatch (HINTS the prover
    // recomputes, so they also exercise the ERROR-level cross-check).
    let windows = PostedWindows::new();
    if let Some(comp) = &composition {
        if let Some(pb) = &comp.post_batch {
            windows.record_posted(PostedWindow {
                from_block: n,
                to_block: n,
                rollup_id: pb.rollup_id,
                public_inputs_hash: b256_of(&pb.public_inputs_hash),
                current_state: b256_of(&pb.current_state),
                attested: false,
                fast_forwarded: false,
                pending_l1: false,
            });
        }
    }

    let event = ControlEvent {
        block_hash,
        block_number: n,
        parent_hash: vec![0u8; 32],
        composition,
        witness: Some(witness),
        block,
    };
    let publisher = ControlPublisher::new(2);
    publisher.publish(event);

    // The composer's REAL ProofSink, verifying attestations recover to the
    // registered attester (hardhat #0 — the prover signs with that key). Wired
    // WITH the shared `windows` ledger (+ a ProofStore) so a verified attestation
    // flips the window's `attested` + advances the verified frontier — exercising
    // the FULL composer-driven loop (dispatch → attest → frontier → stop
    // re-dispatching), not just the prover half.
    let attester = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".parse()?;
    let store: eez_composer::proof_sink::ProofStore =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let proof_sink = ProofSinkSvc::with_store_and_windows(attester, store, windows.clone());

    // Bind address overridable (EEZ_FIXTURE_ADDR) so the rig can dodge a live
    // node already on :50051.
    let addr_str =
        std::env::var("EEZ_FIXTURE_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let addr = addr_str.parse()?;
    println!(
        "feed_fixture: serving block #{n} (settling={settling}) + ProofSink + ProverDispatch on {addr}"
    );
    tonic::transport::Server::builder()
        .add_service(ControlFeedServer::new(ControlFeedSvc::new(Arc::clone(&publisher))))
        .add_service(ProofSinkServer::new(proof_sink))
        .add_service(ProverDispatchServer::new(ProverDispatchSvc::new(windows)))
        .serve(addr)
        .await?;
    Ok(())
}
