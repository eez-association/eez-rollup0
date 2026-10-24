//! Replay the portable transcript against a separately configured prover.
//! Usage: cargo run -p eez-control-rpc --example replay_streaming -- URL [CASE] [FILE]
use std::time::Duration;

use alloy_primitives::Address;

#[path = "../conformance/replay.rs"]
mod replay;

use replay::{Error, replay};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .ok_or("expected prover URL, then optional case name")?;
    let selected = args.next();
    let path = args.next().unwrap_or_else(|| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/conformance/rollup0-transcripts.json"
        )
        .to_owned()
    });
    let document: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let attester: Address = document["oracle"]["attester"]
        .as_str()
        .ok_or("missing attester")?
        .parse()?;
    let mut ran = 0;
    for case in document["cases"].as_array().ok_or("missing cases")? {
        let name = case["name"].as_str().ok_or("missing case name")?;
        if selected.as_deref().is_some_and(|selected| selected != name) {
            continue;
        }
        if let Some(config) = case.get("server_args") {
            if selected.is_none() {
                return Err(
                    "limit vectors require a selected case and its server_args configuration"
                        .into(),
                );
            }
            println!("Required server configuration for {name}: {config}");
        }
        tokio::time::timeout(Duration::from_mins(2), replay(&endpoint, case, attester)).await??;
        println!("PASS {name}");
        ran += 1;
    }
    if ran == 0 {
        return Err("no matching case".into());
    }
    Ok(())
}
