//! Shared run-path for BOTH the cargo-fuzz targets and the corpus-replay
//! regression: decode one input, run it through the harness, assert the oracle.
//!
//! Real fuzzing (`cargo fuzz run`) and CI regression (replay the persisted
//! corpus) call the SAME function, so a corpus case that the fuzzer found can
//! never drift from what CI checks. Compose *errors* are valid rejections; only
//! an oracle panic is a finding.

use arbitrary::{Arbitrary, Unstructured};

use crate::{Dict, FuzzTx, Program, SeqBase, SeqWorld, World};

/// Replay one single-tx input against a booted world.
pub async fn replay_compose(world: &World, dict: &Dict, data: &[u8]) {
    let Ok(input) = FuzzTx::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    let (raw, expected) = input.resolve_and_sign(dict);
    if let Ok(comp) = world.compose(&raw).await {
        world.assert_executes_and_ratifies(&comp, Some(expected));
    }
}

/// Replay one op-sequence input against a fresh fork of the base world.
pub async fn replay_program(base: &SeqBase, data: &[u8]) {
    let Ok(program) = Program::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    SeqWorld::fork(base).run(program).await;
}
