//! Shared run-path for BOTH the cargo-fuzz targets and the corpus-replay
//! regression: decode one input, run it through the harness, assert the oracle.
//!
//! Real fuzzing (`cargo fuzz run`) and CI regression (replay the persisted
//! corpus) call the SAME function, so a corpus case that the fuzzer found can
//! never drift from what CI checks. Compose *errors* are valid rejections; only
//! an oracle panic is a finding.

use arbitrary::{Arbitrary, Unstructured};

use crate::{Dict, Direction, FuzzTx, Program, SeqBase, SeqWorld, World};

/// Replay one single-tx input against the booted worlds, choosing the entry
/// chain from the input's `Direction` bit.
///
/// - [`Direction::L1ToL2`] (implemented): compose against `l1` and run the full
///   execute + ratify + SETTLE oracle. Compose *errors* (EmptyCalls, decode, …)
///   are valid rejections; only an oracle panic is a finding.
/// - [`Direction::L2ToL1`] (an L2 tx that calls a proxy of an L1 contract):
///   compose against `l2`. The composer has no L2-as-entry settling path today,
///   so it rejects the dispatch (the L1 target tx reverts) — a VALID rejection,
///   not a crash. The path is still covered on every `L2ToL1` input.
///
///   TODO(L2→L1): once the composer grows a real L2-as-entry settling path,
///   replace the accept-`Err` arm with the full L1-side oracle (assert the
///   composition settles the L1 destination, `InnerValue@L1` slot-0, to
///   `expected`), and flip `l2_to_l1_is_rejected_today` to assert success.
pub async fn replay_compose(
    l1: &World,
    l1_dict: &Dict,
    l2: &World,
    l2_dict: &Dict,
    data: &[u8],
) {
    let Ok(input) = FuzzTx::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    match input.direction {
        Direction::L1ToL2 => {
            let (raw, expected) = input.resolve_and_sign(l1_dict);
            if let Ok(comp) = l1.compose(&raw).await {
                l1.assert_executes_and_ratifies(&comp, Some(expected));
            }
        }
        Direction::L2ToL1 => {
            let (raw, _expected) = input.resolve_and_sign(l2_dict);
            match l2.compose(&raw).await {
                // Expected today: direction not implemented → rejected.
                Err(_) => {}
                // Forward-compat guard: if the composer ever DOES build an
                // L2→L1 table, it must not be a silent no-op (dormant until
                // L2→L1 support lands; see TODO above).
                Ok(comp) => assert!(
                    comp.source.entry_payload.len() > 4,
                    "L2→L1 produced a non-empty Ok with an empty source payload \
                     (silent no-op) — investigate before trusting it",
                ),
            }
        }
    }
}

/// Replay one op-sequence input against a fresh fork of the base world.
pub async fn replay_program(base: &SeqBase, data: &[u8]) {
    let Ok(program) = Program::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    SeqWorld::fork(base).run(program).await;
}
