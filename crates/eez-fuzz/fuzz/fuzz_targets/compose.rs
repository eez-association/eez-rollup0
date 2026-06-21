//! Coverage-guided fuzz target for `eez_protocol::compose_transaction`.
//!
//! # Why fuzz the composer
//!
//! The composer does sensitive, OPTIMISTIC cross-chain work: it simulates a
//! user's source tx, predicts what each cross-chain call returns, and emits an
//! execution table the settlement side must later honor. The dangerous failure
//! is silent — a composition that looks valid (right return data, matching
//! rolling hash) but whose claimed effect the settlement never realizes, so the
//! L2 commits to a state L1 never produces. That "the cross-chain call never
//! actually executes" class is a soundness break, not a crash, and the input
//! that triggers it is just a user tx — exactly the shape fuzzing explores. The
//! bug surface is deep (nested overlay push/pop pairing, rolling-hash
//! synthesis, return-data prediction), so coverage-guided mutation earns its
//! keep. The oracle that decides correctness lives in [`eez_fuzz`]'s
//! `assertions` module: execute + ratify + assert the SETTLED destination state.
//!
//! # How it's made fuzzable
//!
//! - **Address space restricted by construction.** `arbitrary` decodes `data`
//!   into a `FuzzTx` whose *indices* select a live trigger/method/signer from the
//!   booted world's dictionary — never a raw 20-byte address. A 256-bit address
//!   `EQ` gives libFuzzer no coverage gradient, so a random address never reaches
//!   a proxy; an index always dispatches into the cross-chain path.
//! - **Boot once, fuzz many.** `compose` is read-only against the frozen world,
//!   so the world + tokio runtime boot a single time and are reused for the whole
//!   campaign (deployment cost amortizes to zero). One world per direction.
//! - **Oracle-carrying input.** `resolve_and_sign` hands back the predicted
//!   settled value with the tx, so the oracle checks the destination's real
//!   storage — not the composer's claimed return (which a mock prover can't be
//!   trusted to bind). See the `assertions` module for the full oracle design.
//!
//! The first `FuzzTx` field is a `Direction` bit: `L1ToL2` (the implemented
//! direction) and `L2ToL1` (an L2 tx that calls a proxy of an L1 contract). The
//! direction-aware oracle lives in [`eez_fuzz::replay_compose`] so CI's
//! corpus-replay regression exercises both directions through the same path.
//!
//! Lineage: ItyFuzz (revm + LibAFL) — corpus of `(state, single-tx)` pairs,
//! infant-state corpus, comparison waypoints — is the SOTA direction this grows
//! toward (arXiv:2306.17135).
//!
//! Run: `cargo +nightly fuzz run compose --sanitizer none`
//! (`--sanitizer none` keeps libFuzzer coverage feedback without ASan-
//! instrumenting the whole reth/revm tree.)

#![no_main]

use std::sync::OnceLock;

use eez_fuzz::{Dict, World, replay_compose};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();
// One world per direction (boot once, reuse): L1-entry for L1→L2 and the
// L2-entry world for the L2→L1 direction the review flagged.
static WORLD_L1: OnceLock<(World, Dict)> = OnceLock::new();
static WORLD_L2: OnceLock<(World, Dict)> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let rt = RT.get_or_init(|| {
        // compose's overlay path requires a multi-thread runtime (block_in_place).
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    });
    let (l1, l1_dict) = WORLD_L1.get_or_init(|| {
        let world = World::boot();
        let dict = world.dict();
        (world, dict)
    });
    let (l2, l2_dict) = WORLD_L2.get_or_init(|| {
        let world = World::boot_l2_entry();
        let dict = world.dict();
        (world, dict)
    });
    // Same path CI's corpus-replay regression uses (see eez_fuzz::replay). The
    // input's direction bit selects which world the oracle runs against.
    rt.block_on(replay_compose(l1, l1_dict, l2, l2_dict, data));
});
