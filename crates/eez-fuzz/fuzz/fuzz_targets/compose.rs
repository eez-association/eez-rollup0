//! Coverage-guided fuzz target for `eez_protocol::compose_transaction`.
//!
//! libFuzzer mutates `data`; `arbitrary` decodes it into a `FuzzTx` whose
//! indices select a live trigger/method/signer from the booted world's
//! dictionary (address space restricted by construction — see
//! `docs/FUZZ_TESTING.md`). Each input composes against the frozen world and,
//! on success, must execute + ratify against the real bytecode. The world +
//! tokio runtime boot ONCE and are reused across the campaign.
//!
//! Run: `cargo +nightly fuzz run compose --sanitizer none`
//! (`--sanitizer none` keeps libFuzzer coverage feedback without ASan-
//! instrumenting the whole reth/revm tree.)

#![no_main]

use std::sync::OnceLock;

use arbitrary::{Arbitrary, Unstructured};
use eez_fuzz::{Dict, FuzzTx, World};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();
static WORLD: OnceLock<(World, Dict)> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let rt = RT.get_or_init(|| {
        // compose's overlay path requires a multi-thread runtime (block_in_place).
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    });
    let (world, dict) = WORLD.get_or_init(|| {
        let world = World::boot();
        let dict = world.dict();
        (world, dict)
    });

    let Ok(input) = FuzzTx::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    let raw_tx = input.resolve_and_sign(dict);

    rt.block_on(async {
        // Compose *errors* (EmptyCalls, decode, etc.) are valid rejections, not
        // crashes. Only a successful composition must execute + ratify — a panic
        // inside the oracle (revert / RollingHashMismatch) is the real finding.
        if let Ok(comp) = world.compose(&raw_tx).await {
            world.assert_executes_and_ratifies(&comp);
        }
    });
});
