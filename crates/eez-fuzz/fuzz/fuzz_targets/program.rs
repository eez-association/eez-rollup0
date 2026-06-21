//! Coverage-guided fuzz target for STATEFUL op-sequences.
//!
//! libFuzzer mutates `data`; `arbitrary` decodes it into a `Program` (a
//! `Vec<Op>` of deploy / register-proxy / interact / relay-to-L1). Each program
//! runs against a FRESH mutable dual-chain base, so the steps accumulate —
//! deploys mint targets, registrations grow the live trigger dict, interacts
//! fire through live triggers and settle back. Coverage guidance is what makes
//! this find rare deep states (e.g. an L2 contract calling a proxy that targets
//! L1) that a blind seed loop never would: reaching the relay deploy is new
//! coverage → kept in the corpus → mutated toward an interact that triggers it.
//!
//! The oracle lives in `SeqWorld::run` (cumulative last-writer-wins on each
//! target's settled storage); a panic there is a real finding. Compose errors
//! are valid rejections, not crashes.
//!
//! Run: `cargo +nightly fuzz run program --sanitizer none`

#![no_main]

use std::sync::OnceLock;

use arbitrary::{Arbitrary, Unstructured};
use eez_fuzz::{Program, SeqWorld};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

static RT: OnceLock<Runtime> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let rt = RT.get_or_init(|| {
        // compose's overlay path requires a multi-thread runtime (block_in_place).
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    });
    let Ok(program) = Program::arbitrary(&mut Unstructured::new(data)) else {
        return;
    };
    // Fresh base per program — state accumulates within a program, not across.
    rt.block_on(async {
        SeqWorld::new().run(program).await;
    });
});
