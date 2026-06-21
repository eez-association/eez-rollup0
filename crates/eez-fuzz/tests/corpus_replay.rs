//! CI regression: replay the committed corpus (real inputs the fuzzer found)
//! through the SAME path the cargo-fuzz targets use (`eez_fuzz::replay`). No
//! synthetic seed loops — actual fuzzing campaigns run via `fuzz.sh` in infra;
//! this just guards against re-breaking a persisted case. A corpus input that
//! now panics fails the build.

use std::fs;
use std::path::PathBuf;

use eez_fuzz::{SeqWorld, World, replay_compose, replay_program};

/// Read every committed corpus file for a target.
fn corpus(target: &str) -> Vec<Vec<u8>> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus").join(target);
    let Ok(entries) = fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .map(|p| fs::read(p).expect("read corpus file"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_compose_corpus() {
    // One world per direction — the input's `Direction` bit picks the entry.
    let l1 = World::boot();
    let l1_dict = l1.dict();
    let l2 = World::boot_l2_entry();
    let l2_dict = l2.dict();
    let cases = corpus("compose");
    for data in &cases {
        replay_compose(&l1, &l1_dict, &l2, &l2_dict, data).await;
    }
    assert!(!cases.is_empty(), "no committed compose corpus to replay");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_program_corpus() {
    let base = SeqWorld::boot_base();
    let cases = corpus("program");
    for data in &cases {
        replay_program(&base, data).await;
    }
    assert!(!cases.is_empty(), "no committed program corpus to replay");
}
