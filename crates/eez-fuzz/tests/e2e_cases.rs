//! Curated e2e-scenario ports — the `script/e2e` cases from `eez-core-protocol`
//! expressed in the SAME `Program`/`Op` vocabulary the program fuzzer explores
//! (`eez_fuzz::sequence`). One model, two uses: these hand-written `Program`s
//! are deterministic regression tests, and the same ops are what the fuzzer
//! mutates to discover new cases.
//!
//! KEY DIFFERENCE from the Solidity e2e: those hand-build the execution table;
//! here we express only the TOPOLOGY + the user tx, and the COMPOSER builds the
//! table. So these test the composer on the e2e's topologies (the Solidity e2e
//! tests the contracts given a table — complementary layers).
//!
//! `#[ignore]`d tests are scenarios the composer can't yet handle — the fuzzer
//! would hit the same wall. Not-yet-expressible scenarios (needing new ops or
//! composer features) are catalogued at the bottom as the backlog the e2e suite
//! defines.

use alloy_primitives::U256;
use eez_fuzz::{Direction, FuzzTx, Op, Program, SeqWorld};

fn interact(trigger: u16, v: u128) -> Op {
    Op::Interact(FuzzTx {
        direction: Direction::L1ToL2, // the implemented direction; *L2 cases are backlog
        trigger_sel: trigger,
        method_sel: 0,
        signer_sel: 0,
        nonce: 0,
        value: 0,
        args: [v, 0, 0, 0],
    })
}

/// Boot a fresh world, run one curated program, return the settled slot-0 of the
/// trigger at `idx` (so a compose/settle gap shows up as the unchanged value).
async fn settle(ops: Vec<Op>, idx: usize) -> U256 {
    let base = SeqWorld::boot_base();
    let mut w = SeqWorld::fork(&base);
    w.run(Program::new(ops)).await;
    w.target_value(idx)
}

// ─────────────────────────── passing (regression) ───────────────────────────

/// `counter` — L1→L2 single deferred entry. The pre-registered base trigger
/// (`SetterWrapper` → `Value`) is exactly this shape (trigger index 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter() {
    assert_eq!(settle(vec![interact(0, 5)], 0).await, U256::from(5u64));
}

/// `helloWorld` — L1→L2 with a richer synchronous return. Our `Value` returns
/// `(bool changed, uint256 newValue)`, so the same single-hop path covers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_world() {
    assert_eq!(settle(vec![interact(0, 1)], 0).await, U256::from(1u64));
}

/// `multi-call-twice` — two deferred entries with the SAME `proxyEntryHash`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_call_twice() {
    assert_eq!(settle(vec![Op::RegisterMultiCall, interact(1, 9)], 1).await, U256::from(9u64));
}

/// `multi-call-two-diff` — two deferred entries with DIFFERENT `proxyEntryHash`es.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_call_two_diff() {
    assert_eq!(settle(vec![Op::RegisterTwoDiff, interact(1, 11)], 1).await, U256::from(11u64));
}

/// `revertContinue` — a try/catch wrapper over a cross-chain call that reverts
/// (odd arg) and CONTINUES; the target keeps its prior (even) value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revert_continue() {
    let v = settle(vec![Op::RegisterRevertTolerant, interact(1, 8), interact(1, 7)], 1).await;
    assert_eq!(v, U256::from(8u64), "even settles to 8; odd reverts and leaves it");
}

// ─────────────────────── expected-fail (composer gaps) ───────────────────────

/// `nestedCounter` / `deepNested` (depth-2) — an L2 target that itself makes a
/// cross-chain call. EXPECTED FAIL: the composer rejects same-rollup nesting
/// (`InvalidReentry`); the cross-rollup workaround reverts `RollingHashMismatch`
/// in the oracle, so the target never settles. The fuzzer hits this same wall.
/// Per the `deepNested` e2e, the fix is to accept same-rollup self-referential
/// nested dispatch (which the contracts support).
#[ignore = "composer gap: depth-2 nested compose reverts RollingHashMismatch (overlay pairing / InvalidReentry)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_counter() {
    assert_eq!(settle(vec![Op::DeployRelayToL1, interact(1, 4)], 1).await, U256::from(4u64));
}

// ───────────────────────── backlog (not yet expressible) ─────────────────────
//
// These `script/e2e` scenarios need a new `Op` and/or a composer feature before
// they can be written as a `Program`. The e2e suite IS the op/feature backlog:
//
//   bridge            value/etherDelta → a `Bridge` op (payable trigger + funded
//                     signer) + a balance-based oracle (settled effect is ETH
//                     balance, not slot-0).
//   deepNested(3+)    arbitrary depth → recursive `DeployNested{inner_idx}` over a
//                     unified target pool + same-rollup self-referential proxies.
//   reentrant         4-hop cascading nested actions → a deep-chain op.
//   revertCounter     `revertSpan` forced revert (succeeds, state rolled back,
//                     rolling hash commits success) → a force-revert wrapper + oracle.
//   nestedCallRevert  reverting reentrant → `LookupCall{failed}` fallback → a
//                     nested+revert combo op.
//   multi-call-nested multi-entry mix of pure + nested entries → multi-call + nest.
//   *L2 (counterL2,…) L2-as-entry → drive the new direction-bit/L2-entry world via
//                     an `Op` (the world exists; the op to select it does not yet).
