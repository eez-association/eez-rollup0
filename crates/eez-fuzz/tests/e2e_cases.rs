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

/// A value-bearing interact for a payable trigger (`bridge()`); `v` is wei.
fn interact_value(trigger: u16, v: u64) -> Op {
    Op::Interact(FuzzTx {
        direction: Direction::L1ToL2,
        trigger_sel: trigger,
        method_sel: 0,
        signer_sel: 0,
        nonce: 0,
        value: v,
        args: [0, 0, 0, 0],
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

/// Like [`settle`] but returns the target's settled L2 BALANCE — for `Skip`-mode
/// triggers (a `bridge()` value transfer) whose effect isn't slot-0.
async fn settle_balance(ops: Vec<Op>, idx: usize) -> U256 {
    let base = SeqWorld::boot_base();
    let mut w = SeqWorld::fork(&base);
    w.run(Program::new(ops)).await;
    w.target_balance(idx)
}

/// Like [`settle`] but each step carries an explicit DIRECTION — for ports that
/// enter on L2 (`*L2`). `target_value` reads the chain the target settles on.
async fn settle_mixed(steps: Vec<(Op, Direction)>, idx: usize) -> U256 {
    let base = SeqWorld::boot_base();
    let mut w = SeqWorld::fork(&base);
    w.run(Program::mixed(steps)).await;
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
    assert_eq!(
        settle(vec![Op::RegisterMultiCall, interact(1, 9)], 1).await,
        U256::from(9u64)
    );
}

/// `multi-call-two-diff` — two deferred entries with DIFFERENT `proxyEntryHash`es.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_call_two_diff() {
    assert_eq!(
        settle(vec![Op::RegisterTwoDiff, interact(1, 11)], 1).await,
        U256::from(11u64)
    );
}

/// `revertContinue` — a try/catch wrapper over a cross-chain call that reverts
/// (odd arg) and CONTINUES; the target keeps its prior (even) value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revert_continue() {
    let v = settle(
        vec![Op::RegisterRevertTolerant, interact(1, 8), interact(1, 7)],
        1,
    )
    .await;
    assert_eq!(
        v,
        U256::from(8u64),
        "even settles to 8; odd reverts and leaves it"
    );
}

/// `RegisterProxy` in the L1→L2 direction (the bidirectional op's implemented
/// side): deploy a `Value`, wrap it in an L1 proxy + `SetterWrapper`, settle it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_proxy_l1() {
    // Deploy → pool idx 1; RegisterProxy registers trigger idx 1.
    let p = vec![
        Op::Deploy,
        Op::RegisterProxy { value_idx: 1 },
        interact(1, 5),
    ];
    assert_eq!(settle(p, 1).await, U256::from(5u64));
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
    assert_eq!(
        settle(vec![Op::DeployRelayToL1, interact(1, 4)], 1).await,
        U256::from(4u64)
    );
}

/// `deepNested` (depth-3) — now EXPRESSIBLE via the recursive `DeployNested` op
/// (two levels of same-rollup self-referential nesting over a leaf). EXPECTED
/// FAIL for the same composer reason as `nested_counter`; once same-rollup
/// nesting composes, the fuzzer reaches depth-N by chaining `DeployNested`.
#[ignore = "composer gap: same-rollup nested compose unimplemented (depth wall)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_nested() {
    // base trigger=0; DeployNested registers triggers at 1, 2 (the depth-2 top).
    assert_eq!(
        settle(vec![Op::DeployNested, Op::DeployNested, interact(2, 6)], 2).await,
        U256::from(6u64)
    );
}

/// `reentrant` (depth-3 cascade) — chaining three `DeployNested`s exercises the
/// recursive overlay push/pop pairing the reentrant fixture stresses. EXPECTED
/// FAIL on the same nesting wall; faithful self-reentrancy (a target reentering
/// ITSELF) still wants a dedicated self-referential op.
#[ignore = "composer gap: same-rollup nested compose unimplemented (depth wall)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reentrant_deep() {
    let p = vec![
        Op::DeployNested,
        Op::DeployNested,
        Op::DeployNested,
        interact(3, 9),
    ];
    assert_eq!(settle(p, 3).await, U256::from(9u64));
}

/// `revertCounter` (`revertSpan`) — the cross-chain `setValue` lands but runs in
/// a self-call that always reverts, so its span must be force-discarded: the
/// target MUST stay 0. If the composer instead commits the "successful" call,
/// the value leaks (a soundness break) and this asserts it caught that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revert_counter() {
    // ForceRevertWrapper registers at trigger 1; its target stays unchanged.
    assert_eq!(
        settle(vec![Op::RegisterForceRevert, interact(1, 7)], 1).await,
        U256::ZERO
    );
}

/// `bridge` — L1→L2 value transfer: `BridgeSender.bridge()` forwards `msg.value`
/// to the L2 proxy, which lands on the L2 `BridgeReceiver`. The settled effect
/// is the receiver's BALANCE.
#[ignore = "composer gap: value-bearing cross-chain call not settled (no balance delivery)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge() {
    // Bridge registers at trigger 1; the receiver should receive the wei sent.
    assert_eq!(
        settle_balance(vec![Op::RegisterBridge, interact_value(1, 1000)], 1).await,
        U256::from(1000u64)
    );
}

/// `counterL2` (`*L2`) — the L2-as-entry direction: a user tx on L2 hits an L2
/// proxy of an L1 `Value`. Expressed via `RegisterProxy` in the `L2ToL1`
/// direction (the bidirectional op's mirror). EXPECTED FAIL: the composer has
/// no L2-as-entry settling path, so `compose` returns `Err` (target reverted) —
/// the harness exercises that path but can't settle it. Flips the day L2→L1
/// lands (mirrors `lib.rs::l2_to_l1_is_rejected_today`).
#[ignore = "composer gap: L2→L1 has no settling path (compose returns Err: target reverted)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_l2() {
    let prog = vec![
        (Op::RegisterProxy { value_idx: 0 }, Direction::L2ToL1),
        (interact(1, 5), Direction::L2ToL1), // step dir ignored — derived from trigger
    ];
    assert_eq!(settle_mixed(prog, 1).await, U256::from(5u64));
}

// ───────────────────────── backlog (residual) ────────────────────────────────
//
// Implemented above as ops: bridge (`RegisterBridge`), deepNested/reentrant
// (`DeployNested`), revertCounter (`RegisterForceRevert`), and *L2 via the
// per-step `Direction` axis (`RegisterProxy` mirrored to `L2ToL1`). These two
// still need a new op before they're a faithful `Program`:
//
//   nestedCallRevert  reverting reentrant → `LookupCall{failed}` fallback. Wants
//                     a nested-leaf-reverts op (NestedValue over a RevertableValue).
//   multi-call-nested multi-entry mix of pure + nested entries → a multi-call op
//                     whose entries target a nested proxy.
//
// Both are nesting variants behind the SAME wall `deep_nested`/`nested_counter`
// already document, so they'd add `#[ignore]`s without new coverage until the
// composer accepts same-rollup nesting.
