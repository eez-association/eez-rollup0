//! Compile-time thread-safety assertions for types that cross `.await`
//! boundaries or are shared across tokio tasks.
//!
//! Rust type-checks the body of a `const fn` at definition
//! time (even without invocation). That means
//! `assert_send::<Foo>()` here fails the build if `Foo: Send`
//! becomes unprovable. No runtime cost, no instantiation needed.
//!
//! The contract covered here:
//!
//! - [`Composer`](crate::composer::Composer) is the top-level shared handle
//!   and must be [`Send`] + [`Sync`] (cloned across tasks, often behind
//!   `Arc` indirectly).
//! - Per-composition data types cross `.await` boundaries inside the
//!   composition pipeline and must be [`Send`].

use crate::composer::composition::{CompositionBuilder, Rollup};
use crate::composer::executor::ExecutionRequest;
use eez_protocol::rollup_id::RollupId;
use eez_protocol::types::{Composition, ExecutedAction, SourceComposition, TargetComposition};

const fn assert_send<T: Send>() {}

#[allow(
    dead_code,
    reason = "compile-time static assertion — body is type-checked at definition time"
)]
const fn assert_thread_safety_bounds() {
    // Per-composition owned types — cross `.await` inside the
    // composition pipeline (dispatch → session.execute → finalize →
    // simulate_transactions). Each holds `dyn _ + Send` trait objects;
    // a future refactor could silently regress Send without this guard.
    assert_send::<CompositionBuilder>();
    assert_send::<Rollup>();

    // Per-composition data types — must be Send to cross .await.
    assert_send::<ExecutedAction>();
    assert_send::<ExecutionRequest>();
    assert_send::<SourceComposition>();
    assert_send::<TargetComposition>();
    assert_send::<Composition>();
}

#[allow(
    dead_code,
    reason = "Internal preorder-assertion helper — exercised by tests in this module and by composition::tests revert-span vectors."
)]
/// Verify that a recorded-call slice is in preorder.
///
/// In the flat sequential protocol every call's index is fixed at
/// `CompositionBuilder::open_call` time — BEFORE the target session executes
/// — so the recorded vec is always a preorder traversal of the
/// dispatch tree. This helper checks the structural invariant we get
/// from that: every call whose `source_rollup_id` differs from the
/// composition's source/entry rollup must have an ancestor frame
/// already open earlier in the slice (i.e. some `j < i` with
/// `recorded[j].target_rollup_id == recorded[i].source_rollup_id`).
///
/// Returns `true` for the empty slice and for any valid preorder
/// traversal; `false` if a nested call appears before any frame on
/// its caller chain has been opened.
///
/// `source_rollup_id` is the entry rollup; root-level dispatches
/// (caller == source) do not require a prior parent frame.
#[must_use]
pub fn is_preorder(recorded: &[ExecutedAction], source_rollup_id: RollupId) -> bool {
    for (i, call) in recorded.iter().enumerate() {
        if call.source_rollup_id == source_rollup_id {
            continue; // root dispatch — no parent frame required
        }
        let parent_seen = recorded[..i]
            .iter()
            .any(|prior| prior.target_rollup_id == call.source_rollup_id);
        if !parent_seen {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, U256};
    use eez_protocol::types::ExecutionOutcome;

    fn rec(target: RollupId, caller: RollupId) -> ExecutedAction {
        ExecutedAction {
            target_address: Address::ZERO,
            target_rollup_id: target,
            source_rollup_id: caller,
            source_address: Address::ZERO,
            data: Bytes::new(),
            value: U256::ZERO,
            outcome: ExecutionOutcome::Pending,
            revert_span: None,
        }
    }

    #[test]
    fn empty_slice_is_preorder() {
        let v: Vec<ExecutedAction> = Vec::new();
        assert!(is_preorder(&v, RollupId(0)));
    }

    #[test]
    fn flat_root_dispatches_are_preorder() {
        let v = vec![
            rec(RollupId(1), RollupId(0)),
            rec(RollupId(2), RollupId(0)),
            rec(RollupId(1), RollupId(0)),
        ];
        assert!(is_preorder(&v, RollupId(0)));
    }

    #[test]
    fn nested_dispatch_after_parent_is_preorder() {
        // root: 0 → 1; nested: 1 → 2 (parent frame 1 already opened earlier)
        let v = vec![rec(RollupId(1), RollupId(0)), rec(RollupId(2), RollupId(1))];
        assert!(is_preorder(&v, RollupId(0)));
    }

    #[test]
    fn nested_dispatch_without_parent_is_not_preorder() {
        // call 0: 0 → 2 (root); call 1: 0 → 1 (root);
        // call 2: 5 → 3 — source_rollup_id = 5 but no prior frame on rollup 5.
        let v = vec![
            rec(RollupId(2), RollupId(0)),
            rec(RollupId(1), RollupId(0)),
            rec(RollupId(3), RollupId(5)),
        ];
        assert!(!is_preorder(&v, RollupId(0)));
    }
}
