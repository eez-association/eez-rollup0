//! Compile-time thread-safety assertions for types that cross `.await`
//! boundaries or are shared across tokio tasks.
//!
//! Rust type-checks the body of a generic `const fn` at definition
//! time (even without invocation): the bounds inside an inner call
//! must be provable from the outer signature's bounds. That means
//! `assert_send::<Foo<P>>()` here fails the build if `Foo<P>: Send`
//! becomes unprovable from `P: ChainProtocol + …`. No runtime cost,
//! no instantiation needed.
//!
//! The contract covered here:
//!
//! - [`Composer<P>`](crate::Composer) is the top-level shared handle
//!   and must be [`Send`] + [`Sync`] (cloned across tasks, often behind
//!   `Arc` indirectly).
//! - Per-composition data types cross `.await` boundaries inside the
//!   composition pipeline and must be [`Send`].
//!
//! A sibling chain-family crate with a concrete `ChainProtocol` impl
//! verifies these again at monomorphization — this file is the
//! earliest point in the dependency graph where the regression can
//! fire.

use crate::composer::Composer;
use crate::composition::{CompositionBuilder, Rollup};
use crate::executor::{ExecutionRequest, ExecutionResponse};
use crate::protocol::ChainProtocol;
use crate::rollup_id::RollupId;
use crate::types::{Composition, RecordedCall, SourceComposition, TargetComposition};

const fn assert_send<T: Send>() {}
const fn assert_send_sync<T: Send + Sync>() {}

#[allow(
    dead_code,
    reason = "compile-time static assertion — body is type-checked at definition time"
)]
const fn assert_thread_safety_bounds<P: ChainProtocol + Send + Sync + 'static>()
where
    P::Address: Clone + Send + Sync,
{
    // `Composer<P>` — top-level shared handle, must be Send + Sync.
    assert_send_sync::<Composer<P>>();

    // Per-composition owned types — cross `.await` inside the
    // composition pipeline (dispatch → session.execute → finalize →
    // simulate_transactions). Each holds `dyn _ + Send` trait objects;
    // a future refactor could silently regress Send without this guard.
    assert_send::<CompositionBuilder<P>>();
    assert_send::<Rollup<P>>();

    // Per-composition data types — must be Send to cross .await.
    assert_send::<RecordedCall<P>>();
    assert_send::<ExecutionRequest<P>>();
    assert_send::<ExecutionResponse<P>>();
    assert_send::<SourceComposition<P>>();
    assert_send::<TargetComposition<P>>();
    assert_send::<Composition<P>>();
}

#[allow(
    dead_code,
    reason = "Internal preorder-assertion helper — exercised by tests in this module and by composition::tests revert-span vectors."
)]
/// Verify that a recorded-call slice is in preorder.
///
/// In the flat sequential protocol every call's index is fixed at
/// `Dispatcher::open_call` time — BEFORE the target session executes
/// — so the recorded vec is always a preorder traversal of the
/// dispatch tree. This helper checks the structural invariant we get
/// from that: every call whose `caller_rollup_id` differs from the
/// composition's source/entry rollup must have an ancestor frame
/// already open earlier in the slice (i.e. some `j < i` with
/// `recorded[j].original_rollup_id == recorded[i].caller_rollup_id`).
///
/// Returns `true` for the empty slice and for any valid preorder
/// traversal; `false` if a nested call appears before any frame on
/// its caller chain has been opened.
///
/// `source_rollup_id` is the entry rollup; root-level dispatches
/// (caller == source) do not require a prior parent frame.
#[must_use]
pub fn is_preorder<P: ChainProtocol + ?Sized>(
    recorded: &[RecordedCall<P>],
    source_rollup_id: RollupId,
) -> bool {
    for (i, call) in recorded.iter().enumerate() {
        if call.caller_rollup_id == source_rollup_id {
            continue; // root dispatch — no parent frame required
        }
        let parent_seen = recorded[..i]
            .iter()
            .any(|prior| prior.original_rollup_id == call.caller_rollup_id);
        if !parent_seen {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ExecutionOutcome;

    // Tiny stand-in protocol so we can build RecordedCalls without
    // pulling in the EVM crate.
    #[derive(Debug, Clone, Copy, Default)]
    struct PreorderProto;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
    struct EmptyState;

    impl ChainProtocol for PreorderProto {
        type Address = u64;
        type Value = u64;
        type Calldata = Vec<u8>;
        type Batch = ();
        type Overlay = EmptyState;
        type Witness = EmptyState;
        type Dialect = ();

        fn build_batch(
            &self,
            _recorded: &[RecordedCall<Self>],
            _attribution: &crate::composer::SourceAttribution<'_>,
            _dialect: &Self::Dialect,
            _src: RollupId,
            _raw: &[u8],
        ) -> crate::error::ProtocolResult<Self::Batch> {
            Ok(())
        }
        fn encode_postbatch(&self, (): &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_load_table(&self, (): &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_follower_trigger(
            &self,
            _: &RecordedCall<Self>,
            _: RollupId,
            _: &[u8],
            (): &Self::Dialect,
        ) -> Vec<u8> {
            vec![]
        }
        fn encode_address(&self, a: &u64) -> Vec<u8> {
            a.to_be_bytes().to_vec()
        }
        fn decode_address(&self, b: &[u8]) -> crate::error::ProtocolResult<u64> {
            b.try_into().map(u64::from_be_bytes).map_err(|_e| {
                crate::error::ProtocolErrorKind::InvalidEncoding("addr".into()).into()
            })
        }
        fn encode_value(&self, v: &u64) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        fn decode_value(&self, b: &[u8]) -> crate::error::ProtocolResult<u64> {
            b.try_into().map(u64::from_be_bytes).map_err(|_e| {
                crate::error::ProtocolErrorKind::InvalidEncoding("value".into()).into()
            })
        }
        fn encode_calldata(&self, d: &Vec<u8>) -> Vec<u8> {
            d.clone()
        }
        fn decode_calldata(&self, b: &[u8]) -> crate::error::ProtocolResult<Vec<u8>> {
            Ok(b.to_vec())
        }
    }

    fn rec(target: RollupId, caller: RollupId) -> RecordedCall<PreorderProto> {
        RecordedCall {
            original_address: 0,
            original_rollup_id: target,
            caller_rollup_id: caller,
            caller: 0,
            calldata: Vec::new(),
            value: 0,
            outcome: ExecutionOutcome::Pending,
            revert_span: None,
            static_meta: None,
        }
    }

    #[test]
    fn empty_slice_is_preorder() {
        let v: Vec<RecordedCall<PreorderProto>> = Vec::new();
        assert!(is_preorder::<PreorderProto>(&v, RollupId(0)));
    }

    #[test]
    fn flat_root_dispatches_are_preorder() {
        let v = vec![
            rec(RollupId(1), RollupId(0)),
            rec(RollupId(2), RollupId(0)),
            rec(RollupId(1), RollupId(0)),
        ];
        assert!(is_preorder::<PreorderProto>(&v, RollupId(0)));
    }

    #[test]
    fn nested_dispatch_after_parent_is_preorder() {
        // root: 0 → 1; nested: 1 → 2 (parent frame 1 already opened earlier)
        let v = vec![rec(RollupId(1), RollupId(0)), rec(RollupId(2), RollupId(1))];
        assert!(is_preorder::<PreorderProto>(&v, RollupId(0)));
    }

    #[test]
    fn nested_dispatch_without_parent_is_not_preorder() {
        // call 0: 0 → 2 (root); call 1: 0 → 1 (root);
        // call 2: 5 → 3 — caller_rollup_id = 5 but no prior frame on rollup 5.
        let v = vec![
            rec(RollupId(2), RollupId(0)),
            rec(RollupId(1), RollupId(0)),
            rec(RollupId(3), RollupId(5)),
        ];
        assert!(!is_preorder::<PreorderProto>(&v, RollupId(0)));
    }
}
