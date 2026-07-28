//! Core types

use alloy_primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::abi::EvmBatch;
use crate::rollup_id::RollupId;

/// A cross-chain call detected, dispatched, and recorded with its
/// execution outcome.
///
/// The outcome field is non-optional: `CompositionBuilder::dispatch_call`
/// (the only constructor) runs after the target session has produced an
/// outcome, so holding a `ExecutedAction` means the result is present.
///
/// The action hash is not stored here. It is a derived value
/// [`crate::entries::build_batch`] computes from these raw fields when
/// building entries, keeping it the single source of truth for hashes.
#[derive(Debug, Clone)]
pub struct ExecutedAction {
    /// Address of the target contract on the target chain.
    /// Spec: `Action.targetAddress` / `L2ToL1Call.targetAddress`.
    pub target_address: Address,
    /// Rollup ID of the target chain. Spec: `Action.targetRollupId`.
    pub target_rollup_id: RollupId,
    /// Rollup ID of the chain that triggered this call. Spec:
    /// `Action.sourceRollupId` / `L2ToL1Call.sourceRollupId`.
    ///
    /// For top-level calls detected during source simulation: equal to
    /// the entry rollup id. For nested calls dispatched by target-session
    /// inspectors: the rollup id of the session that dispatched.
    ///
    /// Load-bearing for nested action-hash correctness: the upstream
    /// CCM contracts emit `sourceRollupId = ROLLUP_ID` (their own id)
    /// on nested cross-chain calls.
    pub source_rollup_id: RollupId,
    /// Address of the caller on the source chain. Spec:
    /// `Action.sourceAddress` / `L2ToL1Call.sourceAddress`.
    pub source_address: Address,
    /// Calldata for the cross-chain call. Spec: `Action.data`.
    pub data: Bytes,
    /// Value transferred with the call. Spec: `Action.value`.
    pub value: U256,
    /// Execution outcome from the target-chain executor. `Pending`
    /// from `Dispatcher::open_call` until `Dispatcher::close_call`
    /// resolves it; `Resolved { .. }` thereafter.
    pub outcome: ExecutionOutcome,
    /// Length of the revert span (`recorded[..]` indices, inclusive of this
    /// call) when this call's outer EVM frame reverted; `None` otherwise. Maps
    /// to on-chain `L2ToL1Call.revertSpan`;
    pub revert_span: Option<u32>,
}

/// Lifecycle outcome of a recorded call.
///
/// The dispatch lifecycle is two-phase: `open_call` pushes a
/// `Pending` record and returns its index; `close_call` rewrites
/// that slot with `Resolved { .. }` once the target session has
/// produced a real result. The split exists because the index must
/// be fixed BEFORE recursing into `session.execute` — that's what
/// makes `recorded[..]` preorder rather than post-order, which in
/// turn makes the `revertSpan = recorded_count() - frame_start`
/// arithmetic at `Inspector::call_end` correct without tree
/// reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionOutcome {
    /// `open_call` placeholder. Replaced with `Resolved` by `close_call`.
    Pending,
    /// Final outcome supplied by the target session.
    Resolved {
        /// Raw bytes returned by the call. Empty if the call returns nothing.
        return_data: Vec<u8>,
        /// State root before this call was executed.
        pre_state_root: [u8; 32],
        /// State root after this call completed.
        post_state_root: [u8; 32],
        /// Gas consumed by this call.
        gas_used: u64,
        /// `false` if the call reverted.
        success: bool,
    },
}

impl ExecutionOutcome {
    /// `true` if this outcome is `Resolved` and the call succeeded.
    /// `false` for `Pending` and for `Resolved { success: false, .. }`.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Resolved { success: true, .. })
    }

    /// `true` while the slot is still a placeholder.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Borrow the post-state-root if resolved.
    #[must_use]
    pub fn post_state_root(&self) -> Option<&[u8; 32]> {
        match self {
            Self::Pending => None,
            Self::Resolved {
                post_state_root, ..
            } => Some(post_state_root),
        }
    }

    /// Borrow the pre-state-root if resolved.
    #[must_use]
    pub fn pre_state_root(&self) -> Option<&[u8; 32]> {
        match self {
            Self::Pending => None,
            Self::Resolved { pre_state_root, .. } => Some(pre_state_root),
        }
    }

    /// Borrow the return-data if resolved.
    #[must_use]
    pub fn return_data(&self) -> Option<&[u8]> {
        match self {
            Self::Pending => None,
            Self::Resolved { return_data, .. } => Some(return_data.as_slice()),
        }
    }

    /// Gas used if resolved.
    #[must_use]
    pub fn gas_used(&self) -> Option<u64> {
        match self {
            Self::Pending => None,
            Self::Resolved { gas_used, .. } => Some(*gas_used),
        }
    }
}

/// Source-chain output inside a `Composition`.
#[derive(Debug, Clone)]
pub struct SourceComposition {
    /// Rollup ID of the source chain.
    pub rollup_id: RollupId,
    /// Table-loading batch the source rollup will consume.
    pub batch: EvmBatch,
}

/// Per-target output inside a `Composition`.
///
/// Uses `Vec` (not `HashMap`) for ordering — upstream's invariant 2
/// requires deterministic output from identical inputs.
#[derive(Debug, Clone)]
pub struct TargetComposition {
    /// Rollup ID of the target chain this entry describes.
    pub rollup_id: RollupId,
    /// Table-loading batch this target rollup will consume.
    pub batch: EvmBatch,
}

/// Output of [`CompositionBuilder::finalize`](crate::CompositionBuilder::finalize) —
/// one `batch` per chain. `targets` ordering is significant (invariant 2).
#[derive(Debug, Clone)]
pub struct Composition {
    /// Source-chain output (exactly one source per composition).
    pub source: SourceComposition,
    /// Per-target outputs, one per target rollup. Order is significant.
    pub targets: Vec<TargetComposition>,
}
