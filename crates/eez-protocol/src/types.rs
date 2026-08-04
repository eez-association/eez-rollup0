//! Core types: what a detected call looks like, what an execution
//! returns, and the shape of a finished composition.
//!
//! # Composition output shape
//!
//! ```text
//!   Composition
//!   ├── source:  SourceComposition
//!   │             ├── rollup_id          : RollupId
//!   │             └── batch              : EvmBatch              semantic batch
//!   │
//!   └── targets: Vec<TargetComposition>                          non-empty targets, sorted by rollup ID
//!                 ├── rollup_id            : RollupId
//!                 └── batch                : EvmBatch             semantic batch
//! ```
//!
//! Composition contains semantic batches, not transaction calldata.
//! Downstream code derives system transactions from those batches and later
//! attaches settlement state updates and proof data before encoding submission
//! calldata.

use alloy_primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::action::CallMode;
use crate::batch::EvmBatch;
use crate::rollup_id::RollupId;

/// A recorded cross-chain call and its target execution status.
///
/// [`CompositionBuilder::open_call`](crate::CompositionBuilder::open_call)
/// inserts the action with [`ExecutionOutcome::Pending`] before dispatching
/// target execution. `close_call` replaces it with the resolved result.
/// Recording before dispatch preserves preorder when nested calls occur.
///
/// Protocol call hashes are derived from these fields during entry
/// materialization.
#[derive(Debug, Clone)]
pub struct ExecutedAction {
    /// Effective EVM mode observed when the source call was intercepted.
    pub call_mode: CallMode,
    /// Contract invoked on the destination chain.
    pub target_address: Address,
    /// Rollup ID of the destination chain.
    pub target_rollup_id: RollupId,
    /// Rollup on which the intercepted call executed.
    ///
    /// For a top-level call this is the entry rollup. For a nested dispatch it
    /// is the rollup hosting the target session that observed the nested call.
    /// This value participates in cross-chain call-hash derivation.
    pub source_rollup_id: RollupId,
    /// Address of the caller on the source chain.
    pub source_address: Address,
    /// Calldata for the cross-chain call.
    pub data: Bytes,
    /// Native value transferred with the call.
    pub value: U256,
    /// Target execution status. `open_call` initializes it as `Pending`;
    /// `close_call` replaces it with the target session's result.
    pub outcome: ExecutionOutcome,
    /// Number of consecutive recorded actions covered by a reverted EVM frame,
    /// starting with this action. Only the first action in a span carries this
    /// value; `None` means no enclosing revert was observed.
    ///
    /// The current materializer rejects actions with a revert span, preventing
    /// reverted calls from being emitted as successful entries. The inspector
    /// normally populates it after execution through
    /// [`CompositionBuilder::annotate_revert_span`](crate::CompositionBuilder::annotate_revert_span)
    /// when it observes a reverted frame.
    pub revert_span: Option<u32>,
}

/// Target execution status for a recorded cross-chain call.
///
/// `open_call` records `Pending` before execution, fixing the action's preorder
/// position, and `close_call` replaces it with `Resolved`. Session rollback
/// checkpoints are maintained separately by
/// [`CompositionBuilder`](crate::CompositionBuilder).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionOutcome {
    /// `open_call` placeholder. Replaced with `Resolved` by `close_call`.
    Pending,
    /// Final outcome supplied by the target session.
    Resolved {
        /// Raw target-EVM output, including revert data for an unsuccessful call.
        return_data: Vec<u8>,
        /// State root before this call was executed.
        pre_state_root: [u8; 32],
        /// State root after this call completed.
        post_state_root: [u8; 32],
        /// Gas consumed by this call.
        gas_used: u64,
        /// Whether target EVM execution completed successfully.
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

/// Semantic batch output associated with the entry/source rollup.
#[derive(Debug, Clone)]
pub struct SourceComposition {
    /// Rollup ID of the source chain.
    pub rollup_id: RollupId,
    /// Semantic batch materialized for the source side.
    pub batch: EvmBatch,
}

/// Per-target output inside a `Composition`.
#[derive(Debug, Clone)]
pub struct TargetComposition {
    /// Rollup associated with this target batch.
    pub rollup_id: RollupId,
    /// Semantic batch materialized for this target side.
    pub batch: EvmBatch,
}

/// Semantic batch output of
/// [`CompositionBuilder::finalize`](crate::CompositionBuilder::finalize).
///
/// This value does not contain transaction calldata, settlement state updates,
/// or proof data; downstream code constructs those artifacts. `targets`
/// contains only non-empty target batches and is sorted by rollup ID.
#[derive(Debug, Clone)]
pub struct Composition {
    /// Batch output for the entry/source rollup.
    pub source: SourceComposition,
    /// Non-empty target outputs, sorted by rollup ID.
    pub targets: Vec<TargetComposition>,
}
