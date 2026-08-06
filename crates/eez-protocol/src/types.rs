//! Core types: what a detected call looks like, what an execution
//! returns, and the shape of a finished composition.
//!
//! # Composition output shape
//!
//! ```text
//!   Composition
//!   ├── source:  SourceComposition
//!   │             ├── rollup_id          : RollupId
//!   │             └── batch              : EvmBatch
//!   │
//!   └── targets: Vec<TargetComposition>              non-empty targets, sorted by rollup ID
//!                 ├── rollup_id            : RollupId
//!                 └── batch                : EvmBatch
//! ```
//!
//! `Composition` contains structured `EvmBatch` values, not encoded transaction
//! calldata. Finalization converts each recorded call into entries for the entry
//! rollup and the destination rollup. Downstream code consumes those entries to
//! build system transactions and, for settlement, may merge batches and attach
//! state updates and proof data before encoding submission calldata.

use alloy_primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::abi::EvmBatch;
use crate::action::CallMode;
use crate::rollup_id::RollupId;

/// A recorded cross-chain call and its target execution status.
///
/// [`CompositionBuilder::open_call`](crate::CompositionBuilder::open_call)
/// inserts the action with [`ExecutionOutcome::Pending`] before dispatching
/// target execution. [`CompositionBuilder::close_call`](crate::CompositionBuilder::close_call)
/// replaces it with the resolved result.
/// Recording before target execution preserves preorder when execution
/// dispatches nested calls.
///
/// An `ExecutedAction` is not an ABI entry. Finalization can derive both
/// source-side and target-side entries from the same recorded call.
///
/// Protocol call hashes are derived from these fields during entry
/// materialization.
#[derive(Debug, Clone)]
pub struct ExecutedAction {
    /// Call mode observed at interception and committed to the call hash.
    /// Current composition materialization accepts only mutable calls.
    pub call_mode: CallMode,
    /// Address invoked on the destination chain.
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
    /// Target execution result for this call.
    pub outcome: ExecutionOutcome,
    /// Number of consecutive recorded calls covered by a reverted EVM frame,
    /// starting with this call. Only the first call in a span carries this
    /// value; `None` means no enclosing revert was observed.
    ///
    /// The materializer rejects calls with a revert span, preventing
    /// reverted calls from being emitted as successful entries. The inspector
    /// normally populates it after execution through
    /// [`CompositionBuilder::annotate_revert_span`](crate::CompositionBuilder::annotate_revert_span)
    /// when it observes a reverted frame.
    pub revert_span: Option<u32>,
}

/// Target execution result for a recorded cross-chain call.
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

/// Source-side execution batch for the entry rollup.
#[derive(Debug, Clone)]
pub struct SourceComposition {
    /// Entry rollup whose transaction was simulated.
    pub rollup_id: RollupId,
    /// Source-side entries derived from calls observed during simulation.
    pub batch: EvmBatch,
}

/// Target-side batch for one non-entry rollup reached by recorded calls.
///
/// L1 targets contain immediate post-batch entries. L2 targets contain inbound
/// sidecar entries used to construct system transactions.
#[derive(Debug, Clone)]
pub struct TargetComposition {
    /// Destination rollup represented by this target batch.
    pub rollup_id: RollupId,
    /// Target-side entries derived from calls addressed to `rollup_id`.
    pub batch: EvmBatch,
}

/// Source and target batches derived from one simulated entry transaction by
/// [`CompositionBuilder::finalize`](crate::CompositionBuilder::finalize).
///
/// Target outputs are non-empty and sorted by rollup ID. Transaction
/// construction and settlement occur downstream.
#[derive(Debug, Clone)]
pub struct Composition {
    /// Batch output for the entry/source rollup.
    pub source: SourceComposition,
    /// Non-empty target outputs, sorted by rollup ID.
    pub targets: Vec<TargetComposition>,
}
