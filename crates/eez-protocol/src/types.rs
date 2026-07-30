//! Core types: what a detected call looks like, what an execution
//! returns, and the shape of a finished composition.
//!
//! # Composition output shape
//!
//! ```text
//!   Composition
//!   ├── source:  SourceComposition
//!   │             ├── rollup_id          : RollupId
//!   │             ├── batch              : EvmBatch              table-loading batch
//!   │             └── entry_payload      : Vec<u8>               encoded entry-chain calldata
//!   │                                                            (L1-style: postAndVerifyBatch; L2-style: loadExecutionTable)
//!   │
//!   └── targets: Vec<TargetComposition>                          one per target rollup, ordered
//!                 ├── rollup_id            : RollupId
//!                 ├── batch                : EvmBatch             table-loading batch
//!                 ├── load_table_payload   : Vec<u8>              encoded load-execution-table calldata
//!                 └── execute_payload      : Vec<u8>              encoded execute-cross-chain-call calldata
//! ```
//!
//! Both sides carry the batch AND pre-encoded calldata so callers can
//! either re-hash / verify the batch themselves or ship the payload
//! straight into a wallet for signing + broadcast.

use alloy_primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::action::CallMode;
use crate::batch::EvmBatch;
use crate::rollup_id::RollupId;

/// A cross-chain call detected, dispatched, and recorded with its
/// execution outcome.
///
/// `CompositionBuilder::open_call` records a pending item before recursive
/// dispatch, and `CompositionBuilder::close_call` replaces it with the target
/// execution result.
///
/// The cross-chain call hash is not stored here. Entry materializers derive the
/// appropriate source- or destination-side hash from these raw fields.
#[derive(Debug, Clone)]
pub struct ExecutedAction {
    /// Effective EVM mode observed when the source call was intercepted.
    pub call_mode: CallMode,
    /// Contract invoked on the destination chain.
    pub target_address: Address,
    /// Rollup ID of the destination chain.
    pub target_rollup_id: RollupId,
    /// Rollup ID of the chain that triggered this call.
    ///
    /// For top-level calls detected during source simulation: equal to
    /// the entry rollup id. For nested calls dispatched by target-session
    /// inspectors: the rollup id of the session that dispatched.
    ///
    /// The source manager uses its own rollup id for nested calls, so this
    /// field is part of the nested cross-chain call hash.
    pub source_rollup_id: RollupId,
    /// Address of the caller on the source chain.
    pub source_address: Address,
    /// Calldata for the cross-chain call.
    pub data: Bytes,
    /// Native value transferred with the call.
    pub value: U256,
    /// Execution outcome from the target-chain executor. `Pending`
    /// from `Dispatcher::open_call` until `Dispatcher::close_call`
    /// resolves it; `Resolved { .. }` thereafter.
    pub outcome: ExecutionOutcome,
    /// Length of the revert span (in `recorded[..]` indices,
    /// inclusive of this call) when this call's outer EVM frame
    /// reverted. `None` for calls whose frames returned successfully
    /// or whose revert was not observed.
    ///
    /// Maps directly to the on-chain `L2ToL1Call.revertSpan` field
    /// for top-level calls (see `IEEZ.sol`'s `L2ToL1Call` struct
    /// under the multi-prover protocol; formerly
    /// `CrossChainCall.revertSpan`). Populated post-close by
    /// [`CompositionBuilder::annotate_revert_span`](crate::CompositionBuilder::annotate_revert_span)
    /// when the inspector observes the frame returning with
    /// `InstructionResult::Revert`.
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
///
/// Session rollback checkpoints are stored separately by
/// [`CompositionBuilder`](crate::CompositionBuilder) while a call is open.
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
///
/// Mirrors [`TargetComposition`] so both sides of the composition carry
/// raw entries AND pre-encoded calldata.
#[derive(Debug, Clone)]
pub struct SourceComposition {
    /// Rollup ID of the source chain.
    pub rollup_id: RollupId,
    /// Table-loading batch the source rollup will consume.
    pub batch: EvmBatch,
    /// Encoded calldata for the entry-chain tx that loads `batch`.
    /// Dialect-dependent: L1-style emits `postAndVerifyBatch(...)`;
    /// L2-style emits `loadExecutionTable(...)`.
    pub entry_payload: Vec<u8>,
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
    /// Encoded payload for loading the target execution table.
    pub load_table_payload: Vec<u8>,
    /// Encoded payload for executing the first cross-chain call.
    pub execute_payload: Vec<u8>,
}

/// Output of [`CompositionBuilder::finalize`](crate::CompositionBuilder::finalize) —
/// everything needed for all chains.
///
/// Symmetric: the source side and every target side both carry entries
/// AND pre-encoded calldata. Callers wrap each payload in a tx of their
/// choice to finalize. `targets` ordering is significant (invariant 2).
/// N=2 means `targets` has exactly one element; the design supports any N.
#[derive(Debug, Clone)]
pub struct Composition {
    /// Source-chain output (exactly one source per composition).
    pub source: SourceComposition,
    /// Per-target outputs, one per target rollup. Order is significant.
    pub targets: Vec<TargetComposition>,
}
