//! Core types — generic over [`ChainProtocol`].
//!
//! These are the values that cross trait boundaries: what a detected
//! call looks like, what an execution returns, and the shape of a
//! finished composition.
//!
//! # Composition output shape
//!
//! ```text
//!   Composition<P>
//!   ├── source:  SourceComposition<P>
//!   │             ├── rollup_id          : RollupId
//!   │             ├── batch              : P::Batch             chain-shaped table-loading batch
//!   │             └── entry_payload      : Vec<u8>              encoded entry-chain calldata
//!   │                                                           (L1-style: postVerifyAndExecuteOrSaveExecutionsFromBatch; L2-style: loadExecutionTable)
//!   │
//!   └── targets: Vec<TargetComposition<P>>                       one per target rollup, ordered
//!                 ├── rollup_id            : RollupId
//!                 ├── batch                : P::Batch            chain-shaped table-loading batch
//!                 ├── load_table_payload   : Vec<u8>             encoded load-execution-table calldata
//!                 └── execute_payload      : Vec<u8>             encoded execute-cross-chain-call calldata
//! ```
//!
//! Both sides carry the chain-shaped batch AND pre-encoded calldata so
//! callers can either re-hash / verify the batch themselves or ship the
//! payload straight into a wallet for signing + broadcast.

use serde::{Deserialize, Serialize};

use crate::protocol::ChainProtocol;
use crate::rollup_id::RollupId;

/// A cross-chain call detected, dispatched, and recorded with its
/// execution outcome.
///
/// Generic over `P: ChainProtocol` — each chain uses its own address,
/// value, and calldata types.
///
/// The outcome field is non-optional: `CompositionBuilder::dispatch_call`
/// (the only constructor) runs after the target session has produced an
/// outcome, so holding a `RecordedCall` means the result is present.
///
/// The action hash is not stored here. It is a derived value the
/// protocol (via `ChainProtocol`) computes from these raw fields when
/// building entries. Keeping it out of the struct makes
/// [`crate::ChainProtocol::build_batch`] the single source of truth
/// for hashes.
pub struct RecordedCall<P: ChainProtocol + ?Sized> {
    /// Address of the target contract on the target chain.
    pub original_address: P::Address,
    /// Rollup ID of the target chain.
    pub original_rollup_id: RollupId,
    /// Rollup ID of the chain that triggered this call.
    ///
    /// For top-level calls detected during source simulation: equal to
    /// the entry rollup id. For nested calls dispatched by target-session
    /// inspectors: the rollup id of the session that dispatched.
    ///
    /// Load-bearing for nested action-hash correctness: the upstream
    /// CCM contracts emit `sourceRollupId = ROLLUP_ID` (their own id)
    /// on nested cross-chain calls.
    pub caller_rollup_id: RollupId,
    /// Address of the caller on the source chain.
    pub caller: P::Address,
    /// Calldata for the cross-chain call.
    pub calldata: P::Calldata,
    /// Value transferred with the call.
    pub value: P::Value,
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
    /// for top-level calls (see `ICrossChainManager.sol`'s
    /// `L2ToL1Call` struct under the multi-prover protocol;
    /// formerly `CrossChainCall.revertSpan`). Populated post-close by
    /// [`Dispatcher::annotate_revert_span`](crate::Dispatcher::annotate_revert_span)
    /// when the inspector observes the frame returning with
    /// `InstructionResult::Revert`.
    pub revert_span: Option<u32>,
    /// Static-context metadata, populated by the inspector when this
    /// call was observed inside a `STATICCALL` frame (read-only
    /// reentry). Routes the call into the batch's
    /// `l1ToL2lookupCalls[]` queue (renamed from the prior protocol's
    /// `staticCalls[]`) instead of the entry's flat `L2ToL1Calls[]`
    /// list. `None` for normal (mutating) calls.
    pub static_meta: Option<StaticMeta>,
}

/// Inspector-time metadata for cross-chain calls observed inside a
/// static-execution context (the proxy detects `STATICCALL` reentry
/// via a `tstore` self-call).
///
/// The full `LookupCall` payload (cross-chain call hash, return data,
/// callNumber, lastNestedActionConsumed, sub-calls, rollingHash) is
/// materialized by the unified flat emitter from this marker plus the
/// recorded outcome and the entry-level rolling-hash counters.
/// (`LookupCall` was named `StaticCall` in the pre-multi-prover
/// protocol — same shape, renamed upstream.)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StaticMeta;

// Manual Clone + Debug: `#[derive]` generates overly strict bounds
// (`P: Clone` / `P: Debug`) rather than bounds on the field types
// (`P::Address: Clone` etc.). Hand-written impls use only the
// associated-type bounds `ChainProtocol` already supplies.
impl<P: ChainProtocol + ?Sized> Clone for RecordedCall<P> {
    fn clone(&self) -> Self {
        Self {
            original_address: self.original_address.clone(),
            original_rollup_id: self.original_rollup_id,
            caller_rollup_id: self.caller_rollup_id,
            caller: self.caller.clone(),
            calldata: self.calldata.clone(),
            value: self.value.clone(),
            outcome: self.outcome.clone(),
            revert_span: self.revert_span,
            static_meta: self.static_meta.clone(),
        }
    }
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for RecordedCall<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordedCall")
            .field("original_address", &self.original_address)
            .field("original_rollup_id", &self.original_rollup_id)
            .field("caller_rollup_id", &self.caller_rollup_id)
            .field("caller", &self.caller)
            .field("calldata", &self.calldata)
            .field("value", &self.value)
            .field("outcome", &self.outcome)
            .field("revert_span", &self.revert_span)
            .field("static_meta", &self.static_meta)
            .finish()
    }
}

/// Lifecycle outcome of a recorded call.
///
/// The `Dispatcher` lifecycle is two-phase: `open_call` pushes a
/// `Pending` record and returns its index; `close_call` rewrites
/// that slot with `Resolved { .. }` once the target session has
/// produced a real result. The split exists because the index must
/// be fixed BEFORE recursing into `session.execute` — that's what
/// makes `recorded[..]` preorder rather than post-order, which in
/// turn makes the `revertSpan = recorded_count() - frame_start`
/// arithmetic at `Inspector::call_end` correct without tree
/// reconstruction.
///
/// Does NOT carry the checkpoint (overlay + witness); that lives on
/// [`ExecutionResponse`](crate::ExecutionResponse).
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
pub struct SourceComposition<P: ChainProtocol + ?Sized> {
    /// Rollup ID of the source chain.
    pub rollup_id: RollupId,
    /// Chain-shaped batch the source rollup will consume — see
    /// [`ChainProtocol::Batch`].
    pub batch: P::Batch,
    /// Encoded calldata for the entry-chain tx that loads `batch`.
    /// Dialect-dependent: L1-style emits `postBatch(...)`; L2-style
    /// emits `loadExecutionTable(...)`.
    pub entry_payload: Vec<u8>,
}

// Manual Clone: `#[derive(Clone)]` generates `P: Clone` rather than
// `P::Batch: Clone`, which narrows callers unnecessarily. Hand-written
// impl uses only the bounds already supplied by `ChainProtocol`.
impl<P: ChainProtocol + ?Sized> Clone for SourceComposition<P> {
    fn clone(&self) -> Self {
        Self {
            rollup_id: self.rollup_id,
            batch: self.batch.clone(),
            entry_payload: self.entry_payload.clone(),
        }
    }
}

// Manual Debug: `ChainProtocol::Batch` doesn't require `Debug`, so
// `#[derive(Debug)]` wouldn't compile. Terse output anyway —
// `entry_payload.len()` is what's actionable for diagnostics.
impl<P: ChainProtocol + ?Sized> std::fmt::Debug for SourceComposition<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceComposition")
            .field("rollup_id", &self.rollup_id)
            .field("entry_payload", &self.entry_payload.len())
            .finish()
    }
}

/// Per-target output inside a `Composition`.
///
/// Uses `Vec` (not `HashMap`) for ordering — invariant 2 requires
/// deterministic output from identical inputs.
pub struct TargetComposition<P: ChainProtocol + ?Sized> {
    /// Rollup ID of the target chain this entry describes.
    pub rollup_id: RollupId,
    /// Chain-shaped batch this target rollup will consume — see
    /// [`ChainProtocol::Batch`].
    pub batch: P::Batch,
    /// Encoded `loadExecutionTable(...)` payload for the 2-tx
    /// CCM-verify path. Unused when [`Self::inbound_payload`] is `Some`
    /// (the inbound entry point loads the table itself).
    pub load_table_payload: Vec<u8>,
    /// Encoded `executeL1ToL2Call(...)` payload, paired with
    /// [`Self::load_table_payload`] in the 2-tx path. Unused when
    /// [`Self::inbound_payload`] is `Some`.
    pub execute_payload: Vec<u8>,
    /// `Some` when this target is the **arriving** side of a cross-chain
    /// call (e.g. the L2 side of an L1→L2 deposit). Encodes the fused
    /// `executeIncomingCrossChainCall(...)` system tx
    /// (`EEZL2.sol:174-212`); the composer signs it as ONE L2 system tx
    /// (with [`Self::inbound_value`] as `msg.value`) instead of the
    /// load+exec pair.
    pub inbound_payload: Option<Vec<u8>>,
    /// `msg.value` for the fused inbound system tx — strict-equality
    /// with the outer call's value (`EEZL2.sol:194`, else
    /// `ValueMismatch`). Zero for non-deposit calls.
    pub inbound_value: P::Value,
}

// Manual Clone + Debug — `derive` would infer overly strict
// `P: Clone` / unreachable `P::Batch: Debug` bounds.
impl<P: ChainProtocol + ?Sized> Clone for TargetComposition<P> {
    fn clone(&self) -> Self {
        Self {
            rollup_id: self.rollup_id,
            batch: self.batch.clone(),
            load_table_payload: self.load_table_payload.clone(),
            execute_payload: self.execute_payload.clone(),
            inbound_payload: self.inbound_payload.clone(),
            inbound_value: self.inbound_value.clone(),
        }
    }
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for TargetComposition<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TargetComposition")
            .field("rollup_id", &self.rollup_id)
            .field("load_table_payload", &self.load_table_payload.len())
            .field("execute_payload", &self.execute_payload.len())
            .field(
                "inbound_payload_len",
                &self.inbound_payload.as_ref().map(Vec::len),
            )
            .finish()
    }
}

/// Output of [`compose_transaction`](crate::compose_transaction) —
/// everything needed for all chains.
///
/// Symmetric: the source side and every target side both carry entries
/// AND pre-encoded calldata. Callers wrap each payload in a tx of their
/// choice to finalize. `targets` ordering is significant (invariant 2).
/// N=2 means `targets` has exactly one element; the design supports any N.
pub struct Composition<P: ChainProtocol + ?Sized> {
    /// Source-chain output (exactly one source per composition).
    pub source: SourceComposition<P>,
    /// Per-target outputs, one per target rollup. Order is significant.
    pub targets: Vec<TargetComposition<P>>,
}

// Manual Clone + Debug — same reason pattern as the inner halves
// above: derive would infer overly strict `P: Clone`; Debug is
// deliberately terse (just `targets.len()`).
impl<P: ChainProtocol + ?Sized> Clone for Composition<P> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            targets: self.targets.clone(),
        }
    }
}

impl<P: ChainProtocol + ?Sized> std::fmt::Debug for Composition<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composition")
            .field("source", &self.source)
            .field("targets", &self.targets.len())
            .finish()
    }
}
