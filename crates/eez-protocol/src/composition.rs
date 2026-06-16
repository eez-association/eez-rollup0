//! Composition builder — drives a single cross-chain composition from
//! proxy-call detection through final `Composition` output.
//!
//! The builder is a **concrete generic struct**, not a trait. It
//! merges three concerns that all operate on the same per-composition
//! state (the rollup map + the list of recorded calls):
//!
//! - **Target routing**: on each detected proxy call during source
//!   simulation, the source inspector calls
//!   [`CompositionBuilder::dispatch_call`] (via the [`Dispatcher`]
//!   trait), which looks up the registered rollup session by
//!   `rollup_id` and forwards the call — returning the outcome to the
//!   source inspector so execution can continue.
//! - **Recording**: each dispatched call is stored internally as a
//!   [`RecordedCall`] (outcome non-optional — it's always present by
//!   the time the call is recorded).
//! - **Finalization**: [`CompositionBuilder::finalize`] consumes the
//!   builder, runs per-rollup CCM verification (skipping the entry
//!   rollup), builds source + target entries via [`ChainProtocol`]
//!   methods, and produces a [`crate::types::Composition`].
//!
//! # Design
//!
//! - **Sealed at construction**. All [`Rollup<P>`] plans are passed in
//!   at `new`; no `register_rollup` on the builder itself. The
//!   composer layer enforces uniqueness before calling `new`.
//! - **Owned [`Rollup<P>`] per rollup**. Each rollup bundles the
//!   client, an optional session (`None` until the first dispatch
//!   opens it — the entry rollup's session stays `None` whenever no
//!   inspector dispatches back to the entry chain), the config (for
//!   CCM verify), and the initial state root.
//! - **Entry-aware**. `finalize` skips the entry rollup in both the
//!   CCM-verify loop (entry has no system-tx CCM path — L1 verifies
//!   via `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch`'s
//!   proof bundle) and the target-composition loop (entry rollup's
//!   output lives in `source`, not `targets`).
//!
//! # Lifecycle
//!
//! ```text
//!      ┌──────────────────────────────────────────────────┐
//!      │ CompositionBuilder::new(entry_id, rollups)       │
//!      │   rollups   = HashMap<RollupId, Rollup<P>>       │
//!      │   recorded  = Vec<RecordedCall<P>>  (empty)      │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼ source sim runs, detects proxy call
//!      ┌──────────────────────────────────────────────────┐
//!      │ <builder as Dispatcher>.dispatch_call(...)       │  × N
//!      │   → lazy-open rollups[target].session             │
//!      │   → open_call → session.execute(req, &mut self)  │
//!      │     → close_call resolves the slot's outcome     │
//!      │   → return ExecutionResponse to inspector        │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!      ┌──────────────────────────────────────────────────┐
//!      │ finalize(&protocol, raw_tx)    (consumes self)   │
//!      │   1. validate: recorded + rollups non-empty      │
//!      │   2. CCM verify per non-entry rollup             │
//!      │      → patch terminal recorded.post_state_root   │
//!      │   3. protocol.build_batch(recorded, attribution, │
//!      │      dialect, source_id, raw_tx) — once per      │
//!      │      source + per non-entry target               │
//!      │   4. encode_table_payload + encode_follower_     │
//!      │      trigger per target                          │
//!      │   5. package into Composition<P>                 │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!                  Composition<P>
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{
    CompositionResult, ExecutorError, ExecutorErrorKind, ExecutorResult, ProtocolErrorKind,
};
use crate::executor::{
    ChainClient, ExecutionRequest, ExecutionResponse, TargetExecutionSession, TargetTransaction,
};
use crate::protocol::ChainProtocol;
use crate::rollup_id::RollupId;
use crate::types::{Composition, RecordedCall, SourceComposition, TargetComposition};

// Avoid a protocol → composer layering cycle: TargetConfig lives in
// `composer.rs`, but this module reads `config.verification_context`
// + `config.ccm_gas_limit` during finalize.
use crate::composer::TargetConfig;

// ── Dispatcher ───────────────────────────────────────────────────

/// The dispatch surface a source or target inspector calls when it
/// detects a cross-chain proxy call and needs the composer to route it.
///
/// Abstract so the composer (for in-process dispatch) and a gRPC
/// server (for bidi-streamed remote dispatch) can both satisfy it.
/// `CompositionBuilder<P>`'s blanket impl handles in-process dispatch;
/// gRPC servers use `StreamDispatcher`, which routes
/// `CallbackRequest` / `CallbackResponse` frames across the bidi
/// Execute stream via `BidiDispatchBridge`.
///
/// `Send` only (the inspector thread-scopes a one-shot `block_on` and
/// never moves the dispatcher across real tokio tasks).
///
/// # Lifecycle
///
/// `Dispatcher` runs each call through a two-phase open/close lifecycle:
///
/// 1. [`open_call`](Self::open_call) — push a `Pending` `RecordedCall`
///    placeholder, return its slot index. Called BEFORE recursing into
///    `session.execute`; this is what makes `recorded[..]` a preorder
///    traversal (parent's index is fixed before any nested dispatches
///    can push their own).
/// 2. [`close_call`](Self::close_call) — overwrite the slot's outcome
///    with the resolved result.
///
/// [`dispatch_call`](Self::dispatch_call) is the convenience entry
/// point — it wraps the lifecycle (`open` → `session.execute` →
/// `close`) and is what inspectors call. The split is exposed for
/// callers that need to interleave their own logic between open and
/// session.execute (none in-tree yet; the lifecycle methods are
/// mostly internal-stable for now).
///
/// [`recorded_count`](Self::recorded_count) snapshots the current
/// length so the inspector can bracket a CALL frame:
/// `start = recorded_count()` at frame open, `end = recorded_count()`
/// at frame end, and on revert the inspector calls
/// [`annotate_revert_span`](Self::annotate_revert_span) with the
/// resulting `(start, end - start)` so the bracketed calls' on-chain
/// `revertSpan` is captured.
///
/// `annotate_revert_span` is separate from `close_call` because
/// `Inspector::call_end` fires AFTER the inspector's own dispatch
/// returned and `close_call` already ran — re-rewriting the outcome
/// would trip a "slot already resolved" check. The post-close span
/// write is its own primitive.
#[async_trait::async_trait]
pub trait Dispatcher: Send {
    /// Chain protocol this dispatcher operates on.
    type Protocol: ChainProtocol + 'static;

    /// Convenience entry point: open → execute on the target session
    /// → close. Inspectors call this from their EVM-frame `call`
    /// handler.
    ///
    /// Implementations enforce a same-chain re-entry guard:
    /// `target_id == caller_id && target_id != entry_rollup_id`
    /// returns [`ExecutorErrorKind::InvalidReentry`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::InvalidReentry`] for same-chain
    /// non-entry self-dispatch. Returns [`ExecutorErrorKind::Unavailable`]
    /// if no rollup is registered under `target_id`. Propagates any
    /// executor error from the target session's `execute`.
    async fn dispatch_call(
        &mut self,
        target_id: RollupId,
        caller_id: RollupId,
        req: ExecutionRequest<Self::Protocol>,
    ) -> ExecutorResult<ExecutionResponse<Self::Protocol>>;

    /// Push a `Pending` placeholder for a new call and return its
    /// slot index. Called BEFORE the target session's `execute` so
    /// the index is stable across nested dispatches.
    ///
    /// # Errors
    ///
    /// Same as [`dispatch_call`](Self::dispatch_call) — re-entry guard
    /// and rollup-id validation fire here.
    async fn open_call(
        &mut self,
        target_id: RollupId,
        caller_id: RollupId,
        req: &ExecutionRequest<Self::Protocol>,
    ) -> ExecutorResult<usize>;

    /// Resolve the call opened by [`open_call`](Self::open_call) at `idx` with its outcome.
    /// `revert_span` carries the on-chain
    /// [`L2ToL1CallSol::revertSpan`](crate::ChainProtocol)
    /// for top-level calls when known at close time; most callers
    /// pass `None` and let
    /// [`annotate_revert_span`](Self::annotate_revert_span) fill it
    /// in post-frame.
    fn close_call(
        &mut self,
        idx: usize,
        outcome: crate::types::ExecutionOutcome,
        revert_span: Option<u32>,
    );

    /// Number of [`RecordedCall`]s captured so far in this composition.
    ///
    /// Used by the EVM inspector to bracket a CALL frame: snapshot
    /// the count at frame open, compare at `call_end`, and forward
    /// the resulting `(start, end - start)` to
    /// [`annotate_revert_span`](Self::annotate_revert_span) when the
    /// frame returned with `InstructionResult::Revert`.
    ///
    /// Default returns `0` for dispatchers that do not record calls
    /// locally (e.g. the gRPC `StreamDispatcher` — its server-side
    /// peer holds the recorded list).
    fn recorded_count(&self) -> usize {
        0
    }

    /// Annotate `recorded[idx].revert_span = Some(span)` AND evict
    /// every target session that captured writes inside the
    /// `[idx, idx + span as usize)` window so the next dispatch
    /// lazy-opens fresh from disk.
    ///
    /// Two rollback primitives coexist: explicit
    /// [`TargetExecutionSession::checkpoint`] /
    /// [`rollback`](TargetExecutionSession::rollback) for sessions that
    /// support it, and eviction (drop the in-memory `State`, re-read
    /// disk) for sessions that don't.
    ///
    /// Default is a no-op for dispatchers that do not own the
    /// recorded list (e.g. `StreamDispatcher`).
    #[allow(
        unused_variables,
        reason = "default impl is a no-op; param names document intent"
    )]
    fn annotate_revert_span(&mut self, idx: usize, span: u32) {}

    /// Inject pre-computed per-tx state roots for `rollup_id` into the
    /// dispatcher's eventual `finalize` step.
    ///
    /// Used by the entry-overlay path: the composer's CCM-verify loop
    /// in `finalize` skips the entry rollup (no system-tx CCM contract
    /// on L1), so nested calls attributed to the entry rollup have no
    /// `per_tx_roots` source. The entry overlay session captures one
    /// post-state root per overlay `execute` and, at end of
    /// `simulate_source_tx`, the source-sim path drains that buffer
    /// and forwards it here.
    ///
    /// Default impl is a no-op for dispatchers that don't need this
    /// (e.g. `StreamDispatcher` on the gRPC server side).
    #[allow(
        unused_variables,
        reason = "default impl is a no-op; param names document intent"
    )]
    fn set_extra_per_tx_roots(&mut self, rollup_id: RollupId, roots: Vec<[u8; 32]>) {}
}

// ── Rollup ───────────────────────────────────────────────────────

/// Per-rollup state held inside a [`CompositionBuilder`] during one
/// composition.
///
/// Carries:
///
/// - `client: Arc<dyn ChainClient>` directly (used for CCM-verify
///   `simulate_transactions` and lazy session opening).
/// - `session: Option<Box<dyn _>>`: opened on first `dispatch_call`
///   to this rollup. The entry rollup's session stays `None` whenever
///   no inspector dispatches back to the entry chain.
/// - `config: TargetConfig<P>` — `finalize` reads
///   `config.verification_context()` and `config.proxy_lookup` directly.
pub struct Rollup<P: ChainProtocol + 'static> {
    /// Client for this rollup — shared long-lived trait object.
    pub client: Arc<dyn ChainClient<Protocol = P> + Send + Sync>,
    /// Lazily-opened session for this rollup. `None` until the first
    /// [`CompositionBuilder::dispatch_call`] hits this rollup.
    pub session: Option<Box<dyn TargetExecutionSession<Protocol = P> + Send>>,
    /// Configuration for this rollup (CCM addresses, gas limit, proxy
    /// lookup).
    pub config: TargetConfig<P>,
    /// Root the entry chain currently holds for this rollup. Used as
    /// the `currentState` of the first source entry that touches this
    /// rollup.
    pub initial_state_root: [u8; 32],
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for Rollup<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rollup")
            .field("initial_state_root", &self.initial_state_root)
            .field("session_open", &self.session.is_some())
            .field("client", &"<dyn ChainClient>")
            .field("config", &self.config)
            .finish()
    }
}

// ── CompositionBuilder ───────────────────────────────────────────

/// Drives a single cross-chain composition.
///
/// A concrete generic struct (not a trait). One builder per source
/// transaction. Sealed at construction via [`CompositionBuilder::new`]
/// with the full set of [`Rollup<P>`] plans (including the entry
/// rollup); dispatches each proxy call via the [`Dispatcher`] trait
/// during source simulation; consumed by [`CompositionBuilder::finalize`]
/// to produce the final [`crate::types::Composition`].
pub struct CompositionBuilder<P: ChainProtocol + 'static> {
    pub(crate) entry_rollup_id: RollupId,
    pub(crate) rollups: HashMap<RollupId, Rollup<P>>,
    pub(crate) recorded: Vec<RecordedCall<P>>,
    /// Pre-computed per-tx state roots, keyed by rollup id, injected
    /// via [`Dispatcher::set_extra_per_tx_roots`]. Merged into
    /// `per_tx_roots_by_rollup` at the start of `finalize`'s CCM-verify
    /// loop — values do not get overwritten by CCM-verify when the
    /// rollup is skipped (e.g. the entry rollup), but a follower
    /// rollup that ALSO had pre-computed roots injected would have
    /// them clobbered by the CCM-verify result. In practice only the
    /// entry rollup uses this path (overlay session post-execute roots).
    pub(crate) extra_per_tx_roots: HashMap<RollupId, Vec<[u8; 32]>>,
    /// Per-call snapshot stash, keyed by `recorded[..]` index. Each
    /// open call grabs an opaque [`SessionSnapshot`] right before
    /// recursing into `session.execute`; the snapshot is dropped on
    /// the success path of `close_call` and pushed onto
    /// [`pending_rollbacks`] on the revert path.
    pub(crate) pending_snapshots: HashMap<usize, crate::executor::SessionSnapshot>,
    /// Recorded-call indices whose snapshots need rollback. Drained at
    /// the start of every async `dispatch_call` (and at finalize) so
    /// the rollback runs at the next `.await` point — keeps
    /// `close_call` / `annotate_revert_span` synchronous per the
    /// trait shape.
    pub(crate) pending_rollbacks: Vec<usize>,
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for CompositionBuilder<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rollup_ids: Vec<RollupId> = self.rollups.keys().copied().collect();
        f.debug_struct("CompositionBuilder")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("rollup_ids", &rollup_ids)
            .field("recorded", &self.recorded.len())
            .finish()
    }
}

impl<P: ChainProtocol + 'static> CompositionBuilder<P> {
    /// Construct a new builder for one source transaction.
    ///
    /// `rollups` must include the entry rollup. The composer layer
    /// enforces that invariant before calling `new`.
    #[must_use]
    pub fn new(entry_rollup_id: RollupId, rollups: HashMap<RollupId, Rollup<P>>) -> Self {
        tracing::debug!(
            name: "composer.builder.constructed",
            %entry_rollup_id,
            rollup_count = rollups.len(),
            "composition builder constructed"
        );
        Self {
            entry_rollup_id,
            rollups,
            recorded: Vec::new(),
            extra_per_tx_roots: HashMap::new(),
            pending_snapshots: HashMap::new(),
            pending_rollbacks: Vec::new(),
        }
    }

    /// Drain queued rollbacks and apply them. Called at the top of
    /// every `dispatch_call` (and from `finalize`) so a revert
    /// observed at the previous `Inspector::call_end` propagates to
    /// the affected target sessions before the next call opens.
    async fn process_pending_rollbacks(&mut self) -> ExecutorResult<()> {
        if self.pending_rollbacks.is_empty() {
            return Ok(());
        }
        let queued: Vec<usize> = std::mem::take(&mut self.pending_rollbacks);
        // Track distinct (rollup_id) under rollback so we only call
        // `session.rollback` once per session per dispatch boundary —
        // even if multiple recorded indices in the same span name the
        // same rollup, we use the OUTER (smallest idx) snapshot
        // (the deepest one to revert through).
        let mut handled: std::collections::HashSet<RollupId> = std::collections::HashSet::new();
        for idx in queued {
            let Some(snap) = self.pending_snapshots.remove(&idx) else {
                continue;
            };
            let Some(call) = self.recorded.get(idx) else {
                continue;
            };
            let rollup_id = call.original_rollup_id;
            if !handled.insert(rollup_id) {
                continue;
            }
            if let Some(rollup) = self.rollups.get_mut(&rollup_id) {
                if let Some(session) = rollup.session.as_mut() {
                    session.rollback(snap).await?;
                }
            }
        }
        // Any other snapshots still keyed under bracketed indices are
        // dropped — the head idx's rollback already restores their
        // shared session.
        Ok(())
    }

    /// Clone all recorded calls whose `original_rollup_id` matches
    /// `rollup_id` — the per-target group `finalize` processes.
    ///
    /// The recorded vec is preorder by construction (each call's index
    /// is fixed at `Dispatcher::open_call` time), so a linear filter
    /// preserves dispatch order without tree reconstruction. The
    /// unified emitter walks this pre-filtered slice directly.
    fn group_calls_for(&self, rollup_id: RollupId) -> Vec<RecordedCall<P>> {
        self.recorded
            .iter()
            .filter(|c| c.original_rollup_id == rollup_id)
            .cloned()
            .collect()
    }

    /// Consume the builder and produce the final [`Composition`].
    ///
    /// Steps, in order:
    ///
    /// 1. Validate: both `recorded` and `rollups` non-empty; every
    ///    recorded call targets a registered rollup.
    /// 2. For each **non-entry** rollup: simulate the two CCM system
    ///    transactions (`loadExecutionTable` +
    ///    `executeIncomingCrossChainCall`) and patch the terminal
    ///    recorded call's `post_state_root` with the CCM-path final
    ///    root. Entry rollup is skipped (L1 verifies via
    ///    `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch`'s
    ///    proof bundle, not system txs).
    /// 3. Call `protocol.build_batch` for the source rollup with per-rollup
    ///    initial state roots; encode via `protocol.encode_table_payload`.
    /// 4. Per **non-entry** rollup: `build_batch` + `encode_table_payload`
    ///    + `encode_follower_trigger`. One [`TargetComposition`] per
    ///    rollup.
    /// 5. Package as [`Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::EmptyCalls`] on empty inputs,
    /// [`ProtocolErrorKind::UnknownTarget`] for a recorded rollup not
    /// in the plan set, [`ProtocolErrorKind::InvalidCheckpoint`] if
    /// per-rollup state-delta chaining fails in `build_batch`.
    /// Surfaces any [`ExecutorError`] from CCM verification.
    pub async fn finalize(
        mut self,
        protocol: &P,
        raw_tx: &[u8],
    ) -> CompositionResult<Composition<P>> {
        tracing::debug!(name: "composer.finalize.start", "composition finalize started");

        if self.recorded.is_empty() || self.rollups.is_empty() {
            return Err(ProtocolErrorKind::EmptyCalls.into());
        }

        for call in &self.recorded {
            if !self.rollups.contains_key(&call.original_rollup_id) {
                return Err(ProtocolErrorKind::UnknownTarget {
                    got: call.original_rollup_id,
                }
                .into());
            }
        }

        // Sorted plan order for deterministic output (invariant 2).
        let mut plan_order: Vec<RollupId> = self.rollups.keys().copied().collect();
        plan_order.sort();

        // Per-rollup cumulative post-state roots collected during CCM
        // verify. Keyed by `RollupId`; one `Vec` per rollup that
        // contributed roots (entry rollup contributes from
        // overlay-session executes injected via
        // `Dispatcher::set_extra_per_tx_roots`; non-entry rollups
        // contribute from their CCM-batch simulation in the loop
        // below). Consumed by the source-entry build step via
        // `SourceAttribution::per_tx_roots_by_rollup` for
        // nested-composition invariant-6 chaining.
        let mut per_tx_roots_by_rollup: HashMap<RollupId, Vec<[u8; 32]>> = HashMap::new();

        // initial_roots is hoisted out of Phase 3 so the CCM-verify
        // loop can pass an attribution to `build_batch`.
        // Per-tx roots are still empty at this point — they're
        // populated later by the loop's `simulate_transactions` and
        // by `extra_per_tx_roots` for the entry rollup. The L1-as-
        // follower emitter uses `initial_roots[source_rollup_id]`
        // for its first stateDelta's currentState; with empty
        // per_tx_roots it emits a degenerate-tail chain (newState ==
        // currentState), which `Rollups.executeL2TX`'s simulation
        // accepts (each delta matches the rollup's stored root).
        let initial_roots: HashMap<RollupId, [u8; 32]> = self
            .rollups
            .iter()
            .map(|(id, r)| (*id, r.initial_state_root))
            .collect();

        // Under the multi-prover ABI, `proofs[]` lives inside the
        // batch struct (`ProofSystemBatchPerVerificationEntries.proofs`).
        // The composer's `encode_table_payload` path here emits the
        // empty-`proofs[]` batch destined for the CCM-verify simulator
        // and follower-side `loadExecutionTable` payloads; the real
        // L1-poster path (proofs populated, signatures attached) lives
        // in `eez_evm_inspector::post_batch_submitter`.

        // Phase 2 — per-rollup CCM verify (non-entry rollups only).
        //
        // For each non-entry rollup with non-empty group calls,
        // build the chain-shaped batch via `protocol.build_batch`,
        // assemble the 2-tx CCM-verify batch (load + execute), run
        // `simulate_transactions`, and record the per-tx roots so the
        // entry-side build_batch (Phase 3) can chain stateDeltas
        // through them.
        //
        // Entry rollup branch: drain pre-computed roots from the
        // overlay-session path (`extra_per_tx_roots`). No CCM-verify
        // path exists for the entry chain.
        let mut extra_per_tx_roots = std::mem::take(&mut self.extra_per_tx_roots);
        let mut target_batches: HashMap<RollupId, P::Batch> = HashMap::new();
        for rollup_id in &plan_order {
            if *rollup_id == self.entry_rollup_id {
                if let Some(roots) = extra_per_tx_roots.remove(rollup_id) {
                    per_tx_roots_by_rollup.insert(*rollup_id, roots);
                }
                continue;
            }
            let Some(rollup) = self.rollups.get(rollup_id) else {
                continue;
            };

            let group_calls = self.group_calls_for(*rollup_id);
            if group_calls.is_empty() {
                continue;
            }

            let dialect = &rollup.config.dialect;
            let attribution_so_far = crate::composer::SourceAttribution {
                initial_roots: &initial_roots,
                per_tx_roots_by_rollup: &per_tx_roots_by_rollup,
                entry_rollup_id: self.entry_rollup_id,
            };
            let batch = protocol.build_batch(
                &group_calls,
                &attribution_so_far,
                dialect,
                *rollup_id,
                raw_tx,
            )?;

            // Terminal-revert short-circuit: an empty batch means all
            // calls reverted and there's nothing to verify. Skip CCM
            // verify and the target-composition emission for this
            // rollup.
            if Self::is_batch_empty(protocol, &batch) {
                continue;
            }

            // The "outer root" call drives the follower's first proxy
            // invocation. In preorder the first matching call is the
            // outer-most root by construction.
            let outer_root = &group_calls[0];

            let verification = rollup.config.verification_context();
            let make_ccm_tx = |calldata: P::Calldata, value: P::Value| TargetTransaction::<P> {
                caller: verification.system_address.clone(),
                destination: verification.entrypoint_address.clone(),
                calldata,
                value,
                gas_limit: verification.gas_limit,
            };

            // CCM-verify tx shape branches on the outer call's
            // direction. Arriving (`original == follower`, e.g. L1→L2
            // deposit): the fused `executeIncomingCrossChainCall` system
            // tx — one tx, `msg.value` == outer value. Originating
            // (`caller == follower`, e.g. L2→L1 source side): the 2-tx
            // pattern (`loadExecutionTable` then `executeL1ToL2Call`).
            let is_arriving = outer_root.original_rollup_id == *rollup_id
                && outer_root.caller_rollup_id != *rollup_id;
            let txs: Vec<TargetTransaction<P>> = if let Some(fused) = is_arriving
                .then(|| protocol.encode_inbound_delivery(outer_root, &batch, dialect))
                .flatten()
            {
                let tx_fused =
                    make_ccm_tx(protocol.decode_calldata(&fused)?, outer_root.value.clone());
                vec![tx_fused]
            } else {
                let exec_calldata = protocol.encode_follower_trigger(
                    outer_root,
                    self.entry_rollup_id,
                    raw_tx,
                    dialect,
                );
                let load_calldata = protocol.encode_table_payload(&batch, dialect);
                let tx_load = make_ccm_tx(
                    protocol.decode_calldata(&load_calldata)?,
                    P::Value::default(),
                );
                let tx_exec = make_ccm_tx(
                    protocol.decode_calldata(&exec_calldata)?,
                    outer_root.value.clone(),
                );
                vec![tx_load, tx_exec]
            };

            let sim = rollup.client.simulate_transactions(&txs).await?;

            if let Some(last) = self
                .recorded
                .iter_mut()
                .rev()
                .find(|r| r.original_rollup_id == *rollup_id)
            {
                if let crate::types::ExecutionOutcome::Resolved {
                    post_state_root, ..
                } = &mut last.outcome
                {
                    *post_state_root = sim.final_state_root;
                }
            }

            tracing::debug!(
                name: "composer.ccm_verify",
                %rollup_id,
                final_root = ?sim.final_state_root,
                per_tx = sim.per_tx_roots.len(),
                "ccm verification complete"
            );

            per_tx_roots_by_rollup.insert(*rollup_id, sim.per_tx_roots);
            target_batches.insert(*rollup_id, batch);
        }

        // Phase 3 — entry-rollup batch (across full preorder slice).
        let attribution = crate::composer::SourceAttribution {
            initial_roots: &initial_roots,
            per_tx_roots_by_rollup: &per_tx_roots_by_rollup,
            entry_rollup_id: self.entry_rollup_id,
        };
        let entry_dialect = self
            .rollups
            .get(&self.entry_rollup_id)
            .expect("entry rollup registered at builder construction")
            .config
            .dialect
            .clone();
        let entry_batch = protocol.build_batch(
            &self.recorded,
            &attribution,
            &entry_dialect,
            self.entry_rollup_id,
            raw_tx,
        )?;
        let entry_payload = protocol.encode_table_payload(&entry_batch, &entry_dialect);

        // Phase 4 — target compositions (re-encode from the batches
        // captured in Phase 2). Skip entry rollup + empty groups.
        let mut target_compositions: Vec<TargetComposition<P>> = Vec::new();
        for rollup_id in &plan_order {
            if *rollup_id == self.entry_rollup_id {
                continue;
            }
            let Some(batch) = target_batches.remove(rollup_id) else {
                continue;
            };
            let group_calls = self.group_calls_for(*rollup_id);
            // group_calls[0] guaranteed non-empty because Phase 2 only
            // populated `target_batches` for non-empty groups.
            let outer_root = &group_calls[0];
            let rollup = self
                .rollups
                .get(rollup_id)
                .expect("plan_order from rollups map");
            let dialect = &rollup.config.dialect;
            let load_table_payload = protocol.encode_table_payload(&batch, dialect);
            let execute_payload =
                protocol.encode_follower_trigger(outer_root, self.entry_rollup_id, raw_tx, dialect);
            // Same arriving/originating split as Phase-2 dispatch:
            // arriving outer calls carry a fused inbound payload (signed
            // as one L2 system tx), originating ones fall back to the
            // load + exec pair.
            let is_arriving = outer_root.original_rollup_id == *rollup_id
                && outer_root.caller_rollup_id != *rollup_id;
            let inbound_payload = is_arriving
                .then(|| protocol.encode_inbound_delivery(outer_root, &batch, dialect))
                .flatten();
            let inbound_value = outer_root.value.clone();
            target_compositions.push(TargetComposition {
                rollup_id: *rollup_id,
                batch,
                load_table_payload,
                execute_payload,
                inbound_payload,
                inbound_value,
            });
        }

        tracing::debug!(
            name: "composer.finalize.complete",
            target_count = target_compositions.len(),
            "composition finalize complete"
        );

        Ok(Composition {
            source: SourceComposition {
                rollup_id: self.entry_rollup_id,
                batch: entry_batch,
                entry_payload,
            },
            targets: target_compositions,
        })
    }

    /// Forwarding helper for the terminal-revert short-circuit —
    /// delegates to the protocol's own `batch_is_empty` predicate.
    fn is_batch_empty(protocol: &P, batch: &P::Batch) -> bool {
        protocol.batch_is_empty(batch)
    }
}

#[async_trait::async_trait]
impl<P: ChainProtocol + 'static> Dispatcher for CompositionBuilder<P> {
    type Protocol = P;

    async fn dispatch_call(
        &mut self,
        target_id: RollupId,
        caller_id: RollupId,
        req: ExecutionRequest<P>,
    ) -> ExecutorResult<ExecutionResponse<P>> {
        // Drain any pending rollbacks queued by the previous frame's
        // `annotate_revert_span` / `close_call`. This is the next
        // async point — synchronous lifecycle methods cannot call
        // `session.rollback().await` directly.
        self.process_pending_rollbacks().await?;

        // Same-chain re-entry guard. Entry-to-entry dispatch (e.g. a
        // contract on the entry chain calling another entry-chain
        // contract during normal source simulation) is legitimate
        // and falls through.
        if target_id == caller_id && target_id != self.entry_rollup_id {
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: caller_id,
                target: target_id,
            }));
        }

        // Phase 1 — open: lazy-open the session, snapshot it, push
        // `Pending` placeholder, capture slot index.
        let idx = self.open_call(target_id, caller_id, &req).await?;

        // Phase 2 — run execute on the lazy-opened session.
        let mut session = self
            .rollups
            .get_mut(&target_id)
            .expect("rollup present (just checked)")
            .session
            .take()
            .expect("session opened by open_call");

        // `session.execute` awaits first; nested dispatches from a
        // target-session inspector call back into `self.dispatch_call`
        // and push their own `RecordedCall`s at indices `idx + 1, ..`.
        // The vec is preorder by construction.
        let response_res = session.execute(req, self).await;

        // Put the session back even on error; revert handling is
        // post-close via `annotate_revert_span`.
        self.rollups
            .get_mut(&target_id)
            .expect("rollup not removable")
            .session = Some(session);

        let response = response_res?;

        // Phase 3 — close: resolve the slot with the real outcome.
        self.close_call(idx, response.outcome.clone(), None);

        tracing::debug!(
            name: "composer.dispatch_call",
            %target_id,
            %caller_id,
            success = response.outcome.is_success(),
            gas = response.outcome.gas_used().unwrap_or(0),
            "dispatched cross-chain call"
        );

        Ok(response)
    }

    async fn open_call(
        &mut self,
        target_id: RollupId,
        caller_id: RollupId,
        req: &ExecutionRequest<P>,
    ) -> ExecutorResult<usize> {
        if target_id == caller_id && target_id != self.entry_rollup_id {
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: caller_id,
                target: target_id,
            }));
        }
        if !self.rollups.contains_key(&target_id) {
            return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                format!("no rollup registered for {target_id}"),
            )));
        }
        // Lazy-open the target session and snapshot its current state
        // before the call executes — the snapshot is the rollback
        // anchor if the surrounding frame later reverts. Snapshot is
        // type-erased so this code stays chain-agnostic; the
        // originating session's `rollback` downcasts internally.
        let snap = {
            let rollup = self
                .rollups
                .get_mut(&target_id)
                .expect("rollup present (just checked)");
            if rollup.session.is_none() {
                let new_session = rollup.client.begin_execution_session().await?;
                rollup.session = Some(new_session);
            }
            let session = rollup.session.as_mut().expect("session opened above");
            session.checkpoint().await?
        };

        let idx = self.recorded.len();
        self.recorded.push(RecordedCall {
            original_address: req.destination.clone(),
            original_rollup_id: target_id,
            caller_rollup_id: caller_id,
            caller: req.source_address.clone(),
            calldata: req.calldata.clone(),
            value: req.value.clone(),
            outcome: crate::types::ExecutionOutcome::Pending,
            revert_span: None,
            static_meta: None,
        });
        self.pending_snapshots.insert(idx, snap);
        Ok(idx)
    }

    fn close_call(
        &mut self,
        idx: usize,
        outcome: crate::types::ExecutionOutcome,
        revert_span: Option<u32>,
    ) {
        let slot = &mut self.recorded[idx];
        debug_assert!(
            slot.outcome.is_pending(),
            "close_call called on already-resolved slot {idx}",
        );
        slot.outcome = outcome;
        if let Some(span) = revert_span {
            slot.revert_span = Some(span);
            // Queue this slot's snapshot for rollback at the next
            // async dispatch boundary.
            self.pending_rollbacks.push(idx);
        }
        // Note: when `revert_span` is `None` (the common case — the
        // inspector observes revert post-frame and calls
        // [`annotate_revert_span`] AFTER `close_call` already ran),
        // the snapshot stays stashed in `pending_snapshots`. The
        // bracketing inspector frame's `call_end` decides between
        // queue-rollback (revert) and drop (commit). Any snapshot
        // still stashed after the EVM pass is dropped at finalize.
    }

    fn recorded_count(&self) -> usize {
        self.recorded.len()
    }

    fn annotate_revert_span(&mut self, idx: usize, span: u32) {
        if idx >= self.recorded.len() || span == 0 {
            return;
        }
        // Annotate the bracketing top-level call. Only the call at
        // the head of the span carries the on-chain `revertSpan`;
        // bracketed inner calls don't.
        self.recorded[idx].revert_span = Some(span);
        // Queue rollbacks for every recorded index in the bracket.
        // `process_pending_rollbacks` consolidates by rollup id and
        // applies one rollback per affected session at the next
        // async dispatch boundary.
        let end = idx.saturating_add(span as usize).min(self.recorded.len());
        for i in idx..end {
            if self.pending_snapshots.contains_key(&i) {
                self.pending_rollbacks.push(i);
            }
        }
        tracing::debug!(
            name: "composer.annotate_revert_span",
            idx,
            span,
            "annotated revert span and queued session rollback"
        );
    }

    fn set_extra_per_tx_roots(&mut self, rollup_id: RollupId, roots: Vec<[u8; 32]>) {
        if !roots.is_empty() {
            self.extra_per_tx_roots.insert(rollup_id, roots);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::ExecutionCheckpoint;
    use crate::composer::{DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig};
    use crate::error::ProtocolResult;
    use crate::executor::{TargetBatchSimulation, TargetVerificationContext};
    use crate::types::ExecutionOutcome;
    use serde::{Deserialize, Serialize};

    // ── Minimal FakeProtocol — trivial impls for ChainProtocol surface ──

    #[derive(Debug, Clone, Copy, Default)]
    struct FakeProtocol;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct FakePlaceholder;

    impl ChainProtocol for FakeProtocol {
        type Address = [u8; 20];
        type Value = u128;
        type Calldata = Vec<u8>;
        // Batch wraps the per-call post-state roots so tests can
        // inspect what roots landed in the final composition.
        type Batch = Vec<[u8; 32]>;
        type Overlay = FakePlaceholder;
        type Witness = FakePlaceholder;
        type Dialect = ();

        fn build_batch(
            &self,
            recorded: &[RecordedCall<Self>],
            _attribution: &crate::composer::SourceAttribution<'_>,
            _dialect: &Self::Dialect,
            _source_rollup_id: RollupId,
            _raw_tx: &[u8],
        ) -> ProtocolResult<Self::Batch> {
            // Terminal-revert short-circuit: if the head call reverted,
            // emit an empty batch.
            if recorded.first().is_some_and(|c| !c.outcome.is_success()) {
                return Ok(Vec::new());
            }
            Ok(recorded
                .iter()
                .map(|c| c.outcome.post_state_root().copied().unwrap_or([0u8; 32]))
                .collect())
        }

        fn encode_postbatch(&self, _batch: &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_load_table(&self, _batch: &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn batch_is_empty(&self, batch: &Self::Batch) -> bool {
            batch.is_empty()
        }
        fn encode_follower_trigger(
            &self,
            _call: &RecordedCall<Self>,
            _source_rollup_id: RollupId,
            _raw_tx: &[u8],
            _dialect: &Self::Dialect,
        ) -> Vec<u8> {
            vec![]
        }
        fn encode_address(&self, addr: &Self::Address) -> Vec<u8> {
            addr.to_vec()
        }
        fn decode_address(&self, bytes: &[u8]) -> ProtocolResult<Self::Address> {
            bytes.try_into().map_err(|_err| {
                crate::error::ProtocolErrorKind::InvalidEncoding("address".into()).into()
            })
        }
        fn encode_value(&self, val: &Self::Value) -> Vec<u8> {
            val.to_be_bytes().to_vec()
        }
        fn decode_value(&self, bytes: &[u8]) -> ProtocolResult<Self::Value> {
            bytes.try_into().map(u128::from_be_bytes).map_err(|_err| {
                crate::error::ProtocolErrorKind::InvalidEncoding("value".into()).into()
            })
        }
        fn encode_calldata(&self, data: &Self::Calldata) -> Vec<u8> {
            data.clone()
        }
        fn decode_calldata(&self, bytes: &[u8]) -> ProtocolResult<Self::Calldata> {
            Ok(bytes.to_vec())
        }
    }

    // ── Mock ChainClient (returns a canned CCM final root + session) ─

    struct MockClient {
        final_root: [u8; 32],
        session_outcome: ExecutionOutcome,
    }

    #[async_trait::async_trait]
    impl ChainClient for MockClient {
        type Protocol = FakeProtocol;
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = FakeProtocol> + Send>>
        {
            Ok(Box::new(MockSession {
                outcome: self.session_outcome.clone(),
            }))
        }
        async fn simulate_transactions(
            &self,
            txs: &[TargetTransaction<FakeProtocol>],
        ) -> ExecutorResult<TargetBatchSimulation> {
            if txs.is_empty() {
                return Err(crate::error::ExecutorErrorKind::EmptyBatch.into());
            }
            Ok(TargetBatchSimulation {
                final_state_root: self.final_root,
                per_tx_roots: vec![self.final_root; txs.len()],
            })
        }
    }

    // ── Mock TargetExecutionSession ──────────────────────────────────

    struct MockSession {
        outcome: ExecutionOutcome,
    }

    #[async_trait::async_trait]
    impl TargetExecutionSession for MockSession {
        type Protocol = FakeProtocol;

        async fn execute(
            &mut self,
            _req: ExecutionRequest<Self::Protocol>,
            _dispatcher: &mut (dyn Dispatcher<Protocol = Self::Protocol> + Send),
        ) -> ExecutorResult<ExecutionResponse<Self::Protocol>> {
            Ok(ExecutionResponse {
                outcome: self.outcome.clone(),
                checkpoint: ExecutionCheckpoint {
                    version: 1,
                    chain_id: 1,
                    base_block_number: 0,
                    base_block_hash: [0u8; 32],
                    base_state_root: [0u8; 32],
                    current_root: [0u8; 32],
                    overlay: FakePlaceholder,
                    witness: None,
                },
            })
        }

        async fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
            Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
        }

        async fn rollback(
            &mut self,
            _snap: crate::executor::SessionSnapshot,
        ) -> ExecutorResult<()> {
            Ok(())
        }

        async fn take_checkpoint(
            &mut self,
        ) -> Option<crate::executor::ProtocolCheckpoint<Self::Protocol>> {
            None
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn sample_outcome(post_root: [u8; 32]) -> ExecutionOutcome {
        ExecutionOutcome::Resolved {
            return_data: vec![0xAB],
            pre_state_root: [0u8; 32],
            post_state_root: post_root,
            gas_used: 21_000,
            success: true,
        }
    }

    fn make_request(rollup: u64) -> ExecutionRequest<FakeProtocol> {
        ExecutionRequest {
            destination: [rollup as u8; 20],
            calldata: vec![0x01, 0x02],
            value: 0,
            source_address: [0u8; 20],
            source_rollup: RollupId(0),
        }
    }

    fn target_config() -> TargetConfig<FakeProtocol> {
        TargetConfig {
            ccm_address: [0u8; 20],
            system_address: [0u8; 20],
            ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
            proxy_lookup: ProxyLookupConfig {
                contract_address: [0u8; 20],
                authorized_proxies_slot: 0,
            },
            dialect: (),
        }
    }

    fn entry_rollup(outcome_root: [u8; 32]) -> Rollup<FakeProtocol> {
        Rollup {
            client: Arc::new(MockClient {
                final_root: [0u8; 32],
                session_outcome: sample_outcome(outcome_root),
            }),
            session: None,
            config: target_config(),
            initial_state_root: [0u8; 32],
        }
    }

    fn rollup_with_session(outcome_root: [u8; 32]) -> Rollup<FakeProtocol> {
        Rollup {
            client: Arc::new(MockClient {
                final_root: [0u8; 32],
                session_outcome: sample_outcome(outcome_root),
            }),
            session: None,
            config: target_config(),
            initial_state_root: [0u8; 32],
        }
    }

    fn rollup_with_ccm(outcome_root: [u8; 32], ccm_final_root: [u8; 32]) -> Rollup<FakeProtocol> {
        let _ = TargetVerificationContext::<FakeProtocol> {
            system_address: [0xAA; 20],
            entrypoint_address: [0xBB; 20],
            gas_limit: 30_000_000,
        };
        Rollup {
            client: Arc::new(MockClient {
                final_root: ccm_final_root,
                session_outcome: sample_outcome(outcome_root),
            }),
            session: None,
            config: TargetConfig {
                ccm_address: [0xBB; 20],
                system_address: [0xAA; 20],
                ccm_gas_limit: 30_000_000,
                proxy_lookup: ProxyLookupConfig {
                    contract_address: [0u8; 20],
                    authorized_proxies_slot: 0,
                },
                dialect: (),
            },
            initial_state_root: [0u8; 32],
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_routes_to_registered_session_and_records() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        let response = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");
        assert_eq!(response.outcome.post_state_root(), Some(&[0x11u8; 32]));
        assert_eq!(builder.recorded.len(), 1);
        assert_eq!(builder.recorded[0].original_rollup_id, RollupId(1));
        assert_eq!(builder.recorded[0].caller_rollup_id, RollupId(0));
    }

    #[tokio::test]
    async fn dispatch_unknown_rollup_returns_unavailable() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(99), RollupId(0), make_request(99))
            .await
            .expect_err("should fail");
        assert!(matches!(err.kind(), ExecutorErrorKind::Unavailable(_)));
    }

    #[tokio::test]
    async fn finalize_empty_errors() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);
        let err = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(p.kind(), ProtocolErrorKind::EmptyCalls)
        ));
    }

    #[tokio::test]
    async fn finalize_without_ccm_produces_composition() {
        // Rollup 1's client returns an empty CCM final root — we
        // expect `build_batch` to produce a non-empty batch (which it
        // does in FakeProtocol), so CCM verify runs.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");

        let composition = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect("finalize");
        assert_eq!(composition.source.rollup_id, RollupId(0));
        // Entry rollup is skipped in the targets loop, so only rollup 1
        // appears in targets.
        assert_eq!(composition.targets.len(), 1);
        assert_eq!(composition.targets[0].rollup_id, RollupId(1));
    }

    #[tokio::test]
    async fn finalize_rejects_recorded_calls_for_unregistered_rollups() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);
        builder.recorded.push(RecordedCall {
            original_address: [0u8; 20],
            original_rollup_id: RollupId(99),
            caller_rollup_id: RollupId(0),
            caller: [0u8; 20],
            calldata: vec![],
            value: 0,
            outcome: sample_outcome([0u8; 32]),
            revert_span: None,
            static_meta: None,
        });

        let err = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(p.kind(), ProtocolErrorKind::UnknownTarget { got: RollupId(99) })
        ));
    }

    #[tokio::test]
    async fn finalize_targets_come_out_sorted_by_rollup_id() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(3), rollup_with_session([0x33; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        for id in [3u64, 1, 2] {
            builder
                .dispatch_call(RollupId(id), RollupId(0), make_request(id))
                .await
                .expect("dispatch");
        }

        let composition = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect("finalize");
        let ids: Vec<u64> = composition.targets.iter().map(|t| t.rollup_id.0).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "targets must be sorted by rollup_id, not insertion order (invariant 2)"
        );
    }

    #[tokio::test]
    async fn finalize_with_ccm_patches_terminal_post_state_root() {
        let original_post = [0x22; 32];
        let ccm_patched = [0xFF; 32];
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_ccm(original_post, ccm_patched));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");

        let composition = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect("finalize");
        assert_eq!(composition.source.batch.len(), 1);
        assert_eq!(
            composition.source.batch[0], ccm_patched,
            "terminal source batch entry should carry the CCM-patched root"
        );
    }

    #[tokio::test]
    async fn caller_rollup_id_is_stored_from_dispatch_arg() {
        // Regression guard: caller_rollup_id must come from the
        // `caller_id` arg on dispatch_call, not from req.source_rollup.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        // Pass RollupId(7) as caller_id — distinct from req.source_rollup
        // (which is RollupId(0) from make_request). The stored value
        // must match caller_id, not req.source_rollup.
        builder
            .dispatch_call(RollupId(1), RollupId(7), make_request(1))
            .await
            .expect("dispatch");
        assert_eq!(builder.recorded[0].caller_rollup_id, RollupId(7));
    }

    // ── Terminal-revert short-circuit (Codex A7 pre-flight) ─────────

    /// Pairs with a `MockClient` that panics if `simulate_transactions`
    /// is called — lets the test assert the CCM-verify path was
    /// skipped.
    struct NoCcmClient;

    #[async_trait::async_trait]
    impl ChainClient for NoCcmClient {
        type Protocol = FakeProtocol;
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = FakeProtocol> + Send>>
        {
            Ok(Box::new(MockSession {
                outcome: ExecutionOutcome::Resolved {
                    return_data: b"revert".to_vec(),
                    pre_state_root: [0u8; 32],
                    post_state_root: [0u8; 32],
                    gas_used: 1,
                    success: false,
                },
            }))
        }
        async fn simulate_transactions(
            &self,
            _txs: &[TargetTransaction<FakeProtocol>],
        ) -> ExecutorResult<TargetBatchSimulation> {
            panic!(
                "simulate_transactions must NOT be called on terminal-revert path — \
                 finalize's short-circuit should have skipped CCM verify"
            );
        }
    }

    fn rollup_with_reverted_session() -> Rollup<FakeProtocol> {
        Rollup {
            client: Arc::new(NoCcmClient),
            session: None,
            config: target_config(),
            initial_state_root: [0u8; 32],
        }
    }

    #[tokio::test]
    async fn empty_target_entries_skips_ccm_verify_and_omits_target_composition() {
        // Codex A7 pre-flight: terminal revert — the emitter returns
        // an empty target-entry set, and finalize must honor both
        // sides of that handshake:
        //   (a) skip CCM verify (NoCcmClient::simulate_transactions
        //       panics if called — regression guard);
        //   (b) omit the `TargetComposition` for the reverted rollup
        //       from the returned `Composition`.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_reverted_session());
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");
        assert!(
            !builder.recorded[0].outcome.is_success(),
            "test setup: recorded call must be reverted"
        );

        let composition = builder
            .finalize(&FakeProtocol, &[])
            .await
            .expect("finalize");

        // (a) handshake validated by NoCcmClient's panic-if-called
        //     impl of simulate_transactions — if we got here, CCM
        //     verify did NOT run on the reverted rollup.
        // (b) TargetComposition omitted:
        assert!(
            composition.targets.is_empty(),
            "finalize must omit TargetComposition for a rollup whose target entries are empty"
        );
    }

    // ── Router regression tests ──────────────────────────────────

    /// Same-chain non-entry self-dispatch must surface `InvalidReentry`
    /// loudly. L2 → L2 is architecturally disallowed: a non-entry
    /// rollup that issues a cross-chain call back to itself bypasses
    /// the entry-rollup CCM contract that mediates every legitimate
    /// reentry.
    #[tokio::test]
    async fn dispatch_same_chain_non_entry_returns_invalid_reentry() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(1), RollupId(1), make_request(1))
            .await
            .expect_err("L2 → L2 self-dispatch must be rejected");
        assert!(
            matches!(
                err.kind(),
                ExecutorErrorKind::InvalidReentry { caller, target }
                    if *caller == RollupId(1) && *target == RollupId(1)
            ),
            "expected InvalidReentry {{ caller: 1, target: 1 }}, got {err:?}",
        );
        assert!(
            builder.recorded.is_empty(),
            "nothing must be recorded when the guard fires"
        );
    }

    // ── Preorder lifecycle + revertSpan vector tests ─────────────────

    /// Preorder property: `open_call` fixes a slot index BEFORE the
    /// session executes. Closing the slot fills `Resolved`. Subsequent
    /// dispatches push at later indices regardless of nesting depth.
    #[tokio::test]
    async fn dispatch_records_preorder_at_open_call() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        // Manually walk the lifecycle to assert the index is fixed at
        // open_call BEFORE close_call resolves the slot.
        let req = make_request(1);
        let idx = builder
            .open_call(RollupId(1), RollupId(0), &req)
            .await
            .expect("open");
        assert_eq!(idx, 0);
        assert!(builder.recorded[idx].outcome.is_pending());
        builder.close_call(idx, sample_outcome([0xCC; 32]), None);
        assert!(builder.recorded[idx].outcome.is_success());
        assert_eq!(
            builder.recorded[idx].outcome.post_state_root(),
            Some(&[0xCCu8; 32])
        );

        // A second top-level dispatch lands at idx 1 — preorder.
        let resp = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");
        assert!(resp.outcome.is_success());
        assert_eq!(builder.recorded.len(), 2);
        assert!(crate::assertions::is_preorder::<FakeProtocol>(
            &builder.recorded,
            RollupId(0),
        ));
    }

    /// `annotate_revert_span` writes `revert_span = Some(span)` on
    /// the bracketing call AND queues every snapshot inside the
    /// bracket for rollback at the next async dispatch boundary.
    #[tokio::test]
    async fn annotate_revert_span_writes_span_and_queues_rollback() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");

        builder.annotate_revert_span(0, 1);

        assert_eq!(builder.recorded[0].revert_span, Some(1));
        assert_eq!(
            builder.pending_rollbacks,
            vec![0],
            "annotate_revert_span must queue idx 0 for rollback",
        );
        assert!(
            builder.pending_snapshots.contains_key(&0),
            "snapshot for the reverted slot must still be stashed",
        );
    }

    // Three synthetic revertSpan vectors covering edge cases the
    // upstream KEEP fixtures don't exercise (Codex final-review
    // amendment Major-new-2).

    /// Outer call dispatches 2 inner calls and then the outer's
    /// frame reverts — span = 3 covers the outer + both children.
    /// Bracketed children carry no `revert_span`.
    #[tokio::test]
    async fn revert_span_gt_one_covers_outer_plus_two_children() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        // Frame open: simulate inspector bracket — record start.
        let start = builder.recorded_count();
        // Outer: 0 → 1
        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("outer");
        // Two inner cross-rollup calls inside the outer frame.
        builder
            .dispatch_call(RollupId(2), RollupId(1), make_request(2))
            .await
            .expect("inner-1");
        builder
            .dispatch_call(RollupId(2), RollupId(1), make_request(2))
            .await
            .expect("inner-2");

        let end = builder.recorded_count();
        let span = (end - start) as u32;
        assert_eq!(span, 3);
        builder.annotate_revert_span(start, span);

        assert_eq!(builder.recorded[0].revert_span, Some(3));
        assert_eq!(builder.recorded[1].revert_span, None);
        assert_eq!(builder.recorded[2].revert_span, None);
    }

    /// Outer call A reverts, sibling outer B succeeds. A is annotated
    /// with span=1 (self-only); B is not annotated. Preorder indices
    /// stay monotonic; B is in slot 1, after A's slot 0.
    #[tokio::test]
    async fn sibling_after_revert_only_annotates_reverted_outer() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("A");
        builder.annotate_revert_span(0, 1);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("B");

        assert_eq!(builder.recorded[0].revert_span, Some(1));
        assert_eq!(builder.recorded[1].revert_span, None);
        assert!(crate::assertions::is_preorder::<FakeProtocol>(
            &builder.recorded,
            RollupId(0),
        ));
    }

    /// Outer call dispatches 1 successful inner call, then the outer
    /// reverts — span = 2 covers the outer plus the successful child.
    /// The child succeeded but is rolled back via the outer's bracket.
    #[tokio::test]
    async fn parent_reverts_after_successful_child() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        let start = builder.recorded_count();
        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("outer");
        builder
            .dispatch_call(RollupId(2), RollupId(1), make_request(2))
            .await
            .expect("child (succeeds)");
        let span = (builder.recorded_count() - start) as u32;
        assert_eq!(span, 2);

        // Both calls succeeded individually, but the outer frame
        // reverts — the inspector observes that and annotates.
        builder.annotate_revert_span(start, span);

        assert_eq!(builder.recorded[0].revert_span, Some(2));
        assert!(builder.recorded[1].outcome.is_success());
        assert_eq!(builder.recorded[1].revert_span, None);
        // Both bracketed slots are queued for rollback. The drain
        // happens at the next `dispatch_call` (covered by the E2E
        // suite); for unit-level coverage the queue contents are the
        // observable.
        assert_eq!(builder.pending_rollbacks, vec![0, 1]);
    }

    /// Revert at a previous `Inspector::call_end` is applied at the
    /// top of the next `dispatch_call` — the queued rollbacks drain
    /// to `session.rollback()` invocations before the new call opens.
    #[tokio::test]
    async fn pending_rollbacks_drain_at_next_dispatch() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::<FakeProtocol>::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("first dispatch");
        builder.annotate_revert_span(0, 1);
        assert_eq!(builder.pending_rollbacks, vec![0]);
        assert!(builder.pending_snapshots.contains_key(&0));

        // The next dispatch drains pending rollbacks at its top.
        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("second dispatch");

        assert!(
            builder.pending_rollbacks.is_empty(),
            "queue must be drained at the next async dispatch boundary",
        );
        assert!(
            !builder.pending_snapshots.contains_key(&0),
            "rolled-back snapshot must be removed from stash",
        );
        // The new call's snapshot at idx 1 still stashed (mid-flight).
        assert!(builder.pending_snapshots.contains_key(&1));
    }
}
