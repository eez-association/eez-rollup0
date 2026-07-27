//! Composition builder — drives a single cross-chain composition from
//! proxy-call detection through final `Composition` output.
//!
//! The builder merges three concerns that all operate on the same
//! per-composition state (the rollup map + the list of recorded calls):
//!
//! - **Target routing**: on each detected proxy call during source
//!   simulation, the source inspector calls
//!   [`CompositionBuilder::dispatch_call`], which looks up the
//!   registered rollup session by
//!   `rollup_id` and forwards the call — returning the outcome to the
//!   source inspector so execution can continue.
//! - **Recording**: each dispatched call is stored internally as a
//!   [`ExecutedAction`] (outcome non-optional — it's always present by
//!   the time the call is recorded).
//! - **Finalization**: [`CompositionBuilder::finalize`] consumes the
//!   builder, runs per-rollup CCM verification (skipping the entry
//!   rollup), builds source + target entries via the [`crate::entries`]
//!   builders, and produces a [`crate::types::Composition`].
//!
//! # Design
//!
//! - **Sealed at construction**. All [`Rollup`] plans are passed in
//!   at `new`; no `register_rollup` on the builder itself. The
//!   composer layer enforces uniqueness before calling `new`.
//! - **Owned [`Rollup`] per rollup**. Each rollup bundles the
//!   client, an optional session (`None` until the first dispatch
//!   opens it — the entry rollup's session stays `None` whenever no
//!   inspector dispatches back to the entry chain), the config (for
//!   CCM verify), and the initial state root.
//! - **Entry-aware**. `finalize` skips the entry rollup in both the
//!   CCM-verify loop (entry has no system-tx CCM path — L1 verifies
//!   via `EEZ.postAndVerifyBatch`'s
//!   proof bundle) and the target-composition loop (entry rollup's
//!   output lives in `source`, not `targets`).
//!
//! # Lifecycle
//!
//! ```text
//!      ┌──────────────────────────────────────────────────┐
//!      │ CompositionBuilder::new(entry_id, rollups)       │
//!      │   rollups   = HashMap<RollupId, Rollup>          │
//!      │   recorded  = Vec<ExecutedAction>  (empty)       │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼ source sim runs, detects proxy call
//!      ┌──────────────────────────────────────────────────┐
//!      │ builder.dispatch_call(...)                       │  × N
//!      │   → lazy-open rollups[target].session            │
//!      │   → open_call → session.execute(req, &mut self)  │
//!      │     → close_call resolves the slot's outcome     │
//!      │   → return ExecutionResponse to inspector        │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!      ┌──────────────────────────────────────────────────┐
//!      │ finalize(raw_tx)               (consumes self)   │
//!      │   1. validate: recorded + rollups non-empty      │
//!      │   2. CCM verify per non-entry rollup             │
//!      │      → patch terminal recorded.post_state_root   │
//!      │   3. entries::build_batch(recorded, attribution, │
//!      │      dialect, source_id, raw_tx) — once per      │
//!      │      source + per non-entry target               │
//!      │   4. encode_table_payload + encode_follower_     │
//!      │      trigger per target                          │
//!      │   5. package into Composition                    │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!                  Composition
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Bytes, U256};

use crate::batch::EvmBatch;
use crate::entries;
use crate::error::{CompositionResult, ExecutorError, ExecutorResult, ProtocolError};
use crate::executor::{
    ChainClient, ExecutionRequest, ExecutionResponse, TargetExecutionSession, TargetTransaction,
};
use crate::rollup_id::RollupId;
use crate::types::{Composition, ExecutedAction, SourceComposition, TargetComposition};

// Avoid a protocol → composer layering cycle: TargetConfig lives in
// `composer.rs`, but this module reads `config.verification_context`
// + `config.ccm_gas_limit` during finalize.
use crate::composer::TargetConfig;

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
/// - `config: TargetConfig` — `finalize` reads
///   `config.verification_context()` and `config.proxy_lookup` directly.
pub struct Rollup {
    /// Client for this rollup — shared long-lived trait object.
    pub client: Arc<dyn ChainClient + Send + Sync>,
    /// Lazily-opened session for this rollup. `None` until the first
    /// [`CompositionBuilder::dispatch_call`] hits this rollup.
    pub session: Option<Box<dyn TargetExecutionSession + Send>>,
    /// Configuration for this rollup (CCM addresses, gas limit, proxy
    /// lookup).
    pub config: TargetConfig,
    /// Root the entry chain currently holds for this rollup. Used as
    /// the `currentState` of the first source entry that touches this
    /// rollup.
    pub initial_state_root: [u8; 32],
}

impl std::fmt::Debug for Rollup {
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
/// One builder per source
/// transaction. Sealed at construction via [`CompositionBuilder::new`]
/// with the full set of [`Rollup`] plans (including the entry
/// rollup); dispatches each proxy call via [`CompositionBuilder::dispatch_call`]
/// during source simulation; consumed by [`CompositionBuilder::finalize`]
/// to produce the final [`crate::types::Composition`].
///
/// # Dispatch lifecycle
///
/// Each call runs through a two-phase open/close lifecycle:
///
/// 1. [`open_call`](Self::open_call) — push a `Pending` `ExecutedAction`
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
pub struct CompositionBuilder {
    pub(crate) entry_rollup_id: RollupId,
    pub(crate) rollups: HashMap<RollupId, Rollup>,
    pub(crate) recorded: Vec<ExecutedAction>,
    /// Pre-computed per-tx state roots, keyed by rollup id, injected
    /// via [`Self::set_extra_per_tx_roots`]. Merged into
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
    /// Rollups whose session is currently CHECKED OUT by an in-flight
    /// `dispatch_call` frame (taken at execute, put back after). A nested
    /// dispatch re-entering one of these would lazy-open a DUPLICATE
    /// session whose writes the outer put-back silently drops — refuse it
    /// loudly instead (review 2026-06-11; reachable only at depth>1,
    /// which is unbuilt).
    pub(crate) checked_out: std::collections::HashSet<RollupId>,
    /// Recorded-call indices whose snapshots need rollback. Drained at
    /// the start of every async `dispatch_call` (and at finalize) so
    /// the rollback runs at the next `.await` point — keeps
    /// `close_call` / `annotate_revert_span` synchronous (the
    /// inspector calls them from synchronous EVM hooks).
    pub(crate) pending_rollbacks: Vec<usize>,
}

impl std::fmt::Debug for CompositionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rollup_ids: Vec<RollupId> = self.rollups.keys().copied().collect();
        f.debug_struct("CompositionBuilder")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("rollup_ids", &rollup_ids)
            .field("recorded", &self.recorded.len())
            .finish()
    }
}

impl CompositionBuilder {
    /// Construct a new builder for one source transaction.
    ///
    /// `rollups` must include the entry rollup. The composer layer
    /// enforces that invariant before calling `new`.
    #[must_use]
    pub fn new(entry_rollup_id: RollupId, rollups: HashMap<RollupId, Rollup>) -> Self {
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
            checked_out: std::collections::HashSet::new(),
            pending_rollbacks: Vec::new(),
        }
    }

    /// Seed LIVE target sessions from a previous composition in the same
    /// slot (F1, D-3): the slot drain moves the sessions it took from the
    /// last builder into the next one, so tx_{k+1}'s probes run on the
    /// state tx_k's probes left — mirroring the source side's chained
    /// pass-1 session. A session for a rollup not in this builder's map is
    /// dropped (logged): it cannot be probed, so it cannot drift.
    #[must_use]
    pub fn with_sessions(
        mut self,
        sessions: HashMap<RollupId, Box<dyn TargetExecutionSession + Send>>,
    ) -> Self {
        for (id, session) in sessions {
            match self.rollups.get_mut(&id) {
                Some(rollup) => rollup.session = Some(session),
                None => tracing::warn!(
                    name: "composer.builder.session_dropped",
                    rollup_id = %id,
                    "seeded session for an unregistered rollup — dropped",
                ),
            }
        }
        self
    }

    /// Extract every live target session (F1, D-3). Called by the slot
    /// drain AFTER the pump finishes and BEFORE [`Self::finalize`] consumes
    /// the builder (`finalize` reads recorded outcomes and configs, never
    /// the sessions). The drain either chains them into the next tx's
    /// builder (composition succeeded) or rolls them back to its boundary
    /// snapshots (composition failed) — and drops them all at slot end:
    /// sessions never outlive their slot.
    pub fn take_sessions(&mut self) -> HashMap<RollupId, Box<dyn TargetExecutionSession + Send>> {
        self.rollups
            .iter_mut()
            .filter_map(|(id, rollup)| rollup.session.take().map(|s| (*id, s)))
            .collect()
    }

    /// The recorded cross-chain calls captured so far (preorder). Read
    /// AFTER `simulate_source_tx` but BEFORE `finalize` (which consumes
    /// `self`) when the caller needs a call's resolved `outcome` (e.g. the
    /// inbound delivery's `return_data`) to build a chain-specific payload
    /// the composition output doesn't carry verbatim.
    #[must_use]
    pub fn recorded(&self) -> &[ExecutedAction] {
        &self.recorded
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
            let rollup_id = call.target_rollup_id;
            if !handled.insert(rollup_id) {
                continue;
            }
            if let Some(rollup) = self.rollups.get_mut(&rollup_id)
                && let Some(session) = rollup.session.as_mut()
            {
                session.rollback(snap).await?;
            }
        }
        // Any other snapshots still keyed under bracketed indices are
        // dropped — the head idx's rollback already restores their
        // shared session.
        Ok(())
    }

    /// Clone all recorded calls whose `target_rollup_id` matches
    /// `rollup_id` — the per-target group `finalize` processes.
    ///
    /// The recorded vec is preorder by construction (each call's index
    /// is fixed at `Dispatcher::open_call` time), so a linear filter
    /// preserves dispatch order without tree reconstruction. The
    /// unified emitter walks this pre-filtered slice directly.
    fn group_calls_for(&self, rollup_id: RollupId) -> Vec<ExecutedAction> {
        self.recorded
            .iter()
            .filter(|c| c.target_rollup_id == rollup_id)
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
    ///    `EEZ.postAndVerifyBatch`'s
    ///    proof bundle, not system txs).
    /// 3. Call `entries::build_batch` for the source rollup with per-rollup
    ///    initial state roots; encode via `entries::encode_table_payload`.
    /// 4. Per **non-entry** rollup: `build_batch` + `encode_table_payload`
    ///    + `encode_follower_trigger`. One [`TargetComposition`] per
    ///    rollup.
    /// 5. Package as [`Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::EmptyCalls`] on empty inputs,
    /// [`ProtocolError::UnknownTarget`] for a recorded rollup not
    /// in the plan set, [`ProtocolError::InvalidCheckpoint`] if
    /// per-rollup state-delta chaining fails in `build_batch`.
    /// Surfaces any [`ExecutorError`] from CCM verification.
    #[tracing::instrument(level = "debug", name = "finalize", skip_all, err)]
    pub async fn finalize(mut self, raw_tx: &[u8]) -> CompositionResult<Composition> {
        tracing::debug!(name: "composer.finalize.start", "composition finalize started");

        if self.recorded.is_empty() || self.rollups.is_empty() {
            return Err(ProtocolError::EmptyCalls.into());
        }

        for call in &self.recorded {
            if !self.rollups.contains_key(&call.target_rollup_id) {
                return Err(ProtocolError::UnknownTarget {
                    got: call.target_rollup_id,
                }
                .into());
            }
        }

        // Sorted plan order for deterministic output (upstream's invariant 2).
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
        // nested-composition upstream-invariant-6 chaining.
        let mut per_tx_roots_by_rollup: HashMap<RollupId, Vec<[u8; 32]>> = HashMap::new();

        // initial_roots is hoisted out of Phase 3 so the CCM-verify
        // loop can pass an attribution to `build_batch`.
        // Per-tx roots are still empty at this point — they're
        // populated later by the loop's `simulate_transactions` and
        // by `extra_per_tx_roots` for the entry rollup. The L1-as-
        // follower emitter uses `initial_roots[source_rollup_id]`
        // for its first stateDelta's currentState; with empty
        // per_tx_roots it emits a degenerate-tail chain (newState ==
        // currentState), which `EEZ.executeL2TX`'s simulation
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
        // in `composer-lib::post_batch_submitter` (`submit_with_proof`).

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
        let mut target_batches: HashMap<RollupId, EvmBatch> = HashMap::new();
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

            // zk-poster (L1) dialect — build the EXECUTING L1 `postAndVerifyBatch`
            // mirror, NOT the regular `build_batch`. The L2→L1 calls are TopLevel
            // for the ENTRY (L2) batch (caller==entry) but NestedSuccess here
            // (caller!=L1), so `build_batch(source=L1)` would emit an EMPTY
            // L1-as-caller batch. Per the `counterL2` spec, each L2→L1 call is an
            // IMMEDIATE executing entry on L1 (`proxyEntryHash`=0 + `L2ToL1Calls`).
            //
            // We do NOT `simulate_transactions`: `postAndVerifyBatch` carries a
            // proof not signed until the prover's return path, so simulating it
            // here would revert on empty `proofs[]`. The L1 state transition
            // happens at post-batch SUBMISSION (Step 6). We attribute the L1's
            // REAL current state root as this rollup's post-state root (a
            // placeholder; the prover patches the real L2 `newState` later).
            if dialect.is_zk_poster() {
                let batch = entries::build_l1_postbatch(&group_calls, self.entry_rollup_id);
                if batch.is_empty() {
                    continue;
                }
                let root = rollup.client.current_state_root().await?;
                if let Some(last) = self
                    .recorded
                    .iter_mut()
                    .rev()
                    .find(|r| r.target_rollup_id == *rollup_id)
                    && let crate::types::ExecutionOutcome::Resolved {
                        post_state_root, ..
                    } = &mut last.outcome
                {
                    *post_state_root = root;
                }
                tracing::debug!(
                    name: "composer.zk_poster_l1_postbatch",
                    %rollup_id,
                    l1_root = ?root,
                    entries = group_calls.len(),
                    "zk-poster target: built immediate L1 postBatch (skipping CCM-verify sim; \
                     settlement applied at submission)",
                );
                per_tx_roots_by_rollup.insert(*rollup_id, vec![root]);
                target_batches.insert(*rollup_id, batch);
                continue;
            }

            let attribution_so_far = crate::composer::SourceAttribution {
                initial_roots: &initial_roots,
                per_tx_roots_by_rollup: &per_tx_roots_by_rollup,
            };
            let batch = entries::build_batch(
                &group_calls,
                &attribution_so_far,
                dialect,
                *rollup_id,
                raw_tx,
            )?;

            // Terminal-revert short-circuit: an empty batch means all
            // calls reverted and there's nothing to verify — UNLESS this
            // target has an INCOMING cross-chain call. `build_batch(source =
            // this rollup)` keys "top-level" on a call's SOURCE, so an incoming
            // call (TARGET is this rollup, SOURCE another rollup) is never
            // top-level → empty batch here, even though the L2 must DELIVER it
            // (`executeIncomingCrossChainCall`). Detect that and build the
            // follower-only inbound DA-sidecar entry directly — the inbound
            // mirror of the zk-poster outbound short-circuit above (the lean
            // on-chain entry is produced separately by the source/entry batch).
            // Otherwise the batch is genuinely empty (all reverted) → skip.
            if batch.is_empty() {
                let has_incoming = group_calls.iter().any(|c| c.source_rollup_id != *rollup_id);
                if !dialect.is_zk_poster() && has_incoming {
                    let inbound_batch = entries::build_l1_inbound_sidecar(&group_calls, *rollup_id);
                    if !inbound_batch.is_empty() {
                        // Attribute the inbound delivery's post-state root —
                        // already executed during dispatch (`close_call`
                        // stamped the recorded call's outcome). Mirrors the
                        // zk-poster / CCM-verify per-tx-root attribution.
                        let root = self
                            .recorded
                            .iter()
                            .rev()
                            .find(|r| r.target_rollup_id == *rollup_id)
                            .and_then(|r| r.outcome.post_state_root().copied())
                            .ok_or_else(|| ProtocolError::InvalidCheckpoint {
                                reason: format!(
                                    "inbound target {rollup_id} has no resolved \
                                     post_state_root (close_call did not run?)"
                                ),
                            })?;
                        tracing::debug!(
                            name: "composer.inbound_sidecar",
                            %rollup_id,
                            delivery_root = ?root,
                            entries = group_calls.len(),
                            "inbound target: built follower-only DA-sidecar entry \
                             (build_batch(source=this) cannot express an incoming call)",
                        );
                        per_tx_roots_by_rollup.insert(*rollup_id, vec![root]);
                        target_batches.insert(*rollup_id, inbound_batch);
                    }
                }
                continue;
            }

            // Session-root settlement short-circuit (inbound L1→L2): this
            // target's client settles via its own `execute` — the real
            // post-state root was already reported over the wire
            // (`EndSimulate`) and stamped onto the recorded action by
            // `close_call`. Its client cannot serve a local
            // `simulate_transactions` CCM-verify (a remote bidi-stream
            // client — none in-tree today; the flag is never set), so
            // skip that pass and keep the recorded root, mirroring the
            // zk-poster branch above (which skips sim and attributes
            // `current_state_root` instead). The L2→L1 outbound path never
            // reaches here — its L1 target is zk-poster and short-circuits
            // earlier — so this branch is inbound-only.
            if rollup.config.settles_via_session_root {
                let root = self
                    .recorded
                    .iter()
                    .rev()
                    .find(|r| r.target_rollup_id == *rollup_id)
                    .and_then(|r| r.outcome.post_state_root().copied())
                    .ok_or_else(|| ProtocolError::InvalidCheckpoint {
                        reason: format!(
                            "settles_via_session_root target {rollup_id} has no resolved \
                                 post_state_root (close_call did not run?)"
                        ),
                    })?;
                tracing::debug!(
                    name: "composer.session_root_settle",
                    %rollup_id,
                    session_root = ?root,
                    entries = group_calls.len(),
                    "session-root target: skipping CCM-verify sim; using recorded EndSimulate root",
                );
                per_tx_roots_by_rollup.insert(*rollup_id, vec![root]);
                target_batches.insert(*rollup_id, batch);
                continue;
            }

            // The "outer root" call drives the follower's first proxy
            // invocation. In preorder the first matching call is the
            // outer-most root by construction.
            let outer_root = &group_calls[0];

            let exec_calldata =
                dialect.encode_follower_trigger(outer_root, self.entry_rollup_id, raw_tx);
            let load_calldata = entries::encode_table_payload(&batch, dialect);

            let verification = rollup.config.verification_context();
            let make_ccm_tx = |calldata: Bytes, value: U256| TargetTransaction {
                caller: verification.system_address,
                destination: verification.entrypoint_address,
                calldata,
                value,
                gas_limit: verification.gas_limit,
            };

            let tx_load = make_ccm_tx(Bytes::from(load_calldata), U256::ZERO);
            let tx_exec = make_ccm_tx(Bytes::from(exec_calldata), outer_root.value);
            let txs: Vec<TargetTransaction> = vec![tx_load, tx_exec];

            let sim = rollup.client.simulate_transactions(&txs).await?;

            if let Some(last) = self
                .recorded
                .iter_mut()
                .rev()
                .find(|r| r.target_rollup_id == *rollup_id)
                && let crate::types::ExecutionOutcome::Resolved {
                    post_state_root, ..
                } = &mut last.outcome
            {
                *post_state_root = sim.final_state_root;
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
        };
        let entry_dialect = self
            .rollups
            .get(&self.entry_rollup_id)
            .expect("entry rollup registered at builder construction")
            .config
            .dialect;
        let entry_batch = entries::build_batch(
            &self.recorded,
            &attribution,
            &entry_dialect,
            self.entry_rollup_id,
            raw_tx,
        )?;
        let entry_payload = entries::encode_table_payload(&entry_batch, &entry_dialect);

        // Phase 4 — target compositions (re-encode from the batches
        // captured in Phase 2). Skip entry rollup + empty groups.
        let mut target_compositions: Vec<TargetComposition> = Vec::new();
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
            let load_table_payload = entries::encode_table_payload(&batch, dialect);
            let execute_payload =
                dialect.encode_follower_trigger(outer_root, self.entry_rollup_id, raw_tx);
            target_compositions.push(TargetComposition {
                rollup_id: *rollup_id,
                batch,
                load_table_payload,
                execute_payload,
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

    /// Convenience entry point: open → execute on the target session
    /// → close. Inspectors call this from their EVM-frame `call`
    /// handler.
    ///
    /// Enforces a same-chain re-entry guard:
    /// `target_rollup_id == source_rollup_id && target_rollup_id != entry_rollup_id`
    /// returns [`ExecutorError::InvalidReentry`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::InvalidReentry`] for same-chain
    /// non-entry self-dispatch. Returns [`ExecutorError::Unavailable`]
    /// if no rollup is registered under `target_rollup_id`. Propagates any
    /// executor error from the target session's `execute`.
    #[tracing::instrument(
        level = "debug",
        name = "dispatch_call",
        skip_all,
        fields(target = %target_rollup_id, source = %source_rollup_id),
        err,
    )]
    pub async fn dispatch_call(
        &mut self,
        target_rollup_id: RollupId,
        source_rollup_id: RollupId,
        req: ExecutionRequest,
    ) -> ExecutorResult<ExecutionResponse> {
        // Drain any pending rollbacks queued by the previous frame's
        // `annotate_revert_span` / `close_call`. This is the next
        // async point — synchronous lifecycle methods cannot call
        // `session.rollback().await` directly.
        self.process_pending_rollbacks().await?;

        // Same-chain re-entry guard. Entry-to-entry dispatch (e.g. a
        // contract on the entry chain calling another entry-chain
        // contract during normal source simulation) is legitimate
        // and falls through.
        if target_rollup_id == source_rollup_id && target_rollup_id != self.entry_rollup_id {
            return Err(ExecutorError::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            });
        }

        // Phase 1 — open: lazy-open the session, snapshot it, push
        // `Pending` placeholder, capture slot index.
        let idx = self
            .open_call(target_rollup_id, source_rollup_id, &req)
            .await?;

        // Phase 2 — run execute on the lazy-opened session.
        let mut session = self
            .rollups
            .get_mut(&target_rollup_id)
            .expect("rollup present (just checked)")
            .session
            .take()
            .expect("session opened by open_call");

        // `session.execute` awaits first; nested dispatches from a
        // target-session inspector call back into `self.dispatch_call`
        // and push their own `ExecutedAction`s at indices `idx + 1, ..`.
        // The vec is preorder by construction.
        self.checked_out.insert(target_rollup_id);
        let response_res = session.execute(req, self).await;

        // Put the session back even on error; revert handling is
        // post-close via `annotate_revert_span`.
        self.checked_out.remove(&target_rollup_id);
        self.rollups
            .get_mut(&target_rollup_id)
            .expect("rollup not removable")
            .session = Some(session);

        let response = response_res?;

        // Phase 3 — close: resolve the slot with the real outcome.
        self.close_call(idx, response.outcome.clone(), None);

        tracing::debug!(
            name: "composer.dispatch_call",
            %target_rollup_id,
            %source_rollup_id,
            success = response.outcome.is_success(),
            gas = response.outcome.gas_used().unwrap_or(0),
            "dispatched cross-chain call"
        );

        Ok(response)
    }

    /// Push a `Pending` placeholder for a new call and return its
    /// slot index. Called BEFORE the target session's `execute` so
    /// the index is stable across nested dispatches.
    ///
    /// # Errors
    ///
    /// Same as [`dispatch_call`](Self::dispatch_call) — re-entry guard
    /// and rollup-id validation fire here.
    pub async fn open_call(
        &mut self,
        target_rollup_id: RollupId,
        source_rollup_id: RollupId,
        req: &ExecutionRequest,
    ) -> ExecutorResult<usize> {
        if target_rollup_id == source_rollup_id && target_rollup_id != self.entry_rollup_id {
            return Err(ExecutorError::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            });
        }
        // Cyclic nesting (entry→A→B→A): A's session is checked out by the
        // outer frame, so a lazy-open here would mint a DUPLICATE whose
        // writes the outer put-back drops. Refuse loudly (depth>1 is
        // unbuilt; this turns a silent state loss into an error).
        if self.checked_out.contains(&target_rollup_id) {
            return Err(ExecutorError::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            });
        }
        if !self.rollups.contains_key(&target_rollup_id) {
            return Err(ExecutorError::Unavailable(format!(
                "no rollup registered for {target_rollup_id}"
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
                .get_mut(&target_rollup_id)
                .expect("rollup present (just checked)");
            if rollup.session.is_none() {
                let new_session = rollup.client.begin_execution_session().await?;
                rollup.session = Some(new_session);
            }
            let session = rollup.session.as_mut().expect("session opened above");
            session.checkpoint().await?
        };

        let idx = self.recorded.len();
        self.recorded.push(ExecutedAction {
            target_address: req.target_address,
            target_rollup_id,
            source_rollup_id,
            source_address: req.source_address,
            data: req.data.clone(),
            value: req.value,
            outcome: crate::types::ExecutionOutcome::Pending,
            revert_span: None,
            static_meta: None,
        });
        self.pending_snapshots.insert(idx, snap);
        Ok(idx)
    }

    /// Resolve the call opened by [`open_call`](Self::open_call) at `idx` with its outcome.
    /// `revert_span` carries the on-chain `L2ToL1CallSol::revertSpan`
    /// for top-level calls when known at close time; most callers
    /// pass `None` and let
    /// [`annotate_revert_span`](Self::annotate_revert_span) fill it
    /// in post-frame.
    pub fn close_call(
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

    /// Number of [`ExecutedAction`]s captured so far in this composition.
    ///
    /// Used by the EVM inspector to bracket a CALL frame: snapshot
    /// the count at frame open, compare at `call_end`, and forward
    /// the resulting `(start, end - start)` to
    /// [`annotate_revert_span`](Self::annotate_revert_span) when the
    /// frame returned with `InstructionResult::Revert`.
    #[must_use]
    pub fn recorded_count(&self) -> usize {
        self.recorded.len()
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
    pub fn annotate_revert_span(&mut self, idx: usize, span: u32) {
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

    /// Inject pre-computed per-tx state roots for `rollup_id` into the
    /// builder's eventual `finalize` step.
    ///
    /// Used by the entry-overlay path: the composer's CCM-verify loop
    /// in `finalize` skips the entry rollup (no system-tx CCM contract
    /// on L1), so nested calls attributed to the entry rollup have no
    /// `per_tx_roots` source. The entry overlay session captures one
    /// post-state root per overlay `execute` and, at end of
    /// `simulate_source_tx`, the source-sim path drains that buffer
    /// and forwards it here.
    pub fn set_extra_per_tx_roots(&mut self, rollup_id: RollupId, roots: Vec<[u8; 32]>) {
        if !roots.is_empty() {
            self.extra_per_tx_roots.insert(rollup_id, roots);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::cross_chain_call_hash;
    use crate::checkpoint::ExecutionCheckpoint;
    use crate::composer::{DEFAULT_CCM_GAS_LIMIT, ProxyLookupConfig};
    use crate::dialect::ChainDialect;
    use crate::executor::TargetBatchSimulation;
    use crate::overlay::EvmOverlay;
    use crate::types::ExecutionOutcome;
    use alloy_primitives::Address;

    // ── Mock ChainClient (returns a canned CCM final root + session) ─

    struct MockClient {
        final_root: [u8; 32],
        session_outcome: ExecutionOutcome,
    }

    #[async_trait::async_trait]
    impl ChainClient for MockClient {
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(MockSession {
                outcome: self.session_outcome.clone(),
            }))
        }
        async fn simulate_transactions(
            &self,
            txs: &[TargetTransaction],
        ) -> ExecutorResult<TargetBatchSimulation> {
            if txs.is_empty() {
                return Err(crate::error::ExecutorError::EmptyBatch);
            }
            Ok(TargetBatchSimulation {
                final_state_root: self.final_root,
                per_tx_roots: vec![self.final_root; txs.len()],
            })
        }
    }

    // ── Reentrant fakes (cycle guard, review 2026-06-11) ─────────────

    /// Session whose execute() immediately re-dispatches to its OWN
    /// rollup through the builder — the entry→A→…→A cycle shape. The
    /// checked-out guard must refuse the inner open_call (it would mint
    /// a duplicate session whose writes the outer put-back drops).
    struct ReentrantSession {
        own_rollup: RollupId,
    }

    #[async_trait::async_trait]
    impl TargetExecutionSession for ReentrantSession {
        async fn execute(
            &mut self,
            req: ExecutionRequest,
            dispatcher: &mut CompositionBuilder,
        ) -> ExecutorResult<ExecutionResponse> {
            // Nested dispatch back into the SAME rollup (caller = some
            // other id so the plain target==source guard does not fire).
            dispatcher
                .open_call(self.own_rollup, RollupId(7), &req)
                .await?;
            unreachable!("the checked-out guard must refuse the cyclic open_call");
        }
        async fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
            Ok(Box::new(()) as crate::executor::SessionSnapshot)
        }
        async fn rollback(
            &mut self,
            _snapshot: crate::executor::SessionSnapshot,
        ) -> ExecutorResult<()> {
            Ok(())
        }
        async fn take_checkpoint(&mut self) -> Option<ExecutionCheckpoint> {
            None
        }
    }

    struct ReentrantClient {
        rollup: RollupId,
    }

    #[async_trait::async_trait]
    impl ChainClient for ReentrantClient {
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(ReentrantSession {
                own_rollup: self.rollup,
            }))
        }
        async fn simulate_transactions(
            &self,
            _txs: &[TargetTransaction],
        ) -> ExecutorResult<TargetBatchSimulation> {
            unimplemented!("cycle test never simulates")
        }
    }

    // ── Mock TargetExecutionSession ──────────────────────────────────

    struct MockSession {
        outcome: ExecutionOutcome,
    }

    #[async_trait::async_trait]
    impl TargetExecutionSession for MockSession {
        async fn execute(
            &mut self,
            _req: ExecutionRequest,
            _dispatcher: &mut CompositionBuilder,
        ) -> ExecutorResult<ExecutionResponse> {
            Ok(ExecutionResponse {
                outcome: self.outcome.clone(),
                checkpoint: ExecutionCheckpoint {
                    version: 1,
                    chain_id: 1,
                    base_block_number: 0,
                    base_block_hash: [0u8; 32],
                    base_state_root: [0u8; 32],
                    current_root: [0u8; 32],
                    overlay: EvmOverlay::default(),
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

        async fn take_checkpoint(&mut self) -> Option<ExecutionCheckpoint> {
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

    fn make_request(rollup: u64) -> ExecutionRequest {
        ExecutionRequest {
            target_address: Address::repeat_byte(rollup as u8),
            data: Bytes::from(vec![0x01, 0x02]),
            value: U256::ZERO,
            source_address: Address::ZERO,
            source_rollup_id: RollupId(0),
        }
    }

    fn target_config() -> TargetConfig {
        TargetConfig {
            ccm_address: Address::ZERO,
            system_address: Address::ZERO,
            ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
            proxy_lookup: ProxyLookupConfig {
                contract_address: Address::ZERO,
                authorized_proxies_slot: 0,
            },
            dialect: ChainDialect::EvmL2Style,
            settles_via_session_root: false,
        }
    }

    fn entry_rollup(outcome_root: [u8; 32]) -> Rollup {
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

    fn rollup_with_session(outcome_root: [u8; 32]) -> Rollup {
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

    // ── Tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_routes_to_registered_session_and_records() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let response = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");
        assert_eq!(response.outcome.post_state_root(), Some(&[0x11u8; 32]));
        assert_eq!(builder.recorded.len(), 1);
        assert_eq!(builder.recorded[0].target_rollup_id, RollupId(1));
        assert_eq!(builder.recorded[0].source_rollup_id, RollupId(0));
    }

    #[tokio::test]
    async fn sessions_seed_take_round_trip_and_drop_unregistered() {
        // F1 (D-3): take_sessions extracts the lazily-opened live session;
        // with_sessions seeds it into the next builder; a session for an
        // unregistered rollup is dropped (it cannot be probed there).
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let _ = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch lazy-opens rollup 1's session");
        let sessions = builder.take_sessions();
        assert_eq!(sessions.len(), 1, "exactly the lazily-opened session");
        assert!(sessions.contains_key(&RollupId(1)));
        // Taking is draining: a second take finds nothing.
        assert!(builder.take_sessions().is_empty());

        // Seed into a fresh builder: the slot is occupied (no lazy re-open).
        let mut rollups2 = HashMap::new();
        rollups2.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups2.insert(RollupId(1), rollup_with_session([0x22; 32]));
        let mut builder2 = CompositionBuilder::new(RollupId(0), rollups2).with_sessions(sessions);
        assert!(
            builder2
                .rollups
                .get(&RollupId(1))
                .expect("registered")
                .session
                .is_some(),
            "seeded session occupies the slot",
        );

        // A session keyed to a rollup the next builder does NOT register
        // is dropped, never mis-routed.
        let carried = builder2.take_sessions();
        let mut rollups3 = HashMap::new();
        rollups3.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder3 = CompositionBuilder::new(RollupId(0), rollups3).with_sessions(carried);
        assert!(!builder3.rollups.contains_key(&RollupId(1)));
    }

    #[tokio::test]
    async fn cyclic_nested_dispatch_is_refused() {
        // entry→A→A-again: while A's session is checked out, a nested
        // dispatch back into A must error (InvalidReentry), not mint a
        // duplicate session (whose writes the outer put-back would drop).
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(
            RollupId(1),
            Rollup {
                client: Arc::new(ReentrantClient {
                    rollup: RollupId(1),
                }),
                session: None,
                config: target_config(),
                initial_state_root: [0u8; 32],
            },
        );
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let err = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect_err("cycle must be refused");
        assert!(
            matches!(err, ExecutorError::InvalidReentry { .. }),
            "got: {err}"
        );
        // The outer session was put back despite the inner error.
        assert_eq!(builder.take_sessions().len(), 1, "outer session survives");
    }

    #[tokio::test]
    async fn dispatch_unknown_rollup_returns_unavailable() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(99), RollupId(0), make_request(99))
            .await
            .expect_err("should fail");
        assert!(matches!(err, ExecutorError::Unavailable(_)));
    }

    #[tokio::test]
    async fn finalize_empty_errors() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder = CompositionBuilder::new(RollupId(0), rollups);
        let err = builder.finalize(&[]).await.expect_err("should fail");
        assert!(matches!(
            err,
            crate::error::CompositionError::Protocol(p)
                if matches!(p, crate::error::ProtocolError::EmptyCalls)
        ));
    }

    #[tokio::test]
    async fn finalize_inbound_target_produces_sidecar_composition() {
        // An entry→rollup-1 call is INCOMING from rollup 1's perspective:
        // `build_batch(source = 1)` yields an empty batch (no top-level
        // call sourced from 1), so finalize takes the inbound DA-sidecar
        // branch and the target composition carries the sidecar entry.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");

        let composition = builder.finalize(&[]).await.expect("finalize");
        assert_eq!(composition.source.rollup_id, RollupId(0));
        // Entry rollup is skipped in the targets loop, so only rollup 1
        // appears in targets.
        assert_eq!(composition.targets.len(), 1);
        assert_eq!(composition.targets[0].rollup_id, RollupId(1));

        // The sidecar entry mirrors the recorded call: callCount 1, the
        // call in l2ToL1Calls[0], proxyEntryHash bound to the same
        // 6-field preimage the on-chain entry uses.
        let entries = &composition.targets[0].batch.entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].callCount, U256::from(1u8));
        assert_eq!(entries[0].l2ToL1Calls.len(), 1);
        assert_eq!(
            entries[0].l2ToL1Calls[0].targetAddress,
            Address::repeat_byte(1)
        );
        assert_eq!(
            entries[0].proxyEntryHash,
            cross_chain_call_hash(
                RollupId(1),
                Address::repeat_byte(1),
                U256::ZERO,
                &Bytes::from(vec![0x01, 0x02]),
                Address::ZERO,
                RollupId(0),
            ),
        );

        // The entry batch carries the top-level call as one deferred entry.
        assert_eq!(composition.source.batch.entries.len(), 1);
    }

    #[tokio::test]
    async fn finalize_rejects_recorded_calls_for_unregistered_rollups() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        builder.recorded.push(ExecutedAction {
            target_address: Address::ZERO,
            target_rollup_id: RollupId(99),
            source_rollup_id: RollupId(0),
            source_address: Address::ZERO,
            data: Bytes::new(),
            value: U256::ZERO,
            outcome: sample_outcome([0u8; 32]),
            revert_span: None,
            static_meta: None,
        });

        let err = builder.finalize(&[]).await.expect_err("should fail");
        assert!(matches!(
            err,
            crate::error::CompositionError::Protocol(p)
                if matches!(
                    p,
                    crate::error::ProtocolError::UnknownTarget { got: RollupId(99) }
                )
        ));
    }

    #[tokio::test]
    async fn finalize_targets_come_out_sorted_by_rollup_id() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(3), rollup_with_session([0x33; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        for id in [3u64, 1, 2] {
            builder
                .dispatch_call(RollupId(id), RollupId(0), make_request(id))
                .await
                .expect("dispatch");
        }

        let composition = builder.finalize(&[]).await.expect("finalize");
        let ids: Vec<u64> = composition.targets.iter().map(|t| t.rollup_id.0).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "targets must be sorted by rollup_id, not insertion order (upstream's invariant 2)"
        );
    }

    #[tokio::test]
    async fn source_rollup_id_is_stored_from_dispatch_arg() {
        // Regression guard: source_rollup_id must come from the
        // `source_rollup_id` arg on dispatch_call, not from req.source_rollup.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        // Pass RollupId(7) as source_rollup_id — distinct from req.source_rollup
        // (which is RollupId(0) from make_request). The stored value
        // must match source_rollup_id, not req.source_rollup.
        builder
            .dispatch_call(RollupId(1), RollupId(7), make_request(1))
            .await
            .expect("dispatch");
        assert_eq!(builder.recorded[0].source_rollup_id, RollupId(7));
    }

    // ── Terminal-revert short-circuit ──────────────────────────────

    /// Pairs with a `MockClient` that panics if `simulate_transactions`
    /// is called — lets the test assert the CCM-verify path was
    /// skipped.
    struct NoCcmClient;

    #[async_trait::async_trait]
    impl ChainClient for NoCcmClient {
        async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
            Ok([0u8; 32])
        }
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
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
            _txs: &[TargetTransaction],
        ) -> ExecutorResult<TargetBatchSimulation> {
            panic!(
                "simulate_transactions must NOT be called on terminal-revert path — \
                 finalize's short-circuit should have skipped CCM verify"
            );
        }
    }

    fn rollup_with_reverted_session() -> Rollup {
        Rollup {
            client: Arc::new(NoCcmClient),
            session: None,
            config: target_config(),
            initial_state_root: [0u8; 32],
        }
    }

    #[tokio::test]
    async fn empty_target_entries_skips_ccm_verify_and_omits_target_composition() {
        // Terminal revert — the emitter returns an empty target-entry
        // set, and finalize must honor both sides of that handshake:
        //   (a) skip CCM verify (NoCcmClient::simulate_transactions
        //       panics if called — regression guard);
        //   (b) omit the `TargetComposition` for the reverted rollup
        //       from the returned `Composition`.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_reverted_session());
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch");
        assert!(
            !builder.recorded[0].outcome.is_success(),
            "test setup: recorded call must be reverted"
        );

        let composition = builder.finalize(&[]).await.expect("finalize");

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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(1), RollupId(1), make_request(1))
            .await
            .expect_err("L2 → L2 self-dispatch must be rejected");
        assert!(
            matches!(
                err,
                ExecutorError::InvalidReentry { caller, target }
                    if caller == RollupId(1) && target == RollupId(1)
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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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
        assert!(crate::assertions::is_preorder(
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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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

    // Three synthetic revertSpan vectors covering edge cases.

    /// Outer call dispatches 2 inner calls and then the outer's
    /// frame reverts — span = 3 covers the outer + both children.
    /// Bracketed children carry no `revert_span`.
    #[tokio::test]
    async fn revert_span_gt_one_covers_outer_plus_two_children() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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
        assert!(crate::assertions::is_preorder(
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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

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
