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
//!   builder, builds source + target entries via the [`crate::entries`]
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
//!   target-batch loop (entry has no system-tx CCM path — L1 verifies
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
//!      │   → return ExecutionOutcome to inspector         │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!      ┌──────────────────────────────────────────────────┐
//!      │ finalize(raw_tx)               (consumes self)   │
//!      │   1. validate: recorded + rollups non-empty      │
//!      │   2. per non-entry rollup: zk-poster settlement  │
//!      │      or inbound sidecar batch + root attribution │
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

use crate::batch::EvmBatch;
use crate::entries;
use crate::error::{
    CompositionResult, ExecutorError, ExecutorErrorKind, ExecutorResult, ProtocolErrorKind,
};
use crate::executor::{ChainClient, ExecutionRequest, TargetExecutionSession};
use crate::rollup_id::RollupId;
use crate::types::{
    Composition, ExecutedAction, ExecutionOutcome, SourceComposition, TargetComposition,
};

use crate::authorized_proxies::ProxyLookupConfig;
use crate::dialect::ChainDialect;

/// Per-rollup static configuration.
///
/// Holds the proxy lookup and ABI dialect for one rollup (entry or
/// follower). Registered with the [`CompositionBuilder`] alongside the
/// client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetConfig {
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig,
    /// ABI dialect: selects entry-encoding and batch shape (L1-style vs
    /// L2-style). Default = `EvmL2Style`.
    pub dialect: ChainDialect,
}

// ── Rollup ───────────────────────────────────────────────────────

/// Per-rollup state held inside a [`CompositionBuilder`] during one
/// composition.
///
/// Carries:
///
/// - `client: Arc<dyn ChainClient>` directly (used for lazy session
///   opening).
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
    /// Configuration for this rollup (proxy lookup + dialect).
    pub config: TargetConfig,
}

impl std::fmt::Debug for Rollup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rollup")
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
            pending_snapshots: HashMap::new(),
            checked_out: std::collections::HashSet::new(),
            pending_rollbacks: Vec::new(),
        }
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

    /// Build the Phase-2 batch for one non-entry rollup. `Ok(None)` means
    /// there is nothing to build (no calls, an empty settlement batch, or
    /// all calls reverted).
    ///
    /// Two shapes, by dialect:
    /// - zk-poster (L1): the executing `postAndVerifyBatch` mirror. The
    ///   L2→L1 calls are NestedSuccess here (caller != L1), so the regular
    ///   `build_batch` would emit an empty L1-as-caller batch;
    ///   `build_l1_postbatch` emits the immediate executing entries instead.
    /// - L2 follower: the inbound DA-sidecar delivery. `build_batch` keys
    ///   "top-level" on a call's source, so an incoming call (source is
    ///   another rollup) is never top-level and yields an empty batch — even
    ///   though this L2 must deliver it. Detect that and build the
    ///   follower-only sidecar directly.
    ///
    /// A non-empty follower batch is unreachable: `open_call`'s same-chain
    /// guard refuses target == source for non-entry rollups, so every call
    /// in the group is incoming. It fails loudly rather than emit an
    /// unverified target composition.
    ///
    /// # Errors
    ///
    /// [`ProtocolErrorKind::Unsupported`] for the unreachable non-empty
    /// follower batch, and any error from `build_batch`.
    #[allow(clippy::result_large_err)]
    fn build_target_batch(
        &self,
        rollup_id: RollupId,
        group_calls: &[ExecutedAction],
    ) -> CompositionResult<Option<EvmBatch>> {
        let dialect = self.rollups[&rollup_id].config.dialect;

        if dialect.is_zk_poster() {
            let batch = entries::build_l1_postbatch(group_calls, self.entry_rollup_id);
            Ok((!batch.is_empty()).then_some(batch))
        } else {
            let batch = entries::build_batch(group_calls, &dialect, rollup_id)?;
            if batch.is_empty() {
                let has_incoming = group_calls.iter().any(|c| c.source_rollup_id != rollup_id);
                if has_incoming {
                    let inbound = entries::build_l1_inbound_sidecar(group_calls, rollup_id);
                    Ok((!inbound.is_empty()).then_some(inbound))
                } else {
                    Ok(None)
                }
            } else {
                Err(ProtocolErrorKind::Unsupported(
                    "non-entry target batch with top-level calls (unreachable by construction)",
                )
                .into())
            }
        }
    }

    /// Consume the builder and produce the final [`Composition`].
    ///
    /// Steps, in order:
    ///
    /// 1. Validate: both `recorded` and `rollups` non-empty; every
    ///    recorded call targets a registered rollup.
    /// 2. Per **non-entry** rollup: build the zk-poster settlement batch
    ///    or the inbound DA-sidecar batch and encode its
    ///    [`TargetComposition`]. The entry rollup is the source, not a
    ///    target (L1 verifies via `EEZ.postAndVerifyBatch`'s proof bundle).
    /// 3. Build the source (entry-rollup) batch via `entries::build_batch`
    ///    and encode it.
    /// 4. Package as [`Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::EmptyCalls`] on empty inputs,
    /// [`ProtocolErrorKind::UnknownTarget`] for a recorded rollup not in
    /// the plan set, [`ProtocolErrorKind::Unsupported`] for an unreachable
    /// non-entry top-level batch, and any error from `build_batch`.
    #[tracing::instrument(level = "debug", name = "finalize", skip_all, err)]
    pub async fn finalize(self) -> CompositionResult<Composition> {
        tracing::debug!(name: "composer.finalize.start", "composition finalize started");

        if self.recorded.is_empty() || self.rollups.is_empty() {
            return Err(ProtocolErrorKind::EmptyCalls.into());
        }

        for call in &self.recorded {
            if !self.rollups.contains_key(&call.target_rollup_id) {
                return Err(ProtocolErrorKind::UnknownTarget {
                    got: call.target_rollup_id,
                }
                .into());
            }
        }

        // Sorted plan order for deterministic output (upstream's invariant 2).
        let mut plan_order: Vec<RollupId> = self.rollups.keys().copied().collect();
        plan_order.sort();

        // Under the multi-prover ABI, `proofs[]` lives inside the batch
        // struct (`ProofSystemBatchPerVerificationEntries.proofs`). The
        // composer's `encode_table_payload` path here emits the
        // empty-`proofs[]` batch destined for follower-side
        // `loadExecutionTable` payloads; the real L1-poster path (proofs
        // populated, signatures attached) lives in
        // `composer-lib::post_batch_submitter` (`submit_with_proof`).

        // One TargetComposition per non-entry rollup with a batch. The
        // entry rollup is the composition source, built below.
        let mut target_compositions: Vec<TargetComposition> = Vec::new();
        for rollup_id in &plan_order {
            if *rollup_id == self.entry_rollup_id {
                continue;
            }
            let group_calls = self.group_calls_for(*rollup_id);
            let Some(batch) = self.build_target_batch(*rollup_id, &group_calls)? else {
                // Nothing to build — no calls, empty settlement, or all reverted.
                continue;
            };

            // A `Some` batch means the group is non-empty, so `group_calls[0]`
            // (the outer call driving the follower trigger) exists.
            let dialect = self.rollups[rollup_id].config.dialect;
            target_compositions.push(TargetComposition {
                rollup_id: *rollup_id,
                load_table_payload: entries::encode_table_payload(&batch, &dialect),
                execute_payload: dialect.encode_follower_trigger(&group_calls[0]),
                batch,
            });
        }

        // Source (entry-rollup) batch.
        let entry_dialect = self
            .rollups
            .get(&self.entry_rollup_id)
            .expect("entry rollup registered at builder construction")
            .config
            .dialect;
        let entry_batch = entries::build_batch(&self.recorded, &entry_dialect, self.entry_rollup_id)?;

        tracing::debug!(
            name: "composer.finalize.complete",
            target_count = target_compositions.len(),
            "composition finalize complete"
        );

        Ok(Composition {
            source: SourceComposition {
                rollup_id: self.entry_rollup_id,
                entry_payload: entries::encode_table_payload(&entry_batch, &entry_dialect),
                batch: entry_batch,
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
    /// returns [`ExecutorErrorKind::InvalidReentry`].
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::InvalidReentry`] for same-chain
    /// non-entry self-dispatch. Returns [`ExecutorErrorKind::Unavailable`]
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
    ) -> ExecutorResult<ExecutionOutcome> {
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
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            }));
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
        let outcome_res = session.execute(req, self).await;

        // Put the session back even on error; revert handling is
        // post-close via `annotate_revert_span`.
        self.checked_out.remove(&target_rollup_id);
        self.rollups
            .get_mut(&target_rollup_id)
            .expect("rollup not removable")
            .session = Some(session);

        let outcome = outcome_res?;

        // Phase 3 — close: resolve the slot with the real outcome.
        self.close_call(idx, outcome.clone(), None);

        tracing::debug!(
            name: "composer.dispatch_call",
            %target_rollup_id,
            %source_rollup_id,
            success = outcome.is_success(),
            gas = outcome.gas_used().unwrap_or(0),
            "dispatched cross-chain call"
        );

        Ok(outcome)
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
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            }));
        }
        // Cyclic nesting (entry→A→B→A): A's session is checked out by the
        // outer frame, so a lazy-open here would mint a DUPLICATE whose
        // writes the outer put-back drops. Refuse loudly (depth>1 is
        // unbuilt; this turns a silent state loss into an error).
        if self.checked_out.contains(&target_rollup_id) {
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            }));
        }
        if !self.rollups.contains_key(&target_rollup_id) {
            return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                format!("no rollup registered for {target_rollup_id}"),
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

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::cross_chain_call_hash;
    use alloy_primitives::{Address, Bytes, U256};

    // Cross-tx session carry-over (F1, D-3). No production caller wires
    // this up on this branch, so these live as test-only helpers rather
    // than builder methods: seed live sessions into a builder, and drain
    // them back out.
    fn with_sessions(
        mut builder: CompositionBuilder,
        sessions: HashMap<RollupId, Box<dyn TargetExecutionSession + Send>>,
    ) -> CompositionBuilder {
        for (id, session) in sessions {
            match builder.rollups.get_mut(&id) {
                Some(rollup) => rollup.session = Some(session),
                None => tracing::warn!(
                    name: "composer.builder.session_dropped",
                    rollup_id = %id,
                    "seeded session for an unregistered rollup — dropped",
                ),
            }
        }
        builder
    }

    fn take_sessions(
        builder: &mut CompositionBuilder,
    ) -> HashMap<RollupId, Box<dyn TargetExecutionSession + Send>> {
        builder
            .rollups
            .iter_mut()
            .filter_map(|(id, rollup)| rollup.session.take().map(|s| (*id, s)))
            .collect()
    }

    // ── Mock ChainClient (spawns a canned session) ──────────────────

    struct MockClient {
        session_outcome: ExecutionOutcome,
    }

    #[async_trait::async_trait]
    impl ChainClient for MockClient {
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(MockSession {
                outcome: self.session_outcome.clone(),
            }))
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
        ) -> ExecutorResult<ExecutionOutcome> {
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
    }

    struct ReentrantClient {
        rollup: RollupId,
    }

    #[async_trait::async_trait]
    impl ChainClient for ReentrantClient {
        async fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(ReentrantSession {
                own_rollup: self.rollup,
            }))
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
        ) -> ExecutorResult<ExecutionOutcome> {
            Ok(self.outcome.clone())
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
            proxy_lookup: ProxyLookupConfig {
                contract_address: Address::ZERO,
                authorized_proxies_slot: 0,
            },
            dialect: ChainDialect::EvmL2Style,
        }
    }

    fn entry_rollup(outcome_root: [u8; 32]) -> Rollup {
        rollup_with_session(outcome_root)
    }

    fn rollup_with_session(outcome_root: [u8; 32]) -> Rollup {
        Rollup {
            client: Arc::new(MockClient {
                session_outcome: sample_outcome(outcome_root),
            }),
            session: None,
            config: target_config(),
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
        assert_eq!(response.post_state_root(), Some(&[0x11u8; 32]));
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
        builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect("dispatch lazy-opens rollup 1's session");
        let sessions = take_sessions(&mut builder);
        assert_eq!(sessions.len(), 1, "exactly the lazily-opened session");
        assert!(sessions.contains_key(&RollupId(1)));
        // Taking is draining: a second take finds nothing.
        assert!(take_sessions(&mut builder).is_empty());

        // Seed into a fresh builder: the slot is occupied (no lazy re-open).
        let mut rollups2 = HashMap::new();
        rollups2.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups2.insert(RollupId(1), rollup_with_session([0x22; 32]));
        let mut builder2 = with_sessions(CompositionBuilder::new(RollupId(0), rollups2), sessions);
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
        let carried = take_sessions(&mut builder2);
        let mut rollups3 = HashMap::new();
        rollups3.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder3 = with_sessions(CompositionBuilder::new(RollupId(0), rollups3), carried);
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
                },
        );
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let err = builder
            .dispatch_call(RollupId(1), RollupId(0), make_request(1))
            .await
            .expect_err("cycle must be refused");
        assert!(
            matches!(err.kind(), ExecutorErrorKind::InvalidReentry { .. }),
            "got: {err}"
        );
        // The outer session was put back despite the inner error.
        assert_eq!(take_sessions(&mut builder).len(), 1, "outer session survives");
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
        assert!(matches!(err.kind(), ExecutorErrorKind::Unavailable(_)));
    }

    #[tokio::test]
    async fn finalize_empty_errors() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder = CompositionBuilder::new(RollupId(0), rollups);
        let err = builder.finalize().await.expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(p.kind(), crate::error::ProtocolErrorKind::EmptyCalls)
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

        let composition = builder.finalize().await.expect("finalize");
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
        });

        let err = builder.finalize().await.expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(
                    p.kind(),
                    crate::error::ProtocolErrorKind::UnknownTarget { got: RollupId(99) }
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

        let composition = builder.finalize().await.expect("finalize");
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

    fn rollup_with_reverted_session() -> Rollup {
        Rollup {
            client: Arc::new(MockClient {
                session_outcome: ExecutionOutcome::Resolved {
                    return_data: b"revert".to_vec(),
                    pre_state_root: [0u8; 32],
                    post_state_root: [0u8; 32],
                    gas_used: 1,
                    success: false,
                },
            }),
            session: None,
            config: target_config(),
        }
    }

    #[tokio::test]
    async fn empty_target_entries_omit_target_composition() {
        // Terminal revert — the emitter returns an empty target-entry
        // set (and the sidecar skips reverted incoming calls), so the
        // `TargetComposition` for the reverted rollup must be omitted
        // from the returned `Composition`.
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

        let composition = builder.finalize().await.expect("finalize");

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
        assert!(resp.is_success());
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
