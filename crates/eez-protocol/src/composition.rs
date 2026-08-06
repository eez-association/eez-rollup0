//! Composition builder — drives a single cross-chain composition from
//! proxy-call detection through final `Composition` output.
//!
//! The builder merges three concerns that all operate on the same
//! per-composition state (the rollup map + the list of recorded calls):
//!
//! - **Target routing**: on each detected proxy call during source
//!   simulation, the source inspector calls
//!   [`CompositionBuilder::dispatch_call`], which looks up the registered
//!   target, opens its session if needed, and executes the request. The
//!   outcome is returned to the source inspector so execution can continue.
//! - **Recording**: each dispatched call opens a pending [`ExecutedAction`]
//!   before target execution and resolves that same slot afterward.
//! - **Finalization**: [`CompositionBuilder::finalize`] consumes the
//!   builder, builds source + target entries via the [`crate::entries`]
//!   builders, and produces a [`crate::types::Composition`].
//!
//! # Design
//!
//! - **Sealed at construction**. Every dispatchable target is passed to `new`;
//!   there is no `register_rollup` operation on the builder.
//! - **Owned [`Rollup`] per target**. Each target bundles its client, target
//!   configuration, and an optional session. The builder opens a missing
//!   session lazily and reuses a pre-seeded session.
//! - **Entry-aware**. `finalize` omits the entry rollup from `targets`
//!   because its output lives in `source`.
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
//!      │ finalize()                     (consumes self)   │
//!      │   1. validate recorded calls and target plans    │
//!      │   2. build each non-entry target batch:          │
//!      │      L1 post-batch or L2 inbound sidecar         │
//!      │   3. build the entry-rollup batch                │
//!      │   4. package the batches into Composition        │
//!      └──────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//!                  Composition
//! ```

use std::collections::HashMap;
use std::sync::Arc;

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

// ── Rollup ───────────────────────────────────────────────────────

/// Per-rollup static configuration.
///
/// Holds the proxy lookup for one rollup (entry or follower). Passed to the
/// runtime composer's rollup-registration paths alongside the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetConfig {
    /// Proxy-lookup configuration for this rollup.
    pub proxy_lookup: ProxyLookupConfig,
    /// Contract dialect used for proxy lookup and target-batch construction.
    pub dialect: ChainDialect,
}

/// Per-rollup state held inside a [`CompositionBuilder`] during one
/// composition.
///
/// The builder opens the session on first dispatch when `session` is `None`;
/// otherwise it reuses the pre-seeded session. The configuration selects the
/// target contract dialect and proxy lookup layout.
pub struct Rollup {
    /// Client for this rollup — shared long-lived trait object.
    pub client: Arc<dyn ChainClient + Send + Sync>,
    /// Session for this rollup. [`CompositionBuilder::dispatch_call`] opens one
    /// lazily when this is `None`.
    pub session: Option<Box<dyn TargetExecutionSession + Send>>,
    /// Target contract dialect and proxy lookup configuration for this rollup.
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
/// One builder serves one source transaction. It is constructed with the entry
/// rollup ID and every dispatchable [`Rollup`] target, dispatches proxy calls
/// through [`CompositionBuilder::dispatch_call`] during source simulation, and
/// is consumed by [`CompositionBuilder::finalize`] to produce the final
/// [`crate::types::Composition`].
///
/// # Dispatch lifecycle
///
/// Each call runs through a two-phase open/close lifecycle:
///
/// 1. [`open_call`](Self::open_call) — push a `Pending` `ExecutedAction`
///    placeholder, return its slot index. Called before recursing into
///    `session.execute`; this is what makes `recorded[..]` a preorder
///    traversal (parent's index is fixed before any nested dispatches
///    can push their own).
/// 2. [`close_call`](Self::close_call) — overwrite the slot's outcome
///    with the resolved result.
///
/// [`dispatch_call`](Self::dispatch_call) wraps the open → execute → close
/// lifecycle and is the inspector entry point.
///
/// [`recorded_count`](Self::recorded_count) snapshots the current
/// length so the inspector can bracket a CALL frame:
/// `start = recorded_count()` at frame open, `end = recorded_count()`
/// at frame end, and on revert the inspector calls
/// [`annotate_revert_span`](Self::annotate_revert_span) with the
/// resulting `(start, end - start)` so the bracketed calls' revert scope is
/// retained. The materializer rejects every recorded revert span rather than
/// lowering it to `revertNextNCalls`.
///
/// `annotate_revert_span` is separate from `close_call` because
/// `Inspector::call_end` runs after dispatch has resolved the slot. Rewriting
/// the outcome would violate the pending-to-resolved transition debug-asserted
/// by `close_call`, so the post-close span write is its own operation.
pub struct CompositionBuilder {
    pub(crate) entry_rollup_id: RollupId,
    pub(crate) rollups: HashMap<RollupId, Rollup>,
    pub(crate) recorded: Vec<ExecutedAction>,
    /// Per-call checkpoint keyed by `recorded[..]` index. Checkpoints remain
    /// available while enclosing EVM frames may still revert; reverted spans
    /// queue their indices for rollback. Unused checkpoints are dropped with
    /// the builder.
    pub(crate) pending_snapshots: HashMap<usize, crate::executor::SessionSnapshot>,
    /// Rollups whose sessions are temporarily removed while `execute` runs.
    /// Recursive dispatch to a checked-out rollup is rejected to avoid opening
    /// a second session whose writes would be lost when the first is restored.
    pub(crate) checked_out: std::collections::HashSet<RollupId>,
    /// Recorded-call indices queued whenever a revert span is recorded. They
    /// are consumed at the start of the next dispatch.
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
    /// `rollups` contains every target available for dispatch. It needs the
    /// entry rollup only when a call may target that rollup.
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

    /// Seed target sessions created by an earlier builder.
    ///
    /// A seeded session continues from its existing execution state. Sessions
    /// for rollups absent from this builder's plan are dropped and logged.
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

    /// Remove and return every live target session.
    ///
    /// This lets an orchestrator transfer session state before [`Self::finalize`]
    /// consumes the builder.
    pub fn take_sessions(&mut self) -> HashMap<RollupId, Box<dyn TargetExecutionSession + Send>> {
        self.rollups
            .iter_mut()
            .filter_map(|(id, rollup)| rollup.session.take().map(|s| (*id, s)))
            .collect()
    }

    /// Apply queued rollbacks at the start of the next [`Self::dispatch_call`].
    fn process_pending_rollbacks(&mut self) -> ExecutorResult<()> {
        if self.pending_rollbacks.is_empty() {
            return Ok(());
        }
        let queued: Vec<usize> = std::mem::take(&mut self.pending_rollbacks);
        // Roll back each rollup once using its first queued checkpoint;
        // discard later queued checkpoints for the same rollup.
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
                session.rollback(snap)?;
            }
        }
        // Later queued checkpoints for a handled rollup are discarded because
        // the first rollback already restored that shared session.
        Ok(())
    }

    /// Filtering the preorder action log preserves dispatch order for each
    /// target.
    fn group_calls_for_target_rollup_id(&self, rollup_id: RollupId) -> Vec<ExecutedAction> {
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
    /// 1. Require recorded calls and a non-empty target plan, and verify that
    ///    every call targets a registered rollup.
    /// 2. Require every call to be resolved, mutable, successful, free of a
    ///    revert span, and sourced from the entry rollup.
    /// 3. Build one `EvmBatch` for each non-empty, non-entry target.
    /// 4. Build the entry-rollup batch.
    /// 5. Package the source and sorted target batches as [`Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolErrorKind::EmptyCalls`] when there are no recorded
    /// calls or no registered rollups, [`ProtocolErrorKind::UnknownTarget`] for
    /// an unregistered target, [`ProtocolErrorKind::InvalidEncoding`] for an
    /// unresolved call, and [`ProtocolErrorKind::Unsupported`] for execution
    /// shapes outside the supported materialization profile.
    // Keep the structured public error type rather than boxing it.
    #[allow(clippy::result_large_err)]
    #[tracing::instrument(level = "debug", name = "finalize", skip_all, err)]
    pub fn finalize(self) -> CompositionResult<Composition> {
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
        entries::ensure_materializable_calls(&self.recorded)?;
        entries::ensure_source_side_calls(self.recorded.iter(), self.entry_rollup_id)?;

        // Sort by rollup ID so identical inputs produce identical target order.
        let mut plan: Vec<(&RollupId, &Rollup)> = self.rollups.iter().collect();
        plan.sort_by_key(|(id, _)| **id);

        // Build per-rollup target batches (non-entry rollups only).
        let mut target_compositions = Vec::new();
        for (rollup_id, rollup) in plan {
            if *rollup_id == self.entry_rollup_id {
                continue;
            }

            let group_calls = self.group_calls_for_target_rollup_id(*rollup_id);
            if group_calls.is_empty() {
                continue;
            }

            // `ensure_source_side_calls` guarantees that every recorded source
            // is the entry rollup. Because this loop excludes that rollup,
            // every call in the target group is incoming; only the target
            // dialect determines its materialized form.
            let batch = if rollup.config.dialect.is_evm_l1_style() {
                let batch = entries::build_l1_postbatch(&group_calls, self.entry_rollup_id)?;
                tracing::debug!(
                    name: "composer.evm_l1_postbatch",
                    %rollup_id,
                    entries = group_calls.len(),
                    "EVM L1-style target: built immediate L1 postBatch; settlement applied at submission",
                );
                batch
            } else {
                let batch = entries::build_l1_inbound_sidecar(&group_calls, *rollup_id)?;
                tracing::debug!(
                    name: "composer.inbound_sidecar",
                    %rollup_id,
                    entries = group_calls.len(),
                    "inbound target sidecar built",
                );
                batch
            };
            target_compositions.push(TargetComposition {
                rollup_id: *rollup_id,
                batch,
            });
        }

        // Build the entry-rollup batch across the full preorder slice.
        let entry_batch = entries::build_batch(&self.recorded, self.entry_rollup_id)?;

        tracing::debug!(
            name: "composer.finalize.complete",
            target_count = target_compositions.len(),
            "composition finalize complete"
        );

        Ok(Composition {
            source: SourceComposition {
                rollup_id: self.entry_rollup_id,
                batch: entry_batch,
            },
            targets: target_compositions,
        })
    }

    /// Convenience entry point: open → execute on the target session
    /// → close. Inspectors call this from their EVM-frame `call`
    /// handler.
    ///
    /// Enforces a same-chain re-entry guard when `target_rollup_id` matches
    /// `req.source_rollup_id` outside the entry rollup.
    /// `req.source_rollup_id` is forwarded unchanged to target execution and
    /// becomes the recorded call's source identity for batch materialization.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::InvalidReentry`] for prohibited re-entry
    /// and [`ExecutorErrorKind::Unavailable`] for an unregistered target.
    /// Propagates rollback, session creation, checkpoint, and target-execution
    /// errors.
    #[tracing::instrument(
        level = "debug",
        name = "dispatch_call",
        skip_all,
        fields(target = %target_rollup_id, source = %req.source_rollup_id),
        err,
    )]
    pub fn dispatch_call(
        &mut self,
        target_rollup_id: RollupId,
        req: ExecutionRequest,
    ) -> ExecutorResult<ExecutionOutcome> {
        let source_rollup_id = req.source_rollup_id;
        // Drain rollbacks queued by a previous frame before opening another
        // target call.
        self.process_pending_rollbacks()?;

        // Phase 1 — open: lazy-open the session, snapshot it, push
        // `Pending` placeholder, capture slot index.
        let idx = self.open_call(target_rollup_id, &req)?;

        // Phase 2 — run execute on the lazy-opened session.
        let mut session = self
            .rollups
            .get_mut(&target_rollup_id)
            .expect("rollup present (just checked)")
            .session
            .take()
            .expect("session opened by open_call");

        // A target-session inspector may dispatch recursively during
        // `session.execute`, appending child actions after `idx`. Recording the
        // parent before execution therefore preserves preorder.
        self.checked_out.insert(target_rollup_id);
        let response_res = session.execute(req, self);

        // Restore the checked-out session before propagating the execution
        // result. Controlled EVM reverts arrive as `ExecutionOutcome`, not
        // `Err`.
        self.checked_out.remove(&target_rollup_id);
        self.rollups
            .get_mut(&target_rollup_id)
            .expect("rollup not removable")
            .session = Some(session);

        let response = response_res?;

        // Phase 3 — close: resolve the slot with the real outcome.
        self.close_call(idx, response.clone(), None);

        tracing::debug!(
            name: "composer.dispatch_call",
            %target_rollup_id,
            %source_rollup_id,
            success = response.is_success(),
            gas = response.gas_used().unwrap_or(0),
            "dispatched cross-chain call"
        );

        Ok(response)
    }

    /// Push a `Pending` placeholder for a new call and return its
    /// slot index. Called before the target session's `execute` so
    /// the index is stable across nested dispatches.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid re-entry, an unregistered target, session
    /// creation failure, or checkpoint failure.
    pub fn open_call(
        &mut self,
        target_rollup_id: RollupId,
        req: &ExecutionRequest,
    ) -> ExecutorResult<usize> {
        let source_rollup_id = req.source_rollup_id;
        if target_rollup_id == source_rollup_id && target_rollup_id != self.entry_rollup_id {
            return Err(ExecutorError::from(ExecutorErrorKind::InvalidReentry {
                caller: source_rollup_id,
                target: target_rollup_id,
            }));
        }
        // Nested materialization is rejected later, but simulation must still
        // refuse cycles such as entry→A→B→A to prevent silent session-state
        // loss while A's session is checked out.
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
                let new_session = rollup.client.begin_execution_session()?;
                rollup.session = Some(new_session);
            }
            let session = rollup.session.as_mut().expect("session opened above");
            session.checkpoint()?
        };

        let idx = self.recorded.len();
        self.recorded.push(ExecutedAction {
            call_mode: req.call_mode,
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

    /// Resolve the call opened by [`open_call`](Self::open_call) at `idx`.
    ///
    /// The materializer rejects a non-`None` `revert_span`. Callers normally
    /// pass `None`; [`annotate_revert_span`](Self::annotate_revert_span)
    /// records a reverted enclosing frame after execution.
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
            // Queue this slot's snapshot for rollback at the next dispatch
            // boundary.
            self.pending_rollbacks.push(idx);
        }
        // When `revert_span` is `None`, the checkpoint remains available for
        // an enclosing frame that may still revert. Unused checkpoints are
        // dropped with the builder.
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

    /// Mark the bracketing call with `revert_span` and queue checkpoints for
    /// recorded calls in that range. Rollbacks are coalesced per rollup and
    /// applied before a later dispatch.
    pub fn annotate_revert_span(&mut self, idx: usize, span: u32) {
        if idx >= self.recorded.len() || span == 0 {
            return;
        }
        // Only the head carries the scope; bracketed inner calls do not.
        self.recorded[idx].revert_span = Some(span);
        // Queue rollbacks for every recorded index in the bracket.
        // `process_pending_rollbacks` consolidates by rollup id and
        // applies one rollback per affected session at the next dispatch
        // boundary.
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
    use crate::action::{CallHashInput, CallMode, common_cross_chain_call_hash};
    use alloy_primitives::{Address, Bytes, U256, b256, keccak256};
    use alloy_sol_types::SolValue as _;

    // ── Mock ChainClient (spawns a canned session) ──────────────────

    struct MockClient {
        session_outcome: ExecutionOutcome,
    }

    impl ChainClient for MockClient {
        fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(MockSession {
                outcome: self.session_outcome.clone(),
            }))
        }
    }

    // ── Reentrant fakes ──────────────────────────────────────────

    /// Session whose `execute` immediately attempts to reopen its own rollup,
    /// modeling the open phase of an entry→A→…→A cycle. The checked-out guard
    /// must refuse the inner `open_call`; otherwise a duplicate session could
    /// lose writes when the outer session is restored.
    struct ReentrantSession {
        own_rollup: RollupId,
    }

    impl TargetExecutionSession for ReentrantSession {
        fn execute(
            &mut self,
            req: ExecutionRequest,
            dispatcher: &mut CompositionBuilder,
        ) -> ExecutorResult<ExecutionOutcome> {
            // The target session is already checked out, so reopening its
            // rollup must fail before a duplicate session is created.
            dispatcher.open_call(self.own_rollup, &req)?;
            unreachable!("the checked-out guard must refuse the cyclic open_call");
        }
        fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
            Ok(Box::new(()) as crate::executor::SessionSnapshot)
        }
        fn rollback(&mut self, _snapshot: crate::executor::SessionSnapshot) -> ExecutorResult<()> {
            Ok(())
        }
    }

    struct ReentrantClient {
        rollup: RollupId,
    }

    impl ChainClient for ReentrantClient {
        fn begin_execution_session(
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

    impl TargetExecutionSession for MockSession {
        fn execute(
            &mut self,
            _req: ExecutionRequest,
            _dispatcher: &mut CompositionBuilder,
        ) -> ExecutorResult<ExecutionOutcome> {
            Ok(self.outcome.clone())
        }

        fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
            Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
        }

        fn rollback(&mut self, _snap: crate::executor::SessionSnapshot) -> ExecutorResult<()> {
            Ok(())
        }
    }

    struct SourceEchoClient;

    impl ChainClient for SourceEchoClient {
        fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(SourceEchoSession))
        }
    }

    struct SourceEchoSession;

    impl TargetExecutionSession for SourceEchoSession {
        fn execute(
            &mut self,
            req: ExecutionRequest,
            _dispatcher: &mut CompositionBuilder,
        ) -> ExecutorResult<ExecutionOutcome> {
            Ok(ExecutionOutcome::Resolved {
                return_data: req.source_rollup_id.0.to_be_bytes().to_vec(),
                gas_used: 21_000,
                success: true,
            })
        }

        fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
            Ok(Box::new(()) as crate::executor::SessionSnapshot)
        }

        fn rollback(&mut self, _snapshot: crate::executor::SessionSnapshot) -> ExecutorResult<()> {
            Ok(())
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    fn sample_outcome(return_data: [u8; 32]) -> ExecutionOutcome {
        ExecutionOutcome::Resolved {
            return_data: return_data.to_vec(),
            gas_used: 21_000,
            success: true,
        }
    }

    fn make_request(rollup: u64) -> ExecutionRequest {
        make_request_from(rollup, RollupId::MAINNET)
    }

    fn make_request_from(rollup: u64, source_rollup_id: RollupId) -> ExecutionRequest {
        ExecutionRequest {
            call_mode: crate::CallMode::Mutable,
            target_address: Address::repeat_byte(rollup as u8),
            data: Bytes::from(vec![0x01, 0x02]),
            value: U256::ZERO,
            source_address: Address::ZERO,
            source_rollup_id,
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

    fn entry_rollup(return_data: [u8; 32]) -> Rollup {
        Rollup {
            client: Arc::new(MockClient {
                session_outcome: sample_outcome(return_data),
            }),
            session: None,
            config: target_config(),
        }
    }

    fn rollup_with_session(return_data: [u8; 32]) -> Rollup {
        Rollup {
            client: Arc::new(MockClient {
                session_outcome: sample_outcome(return_data),
            }),
            session: None,
            config: target_config(),
        }
    }

    fn source_echo_rollup() -> Rollup {
        Rollup {
            client: Arc::new(SourceEchoClient),
            session: None,
            config: target_config(),
        }
    }

    // ── Tests ────────────────────────────────────────────────────────

    #[test]
    fn dispatch_routes_to_registered_session_and_records() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let response = builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("dispatch");
        assert_eq!(response.return_data(), Some(&[0x11; 32][..]));
        assert_eq!(builder.recorded.len(), 1);
        assert_eq!(builder.recorded[0].target_rollup_id, RollupId(1));
        assert_eq!(builder.recorded[0].source_rollup_id, RollupId(0));
        assert_eq!(builder.recorded[0].call_mode, crate::CallMode::Mutable);
    }

    #[test]
    fn dispatch_preserves_call_mode() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let mut request = make_request(1);
        request.call_mode = crate::CallMode::Static;

        builder
            .dispatch_call(RollupId(1), request)
            .expect("dispatch");

        assert_eq!(builder.recorded[0].call_mode, crate::CallMode::Static);
    }

    #[test]
    fn sessions_seed_take_round_trip_and_drop_unregistered() {
        // `take_sessions` extracts the lazily opened session and
        // `with_sessions` can seed it into another compatible builder. A
        // session for an unregistered rollup is dropped.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let _ = builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("dispatch lazy-opens rollup 1's session");
        let sessions = builder.take_sessions();
        assert_eq!(sessions.len(), 1, "exactly the lazily-opened session");
        assert!(sessions.contains_key(&RollupId(1)));
        assert!(builder.take_sessions().is_empty());

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

    #[test]
    fn cyclic_nested_dispatch_is_refused() {
        // While A's session is checked out, attempting to reopen A must return
        // `InvalidReentry` instead of creating a duplicate session whose writes
        // would be lost when the outer session is restored.
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
            .dispatch_call(RollupId(1), make_request(1))
            .expect_err("cycle must be refused");
        assert!(
            matches!(err.kind(), ExecutorErrorKind::InvalidReentry { .. }),
            "got: {err}"
        );
        // The outer session was put back despite the inner error.
        assert_eq!(builder.take_sessions().len(), 1, "outer session survives");
    }

    #[test]
    fn dispatch_unknown_rollup_returns_unavailable() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(99), make_request(99))
            .expect_err("should fail");
        assert!(matches!(err.kind(), ExecutorErrorKind::Unavailable(_)));
    }

    #[test]
    fn finalize_empty_errors() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        let builder = CompositionBuilder::new(RollupId(0), rollups);
        let err = builder.finalize().expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(p.kind(), crate::error::ProtocolErrorKind::EmptyCalls)
        ));
    }

    #[test]
    fn finalize_inbound_target_produces_sidecar_composition() {
        // An entry→rollup-1 call produces an L2 inbound target sidecar.
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("dispatch");

        let composition = builder.finalize().expect("finalize");
        assert_eq!(composition.source.rollup_id, RollupId(0));
        assert_eq!(composition.targets.len(), 1);
        assert_eq!(composition.targets[0].rollup_id, RollupId(1));

        // The sidecar entry mirrors the recorded call and binds its
        // destination-side identity in `proxyEntryHash`.
        let entries = &composition.targets[0].batch.entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].l2ToL1Calls.len(), 1);
        assert_eq!(
            entries[0].l2ToL1Calls[0].targetAddress,
            Address::repeat_byte(1)
        );
        assert_eq!(
            entries[0].proxyEntryHash,
            common_cross_chain_call_hash(CallHashInput {
                call_mode: CallMode::Mutable,
                source_address: Address::ZERO,
                source_rollup_id: RollupId::MAINNET,
                target_address: Address::repeat_byte(1),
                target_rollup_id: RollupId(1),
                value: U256::ZERO,
                data: &Bytes::from(vec![0x01, 0x02]),
            },),
        );

        // The entry batch carries the top-level call as one deferred entry.
        assert_eq!(composition.source.batch.entries.len(), 1);
    }

    #[test]
    fn finalize_rejects_recorded_calls_for_unregistered_rollups() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        builder.recorded.push(ExecutedAction {
            call_mode: crate::CallMode::Mutable,
            target_address: Address::ZERO,
            target_rollup_id: RollupId(99),
            source_rollup_id: RollupId(0),
            source_address: Address::ZERO,
            data: Bytes::new(),
            value: U256::ZERO,
            outcome: sample_outcome([0u8; 32]),
            revert_span: None,
        });

        let err = builder.finalize().expect_err("should fail");
        assert!(matches!(
            err.kind(),
            crate::error::CompositionErrorKind::Protocol(p)
                if matches!(
                    p.kind(),
                    crate::error::ProtocolErrorKind::UnknownTarget { got: RollupId(99) }
                )
        ));
    }

    #[test]
    fn finalize_rejects_static_calls_before_building_any_dialect() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);
        let mut request = make_request(1);
        request.call_mode = crate::CallMode::Static;
        builder
            .dispatch_call(RollupId(1), request)
            .expect("dispatch");

        let error = builder
            .finalize()
            .expect_err("static entry materialization is not implemented");

        assert!(matches!(
            error.kind(),
            crate::error::CompositionErrorKind::Protocol(protocol)
                if matches!(
                    protocol.kind(),
                    crate::ProtocolErrorKind::Unsupported(
                        "static cross-chain calls are not supported"
                    )
                )
        ));
    }

    #[test]
    fn finalize_rejects_pending_call_before_target_materialization() {
        let mut l1_rollup = rollup_with_session([0u8; 32]);
        l1_rollup.config.dialect = ChainDialect::EvmL1Style;

        let mut rollups = HashMap::new();
        rollups.insert(RollupId(1), entry_rollup([0u8; 32]));
        rollups.insert(RollupId::MAINNET, l1_rollup);
        let mut builder = CompositionBuilder::new(RollupId(1), rollups);
        builder
            .open_call(RollupId::MAINNET, &make_request_from(0, RollupId(1)))
            .expect("open pending call");

        let error = builder
            .finalize()
            .expect_err("finalize must reject every unresolved call");

        assert!(matches!(
            error.kind(),
            crate::error::CompositionErrorKind::Protocol(protocol)
                if matches!(
                    protocol.kind(),
                    crate::ProtocolErrorKind::InvalidEncoding(reason)
                        if reason == "recorded cross-chain call still has a pending outcome"
                )
        ));
    }

    #[test]
    fn finalize_targets_come_out_sorted_by_rollup_id() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(3), rollup_with_session([0x33; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        for id in [3u64, 1, 2] {
            builder
                .dispatch_call(RollupId(id), make_request(id))
                .expect("dispatch");
        }

        let composition = builder.finalize().expect("finalize");
        let ids: Vec<u64> = composition.targets.iter().map(|t| t.rollup_id.0).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "targets must be sorted by rollup_id, not insertion order"
        );
    }

    #[test]
    fn composition_batch_encodings_are_stable() {
        let entry_rollup_id = RollupId(1);
        let mut l1_target = rollup_with_session([0x20; 32]);
        l1_target.config.dialect = ChainDialect::EvmL1Style;

        let mut rollups = HashMap::new();
        rollups.insert(entry_rollup_id, entry_rollup([0x10; 32]));
        rollups.insert(RollupId::MAINNET, l1_target);
        rollups.insert(RollupId(2), rollup_with_session([0x30; 32]));

        let mut builder = CompositionBuilder::new(entry_rollup_id, rollups);
        builder.recorded = vec![
            ExecutedAction {
                call_mode: CallMode::Mutable,
                target_address: Address::repeat_byte(0xa1),
                target_rollup_id: RollupId::MAINNET,
                source_rollup_id: entry_rollup_id,
                source_address: Address::repeat_byte(0xb1),
                data: Bytes::from_static(&[0x01, 0x02, 0x03]),
                value: U256::from(5),
                outcome: ExecutionOutcome::Resolved {
                    return_data: vec![0x04, 0x05],
                    gas_used: 123,
                    success: true,
                },
                revert_span: None,
            },
            ExecutedAction {
                call_mode: CallMode::Mutable,
                target_address: Address::repeat_byte(0xa2),
                target_rollup_id: RollupId(2),
                source_rollup_id: entry_rollup_id,
                source_address: Address::repeat_byte(0xb2),
                data: Bytes::from_static(&[0x06, 0x07]),
                value: U256::from(8),
                outcome: ExecutionOutcome::Resolved {
                    return_data: vec![0x09, 0x0a],
                    gas_used: 456,
                    success: true,
                },
                revert_span: None,
            },
        ];
        let composition = builder.finalize().expect("finalize composition");
        let source_hash = keccak256(composition.source.batch.abi_encode());
        let target_hashes = composition
            .targets
            .iter()
            .map(|target| (target.rollup_id, keccak256(target.batch.abi_encode())))
            .collect::<Vec<_>>();

        assert_eq!(
            source_hash,
            b256!("280a4e30c195b4f4f21c66889b1a6acb0c0b30f18b5efd435348b39fc986d56b")
        );
        assert_eq!(
            target_hashes,
            vec![
                (
                    RollupId::MAINNET,
                    b256!("f7e0edc00ed7cb620f1251581e6251ec9c459497b151bc80fe3bf6d99103bd71"),
                ),
                (
                    RollupId(2),
                    b256!("486f05e9bf5d8294de7a7f2295e0a0d630b3bef4abea866ced107f0faba42dae"),
                ),
            ]
        );
    }

    #[test]
    fn dispatch_uses_request_source_identity_end_to_end() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(7), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), source_echo_rollup());
        let mut builder = CompositionBuilder::new(RollupId(7), rollups);
        let request = make_request_from(1, RollupId(7));
        let hash_input = CallHashInput {
            call_mode: request.call_mode,
            source_address: request.source_address,
            source_rollup_id: RollupId(7),
            target_address: request.target_address,
            target_rollup_id: RollupId(1),
            value: request.value,
            data: &request.data,
        };
        let expected_hash = common_cross_chain_call_hash(hash_input);
        let mainnet_hash = common_cross_chain_call_hash(CallHashInput {
            source_rollup_id: RollupId::MAINNET,
            ..hash_input
        });

        let response = builder
            .dispatch_call(RollupId(1), request)
            .expect("dispatch");
        let expected_return_data = RollupId(7).0.to_be_bytes();
        assert_eq!(response.return_data(), Some(&expected_return_data[..]));
        assert_eq!(builder.recorded[0].source_rollup_id, RollupId(7));

        let composition = builder.finalize().expect("finalize");
        assert_eq!(composition.targets.len(), 1);
        assert_eq!(composition.targets[0].rollup_id, RollupId(1));
        let target_entry = &composition.targets[0].batch.entries[0];
        assert_eq!(target_entry.l2ToL1Calls[0].sourceRollupId, 7);
        assert_eq!(target_entry.proxyEntryHash, expected_hash);
        assert_ne!(target_entry.proxyEntryHash, mainnet_hash);
    }

    // ── Unsuccessful-call rejection ──────────────────────────────

    struct NoCcmClient;

    impl ChainClient for NoCcmClient {
        fn begin_execution_session(
            &self,
        ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
            Ok(Box::new(MockSession {
                outcome: ExecutionOutcome::Resolved {
                    return_data: b"revert".to_vec(),
                    gas_used: 1,
                    success: false,
                },
            }))
        }
    }

    fn rollup_with_reverted_session() -> Rollup {
        Rollup {
            client: Arc::new(NoCcmClient),
            session: None,
            config: target_config(),
        }
    }

    #[test]
    fn finalize_rejects_unsuccessful_calls() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_reverted_session());
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("dispatch");
        assert!(
            !builder.recorded[0].outcome.is_success(),
            "test setup: recorded call must be reverted"
        );

        let error = builder
            .finalize()
            .expect_err("unsuccessful calls are outside the supported profile");
        assert!(matches!(
            error.kind(),
            crate::error::CompositionErrorKind::Protocol(protocol)
                if matches!(
                    protocol.kind(),
                    crate::ProtocolErrorKind::Unsupported(
                        "unsuccessful cross-chain calls are not supported"
                    )
                )
        ));
    }

    #[test]
    fn finalize_rejects_nested_calls_across_distinct_targets() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("top-level dispatch");
        builder
            .dispatch_call(RollupId(2), make_request_from(2, RollupId(1)))
            .expect("nested dispatch records before materialization");

        let error = builder
            .finalize()
            .expect_err("nested calls are outside the supported profile");
        assert!(matches!(
            error.kind(),
            crate::error::CompositionErrorKind::Protocol(protocol)
                if matches!(
                    protocol.kind(),
                    crate::ProtocolErrorKind::Unsupported(
                        "nested cross-chain calls are not supported"
                    )
                )
        ));
    }

    // ── Re-entry guard tests ──────────────────────────────────────

    /// A non-entry rollup dispatching back to itself must surface
    /// `InvalidReentry` before recording or opening another session.
    #[test]
    fn dispatch_same_chain_non_entry_returns_invalid_reentry() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let err = builder
            .dispatch_call(RollupId(1), make_request_from(1, RollupId(1)))
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
        assert!(
            builder
                .rollups
                .get(&RollupId(1))
                .is_some_and(|rollup| rollup.session.is_none()),
            "the guard must fire before opening a session",
        );
    }

    // ── Preorder lifecycle + revertSpan vector tests ─────────────────

    /// `open_call` fixes a slot before execution and `close_call` resolves it.
    /// A subsequent dispatch appends a later slot.
    #[test]
    fn dispatch_records_preorder_at_open_call() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        // Walk the lifecycle directly to verify that `open_call` fixes the
        // index before `close_call` resolves it.
        let req = make_request(1);
        let idx = builder.open_call(RollupId(1), &req).expect("open");
        assert_eq!(idx, 0);
        assert!(builder.recorded[idx].outcome.is_pending());
        builder.close_call(idx, sample_outcome([0xCC; 32]), None);
        assert!(builder.recorded[idx].outcome.is_success());
        assert_eq!(
            builder.recorded[idx].outcome.return_data(),
            Some(&[0xCC; 32][..])
        );

        let resp = builder
            .dispatch_call(RollupId(1), make_request(1))
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
    /// bracket for rollback at the next dispatch boundary.
    #[test]
    fn annotate_revert_span_writes_span_and_queues_rollback() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
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
    #[test]
    fn revert_span_gt_one_covers_outer_plus_two_children() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        // Frame open: simulate inspector bracket — record start.
        let start = builder.recorded_count();
        // Outer: 0 → 1
        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("outer");
        // Two inner cross-rollup calls inside the outer frame.
        builder
            .dispatch_call(RollupId(2), make_request_from(2, RollupId(1)))
            .expect("inner-1");
        builder
            .dispatch_call(RollupId(2), make_request_from(2, RollupId(1)))
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
    #[test]
    fn sibling_after_revert_only_annotates_reverted_outer() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("A");
        builder.annotate_revert_span(0, 1);

        builder
            .dispatch_call(RollupId(1), make_request(1))
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
    /// The child succeeded but is queued for rollback with the outer bracket.
    #[test]
    fn parent_reverts_after_successful_child() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        rollups.insert(RollupId(2), rollup_with_session([0x22; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        let start = builder.recorded_count();
        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("outer");
        builder
            .dispatch_call(RollupId(2), make_request_from(2, RollupId(1)))
            .expect("child (succeeds)");
        let span = (builder.recorded_count() - start) as u32;
        assert_eq!(span, 2);

        // Both calls succeeded individually, but the outer frame
        // reverts — the inspector observes that and annotates.
        builder.annotate_revert_span(start, span);

        assert_eq!(builder.recorded[0].revert_span, Some(2));
        assert!(builder.recorded[1].outcome.is_success());
        assert_eq!(builder.recorded[1].revert_span, None);
        // Both bracketed slots are queued for rollback; this test observes the
        // queue before the next dispatch drains it.
        assert_eq!(builder.pending_rollbacks, vec![0, 1]);
    }

    /// Revert at a previous `Inspector::call_end` is applied at the
    /// top of the next `dispatch_call` — the queued rollbacks drain
    /// to `session.rollback()` invocations before the new call opens.
    #[test]
    fn pending_rollbacks_drain_at_next_dispatch() {
        let mut rollups = HashMap::new();
        rollups.insert(RollupId(0), entry_rollup([0u8; 32]));
        rollups.insert(RollupId(1), rollup_with_session([0x11; 32]));
        let mut builder = CompositionBuilder::new(RollupId(0), rollups);

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("first dispatch");
        builder.annotate_revert_span(0, 1);
        assert_eq!(builder.pending_rollbacks, vec![0]);
        assert!(builder.pending_snapshots.contains_key(&0));

        builder
            .dispatch_call(RollupId(1), make_request(1))
            .expect("second dispatch");

        assert!(
            builder.pending_rollbacks.is_empty(),
            "queue must be drained at the next dispatch boundary",
        );
        assert!(
            !builder.pending_snapshots.contains_key(&0),
            "rolled-back snapshot must be removed from stash",
        );
        // The completed call's checkpoint remains available in case an
        // enclosing frame later reverts.
        assert!(builder.pending_snapshots.contains_key(&1));
    }
}
