//! `FakeChainClient` — the canonical test double for
//! [`ChainClient`](crate::ChainClient) + [`EntryChainClient`](crate::EntryChainClient).
//!
//! # Scripted hooks
//!
//! - [`with_session_outcomes`](FakeChainClient::with_session_outcomes)
//!   — the queue of `ExecutionOutcome`s the spawned session returns,
//!   one per call.
//! - [`with_simulate_source_hook`](FakeChainClient::with_simulate_source_hook)
//!   — the closure that drives `simulate_source_tx` (entry-only).
//! - [`with_stored_root`](FakeChainClient::with_stored_root) — seed one
//!   `stored_target_state_root` lookup result (entry-only).
//! - [`with_checkpoint_factory`](FakeChainClient::with_checkpoint_factory)
//!   — how the spawned session synthesizes its `ExecutionCheckpoint`.
//!   Required for any test that calls `session.execute(...)`.
//!
//! # Assertions
//!
//! - [`dispatched_outcomes`](FakeChainClient::dispatched_outcomes) —
//!   one entry per `session.execute` call.
//!
//! # Default behavior
//!
//! Every method returns [`ExecutorErrorKind::Unavailable`] when its
//! queue or hook is unset — loud, structured, easy to diagnose. Never
//! `unimplemented!()`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::checkpoint::ExecutionCheckpoint;
use crate::composition::CompositionBuilder;
use crate::error::{ExecutorError, ExecutorErrorKind, ExecutorResult};
use crate::executor::{
    ChainClient, EntryChainClient, ExecutionRequest, ExecutionResponse, TargetExecutionSession,
};
use crate::rollup_id::RollupId;
use crate::types::ExecutionOutcome;

/// Role discriminator for [`FakeChainClient`].
///
/// On a follower fake, `simulate_source_tx` and
/// `stored_target_state_root` return [`ExecutorErrorKind::Unavailable`]
/// — the runtime mirror of the compile-time guarantee that only entry
/// clients implement those methods in production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeRole {
    /// Fake behaves as the composition entry rollup.
    Entry,
    /// Fake behaves as a follower rollup.
    Follower,
}

type SimulateSourceHook =
    Box<dyn FnMut(Vec<u8>, &mut CompositionBuilder) -> ExecutorResult<()> + Send>;

type CheckpointFactory = Box<dyn FnMut() -> ExecutionCheckpoint + Send>;

/// Canonical test double for `ChainClient` + `EntryChainClient`.
///
/// See module docs for the scripted-hook and assertion surface.
pub struct FakeChainClient {
    rollup_id: RollupId,
    role: FakeRole,

    // Session state: shared with the spawned `FakeChainSession`
    // through Arc<Mutex<_>>. The session consumes outcomes; the
    // client observes dispatched_outcomes after the composition runs.
    session_outcomes: Arc<Mutex<VecDeque<ExecutionOutcome>>>,
    dispatched_outcomes: Arc<Mutex<Vec<ExecutionOutcome>>>,
    checkpoint_factory: Arc<Mutex<Option<CheckpointFactory>>>,

    // Client-only scripted queues.
    simulate_source_hook: Mutex<Option<SimulateSourceHook>>,
    stored_roots: Mutex<HashMap<RollupId, [u8; 32]>>,
}

impl std::fmt::Debug for FakeChainClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeChainClient")
            .field("rollup_id", &self.rollup_id)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl FakeChainClient {
    /// Build an entry-role fake.
    #[must_use]
    pub fn new_entry(rollup_id: RollupId) -> Self {
        Self::new(rollup_id, FakeRole::Entry)
    }

    /// Build a follower-role fake.
    #[must_use]
    pub fn new_follower(rollup_id: RollupId) -> Self {
        Self::new(rollup_id, FakeRole::Follower)
    }

    fn new(rollup_id: RollupId, role: FakeRole) -> Self {
        Self {
            rollup_id,
            role,
            session_outcomes: Arc::new(Mutex::new(VecDeque::new())),
            dispatched_outcomes: Arc::new(Mutex::new(Vec::new())),
            checkpoint_factory: Arc::new(Mutex::new(None)),
            simulate_source_hook: Mutex::new(None),
            stored_roots: Mutex::new(HashMap::new()),
        }
    }

    /// Seed the session-outcome queue.
    #[must_use]
    pub fn with_session_outcomes<I: IntoIterator<Item = ExecutionOutcome>>(
        self,
        outcomes: I,
    ) -> Self {
        *self.session_outcomes.lock().expect("fake mutex poisoned") =
            outcomes.into_iter().collect();
        self
    }

    /// Install the `simulate_source_tx` hook (entry-only).
    #[must_use]
    pub fn with_simulate_source_hook<
        F: FnMut(Vec<u8>, &mut CompositionBuilder) -> ExecutorResult<()> + Send + 'static,
    >(
        self,
        hook: F,
    ) -> Self {
        *self
            .simulate_source_hook
            .lock()
            .expect("fake mutex poisoned") = Some(Box::new(hook));
        self
    }

    /// Seed one entry in the `stored_target_state_root` map. Chainable.
    #[must_use]
    pub fn with_stored_root(self, rollup_id: RollupId, root: [u8; 32]) -> Self {
        self.stored_roots
            .lock()
            .expect("fake mutex poisoned")
            .insert(rollup_id, root);
        self
    }

    /// Install the checkpoint factory. Required for any test that
    /// drives `session.execute(...)`.
    #[must_use]
    pub fn with_checkpoint_factory<F: FnMut() -> ExecutionCheckpoint + Send + 'static>(
        self,
        factory: F,
    ) -> Self {
        *self.checkpoint_factory.lock().expect("fake mutex poisoned") = Some(Box::new(factory));
        self
    }

    /// Snapshot of outcomes dispatched via the spawned session. One
    /// entry per `session.execute(...)` call, in dispatch order.
    #[must_use]
    pub fn dispatched_outcomes(&self) -> Vec<ExecutionOutcome> {
        self.dispatched_outcomes
            .lock()
            .expect("fake mutex poisoned")
            .clone()
    }

    fn unavailable(&self, what: &str) -> ExecutorError {
        ExecutorError::from(ExecutorErrorKind::Unavailable(format!(
            "FakeChainClient({:?}, rollup_id={}): {} not scripted",
            self.role, self.rollup_id, what
        )))
    }

    fn require_entry(&self, what: &str) -> ExecutorResult<()> {
        if self.role == FakeRole::Follower {
            return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                format!(
                    "FakeChainClient(Follower, rollup_id={}): {what} called on follower",
                    self.rollup_id,
                ),
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl ChainClient for FakeChainClient {
    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession + Send>> {
        Ok(Box::new(FakeChainSession {
            outcomes: Arc::clone(&self.session_outcomes),
            dispatched_outcomes: Arc::clone(&self.dispatched_outcomes),
            checkpoint_factory: Arc::clone(&self.checkpoint_factory),
            rollup_id: self.rollup_id,
        }))
    }

    async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
        // Self-query for tests reuses the seeded `stored_roots` map under
        // the fake's own rollup_id, falling back to all-zeros so tests
        // that don't seed a self-root don't fail surprisingly.
        Ok(self
            .stored_roots
            .lock()
            .expect("fake mutex poisoned")
            .get(&self.rollup_id)
            .copied()
            .unwrap_or([0u8; 32]))
    }
}

#[async_trait]
impl EntryChainClient for FakeChainClient {
    async fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<()> {
        self.require_entry("simulate_source_tx")?;
        let mut guard = self
            .simulate_source_hook
            .lock()
            .expect("fake mutex poisoned");
        match guard.as_mut() {
            Some(hook) => hook(raw_tx, dispatcher),
            None => Err(self.unavailable("simulate_source_hook")),
        }
    }
}

#[async_trait]
impl crate::executor::CommittedRootReader for FakeChainClient {
    async fn stored_target_state_root(&self, rollup_id: RollupId) -> ExecutorResult<[u8; 32]> {
        self.require_entry("stored_target_state_root")?;
        self.stored_roots
            .lock()
            .expect("fake mutex poisoned")
            .get(&rollup_id)
            .copied()
            .ok_or_else(|| self.unavailable(&format!("stored_root for {rollup_id}")))
    }
}

// ── Session ─────────────────────────────────────────────────────

/// Companion session spawned by [`FakeChainClient::begin_execution_session`].
///
/// Pops one outcome from the client's shared queue per
/// [`execute`](Self::execute) call and invokes the checkpoint factory
/// to synthesize the response.
pub struct FakeChainSession {
    outcomes: Arc<Mutex<VecDeque<ExecutionOutcome>>>,
    dispatched_outcomes: Arc<Mutex<Vec<ExecutionOutcome>>>,
    checkpoint_factory: Arc<Mutex<Option<CheckpointFactory>>>,
    rollup_id: RollupId,
}

impl std::fmt::Debug for FakeChainSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeChainSession")
            .field("rollup_id", &self.rollup_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TargetExecutionSession for FakeChainSession {
    async fn execute(
        &mut self,
        _req: ExecutionRequest,
        _dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionResponse> {
        let outcome = {
            let mut q = self.outcomes.lock().expect("fake mutex poisoned");
            q.pop_front().ok_or_else(|| {
                ExecutorError::from(ExecutorErrorKind::Unavailable(format!(
                    "FakeChainSession(rollup_id={}): session outcome queue empty",
                    self.rollup_id,
                )))
            })?
        };

        self.dispatched_outcomes
            .lock()
            .expect("fake mutex poisoned")
            .push(outcome.clone());

        let checkpoint = {
            let mut guard = self.checkpoint_factory.lock().expect("fake mutex poisoned");
            match guard.as_mut() {
                Some(factory) => factory(),
                None => {
                    return Err(ExecutorError::from(ExecutorErrorKind::Unavailable(
                        format!(
                            "FakeChainSession(rollup_id={}): checkpoint_factory not installed",
                            self.rollup_id,
                        ),
                    )));
                }
            }
        };

        Ok(ExecutionResponse {
            outcome,
            checkpoint,
        })
    }

    async fn checkpoint(&mut self) -> ExecutorResult<crate::executor::SessionSnapshot> {
        // Fake snapshot — captures the current outcome-queue length so
        // a rollback could restore the queue position. Tests don't
        // exercise rollback yet; the marker box is enough to satisfy
        // the trait surface.
        let q_len = self.outcomes.lock().expect("fake mutex poisoned").len();
        Ok(Box::new(q_len) as Box<dyn std::any::Any + Send>)
    }

    async fn rollback(
        &mut self,
        _snapshot: crate::executor::SessionSnapshot,
    ) -> ExecutorResult<()> {
        // Fakes don't actually restore state — they're queue-driven.
        Ok(())
    }

    async fn take_checkpoint(&mut self) -> Option<ExecutionCheckpoint> {
        // Return an ad-hoc checkpoint via the factory; `None` if
        // unset. Most tests don't exercise this path.
        let mut guard = self.checkpoint_factory.lock().expect("fake mutex poisoned");
        guard.as_mut().map(|factory| factory())
    }
}

#[cfg(test)]
mod tests {
    //! Smoke tests exercising `FakeChainClient` through
    //! `compose_transaction`. Confirms the shared fake wires up
    //! correctly and the recorder surfaces what the session saw.

    use super::*;
    use crate::composer::{ProxyLookupConfig, TargetConfig};
    use crate::composition::Rollup;
    use crate::dialect::ChainDialect;
    use crate::overlay::EvmOverlay;
    use alloy_primitives::{Address, Bytes, U256};

    fn outcome(post: [u8; 32]) -> ExecutionOutcome {
        ExecutionOutcome::Resolved {
            return_data: vec![],
            pre_state_root: [0; 32],
            post_state_root: post,
            gas_used: 1,
            success: true,
        }
    }

    fn target_cfg() -> TargetConfig {
        TargetConfig {
            proxy_lookup: ProxyLookupConfig {
                contract_address: Address::ZERO,
                authorized_proxies_slot: 0,
            },
            dialect: ChainDialect::EvmL2Style,
        }
    }

    #[tokio::test]
    async fn fake_chain_client_drives_end_to_end_composition() {
        let entry_id = RollupId(0);
        let target_id = RollupId(1);

        // Follower fake: session returns one outcome. The entry→target
        // call is INCOMING from the target's perspective, so finalize
        // takes the inbound DA-sidecar branch.
        let follower_post = [0x22; 32];
        let follower = Arc::new(
            FakeChainClient::new_follower(target_id)
                .with_session_outcomes(vec![outcome(follower_post)])
                .with_checkpoint_factory(move || ExecutionCheckpoint {
                    version: 1,
                    chain_id: 1,
                    base_block_number: 0,
                    base_block_hash: [0; 32],
                    base_state_root: [0; 32],
                    current_root: follower_post,
                    overlay: EvmOverlay::default(),
                    witness: None,
                }),
        );
        let follower_recorder = Arc::clone(&follower);

        // Entry fake: hook dispatches one call to the follower, the
        // composition pipeline routes it through the shared dispatcher.
        let entry = Arc::new(
            FakeChainClient::new_entry(entry_id)
                .with_stored_root(target_id, [0; 32])
                .with_stored_root(entry_id, [0; 32])
                .with_simulate_source_hook(move |_raw, dispatcher| {
                    let req = ExecutionRequest {
                        target_address: Address::repeat_byte(0xAB),
                        data: Bytes::new(),
                        value: U256::ZERO,
                        source_address: Address::ZERO,
                        source_rollup_id: entry_id,
                    };
                    // The real inspector bridges sync → async via a
                    // scoped OS thread + `Handle::block_on`. Mirror
                    // that here so
                    // the hook can invoke the async dispatcher from a
                    // sync closure without deadlocking the outer
                    // tokio multi-thread test runtime.
                    let handle = tokio::runtime::Handle::current();
                    std::thread::scope(|s| {
                        s.spawn(|| {
                            handle.block_on(dispatcher.dispatch_call(target_id, entry_id, req))
                        })
                        .join()
                        .expect("dispatcher thread panicked")
                    })
                    .map(|_| ())
                }),
        );

        // Build the rollup map the composer would hand to the builder.
        let mut rollups = HashMap::new();
        let entry_as_chain: Arc<dyn ChainClient + Send + Sync> = Arc::clone(&entry) as Arc<_>;
        rollups.insert(
            entry_id,
            Rollup {
                client: entry_as_chain,
                session: None,
                config: target_cfg(),
                initial_state_root: [0; 32],
            },
        );
        rollups.insert(
            target_id,
            Rollup {
                client: Arc::clone(&follower) as Arc<_>,
                session: None,
                config: target_cfg(),
                initial_state_root: [0; 32],
            },
        );

        let mut builder = CompositionBuilder::new(entry_id, rollups);
        entry
            .simulate_source_tx(Vec::new(), &mut builder)
            .await
            .expect("source sim");
        let composition = builder.finalize(&[]).await.expect("finalize");

        // Entry batch carries the top-level call as one deferred entry.
        assert_eq!(composition.source.batch.entries.len(), 1);

        // The target composition carries the inbound DA-sidecar entry.
        assert_eq!(composition.targets.len(), 1);
        assert_eq!(composition.targets[0].rollup_id, target_id);
        assert_eq!(composition.targets[0].batch.entries.len(), 1);

        // Follower saw exactly one dispatched outcome.
        let seen = follower_recorder.dispatched_outcomes();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].post_state_root(), Some(&follower_post));
    }

    #[tokio::test]
    async fn follower_fake_refuses_entry_methods() {
        use crate::executor::CommittedRootReader;
        let follower = FakeChainClient::new_follower(RollupId(1));
        let err = CommittedRootReader::stored_target_state_root(&follower, RollupId(0))
            .await
            .expect_err("follower must reject entry-only methods");
        assert!(matches!(err.kind(), ExecutorErrorKind::Unavailable(s) if s.contains("follower")));
    }
}
