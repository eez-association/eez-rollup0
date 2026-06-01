//! `FakeChainClient` — the canonical test double for
//! [`ChainClient`](crate::ChainClient) + [`EntryChainClient`](crate::EntryChainClient).
//!
//! # Scripted hooks
//!
//! - [`with_session_outcomes`](FakeChainClient::with_session_outcomes)
//!   — the queue of `ExecutionOutcome`s the spawned session returns,
//!   one per call.
//! - [`with_simulate_transactions_results`](FakeChainClient::with_simulate_transactions_results)
//!   — CCM-verify batch results, one per call.
//! - [`with_simulate_source_hook`](FakeChainClient::with_simulate_source_hook)
//!   — the closure that drives `simulate_source_tx` (entry-only).
//! - [`with_stored_root`](FakeChainClient::with_stored_root) — seed one
//!   `stored_target_state_root` lookup result (entry-only).
//! - [`with_checkpoint_factory`](FakeChainClient::with_checkpoint_factory)
//!   — how the spawned session synthesizes its `ExecutionCheckpoint<P>`.
//!   Required for any test that calls `session.execute(...)`.
//!
//! # Assertions
//!
//! - [`dispatched_outcomes`](FakeChainClient::dispatched_outcomes) —
//!   one entry per `session.execute` call.
//! - [`simulated_batches`](FakeChainClient::simulated_batches) — one
//!   entry per `simulate_transactions` call.
//!
//! # Default behavior
//!
//! Every method returns [`ExecutorErrorKind::Unavailable`] when its
//! queue or hook is unset — loud, structured, easy to diagnose. Never
//! `unimplemented!()`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::composition::Dispatcher;
use crate::error::{ExecutorError, ExecutorErrorKind, ExecutorResult};
use crate::executor::{
    ChainClient, EntryChainClient, ExecutionRequest, ExecutionResponse, ProtocolCheckpoint,
    TargetBatchSimulation, TargetExecutionSession, TargetTransaction,
};
use crate::protocol::ChainProtocol;
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

type SimulateSourceHook<P> = Box<
    dyn FnMut(Vec<u8>, &mut (dyn Dispatcher<Protocol = P> + Send)) -> ExecutorResult<()> + Send,
>;

type CheckpointFactory<P> = Box<dyn FnMut() -> ProtocolCheckpoint<P> + Send>;

/// Canonical test double for `ChainClient` + `EntryChainClient`.
///
/// See module docs for the scripted-hook and assertion surface.
pub struct FakeChainClient<P: ChainProtocol + 'static> {
    rollup_id: RollupId,
    role: FakeRole,

    // Session state: shared with the spawned `FakeChainSession`
    // through Arc<Mutex<_>>. The session consumes outcomes; the
    // client observes dispatched_outcomes after the composition runs.
    session_outcomes: Arc<Mutex<VecDeque<ExecutionOutcome>>>,
    dispatched_outcomes: Arc<Mutex<Vec<ExecutionOutcome>>>,
    checkpoint_factory: Arc<Mutex<Option<CheckpointFactory<P>>>>,

    // Client-only scripted queues.
    simulate_transactions_results: Mutex<VecDeque<ExecutorResult<TargetBatchSimulation>>>,
    simulate_source_hook: Mutex<Option<SimulateSourceHook<P>>>,
    stored_roots: Mutex<HashMap<RollupId, [u8; 32]>>,

    // Client-side recorders.
    simulated_batches: Mutex<Vec<Vec<TargetTransaction<P>>>>,
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for FakeChainClient<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeChainClient")
            .field("rollup_id", &self.rollup_id)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl<P: ChainProtocol + 'static> FakeChainClient<P> {
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
            simulate_transactions_results: Mutex::new(VecDeque::new()),
            simulate_source_hook: Mutex::new(None),
            stored_roots: Mutex::new(HashMap::new()),
            simulated_batches: Mutex::new(Vec::new()),
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

    /// Seed the `simulate_transactions` result queue.
    #[must_use]
    pub fn with_simulate_transactions_results<
        I: IntoIterator<Item = ExecutorResult<TargetBatchSimulation>>,
    >(
        self,
        results: I,
    ) -> Self {
        *self
            .simulate_transactions_results
            .lock()
            .expect("fake mutex poisoned") = results.into_iter().collect();
        self
    }

    /// Install the `simulate_source_tx` hook (entry-only).
    #[must_use]
    pub fn with_simulate_source_hook<
        F: FnMut(Vec<u8>, &mut (dyn Dispatcher<Protocol = P> + Send)) -> ExecutorResult<()>
            + Send
            + 'static,
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
    pub fn with_checkpoint_factory<F: FnMut() -> ProtocolCheckpoint<P> + Send + 'static>(
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

    /// Snapshot of batches passed to `simulate_transactions`.
    #[must_use]
    pub fn simulated_batches(&self) -> Vec<Vec<TargetTransaction<P>>> {
        self.simulated_batches
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
impl<P: ChainProtocol + 'static> ChainClient for FakeChainClient<P> {
    type Protocol = P;

    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = P> + Send>> {
        Ok(Box::new(FakeChainSession {
            outcomes: Arc::clone(&self.session_outcomes),
            dispatched_outcomes: Arc::clone(&self.dispatched_outcomes),
            checkpoint_factory: Arc::clone(&self.checkpoint_factory),
            rollup_id: self.rollup_id,
        }))
    }

    async fn simulate_transactions(
        &self,
        txs: &[TargetTransaction<P>],
    ) -> ExecutorResult<TargetBatchSimulation> {
        self.simulated_batches
            .lock()
            .expect("fake mutex poisoned")
            .push(txs.to_vec());
        self.simulate_transactions_results
            .lock()
            .expect("fake mutex poisoned")
            .pop_front()
            .unwrap_or_else(|| Err(self.unavailable("simulate_transactions")))
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
impl<P: ChainProtocol + 'static> EntryChainClient for FakeChainClient<P> {
    async fn simulate_source_tx(
        &self,
        raw_tx: Vec<u8>,
        dispatcher: &mut (dyn Dispatcher<Protocol = Self::Protocol> + Send),
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
impl<P: ChainProtocol + 'static> crate::executor::CommittedRootReader for FakeChainClient<P> {
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
pub struct FakeChainSession<P: ChainProtocol + 'static> {
    outcomes: Arc<Mutex<VecDeque<ExecutionOutcome>>>,
    dispatched_outcomes: Arc<Mutex<Vec<ExecutionOutcome>>>,
    checkpoint_factory: Arc<Mutex<Option<CheckpointFactory<P>>>>,
    rollup_id: RollupId,
}

impl<P: ChainProtocol + 'static> std::fmt::Debug for FakeChainSession<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeChainSession")
            .field("rollup_id", &self.rollup_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<P: ChainProtocol + 'static> TargetExecutionSession for FakeChainSession<P> {
    type Protocol = P;

    async fn execute(
        &mut self,
        _req: ExecutionRequest<Self::Protocol>,
        _dispatcher: &mut (dyn Dispatcher<Protocol = Self::Protocol> + Send),
    ) -> ExecutorResult<ExecutionResponse<Self::Protocol>> {
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
                    )))
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

    async fn take_checkpoint(&mut self) -> Option<ProtocolCheckpoint<Self::Protocol>> {
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
    use crate::checkpoint::ExecutionCheckpoint;
    use crate::compose::compose_transaction;
    use crate::composer::{ProxyLookupConfig, TargetConfig, DEFAULT_CCM_GAS_LIMIT};
    use crate::composition::Rollup;
    use crate::error::ProtocolResult;
    use crate::types::RecordedCall;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, Default)]
    struct SmokeProto;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct SmokeState;

    impl ChainProtocol for SmokeProto {
        type Address = [u8; 20];
        type Value = u128;
        type Calldata = Vec<u8>;
        // Batch = per-call post_state_roots so tests can inspect what
        // landed in the final composition.
        type Batch = Vec<[u8; 32]>;
        type Overlay = SmokeState;
        type Witness = SmokeState;
        type Dialect = ();

        fn build_batch(
            &self,
            recorded: &[RecordedCall<Self>],
            _attribution: &crate::composer::SourceAttribution<'_>,
            _dialect: &Self::Dialect,
            _source_rollup_id: RollupId,
            _raw_tx: &[u8],
        ) -> ProtocolResult<Self::Batch> {
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
        fn encode_follower_trigger(
            &self,
            _: &RecordedCall<Self>,
            _: RollupId,
            _: &[u8],
            (): &Self::Dialect,
        ) -> Vec<u8> {
            vec![]
        }
        fn encode_address(&self, a: &Self::Address) -> Vec<u8> {
            a.to_vec()
        }
        fn decode_address(&self, b: &[u8]) -> ProtocolResult<Self::Address> {
            b.try_into().map_err(|_e| {
                crate::error::ProtocolErrorKind::InvalidEncoding("addr".into()).into()
            })
        }
        fn encode_value(&self, v: &Self::Value) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        fn decode_value(&self, b: &[u8]) -> ProtocolResult<Self::Value> {
            b.try_into()
                .map(u128::from_be_bytes)
                .map_err(|_e| crate::error::ProtocolErrorKind::InvalidEncoding("val".into()).into())
        }
        fn encode_calldata(&self, d: &Self::Calldata) -> Vec<u8> {
            d.clone()
        }
        fn decode_calldata(&self, b: &[u8]) -> ProtocolResult<Self::Calldata> {
            Ok(b.to_vec())
        }
    }

    fn outcome(post: [u8; 32]) -> ExecutionOutcome {
        ExecutionOutcome::Resolved {
            return_data: vec![],
            pre_state_root: [0; 32],
            post_state_root: post,
            gas_used: 1,
            success: true,
        }
    }

    fn target_cfg() -> TargetConfig<SmokeProto> {
        TargetConfig {
            ccm_address: [0; 20],
            system_address: [0; 20],
            ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
            proxy_lookup: ProxyLookupConfig {
                contract_address: [0; 20],
                authorized_proxies_slot: 0,
            },
            dialect: (),
        }
    }

    #[tokio::test]
    async fn fake_chain_client_drives_end_to_end_composition() {
        let entry_id = RollupId(0);
        let target_id = RollupId(1);

        // Follower fake: session returns one outcome; CCM-verify batch
        // returns a matching final root so the terminal post_state_root
        // patching is observable (and idempotent with the test assertion).
        let follower_post = [0x22; 32];
        let follower = Arc::new(
            FakeChainClient::<SmokeProto>::new_follower(target_id)
                .with_session_outcomes(vec![outcome(follower_post)])
                .with_simulate_transactions_results(vec![Ok(TargetBatchSimulation {
                    final_state_root: follower_post,
                    per_tx_roots: vec![follower_post, follower_post],
                })])
                .with_checkpoint_factory(move || ExecutionCheckpoint {
                    version: 1,
                    chain_id: 1,
                    base_block_number: 0,
                    base_block_hash: [0; 32],
                    base_state_root: [0; 32],
                    current_root: follower_post,
                    overlay: SmokeState,
                    witness: None,
                }),
        );
        let follower_recorder = Arc::clone(&follower);

        // Entry fake: hook dispatches one call to the follower, the
        // composition pipeline routes it through the shared dispatcher.
        let follower_for_hook = Arc::clone(&follower);
        let entry = Arc::new(
            FakeChainClient::<SmokeProto>::new_entry(entry_id)
                .with_stored_root(target_id, [0; 32])
                .with_stored_root(entry_id, [0; 32])
                .with_simulate_source_hook(move |_raw, dispatcher| {
                    let _follower = Arc::clone(&follower_for_hook);
                    let req = ExecutionRequest {
                        destination: [0xAB; 20],
                        calldata: vec![],
                        value: 0,
                        source_address: [0; 20],
                        source_rollup: entry_id,
                    };
                    // The real inspector bridges sync → async via a
                    // scoped OS thread + `Handle::block_on` (see
                    // `crosschain-evm-composer`). Mirror that here so
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
        let entry_as_chain: Arc<dyn ChainClient<Protocol = SmokeProto> + Send + Sync> =
            Arc::clone(&entry) as Arc<_>;
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

        let composition = compose_transaction(&SmokeProto, entry.as_ref(), &[], entry_id, rollups)
            .await
            .expect("compose");

        // Source batch carries the dispatched call's post_state_root.
        assert_eq!(composition.source.batch.len(), 1);
        assert_eq!(composition.source.batch[0], follower_post);

        // Follower saw exactly one dispatched outcome + one CCM batch.
        let seen = follower_recorder.dispatched_outcomes();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].post_state_root(), Some(&follower_post));
        assert_eq!(follower_recorder.simulated_batches().len(), 1);
        assert_eq!(
            follower_recorder.simulated_batches()[0].len(),
            2,
            "CCM verify submits a 2-tx load + execute batch"
        );
    }

    #[tokio::test]
    async fn follower_fake_refuses_entry_methods() {
        use crate::executor::CommittedRootReader;
        let follower = FakeChainClient::<SmokeProto>::new_follower(RollupId(1));
        let err = CommittedRootReader::stored_target_state_root(&follower, RollupId(0))
            .await
            .expect_err("follower must reject entry-only methods");
        assert!(matches!(err.kind(), ExecutorErrorKind::Unavailable(s) if s.contains("follower")));
    }
}
