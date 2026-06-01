//! Integration test for [`compose_transaction`] against a minimal mock
//! `ChainProtocol` impl. Validates the happy path and the error-path
//! early returns (empty dispatch, dispatch to unregistered target)
//! without depending on `crosschain-evm`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crosschain_protocol::{
    ChainClient, ChainProtocol, CompositionErrorKind, Dispatcher, EntryChainClient,
    ExecutionOutcome, ExecutionRequest, ExecutionResponse, ExecutorResult, ProtocolErrorKind,
    ProtocolResult, ProxyLookupConfig, RecordedCall, Rollup, RollupId, TargetBatchSimulation,
    TargetConfig, TargetExecutionSession, TargetTransaction, DEFAULT_CCM_GAS_LIMIT,
};

// ── Minimal mock protocol ──────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
struct MockProtocol;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct MockEntry {
    rollup_id: RollupId,
    pre: [u8; 32],
    post: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct MockOverlay;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct MockWitness;

impl ChainProtocol for MockProtocol {
    type Address = u64;
    type Value = u64;
    type Calldata = Vec<u8>;
    type Batch = Vec<MockEntry>;
    type Overlay = MockOverlay;
    type Witness = MockWitness;
    type Dialect = ();

    fn build_batch(
        &self,
        recorded: &[RecordedCall<Self>],
        attribution: &crosschain_protocol::SourceAttribution<'_>,
        _dialect: &Self::Dialect,
        _source_rollup_id: RollupId,
        _raw_tx: &[u8],
    ) -> ProtocolResult<Self::Batch> {
        let initial_roots = attribution.initial_roots;
        Ok(recorded
            .iter()
            .map(|c| {
                let pre = *initial_roots.get(&c.original_rollup_id).unwrap_or(&[0; 32]);
                MockEntry {
                    rollup_id: c.original_rollup_id,
                    pre,
                    post: c.outcome.post_state_root().copied().unwrap_or([0; 32]),
                }
            })
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
        _call: &RecordedCall<Self>,
        _source_rollup_id: RollupId,
        _raw_tx: &[u8],
        _dialect: &Self::Dialect,
    ) -> Vec<u8> {
        vec![]
    }
    fn encode_address(&self, addr: &u64) -> Vec<u8> {
        addr.to_le_bytes().to_vec()
    }
    fn decode_address(&self, bytes: &[u8]) -> ProtocolResult<u64> {
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_err| {
            ProtocolErrorKind::InvalidEncoding("bad address".into())
        })?))
    }
    fn encode_value(&self, val: &u64) -> Vec<u8> {
        val.to_le_bytes().to_vec()
    }
    fn decode_value(&self, bytes: &[u8]) -> ProtocolResult<u64> {
        Ok(u64::from_le_bytes(bytes.try_into().map_err(|_err| {
            ProtocolErrorKind::InvalidEncoding("bad value".into())
        })?))
    }
    fn encode_calldata(&self, data: &Vec<u8>) -> Vec<u8> {
        data.clone()
    }
    fn decode_calldata(&self, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

// ── Mock target session with a pre-seeded outcome queue ────────

struct MockSession {
    outcomes: VecDeque<ExecutionOutcome>,
}

#[async_trait]
impl TargetExecutionSession for MockSession {
    type Protocol = MockProtocol;

    async fn execute(
        &mut self,
        _req: ExecutionRequest<Self::Protocol>,
        _dispatcher: &mut (dyn Dispatcher<Protocol = Self::Protocol> + Send),
    ) -> ExecutorResult<ExecutionResponse<Self::Protocol>> {
        let outcome = self
            .outcomes
            .pop_front()
            .expect("test seeded enough outcomes");
        Ok(ExecutionResponse {
            outcome,
            checkpoint: crosschain_protocol::ExecutionCheckpoint {
                version: 0,
                chain_id: 0,
                base_block_number: 0,
                base_block_hash: [0; 32],
                base_state_root: [0; 32],
                current_root: [0; 32],
                overlay: MockOverlay,
                witness: None,
            },
        })
    }

    async fn checkpoint(&mut self) -> ExecutorResult<crosschain_protocol::SessionSnapshot> {
        Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
    }

    async fn rollback(
        &mut self,
        _snap: crosschain_protocol::SessionSnapshot,
    ) -> ExecutorResult<()> {
        Ok(())
    }

    async fn take_checkpoint(
        &mut self,
    ) -> Option<crosschain_protocol::ProtocolCheckpoint<Self::Protocol>> {
        None
    }
}

// ── Mock ChainClient that opens a MockSession with queued outcomes ──

struct MockClient {
    outcomes: Mutex<VecDeque<ExecutionOutcome>>,
    /// CCM-verify `final_state_root` returned from
    /// `simulate_transactions`. Finalize patches the terminal recorded
    /// call's `post_state_root` with this value, so tests that assert
    /// specific `post_state_root` entries in the final composition must
    /// configure this to match the expected post-CCM value.
    ccm_final_root: [u8; 32],
}

impl MockClient {
    fn new(outcomes: Vec<ExecutionOutcome>, ccm_final_root: [u8; 32]) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into()),
            ccm_final_root,
        }
    }
}

#[async_trait]
impl ChainClient for MockClient {
    type Protocol = MockProtocol;

    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = MockProtocol> + Send>> {
        let outcomes = std::mem::take(&mut *self.outcomes.lock().unwrap());
        Ok(Box::new(MockSession { outcomes }))
    }

    async fn simulate_transactions(
        &self,
        txs: &[TargetTransaction<MockProtocol>],
    ) -> ExecutorResult<TargetBatchSimulation> {
        Ok(TargetBatchSimulation {
            final_state_root: self.ccm_final_root,
            per_tx_roots: vec![self.ccm_final_root; txs.len().max(1)],
        })
    }

    async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
        Ok([0u8; 32])
    }
}

// ── Mock EntryChainClient: dispatches a canned list of (rollup, request) ──

struct MockEntryClient {
    entry_rollup_id: RollupId,
    dispatches: Mutex<Vec<(RollupId, ExecutionRequest<MockProtocol>)>>,
    stored_roots: HashMap<RollupId, [u8; 32]>,
}

impl MockEntryClient {
    fn new(
        entry_rollup_id: RollupId,
        dispatches: Vec<(RollupId, ExecutionRequest<MockProtocol>)>,
    ) -> Self {
        Self {
            entry_rollup_id,
            dispatches: Mutex::new(dispatches),
            stored_roots: HashMap::new(),
        }
    }
}

#[async_trait]
impl ChainClient for MockEntryClient {
    type Protocol = MockProtocol;

    async fn begin_execution_session(
        &self,
    ) -> ExecutorResult<Box<dyn TargetExecutionSession<Protocol = MockProtocol> + Send>> {
        // The entry rollup's session is opened only when the overlay
        // path needs to materialize state on the entry chain (nested
        // L1→L2→L1 reentry). The flat-composition tests in this file
        // never trigger that path, so hitting it here is a test-setup
        // bug.
        unimplemented!("entry session should not be opened in this test path")
    }

    async fn simulate_transactions(
        &self,
        txs: &[TargetTransaction<MockProtocol>],
    ) -> ExecutorResult<TargetBatchSimulation> {
        Ok(TargetBatchSimulation {
            final_state_root: [0; 32],
            per_tx_roots: vec![[0; 32]; txs.len().max(1)],
        })
    }

    async fn current_state_root(&self) -> ExecutorResult<[u8; 32]> {
        Ok(self
            .stored_roots
            .get(&self.entry_rollup_id)
            .copied()
            .unwrap_or([0; 32]))
    }
}

#[async_trait]
impl EntryChainClient for MockEntryClient {
    async fn simulate_source_tx(
        &self,
        _raw_tx: Vec<u8>,
        dispatcher: &mut (dyn Dispatcher<Protocol = MockProtocol> + Send),
    ) -> ExecutorResult<()> {
        let dispatches = std::mem::take(&mut *self.dispatches.lock().unwrap());
        for (rollup_id, req) in dispatches {
            // For top-level dispatches detected by the source inspector,
            // `caller_id` is always the entry rollup. Nested dispatches
            // from target-session inspectors carry their own session's
            // rollup id as caller; this mock only models the top-level
            // path.
            dispatcher
                .dispatch_call(rollup_id, self.entry_rollup_id, req)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl crosschain_protocol::CommittedRootReader for MockEntryClient {
    async fn stored_target_state_root(&self, rollup_id: RollupId) -> ExecutorResult<[u8; 32]> {
        Ok(self
            .stored_roots
            .get(&rollup_id)
            .copied()
            .unwrap_or([0; 32]))
    }
}

// ── Helpers ────────────────────────────────────────────────────

fn req_from(source_rollup: RollupId) -> ExecutionRequest<MockProtocol> {
    ExecutionRequest {
        destination: 1,
        calldata: vec![],
        value: 0,
        source_address: 2,
        source_rollup,
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

fn target_config() -> TargetConfig<MockProtocol> {
    TargetConfig {
        ccm_address: 0,
        system_address: 0,
        ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
        proxy_lookup: ProxyLookupConfig {
            contract_address: 0,
            authorized_proxies_slot: 0,
        },
        dialect: (),
    }
}

fn rollup_map(
    entry_id: RollupId,
    entry_client: Arc<dyn ChainClient<Protocol = MockProtocol> + Send + Sync>,
    followers: Vec<(RollupId, Vec<ExecutionOutcome>, [u8; 32])>,
) -> HashMap<RollupId, Rollup<MockProtocol>> {
    let mut map = HashMap::new();
    map.insert(
        entry_id,
        Rollup {
            client: entry_client,
            session: None,
            config: target_config(),
            initial_state_root: [0; 32],
        },
    );
    for (id, outcomes, ccm_final_root) in followers {
        map.insert(
            id,
            Rollup {
                client: Arc::new(MockClient::new(outcomes, ccm_final_root)),
                session: None,
                config: target_config(),
                initial_state_root: [0; 32],
            },
        );
    }
    map
}

// ── Tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn compose_transaction_happy_path() {
    let source_id = RollupId(0);
    let target_id = RollupId(1);
    let entry = Arc::new(MockEntryClient::new(
        source_id,
        vec![
            (target_id, req_from(source_id)),
            (target_id, req_from(source_id)),
        ],
    ));
    let entry_as_chain: Arc<dyn ChainClient<Protocol = MockProtocol> + Send + Sync> =
        Arc::clone(&entry) as Arc<_>;
    let rollups = rollup_map(
        source_id,
        entry_as_chain,
        // ccm_final_root = [2; 32] so the CCM-verify patch matches
        // the terminal outcome's post_state_root (no observable
        // patching — same value before and after).
        vec![(target_id, vec![outcome([1; 32]), outcome([2; 32])], [2; 32])],
    );

    let composition = crosschain_protocol::compose_transaction(
        &MockProtocol,
        entry.as_ref(),
        &[],
        source_id,
        rollups,
    )
    .await
    .expect("compose");

    assert_eq!(composition.source.batch.len(), 2);
    assert_eq!(composition.source.batch[0].post, [1; 32]);
    assert_eq!(composition.source.batch[1].post, [2; 32]);
}

#[tokio::test]
async fn compose_transaction_no_dispatches_errors() {
    // Source simulation dispatches nothing — finalize should reject
    // with EmptyCalls.
    let source_id = RollupId(0);
    let entry = Arc::new(MockEntryClient::new(source_id, vec![]));
    let entry_as_chain: Arc<dyn ChainClient<Protocol = MockProtocol> + Send + Sync> =
        Arc::clone(&entry) as Arc<_>;
    let rollups = rollup_map(
        source_id,
        entry_as_chain,
        vec![(RollupId(1), vec![], [0; 32])],
    );

    match crosschain_protocol::compose_transaction(
        &MockProtocol,
        entry.as_ref(),
        &[],
        source_id,
        rollups,
    )
    .await
    {
        Err(e)
            if matches!(
                e.kind(),
                CompositionErrorKind::Protocol(p)
                    if matches!(p.kind(), ProtocolErrorKind::EmptyCalls)
            ) => {}
        other => panic!("expected EmptyCalls, got {other:?}"),
    }
}

#[tokio::test]
async fn compose_transaction_dispatch_to_unregistered_target_errors() {
    // Entry dispatches to rollup 2; rollups only cover entry + rollup 1.
    // dispatch_call returns ExecutorErrorKind::Unavailable, which
    // propagates as CompositionErrorKind::Executor.
    let source_id = RollupId(0);
    let entry = Arc::new(MockEntryClient::new(
        source_id,
        vec![(RollupId(2), req_from(source_id))],
    ));
    let entry_as_chain: Arc<dyn ChainClient<Protocol = MockProtocol> + Send + Sync> =
        Arc::clone(&entry) as Arc<_>;
    let rollups = rollup_map(
        source_id,
        entry_as_chain,
        vec![(RollupId(1), vec![], [0; 32])],
    );

    match crosschain_protocol::compose_transaction(
        &MockProtocol,
        entry.as_ref(),
        &[],
        source_id,
        rollups,
    )
    .await
    {
        Err(e) if matches!(e.kind(), CompositionErrorKind::Executor(_)) => {}
        other => panic!("expected Executor error, got {other:?}"),
    }
}
