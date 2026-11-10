//! Production recovery with scripted source nonces and a mock Engine API.
//! Barriers force consumer orderings; no mining, live reorg, or timing sleeps.

use super::*;
use alloy_consensus::Header;
use alloy_provider::{ProviderBuilder, RootProvider, mock::Asserter};
use eez_l1::{BatchRecord, L1CanonicalHead, L1ReaderConfig, SubmitterConfig, scan::PostId};
use reth_chainspec::ChainSpecBuilder;
use reth_engine_primitives::{BeaconEngineMessage, ConsensusEngineHandle, OnForkChoiceUpdated};
use reth_payload_builder::PayloadBuilderHandle;
use reth_primitives_traits::SealedHeader;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const SENDER: Address = Address::repeat_byte(0x11);
const BATCH: B256 = B256::repeat_byte(0xa1);

#[derive(Debug)]
struct UnusedProver;

#[async_trait]
impl eez_prover::Prover for UnusedProver {
    async fn prove(&self, _: ProvingContext) -> Result<Bytes, ProverError> {
        panic!("recovery must not invoke the prover")
    }

    fn vkey(&self) -> B256 {
        B256::ZERO
    }
}

fn header(number: u64) -> SealedHeader<Header> {
    SealedHeader::seal_slow(Header {
        number,
        ..Default::default()
    })
}

fn record(l1: u64, l2: u64, tx_hash: B256) -> BatchRecord {
    BatchRecord {
        post: PostId {
            l1_block: l1,
            l1_block_hash: B256::from(U256::from(l1)),
            log_index: 0,
        },
        tx_hash,
        last_l2_block: l2,
    }
}

struct Fixture {
    composer: Composer<MockEthProvider>,
    engine: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.engine.abort();
    }
}

impl Fixture {
    fn new(l1_provider: RootProvider) -> Self {
        let l2 = Arc::new(MockEthProvider::new());
        let evm_config = EezEvmConfig::new(Arc::new(
            ChainSpecBuilder::mainnet().cancun_activated().build(),
        ))
        .unwrap();
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let engine = tokio::spawn(async move {
            while let Some(message) = engine_rx.recv().await {
                match message {
                    BeaconEngineMessage::ForkchoiceUpdated { tx, .. } => {
                        tx.send(Ok(OnForkChoiceUpdated::valid(
                            alloy_rpc_types_engine::PayloadStatus::from_status(
                                alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                            ),
                        )))
                        .unwrap();
                    }
                    _ => panic!("recovery fixture only expects rollback FCUs"),
                }
            }
        });
        let committer = BlockCommitterHandle::spawn(
            header(10),
            header(9),
            B256::ZERO,
            ConsensusEngineHandle::new(engine_tx),
            PayloadBuilderHandle::new(mpsc::unbounded_channel().0),
            None,
        );
        let signer =
            alloy_signer_local::PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap();
        let reader = L1ReaderConfig {
            rpc_url: "http://127.0.0.1:1".parse().unwrap(),
            trace_rpc_url: "http://127.0.0.1:1".parse().unwrap(),
            eez: Address::ZERO,
            rollup_id: 1,
            deploy_block: 0,
        };
        let submitter = Submitter::new(SubmitterConfig {
            builder_rpc_url: reader.rpc_url.clone(),
            reader,
            target_rpc_url: None,
            poster: signer.clone(),
        });
        let client = crate::LocalChainClient::new_entry(
            l2.as_ref().clone(),
            evm_config.clone(),
            eez_protocol::RollupId(1),
            Address::ZERO,
            eez_protocol::ChainDialect::EvmL2Style,
        );
        let quorum = AttestationQuorum::new(
            vec![crate::attestation_quorum::QuorumMember {
                proof_system: Address::repeat_byte(1),
                attester: Address::repeat_byte(2),
                prover: Arc::new(UnusedProver),
            }],
            Duration::ZERO,
        )
        .unwrap();
        let composer = Composer::new(
            HashMap::from([(
                1,
                RollupState {
                    config: crate::RollupConfig {
                        rollup_id: 1,
                        expect_external_batches: true,
                        fee_recipient: Address::ZERO,
                    },
                    l2_provider: l2,
                    l1_head: Arc::new(L1CanonicalHead::default()),
                    held_pool: Arc::new(HeldPool::new()),
                    optimistic: Arc::new(OptimisticallyIncluded::new()),
                },
            )]),
            quorum,
            evm_config,
            CrossChainWiring {
                entry_rollup_id: eez_protocol::RollupId(1),
                rollups: HashMap::new(),
                exec_ctx: Arc::new(CrossChainExecCtx {
                    eezl2_address: Address::ZERO,
                    l2_chain_id: 1,
                    l1_provider,
                    submitter,
                    l1_poster_signer: signer,
                    l1_chain_id: 1,
                    l1_post_batch_priority_fee: 0,
                    eez_registry: Address::ZERO,
                }),
                local: crate::local::LocalComposeClients {
                    l1_entry: client.clone(),
                    l2_entry: client,
                },
            },
            committer,
            None,
            RollupTiming::new(5000, 1000, 1000, 100),
        )
        .unwrap();
        Self { composer, engine }
    }

    fn rollup(&self) -> &RollupState<MockEthProvider> {
        &self.composer.inner.rollups[&1]
    }

    fn prepare(
        &self,
        direction: Direction,
        canonical_batch: B256,
    ) -> (Vec<HeldTx>, crate::optimistic::FailedBatch) {
        let rollup = self.rollup();
        let txs: Vec<_> = (0..3u8)
            .map(|n| HeldTx {
                raw_tx: Bytes::from(vec![n]),
                hash: B256::with_last_byte(n + 1),
                attempts: if n == 0 { MAX_BUNDLE_ATTEMPTS - 1 } else { 0 },
                sender: SENDER,
                nonce: u64::from(n),
                direction,
                max_fee_per_gas: 1,
                priority_fee_per_gas: 1,
            })
            .collect();
        for tx in &txs {
            rollup.held_pool.push_contiguous(tx.clone(), 0).unwrap();
        }
        // One suffix is in the batch and one remains queued outside it.
        rollup
            .optimistic
            .begin(10, BATCH, header(9), rollup.held_pool.pop_n(2));
        rollup
            .l1_head
            .append(record(100, 9, B256::repeat_byte(0x90)));
        rollup.l1_head.append(record(101, 10, canonical_batch));
        rollup.optimistic.resolve_below_cursor(10);
        rollup
            .l2_provider
            .add_account(SENDER, ExtendedAccount::new(1, U256::ZERO));
        let partial = rollup.optimistic.partial_to_dispose(10).unwrap();
        (txs, partial)
    }

    /// The real shared index mutation and committer FCU used by Deriver rollback.
    /// Caller holds begin_reconcile, as the Deriver does; Engine replies are mocked.
    async fn retreat_deriver(&self) {
        assert_eq!(self.rollup().l1_head.retreat_on_l1_reorg(100).unwrap().0, 9);
        self.composer
            .inner
            .committer
            .reorg_to(header(9))
            .await
            .unwrap();
        self.rollup()
            .l2_provider
            .add_account(SENDER, ExtendedAccount::new(0, U256::ZERO));
        assert_eq!(self.composer.inner.committer.last_header().number(), 9);
    }

    fn notify_composer(&self) {
        self.composer.inner.on_l1_event(
            &Ok(L1Event::Reorg {
                common_ancestor_number: 100,
                common_ancestor_hash: record(100, 9, B256::ZERO).post.l1_block_hash,
                old_head_hash: record(101, 10, BATCH).post.l1_block_hash,
                new_head_number: 101,
                new_head_hash: B256::repeat_byte(0xff),
            }),
            Address::ZERO,
        );
    }

    fn assert_requeued(&self, expected: &[HeldTx]) {
        let queued = self.rollup().held_pool.pop_all();
        let on_chain = expected[0].nonce;
        let identity = |txs: &[HeldTx]| {
            txs.iter()
                .map(|tx| (tx.hash, tx.sender, tx.direction, tx.nonce, tx.attempts))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            identity(&queued),
            identity(expected),
            "no loss or retry charges"
        );
        // Drained transactions reserve exactly their original source nonces.
        for tx in expected {
            let mut replacement = tx.clone();
            replacement.hash = B256::repeat_byte(0xee);
            assert!(
                self.rollup()
                    .held_pool
                    .push_contiguous(replacement, on_chain)
                    .is_err()
            );
        }
        self.rollup().held_pool.release_in_flight_batch(&queued);
        // No reservation is stranded after the recovered transactions are released.
        for tx in expected {
            self.rollup()
                .held_pool
                .push_contiguous(tx.clone(), on_chain)
                .unwrap();
        }
    }
}

#[tokio::test]
async fn deriver_first_retreat_preserves_inbound_at_retry_limit() {
    deriver_first_retreat(Direction::Inbound).await;
}

#[tokio::test]
async fn deriver_first_retreat_preserves_outbound_at_retry_limit() {
    deriver_first_retreat(Direction::Outbound).await;
}

async fn deriver_first_retreat(direction: Direction) {
    let rpc = Asserter::new();
    rpc.push_success(&"0x0"); // Surviving ancestor has not consumed nonce 0.
    let fixture = Fixture::new(ProviderBuilder::default().connect_mocked_client(rpc));
    let (txs, partial) = fixture.prepare(direction, BATCH);
    let guard = fixture.composer.inner.committer.begin_reconcile().await;
    fixture.retreat_deriver().await;
    // The stale selection survives the Deriver retreat. Composer has not
    // received its own Reorg notification yet. Start recovery before the
    // Deriver releases its guard, so this also checks the shared lock.
    let recovery = fixture
        .composer
        .recover_partial_batch(1, fixture.rollup(), partial);
    tokio::pin!(recovery);
    let completed_during_retreat = futures_util::poll!(&mut recovery).is_ready();
    drop(guard);
    if !completed_during_retreat {
        recovery.await;
    }
    fixture.notify_composer();
    assert!(fixture.rollup().optimistic.take_rolled_out(9).is_empty());
    fixture.assert_requeued(&txs);
    assert!(
        !completed_during_retreat,
        "recovery bypassed the Deriver's guard"
    );
}

/// Stop a real nonce RPC at its response, so tests control the read/rollback ordering.
async fn nonce_barrier() -> (
    RootProvider,
    jsonrpsee::server::ServerHandle,
    mpsc::UnboundedReceiver<serde_json::Value>,
    oneshot::Sender<u64>,
) {
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let mut module = jsonrpsee::RpcModule::new((seen_tx, tokio::sync::Mutex::new(Some(reply_rx))));
    module
        .register_async_method("eth_getTransactionCount", |params, ctx, _| async move {
            let request: serde_json::Value = params.parse().unwrap();
            ctx.0.send(request).unwrap();
            let nonce = ctx.1.lock().await.take().unwrap().await.unwrap();
            Ok::<_, jsonrpsee::types::ErrorObjectOwned>(format!("{nonce:#x}"))
        })
        .unwrap();
    let server = jsonrpsee::server::ServerBuilder::default()
        .build("127.0.0.1:0")
        .await
        .unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let handle = server.start(module);
    (
        ProviderBuilder::default().connect_http(url.parse().unwrap()),
        handle,
        seen_rx,
        reply_tx,
    )
}

#[tokio::test]
async fn deriver_retreat_during_nonce_read_preserves_the_batch() {
    retreat_during_nonce_read(false).await;
}

#[tokio::test]
async fn same_height_replacement_invalidates_pending_nonce_reads() {
    retreat_during_nonce_read(true).await;
}

async fn retreat_during_nonce_read(replace: bool) {
    let (provider, server, mut seen, reply) = nonce_barrier().await;
    let fixture = Fixture::new(provider);
    let (txs, partial) = fixture.prepare(Direction::Inbound, BATCH);
    let recovery = fixture
        .composer
        .recover_partial_batch(1, fixture.rollup(), partial);
    tokio::pin!(recovery);
    let request = tokio::select! {
        request = seen.recv() => request.unwrap(),
        () = &mut recovery => panic!("recovery must wait for the nonce response"),
    };
    assert_eq!(
        serde_json::from_value::<alloy_eips::BlockId>(request[1].clone()).unwrap(),
        alloy_eips::BlockId::hash(record(101, 10, BATCH).post.l1_block_hash)
    );
    // Deriver is allowed to finish while the RPC is pending. Composer's own
    // Reorg notification stays delayed until after the stale read returns.
    {
        let _guard = fixture.composer.inner.committer.begin_reconcile().await;
        fixture.retreat_deriver().await;
        if replace {
            fixture
                .rollup()
                .l1_head
                .append(record(102, 10, B256::repeat_byte(0xb2)));
            fixture
                .composer
                .inner
                .committer
                .reorg_to(header(10))
                .await
                .unwrap();
            assert_eq!(fixture.rollup().l1_head.cursor(), 10);
        }
    }
    reply.send(0).unwrap();
    recovery.await;
    fixture.notify_composer();
    fixture.assert_requeued(&txs);
    server.stop().unwrap();
    server.stopped().await;
}

#[tokio::test]
async fn composer_first_reorg_invalidates_pending_partial_disposition() {
    let (provider, server, mut seen, reply) = nonce_barrier().await;
    let fixture = Fixture::new(provider);
    let (txs, partial) = fixture.prepare(Direction::Inbound, BATCH);
    let recovery = fixture
        .composer
        .recover_partial_batch(1, fixture.rollup(), partial);
    tokio::pin!(recovery);
    tokio::select! {
        request = seen.recv() => { assert!(request.is_some()); },
        () = &mut recovery => panic!("recovery must wait for the nonce response"),
    }
    fixture.notify_composer();
    reply.send(0).unwrap(); // A stale unspent verdict must not charge the restored tx.
    recovery.await;
    {
        let _guard = fixture.composer.inner.committer.begin_reconcile().await;
        fixture.retreat_deriver().await;
    }
    fixture.assert_requeued(&txs);
    server.stop().unwrap();
    server.stopped().await;
}

#[tokio::test]
async fn lost_races_preserve_inbound_attempts_and_nonce_suffix() {
    lost_races(Direction::Inbound).await;
}

#[tokio::test]
async fn lost_races_preserve_outbound_attempts_and_nonce_suffix() {
    lost_races(Direction::Outbound).await;
}

async fn lost_races(direction: Direction) {
    let rpc = Asserter::new();
    let fixture = Fixture::new(ProviderBuilder::default().connect_mocked_client(rpc.clone()));
    let winner = B256::repeat_byte(0xb2);
    let (txs, _) = fixture.prepare(direction, winner);
    fixture
        .rollup()
        .l2_provider
        .add_account(SENDER, ExtendedAccount::new(0, U256::ZERO));
    // Every lost race must preserve even a transaction with two prior attempts.
    // Exercise more races than the eviction bound, with both a batched and a
    // queued nonce suffix. Canonical winners never include these transactions.
    for round in 0..=MAX_BUNDLE_ATTEMPTS {
        rpc.push_success(&"0x0");
        if round > 0 {
            let height = 10 + u64::from(round);
            let rollup = fixture.rollup();
            let _guard = fixture.composer.inner.committer.begin_reconcile().await;
            fixture
                .composer
                .inner
                .committer
                .reorg_to(header(height))
                .await
                .unwrap();
            rollup
                .optimistic
                .begin(height, BATCH, header(height - 1), rollup.held_pool.pop_n(2));
            rollup
                .l1_head
                .append(record(101 + u64::from(round), height, winner));
            assert!(rollup.optimistic.resolve_below_cursor(height).is_empty());
        }
        let partial = fixture
            .rollup()
            .optimistic
            .partial_to_dispose(10 + u64::from(round))
            .unwrap();
        fixture
            .composer
            .recover_partial_batch(1, fixture.rollup(), partial)
            .await;
        fixture.assert_requeued(&txs);
        // All transactions are back in the pool; no ledger copy may restore a
        // duplicate on a subsequent reorg.
        assert!(fixture.rollup().optimistic.take_rolled_out(9).is_empty());
    }
}

#[tokio::test]
async fn own_partial_batch_still_charges_the_culprit() {
    for direction in [Direction::Inbound, Direction::Outbound] {
        let rpc = Asserter::new();
        rpc.push_success(&"0x0");
        let fixture = Fixture::new(ProviderBuilder::default().connect_mocked_client(rpc));
        let (txs, partial) = fixture.prepare(direction, BATCH);
        fixture
            .rollup()
            .l2_provider
            .add_account(SENDER, ExtendedAccount::new(0, U256::ZERO));
        fixture
            .composer
            .recover_partial_batch(1, fixture.rollup(), partial)
            .await;
        assert!(
            fixture.rollup().held_pool.is_empty(),
            "an attributable third failure still evicts the nonce suffix"
        );
        assert!(fixture.rollup().optimistic.take_rolled_out(9).is_empty());
        let mut replacement = txs[0].clone();
        replacement.hash = B256::repeat_byte(0xee);
        fixture
            .rollup()
            .held_pool
            .push_contiguous(replacement, 0)
            .unwrap();
    }
}

#[tokio::test]
async fn lost_race_retains_spent_transactions_for_reorg_without_charging_the_tail() {
    for direction in [Direction::Inbound, Direction::Outbound] {
        let rpc = Asserter::new();
        rpc.push_success(&"0x1"); // The winner consumed nonce 0, but not nonce 1.
        let fixture = Fixture::new(ProviderBuilder::default().connect_mocked_client(rpc));
        let (txs, partial) = fixture.prepare(direction, B256::repeat_byte(0xb2));
        fixture
            .composer
            .recover_partial_batch(1, fixture.rollup(), partial)
            .await;
        fixture.assert_requeued(&txs[1..]);
        fixture.notify_composer();
        fixture.assert_requeued(&txs);
    }
}
