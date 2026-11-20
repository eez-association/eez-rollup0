use super::*;
use alloy_consensus::Header;
use eez_l1::L1ReaderConfig;
use reth_chainspec::ChainSpecBuilder;
use reth_engine_primitives::{BeaconEngineMessage, ConsensusEngineHandle, OnForkChoiceUpdated};
use reth_payload_builder::PayloadBuilderHandle;
use reth_provider::test_utils::MockEthProvider;
use std::sync::atomic::AtomicU8;
use tokio::sync::mpsc;

/// The seed canonical L1 serves after the reorg.
const SEED: B256 = B256::repeat_byte(0xaa);
/// The seed the unsafe head copied before the reorg.
const OLD_SEED: B256 = B256::repeat_byte(0x99);

/// Every L1 request returns block 10 at t=120 carrying [`SEED`].
#[derive(Default)]
struct MockL1 {
    unavailable: AtomicBool,
    /// Serve a different block hash on every request, as during a reorg.
    forking: AtomicBool,
    calls: AtomicU8,
}

struct Fixture {
    deriver: Deriver<MockEthProvider<eez_primitives::EezPrimitives>>,
    committer: BlockCommitterHandle<EezEngineTypes>,
    head: SealedHeader<Header>,
    l1: Arc<MockL1>,
    l1_head: Arc<L1CanonicalHead>,
    url: String,
    server: jsonrpsee::server::ServerHandle,
    engine: tokio::task::JoinHandle<()>,
}

impl Fixture {
    /// An unsafe head at L2 block 1 (t=134) that copied `head_seed`. With
    /// `retreat`, a batch indexed at L1 block 10 covers it, so a reorg to
    /// block 9 retreats the safe cursor to 0.
    async fn new(retreat: bool, head_seed: B256, l1_available: bool) -> Self {
        let l1 = Arc::new(MockL1::default());
        l1.unavailable.store(!l1_available, Ordering::Release);
        let mut rpc = jsonrpsee::RpcModule::new(Arc::clone(&l1));
        rpc.register_method("eth_getBlockByNumber", move |_, l1, _| {
            if l1.unavailable.load(Ordering::Acquire) {
                return Err(jsonrpsee::types::ErrorObjectOwned::owned(
                    -32000,
                    "L1 temporarily unavailable",
                    None::<()>,
                ));
            }
            let call = l1.calls.fetch_add(1, Ordering::AcqRel);
            let hash = if l1.forking.load(Ordering::Acquire) {
                B256::repeat_byte(call)
            } else {
                B256::repeat_byte(0xbb)
            };
            let mut block = serde_json::to_value(Header {
                timestamp: 120,
                number: 10,
                mix_hash: SEED,
                ..Header::default()
            })
            .unwrap();
            block["hash"] = serde_json::json!(hash);
            block["transactions"] = serde_json::json!([]);
            block["uncles"] = serde_json::json!([]);
            Ok(block)
        })
        .unwrap();
        let server = jsonrpsee::server::ServerBuilder::default()
            .build("127.0.0.1:0")
            .await
            .unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let server = server.start(rpc);
        let safe = SealedHeader::seal_slow(Header {
            timestamp: 132,
            gas_limit: BUILDER_GAS_LIMIT,
            base_fee_per_gas: Some(1),
            ..Header::default()
        });
        let head = SealedHeader::seal_slow(Header {
            number: 1,
            timestamp: 134,
            parent_hash: safe.hash(),
            mix_hash: head_seed,
            ..Header::default()
        });
        let provider = Arc::new(MockEthProvider::<eez_primitives::EezPrimitives>::new());
        provider.add_header(head.hash(), (*head).clone());
        provider.add_header(safe.hash(), (*safe).clone());
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let engine = tokio::spawn(async move {
            while let Some(message) = engine_rx.recv().await {
                let status = alloy_rpc_types_engine::PayloadStatus::from_status(
                    alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                );
                match message {
                    BeaconEngineMessage::ForkchoiceUpdated { tx, .. } => {
                        tx.send(Ok(OnForkChoiceUpdated::valid(status))).unwrap();
                    }
                    BeaconEngineMessage::NewPayload { tx, .. } => {
                        tx.send(Ok(status)).unwrap();
                    }
                    _ => panic!("unexpected engine request"),
                }
            }
        });
        let committer = BlockCommitterHandle::spawn(
            head.clone(),
            safe,
            B256::ZERO,
            ConsensusEngineHandle::new(engine_tx),
            PayloadBuilderHandle::new(mpsc::unbounded_channel().0),
            None,
        );
        let l1_head = Arc::new(L1CanonicalHead::default());
        if retreat {
            l1_head.append(BatchRecord {
                post: PostId {
                    l1_block: 10,
                    l1_block_hash: B256::repeat_byte(0xcc),
                    log_index: 0,
                },
                tx_hash: B256::repeat_byte(0xdd),
                last_l2_block: 1,
            });
        }
        let deriver = Deriver::new(
            committer.clone(),
            provider,
            reader(&url),
            Arc::new(ChainSpecBuilder::mainnet().cancun_activated().build()),
            2,
            12,
            0,
            Arc::clone(&l1_head),
            None,
            DeriverBoot::Peers,
            None,
        );
        Self {
            deriver,
            committer,
            head,
            l1,
            l1_head,
            url,
            server,
            engine,
        }
    }

    async fn reorg(&self) -> DeriverResult<()> {
        self.deriver
            .on_l1_reorg(9, B256::ZERO, 10, B256::repeat_byte(0xbb))
            .await
    }

    async fn next_l1_head(&self) -> DeriverResult<()> {
        self.deriver
            .handle_event(L1Event::NewHead {
                block_number: 10,
                block_hash: B256::repeat_byte(0xbb),
                timestamp: 120,
            })
            .await
    }

    fn set_l1_available(&self, available: bool) {
        self.l1.unavailable.store(!available, Ordering::Release);
    }

    fn stop(self) {
        self.server.stop().unwrap();
        self.engine.abort();
    }
}

fn reader(url: &str) -> L1Reader {
    L1Reader::new(L1ReaderConfig {
        rpc_url: url.parse().unwrap(),
        trace_rpc_url: url.parse().unwrap(),
        eez: Address::ZERO,
        rollup_id: 1,
        deploy_block: 0,
        beacon_url: None,
    })
}

#[tokio::test]
async fn reorg_without_settlement_retreat_retries_the_unsafe_seed_check() {
    let f = Fixture::new(false, OLD_SEED, false).await;
    assert!(matches!(f.reorg().await, Err(DeriverError::L1Scan(_))));
    assert!(f.committer.sequencing_revision().is_err());
    assert_eq!(f.committer.last_header().hash(), f.head.hash());

    f.set_l1_available(true);
    f.next_l1_head().await.unwrap();
    assert!(f.committer.sequencing_revision().is_ok());
    let replacement = f.committer.last_header();
    assert_eq!(replacement.number, f.head.number);
    assert_eq!(replacement.mix_hash, SEED);
    assert_ne!(replacement.hash(), f.head.hash());
    assert!(!f.deriver.inner.reseed_pending.load(Ordering::Acquire));
    f.stop();
}

#[tokio::test]
async fn settlement_retreat_preserves_unsafe_blocks_when_l1_seed_is_unavailable() {
    let f = Fixture::new(true, OLD_SEED, false).await;
    assert!(matches!(f.reorg().await, Err(DeriverError::L1Scan(_))));
    assert!(f.committer.sequencing_revision().is_err());
    assert_eq!(f.committer.last_header().hash(), f.head.hash());
    assert_eq!(f.l1_head.cursor(), 0);
    assert_eq!(*f.deriver.inner.retreat_pending.lock().unwrap(), Some(0));

    assert!(matches!(
        f.next_l1_head().await,
        Err(DeriverError::L1Scan(_))
    ));
    assert_eq!(*f.deriver.inner.retreat_pending.lock().unwrap(), Some(0));
    assert!(f.committer.sequencing_revision().is_err());
    assert_eq!(f.committer.last_header().hash(), f.head.hash());
    f.stop();
}

/// The deferred retreat completes on the next L1 head once L1 answers again.
#[tokio::test]
async fn settlement_retreat_finishes_once_l1_returns() {
    let f = Fixture::new(true, OLD_SEED, false).await;
    assert!(matches!(f.reorg().await, Err(DeriverError::L1Scan(_))));

    f.set_l1_available(true);
    f.next_l1_head().await.unwrap();
    assert_eq!(*f.deriver.inner.retreat_pending.lock().unwrap(), None);
    assert!(f.committer.sequencing_revision().is_ok());
    let replacement = f.committer.last_header();
    assert_eq!(replacement.number, f.head.number);
    assert_eq!(replacement.mix_hash, SEED);
    assert_ne!(replacement.hash(), f.head.hash());
    f.stop();
}

/// A reorg that leaves the copied L1 block in place keeps the unsafe head.
#[tokio::test]
async fn reorg_keeping_the_seed_keeps_the_unsafe_block() {
    let f = Fixture::new(false, SEED, true).await;
    let before = f.committer.sequencing_revision().unwrap();
    f.reorg().await.unwrap();
    assert_eq!(f.committer.last_header().hash(), f.head.hash());
    assert!(!f.deriver.inner.reseed_pending.load(Ordering::Acquire));
    // Production resumes, but on a new revision.
    assert_ne!(f.committer.sequencing_revision().unwrap(), before);
    f.stop();
}

/// Reorgs that pile up while recovery is pending resume production once, and
/// a block prepared before them is rejected even though its parent is unchanged.
#[tokio::test]
async fn repeated_reorgs_resume_once_and_reject_stale_prepared_blocks() {
    let f = Fixture::new(false, OLD_SEED, false).await;
    let prepared = f.committer.sequencing_revision().unwrap();
    assert!(f.reorg().await.is_err());
    assert!(f.reorg().await.is_err());
    assert!(f.committer.sequencing_revision().is_err());

    f.set_l1_available(true);
    f.next_l1_head().await.unwrap();
    assert_eq!(f.committer.sequencing_revision().unwrap(), prepared + 2);
    let head = f.committer.last_header();
    assert!(matches!(
        f.committer
            .commit_sequenced(head.hash(), Default::default(), prepared)
            .await,
        Err(eez_driver::DriverError::StaleL1View)
    ));
    assert_eq!(f.committer.last_header().hash(), head.hash());
    f.stop();
}

/// A lookup whose L1 head changed forks mid-search returns no seed.
#[tokio::test]
async fn lookup_rejects_a_seed_read_across_l1_forks() {
    let f = Fixture::new(false, SEED, true).await;
    let reader = reader(&f.url);
    assert_eq!(reader.prev_randao_for(134, 12).await.unwrap(), SEED);

    f.l1.forking.store(true, Ordering::Release);
    assert!(matches!(
        reader.prev_randao_for(134, 12).await,
        Err(eez_l1::L1Error::SourceIncomplete { .. })
    ));
    f.stop();
}
