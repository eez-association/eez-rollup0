//! Mock engine API and payload service for driver tests.
//!
//! Both mocks append to one ordered [`EngineCall`] log. An FCU with
//! attributes starts a payload job on the FCU head; resolving the job
//! returns an empty child of that head with the attributes' timestamp.
//! Every call answers VALID unless a status was queued for it.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, U256};
use alloy_rpc_types_engine::{PayloadId, PayloadStatus, PayloadStatusEnum};
use reth_engine_primitives::{BeaconEngineMessage, ConsensusEngineHandle, OnForkChoiceUpdated};
use reth_ethereum_engine_primitives::{EthBuiltPayload, EthEngineTypes, EthPayloadAttributes};
use reth_payload_builder::{PayloadBuilderError, PayloadBuilderHandle, PayloadServiceCommand};
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use tokio::sync::{mpsc, oneshot};

use crate::BlockCommitterHandle;

/// One engine or payload-service call, in arrival order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EngineCall {
    /// `forkchoiceUpdated`; `with_attrs` when it asks for a payload job.
    Fcu { head: B256, with_attrs: bool },
    /// `newPayload` of the block `hash`.
    NewPayload { hash: B256 },
    /// Payload-service resolve of the job `id`.
    Resolve { id: PayloadId },
    /// A test saw the bundle of block `hash` released.
    Released { hash: B256 },
}

/// How the mock answers one FCU or `newPayload`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MockStatus {
    Valid,
    Syncing,
    Invalid,
}

#[derive(Debug, Default)]
struct State {
    log: Vec<EngineCall>,
    /// Headers a payload job can build on, by hash.
    headers: HashMap<B256, Header>,
    /// Started payload jobs: parent hash and attributes.
    jobs: HashMap<PayloadId, (B256, EthPayloadAttributes)>,
    next_job: u64,
    fcu_statuses: VecDeque<MockStatus>,
    new_payload_statuses: VecDeque<MockStatus>,
}

type PayloadFuture =
    Pin<Box<dyn Future<Output = Result<EthBuiltPayload, PayloadBuilderError>> + Send>>;

/// Shared state of the two mocks.
#[derive(Debug, Clone, Default)]
pub(crate) struct MockEngine {
    state: Arc<Mutex<State>>,
}

impl MockEngine {
    /// Spawns both mocks on the current runtime. Payload jobs can build on `head`.
    pub(crate) fn spawn(
        head: &SealedHeader,
    ) -> (
        Self,
        ConsensusEngineHandle<EthEngineTypes>,
        PayloadBuilderHandle<EthEngineTypes>,
    ) {
        let engine = Self::default();
        engine.add_header(head);
        let (engine_tx, engine_rx) = mpsc::unbounded_channel();
        let (service_tx, service_rx) = mpsc::unbounded_channel();
        tokio::spawn(engine.clone().run_engine(engine_rx));
        tokio::spawn(engine.clone().run_payload_service(service_rx));
        (
            engine,
            ConsensusEngineHandle::new(engine_tx),
            PayloadBuilderHandle::new(service_tx),
        )
    }

    /// A committer on the mocks, with `head` as its head, safe and finalized block.
    pub(crate) fn committer(head: &SealedHeader) -> (Self, BlockCommitterHandle<EthEngineTypes>) {
        let (engine, to_engine, payload_builder) = Self::spawn(head);
        let committer = BlockCommitterHandle::spawn(
            head.clone(),
            head.clone(),
            head.hash(),
            to_engine,
            payload_builder,
            None,
        );
        (engine, committer)
    }

    /// Lets payload jobs build on `header`.
    pub(crate) fn add_header(&self, header: &SealedHeader) {
        self.lock()
            .headers
            .insert(header.hash(), header.header().clone());
    }

    /// Every call so far, in order.
    pub(crate) fn calls(&self) -> Vec<EngineCall> {
        self.lock().log.clone()
    }

    /// Logs a bundle release in call order.
    pub(crate) fn note_release(&self, hash: B256) {
        self.lock().log.push(EngineCall::Released { hash });
    }

    /// Answers the next FCU without a queued status with `status`.
    pub(crate) fn queue_fcu(&self, status: MockStatus) {
        self.lock().fcu_statuses.push_back(status);
    }

    /// Answers the next `newPayload` without a queued status with `status`.
    pub(crate) fn queue_new_payload(&self, status: MockStatus) {
        self.lock().new_payload_statuses.push_back(status);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("mock engine state poisoned")
    }

    async fn run_engine(
        self,
        mut rx: mpsc::UnboundedReceiver<BeaconEngineMessage<EthEngineTypes>>,
    ) {
        while let Some(message) = rx.recv().await {
            match message {
                BeaconEngineMessage::ForkchoiceUpdated {
                    state,
                    payload_attrs,
                    tx,
                    ..
                } => {
                    let _ = tx.send(Ok(self.on_fcu(state.head_block_hash, payload_attrs)));
                }
                BeaconEngineMessage::NewPayload { payload, tx, .. } => {
                    let _ = tx.send(Ok(self.on_new_payload(payload.block_hash())));
                }
                other => panic!("mock engine got an unexpected message: {other}"),
            }
        }
    }

    async fn run_payload_service(
        self,
        mut rx: mpsc::UnboundedReceiver<PayloadServiceCommand<EthEngineTypes>>,
    ) {
        while let Some(command) = rx.recv().await {
            match command {
                PayloadServiceCommand::Resolve(id, _kind, tx) => {
                    let built = self.on_resolve(id).map(|payload| {
                        let future: PayloadFuture = Box::pin(async move { Ok(payload) });
                        future
                    });
                    let _ = tx.send(built);
                }
                other => panic!("mock payload service got an unexpected command: {other:?}"),
            }
        }
    }

    fn on_fcu(&self, head: B256, attrs: Option<EthPayloadAttributes>) -> OnForkChoiceUpdated {
        let mut state = self.lock();
        state.log.push(EngineCall::Fcu {
            head,
            with_attrs: attrs.is_some(),
        });
        match state.fcu_statuses.pop_front().unwrap_or(MockStatus::Valid) {
            MockStatus::Syncing => OnForkChoiceUpdated::syncing(),
            MockStatus::Invalid => OnForkChoiceUpdated::with_invalid(invalid()),
            MockStatus::Valid => {
                let status = PayloadStatus::new(PayloadStatusEnum::Valid, Some(head));
                let Some(attrs) = attrs else {
                    return OnForkChoiceUpdated::valid(status);
                };
                state.next_job += 1;
                let id = PayloadId::new(state.next_job.to_be_bytes());
                state.jobs.insert(id, (head, attrs));
                let (id_tx, id_rx) = oneshot::channel();
                let _ = id_tx.send(Ok(id));
                OnForkChoiceUpdated::updated_with_pending_payload_id(status, id_rx)
            }
        }
    }

    fn on_new_payload(&self, hash: B256) -> PayloadStatus {
        let mut state = self.lock();
        state.log.push(EngineCall::NewPayload { hash });
        match state
            .new_payload_statuses
            .pop_front()
            .unwrap_or(MockStatus::Valid)
        {
            MockStatus::Valid => PayloadStatus::new(PayloadStatusEnum::Valid, Some(hash)),
            MockStatus::Syncing => PayloadStatus::from_status(PayloadStatusEnum::Syncing),
            MockStatus::Invalid => invalid(),
        }
    }

    fn on_resolve(&self, id: PayloadId) -> Option<EthBuiltPayload> {
        let mut state = self.lock();
        state.log.push(EngineCall::Resolve { id });
        let (parent_hash, attrs) = state.jobs.remove(&id)?;
        let parent = state
            .headers
            .get(&parent_hash)
            .unwrap_or_else(|| panic!("mock payload job on unknown parent {parent_hash}"));
        let header = Header {
            parent_hash,
            number: parent.number + 1,
            timestamp: attrs.timestamp,
            gas_limit: parent.gas_limit,
            beneficiary: attrs.suggested_fee_recipient,
            mix_hash: attrs.prev_randao,
            ..Default::default()
        };
        let block = SealedBlock::seal_slow(reth_ethereum_primitives::Block {
            header,
            body: Default::default(),
        });
        state.headers.insert(block.hash(), block.header().clone());
        let block = RecoveredBlock::new_sealed(block, Vec::new());
        Some(EthBuiltPayload::new(
            Arc::new(block),
            U256::ZERO,
            None,
            None,
        ))
    }
}

fn invalid() -> PayloadStatus {
    PayloadStatus::from_status(PayloadStatusEnum::Invalid {
        validation_error: "rejected by the mock engine".to_owned(),
    })
}

/// A genesis header at timestamp 1000.
pub(crate) fn genesis() -> SealedHeader {
    SealedHeader::seal_slow(Header {
        timestamp: 1_000,
        gas_limit: 30_000_000,
        ..Default::default()
    })
}

/// Payload attributes with only the timestamp set.
pub(crate) fn attrs(timestamp: u64) -> EthPayloadAttributes {
    EthPayloadAttributes {
        timestamp,
        prev_randao: B256::ZERO,
        suggested_fee_recipient: Address::ZERO,
        withdrawals: None,
        parent_beacon_block_root: None,
        slot_number: None,
        target_gas_limit: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DriverError;

    #[tokio::test]
    async fn sequenced_commit_builds_a_child_and_logs_every_call() {
        let genesis = genesis();
        let (engine, committer) = MockEngine::committer(&genesis);

        let outcome = committer
            .commit_sequenced(genesis.hash(), attrs(1_002))
            .await
            .unwrap();
        let block = outcome.header;
        assert_eq!(block.parent_hash, genesis.hash());
        assert_eq!((block.number, block.timestamp), (1, 1_002));
        assert_eq!(committer.last_header().hash(), block.hash());
        assert_eq!(
            engine.calls(),
            vec![
                EngineCall::Fcu {
                    head: genesis.hash(),
                    with_attrs: true
                },
                EngineCall::Resolve {
                    id: PayloadId::new(1_u64.to_be_bytes())
                },
                EngineCall::NewPayload { hash: block.hash() },
                EngineCall::Fcu {
                    head: block.hash(),
                    with_attrs: false
                },
            ]
        );
    }

    #[tokio::test]
    async fn queued_statuses_answer_one_call_each() {
        let genesis = genesis();
        let (engine, committer) = MockEngine::committer(&genesis);

        engine.queue_new_payload(MockStatus::Invalid);
        let err = committer
            .commit_sequenced(genesis.hash(), attrs(1_002))
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::InvalidPayload(_)), "{err}");
        assert_eq!(committer.last_header().hash(), genesis.hash());

        engine.queue_fcu(MockStatus::Invalid);
        let err = committer
            .commit_sequenced(genesis.hash(), attrs(1_002))
            .await
            .unwrap_err();
        assert!(matches!(err, DriverError::InvalidForkchoice(_)), "{err}");

        engine.queue_fcu(MockStatus::Syncing);
        let err = committer.refresh_forkchoice().await.unwrap_err();
        assert!(matches!(err, DriverError::EngineNotReady(_)), "{err}");

        // The queue is drained: the next commit is VALID throughout.
        committer
            .commit_sequenced(genesis.hash(), attrs(1_002))
            .await
            .unwrap();
    }
}
