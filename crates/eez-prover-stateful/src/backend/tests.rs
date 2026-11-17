use alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH;
use alloy_consensus::{BlockBody, Header, SignableTransaction as _, TxLegacy};
use alloy_primitives::{Bytes, Signature, TxKind, U256};
use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
use eez_proof_signer::validate::fixtures::admitted_block_from_consensus_rlp;
use reth_engine_primitives::{BeaconEngineMessage, OnForkChoiceUpdated};
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_storage_api::errors::provider::ProviderResult;
use reth_storage_api::{AccountReader, BytecodeReader, StateProofProvider, StorageRootProvider};
use reth_trie_common::updates::TrieUpdates;
use reth_trie_common::{
    AccountProof, DecodedMultiProofV2, ExecutionWitnessMode, HashedPostState, HashedStorage,
    KeccakKeyHasher, MultiProof, MultiProofTargets, MultiProofTargetsV2, StorageMultiProof,
    StorageProof, TrieInput,
};

use super::*;

fn admitted_block(block: Block) -> AdmittedBlock {
    let number = block.header.number;
    let parent_hash = block.header.parent_hash;
    let hash = block.header.hash_slow();
    admitted_block_from_consensus_rlp(number, hash, parent_hash, alloy_rlp::encode(block))
}

fn cached_engine(
    kind: &str,
) -> (
    Backend<MockEthProvider<EezPrimitives>>,
    BlockAnchor,
    AdmittedBlock,
) {
    use alloy_consensus::proofs::{calculate_transaction_root, calculate_withdrawals_root};
    use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SystemTransaction};
    let chain_spec = Arc::new(
        reth_chainspec::ChainSpecBuilder::mainnet()
            .cancun_activated()
            .build(),
    );
    let parent = Header {
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        state_root: B256::repeat_byte(0x11),
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        withdrawals_root: Some(calculate_withdrawals_root(&[])),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        ..Default::default()
    };
    let anchor = BlockAnchor {
        number: 0,
        hash: parent.hash_slow(),
        state_root: parent.state_root,
    };
    let with_checkpoints = kind != "ordinal";
    let transactions = match kind {
        "sync" => vec![
            SystemTransaction {
                chain_id: 1,
                nonce: 0,
                to: EEZL2_ADDRESS,
                value: U256::ZERO,
                input: Bytes::new(),
            }
            .into(),
        ],
        "ordinal" => vec![
            TxLegacy {
                chain_id: Some(1),
                gas_limit: 21_000,
                gas_price: 1,
                to: TxKind::Call(EEZL2_ADDRESS),
                ..Default::default()
            }
            .into_signed(Signature::test_signature())
            .into(),
        ],
        "empty" => Vec::new(),
        _ => unreachable!(),
    };
    let transaction_count = transactions.len();
    let block = Block {
        header: Header {
            parent_hash: anchor.hash,
            number: 1,
            timestamp: 1,
            transactions_root: calculate_transaction_root(&transactions),
            ..parent.clone()
        },
        body: BlockBody {
            transactions,
            withdrawals: Some(Default::default()),
            ..Default::default()
        },
    };
    let hash = block.header.hash_slow();
    let input = admitted_block(block.clone());
    let recovered = decode_match_and_recover_signers(&input, &chain_spec).unwrap();
    let (plan, _) = CheckpointPlan::for_block(&recovered, SYSTEM_ADDRESS);
    assert_eq!(!plan.positions().is_empty(), with_checkpoints);
    let provider = MockEthProvider::<EezPrimitives>::new();
    provider.add_header(anchor.hash, parent.clone());
    provider.add_block(hash, block);
    provider.add_receipts(
        1,
        (0..transaction_count)
            .map(|_| EthereumReceipt {
                success: true,
                ..Default::default()
            })
            .collect(),
    );
    // Engine receivers are closed: any attempted reimport must fail, not silently pass this test.
    let (engine_sender, engine_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (payload_sender, payload_receiver) = tokio::sync::mpsc::unbounded_channel();
    drop((engine_receiver, payload_receiver));
    let parent = SealedHeader::new(parent, anchor.hash);
    let committer = BlockCommitterHandle::spawn(
        parent.clone(),
        parent,
        B256::ZERO,
        reth_engine_primitives::ConsensusEngineHandle::new(engine_sender),
        reth_payload_builder::PayloadBuilderHandle::new(payload_sender),
        None,
    );
    let mut checkpoints = LruCache::new(NonZeroUsize::new(1).unwrap());
    if with_checkpoints {
        checkpoints.put(
            hash,
            plan.positions()
                .iter()
                .map(|&at| StateCheckpoint {
                    at,
                    state_root: anchor.state_root,
                    block_hash: hash,
                })
                .collect(),
        );
    }
    (
        Backend {
            provider,
            evm_config: EezEvmConfig::new(Arc::clone(&chain_spec)).unwrap(),
            chain_spec,
            expected_l2_system_address: SYSTEM_ADDRESS,
            committer,
            checkpoints: Arc::new(Mutex::new(checkpoints)),
        },
        anchor,
        input,
    )
}

#[tokio::test]
async fn block_validation_reuses_reth_and_backend_checkpoints_without_execution() {
    for kind in ["empty", "ordinal", "sync"] {
        // No historical execution state is installed. The nonempty ordinal
        // case must use Reth's receipts without opening state to replay checkpoints.
        let (engine, anchor, input) = cached_engine(kind);
        // Untrusted anchor fields are checked at admission, not on each Reth lookup.
        let mut wrong_anchor = anchor;
        wrong_anchor.state_root = B256::ZERO;
        assert!(matches!(
            engine.validate_anchor(wrong_anchor),
            Err(ValidationError::Rejected(_))
        ));
        engine.validate_anchor(anchor).unwrap();
        let with_checkpoints = kind != "ordinal";
        let (output, reused) = engine
            .validate_next(
                anchor,
                &input,
                ExecutionWitness::default(),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(reused);
        assert_eq!(
            output
                .transaction_state_checkpoints()
                .iter()
                .map(|checkpoint| checkpoint.at)
                .collect::<Vec<_>>(),
            match kind {
                "sync" => vec![CheckpointAt::PreExecution, CheckpointAt::Transaction(0)],
                "empty" => vec![CheckpointAt::PreExecution],
                _ => Vec::new(),
            },
        );
        assert_eq!(output.hash(), input.claimed_hash());
        let changed = admitted_block_from_consensus_rlp(
            input.declared_number(),
            input.claimed_hash(),
            input.claimed_parent_hash(),
            [input.rlp(), &[0]].concat(),
        );
        assert!(
            engine
                .validate_next(
                    anchor,
                    &changed,
                    ExecutionWitness::default(),
                    Duration::from_secs(1)
                )
                .await
                .is_err()
        );
        if with_checkpoints {
            // The mock has no execution state. Without supplemental checkpoints, reuse alone is insufficient.
            engine.checkpoints.lock().unwrap().clear();
            assert!(
                engine
                    .validate_next(
                        anchor,
                        &input,
                        ExecutionWitness::default(),
                        Duration::from_secs(1)
                    )
                    .await
                    .is_err()
            );
            assert!(engine.checkpoints.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn block_validation_deadline_bounds_waiting_for_reconciliation() {
    let (engine, anchor, input) = cached_engine("empty");
    let guard = engine.committer.begin_reconcile().await;
    assert!(matches!(
        engine
            .validate_next(
                anchor,
                &input,
                ExecutionWitness::default(),
                Duration::from_millis(10)
            )
            .await,
        Err(ValidationError::DeadlineExceeded)
    ));
    drop(guard);
    assert!(
        engine
            .validate_next(
                anchor,
                &input,
                ExecutionWitness::default(),
                Duration::from_secs(1)
            )
            .await
            .unwrap()
            .1
    );
}

fn mock_engine(
    backend: &mut Backend<MockEthProvider<EezPrimitives>>,
    anchor: BlockAnchor,
) -> tokio::sync::mpsc::UnboundedReceiver<BeaconEngineMessage<EezEngineTypes>> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (payload_sender, _) = tokio::sync::mpsc::unbounded_channel();
    let parent = SealedHeader::new(
        backend.provider.header(anchor.hash).unwrap().unwrap(),
        anchor.hash,
    );
    backend.committer = BlockCommitterHandle::spawn(
        parent.clone(),
        parent,
        B256::ZERO,
        reth_engine_primitives::ConsensusEngineHandle::new(sender),
        reth_payload_builder::PayloadBuilderHandle::new(payload_sender),
        None,
    );
    backend.provider.receipts.lock().clear(); // Force the real import path.
    receiver
}

#[tokio::test]
async fn pending_engine_validation_is_retryable_but_invalid_blocks_are_rejected() {
    for (payload_status, forkchoice_status) in [
        (PayloadStatusEnum::Syncing, PayloadStatusEnum::Valid),
        (PayloadStatusEnum::Accepted, PayloadStatusEnum::Valid),
        (
            PayloadStatusEnum::Invalid {
                validation_error: "bad payload".into(),
            },
            PayloadStatusEnum::Valid,
        ),
        (PayloadStatusEnum::Valid, PayloadStatusEnum::Syncing),
        (
            PayloadStatusEnum::Valid,
            PayloadStatusEnum::Invalid {
                validation_error: "bad forkchoice".into(),
            },
        ),
        (PayloadStatusEnum::Valid, PayloadStatusEnum::Valid),
    ] {
        let invalid = payload_status.is_invalid() || forkchoice_status.is_invalid();
        let valid = payload_status.is_valid() && forkchoice_status.is_valid();
        let (mut backend, anchor, input) = cached_engine("empty");
        let mut engine = mock_engine(&mut backend, anchor);
        let provider = backend.provider.clone();
        let responder = tokio::spawn(async move {
            let Some(BeaconEngineMessage::NewPayload { tx, .. }) = engine.recv().await else {
                panic!("expected newPayload");
            };
            let imported = payload_status.is_valid();
            tx.send(Ok(PayloadStatus::from_status(payload_status)))
                .unwrap();
            if imported {
                let Some(BeaconEngineMessage::ForkchoiceUpdated { tx, .. }) = engine.recv().await
                else {
                    panic!("expected forkchoice update");
                };
                provider.add_receipts(1, Vec::new());
                tx.send(Ok(OnForkChoiceUpdated::valid(PayloadStatus::from_status(
                    forkchoice_status,
                ))))
                .unwrap();
            }
        });
        let result = backend
            .validate_next(
                anchor,
                &input,
                ExecutionWitness::default(),
                Duration::from_secs(1),
            )
            .await;
        if invalid {
            assert!(
                matches!(result, Err(ValidationError::Rejected(_))),
                "{result:?}"
            );
        } else if valid {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(result, Err(ValidationError::Unavailable(_))),
                "{result:?}"
            );
        }
        responder.await.unwrap();
    }
}

#[tokio::test]
async fn timed_out_import_holds_reconciliation_until_engine_finishes() {
    for stall_at_forkchoice in [false, true] {
        let (mut backend, anchor, input) = cached_engine("empty");
        let mut engine = mock_engine(&mut backend, anchor);
        let committer = backend.committer.clone();
        let request = tokio::spawn(async move {
            backend
                .validate_next(
                    anchor,
                    &input,
                    ExecutionWitness::default(),
                    Duration::from_millis(100),
                )
                .await
        });
        let Some(BeaconEngineMessage::NewPayload { tx, .. }) = engine.recv().await else {
            panic!("expected newPayload");
        };
        // Withhold the engine reply until the request has actually timed out.
        if stall_at_forkchoice {
            tx.send(Ok(PayloadStatus::from_status(PayloadStatusEnum::Valid)))
                .unwrap();
            let Some(BeaconEngineMessage::ForkchoiceUpdated { tx, .. }) = engine.recv().await
            else {
                panic!("expected forkchoice update");
            };
            assert!(matches!(
                request.await.unwrap(),
                Err(ValidationError::DeadlineExceeded)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(10), committer.begin_reconcile())
                    .await
                    .is_err()
            );
            tx.send(Ok(OnForkChoiceUpdated::syncing())).unwrap();
        } else {
            assert!(matches!(
                request.await.unwrap(),
                Err(ValidationError::DeadlineExceeded)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(10), committer.begin_reconcile())
                    .await
                    .is_err()
            );
            tx.send(Ok(PayloadStatus::from_status(PayloadStatusEnum::Syncing)))
                .unwrap();
        }
        // The next reconciler can run once the outstanding mutation has returned.
        let _guard = tokio::time::timeout(Duration::from_secs(1), committer.begin_reconcile())
            .await
            .unwrap();
    }
}

fn provider_with_anchor(number: u64, hash: B256, state_root: B256) -> MockEthProvider {
    let provider = MockEthProvider::new();
    provider.add_header(
        hash,
        Header {
            number,
            state_root,
            gas_limit: 30_000_000,
            ..Default::default()
        },
    );
    provider
}

#[derive(Clone)]
struct HistoryCheckpointProvider {
    inner: MockEthProvider,
    checkpoint_root: B256,
    history_value: U256,
}

impl HistoryCheckpointProvider {
    fn assert_history_update(&self, state: &HashedPostState) {
        let address =
            alloy_primitives::keccak256(alloy_eips::eip2935::HISTORY_STORAGE_ADDRESS.as_slice());
        let value = state.storages.get(&address).and_then(|storage| {
            storage
                .storage
                .get(&alloy_primitives::keccak256(B256::ZERO))
        });
        assert_eq!(
            value,
            Some(&self.history_value),
            "checkpoint omitted the EIP-2935 history update"
        );
    }
}

impl AsRef<MockEthProvider> for HistoryCheckpointProvider {
    fn as_ref(&self) -> &MockEthProvider {
        &self.inner
    }
}

reth_storage_api::macros::delegate_impls_to_as_ref!(
    for HistoryCheckpointProvider =>
    AccountReader {
        fn basic_account(&self, address: &Address) -> ProviderResult<Option<reth_primitives_traits::Account>>;
    }
    BlockHashReader {
        fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>>;
        fn canonical_hashes_range(&self, start: u64, end: u64) -> ProviderResult<Vec<B256>>;
    }
    BytecodeReader {
        fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<reth_primitives_traits::Bytecode>>;
    }
    StorageRootProvider {
        fn storage_root(&self, address: Address, state: HashedStorage) -> ProviderResult<B256>;
        fn storage_proof(&self, address: Address, slot: B256, state: HashedStorage) -> ProviderResult<StorageProof>;
        fn storage_multiproof(&self, address: Address, slots: &[B256], state: HashedStorage) -> ProviderResult<StorageMultiProof>;
    }
    StateProofProvider {
        fn proof(&self, input: TrieInput, address: Address, slots: &[B256]) -> ProviderResult<AccountProof>;
        fn multiproof(&self, input: TrieInput, targets: MultiProofTargets) -> ProviderResult<MultiProof>;
        fn multiproof_v2(&self, input: TrieInput, targets: MultiProofTargetsV2) -> ProviderResult<DecodedMultiProofV2>;
        fn witness(&self, input: TrieInput, target: HashedPostState, mode: ExecutionWitnessMode) -> ProviderResult<Vec<Bytes>>;
    }
    StateProvider {
        fn storage(&self, account: Address, key: B256) -> ProviderResult<Option<U256>>;
    }
);

impl HashedPostStateProvider for HistoryCheckpointProvider {
    fn hashed_post_state(
        &self,
        state: &revm::database::BundleState,
    ) -> ProviderResult<HashedPostState> {
        Ok(HashedPostState::from_bundle_state::<KeccakKeyHasher>(
            state.state(),
        ))
    }
}

impl StateRootProvider for HistoryCheckpointProvider {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        self.assert_history_update(&state);
        Ok(self.checkpoint_root)
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        self.inner.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.assert_history_update(&state);
        Ok((self.checkpoint_root, TrieUpdates::default()))
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

#[tokio::test]
async fn backend_identity_is_fixed_at_construction() {
    let chain_spec = Arc::new(ChainSpec::default());
    let chain_id = chain_spec.chain().id();
    let l2_address = Address::repeat_byte(9);
    let provider: MockEthProvider<EezPrimitives> = MockEthProvider::new();
    let (engine, _, _) = cached_engine("empty");
    let backend = Backend::new(provider, chain_spec, l2_address, engine.committer);

    assert_eq!(backend.chain_id(), chain_id);
    assert_eq!(backend.expected_l2_system_address(), l2_address);
}

#[test]
fn execution_errors_preserve_retryability() {
    let invalid_composition = execution_error(BlockExecutionError::Validation(
        reth_evm::block::BlockValidationError::msg("invalid transaction"),
    ));
    assert!(matches!(invalid_composition, ValidationError::Rejected(_)));

    let backend_fault = execution_error(BlockExecutionError::msg("database unavailable"));
    assert!(matches!(backend_fault, ValidationError::Unavailable(_)));
}

#[test]
fn empty_checkpoint_execution_retains_transaction_state() {
    let chain_spec = Arc::new(ChainSpec::default());
    let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec)).unwrap();
    let recipient = Address::repeat_byte(0x22);
    let signed = TxLegacy {
        nonce: 0,
        gas_price: 1,
        gas_limit: 21_000,
        to: TxKind::Call(recipient),
        value: U256::from(1),
        ..Default::default()
    }
    .into_signed(Signature::test_signature());
    let sender = signed.recover_signer().unwrap();
    let block = RecoveredBlock::new_unhashed(
        Block {
            header: Header {
                number: 1,
                gas_limit: 30_000_000,
                timestamp: 1,
                ..Default::default()
            },
            body: BlockBody {
                transactions: vec![signed.into()],
                ..Default::default()
            },
        },
        vec![sender],
    );
    let provider: MockEthProvider = MockEthProvider::new();
    provider.add_account(sender, ExtendedAccount::new(0, U256::from(u64::MAX)));
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(EvmStateProviderAdapter(
            Box::new(provider) as Box<dyn StateProvider + Send>,
        )))
        .with_bundle_update()
        .build();

    execute_block_with_state_checkpoints(&evm_config, &mut state, &block, &[])
        .map(|(result, _, _)| result)
        .unwrap();

    assert_eq!(
        state
            .bundle_state
            .account(&sender)
            .and_then(|account| account.info.as_ref())
            .map(|account| account.nonce),
        Some(1),
        "an empty checkpoint plan must still retain the transaction's sender-nonce update",
    );
}

#[test]
fn empty_checkpoint_includes_eip_2935_and_matches_the_composer_anchor() {
    use alloy_eips::eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE};
    use eez_composer::local::build_sync_block;

    let chain_spec = Arc::new(
        reth_chainspec::ChainSpecBuilder::mainnet()
            .prague_activated()
            .build(),
    );
    let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec)).unwrap();
    let parent = SealedHeader::seal_slow(Header {
        number: 0,
        timestamp: 0,
        gas_limit: 30_000_000,
        base_fee_per_gas: Some(1),
        excess_blob_gas: Some(0),
        blob_gas_used: Some(0),
        ..Default::default()
    });
    let provider = provider_with_anchor(0, parent.hash(), parent.state_root());
    provider.add_account(
        HISTORY_STORAGE_ADDRESS,
        ExtendedAccount::new(0, U256::ZERO).with_bytecode(HISTORY_STORAGE_CODE.clone()),
    );

    let checkpoint_root = B256::repeat_byte(3);
    provider.add_state_root(checkpoint_root);
    let composer_block =
        build_sync_block(&provider, &evm_config, &parent, 1, Address::ZERO, &[]).unwrap();

    let checkpoint_provider = HistoryCheckpointProvider {
        inner: provider,
        checkpoint_root,
        history_value: U256::from_be_slice(parent.hash().as_slice()),
    };
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(EvmStateProviderAdapter(
            Box::new(checkpoint_provider) as Box<dyn StateProvider + Send>,
        )))
        .with_bundle_update()
        .build();

    let (_, checkpoints, _) = execute_block_with_state_checkpoints(
        &evm_config,
        &mut state,
        &composer_block.block,
        &[CheckpointAt::PreExecution],
    )
    .unwrap();

    assert_eq!(
        checkpoints,
        [StateCheckpoint {
            at: CheckpointAt::PreExecution,
            state_root: checkpoint_root,
            block_hash: composer_block.header.hash(),
        }],
    );
}

#[test]
fn native_deposits_execute_with_and_without_checkpoints() {
    use eez_primitives::{EEZL2_ADDRESS, SYSTEM_ADDRESS, SystemTransaction};

    let chain_spec = Arc::new(
        reth_chainspec::ChainSpecBuilder::mainnet()
            .cancun_activated()
            .build(),
    );
    let evm_config = EezEvmConfig::new(chain_spec).unwrap();
    let block = RecoveredBlock::try_recover(Block {
        header: Header {
            number: 1,
            timestamp: 1,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some(1),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(B256::ZERO),
            ..Default::default()
        },
        body: BlockBody {
            transactions: vec![
                SystemTransaction {
                    chain_id: 1,
                    nonce: 0,
                    to: EEZL2_ADDRESS,
                    value: U256::from(13),
                    input: Bytes::new(),
                }
                .into(),
            ],
            ..Default::default()
        },
    })
    .unwrap();
    for with_checkpoints in [false, true] {
        // The mock supplies an empty anchor state and a checkpoint root; the
        // actual EEZ EVM must mint the deposit without a prefunded system account.
        let provider: MockEthProvider = MockEthProvider::new();
        provider.add_state_root(B256::repeat_byte(0x11));
        let mut state = State::builder()
            .with_database(StateProviderDatabase::new(EvmStateProviderAdapter(
                Box::new(provider) as Box<dyn StateProvider + Send>,
            )))
            .with_bundle_update()
            .build();
        let result = if with_checkpoints {
            let (result, checkpoints, _) = execute_block_with_state_checkpoints(
                &evm_config,
                &mut state,
                &block,
                &[CheckpointAt::Transaction(0)],
            )
            .unwrap();
            assert_eq!(checkpoints.len(), 1);
            assert_eq!(checkpoints[0].at, CheckpointAt::Transaction(0));
            result
        } else {
            execute_block_with_state_checkpoints(&evm_config, &mut state, &block, &[])
                .map(|(result, _, _)| result)
                .unwrap()
        };
        assert_eq!(result.receipts.len(), 1);
        assert!(result.receipts[0].success);
        assert_eq!(
            result.receipts[0].tx_type,
            eez_primitives::EezTxType::System
        );
        let system = state
            .bundle_state
            .account(&SYSTEM_ADDRESS)
            .unwrap()
            .info
            .as_ref()
            .unwrap();
        assert_eq!(system.nonce, 1);
        assert_eq!(system.balance, U256::ZERO);
        let recipient = state
            .bundle_state
            .account(&EEZL2_ADDRESS)
            .unwrap()
            .info
            .as_ref()
            .unwrap();
        assert_eq!(recipient.balance, U256::from(13));
    }
}

#[test]
fn beacon_withdrawals_are_rejected_with_and_without_checkpoints() {
    let evm_config = EezEvmConfig::new(Arc::new(ChainSpec::default())).unwrap();
    let block = RecoveredBlock::new_unhashed(
        Block {
            header: Header::default(),
            body: BlockBody {
                withdrawals: Some(
                    vec![alloy_eips::eip4895::Withdrawal {
                        index: 0,
                        validator_index: 0,
                        address: Address::repeat_byte(0xab),
                        amount: 7_000_000_000,
                    }]
                    .into(),
                ),
                ..Default::default()
            },
        },
        vec![],
    );
    for with_checkpoints in [false, true] {
        let provider: MockEthProvider = MockEthProvider::new();
        let mut state = State::builder()
            .with_database(StateProviderDatabase::new(EvmStateProviderAdapter(
                Box::new(provider) as Box<dyn StateProvider + Send>,
            )))
            .with_bundle_update()
            .build();
        let error = if with_checkpoints {
            execute_block_with_state_checkpoints(&evm_config, &mut state, &block, &[]).unwrap_err()
        } else {
            execute_block_with_state_checkpoints(&evm_config, &mut state, &block, &[])
                .map(|(result, _, _)| result)
                .unwrap_err()
        };
        assert!(matches!(error, ValidationError::Rejected(_)));
        assert!(
            error
                .to_string()
                .contains("L2 blocks cannot contain beacon withdrawals")
        );
        assert!(state.bundle_state.state.is_empty());
    }
}
