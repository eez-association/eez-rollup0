//! Node-backed execution evidence.
//!
//! Streaming requests import candidate unsafe
//! blocks through the follower's existing Reth engine and retain only checked
//! identities/evidence; Reth remains the owner of blocks and execution state.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_consensus::BlockHeader as _;
use alloy_eips::eip7928::compute_block_access_list_hash;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_driver::{BlockCommitterHandle, ForkchoiceOutcome};
use eez_evm::EezEvmConfig;
use eez_primitives::Block;
use eez_primitives::EezPrimitives;
use eez_primitives::Receipt as EthereumReceipt;
use eez_primitives::engine::EezEngineTypes;
use eez_proof_signer::validate::support::{
    CheckpointPlan, decode_match_and_recover_signers, observe_outbound_events,
};
use eez_proof_signer::validate::{
    AdmittedBlock, BackendBlockOutput, BlockAnchor, CheckpointAt, SettlementBlockEvidence,
    StateCheckpoint, ValidatedBlock, ValidatedWindow, ValidationBackend, ValidationError,
};
use lru::LruCache;
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus as _, HeaderValidator as _};
use reth_ethereum_consensus::{EthBeaconConsensus, validate_block_post_execution};
use reth_evm::block::BlockExecutionError;
use reth_evm::execute::BlockExecutor as _;
use reth_evm::{ConfigureEvm as _, Evm as _};
use reth_execution_types::BlockExecutionResult;
use reth_payload_primitives::PayloadTypes as _;
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    BlockHashReader, BlockIdReader, BlockReader, EvmStateProviderAdapter, HashedPostStateProvider,
    HeaderProvider, StateProvider, StateProviderFactory, StateRootProvider,
};
use revm::database::states::bundle_state::BundleRetention;
use revm::state::bal::Bal;
use stateless_reth::candidate_block_hash;

const MAX_CACHED_CHECKPOINT_BLOCKS: usize = 32_768;

/// Stateful validation over one live Ethereum node provider.
#[derive(Debug)]
pub struct Backend<P> {
    provider: P,
    chain_spec: Arc<ChainSpec>,
    evm_config: EezEvmConfig,
    expected_l2_system_address: Address,
    committer: BlockCommitterHandle<EezEngineTypes>,
    // Reth owns blocks, state and receipts; retain only supplemental checkpoints here.
    checkpoints: Arc<Mutex<LruCache<B256, Vec<StateCheckpoint>>>>,
}

impl<P> Backend<P> {
    /// Bind the backend to the follower's provider and execution rules.
    pub fn new(
        provider: P,
        chain_spec: Arc<ChainSpec>,
        expected_l2_system_address: Address,
        committer: BlockCommitterHandle<EezEngineTypes>,
    ) -> Self {
        let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec))
            .expect("execution chain configuration was validated at startup");
        Self {
            provider,
            chain_spec,
            evm_config,
            expected_l2_system_address,
            committer,
            checkpoints: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(MAX_CACHED_CHECKPOINT_BLOCKS).unwrap(),
            ))),
        }
    }
}

fn ensure_safe_anchor<P>(provider: &P, anchor: BlockAnchor) -> Result<(), ValidationError>
where
    P: BlockHashReader + BlockIdReader + HeaderProvider<Header = alloy_consensus::Header>,
{
    let local_hash = canonical_hash(provider, anchor.number)?;
    if local_hash != anchor.hash {
        return Err(ValidationError::Aborted(format!(
            "canonical anchor {} is {local_hash}, session anchors to {}",
            anchor.number, anchor.hash,
        )));
    }
    let header = provider
        .header(anchor.hash)
        .map_err(provider_error)?
        .ok_or_else(|| {
            ValidationError::Unavailable(format!(
                "local anchor header {} ({}) is unavailable",
                anchor.number, anchor.hash,
            ))
        })?;
    if header.number != anchor.number || header.state_root != anchor.state_root {
        return Err(ValidationError::Rejected(format!(
            "session anchor fields do not match local block {}",
            anchor.number,
        )));
    }
    match provider.safe_block_num_hash().map_err(provider_error)? {
        Some(safe) if safe.number == anchor.number && safe.hash == anchor.hash => Ok(()),
        // Fresh databases do not persist a safe marker before the first L1
        // advance. Genesis is nevertheless the only valid initial anchor.
        None if anchor.number == 0 => Ok(()),
        safe => Err(ValidationError::Aborted(format!(
            "L1-derived safe anchor moved; session has {} {}, local safe is {safe:?}",
            anchor.number, anchor.hash,
        ))),
    }
}

#[async_trait::async_trait]
impl<P> ValidationBackend for Backend<P>
where
    P: BlockHashReader
        + BlockIdReader
        + BlockReader<Block = Block, Receipt = EthereumReceipt>
        + HeaderProvider<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + 'static,
{
    fn label(&self) -> &'static str {
        "stateful"
    }

    fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    fn expected_l2_system_address(&self) -> Address {
        self.expected_l2_system_address
    }

    fn validate_anchor(&self, anchor: BlockAnchor) -> Result<(), ValidationError> {
        ensure_safe_anchor(&self.provider, anchor)
    }

    async fn prepare_attestation(&self, window: &ValidatedWindow) -> Result<(), ValidationError> {
        let committer = self.committer.clone();
        let forkchoice_guard = Arc::new(committer.begin_reconcile().await);
        let terminal = window.settling_block();
        let terminal = SealedHeader::new(terminal.decoded().header.clone(), terminal.hash());
        // Blocks were imported and executed before their Validated frames.
        // Select the known terminal with one FCU; Reth enforces safe/finalized ancestry.
        // Replaying the prefix through newPayload would race Reth's persistence pipeline.
        let selection = tokio::spawn({
            let guard = Arc::clone(&forkchoice_guard);
            let terminal = terminal.clone();
            async move {
                // The actor keeps running if the request times out.
                let _guard = guard;
                committer.advance_unsafe_head(terminal).await
            }
        });
        match selection
            .await
            .map_err(|error| {
                ValidationError::InternalInvariant(format!(
                    "stateful forkchoice worker failed: {error}"
                ))
            })?
            .map_err(|error| {
                if matches!(error, eez_driver::DriverError::InvalidForkchoice(_)) {
                    ValidationError::Aborted(format!(
                        "Reth rejected validated terminal block {}: {error}",
                        terminal.number(),
                    ))
                } else {
                    ValidationError::Unavailable(format!(
                        "Reth could not select validated terminal block {}: {error}",
                        terminal.number(),
                    ))
                }
            })? {
            ForkchoiceOutcome::Valid => {}
            ForkchoiceOutcome::Syncing => {
                return Err(ValidationError::Unavailable(format!(
                    "Reth is still syncing validated terminal block {} ({})",
                    terminal.number(),
                    terminal.hash(),
                )));
            }
            ForkchoiceOutcome::InvalidState => {
                return Err(ValidationError::Aborted(format!(
                    "Reth rejected validated terminal block {} ({}) against its safe anchor",
                    terminal.number(),
                    terminal.hash(),
                )));
            }
        }
        if canonical_hash(&self.provider, terminal.number())? != terminal.hash() {
            return Err(ValidationError::Aborted(format!(
                "Reth selected a different canonical block at finalized height {}",
                terminal.number(),
            )));
        }
        Ok(())
    }

    async fn validate_next(
        &self,
        parent: BlockAnchor,
        admitted: &AdmittedBlock,
        _witness: ExecutionWitness,
        request_timeout: Duration,
    ) -> Result<(Arc<ValidatedBlock>, bool), ValidationError> {
        tokio::time::timeout(request_timeout, async {
            admitted.check_parent(parent)?;

            let block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
            let number = block.header().number();
            let consensus = EthBeaconConsensus::new(Arc::clone(&self.chain_spec));
            consensus
                .validate_header(block.sealed_block().sealed_header())
                .and_then(|()| consensus.validate_block_pre_execution(block.sealed_block()))
                .map_err(|error| {
                    ValidationError::Rejected(format!(
                        "stateful consensus validation rejected block {number}: {error}"
                    ))
                })?;

            // The engine sees retained forks; the provider only sees the selected
            // branch. On a miss, select the child before reading its parent/evidence.
            // Hold selection and reads together against other sessions/L1 reconciliation.
            let committer = &self.committer;
            let forkchoice_guard = Arc::new(committer.begin_reconcile().await);
            let reused = if self
                .provider
                .find_block_by_hash(block.hash(), reth_storage_api::BlockSource::Any)
                .map_err(provider_error)?
                .is_some()
            {
                self.provider
                    .receipts_by_block(block.hash().into())
                    .map_err(provider_error)?
                    .is_some()
            } else {
                false
            };
            // Visible cached evidence needs no head change. On a miss, the engine
            // imports or reuses its retained payload and selects that branch for reads.
            if !reused {
                let sealed_block = block.sealed_block().clone();
                let header = sealed_block.clone_sealed_header();
                let payload = EezEngineTypes::block_to_payload(sealed_block, None);
                let import = tokio::spawn({
                    let committer = committer.clone();
                    let guard = Arc::clone(&forkchoice_guard);
                    async move {
                        // Keep reconciliation excluded until the actor finishes, even on timeout.
                        let _guard = guard;
                        committer.commit_derived(payload, header, false).await
                    }
                });
                import
                    .await
                    .map_err(|error| {
                        ValidationError::InternalInvariant(format!(
                            "stateful import worker failed: {error}"
                        ))
                    })?
                    .map_err(|error| {
                        if matches!(
                            error,
                            eez_driver::DriverError::InvalidPayload(_)
                                | eez_driver::DriverError::InvalidForkchoice(_)
                        ) {
                            ValidationError::Rejected(format!(
                                "Reth rejected streamed block {number}: {error}"
                            ))
                        } else {
                            ValidationError::Unavailable(format!(
                                "Reth could not import streamed block {number}: {error}"
                            ))
                        }
                    })?;
            }
            let stored = self
                .provider
                .find_block_by_hash(block.hash(), reth_storage_api::BlockSource::Any)
                .map_err(provider_error)?
                .ok_or_else(|| {
                    ValidationError::Unavailable(format!(
                        "Reth accepted block {number} but no longer exposes its exact block"
                    ))
                })?;
            if alloy_rlp::encode(&stored) != admitted.rlp() {
                return Err(ValidationError::Rejected(
                    "cached Reth block does not match submitted bytes".to_owned(),
                ));
            }
            // Reth checked header/parent consensus during import. This read supplies
            // the actual execution root, not another ancestry walk or consensus check.
            let previous_header = self
                .provider
                .header(parent.hash)
                .map_err(provider_error)?
                .ok_or_else(|| {
                    ValidationError::Unavailable(format!(
                        "Reth cannot expose selected parent {} ({})",
                        parent.number, parent.hash,
                    ))
                })?;
            if previous_header.state_root != parent.state_root {
                return Err(ValidationError::Rejected(
                    "streamed parent state root does not match Reth".to_owned(),
                ));
            }
            let receipts = self
                .provider
                .receipts_by_block(block.hash().into())
                .map_err(provider_error)?
                .ok_or_else(|| {
                    ValidationError::Unavailable(format!(
                        "Reth accepted block {number} but no longer exposes its receipts"
                    ))
                })?;

            let (checkpoint_plan, sender_flags) =
                CheckpointPlan::for_block(&block, self.expected_l2_system_address);
            let cached_checkpoints = self.checkpoints.lock().unwrap().get(&block.hash()).cloned();
            let (receipt_successes, observed_outbound_events, checkpoints) =
                if checkpoint_plan.positions().is_empty() || cached_checkpoints.is_some() {
                    (
                        receipts.iter().map(|receipt| receipt.success).collect(),
                        observe_outbound_events(&receipts),
                        cached_checkpoints.unwrap_or_default(),
                    )
                } else {
                    let provider = self.provider.clone();
                    let evm_config = self.evm_config.clone();
                    let chain_spec = Arc::clone(&self.chain_spec);
                    let checkpoint_cache = Arc::clone(&self.checkpoints);
                    let block = block.clone();
                    let checked = tokio::task::spawn_blocking(move || {
                    // Keep reconciliation serialized until replay really finishes, including on timeout.
                    let _forkchoice_guard = forkchoice_guard;
                    let parent_state =
                        provider.state_by_block_hash(parent.hash).map_err(|error| {
                            ValidationError::Unavailable(format!(
                                "Reth cannot open pending parent state for block {number}: {error}"
                            ))
                        })?;
                    let mut state = State::builder()
                        .with_database(StateProviderDatabase::new(
                            parent_state.into_evm_state_provider(),
                        ))
                        .with_bundle_update()
                        .build();
                    let (result, checkpoints, block_access_list_hash) =
                        execute_block_with_state_checkpoints(
                            &evm_config,
                            &mut state,
                            &block,
                            checkpoint_plan.positions(),
                        )?;
                    validate_block_post_execution(
                        &block,
                        chain_spec.as_ref(),
                        &result,
                        None,
                        block_access_list_hash,
                    )
                    .map_err(|error| {
                        ValidationError::Rejected(format!(
                            "stateful terminal-candidate replay rejected block {number}: {error}"
                        ))
                    })?;
                    let computed_root = state_root(&state)?;
                    if computed_root != block.header().state_root() {
                        return Err(ValidationError::Rejected(format!(
                            "stateful terminal-candidate replay of block {number} produced root \
                         {computed_root}, header claims {}",
                            block.header().state_root(),
                        )));
                    }
                    checkpoint_plan.verify_returned(&checkpoints)?;
                    let output = BackendBlockOutput {
                        computed_hash: block.hash(),
                        receipt_successes: result
                            .receipts
                            .iter()
                            .map(|receipt| receipt.success)
                            .collect(),
                        transaction_state_checkpoints: checkpoints,
                        post_state_root: computed_root,
                        settlement_evidence: SettlementBlockEvidence {
                            system_sender_flags: sender_flags,
                            observed_outbound_events: observe_outbound_events(&result.receipts),
                        },
                    };
                    let checked = block.finish(output, parent.state_root)?;
                    checkpoint_cache
                        .lock()
                        .unwrap()
                        .get_or_insert(checked.hash(), || {
                            checked.transaction_state_checkpoints().to_vec()
                        });
                    Ok::<_, ValidationError>(checked)
                })
                .await
                .map_err(|error| {
                    ValidationError::InternalInvariant(format!(
                        "stateful checkpoint worker failed: {error}"
                    ))
                })??;
                    return Ok((checked, false));
                };

            let output = BackendBlockOutput {
                computed_hash: block.hash(),
                receipt_successes,
                transaction_state_checkpoints: checkpoints,
                post_state_root: block.header().state_root(),
                settlement_evidence: SettlementBlockEvidence {
                    system_sender_flags: sender_flags,
                    observed_outbound_events,
                },
            };
            Ok((block.finish(output, previous_header.state_root())?, reused))
        })
        .await
        .map_err(|_| ValidationError::DeadlineExceeded)?
    }
}

/// Execute transactions individually to capture checkpoints and BAL indices.
fn execute_block_with_state_checkpoints(
    evm_config: &EezEvmConfig,
    state: &mut State<
        StateProviderDatabase<EvmStateProviderAdapter<Box<dyn StateProvider + Send>>>,
    >,
    block: &RecoveredBlock<Block>,
    checkpoint_positions: &[CheckpointAt],
) -> Result<
    (
        BlockExecutionResult<EthereumReceipt>,
        Vec<StateCheckpoint>,
        Option<B256>,
    ),
    ValidationError,
> {
    let has_bal = block.header().block_access_list_hash().is_some();
    let (result, checkpoints) = {
        let mut executor = evm_config
            .executor_for_block(state, block)
            .map_err(|error| {
                ValidationError::Rejected(format!("stateful block context rejected: {error}"))
            })?;
        if has_bal {
            executor.evm_mut().db_mut().bal_state.bal_builder = Some(Bal::new());
        } else {
            executor.evm_mut().db_mut().bal_state.bal_builder = None;
        }
        executor
            .apply_pre_execution_changes()
            .map_err(execution_error)?;
        if has_bal {
            executor.evm_mut().db_mut().bump_bal_index();
        }

        let mut checkpoints = Vec::with_capacity(checkpoint_positions.len());
        let mut next_checkpoint = checkpoint_positions.iter().copied().peekable();
        // Sealed before the loop: the anchor's candidate is this block holding
        // no transactions, after the pre-block system calls have run.
        if next_checkpoint.peek() == Some(&CheckpointAt::PreExecution) {
            let state_root = {
                let db = executor.evm_mut().db_mut();
                db.merge_transitions(BundleRetention::Reverts);
                state_root(db)?
            };
            let block_hash =
                candidate_block_hash::<EezPrimitives>(block.sealed_header(), &[], &[], state_root)
                    .map_err(|error| {
                        ValidationError::Rejected(format!(
                            "stateful candidate block hash rejected: {error}"
                        ))
                    })?;
            checkpoints.push(StateCheckpoint {
                at: CheckpointAt::PreExecution,
                state_root,
                block_hash,
            });
            next_checkpoint.next();
        }
        for (transaction_index, transaction) in block.transactions_recovered().enumerate() {
            executor
                .execute_transaction(transaction)
                .map_err(execution_error)?;
            if has_bal {
                executor.evm_mut().db_mut().bump_bal_index();
            }
            if next_checkpoint.peek() == Some(&CheckpointAt::Transaction(transaction_index)) {
                let state_root = {
                    let db = executor.evm_mut().db_mut();
                    db.merge_transitions(BundleRetention::Reverts);
                    state_root(db)?
                };
                let block_hash = candidate_block_hash::<EezPrimitives>(
                    block.sealed_header(),
                    &block.body().transactions[..=transaction_index],
                    &executor.receipts()[..=transaction_index],
                    state_root,
                )
                .map_err(|error| {
                    ValidationError::Rejected(format!(
                        "stateful candidate block hash rejected: {error}"
                    ))
                })?;
                checkpoints.push(StateCheckpoint {
                    at: CheckpointAt::Transaction(transaction_index),
                    state_root,
                    block_hash,
                });
                next_checkpoint.next();
            }
        }
        let result = executor
            .apply_post_execution_changes()
            .map_err(execution_error)?;
        (result, checkpoints)
    };
    state.merge_transitions(BundleRetention::Reverts);
    let block_access_list_hash = state
        .take_built_alloy_bal()
        .as_ref()
        .map(|list| compute_block_access_list_hash(list));
    Ok((result, checkpoints, block_access_list_hash))
}

fn state_root(
    state: &State<StateProviderDatabase<EvmStateProviderAdapter<Box<dyn StateProvider + Send>>>>,
) -> Result<B256, ValidationError> {
    let provider = &state.database.0;
    let hashed_state = provider
        .hashed_post_state(&state.bundle_state)
        .map_err(provider_error)?;
    provider.state_root(hashed_state).map_err(provider_error)
}

fn canonical_hash<P: BlockHashReader>(provider: &P, number: u64) -> Result<B256, ValidationError> {
    provider
        .block_hash(number)
        .map_err(provider_error)?
        .ok_or_else(|| {
            ValidationError::Unavailable(format!("canonical block {number} is not available"))
        })
}

fn provider_error(error: impl std::fmt::Display) -> ValidationError {
    ValidationError::Unavailable(format!("local node provider failed: {error}"))
}

fn execution_error(error: BlockExecutionError) -> ValidationError {
    match error {
        BlockExecutionError::Validation(error) => {
            ValidationError::Rejected(format!("stateful execution rejected: {error}"))
        }
        BlockExecutionError::Internal(error) => {
            ValidationError::Unavailable(format!("stateful execution unavailable: {error}"))
        }
    }
}

#[cfg(test)]
mod tests;
