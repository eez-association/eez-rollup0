//! Node-backed execution evidence.
//!
//! The follower database is never mutated. Each request opens historical state
//! at the proposed window's parent and replays the complete window into a
//! disposable revm overlay.

use std::sync::Arc;

use alloy_consensus::{BlockHeader as _, EthereumReceipt};
use alloy_eips::eip7928::compute_block_access_list_hash;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_proof_signer::cancel::CancellationToken;
use eez_proof_signer::validate::support::{
    CheckpointPlan, check_cancellation, decode_match_and_recover_signers, observe_outbound_events,
    system_sender_flags,
};
use eez_proof_signer::validate::{
    BackendBlockOutput, BackendWindowOutput, SettlementBlockEvidence, TransactionStateCheckpoint,
    ValidationBackend, ValidationError,
};
use eez_proof_signer::window::AdmittedBlock;
use reth_chainspec::ChainSpec;
use reth_consensus::{Consensus as _, HeaderValidator as _};
use reth_ethereum_consensus::{EthBeaconConsensus, validate_block_post_execution};
use reth_ethereum_primitives::Block;
use reth_evm::block::BlockExecutionError;
use reth_evm::execute::BlockExecutor as _;
use reth_evm::{ConfigureEvm as _, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_execution_types::BlockExecutionResult;
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_revm::State;
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{
    BlockHashReader, BlockNumReader, HashedPostStateProvider, HeaderProvider, StateProvider,
    StateProviderFactory, StateRootProvider,
};
use revm::database::states::bundle_state::BundleRetention;
use revm::state::bal::Bal;
use tracing::{debug, trace};

/// Stateful validation over one live Ethereum node provider.
#[derive(Debug)]
pub struct Backend<P> {
    provider: P,
    chain_spec: Arc<ChainSpec>,
    evm_config: EthEvmConfig,
    expected_l2_system_address: Address,
}

impl<P> Backend<P> {
    /// Bind the backend to the follower's provider and execution rules.
    pub fn new(
        provider: P,
        chain_spec: Arc<ChainSpec>,
        expected_l2_system_address: Address,
    ) -> Self {
        let evm_config = EthEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            provider,
            chain_spec,
            evm_config,
            expected_l2_system_address,
        }
    }
}

impl<P> ValidationBackend for Backend<P>
where
    P: BlockHashReader
        + BlockNumReader
        + HeaderProvider<Header = alloy_consensus::Header>
        + StateProviderFactory
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

    fn validate_blocks(
        &self,
        blocks: &[AdmittedBlock],
        _witnesses: &mut [ExecutionWitness],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError> {
        validate_blocks(
            &self.provider,
            &self.chain_spec,
            &self.evm_config,
            self.expected_l2_system_address,
            blocks,
            cancellation,
        )
    }
}

fn validate_blocks<P>(
    provider: &P,
    chain_spec: &Arc<ChainSpec>,
    evm_config: &EthEvmConfig,
    expected_l2_system_address: Address,
    blocks: &[AdmittedBlock],
    cancellation: &CancellationToken,
) -> Result<BackendWindowOutput, ValidationError>
where
    P: BlockHashReader
        + BlockNumReader
        + HeaderProvider<Header = alloy_consensus::Header>
        + StateProviderFactory,
{
    check_cancellation(cancellation, 0, blocks.len())?;
    let first = blocks.first().ok_or_else(|| {
        ValidationError::Rejected("refusing to validate an empty window".to_owned())
    })?;
    let anchor_number = first.declared_number().checked_sub(1).ok_or_else(|| {
        ValidationError::Rejected("stateful windows cannot start at genesis".to_owned())
    })?;
    let claimed_anchor_hash = first.claimed_parent_hash();
    let total_blocks = blocks.len();

    // Derive the settling block's complete checkpoint plan before opening
    // state or replaying any preceding block.
    let settling_block = blocks
        .last()
        .expect("the admitted stateful window was checked as nonempty");
    let recovered_settling_block = decode_match_and_recover_signers(settling_block, chain_spec)?;
    let (settling_checkpoint_plan, settling_sender_flags) =
        CheckpointPlan::from_recovered_block(&recovered_settling_block, expected_l2_system_address);
    let mut prepared_settling_block = Some((
        recovered_settling_block,
        settling_checkpoint_plan,
        settling_sender_flags,
    ));

    let local_height = provider.best_block_number().map_err(provider_error)?;
    if local_height < anchor_number {
        return Err(ValidationError::Unavailable(format!(
            "follower is at block {local_height}, request requires anchor block {anchor_number}",
        )));
    }
    let local_anchor_hash = canonical_hash(provider, anchor_number)?;
    if local_anchor_hash != claimed_anchor_hash {
        return Err(ValidationError::Rejected(format!(
            "local canonical block {anchor_number} is {local_anchor_hash}, request anchors to {claimed_anchor_hash}",
        )));
    }

    // If the follower already has part of the proposed range, a conflicting
    // local canonical block makes this exact request permanently invalid.
    for admitted in blocks
        .iter()
        .take_while(|block| block.declared_number() <= local_height)
    {
        let number = admitted.declared_number();
        let local_hash = canonical_hash(provider, number)?;
        if local_hash != admitted.claimed_hash() {
            return Err(ValidationError::Rejected(format!(
                "local canonical block {number} is {local_hash}, request proposes {}",
                admitted.claimed_hash(),
            )));
        }
    }

    let anchor_header = provider
        .header(claimed_anchor_hash)
        .map_err(provider_error)?
        .ok_or_else(|| {
            ValidationError::Unavailable(format!(
                "local canonical header {anchor_number} is not available",
            ))
        })?;
    let anchor_state = provider
        .state_by_block_hash(claimed_anchor_hash)
        .map_err(|error| {
            ValidationError::Unavailable(format!(
                "local node cannot open state for canonical anchor block {anchor_number}: {error}",
            ))
        })?;
    let mut state = State::builder()
        .with_database(StateProviderDatabase::new(anchor_state))
        .with_bundle_update()
        .build();
    let pre_state_root = anchor_header.state_root;
    let mut previous_header = SealedHeader::new(anchor_header, claimed_anchor_hash);
    let consensus = EthBeaconConsensus::new(Arc::clone(chain_spec));
    let mut outputs = Vec::with_capacity(total_blocks);

    for (index, admitted) in blocks.iter().enumerate() {
        check_cancellation(cancellation, index, total_blocks)?;
        let is_settling = index + 1 == total_blocks;
        let (block, checkpoint_indices, sender_flags) = if is_settling {
            let (block, plan, flags) = prepared_settling_block.take().ok_or_else(|| {
                ValidationError::InternalInvariant(
                    "prepared stateful settling block unexpectedly missing".to_owned(),
                )
            })?;
            (block, plan.transaction_indices().to_vec(), flags)
        } else {
            let block = decode_match_and_recover_signers(admitted, chain_spec)?;
            let flags = system_sender_flags(&block, expected_l2_system_address);
            (block, Vec::new(), flags)
        };
        let number = block.header().number();
        consensus
            .validate_header(block.sealed_block().sealed_header())
            .and_then(|()| {
                consensus.validate_header_against_parent(
                    block.sealed_block().sealed_header(),
                    &previous_header,
                )
            })
            .and_then(|()| consensus.validate_block_pre_execution(block.sealed_block()))
            .map_err(|error| {
                ValidationError::Rejected(format!(
                    "stateful consensus validation rejected block {number}: {error}",
                ))
            })?;
        trace!(
            block_number = number,
            transactions = block.body().transactions.len(),
            "replaying stateful proof block",
        );
        let (result, checkpoints, block_access_list_hash) =
            if checkpoint_indices.is_empty() && block.header().block_access_list_hash().is_none() {
                (
                    execute_block(evm_config, &mut state, &block)?,
                    Vec::new(),
                    None,
                )
            } else {
                execute_block_with_state_checkpoints(
                    evm_config,
                    &mut state,
                    &block,
                    &checkpoint_indices,
                )?
            };
        validate_block_post_execution(
            &block,
            chain_spec.as_ref(),
            &result,
            None,
            block_access_list_hash,
        )
        .map_err(|error| {
            ValidationError::Rejected(format!(
                "stateful post-execution validation rejected block {number}: {error}",
            ))
        })?;

        let post_state_root = state_root(&state)?;
        if post_state_root != block.header().state_root() {
            return Err(ValidationError::Rejected(format!(
                "stateful replay of block {number} produced root {post_state_root}, header claims {}",
                block.header().state_root(),
            )));
        }
        state.block_hashes.insert(number, block.hash());
        previous_header = block.sealed_block().clone_sealed_header();

        let receipt_successes = result
            .receipts
            .iter()
            .map(|receipt| receipt.success)
            .collect();
        let observed_outbound_events = observe_outbound_events(&result.receipts);
        outputs.push(BackendBlockOutput {
            decoded_number: number,
            decoded_parent_hash: block.header().parent_hash(),
            computed_hash: block.hash(),
            decoded_transaction_count: block.body().transactions.len(),
            receipt_successes,
            transaction_state_checkpoints: checkpoints,
            post_state_root,
            settlement_evidence: SettlementBlockEvidence {
                system_sender_flags: sender_flags,
                observed_outbound_events,
            },
        });
        debug!(block_number = number, %post_state_root, "stateful proof block validated");
    }

    check_canonical_snapshot(
        provider,
        anchor_number,
        claimed_anchor_hash,
        local_height,
        blocks,
    )?;

    Ok(BackendWindowOutput {
        pre_state_root,
        blocks: outputs,
    })
}

/// Reject a torn canonical view instead of signing evidence whose anchor or
/// known requested blocks changed while replay was executing.
fn check_canonical_snapshot<P>(
    provider: &P,
    anchor_number: u64,
    claimed_anchor_hash: B256,
    local_height_before: u64,
    blocks: &[AdmittedBlock],
) -> Result<(), ValidationError>
where
    P: BlockHashReader + BlockNumReader,
{
    let anchor_after = provider.block_hash(anchor_number).map_err(provider_error)?;
    if anchor_after != Some(claimed_anchor_hash) {
        return Err(ValidationError::Aborted(format!(
            "canonical anchor {anchor_number} changed from {claimed_anchor_hash} to {anchor_after:?} during replay",
        )));
    }
    let local_height_after = provider.best_block_number().map_err(provider_error)?;
    for admitted in blocks
        .iter()
        .take_while(|block| block.declared_number() <= local_height_before.max(local_height_after))
    {
        let number = admitted.declared_number();
        let current_hash = provider.block_hash(number).map_err(provider_error)?;
        if current_hash != Some(admitted.claimed_hash()) {
            return Err(ValidationError::Aborted(format!(
                "canonical block {number} changed from {} to {current_hash:?} during replay",
                admitted.claimed_hash(),
            )));
        }
    }
    Ok(())
}

/// Use Reth's normal block flow when no checkpoints or BAL output are needed.
fn execute_block(
    evm_config: &EthEvmConfig,
    state: &mut State<StateProviderDatabase<Box<dyn StateProvider + Send>>>,
    block: &RecoveredBlock<Block>,
) -> Result<BlockExecutionResult<EthereumReceipt>, ValidationError> {
    state.bal_state.bal_builder = None;
    let executor = evm_config
        .executor_for_block(state, block)
        .map_err(|never| match never {})?;
    let result = executor
        .execute_block(block.transactions_recovered())
        .map_err(execution_error)?;
    state.merge_transitions(BundleRetention::Reverts);
    Ok(result)
}

/// Execute transactions individually to capture checkpoints and BAL indices.
fn execute_block_with_state_checkpoints(
    evm_config: &EthEvmConfig,
    state: &mut State<StateProviderDatabase<Box<dyn StateProvider + Send>>>,
    block: &RecoveredBlock<Block>,
    checkpoint_indices: &[usize],
) -> Result<
    (
        BlockExecutionResult<EthereumReceipt>,
        Vec<TransactionStateCheckpoint>,
        Option<B256>,
    ),
    ValidationError,
> {
    let has_bal = block.header().block_access_list_hash().is_some();
    let (result, checkpoints) = {
        let mut executor = evm_config
            .executor_for_block(state, block)
            .map_err(|never| match never {})?;
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

        let mut checkpoints = Vec::with_capacity(checkpoint_indices.len());
        let mut next_checkpoint = checkpoint_indices.iter().copied().peekable();
        for (transaction_index, transaction) in block.transactions_recovered().enumerate() {
            executor
                .execute_transaction(transaction)
                .map_err(execution_error)?;
            if has_bal {
                executor.evm_mut().db_mut().bump_bal_index();
            }
            if next_checkpoint.peek() == Some(&transaction_index) {
                let db = executor.evm_mut().db_mut();
                db.merge_transitions(BundleRetention::Reverts);
                checkpoints.push(TransactionStateCheckpoint {
                    transaction_index,
                    state_root: state_root(db)?,
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
    state: &State<StateProviderDatabase<Box<dyn StateProvider + Send>>>,
) -> Result<B256, ValidationError> {
    let provider = &state.database.0;
    let hashed_state = provider.hashed_post_state(&state.bundle_state);
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
