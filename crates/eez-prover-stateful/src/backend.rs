//! Node-backed execution evidence.
//!
//! Legacy v1 requests open historical state and replay a complete window into
//! a disposable revm overlay. Incremental v2 requests import candidate unsafe
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
use eez_proof_signer::cancel::CancellationToken;
use eez_proof_signer::validate::support::{
    CheckpointPlan, check_cancellation, decode_match_and_recover_signers, observe_outbound_events,
    system_sender_flags,
};
use eez_proof_signer::validate::{
    AdmittedBlock, BackendBlockOutput, BackendWindowOutput, IncrementalAnchor,
    IncrementalBlockOutput, SettlementBlockEvidence, TransactionStateCheckpoint,
    ValidatedBlockArtifact, ValidationBackend, ValidationError,
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
    BlockHashReader, BlockIdReader, BlockNumReader, BlockReader, HashedPostStateProvider,
    HeaderProvider, StateProvider, StateProviderFactory, StateRootProvider,
};
use revm::database::states::bundle_state::BundleRetention;
use revm::state::bal::Bal;
use stateless_reth::candidate_block_hash;
use tracing::{debug, trace};

const MAX_CACHED_CHECKPOINT_BLOCKS: usize = 32_768;

/// Stateful validation over one live Ethereum node provider.
#[derive(Debug)]
pub struct Backend<P> {
    provider: P,
    chain_spec: Arc<ChainSpec>,
    evm_config: EezEvmConfig,
    expected_l2_system_address: Address,
    committer: Option<BlockCommitterHandle<EezEngineTypes>>,
    // Reth owns blocks, state and receipts; retain only supplemental checkpoints here.
    checkpoints: Arc<Mutex<LruCache<B256, Vec<TransactionStateCheckpoint>>>>,
}

impl<P> Backend<P> {
    /// Bind the backend to the follower's provider and execution rules.
    pub fn new(
        provider: P,
        chain_spec: Arc<ChainSpec>,
        expected_l2_system_address: Address,
    ) -> Self {
        let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            provider,
            chain_spec,
            evm_config,
            expected_l2_system_address,
            committer: None,
            checkpoints: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(MAX_CACHED_CHECKPOINT_BLOCKS).unwrap(),
            ))),
        }
    }

    /// Enable v2 ingestion through the follower's existing Reth block tree.
    pub fn with_committer(
        provider: P,
        chain_spec: Arc<ChainSpec>,
        expected_l2_system_address: Address,
        committer: BlockCommitterHandle<EezEngineTypes>,
    ) -> Self {
        Self {
            committer: Some(committer),
            ..Self::new(provider, chain_spec, expected_l2_system_address)
        }
    }

    fn committer(&self) -> Result<&BlockCommitterHandle<EezEngineTypes>, ValidationError> {
        self.committer.as_ref().ok_or_else(|| {
            ValidationError::Unavailable(
                "stateful backend was started without a Reth block committer".to_owned(),
            )
        })
    }
}

fn ensure_safe_anchor<P>(provider: &P, anchor: IncrementalAnchor) -> Result<(), ValidationError>
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

fn ancestor_hash<P>(
    provider: &P,
    current: &SealedHeader<alloy_consensus::Header>,
    target_number: u64,
) -> Result<B256, ValidationError>
where
    P: HeaderProvider<Header = alloy_consensus::Header>,
{
    if target_number > current.number() {
        return Err(ValidationError::Aborted(format!(
            "local safe block {target_number} is ahead of session cursor {}",
            current.number(),
        )));
    }
    let mut cursor = current.clone();
    while cursor.number() > target_number {
        let parent_hash = cursor.parent_hash();
        let parent = provider
            .header(parent_hash)
            .map_err(provider_error)?
            .ok_or_else(|| {
                ValidationError::Unavailable(format!(
                    "Reth no longer exposes ancestor {parent_hash} of block {}",
                    cursor.number(),
                ))
            })?;
        if parent.number.checked_add(1) != Some(cursor.number()) {
            return Err(ValidationError::InvalidBackendOutput(format!(
                "Reth returned a non-contiguous ancestor for block {}",
                cursor.number(),
            )));
        }
        cursor = SealedHeader::new(parent, parent_hash);
    }
    Ok(cursor.hash())
}

/// Accept safe-head movement only when Reth proves that both the original
/// session anchor and the fresh safe marker are exact ancestors of the current
/// validated cursor. This lets a Composer keep streaming after L1 advances
/// inside its retained prefix without trusting the Composer's claim.
fn ensure_safe_ancestor<P>(
    provider: &P,
    minimum_anchor: IncrementalAnchor,
    current: &SealedHeader<alloy_consensus::Header>,
) -> Result<(), ValidationError>
where
    P: BlockIdReader + HeaderProvider<Header = alloy_consensus::Header>,
{
    if ancestor_hash(provider, current, minimum_anchor.number)? != minimum_anchor.hash {
        return Err(ValidationError::Aborted(format!(
            "session cursor {} is no longer descended from anchor {} ({})",
            current.number(),
            minimum_anchor.number,
            minimum_anchor.hash,
        )));
    }
    match provider.safe_block_num_hash().map_err(provider_error)? {
        Some(safe) if safe.number < minimum_anchor.number => {
            Err(ValidationError::Aborted(format!(
                "L1-derived safe anchor moved behind session anchor {}; local safe is {safe:?}",
                minimum_anchor.number,
            )))
        }
        Some(safe) if ancestor_hash(provider, current, safe.number)? == safe.hash => Ok(()),
        Some(safe) => Err(ValidationError::Aborted(format!(
            "local safe block {safe:?} is not an ancestor of session cursor {} ({})",
            current.number(),
            current.hash(),
        ))),
        None if minimum_anchor.number == 0 => Ok(()),
        None => Err(ValidationError::Aborted(format!(
            "local safe marker disappeared for session anchor {} ({})",
            minimum_anchor.number, minimum_anchor.hash,
        ))),
    }
}

#[async_trait::async_trait]
impl<P> ValidationBackend for Backend<P>
where
    P: BlockHashReader
        + BlockIdReader
        + BlockNumReader
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

    fn begin_incremental(&self, anchor: IncrementalAnchor) -> Result<(), ValidationError> {
        self.committer()?;
        ensure_safe_anchor(&self.provider, anchor)
    }

    async fn recheck_incremental(
        &self,
        anchor: IncrementalAnchor,
        blocks: &[Arc<ValidatedBlockArtifact>],
    ) -> Result<(), ValidationError> {
        let committer = self.committer()?;
        let _forkchoice_guard = committer.begin_reconcile().await;
        let mut expected_number = anchor.number.checked_add(1).ok_or_else(|| {
            ValidationError::Rejected("incremental anchor block number overflow".to_owned())
        })?;
        let mut expected_parent = anchor.hash;
        let mut terminal = None;
        for block in blocks {
            let (number, hash, parent_hash, rlp) = (
                block.number(),
                block.hash(),
                block.parent_hash(),
                block.rlp(),
            );
            if number != expected_number || parent_hash != expected_parent {
                return Err(ValidationError::InvalidBackendOutput(format!(
                    "validated stateful range is not contiguous at block {number}",
                )));
            }
            let decoded = alloy_rlp::decode_exact::<Block>(rlp).map_err(|error| {
                ValidationError::InvalidBackendOutput(format!(
                    "validated block {number} no longer exact-decodes: {error}",
                ))
            })?;
            let computed_hash = decoded.header.hash_slow();
            if decoded.header.number != number
                || decoded.header.parent_hash != parent_hash
                || computed_hash != hash
            {
                return Err(ValidationError::InvalidBackendOutput(format!(
                    "validated block {number} identity changed before finalization",
                )));
            }
            let header = SealedHeader::new(decoded.header.clone(), hash);
            let known = self
                .provider
                .find_block_by_hash(hash, reth_storage_api::BlockSource::Any)
                .map_err(provider_error)?
                .ok_or_else(|| {
                    ValidationError::Aborted(format!(
                        "validated block {number} ({hash}) is no longer retained by Reth",
                    ))
                })?;
            if known.header.number != number
                || known.header.hash_slow() != hash
                || alloy_rlp::encode(&known) != rlp
            {
                return Err(ValidationError::Aborted(format!(
                    "Reth block identity changed at height {number}",
                )));
            }
            terminal = Some(header);
            expected_number = expected_number.checked_add(1).ok_or_else(|| {
                ValidationError::Rejected("incremental block number overflow".to_owned())
            })?;
            expected_parent = hash;
        }

        let terminal = terminal.ok_or_else(|| {
            ValidationError::Rejected("incremental finalization range is empty".to_owned())
        })?;
        // The blocks were imported and executed when their Validated frames
        // were produced. Finalization must not replay the whole prefix through
        // newPayload: repeatedly walking the canonical head back to block 1
        // races Reth's persistence pipeline on large windows. Prove the exact
        // retained ancestry above the current L1-safe marker, then select the
        // already-known terminal block with one FCU.
        ensure_safe_ancestor(&self.provider, anchor, &terminal)?;
        match committer
            .advance_unsafe_head(terminal.clone())
            .await
            .map_err(|error| {
                if error.is_invalid_forkchoice() {
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
        ensure_safe_ancestor(&self.provider, anchor, &terminal)?;
        Ok(())
    }

    async fn validate_next(
        &self,
        anchor: IncrementalAnchor,
        parent: IncrementalAnchor,
        admitted: &AdmittedBlock,
        _witness: ExecutionWitness,
        request_timeout: Duration,
    ) -> Result<(IncrementalBlockOutput, bool), ValidationError> {
        tokio::time::timeout(request_timeout, async {
            let expected_number = parent.number.checked_add(1).ok_or_else(|| {
                ValidationError::Rejected("incremental block number overflow".to_owned())
            })?;
            if admitted.declared_number() != expected_number
                || admitted.claimed_parent_hash() != parent.hash
            {
                return Err(ValidationError::Rejected(format!(
                    "incremental block {} does not extend {} ({})",
                    admitted.declared_number(),
                    parent.number,
                    parent.hash,
                )));
            }

            // Reth owns every branch's state. Serialize parent lookup, unsafe-head
            // selection and outcome reads with L1 reconciliation and other sessions.
            let committer = self.committer()?;
            let _forkchoice_guard = committer.begin_reconcile().await;
            let previous_header = self
                .provider
                .header(parent.hash)
                .map_err(provider_error)?
                .ok_or_else(|| {
                    ValidationError::Aborted(format!(
                        "validated parent {} ({}) is no longer retained by Reth",
                        parent.number, parent.hash,
                    ))
                })?;
            if previous_header.number != parent.number
                || previous_header.hash_slow() != parent.hash
                || previous_header.state_root != parent.state_root
            {
                return Err(ValidationError::InvalidBackendOutput(
                    "Reth parent does not match its validated identity and state root".to_owned(),
                ));
            }
            let previous_header = SealedHeader::new(previous_header, parent.hash);
            ensure_safe_ancestor(&self.provider, anchor, &previous_header)?;

            let block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
            let number = block.header().number();
            let consensus = EthBeaconConsensus::new(Arc::clone(&self.chain_spec));
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
                        "stateful consensus validation rejected block {number}: {error}"
                    ))
                })?;

            let mut reused = if let Some(stored) = self
                .provider
                .find_block_by_hash(block.hash(), reth_storage_api::BlockSource::Any)
                .map_err(provider_error)?
            {
                if alloy_rlp::encode(&stored) != admitted.rlp() {
                    return Err(ValidationError::Rejected(
                        "cached Reth block does not match submitted bytes".to_owned(),
                    ));
                }
                self.provider
                    .receipts_by_block(block.hash().into())
                    .map_err(provider_error)?
                    .is_some()
            } else {
                false
            };
            if !reused {
                let sealed_block = block.sealed_block().clone();
                let header = sealed_block.clone_sealed_header();
                let payload = EezEngineTypes::block_to_payload(sealed_block, None);
                committer
                    .commit_derived(payload, header, false)
                    .await
                    .map_err(|error| {
                        if error.is_invalid_payload() || error.is_invalid_forkchoice() {
                            ValidationError::Rejected(format!(
                                "Reth rejected incremental block {number}: {error}"
                            ))
                        } else {
                            ValidationError::Unavailable(format!(
                                "Reth could not import incremental block {number}: {error}"
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
            if stored.header.number != number || stored.header.hash_slow() != block.hash() {
                return Err(ValidationError::Aborted(format!(
                    "stored head changed while validating block {number}: expected {}, got {}",
                    block.hash(),
                    stored.header.hash_slow(),
                )));
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
                CheckpointPlan::from_recovered_block(&block, self.expected_l2_system_address);
            let cached_checkpoints = self.checkpoints.lock().unwrap().get(&block.hash()).cloned();
            let (receipt_successes, observed_outbound_events, checkpoints) =
                if checkpoint_plan.transaction_indices().is_empty() {
                    (
                        receipts.iter().map(|receipt| receipt.success).collect(),
                        observe_outbound_events(&receipts),
                        Vec::new(),
                    )
                } else if let Some(checkpoints) = cached_checkpoints {
                    (
                        receipts.iter().map(|receipt| receipt.success).collect(),
                        observe_outbound_events(&receipts),
                        checkpoints,
                    )
                } else {
                    reused = false;
                    let provider = self.provider.clone();
                    let evm_config = self.evm_config.clone();
                    let chain_spec = Arc::clone(&self.chain_spec);
                    let checkpoint_cache = Arc::clone(&self.checkpoints);
                    let block = block.clone();
                    tokio::task::spawn_blocking(move || {
                    // Keep reconciliation serialized until replay really finishes, including on timeout.
                    let _forkchoice_guard = _forkchoice_guard;
                    let parent_state =
                        provider.state_by_block_hash(parent.hash).map_err(|error| {
                            ValidationError::Unavailable(format!(
                                "Reth cannot open pending parent state for block {number}: {error}"
                            ))
                        })?;
                    let mut state = State::builder()
                        .with_database(StateProviderDatabase::new(parent_state))
                        .with_bundle_update()
                        .build();
                    let (result, checkpoints, block_access_list_hash) =
                        execute_block_with_state_checkpoints(
                            &evm_config,
                            &mut state,
                            &block,
                            checkpoint_plan.transaction_indices(),
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
                    let checkpoints = checkpoint_cache
                        .lock()
                        .unwrap()
                        .get_or_insert(block.hash(), || checkpoints)
                        .clone();
                    Ok::<_, ValidationError>((
                        result
                            .receipts
                            .iter()
                            .map(|receipt| receipt.success)
                            .collect(),
                        observe_outbound_events(&result.receipts),
                        checkpoints,
                    ))
                })
                .await
                .map_err(|error| {
                    ValidationError::InternalInvariant(format!(
                        "stateful checkpoint worker failed: {error}"
                    ))
                })??
                };

            let output = BackendBlockOutput {
                decoded_number: number,
                decoded_parent_hash: block.header().parent_hash(),
                computed_hash: block.hash(),
                decoded_transaction_count: block.body().transactions.len(),
                receipt_successes,
                transaction_state_checkpoints: checkpoints,
                post_state_root: block.header().state_root(),
                settlement_evidence: SettlementBlockEvidence {
                    system_sender_flags: sender_flags,
                    observed_outbound_events,
                },
            };
            Ok((
                IncrementalBlockOutput {
                    pre_state_root: previous_header.state_root(),
                    block: output,
                },
                reused,
            ))
        })
        .await
        .map_err(|_| ValidationError::DeadlineExceeded)?
    }
}

fn validate_blocks<P>(
    provider: &P,
    chain_spec: &Arc<ChainSpec>,
    evm_config: &EezEvmConfig,
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

    Ok(BackendWindowOutput { blocks: outputs })
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
    evm_config: &EezEvmConfig,
    state: &mut State<StateProviderDatabase<Box<dyn StateProvider + Send>>>,
    block: &RecoveredBlock<Block>,
) -> Result<BlockExecutionResult<EthereumReceipt>, ValidationError> {
    state.bal_state.bal_builder = None;
    let executor = evm_config
        .executor_for_block(state, block)
        .map_err(|error| {
            ValidationError::Rejected(format!("stateful block context rejected: {error}"))
        })?;
    let result = executor
        .execute_block(block.transactions_recovered())
        .map_err(execution_error)?;
    state.merge_transitions(BundleRetention::Reverts);
    Ok(result)
}

/// Execute transactions individually to capture checkpoints and BAL indices.
fn execute_block_with_state_checkpoints(
    evm_config: &EezEvmConfig,
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
                checkpoints.push(TransactionStateCheckpoint {
                    transaction_index,
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
