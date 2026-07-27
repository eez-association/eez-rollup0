//! Adapter for in-process Stateless validation.
//!
//! This module exact-decodes and binds streamed block claims, loads the
//! operator-configured chain trust input, derives checkpoint plans, invokes
//! Stateless/Reth, checks window continuity, and maps validated execution facts
//! into associated per-block output.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use alloy_consensus::{EthereumReceipt, Transaction as _};
#[cfg(test)]
use alloy_genesis::ChainConfig;
use alloy_genesis::Genesis;
use alloy_sol_types::SolEvent as _;
use eez_evm::SYSTEM_ADDRESS;
use eez_evm::settlement::{is_system_tx, pair_end_positions};
use eez_evm::types::CrossChainCallExecuted;
use reth_chainspec_stateless::{ChainSpec, EthereumHardforks as _};
use reth_ethereum_primitives_stateless::Block;
use reth_evm_ethereum_stateless::EthEvmConfig;
use reth_primitives_traits_stateless::RecoveredBlock;
use stateless_reth::validation::StatelessValidationError;
use stateless_reth::{
    StatelessValidationOutput, stateless_validation_recovered,
    stateless_validation_recovered_with_state_checkpoints,
};
use tracing::{debug, info, trace};

use super::{
    BackendBlockOutput, BackendWindowOutput, OutboundEventObservation, SettlementBlockEvidence,
    TransactionStateCheckpoint, ValidationError,
};
use crate::EEZL2_ADDRESS;
use crate::cancel::CancellationToken;
use crate::window::AdmittedBlock;

mod chain_config;

use chain_config::{ChainDocumentKind, load_chain_document};

const DEBUG_PROGRESS_INTERVAL: usize = 100;

/// Transaction boundaries at which settlement framing needs state roots.
///
/// The indices are derived locally from the recovered block. Composer-supplied
/// metadata never participates in checkpoint selection.
#[derive(Debug, PartialEq, Eq)]
struct CheckpointPlan {
    transaction_indices: Vec<usize>,
}

impl CheckpointPlan {
    /// Enforce the checkpoint trie-reconstruction quota.
    fn try_new(
        transaction_indices: Vec<usize>,
        max_checkpoints: usize,
    ) -> Result<Self, ValidationError> {
        if transaction_indices.len() > max_checkpoints {
            return Err(ValidationError::CheckpointLimit {
                requested: transaction_indices.len(),
                max: max_checkpoints,
            });
        }
        Ok(Self {
            transaction_indices,
        })
    }

    /// Derive effect-candidate boundaries and system-sender flags from the
    /// locally recovered transactions.
    fn from_recovered_block(
        block: &RecoveredBlock<Block>,
        max_checkpoints: usize,
    ) -> Result<(Self, Vec<bool>), ValidationError> {
        let transaction_count = block.body().transactions.len();
        let mut system_sender_flags = Vec::with_capacity(transaction_count);
        let mut sync_system_transaction_flags = Vec::with_capacity(transaction_count);
        for transaction in block.transactions_recovered() {
            let is_system_sender = transaction.signer() == SYSTEM_ADDRESS;
            system_sender_flags.push(is_system_sender);
            sync_system_transaction_flags.push(is_system_tx(
                transaction.signer(),
                transaction.to(),
                EEZL2_ADDRESS,
            ));
        }
        let plan = Self::try_new(
            pair_end_positions(&sync_system_transaction_flags),
            max_checkpoints,
        )?;
        Ok((plan, system_sender_flags))
    }

    /// Check that a checkpoint-capable backend honored the selection exactly.
    fn verify_returned(
        &self,
        checkpoints: &[TransactionStateCheckpoint],
    ) -> Result<(), ValidationError> {
        let selection_matches = checkpoints.len() == self.transaction_indices.len()
            && checkpoints
                .iter()
                .zip(&self.transaction_indices)
                .all(|(checkpoint, requested)| checkpoint.transaction_index == *requested);
        if !selection_matches {
            let returned = checkpoints
                .iter()
                .map(|checkpoint| checkpoint.transaction_index)
                .collect::<Vec<_>>();
            return Err(ValidationError::InvalidBackendOutput(format!(
                "stateless checkpoint response targeted transaction indices {returned:?}; \
                 requested {:?}",
                self.transaction_indices,
            )));
        }
        Ok(())
    }
}

/// Signer-recovered settling block prepared before replay.
///
/// This block is exact-decoded and its signers are recovered, but it remains
/// untrusted until Stateless accepts the same `RecoveredBlock`. Preparing it
/// early lets an excessive checkpoint plan fail before earlier blocks execute.
struct PreparedSettlingBlock {
    block: RecoveredBlock<Block>,
    checkpoint_plan: CheckpointPlan,
    system_sender_flags: Vec<bool>,
}

/// In-process Stateless/Reth backend configured from operator-selected rules.
#[derive(Debug)]
pub(crate) struct Backend {
    chain_spec: Arc<ChainSpec>,
    evm_config: EthEvmConfig,
}

impl Backend {
    /// EIP-155 identity pinned by the loaded execution chain specification.
    pub(super) fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    pub(super) fn from_chain_document_file(path: &Path) -> eyre::Result<Self> {
        let (genesis, document_kind) = load_chain_document(path)?;
        let chain_config = &genesis.config;
        let genesis_timestamp =
            (document_kind == ChainDocumentKind::Genesis).then_some(genesis.timestamp);
        info!(
            path = %path.display(),
            source = document_kind.label(),
            chain_id = chain_config.chain_id,
            "loaded stateless chain configuration",
        );
        debug!(
            ?chain_config,
            ?genesis_timestamp,
            "stateless chain configuration details",
        );
        Ok(Self::from_genesis(genesis))
    }

    #[cfg(test)]
    pub(super) fn new(chain_config: ChainConfig) -> Self {
        Self::from_genesis(Genesis {
            config: chain_config,
            ..Default::default()
        })
    }

    fn from_genesis(genesis: Genesis) -> Self {
        let chain_spec = Arc::new(ChainSpec::from_genesis(genesis));
        let evm_config = EthEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            chain_spec,
            evm_config,
        }
    }

    #[cfg(test)]
    pub(super) fn validate(
        &self,
        blocks: &[AdmittedBlock],
    ) -> Result<BackendWindowOutput, ValidationError> {
        self.validate_blocks(
            &mut blocks.to_vec(),
            &CancellationToken::default(),
            usize::MAX,
        )
    }

    /// Replay every admitted block and return associated per-block output. The
    /// caller still consumes and checks that output before exposing settlement evidence.
    pub(super) fn validate_blocks(
        &self,
        blocks: &mut [AdmittedBlock],
        cancellation: &CancellationToken,
        max_transaction_state_checkpoints: usize,
    ) -> Result<BackendWindowOutput, ValidationError> {
        let validation_started = Instant::now();
        let total_blocks = blocks.len();
        let mut block_outputs = Vec::<BackendBlockOutput>::with_capacity(total_blocks);
        let mut window_pre_state_root = None;

        // Derive and quota-check the final block's checkpoint plan before any
        // witness is consumed or earlier block is replayed.
        check_cancellation(cancellation, 0, total_blocks)?;
        let mut prepared_settling_block = blocks
            .last()
            .map(|admitted| {
                let recovered_block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
                let (checkpoint_plan, system_sender_flags) = CheckpointPlan::from_recovered_block(
                    &recovered_block,
                    max_transaction_state_checkpoints,
                )?;
                Ok(PreparedSettlingBlock {
                    block: recovered_block,
                    checkpoint_plan,
                    system_sender_flags,
                })
            })
            .transpose()?;

        for (index, admitted) in blocks.iter_mut().enumerate() {
            check_cancellation(cancellation, index, total_blocks)?;
            let block_started = Instant::now();
            let block_ordinal = index + 1;
            let (recovered_block, checkpoint_plan, system_sender_flags) = if block_ordinal
                == total_blocks
            {
                let prepared = prepared_settling_block.take().ok_or_else(|| {
                    ValidationError::InternalInvariant(
                        "prepared settling block unexpectedly missing".to_owned(),
                    )
                })?;
                (
                    prepared.block,
                    Some(prepared.checkpoint_plan),
                    prepared.system_sender_flags,
                )
            } else {
                let recovered_block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
                let system_sender_flags = system_sender_flags(&recovered_block);
                (recovered_block, None, system_sender_flags)
            };
            let block_number = recovered_block.header().number;
            let decoded_parent_hash = recovered_block.header().parent_hash;
            let timestamp = recovered_block.header().timestamp;
            let transaction_count = recovered_block.body().transactions.len();
            trace!(
                block_ordinal,
                block_number,
                claimed_block_hash = %admitted.claimed_hash,
                timestamp,
                transactions = transaction_count,
                "validating stateless block",
            );

            // Before this call the decoded block and witness remain untrusted.
            // Successful Stateless output means consensus, witness pre-state,
            // execution receipts, and the recomputed post-state commitment all
            // passed. Only a non-empty final selection uses the checkpoint path.
            let witness = std::mem::take(&mut admitted.witness);
            let (stateless_output, transaction_state_checkpoints) = match checkpoint_plan {
                Some(plan) if !plan.transaction_indices.is_empty() => {
                    let output = stateless_validation_recovered_with_state_checkpoints(
                        recovered_block,
                        witness,
                        Arc::clone(&self.chain_spec),
                        self.evm_config.clone(),
                        &plan.transaction_indices,
                    )
                    .map_err(|error| map_stateless_error(block_number, error))?;
                    let checkpoints = output
                        .checkpoints
                        .transaction_state_checkpoints
                        .into_iter()
                        .map(|checkpoint| TransactionStateCheckpoint {
                            transaction_index: checkpoint.transaction_index,
                            state_root: checkpoint.state_root,
                        })
                        .collect::<Vec<_>>();
                    plan.verify_returned(&checkpoints)?;
                    (output.validation, checkpoints)
                }
                _ => {
                    let output = stateless_validation_recovered(
                        recovered_block,
                        witness,
                        Arc::clone(&self.chain_spec),
                        self.evm_config.clone(),
                    )
                    .map_err(|error| map_stateless_error(block_number, error))?;
                    (output, Vec::new())
                }
            };

            let StatelessValidationOutput {
                block_hash: computed_hash,
                pre_state_root,
                post_state_root,
                execution_output,
                block_access_list: _validated_block_access_list,
            } = stateless_output;
            if let Some(previous_post_state_root) =
                block_outputs.last().map(|output| output.post_state_root)
                && pre_state_root != previous_post_state_root
            {
                return Err(ValidationError::Rejected(format!(
                    "stateless state roots do not telescope at block {block_number}: expected \
                     pre-state root {previous_post_state_root}, got {pre_state_root}",
                )));
            }
            window_pre_state_root.get_or_insert(pre_state_root);

            // These receipts come from successful replay. Observed outbound
            // events are retained here but authorized against batch entries only
            // later by settlement.
            let validated_receipts = execution_output.result.receipts;
            let receipt_successes = validated_receipts
                .iter()
                .map(|receipt| receipt.success)
                .collect::<Vec<_>>();
            let observed_outbound_events = observe_outbound_events(&validated_receipts);

            if tracing::enabled!(tracing::Level::TRACE) {
                let failed_transactions = receipt_successes
                    .iter()
                    .filter(|&&success| !success)
                    .count();
                let malformed_outbound_events = observed_outbound_events
                    .iter()
                    .filter(|observation| observation.decoded_call_hash.is_none())
                    .count();
                trace!(
                    block_ordinal,
                    total_blocks,
                    block_number,
                    computed_block_hash = %computed_hash,
                    validated_pre_state_root = %pre_state_root,
                    validated_post_state_root = %post_state_root,
                    transactions = transaction_count,
                    failed_transactions,
                    outbound_events = observed_outbound_events.len(),
                    malformed_outbound_events,
                    block_elapsed_ms = elapsed_millis(block_started),
                    "stateless block validated",
                );
            }
            block_outputs.push(BackendBlockOutput {
                decoded_number: block_number,
                decoded_parent_hash,
                computed_hash,
                decoded_transaction_count: transaction_count,
                receipt_successes,
                transaction_state_checkpoints,
                post_state_root,
                settlement_evidence: SettlementBlockEvidence {
                    system_sender_flags,
                    observed_outbound_events,
                },
            });

            if block_ordinal.is_multiple_of(DEBUG_PROGRESS_INTERVAL) {
                debug!(
                    validated_blocks = block_ordinal,
                    total_blocks,
                    block_number,
                    validated_post_state_root = %post_state_root,
                    elapsed_ms = elapsed_millis(validation_started),
                    "stateless validation progress",
                );
            }
        }

        let pre_state_root = window_pre_state_root.ok_or_else(|| {
            ValidationError::Rejected("refusing to validate an empty window".to_owned())
        })?;
        Ok(BackendWindowOutput {
            pre_state_root,
            blocks: block_outputs,
        })
    }
}

/// Classify a rejected local checkpoint plan as an internal invariant failure;
/// all other Stateless errors reject the input.
fn map_stateless_error(block_number: u64, error: StatelessValidationError) -> ValidationError {
    match error {
        error @ (StatelessValidationError::UnorderedTransactionCheckpoints { .. }
        | StatelessValidationError::TransactionCheckpointOutOfBounds { .. }) => {
            ValidationError::InternalInvariant(format!(
                "stateless rejected the locally derived checkpoint plan for block {block_number}: \
                 {error}",
            ))
        }
        error => ValidationError::Rejected(format!(
            "stateless validation rejected block {block_number}: {error}",
        )),
    }
}

/// Classify each locally recovered signer for later settlement policy.
fn system_sender_flags(block: &RecoveredBlock<Block>) -> Vec<bool> {
    block
        .transactions_recovered()
        .map(|transaction| transaction.signer() == SYSTEM_ADDRESS)
        .collect()
}

/// Stop between non-interruptible block executions after request cancellation.
fn check_cancellation(
    cancellation: &CancellationToken,
    validated_blocks: usize,
    total_blocks: usize,
) -> Result<(), ValidationError> {
    if cancellation.is_cancelled() {
        debug!(
            validated_blocks,
            total_blocks, "stateless validation stopped after request cancellation",
        );
        Err(ValidationError::Cancelled)
    } else {
        Ok(())
    }
}

fn elapsed_millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Exact-decode Composer-supplied RLP and match its identity to stream metadata.
/// The returned block is still untrusted until Stateless accepts it.
fn decode_and_match_stream_metadata(admitted: &AdmittedBlock) -> Result<Block, ValidationError> {
    let block = alloy_rlp::decode_exact::<Block>(&admitted.rlp).map_err(|error| {
        ValidationError::Rejected(format!(
            "block {} carries invalid consensus RLP: {error}",
            admitted.declared_number,
        ))
    })?;
    if block.header.number != admitted.declared_number {
        return Err(ValidationError::Rejected(format!(
            "decoded block number {} does not match streamed block {}",
            block.header.number, admitted.declared_number,
        )));
    }
    if block.header.parent_hash != admitted.claimed_parent_hash {
        return Err(ValidationError::Rejected(format!(
            "decoded parent hash {} for block {} does not match streamed parent hash {}",
            block.header.parent_hash, admitted.declared_number, admitted.claimed_parent_hash,
        )));
    }
    let computed_hash = block.header.hash_slow();
    if computed_hash != admitted.claimed_hash {
        return Err(ValidationError::Rejected(format!(
            "computed block hash {computed_hash} for block {} does not match streamed block hash {}",
            admitted.declared_number, admitted.claimed_hash,
        )));
    }
    Ok(block)
}

/// Exact-decode, match stream metadata, and recover transaction signers under
/// the configured fork rules. Signer recovery is not execution validation.
fn decode_match_and_recover_signers(
    admitted: &AdmittedBlock,
    chain_spec: &ChainSpec,
) -> Result<RecoveredBlock<Block>, ValidationError> {
    let block = decode_and_match_stream_metadata(admitted)?;
    let number = block.header.number;
    recover_block(block, chain_spec).map_err(|error| {
        ValidationError::Rejected(format!(
            "block {number} transaction recovery failed: {error}",
        ))
    })
}

/// Homestead (EIP-2) forbids high-s signatures, so enforce the low-s rule
/// during recovery only for blocks where that fork is active.
fn recover_block(
    block: Block,
    chain_spec: &ChainSpec,
) -> Result<
    RecoveredBlock<Block>,
    reth_primitives_traits_stateless::transaction::signed::RecoveryError,
> {
    if chain_spec.is_homestead_active_at_block(block.header.number) {
        RecoveredBlock::try_recover(block)
    } else {
        RecoveredBlock::try_recover_unchecked(block)
    }
}

/// Retain every EEZL2 outbound-event candidate with receipt provenance.
///
/// Matching emitter/signature logs remain observations in receipt order.
/// Canonical encodings expose `crossChainCallHash`; malformed or non-canonical
/// encodings remain observations without a hash.
fn observe_outbound_events(
    validated_receipts: &[EthereumReceipt],
) -> Vec<OutboundEventObservation> {
    let mut observations = Vec::new();
    for (transaction_index, receipt) in validated_receipts.iter().enumerate() {
        for (receipt_log_index, log) in receipt.logs.iter().enumerate() {
            if log.address != EEZL2_ADDRESS
                || log.data.topics().first() != Some(&CrossChainCallExecuted::SIGNATURE_HASH)
            {
                continue;
            }
            let decoded_call_hash = CrossChainCallExecuted::decode_log_validate(log)
                .ok()
                .filter(|decoded| &CrossChainCallExecuted::encode_log(decoded) == log)
                .map(|decoded| decoded.data.crossChainCallHash);
            observations.push(OutboundEventObservation {
                transaction_index,
                receipt_log_index,
                decoded_call_hash,
            });
        }
    }
    observations
}

#[cfg(test)]
mod tests;
