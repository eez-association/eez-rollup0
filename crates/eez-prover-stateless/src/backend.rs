//! Adapter for in-process Stateless validation.
//!
//! This module exact-decodes and binds streamed block claims, loads the
//! operator-configured chain trust input, derives checkpoint plans, invokes
//! Stateless/Reth, checks window continuity, and maps validated execution facts
//! into associated per-block output.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use alloy_genesis::ChainConfig;
use alloy_genesis::Genesis;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_evm::EezEvmConfig;
use eez_primitives::Block;
use eez_proof_signer::cancel::CancellationToken;
use eez_proof_signer::validate::support::{
    CheckpointPlan, check_cancellation, decode_match_and_recover_signers, observe_outbound_events,
    system_sender_flags,
};
use eez_proof_signer::validate::{
    BackendBlockOutput, BackendWindowOutput, SettlementBlockEvidence, StateCheckpoint,
    ValidationBackend, ValidationError,
};
use eez_proof_signer::window::AdmittedBlock;
#[cfg(test)]
use eez_proof_signer::window::testing::admitted_block_parts_mut;
use reth_chainspec::ChainSpec;
use reth_primitives_traits::RecoveredBlock;
use stateless_reth::validation::StatelessValidationError;
use stateless_reth::{
    StatelessValidationOutput, stateless_validation_recovered,
    stateless_validation_recovered_with_state_checkpoints,
};
use tracing::{debug, info, trace};

mod chain_config;

use chain_config::{ChainDocumentKind, load_chain_document};

const DEBUG_PROGRESS_INTERVAL: usize = 100;

/// Signer-recovered settling block prepared before replay.
///
/// This block is exact-decoded and its signers are recovered, but it remains
/// untrusted until Stateless accepts the same `RecoveredBlock`. Preparing it
/// early keeps checkpoint selection independent from execution side effects.
struct PreparedSettlingBlock {
    block: RecoveredBlock<Block>,
    checkpoint_plan: CheckpointPlan,
    system_sender_flags: Vec<bool>,
}

/// Each block must execute from its predecessor's post-state.
///
/// This is what proves a window is one contiguous execution rather than a set
/// of independently valid blocks. Free fn so a test can drive it directly —
/// real replay cannot produce a discontinuous window to exercise it with.
///
/// The rejection message keeps the word "telescope": that is the term the
/// signer SPEC and operator runbooks use for this property.
fn executes_from_previous_post_state(
    block_number: u64,
    pre_state_root: B256,
    previous_post_state_root: B256,
) -> Result<(), String> {
    if pre_state_root != previous_post_state_root {
        return Err(format!(
            "stateless state roots do not telescope at block {block_number}: expected \
             pre-state root {previous_post_state_root}, got {pre_state_root}",
        ));
    }
    Ok(())
}

/// In-process Stateless/Reth backend configured from operator-selected rules.
#[derive(Debug)]
pub struct Backend {
    chain_spec: Arc<ChainSpec>,
    evm_config: EezEvmConfig,
    expected_l2_system_address: Address,
}

impl Backend {
    /// EIP-155 identity pinned by the loaded execution chain specification.
    pub fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    /// Load execution rules and bind the deployment's privileged L2 identity.
    pub fn from_chain_document_file(
        path: &Path,
        expected_l2_system_address: Address,
    ) -> eyre::Result<Self> {
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
        Ok(Self::from_genesis(genesis, expected_l2_system_address))
    }

    #[cfg(test)]
    pub(super) fn new(chain_config: ChainConfig, expected_l2_system_address: Address) -> Self {
        Self::from_genesis(
            Genesis {
                config: chain_config,
                ..Default::default()
            },
            expected_l2_system_address,
        )
    }

    fn from_genesis(genesis: Genesis, expected_l2_system_address: Address) -> Self {
        let chain_spec = Arc::new(ChainSpec::from_genesis(genesis));
        let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec));
        Self {
            chain_spec,
            evm_config,
            expected_l2_system_address,
        }
    }

    #[cfg(test)]
    pub(super) fn validate(
        &self,
        mut blocks: Vec<AdmittedBlock>,
    ) -> Result<BackendWindowOutput, ValidationError> {
        self.validate_admitted(&mut blocks, &CancellationToken::default())
    }

    #[cfg(test)]
    fn validate_admitted(
        &self,
        blocks: &mut [AdmittedBlock],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError> {
        let mut witnesses = blocks
            .iter_mut()
            .map(|block| std::mem::take(admitted_block_parts_mut(block).witness))
            .collect::<Vec<_>>();
        let result = self.validate_blocks_inner(blocks, &mut witnesses, cancellation);
        if result.is_err() {
            for (block, witness) in blocks.iter_mut().zip(witnesses) {
                *admitted_block_parts_mut(block).witness = witness;
            }
        }
        result
    }

    /// Replay every admitted block and return associated per-block output. The
    /// caller still consumes and checks that output before exposing settlement evidence.
    fn validate_blocks_inner(
        &self,
        blocks: &[AdmittedBlock],
        witnesses: &mut [ExecutionWitness],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError> {
        let expected_l2_system_address = self.expected_l2_system_address;
        let validation_started = Instant::now();
        let total_blocks = blocks.len();
        if witnesses.len() != total_blocks {
            return Err(ValidationError::InternalInvariant(format!(
                "stateless backend received {} blocks and {} witnesses",
                total_blocks,
                witnesses.len(),
            )));
        }
        let mut block_outputs = Vec::<BackendBlockOutput>::with_capacity(total_blocks);

        // Derive the final block's complete checkpoint plan before any witness
        // is consumed or earlier block is replayed.
        check_cancellation(cancellation, 0, total_blocks)?;
        let mut prepared_settling_block = blocks
            .last()
            .map(|admitted| {
                let recovered_block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
                let (checkpoint_plan, system_sender_flags) = CheckpointPlan::from_recovered_block(
                    &recovered_block,
                    expected_l2_system_address,
                );
                Ok(PreparedSettlingBlock {
                    block: recovered_block,
                    checkpoint_plan,
                    system_sender_flags,
                })
            })
            .transpose()?;

        for (index, (admitted, witness)) in blocks.iter().zip(witnesses).enumerate() {
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
                let system_sender_flags =
                    system_sender_flags(&recovered_block, expected_l2_system_address);
                (recovered_block, None, system_sender_flags)
            };
            let block_number = recovered_block.header().number;
            let decoded_parent_hash = recovered_block.header().parent_hash;
            let timestamp = recovered_block.header().timestamp;
            let transaction_count = recovered_block.body().transactions.len();
            trace!(
                block_ordinal,
                block_number,
                claimed_block_hash = %admitted.claimed_hash(),
                timestamp,
                transactions = transaction_count,
                "validating stateless block",
            );

            // Before this call the decoded block and witness remain untrusted.
            // Successful Stateless output means consensus, witness pre-state,
            // execution receipts, and the recomputed post-state commitment all
            // passed. Only a non-empty final selection uses the checkpoint path.
            let witness = std::mem::take(witness);
            let (stateless_output, transaction_state_checkpoints) = match checkpoint_plan {
                Some(plan) if !plan.positions().is_empty() => {
                    let output = stateless_validation_recovered_with_state_checkpoints(
                        recovered_block,
                        witness,
                        Arc::clone(&self.chain_spec),
                        self.evm_config.clone(),
                        plan.positions(),
                    )
                    .map_err(|error| map_stateless_error(block_number, error))?;
                    let checkpoints = output
                        .checkpoints
                        .checkpoints
                        .into_iter()
                        .map(|checkpoint| StateCheckpoint {
                            at: checkpoint.at,
                            state_root: checkpoint.state_root,
                            block_hash: checkpoint.block_hash,
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
            {
                executes_from_previous_post_state(
                    block_number,
                    pre_state_root,
                    previous_post_state_root,
                )
                .map_err(ValidationError::Rejected)?;
            }

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
                    .filter(|observation| observation.decoded_event.is_none())
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

        if block_outputs.is_empty() {
            return Err(ValidationError::Rejected(
                "refusing to validate an empty window".to_owned(),
            ));
        }
        Ok(BackendWindowOutput {
            blocks: block_outputs,
        })
    }
}

impl ValidationBackend for Backend {
    fn label(&self) -> &'static str {
        "stateless"
    }

    fn chain_id(&self) -> u64 {
        self.chain_id()
    }

    fn expected_l2_system_address(&self) -> Address {
        self.expected_l2_system_address
    }

    fn validate_blocks(
        &self,
        blocks: &[AdmittedBlock],
        witnesses: &mut [ExecutionWitness],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError> {
        self.validate_blocks_inner(blocks, witnesses, cancellation)
    }
}

/// Classify a rejected local checkpoint plan as an internal invariant failure;
/// all other Stateless errors reject the input.
fn map_stateless_error(block_number: u64, error: StatelessValidationError) -> ValidationError {
    match error {
        error @ (StatelessValidationError::UnorderedCheckpoints { .. }
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

fn elapsed_millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
