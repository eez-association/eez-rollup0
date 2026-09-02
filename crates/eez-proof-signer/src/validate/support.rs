//! Replay helpers shared by validation backends.

use alloy_consensus::{EthereumReceipt, Transaction as _};
use alloy_primitives::Address;
use alloy_sol_types::SolEvent as _;
use eez_protocol::abi::eez_l2_events::CrossChainCallExecuted;
use eez_protocol::settlement::{is_system_tx, pair_end_positions};
use reth_chainspec::{ChainSpec, EthereumHardforks as _};
use reth_ethereum_primitives::Block;
use reth_primitives_traits::RecoveredBlock;
use tracing::debug;

use super::{
    DecodedOutboundEvent, OutboundEventObservation, TransactionStateCheckpoint, ValidationError,
};
use crate::EEZL2_ADDRESS;
use crate::cancel::CancellationToken;
use crate::window::AdmittedBlock;

/// Transaction boundaries at which settlement framing needs state roots.
#[derive(Debug, PartialEq, Eq)]
pub struct CheckpointPlan {
    transaction_indices: Vec<usize>,
}

impl CheckpointPlan {
    /// Construct a plan from locally selected transaction boundaries.
    pub fn new(transaction_indices: Vec<usize>) -> Self {
        Self {
            transaction_indices,
        }
    }

    /// Derive effect-candidate boundaries and system-sender flags.
    pub fn from_recovered_block(
        block: &RecoveredBlock<Block>,
        expected_l2_system_address: Address,
    ) -> (Self, Vec<bool>) {
        let transaction_count = block.body().transactions.len();
        let mut system_sender_flags = Vec::with_capacity(transaction_count);
        let mut sync_system_transaction_flags = Vec::with_capacity(transaction_count);
        for transaction in block.transactions_recovered() {
            let is_system_sender = transaction.signer() == expected_l2_system_address;
            system_sender_flags.push(is_system_sender);
            sync_system_transaction_flags.push(is_system_tx(
                transaction.signer(),
                transaction.to(),
                expected_l2_system_address,
                EEZL2_ADDRESS,
            ));
        }
        let plan = Self::new(pair_end_positions(&sync_system_transaction_flags));
        (plan, system_sender_flags)
    }

    /// Transaction indices selected for checkpoint root computation.
    pub fn transaction_indices(&self) -> &[usize] {
        &self.transaction_indices
    }

    /// Check that a checkpoint-capable backend honored the selection exactly.
    pub fn verify_returned(
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
                "checkpoint response targeted transaction indices {returned:?}; requested {:?}",
                self.transaction_indices,
            )));
        }
        Ok(())
    }
}

/// Classify each locally recovered signer for later settlement policy.
pub fn system_sender_flags(
    block: &RecoveredBlock<Block>,
    expected_l2_system_address: Address,
) -> Vec<bool> {
    block
        .transactions_recovered()
        .map(|transaction| transaction.signer() == expected_l2_system_address)
        .collect()
}

/// Stop between non-interruptible block executions after request cancellation.
pub fn check_cancellation(
    cancellation: &CancellationToken,
    validated_blocks: usize,
    total_blocks: usize,
) -> Result<(), ValidationError> {
    if cancellation.is_cancelled() {
        debug!(
            validated_blocks,
            total_blocks, "proof validation stopped after request cancellation",
        );
        Err(ValidationError::Cancelled)
    } else {
        Ok(())
    }
}

/// Exact-decode, match stream metadata, and recover transaction signers.
pub fn decode_match_and_recover_signers(
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

fn decode_and_match_stream_metadata(admitted: &AdmittedBlock) -> Result<Block, ValidationError> {
    let block = alloy_rlp::decode_exact::<Block>(admitted.rlp()).map_err(|error| {
        ValidationError::Rejected(format!(
            "block {} carries invalid consensus RLP: {error}",
            admitted.declared_number(),
        ))
    })?;
    if block.header.number != admitted.declared_number() {
        return Err(ValidationError::Rejected(format!(
            "decoded block number {} does not match streamed block {}",
            block.header.number,
            admitted.declared_number(),
        )));
    }
    if block.header.parent_hash != admitted.claimed_parent_hash() {
        return Err(ValidationError::Rejected(format!(
            "decoded parent hash {} for block {} does not match streamed parent hash {}",
            block.header.parent_hash,
            admitted.declared_number(),
            admitted.claimed_parent_hash(),
        )));
    }
    let computed_hash = block.header.hash_slow();
    if computed_hash != admitted.claimed_hash() {
        return Err(ValidationError::Rejected(format!(
            "computed block hash {computed_hash} for block {} does not match streamed block hash {}",
            admitted.declared_number(),
            admitted.claimed_hash(),
        )));
    }
    Ok(block)
}

fn recover_block(
    block: Block,
    chain_spec: &ChainSpec,
) -> Result<RecoveredBlock<Block>, reth_primitives_traits::transaction::signed::RecoveryError> {
    if chain_spec.is_homestead_active_at_block(block.header.number) {
        RecoveredBlock::try_recover(block)
    } else {
        RecoveredBlock::try_recover_unchecked(block)
    }
}

/// Retain every EEZL2 outbound-event candidate with receipt provenance.
pub fn observe_outbound_events(
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
            let decoded_event = CrossChainCallExecuted::decode_log_validate(log)
                .ok()
                .filter(|decoded| &CrossChainCallExecuted::encode_log(decoded) == log)
                .map(|decoded| {
                    DecodedOutboundEvent::new(decoded.data.crossChainCallHash, decoded.data.callGas)
                });
            observations.push(OutboundEventObservation {
                transaction_index,
                receipt_log_index,
                decoded_event,
            });
        }
    }
    observations
}
