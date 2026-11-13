//! Replay helpers shared by validation backends.

use std::ops::Deref;
use std::sync::Arc;

use alloy_consensus::proofs::{calculate_receipt_root, calculate_transaction_root};
use alloy_consensus::{BlockHeader as _, Header, TxReceipt};
use alloy_eips::eip7685::EMPTY_REQUESTS_HASH;
use alloy_hardforks::EthereumHardforks;
use alloy_primitives::{Address, B256, logs_bloom};
use alloy_sol_types::SolEvent as _;
use eez_primitives::Block;
use eez_primitives::Receipt as EthereumReceipt;
use eez_protocol::abi::eez_l2_events::CrossChainCallExecuted;
use eez_protocol::settlement::{is_system_tx, pair_end_positions};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock, SealedHeader};
use tracing::debug;

use super::{
    AdmittedBlock, BackendBlockOutput, CheckpointAt, DecodedOutboundEvent,
    OutboundEventObservation, StateCheckpoint, ValidatedBlock, ValidationError,
    check_backend_block_output,
};
use crate::EEZL2_ADDRESS;
use crate::cancel::CancellationToken;

/// Positions at which settlement framing needs replay outputs.
#[derive(Debug, PartialEq, Eq)]
pub struct CheckpointPlan {
    positions: Vec<CheckpointAt>,
}

impl CheckpointPlan {
    /// Construct a plan from locally selected positions.
    #[cfg(feature = "test-utils")]
    pub fn new(positions: Vec<CheckpointAt>) -> Self {
        Self { positions }
    }

    /// Derive Sync-candidate checkpoints and system-sender flags for either RPC version.
    /// A nonempty Sync contains native system transactions; an empty Sync needs
    /// its anchor checkpoint. Ordinary nonempty blocks need no checkpoints.
    pub fn for_block(
        block: &RecoveredBlock<Block>,
        expected_l2_system_address: Address,
    ) -> (Self, Vec<bool>) {
        let transaction_count = block.body().transactions.len();
        let mut system_sender_flags = Vec::with_capacity(transaction_count);
        let mut sync_system_transaction_flags = Vec::with_capacity(transaction_count);
        for transaction in block.transactions_recovered() {
            let is_system_sender = transaction.signer() == expected_l2_system_address;
            system_sender_flags.push(is_system_sender);
            sync_system_transaction_flags.push(is_system_tx(&transaction));
        }
        let mut positions = Vec::new();
        if transaction_count == 0 || sync_system_transaction_flags.contains(&true) {
            // The anchor's candidate leads: the block sealed after the pre-block
            // system calls, holding no transactions. Effect boundaries follow.
            positions.push(CheckpointAt::PreExecution);
            positions.extend(
                pair_end_positions(&sync_system_transaction_flags)
                    .into_iter()
                    .map(CheckpointAt::Transaction),
            );
        }
        (Self { positions }, system_sender_flags)
    }

    /// Positions selected for checkpoint root computation.
    pub fn positions(&self) -> &[CheckpointAt] {
        &self.positions
    }

    /// Check that a checkpoint-capable backend honored the selection exactly.
    pub fn verify_returned(&self, checkpoints: &[StateCheckpoint]) -> Result<(), ValidationError> {
        let selection_matches = checkpoints.len() == self.positions.len()
            && checkpoints
                .iter()
                .zip(&self.positions)
                .all(|(checkpoint, requested)| checkpoint.at == *requested);
        if !selection_matches {
            let returned = checkpoints
                .iter()
                .map(|checkpoint| checkpoint.at)
                .collect::<Vec<_>>();
            return Err(ValidationError::InvalidBackendOutput(format!(
                "checkpoint response targeted positions {returned:?}; requested {:?}",
                self.positions,
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

/// Identity-bound input retained through execution, so finishing validation
/// need not decode the submitted bytes or bind their identity a second time.
#[derive(Debug, Clone)]
pub struct DecodedBlock {
    rlp: Vec<u8>,
    block: RecoveredBlock<Block>,
}

impl Deref for DecodedBlock {
    type Target = RecoveredBlock<Block>;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl DecodedBlock {
    /// An owned execution input for engines that consume their recovered block.
    pub fn execution_input(&self) -> RecoveredBlock<Block> {
        self.block.clone()
    }

    /// Publish checked execution evidence. Backends call this before inserting
    /// a reusable result; callers cannot construct a validated block directly.
    pub fn finish(
        self,
        output: BackendBlockOutput,
        pre_state_root: B256,
        allow_checkpoints: bool,
    ) -> Result<Arc<ValidatedBlock>, ValidationError> {
        let hash = self.block.hash();
        let decoded = self.block.into_block();
        check_backend_block_output(
            decoded.header.number,
            hash,
            &decoded,
            &output,
            allow_checkpoints,
        )
        .map_err(|error| ValidationError::InvalidBackendOutput(error.to_string()))?;
        Ok(Arc::new(ValidatedBlock {
            number: decoded.header.number,
            hash: output.computed_hash,
            parent_hash: decoded.header.parent_hash,
            pre_state_root,
            post_state_root: output.post_state_root,
            rlp: self.rlp,
            decoded,
            receipt_successes: output.receipt_successes,
            transaction_state_checkpoints: output.transaction_state_checkpoints,
            settlement_evidence: output.settlement_evidence,
        }))
    }
}

/// Exact-decode, match stream metadata, and recover transaction signers.
pub fn decode_match_and_recover_signers(
    admitted: &AdmittedBlock,
    chain_spec: &impl EthereumHardforks,
) -> Result<DecodedBlock, ValidationError> {
    let block = decode_and_match_stream_metadata(admitted)?;
    let number = block.header.number;
    let block = recover_block(block, chain_spec).map_err(|error| {
        ValidationError::Rejected(format!(
            "block {number} transaction recovery failed: {error}",
        ))
    })?;
    Ok(DecodedBlock {
        rlp: admitted.rlp().to_vec(),
        block,
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
    chain_spec: &impl EthereumHardforks,
) -> Result<RecoveredBlock<Block>, reth_primitives_traits::transaction::signed::RecoveryError> {
    if chain_spec.is_homestead_active_at_block(block.header.number) {
        RecoveredBlock::try_recover(block)
    } else {
        RecoveredBlock::try_recover_unchecked(block)
    }
}

/// A settling block carries a header field a candidate cannot reproduce from a
/// transaction prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("candidate block cannot reproduce {field} from a transaction prefix")]
pub struct CandidateBlockError {
    /// The unreproducible header field.
    pub field: &'static str,
}

/// Seal the candidate block for this prefix, field by field — never by updating
/// a clone, so a new `Header` field fails the build instead of being inherited.
///
/// Shared so every backend that replays a window seals candidates identically.
///
/// # Errors
///
/// Returns [`CandidateBlockError`] when the settling block carries a block access
/// list, non-empty execution requests or blob gas.
pub fn candidate_block_hash<N>(
    settling: &SealedHeader,
    transactions: &[N::SignedTx],
    receipts: &[N::Receipt],
    state_root: B256,
) -> Result<B256, CandidateBlockError>
where
    N: NodePrimitives,
{
    // Neither is reproducible from a prefix, so refuse rather than inherit.
    if settling.block_access_list_hash().is_some() {
        return Err(CandidateBlockError {
            field: "block access list",
        });
    }
    if settling
        .requests_hash()
        .is_some_and(|hash| hash != EMPTY_REQUESTS_HASH)
    {
        return Err(CandidateBlockError {
            field: "execution requests",
        });
    }
    // No blob transactions on an EEZ L2, so no prefix apportioning to get right.
    if settling.blob_gas_used().is_some_and(|used| used != 0) {
        return Err(CandidateBlockError {
            field: "blob transactions",
        });
    }

    let header = Header {
        // Prefix-varying.
        state_root,
        transactions_root: calculate_transaction_root(transactions),
        receipts_root: calculate_receipt_root(
            &receipts
                .iter()
                .map(TxReceipt::with_bloom_ref)
                .collect::<Vec<_>>(),
        ),
        logs_bloom: logs_bloom(receipts.iter().flat_map(TxReceipt::logs)),
        gas_used: receipts
            .last()
            .map(TxReceipt::cumulative_gas_used)
            .unwrap_or_default(),
        // Zero for every prefix: a non-zero value was refused above.
        blob_gas_used: settling.blob_gas_used(),
        // Fixed by parent + payload attributes, so shared with the settling block.
        parent_hash: settling.parent_hash(),
        ommers_hash: settling.ommers_hash(),
        beneficiary: settling.beneficiary(),
        withdrawals_root: settling.withdrawals_root(),
        difficulty: settling.difficulty(),
        number: settling.number(),
        gas_limit: settling.gas_limit(),
        timestamp: settling.timestamp(),
        extra_data: settling.extra_data().clone(),
        mix_hash: settling.mix_hash().unwrap_or_default(),
        nonce: settling.nonce().unwrap_or_default(),
        base_fee_per_gas: settling.base_fee_per_gas(),
        excess_blob_gas: settling.excess_blob_gas(),
        parent_beacon_block_root: settling.parent_beacon_block_root(),
        requests_hash: settling.requests_hash(),
        block_access_list_hash: None,
        slot_number: settling.slot_number(),
    };
    Ok(header.hash_slow())
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
