//! Transaction classification and settling/intermediate block inspection.

use std::num::NonZeroU64;

use alloy_consensus::{Transaction as _, Typed2718 as _};
use eez_protocol::entries::EXECUTE_INCOMING_SELECTOR;
use eez_protocol::settlement::pair_end_positions as framed_effect_candidate_positions;
use thiserror::Error;

use super::inbound::{InboundCandidate, inspect_inbound_candidate};
use crate::EEZL2_ADDRESS;
use crate::validate::{OutboundEventObservation, SettlementBlockEvidence, ValidatedBlock};

pub(super) type EthereumBlock = reth_ethereum_primitives::Block;
pub(super) const RESERVED_SYSTEM_TRANSACTION_TYPE: u8 = 0x7e;

/// Settlement observations derived by combining backend-recovered evidence
/// with the exact settling-block transaction order.
///
/// Candidates are deliberately retained even when malformed so later binding
/// fails closed at their original positions.
/// Construction stays in this module after the block/evidence consistency
/// checks; downstream effect gates receive only immutable views.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SettlingBlockObservations {
    system_sender_flags: Vec<bool>,
    /// Every system-to-EEZL2 transaction carrying the inbound selector, in
    /// block order. Invalid observations remain candidates through positional
    /// binding and fail during inbound authorization rather than disappearing.
    inbound_candidates: Vec<InboundCandidate>,
    /// EEZL2-emitter/signature-matched outbound-event candidates from validated
    /// receipts, in transaction and receipt-log order. Malformed candidates
    /// retain their position with an absent hash.
    outbound_event_candidates: Vec<OutboundEventObservation>,
}

impl SettlingBlockObservations {
    /// Backend-recovered system-sender classification in transaction order.
    pub(crate) fn system_sender_flags(&self) -> &[bool] {
        &self.system_sender_flags
    }

    /// Selector-matched inbound candidates retained in transaction order.
    pub(crate) fn inbound_candidates(&self) -> &[InboundCandidate] {
        &self.inbound_candidates
    }

    /// Outbound event candidates retained in transaction and log order.
    pub(crate) fn outbound_event_candidates(&self) -> &[OutboundEventObservation] {
        &self.outbound_event_candidates
    }

    /// Derive candidate effect boundaries from the ordered system-sender flags.
    /// Semantic gates validate the corresponding inbound or outbound effects.
    pub(super) fn effect_candidate_positions(&self) -> Vec<usize> {
        framed_effect_candidate_positions(&self.system_sender_flags)
    }

    /// Construct explicit observations for focused settlement tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        system_sender_flags: Vec<bool>,
        inbound_candidates: Vec<InboundCandidate>,
        outbound_event_candidates: Vec<OutboundEventObservation>,
    ) -> Self {
        Self {
            system_sender_flags,
            inbound_candidates,
            outbound_event_candidates,
        }
    }

    /// Mutate inbound candidates only in focused rejection-path tests.
    #[cfg(test)]
    pub(crate) fn inbound_candidates_mut_for_test(&mut self) -> &mut Vec<InboundCandidate> {
        &mut self.inbound_candidates
    }

    /// Mutate outbound candidates only in focused rejection-path tests.
    #[cfg(test)]
    pub(crate) fn outbound_event_candidates_mut_for_test(
        &mut self,
    ) -> &mut Vec<OutboundEventObservation> {
        &mut self.outbound_event_candidates
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum BlockInspectionError {
    #[error("block {block_number} RLP does not decode exactly: {reason}")]
    InvalidRlp { block_number: u64, reason: String },
    #[error(
        "recovered system-sender facts cover {actual} transactions, but block {block_number} has {required}"
    )]
    SystemSenderCount {
        block_number: u64,
        required: usize,
        actual: usize,
    },
    #[error(
        "intermediate block {block_number} transaction {transaction_index} is a system transaction"
    )]
    IntermediateSystemTransaction {
        block_number: u64,
        transaction_index: usize,
    },
    #[error(
        "intermediate block {block_number} transaction {transaction_index} emitted an outbound event"
    )]
    IntermediateOutboundEvent {
        block_number: u64,
        transaction_index: usize,
    },
    #[error(
        "transaction statuses cover {actual} transactions, but the settling block has {required}"
    )]
    StatusCount { required: usize, actual: usize },
    #[error("settling system transaction {index} reverted")]
    RevertedSystemTransaction { index: usize },
    #[error(
        "settling block transaction {index} is signed by the reserved system account but does not target EEZL2"
    )]
    ReservedSystemSender { index: usize },
}

/// Bind fork-aware execution facts to the exact settling-block transaction
/// order, then require a successful receipt for every system transaction.
pub(crate) fn inspect_validated_settling_block(
    block: &ValidatedBlock,
    receipt_successes: &[bool],
    expected_rollup_id: NonZeroU64,
) -> Result<SettlingBlockObservations, BlockInspectionError> {
    let decoded = decode_block(block.number(), block.rlp())?;
    inspect_decoded_settling_block(
        block.number(),
        &decoded,
        block.settlement_evidence(),
        receipt_successes,
        expected_rollup_id,
    )
}

fn decode_block(
    block_number: u64,
    block_rlp: &[u8],
) -> Result<EthereumBlock, BlockInspectionError> {
    alloy_rlp::decode_exact::<EthereumBlock>(block_rlp).map_err(|error| {
        BlockInspectionError::InvalidRlp {
            block_number,
            reason: error.to_string(),
        }
    })
}

fn inspect_decoded_settling_block(
    block_number: u64,
    block: &EthereumBlock,
    settlement_evidence: &SettlementBlockEvidence,
    receipt_successes: &[bool],
    expected_rollup_id: NonZeroU64,
) -> Result<SettlingBlockObservations, BlockInspectionError> {
    let transactions = &block.body.transactions;
    if settlement_evidence.system_sender_flags().len() != transactions.len() {
        return Err(BlockInspectionError::SystemSenderCount {
            block_number,
            required: transactions.len(),
            actual: settlement_evidence.system_sender_flags().len(),
        });
    }
    if receipt_successes.len() != transactions.len() {
        return Err(BlockInspectionError::StatusCount {
            required: transactions.len(),
            actual: receipt_successes.len(),
        });
    }

    let mut inbound_candidates = Vec::new();
    for (transaction_index, ((transaction, &is_system_sender), &succeeded)) in transactions
        .iter()
        .zip(settlement_evidence.system_sender_flags())
        .zip(receipt_successes)
        .enumerate()
    {
        // The reserved system signer may authorize only top-level calls to EEZL2.
        if is_system_sender && transaction.to() != Some(EEZL2_ADDRESS) {
            return Err(BlockInspectionError::ReservedSystemSender {
                index: transaction_index,
            });
        }
        // The target is already pinned, so reserved signer plus the inbound
        // selector fully identifies an inbound candidate.
        if is_system_sender
            && transaction
                .input()
                .starts_with(EXECUTE_INCOMING_SELECTOR.as_slice())
        {
            inbound_candidates.push(InboundCandidate {
                transaction_index,
                inspection: inspect_inbound_candidate(
                    transaction.value(),
                    transaction.input(),
                    succeeded,
                    expected_rollup_id,
                ),
            });
        }
        if is_system_sender && !succeeded {
            return Err(BlockInspectionError::RevertedSystemTransaction {
                index: transaction_index,
            });
        }
    }

    // The length checks above guarantee the retained flags cover every
    // settling transaction just inspected.
    Ok(SettlingBlockObservations {
        system_sender_flags: settlement_evidence.system_sender_flags().to_vec(),
        inbound_candidates,
        outbound_event_candidates: settlement_evidence.observed_outbound_events().to_vec(),
    })
}

/// Test-only fallback for canned validators that do not recover transactions.
#[cfg(test)]
pub(crate) fn inspect_settling_block(
    block_rlp: &[u8],
    receipt_successes: &[bool],
    expected_rollup_id: NonZeroU64,
) -> Result<SettlingBlockObservations, BlockInspectionError> {
    let block = decode_block(0, block_rlp)?;
    inspect_decoded_settling_block(
        0,
        &block,
        &SettlementBlockEvidence::from_rlp_for_test(block_rlp),
        receipt_successes,
        expected_rollup_id,
    )
}

/// Reject privileged transactions and outbound-effect observations in every
/// intermediate block.
///
/// Each block is consensus-RLP decoded exactly, and recovered signer flags
/// must cover every transaction. A transaction is privileged if it uses the
/// reserved system type or is signed by [`eez_protocol::SYSTEM_ADDRESS`], regardless of its
/// recipient. Outbound observations are rejected because effects are bound
/// only in the settling block.
pub(crate) fn verify_validated_intermediate_blocks(
    blocks: &[ValidatedBlock],
) -> Result<(), BlockInspectionError> {
    for block in blocks {
        let decoded = decode_block(block.number(), block.rlp())?;
        verify_decoded_intermediate_block(
            block.number(),
            &decoded,
            block.settlement_evidence().system_sender_flags(),
        )?;
        if let Some(observation) = block
            .settlement_evidence()
            .observed_outbound_events()
            .first()
        {
            return Err(BlockInspectionError::IntermediateOutboundEvent {
                block_number: block.number(),
                transaction_index: observation.transaction_index(),
            });
        }
    }
    Ok(())
}

fn verify_decoded_intermediate_block(
    block_number: u64,
    block: &EthereumBlock,
    system_sender_flags: &[bool],
) -> Result<(), BlockInspectionError> {
    let transactions = &block.body.transactions;
    if system_sender_flags.len() != transactions.len() {
        return Err(BlockInspectionError::SystemSenderCount {
            block_number,
            required: transactions.len(),
            actual: system_sender_flags.len(),
        });
    }
    if let Some(transaction_index) = transactions
        .iter()
        .zip(system_sender_flags)
        .enumerate()
        .find_map(|(transaction_index, (transaction, &is_system_sender))| {
            (transaction.ty() == RESERVED_SYSTEM_TRANSACTION_TYPE || is_system_sender)
                .then_some(transaction_index)
        })
    {
        return Err(BlockInspectionError::IntermediateSystemTransaction {
            block_number,
            transaction_index,
        });
    }
    Ok(())
}

/// Test-only RLP entry point for the focused settlement unit tests.
#[cfg(test)]
pub(crate) fn verify_no_intermediate_system_transactions<'a>(
    blocks: impl IntoIterator<Item = (u64, &'a [u8])>,
) -> Result<(), BlockInspectionError> {
    for (block_number, block_rlp) in blocks {
        let block = decode_block(block_number, block_rlp)?;
        verify_decoded_intermediate_block(
            block_number,
            &block,
            SettlementBlockEvidence::from_rlp_for_test(block_rlp).system_sender_flags(),
        )?;
    }
    Ok(())
}
