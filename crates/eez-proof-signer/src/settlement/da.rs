//! Decoding and exact verification of the batch data-availability payload.

use alloy_consensus::Transaction as _;
use alloy_primitives::Bytes;
use alloy_sol_types::SolValue as _;
use eez_payload_codec::{Action, DecodedSpan};
use eez_protocol::RollupId;
use eez_protocol::entries::manifest::entry_from_action;
use reth_primitives_traits::BlockBody as _;
use thiserror::Error;

use super::{
    AuthorizedInboundEffects, AuthorizedOutboundEffects, CanonicalPostBatch, EthereumBlock,
    SystemTransactionReconstructor,
};
use super::{inbound::AuthorizedInboundEffect, outbound::AuthorizedOutboundEffect};
use crate::validate::ValidatedWindow;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum DaPayloadError {
    #[error("batch callData does not decode: {reason}")]
    Decode { reason: String },
    #[error("batch callData has {trailing} trailing bytes")]
    TrailingBytes { trailing: usize },
    #[error("batch callData covers {actual} blocks; validated window has {expected}")]
    BlockCount { expected: usize, actual: usize },
    #[error("batch callData contains unexpected {field} items after the expected {expected}")]
    UnexpectedItems {
        field: &'static str,
        expected: usize,
    },
    #[error("validated block {block_number} RLP does not decode exactly: {reason}")]
    InvalidBlockRlp { block_number: u64, reason: String },
    #[error(
        "batch callData claims {submitted} retained transactions for block {block_number}; expected projection has {expected}"
    )]
    ProjectedTransactionCount {
        block_number: u64,
        submitted: usize,
        expected: usize,
    },
    #[error(
        "batch callData transaction {transaction_index} for block {block_number} does not match the validated transaction bytes"
    )]
    TransactionMismatch {
        block_number: u64,
        transaction_index: usize,
    },
    #[error(
        "batch callData is missing action {entry_index} for effect transaction {transaction_index}"
    )]
    MissingAction {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error(
        "batch callData action {entry_index} does not rebuild the entry for effect transaction {transaction_index}"
    )]
    ActionMismatch {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error("batch callData action {entry_index} does not describe rollup {rollup_id}: {reason}")]
    UnrebuildableAction {
        entry_index: usize,
        rollup_id: u64,
        reason: String,
    },
    #[error(
        "batch callData claims {field} for block {block_number} that the validated header does not carry"
    )]
    HeaderInputMismatch {
        field: &'static str,
        block_number: u64,
    },
    #[error("canonical Sync-block reconstruction failed: {reason}")]
    SystemTransactionReconstruction { reason: String },
    #[error(
        "canonical Sync-block transaction {transaction_index} does not match the validated transaction bytes"
    )]
    SyncBlockTransactionMismatch { transaction_index: usize },
    #[error(
        "canonical Sync block has {actual} transactions; reconstructed effect sequence has {expected}"
    )]
    SyncBlockTransactionCount { expected: usize, actual: usize },
    #[error("validated effect positions are inconsistent with the settling block transactions")]
    InvalidEffectTransactionLayout,
}

/// The decoded payload plus a cursor into its flat transaction column.
///
/// Parsing is `eez-payload-codec`, the same decoder the composer encodes with
/// and the deriver reads with. A second implementation here is what let the
/// wire format drift from the gate that is supposed to police it; the
/// independence that matters is the VERIFICATION below, which binds the
/// payload to the validated blocks and never trusts what it decoded.
struct DaPayload {
    span: DecodedSpan,
    actions: Vec<Action>,
    next_transaction: usize,
}

impl DaPayload {
    fn decode(encoded_payload: &[u8]) -> Result<Self, DaPayloadError> {
        let container =
            eez_payload_codec::decode_container(encoded_payload).map_err(|error| match error {
                eez_payload_codec::CodecError::TrailingBytes(trailing) => {
                    DaPayloadError::TrailingBytes { trailing }
                }
                other => invalid_da_payload(other.to_string()),
            })?;
        Ok(Self {
            span: container.span,
            actions: container.actions,
            next_transaction: 0,
        })
    }

    /// The submitted transaction count for the `index`-th block of the span.
    fn block_count(&self, expected_blocks: usize, index: usize) -> Result<usize, DaPayloadError> {
        let actual = self.span.block_count();
        if actual != expected_blocks {
            return Err(DaPayloadError::BlockCount {
                expected: expected_blocks,
                actual,
            });
        }
        // Bounds-checked rather than indexed: the count equality above already
        // implies `index` is in range, but this gate runs on composer-supplied
        // input and must fail closed rather than panic if that ever changes.
        self.span
            .block_tx_counts
            .get(index)
            .map(|count| *count as usize)
            .ok_or(DaPayloadError::BlockCount {
                expected: expected_blocks,
                actual,
            })
    }

    /// The next transaction in block-major order, or a mismatch if exhausted.
    fn expect_transaction(
        &mut self,
        block_number: u64,
        transaction_index: usize,
        expected: &[u8],
    ) -> Result<(), DaPayloadError> {
        let submitted = self.span.transactions.get(self.next_transaction);
        self.next_transaction += 1;
        if submitted.is_none_or(|tx| tx.as_slice() != expected) {
            return Err(DaPayloadError::TransactionMismatch {
                block_number,
                transaction_index,
            });
        }
        Ok(())
    }

    /// Every transaction the span carries must have been claimed by a block.
    fn transactions_exhausted(&self, retained: usize) -> Result<(), DaPayloadError> {
        if self.next_transaction == self.span.transactions.len() {
            return Ok(());
        }
        Err(DaPayloadError::UnexpectedItems {
            field: "transactions",
            expected: retained,
        })
    }
}

fn invalid_da_payload(reason: impl Into<String>) -> DaPayloadError {
    DaPayloadError::Decode {
        reason: reason.into(),
    }
}

/// Bind the decoded batch's submitted DA payload to the validated window and
/// authorized effects.
///
/// The preceding blocks carry every transaction verbatim. In the terminal
/// settling block, system loads and inbound deliveries are omitted while
/// outbound user transactions remain. Each effect has one exact, ordered
/// derivation sidecar, and the resulting mixed Sync sequence is reconstructed
/// byte for byte.
pub(crate) fn verify_da_payload(
    batch: &CanonicalPostBatch,
    validated_window: &ValidatedWindow,
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    system_transaction_reconstructor: &SystemTransactionReconstructor,
    expected_rollup_id: u64,
) -> Result<(), DaPayloadError> {
    let settling = validated_window.settling_block().block();
    verify_encoded_da_payload(
        batch.as_batch().callData.as_ref(),
        validated_window
            .preceding_blocks()
            .iter()
            .map(|block| (block.number(), block.rlp())),
        (settling.number(), settling.rlp()),
        outbound_effects,
        inbound_effects,
        system_transaction_reconstructor,
        expected_rollup_id,
    )
}

/// Raw-input adapter for focused DA unit tests.
#[cfg(test)]
pub(crate) fn verify_da_payload_for_test<'a, I>(
    encoded_payload: &[u8],
    intermediate_blocks: I,
    settling_block: (u64, &'a [u8]),
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    system_transaction_reconstructor: &SystemTransactionReconstructor,
    expected_rollup_id: u64,
) -> Result<(), DaPayloadError>
where
    I: ExactSizeIterator<Item = (u64, &'a [u8])>,
{
    verify_encoded_da_payload(
        encoded_payload,
        intermediate_blocks,
        settling_block,
        outbound_effects,
        inbound_effects,
        system_transaction_reconstructor,
        expected_rollup_id,
    )
}

fn verify_encoded_da_payload<'a, I>(
    encoded_payload: &[u8],
    intermediate_blocks: I,
    settling_block: (u64, &'a [u8]),
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    system_transaction_reconstructor: &SystemTransactionReconstructor,
    expected_rollup_id: u64,
) -> Result<(), DaPayloadError>
where
    I: ExactSizeIterator<Item = (u64, &'a [u8])>,
{
    let mut payload_cursor = DaPayload::decode(encoded_payload)?;
    let expected_blocks = intermediate_blocks.len() + 1;
    let omitted_terminal_count = outbound_effects
        .len()
        .checked_add(inbound_effects.len())
        .ok_or(DaPayloadError::InvalidEffectTransactionLayout)?;
    let preceding_transaction_count = verify_preceding_block_transactions(
        &mut payload_cursor,
        intermediate_blocks,
        expected_blocks,
    )?;
    let settling = verify_settling_block_transactions(
        &mut payload_cursor,
        settling_block,
        expected_blocks,
        outbound_effects,
        inbound_effects,
        omitted_terminal_count,
    )?;
    // Cannot overflow: every counted transaction occupies at least one byte of
    // memory-resident block RLP; the sum only feeds an error diagnostic.
    let retained_transactions =
        preceding_transaction_count.saturating_add(settling.retained_transaction_count);

    payload_cursor.transactions_exhausted(retained_transactions)?;

    verify_effect_sidecars(
        &payload_cursor,
        outbound_effects,
        inbound_effects,
        omitted_terminal_count,
        expected_rollup_id,
    )?;
    if settling.requires_sync_reconstruction {
        verify_reconstructed_sync_block(
            &settling.block,
            &settling.encoded_transactions,
            outbound_effects,
            inbound_effects,
            system_transaction_reconstructor,
        )?;
    }
    Ok(())
}

/// Verify that every preceding block is reproduced transaction-for-transaction.
fn verify_preceding_block_transactions<'a, I>(
    payload_cursor: &mut DaPayload,
    preceding_blocks: I,
    expected_blocks: usize,
) -> Result<usize, DaPayloadError>
where
    I: Iterator<Item = (u64, &'a [u8])>,
{
    let mut retained_transaction_count = 0usize;
    for (block_index, (block_number, block_rlp)) in preceding_blocks.enumerate() {
        let block = decode_validated_block(block_number, block_rlp)?;
        let submitted_count = payload_cursor.block_count(expected_blocks, block_index)?;
        verify_header_inputs(payload_cursor, block_index, block_number, &block)?;
        let validated_count = block.body.transactions.len();
        if submitted_count != validated_count {
            return Err(DaPayloadError::ProjectedTransactionCount {
                block_number,
                submitted: submitted_count,
                expected: validated_count,
            });
        }
        retained_transaction_count = retained_transaction_count.saturating_add(validated_count);
        for (transaction_index, transaction) in
            block.body.encoded_2718_transactions_iter().enumerate()
        {
            payload_cursor.expect_transaction(block_number, transaction_index, &transaction)?;
        }
    }
    Ok(retained_transaction_count)
}

struct SettlingBlockDaVerification {
    block: EthereumBlock,
    encoded_transactions: Vec<Bytes>,
    retained_transaction_count: usize,
    requires_sync_reconstruction: bool,
}

/// Verify the settling-block transaction projection and retain exact bytes only
/// when the canonical Sync sequence must be reconstructed.
fn verify_settling_block_transactions(
    payload_cursor: &mut DaPayload,
    (block_number, block_rlp): (u64, &[u8]),
    expected_blocks: usize,
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    omitted_transaction_count: usize,
) -> Result<SettlingBlockDaVerification, DaPayloadError> {
    let block = decode_validated_block(block_number, block_rlp)?;
    let submitted_count = payload_cursor.block_count(expected_blocks, expected_blocks - 1)?;
    verify_header_inputs(payload_cursor, expected_blocks - 1, block_number, &block)?;
    let validated_count = block.body.transactions.len();
    let retained_transaction_count = validated_count
        .checked_sub(omitted_transaction_count)
        .ok_or(DaPayloadError::InvalidEffectTransactionLayout)?;
    if submitted_count != retained_transaction_count {
        return Err(DaPayloadError::ProjectedTransactionCount {
            block_number,
            submitted: submitted_count,
            expected: retained_transaction_count,
        });
    }

    // With no authorized effects the settling block is ordinary: nothing was
    // omitted and there is no canonical Sync layout to reconstruct.
    let requires_sync_reconstruction = omitted_transaction_count != 0;
    let mut encoded_transactions = if requires_sync_reconstruction {
        Vec::with_capacity(validated_count)
    } else {
        Vec::new()
    };
    // Outbound-before-inbound ordering was established by authorization, so
    // chaining the two position lists preserves block order.
    let mut omitted_positions = outbound_effects
        .iter()
        .map(AuthorizedOutboundEffect::load_transaction_index)
        .chain(
            inbound_effects
                .iter()
                .map(AuthorizedInboundEffect::transaction_index),
        )
        .peekable();
    for (transaction_index, transaction) in block
        .body
        .encoded_2718_transactions_iter()
        .map(Bytes::from)
        .enumerate()
    {
        if requires_sync_reconstruction {
            encoded_transactions.push(transaction.clone());
        }
        if omitted_positions
            .peek()
            .is_some_and(|&position| position == transaction_index)
        {
            omitted_positions.next();
            continue;
        }
        payload_cursor.expect_transaction(block_number, transaction_index, transaction.as_ref())?;
    }
    if omitted_positions.next().is_some() {
        return Err(DaPayloadError::InvalidEffectTransactionLayout);
    }

    Ok(SettlingBlockDaVerification {
        block,
        encoded_transactions,
        retained_transaction_count,
        requires_sync_reconstruction,
    })
}

/// Verify one ordered canonical sidecar for every authorized effect.
///
/// The DA publishes ACTIONS, not entries, so each one is rebuilt through the
/// same projection derivation uses and the result must equal the entry the
/// authorized effect derives. That binds the action's own fields — target,
/// value, calldata, outcome — because the rebuilt entry's `proxyEntryHash` and
/// `rollingHash` are computed from them, not carried.
fn verify_effect_sidecars(
    payload: &DaPayload,
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    expected_sidecar_count: usize,
    expected_rollup_id: u64,
) -> Result<(), DaPayloadError> {
    let expected_sidecars = outbound_effects
        .iter()
        .map(|outbound| {
            (
                outbound.transaction_index(),
                outbound.derived_da_entry().abi_encode(),
            )
        })
        .chain(inbound_effects.iter().map(|inbound| {
            (
                inbound.transaction_index(),
                inbound.observation().derived_da_entry.encoded(),
            )
        }));
    for (entry_index, (transaction_index, expected_encoding)) in expected_sidecars.enumerate() {
        let action = payload
            .actions
            .get(entry_index)
            .ok_or(DaPayloadError::MissingAction {
                entry_index,
                transaction_index,
            })?;
        let rebuilt = entry_from_action(action, RollupId(expected_rollup_id)).map_err(|error| {
            DaPayloadError::UnrebuildableAction {
                entry_index,
                rollup_id: expected_rollup_id,
                reason: error.to_string(),
            }
        })?;
        if rebuilt.abi_encode() != expected_encoding {
            return Err(DaPayloadError::ActionMismatch {
                entry_index,
                transaction_index,
            });
        }
    }
    if payload.actions.len() != expected_sidecar_count {
        return Err(DaPayloadError::UnexpectedItems {
            field: "actions",
            expected: expected_sidecar_count,
        });
    }
    Ok(())
}

/// Bind the span's claimed per-block header inputs to the validated header.
///
/// The composer selects beneficiary and extraData per block and derivation
/// rebuilds headers from these values, so an unchecked claim here would let a
/// composer publish inputs that disagree with the blocks it actually built and
/// send followers to a different chain.
fn verify_header_inputs(
    payload: &DaPayload,
    block_index: usize,
    block_number: u64,
    block: &EthereumBlock,
) -> Result<(), DaPayloadError> {
    let submitted_beneficiary =
        payload
            .span
            .beneficiaries
            .get(block_index)
            .ok_or(DaPayloadError::HeaderInputMismatch {
                field: "beneficiary",
                block_number,
            })?;
    if submitted_beneficiary != &block.header.beneficiary.0.0 {
        return Err(DaPayloadError::HeaderInputMismatch {
            field: "beneficiary",
            block_number,
        });
    }
    let submitted_extra_data =
        payload
            .span
            .extra_data
            .get(block_index)
            .ok_or(DaPayloadError::HeaderInputMismatch {
                field: "extraData",
                block_number,
            })?;
    if submitted_extra_data.as_slice() != block.header.extra_data.as_ref() {
        return Err(DaPayloadError::HeaderInputMismatch {
            field: "extraData",
            block_number,
        });
    }
    Ok(())
}

fn decode_validated_block(
    block_number: u64,
    block_rlp: &[u8],
) -> Result<EthereumBlock, DaPayloadError> {
    alloy_rlp::decode_exact(block_rlp).map_err(|error| DaPayloadError::InvalidBlockRlp {
        block_number,
        reason: error.to_string(),
    })
}

fn verify_reconstructed_sync_block(
    block: &EthereumBlock,
    validated_transactions: &[Bytes],
    outbound_effects: &AuthorizedOutboundEffects,
    inbound_effects: &AuthorizedInboundEffects<'_>,
    reconstructor: &SystemTransactionReconstructor,
) -> Result<(), DaPayloadError> {
    let first_system_position = outbound_effects
        .iter()
        .next()
        .map(AuthorizedOutboundEffect::load_transaction_index)
        .or_else(|| {
            inbound_effects
                .iter()
                .next()
                .map(AuthorizedInboundEffect::transaction_index)
        })
        .ok_or(DaPayloadError::InvalidEffectTransactionLayout)?;
    let starting_nonce = block
        .body
        .transactions
        .get(first_system_position)
        .ok_or(DaPayloadError::InvalidEffectTransactionLayout)?
        .nonce();
    let outbound_reconstruction_inputs = outbound_effects
        .iter()
        .map(|binding| {
            let user_transaction = validated_transactions
                .get(binding.transaction_index())
                .ok_or(DaPayloadError::InvalidEffectTransactionLayout)?;
            Ok((binding.derived_da_entry().clone(), user_transaction.clone()))
        })
        .collect::<Result<Vec<_>, DaPayloadError>>()?;
    let inbound_reconstruction_entries = inbound_effects
        .iter()
        .map(|binding| binding.observation().derived_da_entry.as_entry().clone())
        .collect::<Vec<_>>();
    let pairs = reconstructor
        .reconstruct_sync_pairs(
            &outbound_reconstruction_inputs,
            &inbound_reconstruction_entries,
            starting_nonce,
        )
        .map_err(|reason| DaPayloadError::SystemTransactionReconstruction { reason })?;
    let reconstructed = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);

    // The canonical Sync layout contains exactly `2 * outbound + inbound`
    // transactions. Reject a different terminal-block length before comparing
    // transaction bytes.
    if reconstructed.len() != validated_transactions.len() {
        return Err(DaPayloadError::SyncBlockTransactionCount {
            expected: reconstructed.len(),
            actual: validated_transactions.len(),
        });
    }
    for (transaction_index, (validated_transaction, reconstructed_transaction)) in
        validated_transactions.iter().zip(reconstructed).enumerate()
    {
        if validated_transaction.as_ref() != reconstructed_transaction.as_ref() {
            return Err(DaPayloadError::SyncBlockTransactionMismatch { transaction_index });
        }
    }
    Ok(())
}

/// The chain id these settlement fixtures build streams for. The signer binds
/// the batch's rollup id, not the stream's chain id, so any stable value works.
#[cfg(test)]
const TEST_CHAIN_ID: u64 = 7331;

/// Encode a DA payload for focused tests, through the shared codec so a test
/// fixture can never encode a shape the composer cannot produce.
///
/// `entries` are projected to actions exactly as the composer projects them,
/// at the rollup id every settlement test uses.
#[cfg(test)]
pub(crate) fn encode_da_payload(
    blocks: &[Vec<Vec<u8>>],
    entries: &[eez_protocol::abi::ExecutionEntrySol],
) -> Vec<u8> {
    let rollup_id = 1;
    let span: Vec<eez_payload_codec::SpanBlock> = blocks
        .iter()
        .map(|transactions| eez_payload_codec::SpanBlock {
            transactions: transactions.clone(),
            ..Default::default()
        })
        .collect();
    let actions: Vec<Action> = entries
        .iter()
        .map(|entry| {
            eez_protocol::entries::manifest::action_from_entry(entry, RollupId(rollup_id))
                .expect("test entry projects to an action")
        })
        .collect();
    eez_payload_codec::encode_container(TEST_CHAIN_ID, &span, &actions)
        .expect("test payload encodes")
}
