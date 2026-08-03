//! Execution validation and settlement-evidence normalization.
//!
//! Values move through explicit trust stages: Composer supplies structurally
//! admitted blocks; this module exact-decodes and re-executes them; a consuming
//! cross-check binds every backend result to its admitted block; only then does
//! [`ValidatedWindow`] expose the evidence used by settlement.

use std::path::Path;

use alloy_primitives::B256;
use thiserror::Error;

use crate::cancel::CancellationToken;
use crate::window::{AdmittedBlock, AdmittedBlocks};

mod stateless;

type EthereumBlock = reth_ethereum_primitives::Block;

/// A cumulative state root locally recomputed during successful block replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionStateCheckpoint {
    /// Zero-based position of the transaction within its block.
    pub(crate) transaction_index: usize,
    /// State root immediately after this transaction, before post-block changes.
    pub(crate) state_root: B256,
}

/// Successful backend output for one block before the shared consuming check.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BackendBlockOutput {
    /// Block number exact-decoded by the backend from consensus RLP.
    pub(crate) decoded_number: u64,
    /// Parent hash exact-decoded by the backend from consensus RLP.
    pub(crate) decoded_parent_hash: B256,
    /// Hash computed from the exact-decoded block header.
    pub(crate) computed_hash: B256,
    /// Transaction count in the exact-decoded block body.
    pub(crate) decoded_transaction_count: usize,
    /// Receipt success flag for every replayed transaction, in block order.
    pub(crate) receipt_successes: Vec<bool>,
    /// Locally computed roots at selected transaction boundaries, strictly
    /// ordered by transaction index. Non-settling blocks have no checkpoints.
    pub(crate) transaction_state_checkpoints: Vec<TransactionStateCheckpoint>,
    /// Post-state root recomputed and matched against the block header.
    pub(crate) post_state_root: B256,
    /// Locally derived facts retained specifically for settlement gates.
    pub(crate) settlement_evidence: SettlementBlockEvidence,
}

/// Successful backend output for a contiguous block window.
///
/// This is not settlement-ready until the consuming cross-check binds each
/// computed hash and transaction count to its corresponding admitted block.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BackendWindowOutput {
    /// Validated state root from which the first block was replayed.
    pub(crate) pre_state_root: B256,
    /// One associated output per replayed block, oldest first.
    pub(crate) blocks: Vec<BackendBlockOutput>,
}

/// One outbound-event candidate observed in the validated execution output.
///
/// `decoded_call_hash` is absent when a log from EEZL2 has the outbound event
/// signature but its complete event encoding is malformed. Retaining that
/// candidate lets settlement fail closed instead of silently ignoring it.
/// Production construction is restricted to validation backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutboundEventObservation {
    /// Zero-based transaction position within the block.
    pub(in crate::validate) transaction_index: usize,
    /// Zero-based log position within that transaction's receipt.
    pub(in crate::validate) receipt_log_index: usize,
    /// Decoded `crossChainCallHash`, when the complete event is canonical.
    pub(in crate::validate) decoded_call_hash: Option<B256>,
}

impl OutboundEventObservation {
    /// Zero-based transaction position within the validated block.
    pub(crate) const fn transaction_index(&self) -> usize {
        self.transaction_index
    }

    /// Zero-based receipt-log position within the transaction.
    pub(crate) const fn receipt_log_index(&self) -> usize {
        self.receipt_log_index
    }

    /// Canonically decoded call hash, or `None` for a malformed candidate.
    pub(crate) const fn decoded_call_hash(&self) -> Option<B256> {
        self.decoded_call_hash
    }

    /// Construct synthetic backend evidence for tests outside `validate`.
    #[cfg(test)]
    pub(crate) const fn for_test(
        transaction_index: usize,
        receipt_log_index: usize,
        decoded_call_hash: Option<B256>,
    ) -> Self {
        Self {
            transaction_index,
            receipt_log_index,
            decoded_call_hash,
        }
    }
}

/// Facts derived locally from the same block and receipts accepted by replay.
/// Production construction is restricted to validation backends; settlement
/// receives only immutable views.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SettlementBlockEvidence {
    /// Whether each transaction's fork-aware recovered signer is the reserved
    /// system address, in exact block order.
    pub(in crate::validate) system_sender_flags: Vec<bool>,
    /// Outbound event candidates, in transaction and receipt-log order.
    pub(in crate::validate) observed_outbound_events: Vec<OutboundEventObservation>,
}

impl SettlementBlockEvidence {
    /// Recovered system-sender classification in exact transaction order.
    pub(crate) fn system_sender_flags(&self) -> &[bool] {
        &self.system_sender_flags
    }

    /// Outbound event candidates in transaction and receipt-log order.
    pub(crate) fn observed_outbound_events(&self) -> &[OutboundEventObservation] {
        &self.observed_outbound_events
    }

    /// Construct synthetic backend evidence for tests outside `validate`.
    #[cfg(test)]
    pub(crate) fn for_test(
        system_sender_flags: Vec<bool>,
        observed_outbound_events: Vec<OutboundEventObservation>,
    ) -> Self {
        Self {
            system_sender_flags,
            observed_outbound_events,
        }
    }

    /// Replace recovered sender evidence in tests that exercise rejection paths.
    #[cfg(test)]
    pub(crate) fn set_system_sender_flags_for_test(&mut self, flags: Vec<bool>) {
        self.system_sender_flags = flags;
    }
}

/// Block data retained after backend validation and output cross-checks.
///
/// Settlement receives the exact RLP and the execution facts required by its
/// framing and effect gates. This type omits the witness consumed by validation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedBlock {
    /// Number exact-decoded from RLP and matched to the Composer declaration.
    number: u64,
    /// Exact Composer-supplied bytes accepted by Stateless replay.
    rlp: Vec<u8>,
    /// Locally derived evidence from the accepted block and its receipts.
    settlement_evidence: SettlementBlockEvidence,
}

impl ValidatedBlock {
    /// Number exact-decoded from the validated block RLP.
    pub(crate) fn number(&self) -> u64 {
        self.number
    }

    /// Exact block RLP accepted by the validation backend.
    pub(crate) fn rlp(&self) -> &[u8] {
        &self.rlp
    }

    /// Locally derived execution evidence bound to this block.
    pub(crate) fn settlement_evidence(&self) -> &SettlementBlockEvidence {
        &self.settlement_evidence
    }

    /// Construct an explicitly synthetic validated block for unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        number: u64,
        rlp: Vec<u8>,
        settlement_evidence: SettlementBlockEvidence,
    ) -> Self {
        Self {
            number,
            rlp,
            settlement_evidence,
        }
    }
}

/// The terminal block and its execution outcome, kept together so settlement
/// cannot accidentally combine evidence from different positions.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedSettlingBlock {
    block: ValidatedBlock,
    /// Receipt status for every transaction in the settling block.
    receipt_successes: Vec<bool>,
    /// Locally recomputed state roots selected for settlement effects.
    transaction_state_checkpoints: Vec<TransactionStateCheckpoint>,
}

impl ValidatedSettlingBlock {
    /// Terminal validated block to which these replay results belong.
    pub(crate) fn block(&self) -> &ValidatedBlock {
        &self.block
    }

    /// Receipt success flag for every transaction, in block order.
    pub(crate) fn receipt_successes(&self) -> &[bool] {
        &self.receipt_successes
    }

    /// Locally recomputed roots requested at selected transaction boundaries.
    pub(crate) fn transaction_state_checkpoints(&self) -> &[TransactionStateCheckpoint] {
        &self.transaction_state_checkpoints
    }

    /// Construct explicitly synthetic settling-block evidence for unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        block: ValidatedBlock,
        receipt_successes: Vec<bool>,
        transaction_state_checkpoints: Vec<TransactionStateCheckpoint>,
    ) -> Self {
        Self {
            block,
            receipt_successes,
            transaction_state_checkpoints,
        }
    }
}

/// Validation output normalized for direct settlement consumption.
///
/// Construction consumes fully cross-checked backend output, separates the
/// terminal block from its predecessors, and derives the root immediately before that
/// terminal block. Callers therefore need no fallible indexing or parallel
/// vector manipulation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ValidatedWindow {
    /// Validated state root before the first streamed block. Settlement still
    /// has to bind the batch's claimed anchor to this value.
    window_pre_state_root: B256,
    /// Validated state root immediately before the settling block.
    settling_pre_state_root: B256,
    /// Validated state root after the settling block and complete window.
    window_post_state_root: B256,
    /// Validated blocks preceding the terminal settling block.
    preceding_blocks: Vec<ValidatedBlock>,
    /// Terminal block together with its settlement-specific replay results.
    settling_block: ValidatedSettlingBlock,
}

impl ValidatedWindow {
    /// Validated state root before the first streamed block.
    pub(crate) fn window_pre_state_root(&self) -> B256 {
        self.window_pre_state_root
    }

    /// Validated state root immediately before the terminal block.
    pub(crate) fn settling_pre_state_root(&self) -> B256 {
        self.settling_pre_state_root
    }

    /// Validated state root after the complete window.
    pub(crate) fn window_post_state_root(&self) -> B256 {
        self.window_post_state_root
    }

    /// Validated blocks preceding the terminal settling block.
    pub(crate) fn preceding_blocks(&self) -> &[ValidatedBlock] {
        &self.preceding_blocks
    }

    /// Terminal block and its settlement-specific replay evidence.
    pub(crate) fn settling_block(&self) -> &ValidatedSettlingBlock {
        &self.settling_block
    }

    /// Construct an explicitly synthetic validated window for unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        window_pre_state_root: B256,
        settling_pre_state_root: B256,
        window_post_state_root: B256,
        preceding_blocks: Vec<ValidatedBlock>,
        settling_block: ValidatedSettlingBlock,
    ) -> Self {
        Self {
            window_pre_state_root,
            settling_pre_state_root,
            window_post_state_root,
            preceding_blocks,
            settling_block,
        }
    }
}

/// One admitted block paired with the successful backend output it was checked against.
struct CheckedBlock {
    admitted: AdmittedBlock,
    output: BackendBlockOutput,
}

/// Backend output whose shape and Composer-facing claims have been consumed
/// and cross-checked. Private fields make bypassing that boundary impossible.
struct CheckedBackendWindowOutput {
    window_pre_state_root: B256,
    preceding_blocks: Vec<CheckedBlock>,
    settling_block: CheckedBlock,
}

/// A validation failure classified for the RPC boundary.
#[derive(Debug, Error)]
pub(crate) enum ValidationError {
    /// The backend rejected the input before claiming successful validation.
    #[error("{0}")]
    Rejected(String),
    /// A backend claimed success with output that violates its internal contract.
    #[error("{0}")]
    InvalidBackendOutput(String),
    /// Locally prepared validation state violated an implementation invariant.
    #[error("{0}")]
    InternalInvariant(String),
    /// The locally derived checkpoint plan exceeds the operator's quota.
    #[error("transaction checkpoint plan requests {requested} roots; maximum is {max}")]
    CheckpointLimit { requested: usize, max: usize },
    /// The request disappeared while a synchronous backend was still active.
    #[error("validation cancelled")]
    Cancelled,
}

/// Validation backend selected at startup.
#[derive(Debug)]
pub(crate) enum Validator {
    /// In-process re-execution using each block's streamed witness.
    Stateless(stateless::Backend),
    /// Canned responses for service-level tests.
    #[cfg(test)]
    Stub(testing::StubValidator),
}

impl Validator {
    /// Construct the production backend from the operator-configured execution
    /// rules. Composer never supplies this trust input.
    pub(crate) fn stateless(chain_document_path: &Path) -> eyre::Result<Self> {
        stateless::Backend::from_chain_document_file(chain_document_path).map(Self::Stateless)
    }

    #[cfg(test)]
    pub(crate) fn stateless_for_test(chain_config: alloy_genesis::ChainConfig) -> Self {
        Self::Stateless(stateless::Backend::new(chain_config))
    }

    /// Static, non-sensitive identifier for startup logs.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Stateless(_) => "stateless",
            #[cfg(test)]
            Self::Stub(_) => "stub",
        }
    }

    /// EIP-155 chain id from the operator-configured replay specification.
    pub(crate) fn chain_id(&self) -> u64 {
        match self {
            Self::Stateless(backend) => backend.chain_id(),
            #[cfg(test)]
            Self::Stub(_) => 1,
        }
    }

    /// Validate admitted blocks through the same checked boundary as production.
    #[cfg(test)]
    pub(crate) fn validate(
        &self,
        blocks: &[AdmittedBlock],
    ) -> Result<ValidatedWindow, ValidationError> {
        if blocks.is_empty() {
            return Err(ValidationError::Rejected(
                "refusing to validate an empty window".to_owned(),
            ));
        }
        self.validate_window(
            AdmittedBlocks::for_test(blocks.to_vec()),
            &CancellationToken::default(),
            usize::MAX,
        )
    }

    /// Consume and validate a window with cooperative cancellation between
    /// non-interruptible backend work units. Ownership lets re-executing
    /// backends move witnesses without cloning their outer collections.
    pub(crate) fn validate_window(
        &self,
        blocks: AdmittedBlocks,
        cancellation: &CancellationToken,
        max_transaction_state_checkpoints: usize,
    ) -> Result<ValidatedWindow, ValidationError> {
        let mut blocks = blocks.into_vec();
        let output =
            self.run_backend(&mut blocks, cancellation, max_transaction_state_checkpoints)?;
        check_backend_window_output(blocks, output)
            .map(CheckedBackendWindowOutput::into_validated_window)
            .map_err(|error| ValidationError::InvalidBackendOutput(error.to_string()))
    }

    /// Run the selected backend. Its result remains unchecked against the
    /// Composer-facing claims until `check_backend_window_output` consumes it.
    fn run_backend(
        &self,
        blocks: &mut [AdmittedBlock],
        cancellation: &CancellationToken,
        max_transaction_state_checkpoints: usize,
    ) -> Result<BackendWindowOutput, ValidationError> {
        match self {
            Self::Stateless(backend) => {
                backend.validate_blocks(blocks, cancellation, max_transaction_state_checkpoints)
            }
            #[cfg(test)]
            Self::Stub(stub) => {
                if cancellation.is_cancelled() {
                    return Err(ValidationError::Cancelled);
                }
                stub.next_response()
                    .map_err(|error| ValidationError::Rejected(error.to_string()))
            }
        }
    }
}

/// Consume the backend result while binding every computed value to the
/// admitted Composer input. Successful construction is the type-state boundary
/// after which settlement evidence may be exposed.
fn check_backend_window_output(
    admitted_blocks: Vec<AdmittedBlock>,
    backend_output: BackendWindowOutput,
) -> eyre::Result<CheckedBackendWindowOutput> {
    eyre::ensure!(
        backend_output.blocks.len() == admitted_blocks.len(),
        "backend output covers {} blocks, the admitted window has {}",
        backend_output.blocks.len(),
        admitted_blocks.len(),
    );

    let total_blocks = admitted_blocks.len();
    let mut checked_blocks = Vec::with_capacity(total_blocks);
    for (position, (admitted, output)) in admitted_blocks
        .into_iter()
        .zip(backend_output.blocks)
        .enumerate()
    {
        eyre::ensure!(
            output.decoded_number == admitted.declared_number,
            "backend decoded block number {} for Composer-declared block {}",
            output.decoded_number,
            admitted.declared_number,
        );
        eyre::ensure!(
            output.decoded_parent_hash == admitted.claimed_parent_hash,
            "backend decoded parent hash {} for block {} but Composer claimed {}",
            output.decoded_parent_hash,
            admitted.declared_number,
            admitted.claimed_parent_hash,
        );
        eyre::ensure!(
            output.computed_hash == admitted.claimed_hash,
            "computed hash {} for block {} does not match Composer-claimed hash {}",
            output.computed_hash,
            admitted.declared_number,
            admitted.claimed_hash,
        );
        let rlp_transaction_count = decode_transaction_count(&admitted)?;
        eyre::ensure!(
            output.decoded_transaction_count == rlp_transaction_count,
            "backend decoded {} transactions for block {}, exact RLP contains {}",
            output.decoded_transaction_count,
            admitted.declared_number,
            rlp_transaction_count,
        );
        let transaction_count = output.decoded_transaction_count;
        eyre::ensure!(
            output.receipt_successes.len() == transaction_count,
            "receipt statuses for block {} cover {} transactions, expected {}",
            admitted.declared_number,
            output.receipt_successes.len(),
            transaction_count,
        );
        let settlement_evidence = &output.settlement_evidence;
        eyre::ensure!(
            settlement_evidence.system_sender_flags.len() == transaction_count,
            "system-sender flags for block {} cover {} transactions, expected {}",
            admitted.declared_number,
            settlement_evidence.system_sender_flags.len(),
            transaction_count,
        );
        for observation in &settlement_evidence.observed_outbound_events {
            eyre::ensure!(
                observation.transaction_index < transaction_count,
                "outbound event observation targets transaction {} in block {} with {} \
                 transactions",
                observation.transaction_index,
                admitted.declared_number,
                transaction_count,
            );
        }
        for pair in settlement_evidence.observed_outbound_events.windows(2) {
            let previous = (pair[0].transaction_index, pair[0].receipt_log_index);
            let current = (pair[1].transaction_index, pair[1].receipt_log_index);
            eyre::ensure!(
                previous < current,
                "outbound event observations for block {} are not strictly ordered: {:?} is \
                 followed by {:?}",
                admitted.declared_number,
                previous,
                current,
            );
        }
        let checkpoints = &output.transaction_state_checkpoints;
        eyre::ensure!(
            position + 1 == total_blocks || checkpoints.is_empty(),
            "backend output supplied transaction state checkpoints for preceding block {}",
            admitted.declared_number,
        );
        for pair in checkpoints.windows(2) {
            eyre::ensure!(
                pair[0].transaction_index < pair[1].transaction_index,
                "transaction state checkpoints for block {} are not strictly ordered: index \
                 {} is followed by {}",
                admitted.declared_number,
                pair[0].transaction_index,
                pair[1].transaction_index,
            );
        }
        for checkpoint in checkpoints {
            eyre::ensure!(
                checkpoint.transaction_index < transaction_count,
                "transaction state checkpoint index {} is out of bounds for block {} with {} \
                 transactions",
                checkpoint.transaction_index,
                admitted.declared_number,
                transaction_count,
            );
        }

        checked_blocks.push(CheckedBlock { admitted, output });
    }

    let settling_block = checked_blocks
        .pop()
        .ok_or_else(|| eyre::eyre!("backend output unexpectedly has no blocks"))?;
    Ok(CheckedBackendWindowOutput {
        window_pre_state_root: backend_output.pre_state_root,
        preceding_blocks: checked_blocks,
        settling_block,
    })
}

impl CheckedBackendWindowOutput {
    /// Rearrange already checked evidence for settlement without performing
    /// another trust transition.
    fn into_validated_window(self) -> ValidatedWindow {
        let settling_pre_state_root = self
            .preceding_blocks
            .last()
            .map(|block| block.output.post_state_root)
            .unwrap_or(self.window_pre_state_root);
        let window_post_state_root = self.settling_block.output.post_state_root;
        ValidatedWindow {
            window_pre_state_root: self.window_pre_state_root,
            settling_pre_state_root,
            window_post_state_root,
            preceding_blocks: self
                .preceding_blocks
                .into_iter()
                .map(CheckedBlock::into_validated_block)
                .collect(),
            settling_block: self.settling_block.into_validated_settling_block(),
        }
    }
}

impl CheckedBlock {
    /// Retain only the admitted bytes and locally derived settlement evidence.
    fn into_validated_block(self) -> ValidatedBlock {
        ValidatedBlock {
            number: self.output.decoded_number,
            rlp: self.admitted.rlp,
            settlement_evidence: self.output.settlement_evidence,
        }
    }

    /// Preserve the settling block's receipt statuses and computed checkpoints.
    fn into_validated_settling_block(self) -> ValidatedSettlingBlock {
        let BackendBlockOutput {
            decoded_number,
            receipt_successes,
            transaction_state_checkpoints,
            settlement_evidence,
            ..
        } = self.output;
        ValidatedSettlingBlock {
            block: ValidatedBlock {
                number: decoded_number,
                rlp: self.admitted.rlp,
                settlement_evidence,
            },
            receipt_successes,
            transaction_state_checkpoints,
        }
    }
}

/// Exact-decode Composer-supplied consensus RLP for result-coverage checks.
fn decode_transaction_count(block: &AdmittedBlock) -> eyre::Result<usize> {
    alloy_rlp::decode_exact::<EthereumBlock>(&block.rlp)
        .map(|block| block.body.transactions.len())
        .map_err(|error| {
            eyre::eyre!(
                "block {} RLP does not decode exactly: {error}",
                block.declared_number
            )
        })
}

/// Test-support stub backend, shared with the service-level tests.
#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
