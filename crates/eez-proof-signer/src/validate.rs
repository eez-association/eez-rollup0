//! Execution validation and settlement-evidence normalization.
//!
//! Values move through explicit trust stages: Composer supplies structurally
//! admitted blocks; backend adapters bind and execute them, then construct
//! immutable checked blocks. Prefix owners assemble windows; settlement relies
//! on those guarantees when checking the newly submitted batch.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_rpc_types_debug::ExecutionWitness;
use eez_control_rpc::v1::ExecutionWitness as WireExecutionWitness;
use thiserror::Error;

use crate::cancel::CancellationToken;
use crate::window::AdmittedBlocks;

pub mod support;

type EthereumBlock = eez_primitives::Block;

/// Untrusted block input shared by v1 window and v2 session validation.
///
/// Transport parsing checks hash widths and witness presence. Validation must
/// bind the number and hash claims to the RLP, check the parent, and execute it.
#[derive(Debug, Clone)]
pub struct AdmittedBlock {
    pub(crate) declared_number: u64,
    pub(crate) claimed_hash: B256,
    pub(crate) claimed_parent_hash: B256,
    pub(crate) rlp: Vec<u8>,
    pub(crate) witness: ExecutionWitness,
}

impl AdmittedBlock {
    /// Reject a submission that cannot extend the supplied parent before execution.
    pub fn check_parent(&self, parent: BlockAnchor) -> Result<(), ValidationError> {
        if parent.number.checked_add(1) != Some(self.declared_number)
            || parent.hash != self.claimed_parent_hash
        {
            return Err(ValidationError::Rejected(
                "block does not extend its exact parent".to_owned(),
            ));
        }
        Ok(())
    }

    /// Composer-declared block number admitted from the stream.
    pub const fn declared_number(&self) -> u64 {
        self.declared_number
    }

    /// Composer-claimed block hash admitted from the stream.
    pub const fn claimed_hash(&self) -> B256 {
        self.claimed_hash
    }

    /// Composer-claimed parent hash admitted from the stream.
    pub const fn claimed_parent_hash(&self) -> B256 {
        self.claimed_parent_hash
    }

    /// Exact consensus RLP admitted from the stream.
    pub fn rlp(&self) -> &[u8] {
        &self.rlp
    }

    /// Move the admitted execution witness into a consuming backend.
    #[doc(hidden)]
    pub(crate) fn take_witness(&mut self) -> ExecutionWitness {
        std::mem::take(&mut self.witness)
    }

    /// Construct the minimal admitted block used by unit tests.
    #[cfg(test)]
    pub(crate) fn test(
        declared_number: u64,
        claimed_parent_hash_byte: u8,
        claimed_hash_byte: u8,
    ) -> Self {
        Self {
            declared_number,
            claimed_hash: B256::repeat_byte(claimed_hash_byte),
            claimed_parent_hash: B256::repeat_byte(claimed_parent_hash_byte),
            rlp: Vec::new(),
            witness: ExecutionWitness::default(),
        }
    }
}

/// Convert the protobuf witness into the Alloy type consumed by backends.
/// Converting each payload into `Bytes` reuses its source allocation.
pub(crate) fn into_execution_witness(witness: WireExecutionWitness) -> ExecutionWitness {
    let WireExecutionWitness {
        state,
        codes,
        keys,
        headers,
    } = witness;
    ExecutionWitness {
        state: state.into_iter().map(Into::into).collect(),
        codes: codes.into_iter().map(Into::into).collect(),
        keys: keys.into_iter().map(Into::into).collect(),
        headers: headers.into_iter().map(Into::into).collect(),
    }
}

/// Count protobuf witness items before allocating the retained representation.
pub(crate) fn wire_witness_item_count(witness: &WireExecutionWitness) -> usize {
    witness
        .state
        .len()
        .saturating_add(witness.codes.len())
        .saturating_add(witness.keys.len())
        .saturating_add(witness.headers.len())
}

/// Where a candidate is sealed: the pre-execution seal or a transaction
/// boundary. Re-exported from the validator so both backends and this contract
/// name one type.
pub use stateless_reth::CheckpointAt;

/// One checkpoint position's replay outputs: the cumulative state root and the
/// hash of the candidate block sealed over that prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateCheckpoint {
    /// Where in the block this was sealed.
    pub at: CheckpointAt,
    /// State root at this position, before post-block changes.
    pub state_root: B256,
    /// Hash of the candidate block holding exactly this prefix — what settlement
    /// gates compare an entry's claimed `newState` against.
    pub block_hash: B256,
}

/// Successful backend output for one block before the shared consuming check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendBlockOutput {
    /// Hash computed from the exact-decoded block header.
    pub computed_hash: B256,
    /// Receipt success flag for every replayed transaction, in block order.
    pub receipt_successes: Vec<bool>,
    /// Locally computed roots at the selected positions, in execution order: the
    /// empty prefix first, then transaction boundaries. Non-settling blocks have
    /// no checkpoints.
    pub transaction_state_checkpoints: Vec<StateCheckpoint>,
    /// Post-state root recomputed and matched against the block header.
    pub post_state_root: B256,
    /// Locally derived facts retained specifically for settlement gates.
    pub settlement_evidence: SettlementBlockEvidence,
}

/// Successful backend output for a contiguous block window.
///
/// The backend binds each admitted input in order and verifies execution-state
/// continuity; v1 admission owns the declared sequence and completeness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendWindowOutput {
    /// One associated output per replayed block, oldest first.
    pub blocks: Vec<Arc<ValidatedBlock>>,
}

/// Canonically decoded fields used to bind an outbound effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedOutboundEvent {
    call_hash: B256,
    call_gas: u64,
}

impl DecodedOutboundEvent {
    /// Construct fields recovered from one canonical `EEZL2` event.
    pub const fn new(call_hash: B256, call_gas: u64) -> Self {
        Self {
            call_hash,
            call_gas,
        }
    }

    /// Hash emitted for the executed call.
    pub const fn call_hash(&self) -> B256 {
        self.call_hash
    }

    /// Manager-entry gas value included in the emitted hash.
    ///
    /// This is distinct from any gas limit forwarded to the destination.
    pub const fn call_gas(&self) -> u64 {
        self.call_gas
    }
}

/// One outbound-event candidate observed in the validated execution output.
///
/// `decoded_event` is absent when a log from EEZL2 has the outbound event
/// signature but its complete event encoding is malformed. Retaining that
/// candidate lets settlement fail closed instead of silently ignoring it.
/// Production construction is restricted to validation backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundEventObservation {
    /// Zero-based transaction position within the block.
    pub transaction_index: usize,
    /// Zero-based log position within that transaction's receipt.
    pub receipt_log_index: usize,
    /// Decoded fields, when the complete event is canonical.
    pub decoded_event: Option<DecodedOutboundEvent>,
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

    /// Canonically decoded event, or `None` for a malformed candidate.
    pub(crate) const fn decoded_event(&self) -> Option<DecodedOutboundEvent> {
        self.decoded_event
    }

    /// Construct synthetic backend evidence with decoded event fields.
    #[cfg(test)]
    pub(crate) const fn decoded_for_test(
        transaction_index: usize,
        receipt_log_index: usize,
        call_hash: B256,
        call_gas: u64,
    ) -> Self {
        Self {
            transaction_index,
            receipt_log_index,
            decoded_event: Some(DecodedOutboundEvent::new(call_hash, call_gas)),
        }
    }

    /// Construct a malformed synthetic event candidate for rejection tests.
    #[cfg(test)]
    pub(crate) const fn malformed_for_test(
        transaction_index: usize,
        receipt_log_index: usize,
    ) -> Self {
        Self {
            transaction_index,
            receipt_log_index,
            decoded_event: None,
        }
    }
}

/// Facts derived locally from the same block and receipts accepted by replay.
/// Production construction is restricted to validation backends; settlement
/// receives only immutable views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementBlockEvidence {
    /// Whether each transaction's fork-aware recovered signer is the reserved
    /// system address, in exact block order.
    pub system_sender_flags: Vec<bool>,
    /// Outbound event candidates, in transaction and receipt-log order.
    pub observed_outbound_events: Vec<OutboundEventObservation>,
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

/// Immutable block and execution evidence, checked once by the backend adapter.
/// Sessions, backend caches and settlement share this same representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedBlock {
    number: u64,
    hash: B256,
    parent_hash: B256,
    pre_state_root: B256,
    post_state_root: B256,
    rlp: Vec<u8>,
    decoded: EthereumBlock,
    receipt_successes: Vec<bool>,
    transaction_state_checkpoints: Vec<StateCheckpoint>,
    settlement_evidence: SettlementBlockEvidence,
}

impl ValidatedBlock {
    /// Estimated retained bytes for cache admission, including decoded data and evidence.
    /// Shared allocations are charged in full; allocator bookkeeping is not included.
    pub fn size(&self) -> usize {
        use reth_primitives_traits::InMemorySize as _;
        std::mem::size_of::<Self>()
            + self.rlp.capacity()
            + self.decoded.size()
            + self.receipt_successes.capacity()
            + self.transaction_state_checkpoints.capacity() * std::mem::size_of::<StateCheckpoint>()
            + self.settlement_evidence.system_sender_flags.capacity()
            + self.settlement_evidence.observed_outbound_events.capacity()
                * std::mem::size_of::<OutboundEventObservation>()
    }

    pub const fn number(&self) -> u64 {
        self.number
    }
    pub const fn hash(&self) -> B256 {
        self.hash
    }
    pub const fn parent_hash(&self) -> B256 {
        self.parent_hash
    }
    pub const fn pre_state_root(&self) -> B256 {
        self.pre_state_root
    }
    pub const fn post_state_root(&self) -> B256 {
        self.post_state_root
    }
    pub fn rlp(&self) -> &[u8] {
        &self.rlp
    }
    pub fn decoded(&self) -> &EthereumBlock {
        &self.decoded
    }
    pub fn receipt_successes(&self) -> &[bool] {
        &self.receipt_successes
    }
    pub fn transaction_state_checkpoints(&self) -> &[StateCheckpoint] {
        &self.transaction_state_checkpoints
    }
    pub fn settlement_evidence(&self) -> &SettlementBlockEvidence {
        &self.settlement_evidence
    }
    pub fn as_anchor(&self) -> BlockAnchor {
        BlockAnchor {
            number: self.number,
            hash: self.hash,
            state_root: self.post_state_root,
        }
    }

    /// Bind a new cache-hit submission to the previously checked execution.
    pub fn matches_submission(
        &self,
        admitted: &AdmittedBlock,
        parent: BlockAnchor,
    ) -> Result<(), ValidationError> {
        admitted.check_parent(parent)?;
        if self.number != admitted.declared_number
            || self.hash != admitted.claimed_hash
            || self.parent_hash != admitted.claimed_parent_hash
            || self.rlp != admitted.rlp
            || self.pre_state_root != parent.state_root
        {
            return Err(ValidationError::Rejected(
                "cached block does not match the submitted bytes and parent".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        number: u64,
        rlp: Vec<u8>,
        settlement_evidence: SettlementBlockEvidence,
    ) -> Self {
        let decoded = alloy_rlp::decode_exact::<EthereumBlock>(&rlp).expect("synthetic block RLP");
        Self {
            number,
            hash: decoded.header.hash_slow(),
            parent_hash: decoded.header.parent_hash,
            pre_state_root: B256::ZERO,
            post_state_root: decoded.header.state_root,
            rlp,
            receipt_successes: vec![true; decoded.body.transactions.len()],
            decoded,
            transaction_state_checkpoints: Vec::new(),
            settlement_evidence,
        }
    }
}

/// A selected nonempty range of a validated prefix. Block data is shared, not
/// decoded or copied again when a session selects a settlement window.
#[derive(Debug, PartialEq, Eq)]
pub struct ValidatedWindow {
    window_pre_block_hash: B256,
    preceding_blocks: Vec<Arc<ValidatedBlock>>,
    settling_block: Arc<ValidatedBlock>,
}

impl ValidatedWindow {
    pub const fn window_pre_block_hash(&self) -> B256 {
        self.window_pre_block_hash
    }
    #[cfg(test)]
    pub(crate) fn settling_pre_block_hash(&self) -> B256 {
        self.settling_block.parent_hash
    }
    pub fn window_post_block_hash(&self) -> B256 {
        self.settling_block.hash
    }
    pub fn preceding_blocks(&self) -> &[Arc<ValidatedBlock>] {
        &self.preceding_blocks
    }
    pub fn settling_block(&self) -> &ValidatedBlock {
        &self.settling_block
    }
    pub fn blocks(&self) -> impl Iterator<Item = &ValidatedBlock> {
        self.preceding_blocks
            .iter()
            .map(AsRef::as_ref)
            .chain(std::iter::once(self.settling_block.as_ref()))
    }

    /// Package a range from a validated prefix. V1 admission/backend replay or
    /// v2 session append established continuity; the caller owns range selection.
    pub(crate) fn from_validated_prefix(
        anchor: B256,
        preceding_blocks: Vec<Arc<ValidatedBlock>>,
        settling_block: Arc<ValidatedBlock>,
    ) -> Self {
        Self {
            window_pre_block_hash: anchor,
            preceding_blocks,
            settling_block,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        window_pre_block_hash: B256,
        _settling_pre_block_hash: B256,
        window_post_block_hash: B256,
        preceding_blocks: Vec<ValidatedBlock>,
        mut settling_block: ValidatedBlock,
    ) -> Self {
        settling_block.hash = window_post_block_hash;
        Self {
            window_pre_block_hash,
            preceding_blocks: preceding_blocks.into_iter().map(Arc::new).collect(),
            settling_block: Arc::new(settling_block),
        }
    }
}

/// A validation failure classified for the RPC boundary.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// Block validation did not complete within its configured deadline.
    #[error("block validation deadline exceeded")]
    DeadlineExceeded,
    /// The backend cannot currently acquire the required validation state.
    #[error("{0}")]
    Unavailable(String),
    /// The backend's validation snapshot changed while work was in progress.
    #[error("{0}")]
    Aborted(String),
    /// The backend rejected the input before claiming successful validation.
    #[error("{0}")]
    Rejected(String),
    /// A backend claimed success with output that violates its internal contract.
    #[error("{0}")]
    InvalidBackendOutput(String),
    /// Locally prepared validation state violated an implementation invariant.
    #[error("{0}")]
    InternalInvariant(String),
    /// The request disappeared while a synchronous backend was still active.
    #[error("validation cancelled")]
    Cancelled,
}

/// Exact block identity and state root at an execution boundary.
/// Used for the session anchor and the validated parent of a new block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockAnchor {
    pub number: u64,
    pub hash: B256,
    pub state_root: B256,
}

/// Execution-evidence source used by the shared settlement and signing pipeline.
/// Implementations are trusted to derive all evidence from replay.
#[async_trait::async_trait]
pub trait ValidationBackend: std::fmt::Debug + Send + Sync + 'static {
    /// Static identifier used only in diagnostics.
    fn label(&self) -> &'static str;

    /// EIP-155 identity fixed by the backend's execution rules.
    fn chain_id(&self) -> u64;

    /// Deployment address used to classify privileged L2 transactions.
    fn expected_l2_system_address(&self) -> alloy_primitives::Address;

    /// Replay the admitted sequence in order, verify execution-state continuity,
    /// and return checked evidence bound to each corresponding input.
    /// `witnesses[i]` belongs to `blocks[i]` and may be consumed by the backend.
    fn validate_blocks(
        &self,
        blocks: &[AdmittedBlock],
        witnesses: &mut [ExecutionWitness],
        cancellation: &CancellationToken,
    ) -> Result<BackendWindowOutput, ValidationError>;

    /// Check a proving session's starting anchor without allocating a backend cursor.
    /// Legacy-only backends reject v2 admission until they are migrated.
    fn validate_anchor(&self, _anchor: BlockAnchor) -> Result<(), ValidationError> {
        Err(ValidationError::Unavailable(format!(
            "{} backend does not support per-block validation",
            self.label()
        )))
    }

    /// Validate or reuse one block against its exact parent, returning whether execution was reused.
    /// The parent is the session's admitted anchor or a previously validated block.
    /// The session owns prefix continuity; safe compatibility is enforced at finalization.
    /// Backends own their caches (including checkpoints), execution limits and deadlines.
    /// Cache hits must bind the submitted bytes and parent; only successful results may be cached.
    async fn validate_next(
        &self,
        _parent: BlockAnchor,
        _block: &AdmittedBlock,
        _witness: ExecutionWitness,
        _request_timeout: Duration,
    ) -> Result<(Arc<ValidatedBlock>, bool), ValidationError> {
        Err(ValidationError::Unavailable(format!(
            "{} backend does not support per-block validation",
            self.label()
        )))
    }

    /// Prepare a selected, already-validated window for attestation. Stateful
    /// backends check retention and require Reth to accept the terminal forkchoice;
    /// immutable block identity and range continuity are supplied by the window.
    /// Success describes this operation's snapshot, not a lock held until signing.
    async fn prepare_attestation(&self, _window: &ValidatedWindow) -> Result<(), ValidationError> {
        Ok(())
    }

    /// Number of queued actions exposed only by the canned unit-test backend.
    #[cfg(test)]
    fn remaining_test_actions(&self) -> Option<usize> {
        None
    }
}

/// Execute an admitted v1 window and package its checked blocks for settlement.
/// Move witnesses into the backend without cloning their outer collections.
pub(crate) fn validate_window(
    backend: &dyn ValidationBackend,
    blocks: AdmittedBlocks,
    cancellation: &CancellationToken,
) -> Result<ValidatedWindow, ValidationError> {
    let mut blocks = blocks.into_vec();
    let mut witnesses = blocks
        .iter_mut()
        .map(AdmittedBlock::take_witness)
        .collect::<Vec<_>>();
    let output = backend.validate_blocks(&blocks, &mut witnesses, cancellation)?;
    if output.blocks.len() != blocks.len() {
        return Err(ValidationError::InvalidBackendOutput(
            "backend block count does not match admitted window".to_owned(),
        ));
    }
    // V1 admission checked the claimed sequence; each backend binds those
    // claims and verifies execution continuity while replaying in order.
    let mut preceding = output.blocks;
    let terminal = preceding.pop().ok_or_else(|| {
        ValidationError::Rejected("refusing to validate an empty window".to_owned())
    })?;
    let anchor = preceding
        .first()
        .map_or(terminal.parent_hash, |block| block.parent_hash);
    Ok(ValidatedWindow::from_validated_prefix(
        anchor, preceding, terminal,
    ))
}

/// Check backend evidence once, before a result becomes cacheable or visible to
/// a session. The decoded input was already bound to the submitted RLP.
fn check_backend_block_output(
    number: u64,
    expected_hash: B256,
    decoded: &EthereumBlock,
    output: &BackendBlockOutput,
    allow_checkpoints: bool,
) -> eyre::Result<()> {
    eyre::ensure!(
        output.computed_hash == expected_hash,
        "computed hash {} for block {} does not match Composer-claimed hash {}",
        output.computed_hash,
        number,
        expected_hash,
    );
    let transaction_count = decoded.body.transactions.len();
    eyre::ensure!(
        output.receipt_successes.len() == transaction_count,
        "receipt statuses for block {} cover {} transactions, expected {}",
        number,
        output.receipt_successes.len(),
        transaction_count,
    );
    let settlement_evidence = &output.settlement_evidence;
    eyre::ensure!(
        settlement_evidence.system_sender_flags.len() == transaction_count,
        "system-sender flags for block {} cover {} transactions, expected {}",
        number,
        settlement_evidence.system_sender_flags.len(),
        transaction_count,
    );
    for observation in &settlement_evidence.observed_outbound_events {
        eyre::ensure!(
            observation.transaction_index < transaction_count,
            "outbound event observation targets transaction {} in block {} with {} \
                 transactions",
            observation.transaction_index,
            number,
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
            number,
            previous,
            current,
        );
    }
    let checkpoints = &output.transaction_state_checkpoints;
    eyre::ensure!(
        allow_checkpoints || checkpoints.is_empty(),
        "backend output supplied transaction state checkpoints for preceding block {}",
        number,
    );
    // `CheckpointAt`'s variant order is execution order, so `Ord` is the
    // ordering check: pre-execution precedes every transaction boundary.
    for pair in checkpoints.windows(2) {
        eyre::ensure!(
            pair[0].at < pair[1].at,
            "state checkpoints for block {} are not strictly ordered: {} is followed by {}",
            number,
            pair[0].at,
            pair[1].at,
        );
    }
    for checkpoint in checkpoints {
        // Exhaustive, so a new position kind cannot skip this check unseen.
        let index = match checkpoint.at {
            CheckpointAt::Transaction(index) => index,
            // Names no transaction, so there is nothing to bound.
            CheckpointAt::PreExecution => continue,
        };
        eyre::ensure!(
            index < transaction_count,
            "state checkpoint at {} is out of bounds for block {} with {} transactions",
            checkpoint.at,
            number,
            transaction_count,
        );
    }

    Ok(())
}

/// Test-support stub backend, shared with the service-level tests.
#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
