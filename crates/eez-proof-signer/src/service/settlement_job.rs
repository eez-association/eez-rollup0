//! Synchronous validation and settlement performed by one `Prove` request.

use std::num::NonZeroU64;

use alloy_primitives::{Address, B256};
use eez_control_rpc::v1::{InboundFailure, OutboundFailure, ProveFailure, prove_failure};
use tokio::time::Instant;
use tonic::Status;

use super::ServiceState;
use crate::cancel::CancellationToken;
use crate::{settlement, validate, window};

/// A locally recomputed digest authorized by every settlement gate.
///
/// Its private field makes the complete settlement job the only constructor;
/// the attester therefore cannot accept a profile-only recomputation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttestablePublicInputsHash(settlement::RecomputedPublicInputsHash);

impl AttestablePublicInputsHash {
    /// Return the digest bytes after the full request pipeline authorized them.
    pub(crate) const fn into_inner(self) -> B256 {
        self.0.into_inner()
    }
}

/// Fully checked material that may cross the attestation boundary.
pub(super) struct AttestationMaterial {
    pub(super) attestable_public_inputs_hash: AttestablePublicInputsHash,
    pub(super) validated_window_pre_state_root: B256,
    pub(super) validated_window_post_state_root: B256,
}

/// Failure from either synchronous phase of the request pipeline.
#[derive(Debug, thiserror::Error)]
pub(super) enum PipelineError {
    #[error(transparent)]
    Validation(#[from] validate::ValidationError),
    #[error(transparent)]
    Settlement(#[from] SettlementPipelineError),
    #[error("request deadline elapsed before settlement")]
    DeadlineBeforeSettlement,
}

impl PipelineError {
    pub(super) const fn phase(&self) -> &'static str {
        match self {
            Self::Validation(_) => "validation",
            Self::Settlement(_) | Self::DeadlineBeforeSettlement => "settlement",
        }
    }

    pub(super) const fn gate(&self) -> &'static str {
        match self {
            Self::Validation(_) => "execution",
            Self::Settlement(error) => error.gate(),
            Self::DeadlineBeforeSettlement => "deadline",
        }
    }

    pub(super) fn status(&self) -> Status {
        match self {
            Self::Validation(error) => match error {
                validate::ValidationError::Rejected(_) => {
                    Status::failed_precondition("window validation rejected")
                }
                validate::ValidationError::InvalidBackendOutput(_) => {
                    Status::internal("validation backend returned invalid output")
                }
                validate::ValidationError::InternalInvariant(_) => {
                    Status::internal("request pipeline invariant failed")
                }
                validate::ValidationError::CheckpointLimit { .. } => {
                    Status::resource_exhausted("window validation checkpoint quota exceeded")
                }
                validate::ValidationError::Cancelled => {
                    Status::cancelled("Prove request cancelled")
                }
            },
            Self::Settlement(error) => error.status(),
            Self::DeadlineBeforeSettlement => {
                Status::deadline_exceeded("Prove request deadline exceeded")
            }
        }
    }
}

/// Re-execute the window and immediately bind its evidence to settlement.
///
/// Keeping both CPU-bound phases in one worker avoids a second blocking-task
/// handoff and prevents callers from handling parallel output/block vectors.
pub(super) fn validate_and_settle(
    state: &ServiceState,
    blocks: window::AdmittedBlocks,
    submitted_post_batch_calldata: Vec<u8>,
    max_transaction_state_checkpoints: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<AttestationMaterial, PipelineError> {
    let validated_window =
        state
            .validator
            .validate_window(blocks, cancellation, max_transaction_state_checkpoints)?;
    if Instant::now() >= deadline {
        return Err(PipelineError::DeadlineBeforeSettlement);
    }
    let attestable_public_inputs_hash = run_settlement(SettlementInput {
        submitted_post_batch_calldata,
        validated_window: &validated_window,
        expected_rollup_id: state.expected_rollup_id,
        expected_l2_system_address: state.expected_l2_system_address,
        proof_system_vkey: state.attester.proof_system_vkey(),
        expected_proof_system: state.attester.expected_proof_system(),
        system_transaction_reconstructor: &state.system_transaction_reconstructor,
        cancellation,
    })?;

    Ok(AttestationMaterial {
        attestable_public_inputs_hash,
        validated_window_pre_state_root: validated_window.window_pre_state_root(),
        validated_window_post_state_root: validated_window.window_post_state_root(),
    })
}

/// Inputs for the synchronous settlement job.
///
/// The job consumes the submitted calldata at its decode boundary. Other large
/// values remain borrowed from the validated window owned by the worker.
pub(super) struct SettlementInput<'a> {
    pub(super) submitted_post_batch_calldata: Vec<u8>,
    pub(super) validated_window: &'a validate::ValidatedWindow,
    pub(super) expected_rollup_id: NonZeroU64,
    pub(super) expected_l2_system_address: Address,
    pub(super) proof_system_vkey: crate::attest::NonZeroProofSystemVkey,
    pub(super) expected_proof_system: Address,
    pub(super) system_transaction_reconstructor: &'a settlement::SystemTransactionReconstructor,
    pub(super) cancellation: &'a CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SettlementPipelineError {
    #[error("{source}")]
    Actionable {
        source: Box<SettlementPipelineError>,
        failure: ProveFailure,
    },
    #[error(transparent)]
    PostBatchCalldata(#[from] settlement::PostBatchDecodeError),
    #[error(transparent)]
    PublicInputs(#[from] settlement::PublicInputError),
    #[error(transparent)]
    BlockInspection(#[from] settlement::BlockInspectionError),
    #[error(transparent)]
    StateUpdateChain(#[from] settlement::StateUpdateChainError),
    #[error(transparent)]
    EffectPrefix(#[from] settlement::EffectPrefixError),
    #[error(transparent)]
    InboundEffects(#[from] settlement::InboundEffectError),
    #[error(transparent)]
    OutboundEffects(#[from] settlement::OutboundEffectError),
    #[error(transparent)]
    DaPayload(#[from] settlement::DaPayloadError),
    #[error("request cancelled")]
    Cancelled,
}

impl SettlementPipelineError {
    pub(super) const fn gate(&self) -> &'static str {
        match self {
            Self::Actionable { source, .. } => source.gate(),
            Self::PostBatchCalldata(_) => "post_batch_calldata",
            Self::PublicInputs(_) => "public_inputs",
            Self::BlockInspection(_) => "block_inspection",
            Self::StateUpdateChain(_) => "state_update_chain",
            Self::EffectPrefix(_) => "effect_prefix",
            Self::InboundEffects(_) => "inbound_effects",
            Self::OutboundEffects(_) => "outbound_effects",
            Self::DaPayload(_) => "da_payload",
            Self::Cancelled => "cancellation",
        }
    }

    /// Map every pipeline error to its public gRPC code and message.
    pub(super) fn status(&self) -> Status {
        if let Self::Actionable { source, failure } = self {
            let status = source.status();
            debug_assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            return Status::with_details(
                status.code(),
                status.message(),
                eez_control_rpc::encode_prove_failure(failure).into(),
            );
        }

        let (code, message) = match self {
            Self::Actionable { .. } => unreachable!("handled above"),
            Self::PostBatchCalldata(_) => {
                (tonic::Code::InvalidArgument, "invalid PostBatch calldata")
            }
            Self::PublicInputs(
                settlement::PublicInputError::Computation(_)
                | settlement::PublicInputError::HashCount { .. },
            ) => (
                tonic::Code::Internal,
                "public-input hash computation violated its contract",
            ),
            Self::BlockInspection(error) => match error {
                settlement::BlockInspectionError::InvalidRlp { .. }
                | settlement::BlockInspectionError::SystemSenderCount { .. }
                | settlement::BlockInspectionError::StatusCount { .. } => (
                    tonic::Code::Internal,
                    "validation backend returned invalid output",
                ),
                settlement::BlockInspectionError::IntermediateSystemTransaction { .. }
                | settlement::BlockInspectionError::IntermediateOutboundEvent { .. }
                | settlement::BlockInspectionError::RevertedSystemTransaction { .. }
                | settlement::BlockInspectionError::ReservedSystemSender { .. } => (
                    tonic::Code::FailedPrecondition,
                    "settlement validation rejected",
                ),
            },
            Self::EffectPrefix(error) => match error {
                settlement::EffectPrefixError::TransactionStateCheckpointCountMismatch {
                    ..
                }
                | settlement::EffectPrefixError::TransactionStateCheckpointIndexMismatch {
                    ..
                } => (
                    tonic::Code::Internal,
                    "validation backend returned invalid output",
                ),
                settlement::EffectPrefixError::LeadingEntryNotAnchor { .. }
                | settlement::EffectPrefixError::LaterAnchor { .. }
                | settlement::EffectPrefixError::InvalidEntry { .. }
                | settlement::EffectPrefixError::EffectCountMismatch { .. }
                | settlement::EffectPrefixError::AnchorRootMismatch { .. }
                | settlement::EffectPrefixError::EffectKindMismatch { .. }
                | settlement::EffectPrefixError::EffectStateRootMismatch { .. }
                | settlement::EffectPrefixError::NonZeroAnchorEtherDelta { .. } => (
                    tonic::Code::FailedPrecondition,
                    "settlement validation rejected",
                ),
            },
            Self::PublicInputs(settlement::PublicInputError::InvalidStructure(_))
            | Self::StateUpdateChain(_)
            | Self::InboundEffects(_)
            | Self::OutboundEffects(_) => (
                tonic::Code::FailedPrecondition,
                "settlement validation rejected",
            ),
            Self::DaPayload(error) => match error {
                settlement::DaPayloadError::Decode { .. }
                | settlement::DaPayloadError::TrailingBytes { .. } => {
                    (tonic::Code::InvalidArgument, "invalid batch callData")
                }
                settlement::DaPayloadError::InvalidBlockRlp { .. }
                | settlement::DaPayloadError::SystemTransactionReconstruction { .. }
                | settlement::DaPayloadError::InvalidEffectTransactionLayout => (
                    tonic::Code::Internal,
                    "validation backend returned invalid output",
                ),
                settlement::DaPayloadError::BlockCount { .. }
                | settlement::DaPayloadError::UnexpectedItems { .. }
                | settlement::DaPayloadError::ProjectedTransactionCount { .. }
                | settlement::DaPayloadError::TransactionMismatch { .. }
                | settlement::DaPayloadError::MissingL2Entry { .. }
                | settlement::DaPayloadError::L2EntryMismatch { .. }
                | settlement::DaPayloadError::SyncBlockTransactionCount { .. }
                | settlement::DaPayloadError::SyncBlockTransactionMismatch { .. } => (
                    tonic::Code::FailedPrecondition,
                    "settlement validation rejected",
                ),
            },
            Self::Cancelled => (tonic::Code::Cancelled, "Prove request cancelled"),
        };
        Status::new(code, message)
    }

    /// Attach a machine-readable eviction hint without changing the public
    /// status code, message, or settlement gate.
    fn with_actionable(self, failure: Option<ProveFailure>) -> Self {
        match failure {
            Some(failure) => Self::Actionable {
                source: Box::new(self),
                failure,
            },
            None => self,
        }
    }
}

/// Run the synchronous settlement gates, stopping at the first failure.
///
/// Cancellation is checked before decoding and before `verify_da_payload`;
/// each settlement gate is synchronous and non-interruptible.
pub(super) fn run_settlement(
    input: SettlementInput<'_>,
) -> Result<AttestablePublicInputsHash, SettlementPipelineError> {
    let SettlementInput {
        submitted_post_batch_calldata,
        validated_window,
        expected_rollup_id,
        expected_l2_system_address,
        proof_system_vkey,
        expected_proof_system,
        system_transaction_reconstructor,
        cancellation,
    } = input;
    ensure_settlement_active(cancellation)?;
    let settling_block = validated_window.settling_block();
    let canonical_batch = settlement::decode_canonical_post_batch(submitted_post_batch_calldata)?;
    let checked_public_input_profile = settlement::CheckedPublicInputProfile::new(
        &canonical_batch,
        expected_rollup_id,
        expected_proof_system,
    )?;

    let verified_state_chain = settlement::verify_state_update_chain(
        &canonical_batch,
        expected_rollup_id,
        validated_window.window_pre_state_root(),
        validated_window.window_post_state_root(),
    )?;

    // If an outbound load reverted, report its paired user transaction.
    // Reverted inbound deliveries are handled by the inbound gate below.
    let settling_observations = settlement::inspect_validated_settling_block(
        settling_block.block(),
        settling_block.receipt_successes(),
        expected_rollup_id,
    )
    .map_err(|error| {
        let failure = actionable_block_inspection_failure(&error, settling_block.block());
        SettlementPipelineError::BlockInspection(error).with_actionable(failure)
    })?;
    settlement::verify_validated_intermediate_blocks(validated_window.preceding_blocks())?;

    let bound_effects = settlement::bind_effects_to_execution(
        &verified_state_chain,
        validated_window.settling_pre_state_root(),
        settling_block.transaction_state_checkpoints(),
        &settling_observations,
    )?;

    // Direction-specific gates turn the ordered bindings into the only effect
    // capabilities accepted by exact DA reconstruction.
    let authorized_inbound_effects =
        settlement::authorize_inbound_effects(&bound_effects, expected_rollup_id).map_err(
            |error| {
                let failure = actionable_inbound_failure(&error, &canonical_batch);
                SettlementPipelineError::InboundEffects(error).with_actionable(failure)
            },
        )?;
    let authorized_outbound_effects = settlement::authorize_outbound_effects(
        &bound_effects,
        expected_rollup_id,
        expected_l2_system_address,
    )
    .map_err(|error| {
        let failure = actionable_outbound_failure(&error, settling_block.block());
        SettlementPipelineError::OutboundEffects(error).with_actionable(failure)
    })?;

    ensure_settlement_active(cancellation)?;

    // Bind posted transactions and effect sidecars to the validated block bytes,
    // including the reconstructed Sync-block transaction sequence.
    settlement::verify_da_payload(
        &canonical_batch,
        validated_window,
        &authorized_outbound_effects,
        &authorized_inbound_effects,
        system_transaction_reconstructor,
    )?;

    let recomputed_public_inputs_hash =
        checked_public_input_profile.recompute_public_inputs_hash(proof_system_vkey)?;
    Ok(AttestablePublicInputsHash(recomputed_public_inputs_hash))
}

fn actionable_block_inspection_failure(
    error: &settlement::BlockInspectionError,
    block: &validate::ValidatedBlock,
) -> Option<ProveFailure> {
    let settlement::BlockInspectionError::RevertedSystemTransaction { index } = error else {
        return None;
    };
    let (transaction_index, transaction_hash) =
        settlement::paired_outbound_transaction(block, *index)?;
    outbound_failure(transaction_index, transaction_hash)
}

fn actionable_outbound_failure(
    error: &settlement::OutboundEffectError,
    block: &validate::ValidatedBlock,
) -> Option<ProveFailure> {
    let transaction_index = error.poisoned_transaction_index()?;
    let transaction_hash = settlement::transaction_hash_at(block, transaction_index)?;
    outbound_failure(transaction_index, transaction_hash)
}

fn outbound_failure(transaction_index: usize, transaction_hash: B256) -> Option<ProveFailure> {
    Some(ProveFailure {
        actionable_failure: Some(prove_failure::ActionableFailure::Outbound(
            OutboundFailure {
                transaction_index: transaction_index.try_into().ok()?,
                transaction_hash: transaction_hash.as_slice().to_vec(),
            },
        )),
    })
}

fn actionable_inbound_failure(
    error: &settlement::InboundEffectError,
    batch: &settlement::CanonicalPostBatch,
) -> Option<ProveFailure> {
    let entry_index = error.poisoned_entry_index()?;
    let entry_hash = batch.entry_hash_at(entry_index)?;
    Some(ProveFailure {
        actionable_failure: Some(prove_failure::ActionableFailure::Inbound(InboundFailure {
            entry_index: entry_index.try_into().ok()?,
            entry_hash: entry_hash.as_slice().to_vec(),
        })),
    })
}

/// Stop settlement work after its request has been cancelled.
fn ensure_settlement_active(
    cancellation: &CancellationToken,
) -> Result<(), SettlementPipelineError> {
    if cancellation.is_cancelled() {
        Err(SettlementPipelineError::Cancelled)
    } else {
        Ok(())
    }
}
