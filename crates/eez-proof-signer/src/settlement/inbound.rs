//! Inbound transaction inspection and settlement-effect authorization.

use std::fmt;
use std::num::NonZeroU64;

use alloy_primitives::{B256, Bytes, I256, U256};
use alloy_sol_types::{SolCall as _, SolValue as _};
use eez_protocol::abi::{ExecutionEntrySol, L2ToL1CallSol, executeIncomingCrossChainCallCall};
use eez_protocol::entries::decode_inbound;
use eez_protocol::{CallHashInput, CallMode, RollupId, common_cross_chain_call_hash};
use thiserror::Error;

use super::effect_binding::{BoundEffect, BoundEffectSequence, EffectKind};

/// Strict semantic evidence extracted from one top-level inbound system tx.
/// `derived_da_entry` is the canonical sidecar entry the DA payload must carry
/// for this delivery, derived from the calldata that was actually re-executed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InboundObservation {
    pub(crate) recomputed_call_hash: B256,
    pub(crate) value: U256,
    pub(crate) return_data: Bytes,
    /// Inner-call outcome committed by the calldata rolling hash; this is
    /// distinct from the validated top-level receipt status.
    pub(crate) rolling_hash_committed_success: bool,
    pub(super) derived_da_entry: DerivedInboundDaEntry,
}

/// Sidecar wrapper whose diagnostics and equality use canonical ABI bytes.
pub(super) struct DerivedInboundDaEntry(ExecutionEntrySol);

impl DerivedInboundDaEntry {
    /// Borrow the typed entry for canonical Sync reconstruction.
    pub(super) fn as_entry(&self) -> &ExecutionEntrySol {
        &self.0
    }

    /// Encode the exact wire identity expected in `l2Entries`.
    pub(super) fn encoded(&self) -> Vec<u8> {
        self.0.abi_encode()
    }
}

impl fmt::Debug for DerivedInboundDaEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DerivedInboundDaEntry")
            .field(&self.encoded())
            .finish()
    }
}

impl PartialEq for DerivedInboundDaEntry {
    fn eq(&self, other: &Self) -> bool {
        self.encoded() == other.encoded()
    }
}

impl Eq for DerivedInboundDaEntry {}

/// One selector-level inbound candidate and the result of checking its call
/// envelope and execution-table shape. Byte-exact transaction reconstruction
/// remains a separate DA requirement.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InboundCandidate {
    pub(crate) transaction_index: usize,
    pub(crate) inspection: Result<InboundObservation, InboundObservationError>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub(crate) enum InboundObservationError {
    #[error("top-level system transaction reverted")]
    RevertedTransaction,
    #[error("inbound calldata does not decode: {reason}")]
    InvalidAbi { reason: String },
    #[error("inbound calldata is not its canonical complete ABI encoding")]
    NonCanonicalAbi,
    #[error("native transaction value is {actual}; outer inbound value is {expected}")]
    NativeValueMismatch { expected: U256, actual: U256 },
    #[error("inbound source rollup is {actual}; expected L1 rollup 0")]
    SourceRollup { actual: U256 },
    #[error("inbound calldata carries {actual} execution entries; expected exactly one")]
    EntryCount { actual: usize },
    #[error("inbound calldata carries {actual} lookup calls; expected none")]
    LookupCount { actual: usize },
    #[error("inbound execution entry has invalid {field}")]
    InvalidEntryShape { field: &'static str },
    #[error("outer and inner inbound {field} differ")]
    OuterInnerMismatch { field: &'static str },
    #[error("inbound claimed call hash is {claimed}; recomputed {recomputed}")]
    CallHashMismatch { recomputed: B256, claimed: B256 },
    #[error("inbound call hash is the reserved zero value")]
    ZeroCallHash,
    #[error("inbound rolling hash matches neither success outcome")]
    InvalidOutcome,
}

/// Validate the envelope, canonical calldata, entry shape, call hash, and
/// rolling-hash-committed outcome of one inbound candidate.
///
/// `authorize_inbound_effects` and `verify_da_payload` separately bind the
/// observation to its settlement entry and DA sidecar.
pub(super) fn inspect_inbound_candidate(
    transaction_value: U256,
    calldata: &[u8],
    transaction_succeeded: bool,
    expected_rollup_id: NonZeroU64,
) -> Result<InboundObservation, InboundObservationError> {
    if !transaction_succeeded {
        return Err(InboundObservationError::RevertedTransaction);
    }

    let call = executeIncomingCrossChainCallCall::abi_decode(calldata).map_err(|error| {
        InboundObservationError::InvalidAbi {
            reason: error.to_string(),
        }
    })?;
    if call.abi_encode().as_slice() != calldata {
        return Err(InboundObservationError::NonCanonicalAbi);
    }
    if transaction_value != call.value {
        return Err(InboundObservationError::NativeValueMismatch {
            expected: call.value,
            actual: transaction_value,
        });
    }
    if call.sourceRollup != U256::ZERO {
        return Err(InboundObservationError::SourceRollup {
            actual: call.sourceRollup,
        });
    }
    let [entry] = call.entries.as_slice() else {
        return Err(InboundObservationError::EntryCount {
            actual: call.entries.len(),
        });
    };
    if !call.lookupCalls.is_empty() {
        return Err(InboundObservationError::LookupCount {
            actual: call.lookupCalls.len(),
        });
    }
    let [inner] = entry.incomingCalls.as_slice() else {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "incomingCalls",
        });
    };
    if entry.callCount != U256::from(1) {
        return Err(InboundObservationError::InvalidEntryShape { field: "callCount" });
    }
    if !entry.expectedOutgoingCalls.is_empty() {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "expectedOutgoingCalls",
        });
    }
    if !entry.expectedLookups.is_empty() {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "expectedLookups",
        });
    }

    for (matches, field) in [
        (call.destination == inner.targetAddress, "destination"),
        (call.value == inner.value, "value"),
        (call.data == inner.data, "data"),
        (call.sourceAddress == inner.sourceAddress, "sourceAddress"),
        (call.sourceRollup == inner.sourceRollupId, "sourceRollup"),
    ] {
        if !matches {
            return Err(InboundObservationError::OuterInnerMismatch { field });
        }
    }
    if inner.revertSpan != U256::ZERO {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "revertSpan",
        });
    }

    let recomputed_call_hash = common_cross_chain_call_hash(
        CallMode::Mutable,
        CallHashInput {
            source_address: call.sourceAddress,
            source_rollup_id: RollupId::MAINNET,
            target_address: call.destination,
            target_rollup_id: RollupId(expected_rollup_id.get()),
            value: call.value,
            data: &call.data,
        },
    );
    if recomputed_call_hash == B256::ZERO {
        return Err(InboundObservationError::ZeroCallHash);
    }
    if entry.proxyEntryHash != recomputed_call_hash {
        return Err(InboundObservationError::CallHashMismatch {
            recomputed: recomputed_call_hash,
            claimed: entry.proxyEntryHash,
        });
    }

    // `decode_inbound` is only an outcome recognizer; the envelope and
    // single-entry shape are validated before its result is accepted.
    let decoded_outcome =
        decode_inbound(calldata).ok_or(InboundObservationError::InvalidOutcome)?;
    let derived_da_entry = ExecutionEntrySol {
        stateDeltas: Vec::new(),
        proxyEntryHash: recomputed_call_hash,
        destinationRollupId: U256::from(expected_rollup_id.get()),
        l2ToL1Calls: vec![L2ToL1CallSol {
            targetAddress: inner.targetAddress,
            value: inner.value,
            data: inner.data.clone(),
            sourceAddress: inner.sourceAddress,
            sourceRollupId: inner.sourceRollupId,
            revertSpan: inner.revertSpan,
        }],
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: entry.callCount,
        returnData: entry.returnData.clone(),
        rollingHash: entry.rollingHash,
    };
    Ok(InboundObservation {
        recomputed_call_hash,
        value: call.value,
        return_data: decoded_outcome.return_data,
        rolling_hash_committed_success: decoded_outcome.success,
        derived_da_entry: DerivedInboundDaEntry(derived_da_entry),
    })
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum InboundEffectError {
    #[error(
        "batch carries {actual} L1-to-L2 lookup calls; successful-inbound profile requires none"
    )]
    LookupCalls { actual: usize },
    #[error(
        "inbound effect entry {entry_index} at transaction {transaction_index} has no matching candidate"
    )]
    MissingCandidate {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error("inbound candidate at transaction {transaction_index} has no matching effect")]
    UnexpectedCandidate { transaction_index: usize },
    #[error(
        "inbound candidate for entry {entry_index} at transaction {transaction_index} is invalid: {source}"
    )]
    InvalidObservation {
        entry_index: usize,
        transaction_index: usize,
        source: InboundObservationError,
    },
    #[error(
        "inbound call for entry {entry_index} at transaction {transaction_index} commits to an inner-call failure; only success is supported"
    )]
    FailedCall {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error("deferred inbound entry {entry_index} has invalid {field}")]
    InvalidEntryShape {
        entry_index: usize,
        field: &'static str,
    },
    #[error(
        "deferred inbound entry {entry_index} targets rollup {actual}; expected rollup {expected}"
    )]
    DestinationRollupMismatch {
        entry_index: usize,
        expected: u64,
        actual: U256,
    },
    #[error(
        "deferred inbound entry {entry_index} claims call hash {claimed}; transaction recomputed {recomputed}"
    )]
    CallHashMismatch {
        entry_index: usize,
        recomputed: B256,
        claimed: B256,
    },
    #[error(
        "deferred inbound entry {entry_index} return data differs from transaction observation"
    )]
    ReturnDataMismatch { entry_index: usize },
    #[error(
        "inbound transaction {transaction_index} for entry {entry_index} has value {value}, which exceeds the int256 range"
    )]
    ValueOutOfRange {
        entry_index: usize,
        transaction_index: usize,
        value: U256,
    },
    #[error(
        "deferred inbound entry {entry_index} ether delta is {actual}; expected deposited value {expected}"
    )]
    EtherDeltaMismatch {
        entry_index: usize,
        expected: I256,
        actual: I256,
    },
}

/// Successful inbound effects whose transaction, settlement entry, value, and
/// canonical DA projection have all been bound positionally.
/// Only [`authorize_inbound_effects`] can construct this in production.
#[cfg_attr(test, derive(Default))]
pub(crate) struct AuthorizedInboundEffects<'settling> {
    bindings: Vec<AuthorizedInboundEffect<'settling>>,
}

pub(super) struct AuthorizedInboundEffect<'settling> {
    transaction_index: usize,
    observation: &'settling InboundObservation,
}

impl<'settling> AuthorizedInboundEffects<'settling> {
    pub(super) fn len(&self) -> usize {
        self.bindings.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &AuthorizedInboundEffect<'settling>> {
        self.bindings.iter()
    }
}

impl AuthorizedInboundEffect<'_> {
    pub(super) const fn transaction_index(&self) -> usize {
        self.transaction_index
    }

    pub(super) const fn observation(&self) -> &InboundObservation {
        self.observation
    }
}

/// Bind each successful inbound candidate to the deferred settlement entry at
/// the same effect position.
///
/// Rejects missing, extra, invalid, failed, or reordered candidates and checks
/// lookup absence, entry shape, rollup and call identity, return data, and
/// deposited ether delta.
pub(crate) fn authorize_inbound_effects<'settling>(
    bound_effects: &BoundEffectSequence<'_, 'settling>,
    expected_rollup_id: NonZeroU64,
) -> Result<AuthorizedInboundEffects<'settling>, InboundEffectError> {
    if !bound_effects.submitted_lookup_calls().is_empty() {
        return Err(InboundEffectError::LookupCalls {
            actual: bound_effects.submitted_lookup_calls().len(),
        });
    }
    let settling_observations = bound_effects.settling_observations();
    let mut candidates = settling_observations.inbound_candidates().iter().peekable();
    let mut authorized_bindings = Vec::with_capacity(bound_effects.inbound_count());

    // Effects and candidates both ascend by transaction index, so one forward
    // merge suffices; any candidate no inbound effect claims fails closed.
    for effect in bound_effects.effects() {
        if let Some(candidate) = candidates.peek()
            && candidate.transaction_index < effect.transaction_index()
        {
            return Err(InboundEffectError::UnexpectedCandidate {
                transaction_index: candidate.transaction_index,
            });
        }

        match effect.kind() {
            EffectKind::Outbound => {
                if candidates.peek().is_some_and(|candidate| {
                    candidate.transaction_index == effect.transaction_index()
                }) {
                    return Err(InboundEffectError::UnexpectedCandidate {
                        transaction_index: effect.transaction_index(),
                    });
                }
            }
            EffectKind::Inbound => {
                let candidate = candidates
                    .next_if(|candidate| candidate.transaction_index == effect.transaction_index());
                let candidate = candidate.ok_or(InboundEffectError::MissingCandidate {
                    entry_index: effect.entry_index(),
                    transaction_index: effect.transaction_index(),
                })?;
                let observation = candidate.inspection.as_ref().map_err(|source| {
                    InboundEffectError::InvalidObservation {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                        source: source.clone(),
                    }
                })?;
                if !observation.rolling_hash_committed_success {
                    return Err(InboundEffectError::FailedCall {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                    });
                }
                authorize_inbound_effect(effect, observation, expected_rollup_id)?;
                authorized_bindings.push(AuthorizedInboundEffect {
                    transaction_index: effect.transaction_index(),
                    observation,
                });
            }
        }
    }

    if let Some(candidate) = candidates.next() {
        return Err(InboundEffectError::UnexpectedCandidate {
            transaction_index: candidate.transaction_index,
        });
    }
    Ok(AuthorizedInboundEffects {
        bindings: authorized_bindings,
    })
}

fn authorize_inbound_effect(
    effect: &BoundEffect<'_>,
    observation: &InboundObservation,
    expected_rollup_id: NonZeroU64,
) -> Result<(), InboundEffectError> {
    let entry_index = effect.entry_index();
    let entry = effect.claimed_entry();
    if entry.destinationRollupId != U256::from(expected_rollup_id.get()) {
        return Err(InboundEffectError::DestinationRollupMismatch {
            entry_index,
            expected: expected_rollup_id.get(),
            actual: entry.destinationRollupId,
        });
    }
    for (valid, field) in [
        (entry.l2ToL1Calls.is_empty(), "l2ToL1Calls"),
        (entry.expectedL1ToL2Calls.is_empty(), "expectedL1ToL2Calls"),
        (entry.expectedLookups.is_empty(), "expectedLookups"),
        (entry.callCount.is_zero(), "callCount"),
        (entry.rollingHash == B256::ZERO, "rollingHash"),
    ] {
        if !valid {
            return Err(InboundEffectError::InvalidEntryShape { entry_index, field });
        }
    }
    if entry.proxyEntryHash != observation.recomputed_call_hash {
        return Err(InboundEffectError::CallHashMismatch {
            entry_index,
            recomputed: observation.recomputed_call_hash,
            claimed: entry.proxyEntryHash,
        });
    }
    if entry.returnData != observation.return_data {
        return Err(InboundEffectError::ReturnDataMismatch { entry_index });
    }
    let expected =
        I256::try_from(observation.value).map_err(|_| InboundEffectError::ValueOutOfRange {
            entry_index,
            transaction_index: effect.transaction_index(),
            value: observation.value,
        })?;
    if effect.claimed_state_delta().etherDelta != expected {
        return Err(InboundEffectError::EtherDeltaMismatch {
            entry_index,
            expected,
            actual: effect.claimed_state_delta().etherDelta,
        });
    }
    Ok(())
}
