//! Inbound transaction inspection and settlement-effect authorization.

use std::fmt;
use std::num::NonZeroU64;

use alloy_primitives::{B256, Bytes, I256, U256};
use alloy_sol_types::{SolCall as _, SolValue as _};
use eez_protocol::abi::{ExecutionEntrySol, L2ToL1CallSol, executeIncomingCrossChainCallCall};
use eez_protocol::rolling_hash::EntryRollingHash;
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
    SourceRollup { actual: u64 },
    #[error("inbound calldata carries {actual} execution entries; expected exactly one")]
    EntryCount { actual: usize },
    #[error("inbound calldata carries {actual} static entries; expected none")]
    StaticEntryCount { actual: usize },
    #[error("inbound execution entry has invalid {field}")]
    InvalidEntryShape { field: &'static str },
    #[error("outer and inner inbound {field} differ")]
    OuterInnerMismatch { field: &'static str },
    #[error("inbound claimed call hash is {claimed}; recomputed {recomputed}")]
    CallHashMismatch { recomputed: B256, claimed: B256 },
    #[error("inbound call hash is the reserved zero value")]
    ZeroCallHash,
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
    if call.sourceRollup != RollupId::MAINNET.0 {
        return Err(InboundObservationError::SourceRollup {
            actual: call.sourceRollup,
        });
    }
    let [entry] = call._entries.as_slice() else {
        return Err(InboundObservationError::EntryCount {
            actual: call._entries.len(),
        });
    };
    if !call._staticEntries.is_empty() {
        return Err(InboundObservationError::StaticEntryCount {
            actual: call._staticEntries.len(),
        });
    }
    let [inner] = entry.incomingCalls.as_slice() else {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "incomingCalls",
        });
    };
    if !entry.success {
        return Err(InboundObservationError::InvalidEntryShape { field: "success" });
    }
    if !entry.expectedOutgoingCalls.is_empty() {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "expectedOutgoingCalls",
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
    for (valid, field) in [
        (inner.revertNextNCalls == 0, "revertNextNCalls"),
        (!inner.isStatic, "isStatic"),
        (inner.gas == 0, "gas"),
    ] {
        if !valid {
            return Err(InboundObservationError::InvalidEntryShape { field });
        }
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

    let mut l2_rolling_hash = EntryRollingHash::for_l2(entry.proxyEntryHash);
    l2_rolling_hash.call_begin(entry.proxyEntryHash);
    l2_rolling_hash.call_end(entry.success, &entry.returnData);
    if entry.rollingHash != l2_rolling_hash.current() {
        return Err(InboundObservationError::InvalidEntryShape {
            field: "rollingHash",
        });
    }
    let derived_da_entry = ExecutionEntrySol {
        stateUpdates: Vec::new(),
        proxyEntryHash: recomputed_call_hash,
        l2ToL1Calls: vec![L2ToL1CallSol {
            revertNextNCalls: inner.revertNextNCalls,
            isStatic: inner.isStatic,
            gas: inner.gas,
            sourceAddress: inner.sourceAddress,
            sourceRollupId: inner.sourceRollupId,
            targetAddress: inner.targetAddress,
            value: inner.value,
            data: inner.data.clone(),
        }],
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: entry.rollingHash,
        destinationRollupId: expected_rollup_id.get(),
        success: entry.success,
        returnData: entry.returnData.clone(),
    };
    Ok(InboundObservation {
        recomputed_call_hash,
        value: call.value,
        return_data: entry.returnData.clone(),
        derived_da_entry: DerivedInboundDaEntry(derived_da_entry),
    })
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum InboundEffectError {
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
        actual: u64,
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
/// entry shape, rollup and call identity, return data, rolling hash, and
/// deposited ether delta.
pub(crate) fn authorize_inbound_effects<'settling>(
    bound_effects: &BoundEffectSequence<'_, 'settling>,
    expected_rollup_id: NonZeroU64,
) -> Result<AuthorizedInboundEffects<'settling>, InboundEffectError> {
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
    if entry.destinationRollupId != expected_rollup_id.get() {
        return Err(InboundEffectError::DestinationRollupMismatch {
            entry_index,
            expected: expected_rollup_id.get(),
            actual: entry.destinationRollupId,
        });
    }
    for (valid, field) in [
        (entry.l2ToL1Calls.is_empty(), "l2ToL1Calls"),
        (entry.expectedL1ToL2Calls.is_empty(), "expectedL1ToL2Calls"),
        (entry.success, "success"),
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
    let update = effect.claimed_state_update();
    let expected_rolling_hash = EntryRollingHash::for_l1(
        [(update.rollupId, update.currentState)],
        entry.proxyEntryHash,
    )
    .current();
    if entry.rollingHash != expected_rolling_hash {
        return Err(InboundEffectError::InvalidEntryShape {
            entry_index,
            field: "rollingHash",
        });
    }
    let expected =
        I256::try_from(observation.value).map_err(|_| InboundEffectError::ValueOutOfRange {
            entry_index,
            transaction_index: effect.transaction_index(),
            value: observation.value,
        })?;
    if update.etherDelta != expected {
        return Err(InboundEffectError::EtherDeltaMismatch {
            entry_index,
            expected,
            actual: update.etherDelta,
        });
    }
    Ok(())
}
