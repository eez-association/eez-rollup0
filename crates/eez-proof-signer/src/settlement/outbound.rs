//! Outbound event binding and settlement-effect authorization.

use std::num::NonZeroU64;

use alloy_primitives::{Address, B256, I256, U256};
use eez_protocol::abi::ExecutionEntrySol;
use eez_protocol::rolling_hash::EntryRollingHash;
use eez_protocol::{
    CallHashInput, CallMode, RollupId, common_cross_chain_call_hash, l2_outbound_call_hash,
};
use thiserror::Error;

use super::effect_binding::{BoundEffect, BoundEffectSequence, EffectKind};
use crate::validate::OutboundEventObservation;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum OutboundEffectError {
    #[error(
        "outbound effect entry {entry_index} at transaction {transaction_index} is not immediately preceded by a system transaction"
    )]
    MissingPrecedingSystemTransaction {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error(
        "outbound effect entry {entry_index} at transaction {transaction_index} follows an inbound effect"
    )]
    NonCanonicalEffectOrder {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error(
        "outbound effect entry {entry_index} at transaction {transaction_index} has no matching event"
    )]
    MissingObservation {
        entry_index: usize,
        transaction_index: usize,
    },
    #[error(
        "outbound event at transaction {transaction_index}, receipt log {receipt_log_index}, has no matching effect"
    )]
    UnexpectedObservation {
        transaction_index: usize,
        receipt_log_index: usize,
    },
    #[error(
        "outbound event at transaction {transaction_index}, receipt log {receipt_log_index}, is malformed"
    )]
    MalformedObservation {
        transaction_index: usize,
        receipt_log_index: usize,
    },
    #[error(
        "outbound event at transaction {transaction_index}, receipt log {receipt_log_index}, uses callGas {actual}; expected 0"
    )]
    UnsupportedCallGas {
        transaction_index: usize,
        receipt_log_index: usize,
        actual: u64,
    },
    #[error("outbound entry {entry_index} has {actual} L2-to-L1 calls; expected exactly one")]
    L2ToL1CallCount { entry_index: usize, actual: usize },
    #[error("outbound entry {entry_index} targets rollup {actual}; expected rollup {expected}")]
    DestinationRollupMismatch {
        entry_index: usize,
        expected: u64,
        actual: u64,
    },
    #[error(
        "outbound entry {entry_index} call has source rollup {actual}; expected rollup {expected}"
    )]
    SourceRollupMismatch {
        entry_index: usize,
        expected: u64,
        actual: u64,
    },
    #[error(
        "outbound entry {entry_index} recomputes call hash {recomputed}; transaction {transaction_index} emitted {observed}"
    )]
    CallHashMismatch {
        entry_index: usize,
        transaction_index: usize,
        recomputed: B256,
        observed: B256,
    },
    #[error("outbound entry {entry_index} has a non-canonical {field}")]
    InvalidEntryShape {
        entry_index: usize,
        field: &'static str,
    },
    #[error("outbound entry {entry_index} claims rolling hash {claimed}; recomputed {recomputed}")]
    RollingHashMismatch {
        entry_index: usize,
        recomputed: B256,
        claimed: B256,
    },
    #[error("outbound entry {entry_index} uses the reserved system account as its source")]
    ReservedSourceAddress { entry_index: usize },
    #[error("outbound entry {entry_index} value {value} exceeds the non-negative int256 range")]
    ValueOutOfRange { entry_index: usize, value: U256 },
    #[error(
        "outbound entry {entry_index} has ether delta {actual}; expected released value delta {expected}"
    )]
    EtherDeltaMismatch {
        entry_index: usize,
        expected: I256,
        actual: I256,
    },
}

/// Outbound effects bound to positioned EEZL2 events and state-update-free
/// sidecar projections.
///
/// `verify_da_payload` binds each projection to its canonical `[load, user]`
/// transaction pair.
/// Only [`authorize_outbound_effects`] can construct this in production.
#[cfg_attr(test, derive(Default))]
pub(crate) struct AuthorizedOutboundEffects {
    bindings: Vec<AuthorizedOutboundEffect>,
}

/// One authorized effect: the preceding system-load position, the user
/// transaction position, and the unfinished source entry carried in DA.
pub(super) struct AuthorizedOutboundEffect {
    load_transaction_index: usize,
    transaction_index: usize,
    derived_da_entry: ExecutionEntrySol,
}

impl AuthorizedOutboundEffects {
    pub(super) fn len(&self) -> usize {
        self.bindings.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &AuthorizedOutboundEffect> {
        self.bindings.iter()
    }
}

impl AuthorizedOutboundEffect {
    pub(super) const fn load_transaction_index(&self) -> usize {
        self.load_transaction_index
    }

    pub(super) const fn transaction_index(&self) -> usize {
        self.transaction_index
    }

    pub(super) const fn derived_da_entry(&self) -> &ExecutionEntrySol {
        &self.derived_da_entry
    }
}

/// Authorize outbound effects from re-executed transaction events and
/// settlement entries.
///
/// Each outbound effect must occupy a user transaction immediately after a
/// system load. The function enforces outbound-before-inbound ordering, one
/// matching event and call, matching rollup IDs and call hash, a successful
/// single-call rolling hash, a non-system source, and an ether delta of
/// `-value`. Stored entries omit state updates and their unfinished L1 rolling
/// hash for DA reconstruction.
pub(crate) fn authorize_outbound_effects(
    bound_effects: &BoundEffectSequence<'_, '_>,
    expected_rollup_id: NonZeroU64,
    expected_l2_system_address: Address,
) -> Result<AuthorizedOutboundEffects, OutboundEffectError> {
    let mut observations = bound_effects
        .settling_observations()
        .outbound_event_candidates()
        .iter()
        .peekable();
    let mut authorized_bindings = Vec::with_capacity(bound_effects.outbound_count());
    let mut saw_inbound = false;

    // Effects and observations both ascend by transaction index, so one forward
    // merge suffices; any event no outbound effect claims fails closed.
    for effect in bound_effects.effects() {
        if let Some(observation) = observations.peek()
            && observation.transaction_index() < effect.transaction_index()
        {
            return Err(unexpected_outbound_observation(observation));
        }

        match effect.kind() {
            EffectKind::Inbound => {
                saw_inbound = true;
                if let Some(observation) = observations.peek()
                    && observation.transaction_index() == effect.transaction_index()
                {
                    return Err(unexpected_outbound_observation(observation));
                }
            }
            EffectKind::Outbound => {
                if saw_inbound {
                    return Err(OutboundEffectError::NonCanonicalEffectOrder {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                    });
                }
                let Some(load_transaction_index) = effect.transaction_index().checked_sub(1) else {
                    return Err(OutboundEffectError::MissingPrecedingSystemTransaction {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                    });
                };
                if bound_effects
                    .settling_observations()
                    .system_sender_flags()
                    .get(load_transaction_index)
                    .copied()
                    != Some(true)
                {
                    return Err(OutboundEffectError::MissingPrecedingSystemTransaction {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                    });
                }
                let observation = observations
                    .next_if(|observation| {
                        observation.transaction_index() == effect.transaction_index()
                    })
                    .ok_or(OutboundEffectError::MissingObservation {
                        entry_index: effect.entry_index(),
                        transaction_index: effect.transaction_index(),
                    })?;
                if let Some(next) = observations.peek()
                    && next.transaction_index() == effect.transaction_index()
                {
                    return Err(unexpected_outbound_observation(next));
                }
                authorize_outbound_effect(
                    effect,
                    observation,
                    expected_rollup_id,
                    expected_l2_system_address,
                )?;
                let mut derived_da_entry = effect.claimed_entry().clone();
                derived_da_entry.stateUpdates.clear();
                derived_da_entry.rollingHash = B256::ZERO;
                authorized_bindings.push(AuthorizedOutboundEffect {
                    load_transaction_index,
                    transaction_index: effect.transaction_index(),
                    derived_da_entry,
                });
            }
        }
    }

    if let Some(observation) = observations.next() {
        return Err(unexpected_outbound_observation(observation));
    }
    Ok(AuthorizedOutboundEffects {
        bindings: authorized_bindings,
    })
}

fn unexpected_outbound_observation(observation: &OutboundEventObservation) -> OutboundEffectError {
    OutboundEffectError::UnexpectedObservation {
        transaction_index: observation.transaction_index(),
        receipt_log_index: observation.receipt_log_index(),
    }
}

fn authorize_outbound_effect(
    effect: &BoundEffect<'_>,
    observation: &OutboundEventObservation,
    expected_rollup_id: NonZeroU64,
    expected_l2_system_address: Address,
) -> Result<(), OutboundEffectError> {
    let entry_index = effect.entry_index();
    let entry = effect.claimed_entry();
    let [call] = entry.l2ToL1Calls.as_slice() else {
        return Err(OutboundEffectError::L2ToL1CallCount {
            entry_index,
            actual: entry.l2ToL1Calls.len(),
        });
    };
    if entry.destinationRollupId != expected_rollup_id.get() {
        return Err(OutboundEffectError::DestinationRollupMismatch {
            entry_index,
            expected: expected_rollup_id.get(),
            actual: entry.destinationRollupId,
        });
    }
    if call.sourceRollupId != expected_rollup_id.get() {
        return Err(OutboundEffectError::SourceRollupMismatch {
            entry_index,
            expected: expected_rollup_id.get(),
            actual: call.sourceRollupId,
        });
    }
    for (valid, field) in [
        (entry.proxyEntryHash == B256::ZERO, "proxyEntryHash"),
        (entry.expectedL1ToL2Calls.is_empty(), "expectedL1ToL2Calls"),
        (entry.success, "success"),
        (call.revertNextNCalls == 0, "revertNextNCalls"),
        (!call.isStatic, "isStatic"),
        (call.gas == 0, "gas"),
    ] {
        if !valid {
            return Err(OutboundEffectError::InvalidEntryShape { entry_index, field });
        }
    }

    let decoded_event =
        observation
            .decoded_event()
            .ok_or(OutboundEffectError::MalformedObservation {
                transaction_index: observation.transaction_index(),
                receipt_log_index: observation.receipt_log_index(),
            })?;
    if decoded_event.call_gas() != 0 {
        return Err(OutboundEffectError::UnsupportedCallGas {
            transaction_index: observation.transaction_index(),
            receipt_log_index: observation.receipt_log_index(),
            actual: decoded_event.call_gas(),
        });
    }
    let observed_call_hash = decoded_event.call_hash();
    let recomputed_call_hash = l2_outbound_call_hash(
        CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: call.sourceAddress,
            source_rollup_id: RollupId(expected_rollup_id.get()),
            target_address: call.targetAddress,
            target_rollup_id: RollupId::MAINNET,
            value: call.value,
            data: &call.data,
        },
        decoded_event.call_gas(),
    );
    if observed_call_hash != recomputed_call_hash {
        return Err(OutboundEffectError::CallHashMismatch {
            entry_index,
            transaction_index: observation.transaction_index(),
            recomputed: recomputed_call_hash,
            observed: observed_call_hash,
        });
    }

    // L1 uses the common gas-free call identity in the rolling hash. This is
    // deliberately distinct from the gas-aware L2 event hash checked above.
    let l1_call_hash = common_cross_chain_call_hash(CallHashInput {
        call_mode: CallMode::Mutable,
        source_address: call.sourceAddress,
        source_rollup_id: RollupId(call.sourceRollupId),
        target_address: call.targetAddress,
        target_rollup_id: RollupId::MAINNET,
        value: call.value,
        data: &call.data,
    });
    let update = effect.claimed_state_update();
    let mut rolling_hash = EntryRollingHash::for_l1(
        [(update.rollupId, update.currentState)],
        entry.proxyEntryHash,
    );
    rolling_hash.call_begin(l1_call_hash);
    rolling_hash.call_end(entry.success, &entry.returnData);
    let recomputed_rolling_hash = rolling_hash.current();
    if entry.rollingHash != recomputed_rolling_hash {
        return Err(OutboundEffectError::RollingHashMismatch {
            entry_index,
            recomputed: recomputed_rolling_hash,
            claimed: entry.rollingHash,
        });
    }

    if call.sourceAddress == expected_l2_system_address {
        return Err(OutboundEffectError::ReservedSourceAddress { entry_index });
    }
    // Available L1 funding is outside this claim; bind the ledger delta to
    // `-value`.
    let expected_delta =
        -I256::try_from(call.value).map_err(|_| OutboundEffectError::ValueOutOfRange {
            entry_index,
            value: call.value,
        })?;
    if update.etherDelta != expected_delta {
        return Err(OutboundEffectError::EtherDeltaMismatch {
            entry_index,
            expected: expected_delta,
            actual: update.etherDelta,
        });
    }
    Ok(())
}
