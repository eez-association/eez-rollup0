//! Binding Composer-claimed effects to locally validated execution evidence.

use alloy_primitives::{B256, I256};
use eez_protocol::abi::{ExecutionEntrySol, StateUpdateSol};
use eez_protocol::rolling_hash::EntryRollingHash;
use thiserror::Error;

use super::blocks::SettlingBlockObservations;
use super::state_chain::VerifiedStateUpdateChain;
use crate::validate::TransactionStateCheckpoint;

/// Shape classification of one claimed batch entry; every unsupported shape
/// is `Invalid` and fails closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimedEntryShape {
    Anchor,
    Outbound,
    Inbound,
    Invalid,
}

/// Effect kind inferred from locally recovered settling-block evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservedEffectKind {
    Outbound,
    Inbound,
}

/// Effect kind after the anchor/invalid shapes have been rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EffectKind {
    Outbound,
    Inbound,
}

impl EffectKind {
    const fn claimed_shape(self) -> ClaimedEntryShape {
        match self {
            Self::Outbound => ClaimedEntryShape::Outbound,
            Self::Inbound => ClaimedEntryShape::Inbound,
        }
    }
}

/// Exact correspondence established while validating the effect prefix.
/// Private fields keep sibling modules from manufacturing a binding without
/// passing through [`bind_effects_to_execution`].
#[derive(Clone, Copy)]
pub(super) struct BoundEffect<'batch> {
    entry_index: usize,
    transaction_index: usize,
    kind: EffectKind,
    claimed_entry: &'batch ExecutionEntrySol,
    claimed_state_update: &'batch StateUpdateSol,
}

impl<'batch> BoundEffect<'batch> {
    pub(super) const fn entry_index(&self) -> usize {
        self.entry_index
    }

    pub(super) const fn transaction_index(&self) -> usize {
        self.transaction_index
    }

    pub(super) const fn kind(&self) -> EffectKind {
        self.kind
    }

    pub(super) const fn claimed_entry(&self) -> &'batch ExecutionEntrySol {
        self.claimed_entry
    }

    pub(super) const fn claimed_state_update(&self) -> &'batch StateUpdateSol {
        self.claimed_state_update
    }
}

/// Ordered correspondence between claimed batch effects and settling-block
/// candidate positions.
/// Construction remains local so this type can serve as validation evidence.
pub(crate) struct BoundEffectSequence<'batch, 'settling> {
    effects: Vec<BoundEffect<'batch>>,
    settling_observations: &'settling SettlingBlockObservations,
}

impl<'batch, 'settling> BoundEffectSequence<'batch, 'settling> {
    pub(super) fn effects(&self) -> &[BoundEffect<'batch>] {
        &self.effects
    }

    pub(super) const fn settling_observations(&self) -> &'settling SettlingBlockObservations {
        self.settling_observations
    }

    pub(crate) fn inbound_count(&self) -> usize {
        self.count(EffectKind::Inbound)
    }

    pub(crate) fn outbound_count(&self) -> usize {
        self.count(EffectKind::Outbound)
    }

    fn count(&self, kind: EffectKind) -> usize {
        self.effects
            .iter()
            .filter(|effect| effect.kind == kind)
            .count()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum EffectPrefixError {
    #[error("leading settlement entry is {actual:?}; expected a canonical anchor")]
    LeadingEntryNotAnchor { actual: ClaimedEntryShape },
    #[error("settlement entry {entry_index} is a second anchor")]
    LaterAnchor { entry_index: usize },
    #[error("settlement entry {entry_index} has an invalid effect shape")]
    InvalidEntry { entry_index: usize },
    #[error(
        "claimed settlement effects number {claimed}, but re-execution observed {observed} effect candidates"
    )]
    EffectCountMismatch { claimed: usize, observed: usize },
    #[error(
        "anchor claims post-state {claimed_anchor_post_state}; validated pre-settling root is {validated_pre_settling_root}"
    )]
    AnchorRootMismatch {
        validated_pre_settling_root: B256,
        claimed_anchor_post_state: B256,
    },
    #[error(
        "settlement effects require {expected} transaction state checkpoints, but received {actual}"
    )]
    TransactionStateCheckpointCountMismatch { expected: usize, actual: usize },
    #[error(
        "transaction state checkpoint {checkpoint_index} targets transaction {actual}; expected effect-candidate transaction {expected}"
    )]
    TransactionStateCheckpointIndexMismatch {
        checkpoint_index: usize,
        expected: usize,
        actual: usize,
    },
    #[error(
        "settlement entry {entry_index} is {claimed:?}, but effect-candidate transaction {transaction_index} is observed as {observed:?}"
    )]
    EffectKindMismatch {
        entry_index: usize,
        transaction_index: usize,
        claimed: ClaimedEntryShape,
        observed: ObservedEffectKind,
    },
    #[error(
        "settlement entry {entry_index} claims post-state {claimed_post_state}; transaction {transaction_index} recomputed {recomputed_checkpoint}"
    )]
    EffectStateRootMismatch {
        entry_index: usize,
        transaction_index: usize,
        recomputed_checkpoint: B256,
        claimed_post_state: B256,
    },
    #[error("anchor ether delta is {claimed}; expected zero")]
    NonZeroAnchorEtherDelta { claimed: I256 },
}

/// Bind each verified state-update-chain entry to its settling-block candidate and
/// transaction state checkpoint.
///
/// [`VerifiedStateUpdateChain`] already guarantees a nonempty batch with exactly
/// one continuous, expected-rollup update per entry. This gate adds effect-shape,
/// candidate, and checkpoint bindings. For a nonempty effect set, the anchor's
/// `newState` must equal the validated pre-settling root.
pub(crate) fn bind_effects_to_execution<'batch, 'settling>(
    verified_state_chain: &VerifiedStateUpdateChain<'batch>,
    validated_settling_pre_state_root: B256,
    computed_transaction_state_checkpoints: &[TransactionStateCheckpoint],
    settling_observations: &'settling SettlingBlockObservations,
) -> Result<BoundEffectSequence<'batch, 'settling>, EffectPrefixError> {
    let (leading, anchor_update) = verified_state_chain.leading();
    let settled_rollup = verified_state_chain.expected_rollup();
    let leading_kind = classify_claimed_entry(leading, anchor_update, settled_rollup);
    if leading_kind != ClaimedEntryShape::Anchor {
        return Err(EffectPrefixError::LeadingEntryNotAnchor {
            actual: leading_kind,
        });
    }

    let mut claimed_effects = Vec::with_capacity(verified_state_chain.trailing_len());
    for (entry_index, entry, update) in verified_state_chain.trailing() {
        match classify_claimed_entry(entry, update, settled_rollup) {
            ClaimedEntryShape::Anchor => {
                return Err(EffectPrefixError::LaterAnchor { entry_index });
            }
            ClaimedEntryShape::Invalid => {
                return Err(EffectPrefixError::InvalidEntry { entry_index });
            }
            ClaimedEntryShape::Outbound => {
                claimed_effects.push((entry_index, EffectKind::Outbound, entry, update));
            }
            ClaimedEntryShape::Inbound => {
                claimed_effects.push((entry_index, EffectKind::Inbound, entry, update));
            }
        }
    }

    let claimed = claimed_effects.len();
    let effect_candidate_positions = settling_observations.effect_candidate_positions();
    let observed = effect_candidate_positions.len();
    if claimed != observed {
        return Err(EffectPrefixError::EffectCountMismatch { claimed, observed });
    }
    // A canonical anchor carries no cross-chain value. Check this only after
    // the leading Composer entry has actually been classified as an anchor.
    if anchor_update.etherDelta != I256::ZERO {
        return Err(EffectPrefixError::NonZeroAnchorEtherDelta {
            claimed: anchor_update.etherDelta,
        });
    }
    // With no effects the batch never represents the pre-settling root; the
    // state-update-chain gate binds the anchor's `newState` to the final root instead.
    if claimed == 0 {
        if !computed_transaction_state_checkpoints.is_empty() {
            return Err(EffectPrefixError::TransactionStateCheckpointCountMismatch {
                expected: 0,
                actual: computed_transaction_state_checkpoints.len(),
            });
        }
        return Ok(BoundEffectSequence {
            effects: Vec::new(),
            settling_observations,
        });
    }

    if anchor_update.newState != validated_settling_pre_state_root {
        return Err(EffectPrefixError::AnchorRootMismatch {
            validated_pre_settling_root: validated_settling_pre_state_root,
            claimed_anchor_post_state: anchor_update.newState,
        });
    }
    if computed_transaction_state_checkpoints.len() != observed {
        return Err(EffectPrefixError::TransactionStateCheckpointCountMismatch {
            expected: observed,
            actual: computed_transaction_state_checkpoints.len(),
        });
    }

    let mut bound_effects = Vec::with_capacity(claimed);
    // Equal counts were established above, so one index names the claim, its
    // observed transaction boundary, and the locally recomputed checkpoint.
    for (effect_index, (entry_index, kind, entry, update)) in
        claimed_effects.into_iter().enumerate()
    {
        let transaction_index = effect_candidate_positions[effect_index];
        let checkpoint = &computed_transaction_state_checkpoints[effect_index];
        let claimed_kind = kind.claimed_shape();
        let observed_kind = if settling_observations.system_sender_flags()[transaction_index] {
            ObservedEffectKind::Inbound
        } else {
            ObservedEffectKind::Outbound
        };
        let observed_bound_kind = match observed_kind {
            ObservedEffectKind::Outbound => EffectKind::Outbound,
            ObservedEffectKind::Inbound => EffectKind::Inbound,
        };
        if kind != observed_bound_kind {
            return Err(EffectPrefixError::EffectKindMismatch {
                entry_index,
                transaction_index,
                claimed: claimed_kind,
                observed: observed_kind,
            });
        }

        if checkpoint.transaction_index != transaction_index {
            return Err(EffectPrefixError::TransactionStateCheckpointIndexMismatch {
                checkpoint_index: effect_index,
                expected: transaction_index,
                actual: checkpoint.transaction_index,
            });
        }
        let recomputed_checkpoint = checkpoint.state_root;
        let claimed_post_state = update.newState;
        if claimed_post_state != recomputed_checkpoint {
            return Err(EffectPrefixError::EffectStateRootMismatch {
                entry_index,
                transaction_index,
                recomputed_checkpoint,
                claimed_post_state,
            });
        }
        bound_effects.push(BoundEffect {
            entry_index,
            transaction_index,
            kind,
            claimed_entry: entry,
            claimed_state_update: update,
        });
    }

    Ok(BoundEffectSequence {
        effects: bound_effects,
        settling_observations,
    })
}

/// Classification reads only the discriminating fields; the per-kind effect
/// gates validate the entries in depth.
fn classify_claimed_entry(
    entry: &ExecutionEntrySol,
    update: &StateUpdateSol,
    settled_rollup: u64,
) -> ClaimedEntryShape {
    if !entry.success || !entry.expectedL1ToL2Calls.is_empty() {
        return ClaimedEntryShape::Invalid;
    }

    if entry.proxyEntryHash != B256::ZERO {
        return if entry.l2ToL1Calls.is_empty() {
            ClaimedEntryShape::Inbound
        } else {
            ClaimedEntryShape::Invalid
        };
    }
    if !entry.l2ToL1Calls.is_empty() {
        return if entry.l2ToL1Calls.len() == 1
            && entry.l2ToL1Calls[0].revertNextNCalls == 0
            && !entry.l2ToL1Calls[0].isStatic
            && entry.l2ToL1Calls[0].gas == 0
        {
            ClaimedEntryShape::Outbound
        } else {
            ClaimedEntryShape::Invalid
        };
    }

    let expected_anchor_hash =
        EntryRollingHash::for_l1([(update.rollupId, update.currentState)], B256::ZERO).current();
    if entry.destinationRollupId == settled_rollup
        && entry.returnData.is_empty()
        && entry.rollingHash == expected_anchor_hash
    {
        ClaimedEntryShape::Anchor
    } else {
        ClaimedEntryShape::Invalid
    }
}
