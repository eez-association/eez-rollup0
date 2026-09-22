//! Continuity checks for Composer-claimed state-update chains.

use std::num::NonZeroU64;

use alloy_primitives::B256;
use eez_protocol::abi::{ExecutionEntrySol, StateUpdateSol};
use thiserror::Error;

use super::post_batch::CanonicalPostBatch;

#[derive(Clone, Copy)]
struct VerifiedStateUpdateEntry<'batch> {
    claimed_entry: &'batch ExecutionEntrySol,
    claimed_update: &'batch StateUpdateSol,
}

/// A nonempty, single-rollup state-update chain bound to validated endpoints.
///
/// Private fields make construction exclusive to [`verify_state_update_chain`].
/// Downstream effect binding can therefore use each retained entry/update pair
/// without repeating the nonempty and exactly-one-update checks.
pub(crate) struct VerifiedStateUpdateChain<'batch> {
    expected_rollup: u64,
    leading: VerifiedStateUpdateEntry<'batch>,
    trailing: Vec<VerifiedStateUpdateEntry<'batch>>,
}

impl<'batch> VerifiedStateUpdateChain<'batch> {
    pub(super) const fn expected_rollup(&self) -> u64 {
        self.expected_rollup
    }

    pub(super) const fn leading(&self) -> (&'batch ExecutionEntrySol, &'batch StateUpdateSol) {
        (self.leading.claimed_entry, self.leading.claimed_update)
    }

    pub(super) fn trailing(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, &'batch ExecutionEntrySol, &'batch StateUpdateSol)> + '_
    {
        self.trailing
            .iter()
            .enumerate()
            .map(|(index, entry)| (index + 1, entry.claimed_entry, entry.claimed_update))
    }

    pub(super) fn trailing_len(&self) -> usize {
        self.trailing.len()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum StateUpdateChainError {
    #[error("batch has no execution entries")]
    NoEntries,
    #[error("batch entry {entry_index} has {actual} state updates; expected exactly one")]
    UpdateCount { entry_index: usize, actual: usize },
    #[error("batch claims rollup id {claimed}; expected rollup id {expected}")]
    ExpectedRollupMismatch { expected: u64, claimed: u64 },
    #[error(
        "batch entry {entry_index} claims rollup id {claimed}; expected common rollup id {expected}"
    )]
    RollupMismatch {
        entry_index: usize,
        expected: u64,
        claimed: u64,
    },
    #[error(
        "leading state update claims initial block {claimed}; validated window opens at {validated}"
    )]
    InitialBlockMismatch { validated: B256, claimed: B256 },
    #[error(
        "commitment chain breaks before entry {entry_index}: previous claimed block is {previous_claimed_candidate}, next claimed predecessor is {next_claimed_predecessor}"
    )]
    ChainBreak {
        entry_index: usize,
        previous_claimed_candidate: B256,
        next_claimed_predecessor: B256,
    },
    #[error(
        "commitment chain claims final block {claimed}; validated window closes at {validated}"
    )]
    FinalMismatch { validated: B256, claimed: B256 },
}

/// Require one claimed state update per entry, the expected rollup, and a
/// continuous commitment chain between the locally validated window endpoints.
///
/// This checks continuity of Composer claims; `bind_effects_to_execution`
/// separately proves that the leading entry is an anchor and binds interior
/// commitments to the candidate blocks sealed at each transaction boundary.
pub(crate) fn verify_state_update_chain(
    batch: &CanonicalPostBatch,
    expected_rollup_id: NonZeroU64,
    validated_window_pre_block_hash: B256,
    validated_window_post_block_hash: B256,
) -> Result<VerifiedStateUpdateChain<'_>, StateUpdateChainError> {
    let submitted_batch = batch.as_batch();
    let (leading_claimed_entry, trailing_claimed_entries) =
        submitted_batch
            .entries
            .split_first()
            .ok_or(StateUpdateChainError::NoEntries)?;
    let leading_claimed_update = sole_update(leading_claimed_entry.stateUpdates.as_slice(), 0)?;
    let expected_rollup = expected_rollup_id.get();
    if leading_claimed_update.rollupId != expected_rollup {
        return Err(StateUpdateChainError::ExpectedRollupMismatch {
            expected: expected_rollup,
            claimed: leading_claimed_update.rollupId,
        });
    }
    if leading_claimed_update.currentState != validated_window_pre_block_hash {
        return Err(StateUpdateChainError::InitialBlockMismatch {
            validated: validated_window_pre_block_hash,
            claimed: leading_claimed_update.currentState,
        });
    }

    let claimed_rollup = leading_claimed_update.rollupId;
    let mut previous_claimed_candidate = leading_claimed_update.newState;
    let mut verified_trailing = Vec::with_capacity(trailing_claimed_entries.len());
    for (entry_index, entry) in trailing_claimed_entries.iter().enumerate() {
        let entry_index = entry_index + 1;
        let claimed_update = sole_update(entry.stateUpdates.as_slice(), entry_index)?;
        if claimed_update.rollupId != claimed_rollup {
            return Err(StateUpdateChainError::RollupMismatch {
                entry_index,
                expected: claimed_rollup,
                claimed: claimed_update.rollupId,
            });
        }
        if claimed_update.currentState != previous_claimed_candidate {
            return Err(StateUpdateChainError::ChainBreak {
                entry_index,
                previous_claimed_candidate,
                next_claimed_predecessor: claimed_update.currentState,
            });
        }
        previous_claimed_candidate = claimed_update.newState;
        verified_trailing.push(VerifiedStateUpdateEntry {
            claimed_entry: entry,
            claimed_update,
        });
    }

    if previous_claimed_candidate != validated_window_post_block_hash {
        return Err(StateUpdateChainError::FinalMismatch {
            validated: validated_window_post_block_hash,
            claimed: previous_claimed_candidate,
        });
    }

    Ok(VerifiedStateUpdateChain {
        expected_rollup,
        leading: VerifiedStateUpdateEntry {
            claimed_entry: leading_claimed_entry,
            claimed_update: leading_claimed_update,
        },
        trailing: verified_trailing,
    })
}

fn sole_update(
    state_updates: &[StateUpdateSol],
    entry_index: usize,
) -> Result<&StateUpdateSol, StateUpdateChainError> {
    let [update] = state_updates else {
        return Err(StateUpdateChainError::UpdateCount {
            entry_index,
            actual: state_updates.len(),
        });
    };
    Ok(update)
}
