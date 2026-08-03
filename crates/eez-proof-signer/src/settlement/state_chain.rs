//! Continuity checks for Composer-claimed state-delta chains.

use std::num::NonZeroU64;

use alloy_primitives::{B256, U256};
use eez_protocol::abi::{ExecutionEntrySol, LookupCallSol, StateDeltaSol};
use thiserror::Error;

use super::post_batch::CanonicalPostBatch;

#[derive(Clone, Copy)]
struct VerifiedStateDeltaEntry<'batch> {
    claimed_entry: &'batch ExecutionEntrySol,
    claimed_delta: &'batch StateDeltaSol,
}

/// A nonempty, single-rollup state-delta chain bound to validated endpoints.
///
/// Private fields make construction exclusive to [`verify_state_delta_chain`].
/// Downstream effect binding can therefore use each retained entry/delta pair
/// without repeating the nonempty and exactly-one-delta checks.
pub(crate) struct VerifiedStateDeltaChain<'batch> {
    expected_rollup: U256,
    leading: VerifiedStateDeltaEntry<'batch>,
    trailing: Vec<VerifiedStateDeltaEntry<'batch>>,
    submitted_lookup_calls: &'batch [LookupCallSol],
}

impl<'batch> VerifiedStateDeltaChain<'batch> {
    pub(super) const fn expected_rollup(&self) -> U256 {
        self.expected_rollup
    }

    pub(super) const fn leading(&self) -> (&'batch ExecutionEntrySol, &'batch StateDeltaSol) {
        (self.leading.claimed_entry, self.leading.claimed_delta)
    }

    pub(super) fn trailing(
        &self,
    ) -> impl ExactSizeIterator<Item = (usize, &'batch ExecutionEntrySol, &'batch StateDeltaSol)> + '_
    {
        self.trailing
            .iter()
            .enumerate()
            .map(|(index, entry)| (index + 1, entry.claimed_entry, entry.claimed_delta))
    }

    pub(super) fn trailing_len(&self) -> usize {
        self.trailing.len()
    }

    pub(super) const fn submitted_lookup_calls(&self) -> &'batch [LookupCallSol] {
        self.submitted_lookup_calls
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum StateDeltaChainError {
    #[error("batch has no execution entries")]
    NoEntries,
    #[error("batch entry {entry_index} has {actual} state deltas; expected exactly one")]
    DeltaCount { entry_index: usize, actual: usize },
    #[error("batch claims rollup id {claimed}; expected rollup id {expected}")]
    ExpectedRollupMismatch { expected: u64, claimed: U256 },
    #[error(
        "batch entry {entry_index} claims rollup id {claimed}; expected common rollup id {expected}"
    )]
    RollupMismatch {
        entry_index: usize,
        expected: U256,
        claimed: U256,
    },
    #[error(
        "leading state delta claims initial root {claimed}; validated window root is {validated}"
    )]
    InitialRootMismatch { validated: B256, claimed: B256 },
    #[error(
        "state-delta chain breaks before entry {entry_index}: previous claimed post-state is {previous_claimed_post_state}, next claimed pre-state is {next_claimed_pre_state}"
    )]
    ChainBreak {
        entry_index: usize,
        previous_claimed_post_state: B256,
        next_claimed_pre_state: B256,
    },
    #[error("state-delta chain claims final root {claimed}; validated final root is {validated}")]
    FinalMismatch { validated: B256, claimed: B256 },
}

/// Require one claimed state delta per entry, the expected rollup, and a
/// continuous root chain between the locally validated window endpoints.
///
/// This checks continuity of Composer claims; `bind_effects_to_execution`
/// separately proves that the leading entry is an anchor and binds interior
/// roots to transaction checkpoints.
pub(crate) fn verify_state_delta_chain(
    batch: &CanonicalPostBatch,
    expected_rollup_id: NonZeroU64,
    validated_window_pre_state_root: B256,
    validated_window_post_state_root: B256,
) -> Result<VerifiedStateDeltaChain<'_>, StateDeltaChainError> {
    let submitted_batch = batch.as_batch();
    let (leading_claimed_entry, trailing_claimed_entries) =
        submitted_batch
            .entries
            .split_first()
            .ok_or(StateDeltaChainError::NoEntries)?;
    let leading_claimed_delta = sole_delta(leading_claimed_entry.stateDeltas.as_slice(), 0)?;
    let expected_rollup = expected_rollup_id.get();
    if leading_claimed_delta.rollupId != U256::from(expected_rollup) {
        return Err(StateDeltaChainError::ExpectedRollupMismatch {
            expected: expected_rollup,
            claimed: leading_claimed_delta.rollupId,
        });
    }
    if leading_claimed_delta.currentState != validated_window_pre_state_root {
        return Err(StateDeltaChainError::InitialRootMismatch {
            validated: validated_window_pre_state_root,
            claimed: leading_claimed_delta.currentState,
        });
    }

    let claimed_rollup = leading_claimed_delta.rollupId;
    let mut previous_claimed_post_state = leading_claimed_delta.newState;
    let mut verified_trailing = Vec::with_capacity(trailing_claimed_entries.len());
    for (entry_index, entry) in trailing_claimed_entries.iter().enumerate() {
        let entry_index = entry_index + 1;
        let claimed_delta = sole_delta(entry.stateDeltas.as_slice(), entry_index)?;
        if claimed_delta.rollupId != claimed_rollup {
            return Err(StateDeltaChainError::RollupMismatch {
                entry_index,
                expected: claimed_rollup,
                claimed: claimed_delta.rollupId,
            });
        }
        if claimed_delta.currentState != previous_claimed_post_state {
            return Err(StateDeltaChainError::ChainBreak {
                entry_index,
                previous_claimed_post_state,
                next_claimed_pre_state: claimed_delta.currentState,
            });
        }
        previous_claimed_post_state = claimed_delta.newState;
        verified_trailing.push(VerifiedStateDeltaEntry {
            claimed_entry: entry,
            claimed_delta,
        });
    }

    if previous_claimed_post_state != validated_window_post_state_root {
        return Err(StateDeltaChainError::FinalMismatch {
            validated: validated_window_post_state_root,
            claimed: previous_claimed_post_state,
        });
    }
    Ok(VerifiedStateDeltaChain {
        expected_rollup: U256::from(expected_rollup),
        leading: VerifiedStateDeltaEntry {
            claimed_entry: leading_claimed_entry,
            claimed_delta: leading_claimed_delta,
        },
        trailing: verified_trailing,
        submitted_lookup_calls: &submitted_batch.l1ToL2lookupCalls,
    })
}

fn sole_delta(
    state_deltas: &[StateDeltaSol],
    entry_index: usize,
) -> Result<&StateDeltaSol, StateDeltaChainError> {
    let [delta] = state_deltas else {
        return Err(StateDeltaChainError::DeltaCount {
            entry_index,
            actual: state_deltas.len(),
        });
    };
    Ok(delta)
}
