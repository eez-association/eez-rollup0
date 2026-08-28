//! Batch-scoped proof-system resolution.
//!
//! Under the multi-prover protocol, every batch destined for L1's
//! `EEZ.postAndVerifyBatch` carries:
//!
//! - a batch-wide ordered `proofSystems[]` (strictly increasing by
//!   address),
//! - per-rollup `RollupIdWithProofSystems[]` mapping each touched
//!   rollup to its subset of `proofSystems[]` (also strictly
//!   increasing `uint64[]` indices into the global ordering),
//! - a jagged `vkMatrix` row-per-rollup, column-per-local-PS,
//! - one opaque `customData` blob per rollup, returned by its manager.
//!
//! A per-rollup-only resolver API cannot express this ordering
//! (the global `proofSystems[]` ordering, the local→global index
//! mapping, the jagged-matrix shape all live at the batch level).
//! The runtime proof-plan resolver resolves a batch's full proof plan in one
//! call.
//!
//! # Spec anchors
//!
//! The shapes here mirror the upstream `eez-core-protocol`
//! Solidity contracts (not vendored in this repo); the
//! `publicInputsHash` fold and on-chain `_validateStructure` are
//! the construction this plan is shaped to feed.

use alloy_primitives::{Address, Bytes};

use crate::rollup_id::RollupId;

/// Per-rollup attestation entry inside a [`ProofPlan`]. Mirrors
/// the on-chain `RollupIdWithProofSystems` struct exactly.
///
/// `proof_system_indexes` is a strictly increasing `Vec<u64>` of
/// indices into [`ProofPlan::proof_systems`]. The strict-
/// increasing invariant is enforced both off-chain by the
/// resolver and on-chain by `EEZ.sol::_validateStructure`; it's
/// what makes `EEZ.sol`'s `_findIndexPosition` binary search
/// safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupProofAssignment {
    /// The rollup this assignment covers. `RollupProofAssignment`s
    /// inside a [`ProofPlan`] are sorted by `rollup_id` ascending
    /// (the canonical batch ordering).
    pub rollup_id: RollupId,
    /// Sorted indices into `ProofPlan::proof_systems`. Each entry
    /// must be `< proof_systems.len()`.
    pub proof_system_indexes: Vec<u64>,
}

/// Complete proof-system plan for one L1 `postAndVerifyBatch` call.
///
/// Maps directly to the on-chain
/// `ProofSystemBatchPerVerificationEntries` struct's
/// proof-related fields. Consumed by the
/// `publicInputsHash`-computing layer + the signer-driven
/// proof-population layer (both upstream-specified).
///
/// The `vk_matrix` is jagged:
/// `vk_matrix[r]` has the same length as
/// `rollup_assignments[r].proof_system_indexes`.
#[derive(Debug, Clone)]
pub struct ProofPlan {
    /// Batch-wide ordered PS set. Strictly increasing by
    /// address — matches `ProofSystemBatchPerVerificationEntries
    /// .proofSystems` exactly.
    pub proof_systems: Vec<Address>,
    /// Per-rollup assignments, sorted by `rollup_id` ascending.
    /// Length is the rollup count for this batch.
    pub rollup_assignments: Vec<RollupProofAssignment>,
    /// Opaque manager data parallel to `rollup_assignments`. On-chain this is
    /// returned by `getCustomData(batch.blockNumber)`.
    pub custom_data: Vec<Bytes>,
    /// Jagged vkey matrix. `vk_matrix[r][j]` is the vkey rollup
    /// `r`'s manager returned for the PS at
    /// `rollup_assignments[r].proof_system_indexes[j]`. Row count
    /// equals `rollup_assignments.len()`; each row's length
    /// equals that row's `proof_system_indexes.len()`.
    pub vk_matrix: Vec<Vec<[u8; 32]>>,
}

impl ProofPlan {
    /// Number of rollups this plan attests to.
    #[must_use]
    pub fn rollup_count(&self) -> usize {
        self.rollup_assignments.len()
    }

    /// Number of proof systems in the batch-wide ordering.
    #[must_use]
    pub fn proof_system_count(&self) -> usize {
        self.proof_systems.len()
    }
}

/// Structural-validation errors a [`ProofPlan`] can fail with at
/// shape-check time. Distinct from
/// [`crate::error::ExecutorError`] — the resolver returns
/// `ExecutorError` for on-chain / transport-level failures; this
/// type covers in-memory shape invariants that a resolver might
/// produce a malformed plan against.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProofPlanInvariantError {
    /// `proof_systems` was empty. The on-chain
    /// `_validateStructure` rejects this with
    /// `InvalidProofSystemConfig`.
    #[error("proof_systems is empty")]
    EmptyProofSystems,
    /// `rollup_assignments` was empty. The on-chain
    /// `_validateStructure` rejects this with
    /// `InvalidProofSystemConfig`.
    #[error("rollup_assignments is empty")]
    EmptyRollupAssignments,
    /// `proof_systems` was not strictly increasing.
    #[error("proof_systems not strictly increasing at index {index}")]
    ProofSystemsNotSorted {
        /// Position of the first non-increasing entry.
        index: usize,
    },
    /// `rollup_assignments` was not sorted strictly ascending
    /// by `rollup_id`. The on-chain check uses `prevRid =
    /// MAINNET_ROLLUP_ID (= 0)` as the sentinel, so the first
    /// assignment's `rollup_id` MUST be `> 0`. This validator
    /// matches that behavior — `index == 0` means the first
    /// rollup_id was 0 (reserved).
    #[error(
        "rollup_assignments not strictly increasing at index {index} (sentinel = MAINNET_ROLLUP_ID = 0)"
    )]
    RollupAssignmentsNotSorted {
        /// Position of the first non-sorted entry; `0` if the
        /// first entry's `rollup_id` itself was `0`.
        index: usize,
    },
    /// A `rollup_assignments[r].proof_system_indexes` row was empty.
    /// The on-chain check rejects this with
    /// `InvalidProofSystemConfig`.
    #[error("rollup_assignments[{rollup_idx}].proof_system_indexes is empty")]
    EmptyProofSystemIndex {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
    },
    /// A `proof_system_indexes[r][j]` was not strictly increasing.
    #[error(
        "rollup_assignments[{rollup_idx}].proof_system_indexes not strictly increasing at {index}"
    )]
    ProofSystemIndexNotSorted {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
        /// Position within that row's `proof_system_indexes` where
        /// the strict-increasing invariant first broke.
        index: usize,
    },
    /// A `proof_system_indexes[r][j]` was out of bounds for
    /// `proof_systems`.
    #[error(
        "rollup_assignments[{rollup_idx}].proof_system_indexes[{index}]={value} >= proof_systems.len()={ps_len}"
    )]
    ProofSystemIndexOutOfBounds {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
        /// Position within that row's `proof_system_index`.
        index: usize,
        /// The out-of-bounds value found.
        value: u64,
        /// Length of the batch-wide `proof_systems`.
        ps_len: usize,
    },
    /// `vk_matrix.len()` didn't equal `rollup_assignments.len()`.
    #[error(
        "vk_matrix outer length mismatch: expected {expected} rows (rollup_assignments.len()), got {got}"
    )]
    VkMatrixOuterLength {
        /// Expected outer length.
        expected: usize,
        /// Actual outer length found.
        got: usize,
    },
    /// A `vk_matrix[r]` row's length didn't equal the
    /// corresponding `proof_system_indexes[r].len()`.
    #[error(
        "vk_matrix[{rollup_idx}] row width mismatch: expected {expected} (proof_system_indexes.len()), got {got}"
    )]
    VkMatrixRowLength {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
        /// Expected row width.
        expected: usize,
        /// Actual row width found.
        got: usize,
    },
    /// `custom_data.len()` didn't equal `rollup_assignments.len()`.
    #[error("custom_data length {got} != rollup_assignments length {expected}")]
    CustomDataLength {
        /// Expected length (matches `rollup_assignments.len()`).
        expected: usize,
        /// Actual length found.
        got: usize,
    },
}

impl ProofPlan {
    /// Walk every structural invariant a well-formed `ProofPlan`
    /// must satisfy, returning the first violation. Resolvers
    /// SHOULD call this on their output before returning;
    /// downstream consumers (the publicInputsHash compute layer)
    /// may rely on the invariants without re-checking.
    ///
    /// # Errors
    ///
    /// Returns the first [`ProofPlanInvariantError`] encountered.
    pub fn check_invariants(&self) -> Result<(), ProofPlanInvariantError> {
        // Non-emptiness: matches on-chain `_validateStructure`.
        // A batch with zero PSes or zero rollups is rejected loudly.
        if self.proof_systems.is_empty() {
            return Err(ProofPlanInvariantError::EmptyProofSystems);
        }
        if self.rollup_assignments.is_empty() {
            return Err(ProofPlanInvariantError::EmptyRollupAssignments);
        }

        // Solidity seeds the ordering check with address(0), so the first
        // proof system must also be non-zero.
        if self.proof_systems[0] == Address::ZERO {
            return Err(ProofPlanInvariantError::ProofSystemsNotSorted { index: 0 });
        }

        // proof_systems strictly increasing.
        for (i, pair) in self.proof_systems.windows(2).enumerate() {
            if pair[0] >= pair[1] {
                return Err(ProofPlanInvariantError::ProofSystemsNotSorted { index: i + 1 });
            }
        }

        // rollup_assignments sorted strictly ascending by
        // rollup_id, with the on-chain `prevRid =
        // MAINNET_ROLLUP_ID (= 0)` sentinel: the first
        // rollup_id MUST be > 0. Catches both unsorted and the
        // reserved `rollup_id == 0` case in one pass.
        let mut prev_rid: u64 = 0;
        let mut first = true;
        for (i, assignment) in self.rollup_assignments.iter().enumerate() {
            if !first && assignment.rollup_id.0 <= prev_rid {
                return Err(ProofPlanInvariantError::RollupAssignmentsNotSorted { index: i });
            }
            if first && assignment.rollup_id.0 == 0 {
                return Err(ProofPlanInvariantError::RollupAssignmentsNotSorted { index: 0 });
            }
            prev_rid = assignment.rollup_id.0;
            first = false;
        }

        // Custom-data rows stay parallel to rollup assignments.
        if self.custom_data.len() != self.rollup_assignments.len() {
            return Err(ProofPlanInvariantError::CustomDataLength {
                expected: self.rollup_assignments.len(),
                got: self.custom_data.len(),
            });
        }

        // vk_matrix outer length matches.
        if self.vk_matrix.len() != self.rollup_assignments.len() {
            return Err(ProofPlanInvariantError::VkMatrixOuterLength {
                expected: self.rollup_assignments.len(),
                got: self.vk_matrix.len(),
            });
        }

        // Per-rollup checks: non-empty index, strict-increasing
        // local PS indices, bounded by proof_systems.len();
        // vkey row width matches.
        let ps_len = self.proof_systems.len();
        for (r, assignment) in self.rollup_assignments.iter().enumerate() {
            if assignment.proof_system_indexes.is_empty() {
                return Err(ProofPlanInvariantError::EmptyProofSystemIndex { rollup_idx: r });
            }
            for (j, pair) in assignment.proof_system_indexes.windows(2).enumerate() {
                if pair[0] >= pair[1] {
                    return Err(ProofPlanInvariantError::ProofSystemIndexNotSorted {
                        rollup_idx: r,
                        index: j + 1,
                    });
                }
            }
            for (j, &idx) in assignment.proof_system_indexes.iter().enumerate() {
                if (idx as usize) >= ps_len {
                    return Err(ProofPlanInvariantError::ProofSystemIndexOutOfBounds {
                        rollup_idx: r,
                        index: j,
                        value: idx,
                        ps_len,
                    });
                }
            }
            let expected_row = assignment.proof_system_indexes.len();
            let got_row = self.vk_matrix[r].len();
            if expected_row != got_row {
                return Err(ProofPlanInvariantError::VkMatrixRowLength {
                    rollup_idx: r,
                    expected: expected_row,
                    got: got_row,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_ps_plan(rollup_id: RollupId, ps: Address, vkey: [u8; 32]) -> ProofPlan {
        ProofPlan {
            proof_systems: vec![ps],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id,
                proof_system_indexes: vec![0],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![vkey]],
        }
    }

    #[test]
    fn empty_proof_systems_rejected() {
        let plan = ProofPlan {
            proof_systems: vec![],
            rollup_assignments: vec![],
            custom_data: vec![],
            vk_matrix: vec![],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyProofSystems)
        );
    }

    #[test]
    fn empty_rollup_assignments_rejected() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![],
            custom_data: vec![],
            vk_matrix: vec![],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyRollupAssignments)
        );
    }

    #[test]
    fn rollup_id_zero_rejected_first_position() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(0),
                proof_system_indexes: vec![0],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0u8; 32]]],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::RollupAssignmentsNotSorted { index: 0 })
        );
    }

    #[test]
    fn empty_proof_system_index_rejected() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![]],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyProofSystemIndex { rollup_idx: 0 })
        );
    }

    #[test]
    fn vk_matrix_outer_length_mismatch_caught() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![0],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![], // empty — should have 1 row
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::VkMatrixOuterLength {
                expected: 1,
                got: 0,
            })
        );
    }

    #[test]
    fn single_rollup_single_ps_is_valid() {
        let plan = single_ps_plan(RollupId(1), Address::repeat_byte(0xaa), [0x42; 32]);
        plan.check_invariants().expect("happy path");
        assert_eq!(plan.rollup_count(), 1);
        assert_eq!(plan.proof_system_count(), 1);
    }

    #[test]
    fn proof_systems_must_be_strictly_increasing() {
        let mut plan = single_ps_plan(RollupId(1), Address::repeat_byte(0xaa), [0x42; 32]);
        plan.proof_systems = vec![Address::repeat_byte(0xaa), Address::repeat_byte(0xaa)];
        plan.vk_matrix = vec![vec![[0x42; 32]]]; // shape unchanged for the one rollup
        plan.rollup_assignments[0].proof_system_indexes = vec![0];
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemsNotSorted { .. })
        ));
    }

    #[test]
    fn zero_proof_system_rejected_first_position() {
        let plan = single_ps_plan(RollupId(1), Address::ZERO, [0x42; 32]);
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemsNotSorted { index: 0 })
        );
    }

    #[test]
    fn rollup_assignments_must_be_sorted() {
        let mut plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![
                RollupProofAssignment {
                    rollup_id: RollupId(2),
                    proof_system_indexes: vec![0],
                },
                RollupProofAssignment {
                    rollup_id: RollupId(1),
                    proof_system_indexes: vec![0],
                },
            ],
            custom_data: vec![Bytes::new(); 2],
            vk_matrix: vec![vec![[0u8; 32]], vec![[0u8; 32]]],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::RollupAssignmentsNotSorted { .. })
        ));
        plan.rollup_assignments.reverse();
        plan.check_invariants().expect("sorted now");
    }

    #[test]
    fn proof_system_index_must_be_strictly_increasing() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa), Address::repeat_byte(0xbb)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![1, 0],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0u8; 32], [0u8; 32]]],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemIndexNotSorted { .. })
        ));
    }

    #[test]
    fn proof_system_index_out_of_bounds_caught() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![5],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0u8; 32]]],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemIndexOutOfBounds {
                value: 5,
                ps_len: 1,
                ..
            })
        ));
    }

    #[test]
    fn vk_matrix_row_width_must_match_local_index_length() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa), Address::repeat_byte(0xbb)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![0, 1],
            }],
            custom_data: vec![Bytes::new()],
            vk_matrix: vec![vec![[0u8; 32]]], // only 1 col, expected 2
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::VkMatrixRowLength {
                expected: 2,
                got: 1,
                ..
            })
        ));
    }

    #[test]
    fn custom_data_length_must_match() {
        let plan = ProofPlan {
            proof_systems: vec![Address::repeat_byte(0xaa)],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_indexes: vec![0],
            }],
            custom_data: vec![],
            vk_matrix: vec![vec![[0u8; 32]]],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::CustomDataLength { .. })
        ));
    }

    #[test]
    fn custom_data_is_opaque() {
        let mut plan = single_ps_plan(RollupId(1), Address::repeat_byte(0xaa), [0x42; 32]);
        plan.custom_data[0] = Bytes::from_static(b"manager-defined");
        plan.check_invariants().expect("opaque bytes are accepted");
    }
}
