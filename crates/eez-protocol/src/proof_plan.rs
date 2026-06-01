//! Batch-scoped proof-system resolution.
//!
//! Under the multi-prover protocol, every batch destined for L1's
//! `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch` carries:
//!
//! - a batch-wide ordered `proofSystems[]` (strictly increasing by
//!   address),
//! - per-rollup `RollupIdWithProofSystems[]` mapping each touched
//!   rollup to its subset of `proofSystems[]` (also strictly
//!   increasing `uint64[]` indices into the global ordering),
//! - a jagged `vkMatrix` row-per-rollup, column-per-local-PS,
//!   where row `r`'s column `j` is the vkey rollup `r`'s manager
//!   returned for the PS at `proofSystemIndex[r][j]`,
//! - per-rollup `(timestamp, blockHash)` pairs read once from
//!   each manager's `getTimestampAndBlockHash()`.
//!
//! A per-rollup-only resolver API cannot express this ordering
//! (the global `proofSystems[]` ordering, the local→global index
//! mapping, the jagged-matrix shape all live at the batch level).
//! [`ProofPlanResolver`] resolves a batch's full proof plan in
//! one call.
//!
//! # Chain-agnostic shape
//!
//! The trait is parameterized by [`crate::ChainProtocol`] so the
//! `Address` type stays chain-specific. Timestamps and block
//! hashes use the wire shape `[u8; 32]` (big-endian for
//! timestamps) — that's the natural input to the chain's
//! `abi.encode`-equivalent fold and avoids pulling alloy into
//! the protocol crate. EVM-side concrete impls convert to
//! `alloy_primitives::U256` / `B256` at the encoder boundary.
//!
//! # Spec anchors
//!
//! - [`docs/DERIVATION.md`](../../docs/DERIVATION.md) §6e
//!   (Posting — the publicInputsHash fold).
//! - [`INVARIANTS.md`](../../INVARIANTS.md) Inv 3 (Real-time
//!   proofs / proof-system pluggable).
//! - `sync-rollups-protocol@0864392 src/EEZ.sol:606-668` — the
//!   on-chain construction this trait is shaped to feed.

use async_trait::async_trait;

use crate::error::ExecutorResult;
use crate::protocol::ChainProtocol;
use crate::rollup_id::RollupId;

/// Per-rollup attestation entry inside a [`ProofPlan`]. Mirrors
/// the on-chain `RollupIdWithProofSystems` struct exactly.
///
/// `proof_system_index` is a strictly increasing `Vec<u64>` of
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
    pub proof_system_index: Vec<u64>,
}

/// Per-rollup `(timestamp, blockHash)` pair fetched once from
/// each manager's `getTimestampAndBlockHash()`. Folded into the
/// per-PS `publicInputsHash` accumulator (§6e of DERIVATION.md).
///
/// Wire shape `[u8; 32]` is the natural input to a chain's
/// `abi.encode`-equivalent fold. The reference `Rollup.sol` stub
/// returns `(0, bytes32(0))`; real managers override with
/// rollup-specific block info.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampAndBlockHash {
    /// Manager-attested timestamp at the moment of `postBatch`
    /// execution. On-chain type is `uint256`; encoded as
    /// big-endian 32 bytes by the EVM-side `abi.encode`
    /// boundary.
    pub timestamp: [u8; 32],
    /// Manager-attested block hash.
    pub block_hash: [u8; 32],
}

impl Default for TimestampAndBlockHash {
    /// Matches the reference `Rollup.sol`'s
    /// `getTimestampAndBlockHash()` stub: `(0, bytes32(0))`.
    fn default() -> Self {
        Self {
            timestamp: [0u8; 32],
            block_hash: [0u8; 32],
        }
    }
}

/// Complete proof-system plan for one L1 `postBatch` call.
///
/// Maps directly to the on-chain
/// `ProofSystemBatchPerVerificationEntries` struct's
/// proof-related fields. Consumed by the
/// `publicInputsHash`-computing layer (Phase 09 §C) +
/// signer-driven proof population layer (Phase 09 §E).
///
/// Generic over `P: ChainProtocol` so the address type (`P::
/// Address`) stays chain-specific. The `vk_matrix` is jagged:
/// `vk_matrix[r]` has the same length as
/// `rollup_assignments[r].proof_system_index`.
#[derive(Debug, Clone)]
pub struct ProofPlan<P: ChainProtocol + ?Sized>
where
    P::Address: PartialEq + Eq,
{
    /// Batch-wide ordered PS set. Strictly increasing by
    /// address — matches `ProofSystemBatchPerVerificationEntries
    /// .proofSystems` exactly.
    pub proof_systems: Vec<P::Address>,
    /// Per-rollup assignments, sorted by `rollup_id` ascending.
    /// Length is the rollup count for this batch.
    pub rollup_assignments: Vec<RollupProofAssignment>,
    /// Per-rollup `(timestamp, blockHash)` parallel to
    /// `rollup_assignments`. Each entry was fetched once via the
    /// manager's view function ahead of the per-PS loop, per
    /// `EEZ.sol:642-651`.
    pub per_rollup_context: Vec<TimestampAndBlockHash>,
    /// Jagged vkey matrix. `vk_matrix[r][j]` is the vkey rollup
    /// `r`'s manager returned for the PS at
    /// `rollup_assignments[r].proof_system_index[j]`. Row count
    /// equals `rollup_assignments.len()`; each row's length
    /// equals that row's `proof_system_index.len()`.
    pub vk_matrix: Vec<Vec<[u8; 32]>>,
    /// `crossProofSystemInteractions` field on the batch. For
    /// single-PS phases this is `[0u8; 32]`; multi-PS quorums
    /// fold their cross-interaction commitment in here.
    pub cross_proof_system_interactions: [u8; 32],
}

impl<P: ChainProtocol + ?Sized> ProofPlan<P>
where
    P::Address: PartialEq + Eq,
{
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

/// Resolve a batch's complete proof plan in one call. Stateless
/// from the trait's perspective; impls hold the underlying
/// candidate-PS configuration + on-chain provider.
///
/// # Errors
///
/// Impl-dependent. Typical failure modes (per-impl):
/// - candidate PS not allowed for a touched rollup (manager's
///   `checkProofSystemsAndGetVkeys` reverts);
/// - touched rollup not registered on the central registry;
/// - on-chain provider transport failure.
#[async_trait]
pub trait ProofPlanResolver<P: ChainProtocol + ?Sized>: Send + Sync
where
    P::Address: PartialEq + Eq + Send + Sync,
{
    /// Resolve the plan for the given touched rollup set. The
    /// returned plan's `rollup_assignments` MUST be sorted by
    /// `rollup_id` ascending (canonical batch order) regardless
    /// of input order.
    async fn resolve(&self, touched: &[RollupId]) -> ExecutorResult<ProofPlan<P>>;
}

/// Structural-validation errors a [`ProofPlan`] can fail with at
/// shape-check time. Distinct from
/// [`crate::error::ExecutorError`] — the resolver returns
/// `ExecutorError` for on-chain / transport-level failures; this
/// type covers in-memory shape invariants that an impl might
/// produce a malformed plan against.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProofPlanInvariantError {
    /// `proof_systems` was empty. The on-chain
    /// `_validateStructure` rejects this with
    /// `InvalidProofSystemConfig` (`EEZ.sol:488`).
    #[error("proof_systems is empty")]
    EmptyProofSystems,
    /// `rollup_assignments` was empty. The on-chain
    /// `_validateStructure` rejects this with
    /// `InvalidProofSystemConfig` (`EEZ.sol:490`).
    #[error("rollup_assignments is empty")]
    EmptyRollupAssignments,
    /// `proof_systems` was not strictly increasing. The
    /// chain-agnostic validator can't check for the zero
    /// "address" (the `P::Address` type's zero is
    /// chain-specific); EVM-side impls additionally reject
    /// `address(0)` per `EEZ.sol:500`.
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
    #[error("rollup_assignments not strictly increasing at index {index} (sentinel = MAINNET_ROLLUP_ID = 0)")]
    RollupAssignmentsNotSorted {
        /// Position of the first non-sorted entry; `0` if the
        /// first entry's `rollup_id` itself was `0`.
        index: usize,
    },
    /// A `rollup_assignments[r].proof_system_index` was empty.
    /// The on-chain check rejects this with
    /// `InvalidProofSystemConfig` (`EEZ.sol:517`).
    #[error("rollup_assignments[{rollup_idx}].proof_system_index is empty")]
    EmptyProofSystemIndex {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
    },
    /// A `proof_system_index[r][j]` was not strictly increasing.
    #[error(
        "rollup_assignments[{rollup_idx}].proof_system_index not strictly increasing at {index}"
    )]
    ProofSystemIndexNotSorted {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
        /// Position within that row's `proof_system_index` where
        /// the strict-increasing invariant first broke.
        index: usize,
    },
    /// A `proof_system_index[r][j]` was out of bounds for
    /// `proof_systems`.
    #[error(
        "rollup_assignments[{rollup_idx}].proof_system_index[{index}]={value} >= proof_systems.len()={ps_len}"
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
    /// corresponding `proof_system_index[r].len()`.
    #[error(
        "vk_matrix[{rollup_idx}] row width mismatch: expected {expected} (proof_system_index.len()), got {got}"
    )]
    VkMatrixRowLength {
        /// Index into `rollup_assignments` of the offending row.
        rollup_idx: usize,
        /// Expected row width.
        expected: usize,
        /// Actual row width found.
        got: usize,
    },
    /// `per_rollup_context.len()` didn't equal
    /// `rollup_assignments.len()`.
    #[error("per_rollup_context length {got} != rollup_assignments length {expected}")]
    PerRollupContextLength {
        /// Expected length (matches `rollup_assignments.len()`).
        expected: usize,
        /// Actual length found.
        got: usize,
    },
}

impl<P: ChainProtocol + ?Sized> ProofPlan<P>
where
    P::Address: PartialEq + Eq + Ord,
{
    /// Walk every structural invariant a well-formed `ProofPlan`
    /// must satisfy, returning the first violation. Resolvers
    /// SHOULD call this on their output before returning;
    /// downstream consumers (the publicInputsHash computation in
    /// Phase 09 §C) may rely on the invariants without
    /// re-checking.
    ///
    /// `P::Address: Ord` so we can verify strict-increasing
    /// ordering on `proof_systems`.
    ///
    /// # Errors
    ///
    /// Returns the first [`ProofPlanInvariantError`] encountered.
    pub fn check_invariants(&self) -> Result<(), ProofPlanInvariantError> {
        // Non-emptiness: matches on-chain `_validateStructure`
        // (`EEZ.sol:488`, `EEZ.sol:490`). A batch with zero PSes
        // or zero rollups is rejected loudly.
        if self.proof_systems.is_empty() {
            return Err(ProofPlanInvariantError::EmptyProofSystems);
        }
        if self.rollup_assignments.is_empty() {
            return Err(ProofPlanInvariantError::EmptyRollupAssignments);
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

        // per_rollup_context length matches.
        if self.per_rollup_context.len() != self.rollup_assignments.len() {
            return Err(ProofPlanInvariantError::PerRollupContextLength {
                expected: self.rollup_assignments.len(),
                got: self.per_rollup_context.len(),
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
            if assignment.proof_system_index.is_empty() {
                return Err(ProofPlanInvariantError::EmptyProofSystemIndex { rollup_idx: r });
            }
            for (j, pair) in assignment.proof_system_index.windows(2).enumerate() {
                if pair[0] >= pair[1] {
                    return Err(ProofPlanInvariantError::ProofSystemIndexNotSorted {
                        rollup_idx: r,
                        index: j + 1,
                    });
                }
            }
            for (j, &idx) in assignment.proof_system_index.iter().enumerate() {
                if (idx as usize) >= ps_len {
                    return Err(ProofPlanInvariantError::ProofSystemIndexOutOfBounds {
                        rollup_idx: r,
                        index: j,
                        value: idx,
                        ps_len,
                    });
                }
            }
            let expected_row = assignment.proof_system_index.len();
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

    /// Stand-in protocol with `u64` addresses — keeps the test
    /// generic-machinery happy without dragging in EVM types.
    #[derive(Debug, Clone, Copy, Default)]
    struct Proto;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
    struct Empty;

    impl ChainProtocol for Proto {
        type Address = u64;
        type Value = u64;
        type Calldata = Vec<u8>;
        type Batch = ();
        type Overlay = Empty;
        type Witness = Empty;
        type Dialect = ();

        fn build_batch(
            &self,
            _recorded: &[crate::types::RecordedCall<Self>],
            _attribution: &crate::composer::SourceAttribution<'_>,
            _dialect: &Self::Dialect,
            _src: RollupId,
            _raw: &[u8],
        ) -> crate::error::ProtocolResult<Self::Batch> {
            Ok(())
        }
        fn encode_postbatch(&self, (): &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_load_table(&self, (): &Self::Batch) -> Vec<u8> {
            vec![]
        }
        fn encode_follower_trigger(
            &self,
            _: &crate::types::RecordedCall<Self>,
            _: RollupId,
            _: &[u8],
            (): &Self::Dialect,
        ) -> Vec<u8> {
            vec![]
        }
        fn encode_address(&self, a: &u64) -> Vec<u8> {
            a.to_be_bytes().to_vec()
        }
        fn decode_address(&self, b: &[u8]) -> crate::error::ProtocolResult<u64> {
            b.try_into().map(u64::from_be_bytes).map_err(|_e| {
                crate::error::ProtocolErrorKind::InvalidEncoding("addr".into()).into()
            })
        }
        fn encode_value(&self, v: &u64) -> Vec<u8> {
            v.to_be_bytes().to_vec()
        }
        fn decode_value(&self, b: &[u8]) -> crate::error::ProtocolResult<u64> {
            b.try_into().map(u64::from_be_bytes).map_err(|_e| {
                crate::error::ProtocolErrorKind::InvalidEncoding("value".into()).into()
            })
        }
        fn encode_calldata(&self, d: &Vec<u8>) -> Vec<u8> {
            d.clone()
        }
        fn decode_calldata(&self, b: &[u8]) -> crate::error::ProtocolResult<Vec<u8>> {
            Ok(b.to_vec())
        }
    }

    fn single_ps_plan(rollup_id: RollupId, ps: u64, vkey: [u8; 32]) -> ProofPlan<Proto> {
        ProofPlan {
            proof_systems: vec![ps],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id,
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![TimestampAndBlockHash::default()],
            vk_matrix: vec![vec![vkey]],
            cross_proof_system_interactions: [0u8; 32],
        }
    }

    #[test]
    fn empty_proof_systems_rejected() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![],
            rollup_assignments: vec![],
            per_rollup_context: vec![],
            vk_matrix: vec![],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyProofSystems)
        );
    }

    #[test]
    fn empty_rollup_assignments_rejected() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![],
            per_rollup_context: vec![],
            vk_matrix: vec![],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyRollupAssignments)
        );
    }

    #[test]
    fn rollup_id_zero_rejected_first_position() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(0),
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![vec![[0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::RollupAssignmentsNotSorted { index: 0 })
        );
    }

    #[test]
    fn empty_proof_system_index_rejected() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![vec![]],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert_eq!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::EmptyProofSystemIndex { rollup_idx: 0 })
        );
    }

    #[test]
    fn vk_matrix_outer_length_mismatch_caught() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![], // empty — should have 1 row
            cross_proof_system_interactions: [0u8; 32],
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
        let plan = single_ps_plan(RollupId(1), 0xaa, [0x42; 32]);
        plan.check_invariants().expect("happy path");
        assert_eq!(plan.rollup_count(), 1);
        assert_eq!(plan.proof_system_count(), 1);
    }

    #[test]
    fn proof_systems_must_be_strictly_increasing() {
        let mut plan = single_ps_plan(RollupId(1), 0xaa, [0x42; 32]);
        plan.proof_systems = vec![0xaa, 0xaa];
        plan.vk_matrix = vec![vec![[0x42; 32]]]; // shape unchanged for the one rollup
        plan.rollup_assignments[0].proof_system_index = vec![0];
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemsNotSorted { .. })
        ));
    }

    #[test]
    fn rollup_assignments_must_be_sorted() {
        let mut plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![
                RollupProofAssignment {
                    rollup_id: RollupId(2),
                    proof_system_index: vec![0],
                },
                RollupProofAssignment {
                    rollup_id: RollupId(1),
                    proof_system_index: vec![0],
                },
            ],
            per_rollup_context: vec![Default::default(); 2],
            vk_matrix: vec![vec![[0u8; 32]], vec![[0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
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
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa, 0xbb],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![1, 0],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![vec![[0u8; 32], [0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::ProofSystemIndexNotSorted { .. })
        ));
    }

    #[test]
    fn proof_system_index_out_of_bounds_caught() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![5],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![vec![[0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
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
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa, 0xbb],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![0, 1],
            }],
            per_rollup_context: vec![Default::default()],
            vk_matrix: vec![vec![[0u8; 32]]], // only 1 col, expected 2
            cross_proof_system_interactions: [0u8; 32],
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
    fn per_rollup_context_length_must_match() {
        let plan: ProofPlan<Proto> = ProofPlan {
            proof_systems: vec![0xaa],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![],
            vk_matrix: vec![vec![[0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
        };
        assert!(matches!(
            plan.check_invariants(),
            Err(ProofPlanInvariantError::PerRollupContextLength { .. })
        ));
    }

    #[test]
    fn timestamp_and_block_hash_default_matches_solidity_stub() {
        let ctx = TimestampAndBlockHash::default();
        assert_eq!(ctx.timestamp, [0u8; 32]);
        assert_eq!(ctx.block_hash, [0u8; 32]);
    }
}
