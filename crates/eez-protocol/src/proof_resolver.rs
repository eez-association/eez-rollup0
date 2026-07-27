//! Batch proof-plan resolver.
//!
//! Reads `EEZ.rollups(rid).rollupContract`,
//! `Rollup.checkProofSystemsAndGetVkeys(candidates)`, and
//! `IRollupContract.getTimestampAndBlockHash()` over an alloy
//! provider; assembles the result into a
//! `ProofPlan` validated against
//! [`crate::ProofPlan::check_invariants`] before
//! return.
//!
//! # Reader abstraction
//!
//! The on-chain reads are routed through the [`RollupReader`]
//! trait. This serves two purposes:
//!
//! - the prod path wraps an alloy `DynProvider` via
//!   [`AlloyRollupReader`];
//! - unit tests use a small in-memory fake without needing alloy
//!   mock-provider machinery.
//!
//! Either way, the [`ProofPlanResolver`] surface stays
//! identical.
//!
//! # Spec anchors
//!
//! Mirrors the `sync-rollups-protocol` Solidity contracts (in-tree
//! submodule at `sync-rollups-protocol`): `EEZ.sol`'s
//! `rollups` storage getter +
//! `_validateStructure` invariants, and `Rollup.sol`'s
//! `checkProofSystemsAndGetVkeys` + `getTimestampAndBlockHash`
//! views.

use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    ExecutorError, ExecutorResult, ProofPlan, RollupId, RollupProofAssignment,
    TimestampAndBlockHash,
};
use alloy_network::Ethereum;
use alloy_primitives::{Address, U256};
use alloy_provider::DynProvider;
use alloy_sol_types::sol;
use async_trait::async_trait;

// ── Solidity bindings ─────────────────────────────────────────────

sol! {
    /// Storage getter for `EEZ.rollups[rollupId]` (auto-generated
    /// from `mapping(uint256 => RollupConfig) public rollups`).
    /// Returns three fields in declaration order: `rollupContract`,
    /// `stateRoot`, `etherBalance` — only `rollupContract` is
    /// consumed here.
    #[sol(rpc)]
    interface IEEZReader {
        function rollups(uint256 rollupId)
            external
            view
            returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
    }

    /// View surface of `Rollup.sol` we need: vkey resolution +
    /// per-rollup (timestamp, blockHash). `IRollupContract` carries
    /// more, but this is the minimum surface for proof-plan
    /// resolution.
    #[sol(rpc)]
    interface IRollupReader {
        function checkProofSystemsAndGetVkeys(address[] calldata proofSystems)
            external
            view
            returns (bytes32[] memory vkeys);

        function getTimestampAndBlockHash(uint64 blockNumber)
            external
            view
            returns (uint256 timestamp, bytes32 blockHash);
    }
}

// ── Reader abstraction ────────────────────────────────────────────

/// The three on-chain reads [`ProofPlanResolver`] performs to
/// build a [`ProofPlan`]. Abstracted so tests can substitute an
/// in-memory fake without alloy mock-provider machinery.
#[async_trait]
pub trait RollupReader: Send + Sync {
    /// Read `EEZ.rollups(rid).rollupContract`. Returns
    /// `Ok(Address::ZERO)` for unregistered rollups (the caller
    /// translates to a typed error so the on-chain
    /// `_validateStructure`'s `rollupContract != 0` check is
    /// mirrored loudly).
    async fn rollup_contract(&self, rid: RollupId) -> ExecutorResult<Address>;

    /// Call `Rollup(rollupContract).checkProofSystemsAndGetVkeys(
    /// candidates)`. The manager enforces every candidate is
    /// allowed for this rollup (non-zero vkey) AND that
    /// `candidates.length ≥ threshold`; either failure reverts,
    /// which the impl surfaces as an [`ExecutorError`].
    /// Returns the resolved vkeys parallel to `candidates`.
    async fn check_proof_systems_and_get_vkeys(
        &self,
        rollup_contract: Address,
        candidates: &[Address],
    ) -> ExecutorResult<Vec<[u8; 32]>>;

    /// Read `IRollupContract(rollupContract).getTimestampAndBlockHash()`.
    /// Reference `Rollup.sol`'s stub returns `(0, bytes32(0))`;
    /// real managers override.
    async fn timestamp_and_block_hash(
        &self,
        rollup_contract: Address,
    ) -> ExecutorResult<TimestampAndBlockHash>;
}

/// Alloy-backed [`RollupReader`]. Wraps a `DynProvider<Ethereum>`
/// (the network type is hard-pinned here — extending to other
/// networks is a future generic over `N: Network`).
#[derive(Clone)]
pub struct AlloyRollupReader {
    provider: Arc<DynProvider<Ethereum>>,
    eez: Address,
}

impl std::fmt::Debug for AlloyRollupReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlloyRollupReader")
            .field("eez", &self.eez)
            .finish_non_exhaustive()
    }
}

impl AlloyRollupReader {
    /// Build a reader pinned to the central registry at `eez`.
    /// `provider` is shared via `Arc` so a single transport pool
    /// backs every read in one `resolve()` call.
    #[must_use]
    pub fn new(provider: Arc<DynProvider<Ethereum>>, eez: Address) -> Self {
        Self { provider, eez }
    }
}

#[async_trait]
impl RollupReader for AlloyRollupReader {
    async fn rollup_contract(&self, rid: RollupId) -> ExecutorResult<Address> {
        let eez_view = IEEZReader::new(self.eez, self.provider.as_ref());
        let resp = eez_view
            .rollups(U256::from(rid.0))
            .call()
            .await
            .map_err(|e| {
                // `eth_call` surface — surfaces both transport
                // failures AND contract-level reverts. Workspace
                // convention is `Provider` for the eth_call shape
                // (vs `Transport` for raw HTTP/gRPC stack
                // failures); keep all three reads in this impl
                // consistent.
                ExecutorError::Provider(format!("EEZ.rollups({}): {e}", rid.0).into())
            })?;
        Ok(resp.rollupContract)
    }

    async fn check_proof_systems_and_get_vkeys(
        &self,
        rollup_contract: Address,
        candidates: &[Address],
    ) -> ExecutorResult<Vec<[u8; 32]>> {
        let rollup_view = IRollupReader::new(rollup_contract, self.provider.as_ref());
        let vkeys = rollup_view
            .checkProofSystemsAndGetVkeys(candidates.to_vec())
            .call()
            .await
            .map_err(|e| {
                ExecutorError::Provider(
                    format!("checkProofSystemsAndGetVkeys @ {rollup_contract}: {e}").into(),
                )
            })?;
        Ok(vkeys.into_iter().map(Into::into).collect())
    }

    async fn timestamp_and_block_hash(
        &self,
        rollup_contract: Address,
    ) -> ExecutorResult<TimestampAndBlockHash> {
        let rollup_view = IRollupReader::new(rollup_contract, self.provider.as_ref());
        // DORMANT-SURFACE CAVEAT: this resolver reads the TIMELESS context
        // (blockNumber=0 → "(0, bytes32(0))"). The LIVE pipeline no longer
        // uses it — batches bind a recent N (`prepare_post_batch`) and both
        // sides fold `(0, blockhash(N))` via `public_inputs_hashes`. A future
        // consumer of this resolver must thread the batch's N here (the call
        // reverts past the 256-block window, mirroring on-chain semantics).
        let resp = rollup_view
            .getTimestampAndBlockHash(0)
            .call()
            .await
            .map_err(|e| {
                ExecutorError::Provider(
                    format!("getTimestampAndBlockHash @ {rollup_contract}: {e}").into(),
                )
            })?;
        Ok(TimestampAndBlockHash {
            timestamp: resp.timestamp.to_be_bytes::<32>(),
            block_hash: resp.blockHash.into(),
        })
    }
}

// ── Resolver ──────────────────────────────────────────────────────

/// Resolve a batch's complete proof plan in one call.
///
/// Holds the batch-wide candidate PS set (sorted ascending by
/// address; validated at construction) and a [`RollupReader`] for
/// on-chain reads. `resolve()` is stateless from the caller's
/// perspective.
///
/// For the single-PS devnet flow, `candidates` is `[ECDSA_PS]`;
/// a future multi-PS quorum mode passes a larger sorted candidate
/// set.
#[derive(Debug, Clone)]
pub struct ProofPlanResolver<R: RollupReader> {
    reader: R,
    candidate_proof_systems: Vec<Address>,
}

/// Construction-time validation errors. Distinct from runtime
/// [`ExecutorError`] because these are caller bugs in setup, not
/// transport / contract failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolverConfigError {
    /// `candidate_proof_systems` was empty. The on-chain
    /// `_validateStructure` rejects zero-PS batches; resolver
    /// rejects this early so the caller sees a clear setup error
    /// instead of a confusing later revert.
    #[error("candidate_proof_systems is empty — provide at least one IProofSystem address")]
    EmptyCandidates,
    /// `candidate_proof_systems` contained `address(0)`. The
    /// on-chain `_validateStructure` rejects this via the
    /// strict-increasing check (zero address is the sentinel).
    #[error("candidate_proof_systems contains the zero address")]
    ZeroAddressCandidate,
    /// `candidate_proof_systems` was not strictly increasing.
    #[error("candidate_proof_systems not strictly increasing at index {index}")]
    NotSorted {
        /// Position of the first non-increasing entry.
        index: usize,
    },
}

impl<R: RollupReader> ProofPlanResolver<R> {
    /// Build a resolver. `candidate_proof_systems` MUST be strictly
    /// increasing by address (matching the on-chain `proofSystems`
    /// ordering invariant) and free of `address(0)`. Returns
    /// [`ResolverConfigError`] if either invariant is violated.
    ///
    /// # Errors
    ///
    /// See [`ResolverConfigError`].
    pub fn new(
        reader: R,
        candidate_proof_systems: Vec<Address>,
    ) -> Result<Self, ResolverConfigError> {
        if candidate_proof_systems.is_empty() {
            return Err(ResolverConfigError::EmptyCandidates);
        }
        for (i, ps) in candidate_proof_systems.iter().enumerate() {
            if *ps == Address::ZERO {
                return Err(ResolverConfigError::ZeroAddressCandidate);
            }
            if i > 0 && candidate_proof_systems[i - 1] >= *ps {
                return Err(ResolverConfigError::NotSorted { index: i });
            }
        }
        Ok(Self {
            reader,
            candidate_proof_systems,
        })
    }

    /// Read-only view of the configured candidate set.
    #[must_use]
    pub fn candidate_proof_systems(&self) -> &[Address] {
        &self.candidate_proof_systems
    }
}

impl<R: RollupReader> ProofPlanResolver<R> {
    /// Resolve the plan for the given touched rollup set. The
    /// returned plan's `rollup_assignments` MUST be sorted by
    /// `rollup_id` ascending (canonical batch order) regardless
    /// of input order.
    ///
    /// # Errors
    ///
    /// Typical failure modes:
    /// - candidate PS not allowed for a touched rollup (manager's
    ///   `checkProofSystemsAndGetVkeys` reverts);
    /// - touched rollup not registered on the central registry;
    /// - on-chain provider transport failure.
    pub async fn resolve(&self, touched: &[RollupId]) -> ExecutorResult<ProofPlan> {
        // Sort + dedup the touched set into canonical batch order.
        // Duplicates are silently collapsed (the on-chain
        // `_validateStructure` would reject duplicate rollupIds
        // with `InvalidProofSystemConfig`, but the caller's input
        // is a "set of touched rollups" not a "list of intent"
        // — dedup here is the natural shape). Empty touched
        // surfaces a typed error below.
        let mut sorted: Vec<RollupId> = {
            let unique: HashSet<RollupId> = touched.iter().copied().collect();
            unique.into_iter().collect()
        };
        sorted.sort_by_key(|r| r.0);
        if sorted.is_empty() {
            return Err(ExecutorError::Unavailable(
                "ProofPlanResolver::resolve: touched set is empty (postBatch \
                 requires at least one rollup per EEZ.sol)"
                    .into(),
            ));
        }

        // Per-rollup on-chain reads. One round-trip per rollup per
        // read; future optimization is multicall batching.
        let n = sorted.len();
        let mut rollup_assignments: Vec<RollupProofAssignment> = Vec::with_capacity(n);
        let mut per_rollup_context: Vec<TimestampAndBlockHash> = Vec::with_capacity(n);
        let mut vk_matrix: Vec<Vec<[u8; 32]>> = Vec::with_capacity(n);

        let candidates = &self.candidate_proof_systems;
        let ps_count = candidates.len();
        // For the single-PS-uniform case (every touched rollup
        // attests with the full candidate set), each rollup's
        // local index list is [0..ps_count) regardless of rollup.
        // A future multi-PS quorum mode where rollups attest with
        // subsets would compute this per-rollup from the manager's
        // allowed set; that's a separate resolver mode.
        let uniform_index: Vec<u64> = (0..ps_count as u64).collect();

        for rid in &sorted {
            let manager = self.reader.rollup_contract(*rid).await?;
            if manager == Address::ZERO {
                return Err(ExecutorError::Unavailable(format!(
                    "EEZ.rollups({}).rollupContract == 0 — rollup not registered",
                    rid.0
                )));
            }
            // checkProofSystemsAndGetVkeys reverts if any candidate
            // isn't in the rollup's allowed set OR if
            // candidates.length < threshold. Both surface as
            // ExecutorError::Provider; the resolver doesn't
            // try to distinguish here.
            let vkeys = self
                .reader
                .check_proof_systems_and_get_vkeys(manager, candidates)
                .await?;
            if vkeys.len() != ps_count {
                return Err(ExecutorError::Decode(format!(
                    "checkProofSystemsAndGetVkeys returned {} vkeys for {} candidates \
                     (rollup_id={})",
                    vkeys.len(),
                    ps_count,
                    rid.0,
                )));
            }
            let context = self.reader.timestamp_and_block_hash(manager).await?;

            rollup_assignments.push(RollupProofAssignment {
                rollup_id: *rid,
                proof_system_index: uniform_index.clone(),
            });
            per_rollup_context.push(context);
            vk_matrix.push(vkeys);
        }

        let plan = ProofPlan {
            proof_systems: candidates.clone(),
            rollup_assignments,
            per_rollup_context,
            vk_matrix,
            cross_proof_system_interactions: [0u8; 32],
        };

        // Belt-and-suspenders: the validator enforces the on-chain
        // structural floor (`_validateStructure`). A malformed
        // plan here is a resolver bug, NOT a provider failure —
        // surface it as `Decode` so callers don't conflate this
        // with an `eth_call` revert. Fail loud per upstream's
        // invariant 7.
        plan.check_invariants().map_err(|e| {
            ExecutorError::Decode(format!(
                "ProofPlanResolver produced an invariant-violating plan: {e}"
            ))
        })?;

        Ok(plan)
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;
    use std::collections::HashMap;

    /// In-memory fake `RollupReader` for tests. Holds:
    /// - registry: rollup_id → manager address (Address::ZERO ⇒
    ///   not registered);
    /// - vkeys: (manager, candidate) → vkey, or `None` ⇒
    ///   `checkProofSystemsAndGetVkeys` reverts;
    /// - context: manager → (timestamp, blockHash).
    #[derive(Debug, Default)]
    struct FakeReader {
        registry: HashMap<u64, Address>,
        vkeys: HashMap<(Address, Address), Option<[u8; 32]>>,
        context: HashMap<Address, TimestampAndBlockHash>,
    }

    #[async_trait]
    impl RollupReader for FakeReader {
        async fn rollup_contract(&self, rid: RollupId) -> ExecutorResult<Address> {
            Ok(self.registry.get(&rid.0).copied().unwrap_or(Address::ZERO))
        }

        async fn check_proof_systems_and_get_vkeys(
            &self,
            rollup_contract: Address,
            candidates: &[Address],
        ) -> ExecutorResult<Vec<[u8; 32]>> {
            let mut out = Vec::with_capacity(candidates.len());
            for ps in candidates {
                match self.vkeys.get(&(rollup_contract, *ps)) {
                    Some(Some(vk)) => out.push(*vk),
                    Some(None) | None => {
                        return Err(ExecutorError::Provider(
                            format!("ProofSystemNotAllowed({ps})").into(),
                        ));
                    }
                }
            }
            Ok(out)
        }

        async fn timestamp_and_block_hash(
            &self,
            rollup_contract: Address,
        ) -> ExecutorResult<TimestampAndBlockHash> {
            Ok(self
                .context
                .get(&rollup_contract)
                .copied()
                .unwrap_or_default())
        }
    }

    const PS_A: Address = address!("00000000000000000000000000000000000000aa");
    const PS_B: Address = address!("00000000000000000000000000000000000000bb");
    const MGR_1: Address = address!("0000000000000000000000000000000000000011");
    const MGR_2: Address = address!("0000000000000000000000000000000000000012");

    fn fake_with_one_rollup() -> FakeReader {
        let mut r = FakeReader::default();
        r.registry.insert(1, MGR_1);
        r.vkeys.insert((MGR_1, PS_A), Some([0x42; 32]));
        r
    }

    // ── Construction ──────────────────────────────────────────

    #[test]
    fn new_rejects_empty_candidates() {
        let r = FakeReader::default();
        let err = ProofPlanResolver::new(r, vec![]).unwrap_err();
        assert_eq!(err, ResolverConfigError::EmptyCandidates);
    }

    #[test]
    fn new_rejects_zero_address_candidate() {
        let r = FakeReader::default();
        let err = ProofPlanResolver::new(r, vec![Address::ZERO]).unwrap_err();
        assert_eq!(err, ResolverConfigError::ZeroAddressCandidate);
    }

    #[test]
    fn new_rejects_unsorted_candidates() {
        let r = FakeReader::default();
        let err = ProofPlanResolver::new(r, vec![PS_B, PS_A]).unwrap_err();
        assert!(matches!(err, ResolverConfigError::NotSorted { index: 1 }));
    }

    #[test]
    fn new_accepts_singleton() {
        let r = FakeReader::default();
        let resolver = ProofPlanResolver::new(r, vec![PS_A]).unwrap();
        assert_eq!(resolver.candidate_proof_systems(), &[PS_A]);
    }

    // ── Happy path ────────────────────────────────────────────

    #[tokio::test]
    async fn single_rollup_single_ps_happy_path() {
        let reader = fake_with_one_rollup();
        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let plan = resolver.resolve(&[RollupId(1)]).await.unwrap();

        assert_eq!(plan.proof_systems, vec![PS_A]);
        assert_eq!(plan.rollup_assignments.len(), 1);
        assert_eq!(plan.rollup_assignments[0].rollup_id, RollupId(1));
        assert_eq!(plan.rollup_assignments[0].proof_system_index, vec![0]);
        assert_eq!(plan.vk_matrix, vec![vec![[0x42; 32]]]);
        assert_eq!(plan.per_rollup_context.len(), 1);
        assert_eq!(plan.cross_proof_system_interactions, [0u8; 32]);

        // check_invariants() was implicitly called by resolve();
        // re-running it should still succeed.
        plan.check_invariants().unwrap();
    }

    // ── Negative paths ────────────────────────────────────────

    #[tokio::test]
    async fn empty_touched_set_fails() {
        let reader = fake_with_one_rollup();
        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let err = resolver.resolve(&[]).await.unwrap_err();
        assert!(matches!(err, ExecutorError::Unavailable(_)));
    }

    #[tokio::test]
    async fn unregistered_rollup_fails_loud() {
        let reader = FakeReader::default();
        // registry empty → rollup_contract returns ZERO
        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let err = resolver.resolve(&[RollupId(42)]).await.unwrap_err();
        assert!(matches!(err, ExecutorError::Unavailable(_)));
    }

    #[tokio::test]
    async fn ps_not_allowed_for_rollup_propagates_manager_revert() {
        let mut reader = FakeReader::default();
        reader.registry.insert(1, MGR_1);
        // MGR_1 doesn't have PS_A in its vkey map → reverts
        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let err = resolver.resolve(&[RollupId(1)]).await.unwrap_err();
        assert!(matches!(err, ExecutorError::Provider(_)));
    }

    // ── Canonical ordering ────────────────────────────────────

    #[tokio::test]
    async fn multi_rollup_unsorted_input_produces_sorted_plan() {
        let mut reader = FakeReader::default();
        reader.registry.insert(1, MGR_1);
        reader.registry.insert(2, MGR_2);
        reader.vkeys.insert((MGR_1, PS_A), Some([0x42; 32]));
        reader.vkeys.insert((MGR_2, PS_A), Some([0x52; 32]));

        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        // Input deliberately unsorted: [2, 1]; output must be [1, 2].
        let plan = resolver.resolve(&[RollupId(2), RollupId(1)]).await.unwrap();

        assert_eq!(plan.rollup_assignments.len(), 2);
        assert_eq!(plan.rollup_assignments[0].rollup_id, RollupId(1));
        assert_eq!(plan.rollup_assignments[1].rollup_id, RollupId(2));
        assert_eq!(plan.vk_matrix[0], vec![[0x42; 32]]);
        assert_eq!(plan.vk_matrix[1], vec![[0x52; 32]]);
        plan.check_invariants().unwrap();
    }

    #[tokio::test]
    async fn duplicate_touched_ids_deduped() {
        let mut reader = FakeReader::default();
        reader.registry.insert(1, MGR_1);
        reader.vkeys.insert((MGR_1, PS_A), Some([0x42; 32]));

        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let plan = resolver
            .resolve(&[RollupId(1), RollupId(1), RollupId(1)])
            .await
            .unwrap();

        assert_eq!(plan.rollup_assignments.len(), 1);
    }

    // ── Multi-PS uniform subset ───────────────────────────────

    #[tokio::test]
    async fn non_zero_timestamp_and_block_hash_pass_through() {
        // Locks the `U256 → [u8; 32]` + `bytes32 → [u8; 32]`
        // conversion the AlloyRollupReader does. Real managers
        // override `getTimestampAndBlockHash`; the resolver MUST
        // round-trip the bytes exactly without narrowing.
        let mut reader = fake_with_one_rollup();
        let ts_big: u128 = 1_778_573_388;
        let mut ts_bytes = [0u8; 32];
        ts_bytes[16..32].copy_from_slice(&ts_big.to_be_bytes());
        let bh_bytes = [0xab; 32];
        reader.context.insert(
            MGR_1,
            TimestampAndBlockHash {
                timestamp: ts_bytes,
                block_hash: bh_bytes,
            },
        );

        let resolver = ProofPlanResolver::new(reader, vec![PS_A]).unwrap();
        let plan = resolver.resolve(&[RollupId(1)]).await.unwrap();

        assert_eq!(plan.per_rollup_context[0].timestamp, ts_bytes);
        assert_eq!(plan.per_rollup_context[0].block_hash, bh_bytes);
        // Spot-check the timestamp bytes ARE the big-endian
        // encoding of the underlying u128 — narrowing through u64
        // would zero the high bits.
        assert_eq!(&plan.per_rollup_context[0].timestamp[0..16], &[0u8; 16]);
        assert_eq!(
            &plan.per_rollup_context[0].timestamp[16..32],
            &ts_big.to_be_bytes(),
        );
    }

    #[tokio::test]
    async fn multi_ps_uniform_subset_per_rollup() {
        let mut reader = FakeReader::default();
        reader.registry.insert(1, MGR_1);
        reader.vkeys.insert((MGR_1, PS_A), Some([0x42; 32]));
        reader.vkeys.insert((MGR_1, PS_B), Some([0x43; 32]));

        let resolver = ProofPlanResolver::new(reader, vec![PS_A, PS_B]).unwrap();
        let plan = resolver.resolve(&[RollupId(1)]).await.unwrap();

        assert_eq!(plan.proof_systems, vec![PS_A, PS_B]);
        assert_eq!(plan.rollup_assignments[0].proof_system_index, vec![0, 1]);
        assert_eq!(plan.vk_matrix[0], vec![[0x42; 32], [0x43; 32]]);
        plan.check_invariants().unwrap();
    }
}
