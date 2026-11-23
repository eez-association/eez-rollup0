//! The batcher: price the L1 post, build and prove the postBatch, sign it, send the bundle.

use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolCall;
use eez_driver::{
    BundleRelease, MAX_BLOCKS_PER_BATCH, RollupTiming, SyncSlotBlock, SyncSlotMode,
    witness::{ExecutionWitnessMode, block_witness},
};
use eez_l1::{BundleTarget, Submitter};
use eez_prover::{ActionableProverFailure, BlockWitness, ProverError, ProvingContext};
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{BlockReader, BlockSource, StateProviderFactory, TransactionsProvider};
use tracing::{Level, event};

use super::blob::{BlobCarrier, blob_stream_len, finish_blob_carrier, start_blob_sidecar};
use super::post_batch::{
    apply_attestations, attestation_error, batch_range, build_da_payload, build_settlement_batch,
    encode_da, postbatch_gas_with_floor,
};
use super::recovery::observe_bundle_outcome;
use super::{BlockGasFit, Composer, ComposerConfigError, CrossChainExecCtx};
use crate::attestation_quorum::assign_proof_systems;
use crate::local::{SyncCandidates, build_sync_block};
use crate::optimistic::OptimisticallyIncluded;
use crate::rollup::RollupState;

pub(super) fn bundle_targets_for_slot(
    target_l1_block: Option<u64>,
    timestamp: u64,
    mode: SyncSlotMode,
) -> (BundleTarget, BundleTarget) {
    let bundle_target = match (target_l1_block, mode) {
        (Some(block), SyncSlotMode::Steady) => BundleTarget::Exact { block, timestamp },
        _ => BundleTarget::NextBlock,
    };
    let historical_target = target_l1_block
        .map(|block| BundleTarget::ExactBlock { block })
        .unwrap_or(BundleTarget::NextBlock);
    (bundle_target, historical_target)
}

/// Stand-in for each proof when sizing before the attesters run. Sized for an
/// ECDSA proof (65 B), the only kind the prover client accepts.
pub(super) const MAX_PROOF_BYTES: usize = 128;

/// Ceiling on a postBatch's gas limit (`EEZ_MAX_POSTBATCH_GAS` overrides): the
/// EIP-7825 per-tx cap, above which no tx is valid at any block gas limit.
const DEFAULT_MAX_POSTBATCH_GAS: u64 = 16_777_216;

/// The belt prices bytes the drain cannot see (ABI framing, proof, DA shape), so
/// the drain must stop earlier or the same set requeues forever. Gap seen: 230k.
const POSTBATCH_DRAIN_MARGIN: u64 = 600_000;

/// Execution for a batch with just the leading immediate entry, calldata aside.
/// Measured 139k, 10% slack; pinned by `contracts/test/PostBatchGasPins.t.sol`.
const POSTBATCH_BASE_GAS_PIN: u64 = 160_000;

/// One more entry costs EEZ.sol hashing, a state SSTORE, and a CREATE2 proxy
/// deploy for a new sender. Same pin test; measured 334k worst case, 10% slack.
const POSTBATCH_ENTRY_GAS_PIN: u64 = 370_000;

/// Each `ECDSAProofSystem` past the first costs a manager vkey read and its
/// verify call. Same pin test; measured 12,264 worst case (sixteen), 10% slack.
const POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN: u64 = 14_000;

/// Execution the extra ECDSA proofs of a batch carrying `proof_systems` add,
/// calldata aside.
pub(super) const fn attestation_gas(proof_systems: u64) -> u64 {
    POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN.saturating_mul(proof_systems.saturating_sub(1))
}

/// Below this the drain of a batch carrying `proof_systems` proofs admits
/// nothing and evicts the first held tx as poison.
const fn min_viable_postbatch_gas(proof_systems: u64) -> u64 {
    projected_postbatch_gas(2, 0, 0)
        .saturating_add(attestation_gas(proof_systems))
        .saturating_add(POSTBATCH_DRAIN_MARGIN)
}

/// EIP-7623 calldata floor — below it the tx is invalid and dies at simulation.
pub(super) fn calldata_floor_gas(calldata: &[u8]) -> u64 {
    let nonzero = calldata.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = calldata.len() as u64 - nonzero;
    21_000 + 10 * (zero + 4 * nonzero)
}

/// Exact standard-rate calldata gas: 4 per zero byte, 16 per non-zero byte.
pub(super) fn calldata_gas(bytes: &[u8]) -> u64 {
    let nonzero = bytes.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = bytes.len() as u64 - nonzero;
    4 * zero + 16 * nonzero
}

/// Whole-postBatch gas on EIP-7623's standard branch: tx base, calldata, pins,
/// probed target calls. The floor branch is checked in `sign_post_batch_tx`.
pub(super) const fn projected_postbatch_gas(
    entry_count: u64,
    calldata_gas: u64,
    target_gas: u64,
) -> u64 {
    21_000_u64
        .saturating_add(POSTBATCH_BASE_GAS_PIN)
        .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(entry_count))
        .saturating_add(calldata_gas)
        .saturating_add(target_gas)
}

/// What one accepted held tx adds to the postBatch projection, per term.
#[derive(Debug, Clone, Copy)]
pub(super) struct TxL1Gas {
    /// `batch.entries` rows it contributes, each costing [`POSTBATCH_ENTRY_GAS_PIN`].
    entries: u64,
    /// Standard-rate gas for every byte it puts in the postBatch calldata.
    calldata_gas: u64,
    /// Probed cost of the target call EEZ.sol runs inline (outbound only).
    target_gas: u64,
}

/// L1 gas one accepted tx adds. `da_entries`/`da_tx` are its DA copies; only
/// outbound pays `target_gas` inline.
fn projected_tx_l1_gas(
    batch_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_tx: &[u8],
    target_gas: u64,
) -> TxL1Gas {
    use alloy_sol_types::SolValue as _;
    let encoded = |entries: &[eez_protocol::abi::ExecutionEntrySol]| -> u64 {
        entries
            .iter()
            .map(|entry| calldata_gas(&entry.abi_encode()))
            .sum()
    };
    TxL1Gas {
        entries: batch_entries.len() as u64,
        calldata_gas: encoded(batch_entries)
            .saturating_add(encoded(da_entries))
            .saturating_add(calldata_gas(da_tx)),
        target_gas,
    }
}

/// Running gas and blob-byte projections, so the drain stops before building
/// a batch that exceeds the L1 transaction gas cap or blob capacity.
#[derive(Debug, Clone, Copy)]
pub(super) struct PostBatchGasBudget {
    pub(super) cap: u64,
    /// Running total, seeded with the leading immediate entry every postBatch
    /// carries. The projection is linear, so costs just add.
    pub(super) projected: u64,
    /// `None` when the batch carries its DA in calldata.
    pub(super) blob: Option<BlobBytes>,
    /// Attestation gas for the configured attesters, priced into every
    /// postBatch projection.
    pub(super) attestation: u64,
}

/// Blob stream bytes: the post's capacity, the bytes reserved so far, and
/// the bytes of an empty interval, the least any later slot reserves.
#[derive(Debug, Clone, Copy)]
pub(super) struct BlobBytes {
    pub(super) limit: usize,
    pub(super) used: usize,
    pub(super) floor: usize,
}

impl PostBatchGasBudget {
    /// A budget for a batch that may carry a proof from each of
    /// `proof_systems` attesters.
    pub(super) fn new(max_gas: u64, proof_systems: u64) -> Self {
        let attestation = attestation_gas(proof_systems);
        Self {
            cap: max_gas.saturating_sub(POSTBATCH_DRAIN_MARGIN),
            projected: projected_postbatch_gas(1, 0, 0).saturating_add(attestation),
            blob: None,
            attestation,
        }
    }

    /// The cost of `cost` as a postBatch of its own, for the cut event.
    pub(super) fn standalone_cost(&self, cost: TxL1Gas) -> u64 {
        projected_postbatch_gas(cost.entries, cost.calldata_gas, cost.target_gas)
            .saturating_add(self.attestation)
    }

    /// L1 gas one accepted tx adds. Its DA copies cost calldata gas only when
    /// the batch carries its DA in calldata, not in blobs.
    pub(super) fn tx_cost(
        &self,
        batch_entries: &[eez_protocol::abi::ExecutionEntrySol],
        da_entries: &[eez_protocol::abi::ExecutionEntrySol],
        da_tx: &[u8],
        target_gas: u64,
    ) -> TxL1Gas {
        if self.blob.is_some() {
            projected_tx_l1_gas(batch_entries, &[], &[], target_gas)
        } else {
            projected_tx_l1_gas(batch_entries, da_entries, da_tx, target_gas)
        }
    }

    /// Charge the blob bytes one held tx adds; only `Accept` charges them.
    pub(super) fn try_accept_da(
        &mut self,
        rollup_id: u64,
        entries: &[eez_protocol::abi::ExecutionEntrySol],
        user: Option<&Bytes>,
    ) -> Result<BlockGasFit, String> {
        let Some(blob) = &mut self.blob else {
            return Ok(BlockGasFit::Accept);
        };
        // A full range cannot admit another held tx; defer it to a later slot.
        if blob.used >= blob.limit {
            return Ok(BlockGasFit::Defer);
        }
        let actions = entries
            .iter()
            .map(|entry| {
                eez_protocol::entries::manifest::action_from_entry(
                    entry,
                    eez_protocol::RollupId(rollup_id),
                )
                .map_err(|e| format!("DA action for drain: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Outbound users travel in blob DA; inbound users are already on L1.
        let block = eez_payload_codec::SpanBlock {
            extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
            transactions: user.into_iter().map(|user| user.to_vec()).collect(),
            ..Default::default()
        };
        // One envelope per candidate conservatively covers aggregate prefix growth.
        let contribution = blob_stream_len(rollup_id, &[block], &actions)?;
        let next = blob.used.saturating_add(contribution);
        Ok(if next <= blob.limit {
            blob.used = next;
            BlockGasFit::Accept
        } else if blob.floor.saturating_add(contribution) > blob.limit {
            // Not even a range holding one empty interval could take it.
            BlockGasFit::Unfittable
        } else {
            BlockGasFit::Defer
        })
    }

    /// Charge `cost` if it fits. `false` leaves the projection untouched, so the
    /// caller can defer this tx and stop draining.
    pub(super) fn try_accept(&mut self, cost: TxL1Gas) -> bool {
        let next = self
            .projected
            .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(cost.entries))
            .saturating_add(cost.calldata_gas)
            .saturating_add(cost.target_gas);
        if next > self.cap {
            return false;
        }
        self.projected = next;
        true
    }
}

/// The Sync slot a batch is built for: the block it extends, its timestamp and
/// fee recipient, and the L1 block its bundle targets.
#[derive(Debug, Clone, Copy)]
pub(super) struct SlotTarget<'a> {
    pub(super) parent_header: &'a reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
    pub(super) timestamp: u64,
    pub(super) fee_recipient: Address,
    pub(super) bundle_target: BundleTarget,
}

/// Emission bounds, resolved once at construction. `timing` is here for its
/// `k()` — the grid the historical chunk boundary snaps to.
#[derive(Debug, Clone, Copy)]
pub(super) struct EmissionLimits {
    pub(super) timing: RollupTiming,
    /// Cap on a batch's block span, `cursor+1 ..= terminal`.
    pub(super) max_blocks: u64,
    /// Gas limit every postBatch is signed with, and the cap the drain uses.
    pub(super) max_gas: u64,
    /// Cap on held transactions attempted in one bundle.
    pub(super) max_user_txs: usize,
    /// Configured attesters: a batch may carry a proof from each.
    pub(super) attesters: u64,
}

impl EmissionLimits {
    pub(super) fn from_env(timing: RollupTiming, attesters: u64) -> Self {
        // Absent, malformed, and zero all mean "unset" — fall back to the default.
        let read_var = |var: &str| {
            std::env::var(var)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&n| n > 0)
        };
        let k = u64::from(timing.k());
        let requested_blocks = read_var("EEZ_MAX_BLOCKS_PER_BATCH");
        let requested_gas = read_var("EEZ_MAX_POSTBATCH_GAS");
        let max_user_txs = read_var("EEZ_MAX_USER_TXS_PER_BUNDLE")
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(50);
        let max_blocks = grid_aligned_cap(requested_blocks.unwrap_or(MAX_BLOCKS_PER_BATCH), k);
        let max_gas = clamp_max_postbatch_gas(
            requested_gas.unwrap_or(DEFAULT_MAX_POSTBATCH_GAS),
            attesters,
        );
        // The one place the effective bounds are visible, adjusted or not.
        event!(
            name: "eez.composer.emission.limits",
            Level::INFO,
            max_blocks,
            max_gas,
            max_user_txs,
            k,
            requested_blocks = ?requested_blocks,
            requested_gas = ?requested_gas,
            "emission bounds: at most {max_blocks} blocks and {max_gas} gas per postBatch",
        );
        Self {
            timing,
            max_blocks,
            max_gas,
            max_user_txs,
            attesters,
        }
    }
}

/// Largest multiple of `k` not above `requested`, never below one `k`.
const fn grid_aligned_cap(requested: u64, k: u64) -> u64 {
    if k == 0 {
        return requested; // RollupTiming::validate rejects K < 2 upstream
    }
    let aligned = requested - requested % k;
    if aligned == 0 { k } else { aligned }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum PreparePostBatchError {
    #[error("{0}")]
    Build(String),
    #[error("prover.prove: {0}")]
    Prover(#[source] ProverError),
    #[error("prover rejected {0}")]
    Actionable(ActionableProverFailure),
}

impl From<String> for PreparePostBatchError {
    fn from(error: String) -> Self {
        Self::Build(error)
    }
}

impl From<&str> for PreparePostBatchError {
    fn from(error: &str) -> Self {
        Self::Build(error.to_owned())
    }
}

/// Both ends are unusable and clamp to the default: above it no tx is valid
/// (EIP-7825), and below [`min_viable_postbatch_gas`] for `attesters` proofs
/// every emission path refuses forever.
fn clamp_max_postbatch_gas(requested: u64, attesters: u64) -> u64 {
    let min_viable = min_viable_postbatch_gas(attesters);
    if (min_viable..=DEFAULT_MAX_POSTBATCH_GAS).contains(&requested) {
        return requested;
    }
    event!(
        name: "eez.composer.emission.gas_budget_clamped",
        Level::ERROR,
        requested,
        clamped_to = DEFAULT_MAX_POSTBATCH_GAS,
        min_viable,
        "EEZ_MAX_POSTBATCH_GAS is out of range (must leave room for the drain margin plus one held tx, and stay within the EIP-7825 tx gas cap) — clamping to the default",
    );
    DEFAULT_MAX_POSTBATCH_GAS
}

pub(super) fn ensure_submission_identity(
    submitter_poster: Address,
    post_batch_signer: Address,
) -> Result<(), ComposerConfigError> {
    if submitter_poster != post_batch_signer {
        return Err(ComposerConfigError::SubmissionIdentityMismatch {
            submitter_poster,
            post_batch_signer,
        });
    }
    Ok(())
}

impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Send
        + Sync
        + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Build an empty Sync block, sign a leading-immediate-only postBatch
    /// covering `posted+1..=sync_height`, dispatch it to the background
    /// observer, and return the block for immediate commit. The Sync block is
    /// the last block of the batch range.
    ///
    /// If even the minimal postBatch can't be prepared, the block is
    /// still returned (commit-without-emission) — L2 cadence is
    /// unconditional; L1 catches up via the next emitted batch.
    pub(super) async fn dispatch_minimal_postbatch(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        slot: SlotTarget<'_>,
    ) -> Result<Option<SyncSlotBlock>, String> {
        let SlotTarget {
            parent_header,
            timestamp,
            fee_recipient: suggested_fee_recipient,
            bundle_target,
        } = slot;
        let empty_built = match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &[], // no system_txs — empty Sync block
        ) {
            Ok(built) => built,
            Err(err) => {
                event!(
                    name: "eez.composer.phase1.build_failed",
                    Level::ERROR,
                    rollup_id,
                    error = %err,
                    "minimal Sync block build failed; Sequencer's commit_one fallback takes over",
                );
                return Ok(None);
            }
        };

        let minimal_postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &[], // no compositions → leading immediate only
                parent_header,
                &empty_built.header,
                Some(&empty_built.block),
                &SyncCandidates {
                    anchor: empty_built.header.hash(),
                    per_effect: Vec::new(),
                },
                &[], // no outbound entries
                &[], // no outbound user txs
                0,   // no inline outbound target calls
                bundle_target,
                None,
            )
            .await
            .and_then(|raw| {
                raw.ok_or_else(|| {
                    PreparePostBatchError::Build(format!(
                        "range exceeds the {} postBatch gas budget; bounded chunks cover it",
                        self.inner.emission.max_gas
                    ))
                })
            }) {
            Ok(raw) => raw,
            Err(err) => {
                // The whole range will not fit; settle a bounded prefix of it so
                // the range shrinks and the next slot's rich batch can fit.
                let cursor = rollup.l1_head.cursor();
                let sync_height = parent_header.number() + 1;
                if let Err(chunk_err) = self
                    .emit_historical_chunk(
                        ctx,
                        rollup_id,
                        rollup,
                        cursor,
                        sync_height,
                        BundleTarget::NextBlock,
                    )
                    .await
                {
                    event!(
                        name: "eez.composer.emission.prefix_fallback_failed",
                        Level::ERROR,
                        rollup_id,
                        cursor,
                        sync_height,
                        error = %chunk_err,
                        "neither the full range nor a bounded prefix could be emitted",
                    );
                }
                event!(
                    name: "eez.composer.phase1.prepare_failed",
                    Level::ERROR,
                    rollup_id,
                    error = %err,
                    "minimal postBatch prepare failed; committing Sync block without emission — L1 catches up next slot",
                );
                return Ok(Some(SyncSlotBlock {
                    payload: empty_built.payload,
                    header: empty_built.header,
                    release: None,
                }));
            }
        };
        let sync_height = empty_built.header.number();
        let SignedPostBatch {
            raw: minimal_postbatch_raw,
            hash: post_batch_hash,
        } = minimal_postbatch_raw;
        event!(
            name: "eez.composer.phase1.bundle.staged",
            Level::INFO,
            event_name = "eez.composer.phase1.bundle.staged",
            rollup_id,
            sync_height,
            post_batch_hash = %post_batch_hash,
            "minimal postBatch staged (leading immediate only); sent when the Sync block commits",
        );
        rollup.optimistic.begin(
            sync_height,
            post_batch_hash,
            parent_header.clone(),
            empty_built.header.hash(),
            Vec::new(),
        );
        let (release, released) = BundleRelease::new();
        self.spawn_bundle_observer(
            ctx,
            rollup_id,
            sync_height,
            vec![minimal_postbatch_raw],
            post_batch_hash,
            empty_built.header.hash(),
            Arc::clone(&rollup.optimistic),
            bundle_target,
            Some(Staged {
                released,
                dispatched: Dispatched::Minimal,
            }),
        );
        Ok(Some(SyncSlotBlock {
            payload: empty_built.payload,
            header: empty_built.header,
            release: Some(release),
        }))
    }

    /// A leading-immediate-only postBatch ending at a PAST on-grid empty block.
    /// `cursor` must be the emission decision's read, not a fresh one.
    pub(super) async fn emit_historical_chunk(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
        sync_height: u64,
        historical_target: BundleTarget,
    ) -> Result<(), String> {
        let limits = self.inner.emission;
        let k = u64::from(limits.timing.k());
        // Under the cap the cap-bounded boundary is the sync height itself, which
        // is not a PAST block — step one K down so a range too big can still shrink.
        let snapped = limits
            .timing
            .historical_chunk_boundary(cursor, sync_height, limits.max_blocks)
            .unwrap_or_else(|| sync_height.saturating_sub(k));
        if snapped <= cursor {
            return Err(format!(
                "no on-grid boundary above cursor {cursor} below sync height {sync_height}"
            ));
        }
        // Candidates highest-first; each is priced exactly by
        // `prepare_post_batch_raw`, which returns `Ok(None)` when the range does
        // not fit the gas or blob budget — then step back a whole K and re-prepare.
        // The gate precedes witness assembly and proving, so a rejected
        // candidate costs one block walk plus one encode.
        let candidates = self.empty_chunk_boundaries(rollup, cursor, snapped)?;
        let provider = rollup.l2_provider.as_ref();
        let mut over_budget = 0usize;
        for boundary in candidates {
            let boundary_header = provider
                .sealed_header(boundary)
                .map_err(|e| format!("sealed_header({boundary}): {e}"))?
                .ok_or_else(|| format!("local L2 header at boundary {boundary} missing"))?;
            let boundary_parent = provider
                .sealed_header(boundary - 1)
                .map_err(|e| format!("sealed_header({}): {e}", boundary - 1))?
                .ok_or_else(|| format!("local L2 header at {} missing", boundary - 1))?;

            let Some(raw) = self
                .prepare_post_batch_raw(
                    ctx,
                    rollup_id,
                    &[], // no compositions → leading immediate only
                    &boundary_parent,
                    &boundary_header,
                    None, // terminal is committed → witnesses come from the store
                    &SyncCandidates {
                        anchor: boundary_header.hash(),
                        per_effect: Vec::new(),
                    },
                    &[],
                    &[],
                    0, // no inline outbound target calls
                    historical_target,
                    None,
                )
                .await
                .map_err(|error| error.to_string())?
            else {
                over_budget += 1;
                continue;
            };

            let SignedPostBatch {
                raw,
                hash: post_batch_hash,
            } = raw;
            event!(
                name: "eez.composer.emission.historical_chunk",
                Level::INFO,
                event_name = "eez.composer.emission.historical_chunk",
                rollup_id,
                cursor,
                boundary,
                sync_height,
                span = boundary - cursor,
                backlog = sync_height - cursor,
                over_budget,
                post_batch_hash = %post_batch_hash,
                "settlement backlog exceeds the batch cap; settling a bounded historical chunk",
            );

            rollup.optimistic.begin(
                boundary,
                post_batch_hash,
                boundary_parent,
                boundary_header.hash(),
                Vec::new(),
            );
            // A past terminal cannot carry a timestamp pin, but the bundle can
            // still target the exact L1 slot that triggered this composition.
            // Its terminal is committed, so it sends at once.
            self.spawn_bundle_observer(
                ctx,
                rollup_id,
                boundary,
                vec![raw],
                post_batch_hash,
                boundary_header.hash(),
                Arc::clone(&rollup.optimistic),
                historical_target,
                None,
            );
            return Ok(());
        }
        Err(format!(
            "every empty on-grid boundary in ({cursor}, {snapped}] exceeds the {} gas budget \
             ({over_budget} candidates priced); retrying next slot",
            self.inner.emission.max_gas,
        ))
    }

    /// On-grid heights in `(cursor, snapped]` whose block is EMPTY, highest
    /// first. Empty because the prover binds every settling-block tx to a
    /// claimed entry, so a tx-bearing terminal is unattestable for an
    /// anchor-only batch (§7c). Gas is NOT considered here — the caller prices
    /// each candidate on its fully encoded batch.
    fn empty_chunk_boundaries(
        &self,
        rollup: &RollupState<L2>,
        cursor: u64,
        snapped: u64,
    ) -> Result<Vec<u64>, String> {
        let k = u64::from(self.inner.emission.timing.k());
        let provider = rollup.l2_provider.as_ref();
        // Walk parent hashes rather than numbers: `block_by_number` races reth's
        // provider index for the newest block.
        let mut empty_at: Vec<bool> = Vec::new();
        let mut hash = provider
            .sealed_header(snapped)
            .map_err(|e| format!("sealed_header({snapped}): {e}"))?
            .ok_or_else(|| format!("local L2 header at {snapped} missing"))?
            .hash();
        let mut number = snapped;
        while number > cursor {
            let header = provider
                .sealed_header_by_hash(hash)
                .map_err(|e| format!("sealed_header_by_hash({hash}, n={number}): {e}"))?
                .ok_or_else(|| format!("local L2 header {hash} (n={number}) missing"))?;
            empty_at
                .push(header.transactions_root() == alloy_consensus::constants::EMPTY_ROOT_HASH);
            hash = header.parent_hash();
            number -= 1;
        }
        let step = usize::try_from(k).map_err(|e| format!("K overflows usize: {e}"))?;
        if step == 0 {
            return Err("K is zero".to_string());
        }
        let boundaries: Vec<u64> = (0..empty_at.len())
            .step_by(step)
            .filter(|&i| empty_at[i])
            .map(|i| snapped - i as u64)
            .collect();
        if boundaries.is_empty() {
            return Err(format!(
                "no empty on-grid boundary in ({cursor}, {snapped}]; retrying next slot"
            ));
        }
        Ok(boundaries)
    }

    /// Spawn the background bundle-observer task. It owns the submission inputs
    /// and records only the verdict in the optimistic ledger; it holds neither
    /// the Composer nor the block committer, so chain recovery stays in slot
    /// context through `recover_failed_batch`. A `staged` bundle is sent only
    /// once its Sync block commits.
    pub(super) fn spawn_bundle_observer(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        sync_height: u64,
        bundle: Vec<Bytes>,
        post_batch_hash: alloy_primitives::TxHash,
        expected_final_root: B256,
        optimistic: Arc<OptimisticallyIncluded>,
        target: BundleTarget,
        staged: Option<Staged>,
    ) {
        tokio::spawn(send_after_commit(
            rollup_id,
            sync_height,
            bundle,
            post_batch_hash,
            expected_final_root,
            optimistic,
            ctx.submitter.clone(),
            target,
            staged,
            tokio::time::Instant::now(),
        ));
    }

    /// Read committed DA from the parent's ancestry, including pending blocks.
    /// Drain sizing and final encoding must share the same system-tx guard.
    pub(super) fn committed_da_blocks(
        &self,
        rollup: &RollupState<L2>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        from: u64,
    ) -> Result<Vec<eez_payload_codec::SpanBlock>, String> {
        let mut blocks_rev = Vec::new();
        let mut cursor_hash = parent_header.hash();
        let mut cursor_number = parent_header.number();
        while cursor_number >= from {
            // `BlockSource::Any` so the lookup finds the parent even
            // while it's still "pending" in reth: at compose_sync_slot
            // time the Sequencer has done `newPayload(parent)` but the
            // promoting FCU fires on the next commit, so the parent is in
            // reth's tree but not yet canonical-head. Deeper ancestors
            // are already canonical, so `Any` finds them too.
            let block = rollup
                .l2_provider
                .find_block_by_hash(cursor_hash, BlockSource::Any)
                .map_err(|e| {
                    format!("l2_provider.find_block_by_hash({cursor_hash}, n={cursor_number}): {e}")
                })?
                .ok_or_else(|| {
                    format!("local L2 block hash {cursor_hash} (n={cursor_number}) missing")
                })?;
            let tx_bytes: Vec<Vec<u8>> = block
                .body()
                .transactions()
                .iter()
                .map(Encodable2718::encoded_2718)
                .collect();
            // Intermediate blocks may contain only user transactions; system
            // transactions belong to the Sync block. Reject both typed and
            // SYSTEM_ADDRESS-signed forms so an unrecovered failed Sync block
            // cannot reintroduce phantom cross-chain effects.
            for enc in &tx_bytes {
                let is_system = if enc.first() == Some(&eez_primitives::SYSTEM_TX_TYPE) {
                    true
                } else {
                    use alloy_eips::eip2718::Decodable2718 as _;
                    use reth_primitives_traits::SignerRecoverable as _;
                    let mut raw: &[u8] = enc.as_slice();
                    let tx = reth_ethereum_primitives::TransactionSigned::decode_2718(&mut raw)
                        .map_err(|e| {
                            format!(
                                "system-tx guard: decode_2718 failed for a tx in \
                                 intermediate block {cursor_number}: {e}"
                            )
                        })?;
                    let signer = tx.recover_signer().map_err(|e| {
                        format!(
                            "system-tx guard: recover_signer failed for a tx in \
                             intermediate block {cursor_number}: {e}"
                        )
                    })?;
                    signer == eez_primitives::SYSTEM_ADDRESS
                };
                if is_system {
                    return Err(format!(
                        "intermediate block {cursor_number} carries a system transaction; \
                         emission is blocked until the failed Sync block is recovered"
                    ));
                }
            }
            // Beneficiary and extraData are per-block choices, so the DA
            // carries them rather than derivation assuming a constant.
            blocks_rev.push(eez_payload_codec::SpanBlock {
                beneficiary: block.header().beneficiary().into(),
                extra_data: block.header().extra_data().to_vec(),
                transactions: tx_bytes,
            });
            if cursor_number == 0 {
                break;
            }
            cursor_hash = block.header().parent_hash();
            cursor_number -= 1;
        }
        blocks_rev.reverse();
        Ok(blocks_rev)
    }

    /// Build + sign the L1 `postBatch` raw tx for a Sync slot's
    /// compositions, covering blocks `posted+1..=parent+1` (intermediate
    /// blocks + the new Sync block at `parent+1`, always the range's last
    /// block). Returns only the signed EIP-2718 `postBatch` bytes; the caller
    /// appends surviving inbound user transactions to the L1 bundle.
    ///
    /// A later `postAndVerifyBatch` for the same rollup and L1 block replaces the
    /// earlier deferred queue. The composer therefore merges one Sync slot into
    /// one batch so no earlier entries are lost. Entries are ordered as the
    /// leading anchor, outbound immediate entries in drain order, then inbound
    /// deferred entries in drain order. One proof covers the merged batch.
    ///
    /// **Chained state updates**: this function
    /// stitches the merged entries so `entries[k].currentRoot ==
    /// entries[k-1].newRoot` per rollup; EEZ.sol's `_applyRollupUpdates`
    /// enforces the chain on L1 (`StateRootMismatch` revert) regardless of
    /// proof system. Each effect entry's `newRoot` is its per-effect root from
    /// `candidates.per_effect` (verified by the proof signer's effect-prefix
    /// checks); the anchor claims `candidates.anchor`, so every entry
    /// commits to a candidate at the Sync block's height. The
    /// last is the terminal block's own hash, which is the required
    /// settlement-chain endpoint and comes from `terminal_header`.
    ///
    /// `sync_block` is the terminal, `Some` only while freshly built; `None`
    /// (historical chunk) takes its witness from the store like the rest.
    ///
    /// `outbound_target_gas`: probed cost of the target calls EEZ.sol runs
    /// inline, which calldata size cannot predict.
    /// # Errors
    ///
    /// Returns an error for missing chain data, inconsistent IDs or roots,
    /// malformed batch ranges, witness or proof failure, transaction signing,
    /// or RPC data required to build the submission.
    pub(super) async fn prepare_post_batch_raw(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        compositions: &[&eez_protocol::Composition],
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        terminal_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        sync_block: Option<&reth_primitives_traits::RecoveredBlock<eez_primitives::Block>>,
        candidates: &SyncCandidates,
        outbound_entries: &[eez_protocol::abi::ExecutionEntrySol],
        outbound_user_txs: &[Bytes],
        outbound_target_gas: u64,
        bundle_target: BundleTarget,
        l1_context: Option<eez_protocol::l1_context::LiveSettlementContext>,
    ) -> Result<Option<SignedPostBatch>, PreparePostBatchError> {
        use eez_protocol::abi::postAndVerifyBatchCall;

        let sync_block_hash = terminal_header.hash();

        // The L1-confirmed cursor block anchors the batch: its hash is the
        // leading immediate entry's `currentRoot` and `posted + 1` starts the
        // covered range.
        let rollup =
            self.inner.rollups.get(&rollup_id).ok_or_else(|| {
                format!("unknown rollup_id {rollup_id} in prepare_post_batch_raw")
            })?;
        let posted = rollup.l1_head.cursor();
        let (pre_block_hash, pre_state_root): (B256, B256) = {
            let h = rollup
                .l2_provider
                .sealed_header(posted)
                .map_err(|e| format!("sealed_header({posted}): {e}"))?
                .ok_or_else(|| format!("local L2 header at {posted} missing"))?;
            (h.hash(), h.state_root())
        };

        let mut batch = build_settlement_batch(
            rollup_id,
            compositions,
            outbound_entries,
            pre_block_hash,
            candidates,
            sync_block_hash,
            l1_context.is_some(),
        )?;
        // Every configured attester until the quorum answers; the settled
        // batch then names exactly the proof systems whose proofs it carries.
        assign_proof_systems(
            &mut batch,
            rollup_id,
            self.inner.quorum.proof_systems().collect(),
        );
        // Encode the full L2 block range this batch covers, not just the
        // Sync block: the composer accumulates intermediate live blocks between
        // Sync slots and the deriver must replay all of them. Range:
        // from = cursor+1 (first unposted L2
        // block), to = parent+1 (the Sync block).
        //
        // Intermediate blocks [from..to-1] are walked via parent-hash
        // from `parent_header` — `block_by_hash` is reliable for
        // canonical blocks, unlike `block_by_number`, which races reth's
        // provider index for the newest block. The Sync-block DA carries
        // outbound user transactions; its system transactions are reconstructed
        // by the deriver from the postBatch entries.
        // Reuse the SAME cursor read that anchored the leading
        // immediate's currentRoot above — a second read could race the
        // Deriver's cursor advance and desync the callData range from
        // the state-update anchor (TOCTOU).
        let (from, sync_block_number, span) = batch_range(
            posted,
            parent_header.number(),
            self.inner.emission.max_blocks,
        )?;
        let committed = self.committed_da_blocks(rollup, parent_header, from)?;
        let (blocks, actions) = build_da_payload(
            rollup_id,
            committed,
            terminal_header,
            outbound_user_txs,
            compositions,
            outbound_entries,
        )?;
        let physical_blobs = encode_da(
            ctx,
            self.inner.emission.timing.l1_block_time(),
            bundle_target,
            rollup_id,
            &blocks,
            &actions,
            &mut batch,
        )
        .await?;

        // Priced on the encoded candidate before witnesses and proving; `Ok(None)`
        // is over-budget, not an error, and the caller chooses what to do.
        let ceiling = self.inner.emission.max_gas;
        let (projected_gas, calldata_bytes) = postbatch_gas_with_floor(
            &batch,
            self.inner.quorum.len(),
            outbound_target_gas,
            self.inner.emission.attesters,
        );
        let over_blobs = physical_blobs
            .as_ref()
            .is_some_and(|physical| physical.blobs.len() > physical.limit.max_blobs);
        if projected_gas > ceiling || over_blobs {
            event!(
                name: "eez.composer.emission.candidate_over_budget",
                Level::DEBUG,
                rollup_id,
                from,
                to = sync_block_number,
                projected_gas,
                ceiling,
                calldata_bytes,
                blobs = batch.blobIndices.len(),
                "candidate range exceeds the postBatch gas or blob budget",
            );
            return Ok(None);
        }

        let (blobs, pending_sidecar) = start_blob_sidecar(&ctx.l1_provider, physical_blobs).await?;
        // Prove the assembled window (proofs[] empty — not part of the
        // publicInputsHash). Mock ignores the context; a remote prover re-executes
        // `blocks`. Settlement path, off block production.
        // The manager's attestation settings are read while the witnesses build.
        let (block_witnesses, round) = tokio::join!(
            self.window_witnesses(rollup_id, from, sync_block_number, sync_block),
            self.inner
                .quorum
                .round(&ctx.l1_provider, ctx.eez_registry, rollup_id),
        );
        let block_witnesses = block_witnesses?;
        // Unreadable or unreachable attestation settings are not a candidate's
        // fault: requeue without charging.
        let round = round.map_err(|error| error.to_string())?;
        let proving_ctx = ProvingContext {
            rollup_id,
            from_block: from,
            to_block: sync_block_number,
            anchor: eez_prover::ProvingAnchor {
                number: posted,
                hash: pre_block_hash,
                state_root: pre_state_root,
            },
            batch: batch.clone(),
            blocks: block_witnesses,
            l1_context,
            blobs,
            verification_key: None,
        };
        let attestations = round
            .attest(
                &proving_ctx,
                self.inner.emission.timing,
                bundle_target,
                self.inner.clock.as_ref(),
            )
            .await
            .map_err(|error| attestation_error(rollup_id, error, &batch, sync_block))?;
        apply_attestations(&mut batch, rollup_id, attestations);

        let calldata = postAndVerifyBatchCall {
            batch: batch.clone(),
        }
        .abi_encode();

        // Log both settlement anchors. `StateRootMismatch` means
        // `current_root` disagreed with L1's registered root at submission;
        // an over-budget `floor_gas` names the too-fat case.
        event!(
            name: "eez.composer.postbatch.anchors",
            Level::INFO,
            rollup_id,
            posted,
            from,
            sync_block_number,
            span,
            calldata_bytes = calldata.len(),
            floor_gas = calldata_floor_gas(&calldata),
            current_root = %pre_block_hash,
            claimed_final = %sync_block_hash,
            "postBatch anchors: cursor block hash, claimed final Sync block hash",
        );

        let carrier = finish_blob_carrier(pending_sidecar).await?;
        Ok(sign_post_batch_tx(
            &ctx.l1_poster_signer,
            &ctx.l1_provider,
            ctx.eez_registry,
            calldata,
            ctx.l1_chain_id,
            ctx.l1_post_batch_priority_fee,
            self.inner.emission.max_gas,
            carrier,
        )
        .await
        .map(Some)?)
    }

    /// The witnesses of blocks `from..=to` the attesters re-execute.
    async fn window_witnesses(
        &self,
        rollup_id: u64,
        from: u64,
        to: u64,
        sync_block: Option<&reth_primitives_traits::RecoveredBlock<eez_primitives::Block>>,
    ) -> Result<Vec<BlockWitness>, PreparePostBatchError> {
        // Tests may use a lightweight prover without a witness source.
        let Some(src) = self.inner.witness_source.as_ref() else {
            return Ok(Vec::new());
        };
        // Remote-prover mode. Intermediate blocks `[from..to)` are committed
        // (served by the witness store); a freshly-built endpoint isn't, so
        // capture it here from the in-memory block.
        // Witness generation is a CPU-heavy trie walk / re-exec. Run it on
        // the blocking pool so it can't stall async worker threads on the
        // settlement path. (Store hits are cheap; the rare store miss and
        // the endpoint capture are the heavy parts.)
        let src = Arc::clone(src);
        let l2_provider = Arc::clone(
            &self
                .inner
                .rollups
                .get(&rollup_id)
                .ok_or_else(|| format!("unknown rollup_id {rollup_id}"))?
                .l2_provider,
        );
        let evm_config = self.inner.evm_config.clone();
        let terminal_block = sync_block.cloned();
        let witnesses =
            tokio::task::spawn_blocking(move || -> Result<Vec<BlockWitness>, String> {
                let mut ws = (from..to)
                    .map(|n| src.block_witness(n))
                    .collect::<Result<Vec<_>, String>>()
                    .map_err(|e| format!("witness_source: {e}"))?;
                match terminal_block {
                    // Just-built: nothing can serve an uncommitted block, and
                    // its parent state is hot, so re-execute in-memory.
                    Some(block) => ws.push(
                        block_witness(
                            l2_provider.as_ref(),
                            &evm_config,
                            &block,
                            ExecutionWitnessMode::Legacy,
                        )
                        .map_err(|e| {
                            format!(
                                "terminal-block witness (block {}): {e}",
                                block.header().number()
                            )
                        })?,
                    ),
                    // Committed (historical chunk): from the store — an
                    // in-memory capture needs a possibly-pruned parent state.
                    None => ws.push(
                        src.block_witness(to)
                            .map_err(|e| format!("witness_source: {e}"))?,
                    ),
                }
                Ok(ws)
            })
            .await
            .map_err(|e| format!("witness spawn_blocking join: {e}"))??;
        Ok(witnesses)
    }
}

/// Refuse to settle a batch carrying any `destinationRollupId` / `sourceRollupId`
/// that isn't this rollup's registry id — a wiring bug (e.g. an outbound entry
/// whose `dest` stayed at the call's MAINNET(0) target) that L1 would misattribute
/// and that folds into the `publicInputsHash`. Guards the outbound `dest=rid` rewrite.
pub(super) fn ensure_batch_registry_native(
    batch: &eez_protocol::EvmBatch,
    expected_rollup_id: u64,
) -> Result<(), String> {
    for (i, entry) in batch.entries.iter().enumerate() {
        if entry.destinationRollupId != expected_rollup_id {
            return Err(format!(
                "entry[{i}].destinationRollupId = {} is not the configured registry id {expected_rollup_id} — \
                 a non-registry id reached the settlement batch (composition must be registry-native)",
                entry.destinationRollupId,
            ));
        }
        for (j, call) in entry.l2ToL1Calls.iter().enumerate() {
            if call.sourceRollupId != expected_rollup_id {
                return Err(format!(
                    "entry[{i}].l2ToL1Calls[{j}].sourceRollupId = {} is not the configured registry id {expected_rollup_id}",
                    call.sourceRollupId,
                ));
            }
        }
    }
    Ok(())
}

/// A bundle waiting for its Sync block to commit.
pub(super) struct Staged {
    /// Receives the hash of the block committed at the Sync height.
    pub(super) released: tokio::sync::oneshot::Receiver<B256>,
    /// The event the bundle logs when it is sent.
    pub(super) dispatched: Dispatched,
}

/// The `dispatched` event of a staged bundle, with the fields known at staging.
#[derive(Debug, Clone, Copy)]
pub(super) enum Dispatched {
    /// `eez.composer.bundle.dispatched`: a rich Sync block's bundle.
    Rich {
        entry_count: usize,
        evicted_poison: usize,
        evicted_stale: usize,
    },
    /// `eez.composer.phase1.bundle.dispatched`: a minimal postBatch.
    Minimal,
}

/// Send the bundle once its Sync block commits, then observe it. A bundle
/// whose block is not committed is never sent, so nothing is charged.
async fn send_after_commit(
    rollup_id: u64,
    sync_height: u64,
    bundle: Vec<Bytes>,
    post_batch_hash: alloy_primitives::TxHash,
    expected_final_root: B256,
    optimistic: Arc<OptimisticallyIncluded>,
    submitter: Submitter,
    target: BundleTarget,
    staged: Option<Staged>,
    staged_at: tokio::time::Instant,
) {
    let dispatched = match staged {
        Some(Staged {
            released,
            dispatched,
        }) => {
            if !await_commit(rollup_id, sync_height, expected_final_root, released).await {
                optimistic.mark_failed(sync_height, post_batch_hash, true);
                return;
            }
            Some((dispatched, staged_at.elapsed()))
        }
        // A historical chunk is sent at once; `emit_historical_chunk` logged its one event.
        None => None,
    };
    let tx_count = bundle.len();
    let on_dispatch = move || {
        if let Some((dispatched, commit_wait)) = dispatched {
            log_dispatched(
                rollup_id,
                sync_height,
                post_batch_hash,
                tx_count,
                dispatched,
                commit_wait,
            );
        }
    };
    observe_bundle_outcome(
        rollup_id,
        sync_height,
        bundle,
        post_batch_hash,
        expected_final_root,
        optimistic,
        submitter,
        target,
        on_dispatch,
    )
    .await;
}

/// Log that a staged bundle goes out, `commit_wait` after it was staged.
fn log_dispatched(
    rollup_id: u64,
    sync_height: u64,
    post_batch_hash: alloy_primitives::TxHash,
    tx_count: usize,
    dispatched: Dispatched,
    commit_wait: std::time::Duration,
) {
    let commit_wait_ms = u64::try_from(commit_wait.as_millis()).unwrap_or(u64::MAX);
    match dispatched {
        Dispatched::Rich {
            entry_count,
            evicted_poison,
            evicted_stale,
        } => event!(
            name: "eez.composer.bundle.dispatched",
            Level::INFO,
            event_name = "eez.composer.bundle.dispatched",
            rollup_id,
            sync_height,
            post_batch_hash = %post_batch_hash,
            tx_count,
            entry_count,
            evicted_poison,
            evicted_stale,
            commit_wait_ms,
            "Sync block committed; sending its rich bundle",
        ),
        Dispatched::Minimal => event!(
            name: "eez.composer.phase1.bundle.dispatched",
            Level::INFO,
            event_name = "eez.composer.phase1.bundle.dispatched",
            rollup_id,
            sync_height,
            post_batch_hash = %post_batch_hash,
            commit_wait_ms,
            "Sync block committed; sending its minimal postBatch",
        ),
    }
}

/// Wait until the Sync block `expected` is committed. `false` withholds the
/// bundle: the block was not committed, or another block was.
async fn await_commit(
    rollup_id: u64,
    sync_height: u64,
    expected: B256,
    released: tokio::sync::oneshot::Receiver<B256>,
) -> bool {
    match released.await {
        Ok(committed) if committed == expected => true,
        Ok(committed) => {
            event!(
                name: "eez.composer.bundle.withheld",
                Level::ERROR,
                rollup_id,
                sync_height,
                expected = %expected,
                committed = %committed,
                "a different block was committed for this bundle; withholding it",
            );
            false
        }
        Err(_) => {
            event!(
                name: "eez.composer.bundle.withheld",
                Level::WARN,
                rollup_id,
                sync_height,
                "Sync block was not committed; withholding its bundle",
            );
            false
        }
    }
}

/// A signed postBatch and the hash L1 reports for it, which excludes a blob
/// post's sidecar.
pub(super) struct SignedPostBatch {
    pub(super) raw: Bytes,
    pub(super) hash: B256,
}

/// Sign an EIP-1559 or blob L1 transaction for `postAndVerifyBatch` submission.
///
/// Sets `max_priority_fee_per_gas` from the caller (so we can order
/// the postBatch ahead of the held user_tx) and `max_fee_per_gas` to
/// `2 * base_fee + priority_fee` per the standard EIP-1559 formula.
///
/// # Errors
///
/// Returns a `String` error if the pending-nonce or latest-block RPC fails, or
/// if transaction signing fails.
async fn sign_post_batch_tx(
    signer: &alloy_signer_local::PrivateKeySigner,
    provider: &alloy_provider::RootProvider,
    eez_address: Address,
    calldata: Vec<u8>,
    chain_id: u64,
    priority_fee: u128,
    max_gas: u64,
    blob: Option<BlobCarrier>,
) -> Result<SignedPostBatch, String> {
    use alloy_consensus::{SignableTransaction as _, TxEip1559, TxEip4844, TxEip4844WithSidecar};
    use alloy_eips::BlockNumberOrTag;
    use alloy_network::TxSignerSync;
    use alloy_primitives::TxKind;
    use alloy_provider::Provider as _;
    use reth_ethereum_primitives::{Transaction, TransactionSigned};

    let from = signer.address();
    let nonce = provider
        .get_transaction_count(from)
        .pending()
        .await
        .map_err(|e| format!("get_transaction_count({from}): {e}"))?;

    // Dev-reth base fee is 0 in practice; pull it anyway so this
    // path stays correct on non-dev chains.
    let latest = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .map_err(|e| format!("get_block latest: {e}"))?
        .ok_or_else(|| "get_block latest: None".to_string())?;
    let base_fee = u128::from(latest.header.base_fee_per_gas.unwrap_or(0));
    let max_fee_per_gas = base_fee.saturating_mul(2).saturating_add(priority_fee);

    // Only gas USED is charged, so sign at the ceiling and let the drain's
    // projection bound the batch.
    let gas_limit = max_gas;
    // A calldata floor over the cap is invalid at ANY gas limit. Refusing means
    // no emission this slot; bounded chunks cover the range later.
    let floor_gas = calldata_floor_gas(&calldata);
    if floor_gas > max_gas {
        return Err(format!(
            "postBatch calldata floor {floor_gas} exceeds EEZ_MAX_POSTBATCH_GAS {max_gas} \
             ({} calldata bytes); refusing emission — bounded chunks cover the range",
            calldata.len(),
        ));
    }

    // Diagnostic only — from the outside a drained poster looks exactly like a
    // dead relay. Don't abort; the tx still lands if the base fee settles.
    let blob_reserve = blob.as_ref().map_or(U256::ZERO, |blob| {
        let blob_gas = blob.sidecar.blobs().len() as u64 * alloy_eips::eip4844::DATA_GAS_PER_BLOB;
        U256::from(blob_gas).saturating_mul(U256::from(blob.max_fee_per_blob_gas))
    });
    let required = U256::from(gas_limit)
        .saturating_mul(U256::from(max_fee_per_gas))
        .saturating_add(blob_reserve);
    if let Ok(balance) = provider.get_balance(from).await
        && balance < required
    {
        event!(
            name: "eez.composer.postbatch.poster_underfunded",
            Level::ERROR,
            poster = %from,
            balance = %balance,
            required = %required,
            gas_limit,
            max_fee_per_gas,
            "poster balance below postBatch gas reserve; fund the poster or the batch will not be included",
        );
    }

    if let Some(BlobCarrier {
        sidecar,
        max_fee_per_blob_gas,
    }) = blob
    {
        let tx = TxEip4844 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas: priority_fee,
            to: eez_address,
            value: U256::ZERO,
            access_list: alloy_eips::eip2930::AccessList::default(),
            blob_versioned_hashes: sidecar.versioned_hashes().collect(),
            max_fee_per_blob_gas,
            input: calldata.into(),
        };
        // Pooled network encoding: the relay needs the sidecar beside the tx.
        let mut pooled = TxEip4844WithSidecar { tx, sidecar };
        let sig = signer
            .sign_transaction_sync(&mut pooled)
            .map_err(|e| format!("sign blob postBatch: {e}"))?;
        let signed = pooled.into_signed(sig);
        return Ok(SignedPostBatch {
            hash: *signed.hash(),
            raw: signed.encoded_2718().into(),
        });
    }
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas: priority_fee,
        to: TxKind::Call(eez_address),
        value: U256::ZERO,
        access_list: alloy_eips::eip2930::AccessList::default(),
        input: calldata.into(),
    };
    let sig = signer
        .sign_transaction_sync(&mut tx)
        .map_err(|e| format!("sign postBatch tx: {e}"))?;
    let signed = TransactionSigned::new_unhashed(Transaction::Eip1559(tx), sig);
    let mut buf = Vec::with_capacity(512);
    signed.encode_2718(&mut buf);
    Ok(SignedPostBatch {
        hash: *signed.tx_hash(),
        raw: Bytes::from(buf),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attestation_quorum::MAX_ATTESTERS;
    use crate::composer::tests::signed_user_tx;
    use alloy_primitives::TxHash;

    #[tokio::test]
    async fn blob_post_is_a_pooled_tx_bound_to_its_sidecar() {
        use alloy_eips::{Decodable2718 as _, eip4844::env_settings::EnvKzgSettings};
        use alloy_provider::{ProviderBuilder, mock::Asserter};
        let signer = alloy_signer_local::PrivateKeySigner::random();
        let blobs = [vec![0_u8; eez_blob::BYTES_PER_BLOB]];
        for encoding in [
            eez_blob::BlobSidecarEncoding::Eip4844,
            eez_blob::BlobSidecarEncoding::Eip7594,
        ] {
            let asserter = Asserter::new();
            asserter.push_success(&alloy_primitives::U64::from(7));
            asserter.push_success(&alloy_rpc_types_eth::Block::<
                alloy_rpc_types_eth::Transaction,
            >::default());
            asserter.push_success(&U256::MAX);
            let SignedPostBatch { raw, hash } = sign_post_batch_tx(
                &signer,
                &ProviderBuilder::default().connect_mocked_client(asserter),
                Address::repeat_byte(0x11),
                vec![0xaa],
                1,
                1,
                DEFAULT_MAX_POSTBATCH_GAS,
                Some(BlobCarrier {
                    sidecar: eez_blob::prepare_blob_sidecar(&blobs, encoding).unwrap(),
                    max_fee_per_blob_gas: 1,
                }),
            )
            .await
            .unwrap();
            let pooled = alloy_consensus::Signed::<
                alloy_consensus::TxEip4844WithSidecar<
                    alloy_eips::eip7594::BlobTransactionSidecarVariant,
                >,
            >::decode_2718_exact(&raw)
            .unwrap();
            let tx = pooled.tx();
            tx.tx
                .validate_blob(&tx.sidecar, EnvKzgSettings::Default.get())
                .unwrap();
            // The composer observes `hash`; the submitter derives it from `raw`.
            assert_eq!(hash, *pooled.hash());
            assert_eq!(eez_blob::post_batch_tx_hash(&raw).unwrap(), hash);
        }
    }

    /// A bundle whose Sync block never commits is never sent, and recovery
    /// re-queues it without a charge.
    #[tokio::test]
    async fn withheld_bundle_is_never_sent() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // Every L1 read and the relay share this listener, which never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&connections);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                std::mem::forget(socket);
            }
        });
        let reader = eez_l1::L1ReaderConfig {
            rpc_url: url.parse().unwrap(),
            trace_rpc_url: url.parse().unwrap(),
            eez: Address::ZERO,
            rollup_id: 1,
            deploy_block: 0,
            beacon_url: None,
        };
        let submitter = Submitter::new(eez_l1::SubmitterConfig {
            builder_rpc_url: url.parse().unwrap(),
            reader,
            target_rpc_url: None,
            poster: alloy_signer_local::PrivateKeySigner::random(),
        });
        let optimistic = Arc::new(OptimisticallyIncluded::new());
        let block = B256::repeat_byte(7);
        let post_batch = TxHash::repeat_byte(1);
        let parent =
            reth_primitives_traits::SealedHeader::seal_slow(alloy_consensus::Header::default());
        optimistic.begin(6, post_batch, parent, block, Vec::new());

        let (release, released) = BundleRelease::new();
        drop(release);
        let observer = send_after_commit(
            1,
            6,
            vec![signed_user_tx(21_000)],
            post_batch,
            block,
            Arc::clone(&optimistic),
            submitter,
            BundleTarget::Exact {
                block: 9,
                timestamp: 1,
            },
            Some(Staged {
                released,
                dispatched: Dispatched::Minimal,
            }),
            tokio::time::Instant::now(),
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), observer)
            .await
            .expect("a withheld bundle must not wait on L1");

        assert_eq!(connections.load(Ordering::SeqCst), 0);
        let failed = optimistic.take_failed_for_recovery(0).unwrap();
        assert!(failed.slot_skipped);
    }

    #[test]
    fn catchup_marker_is_unpinned_but_historical_target_keeps_the_l1_block() {
        let (catchup, historical) =
            bundle_targets_for_slot(Some(101), 1_700_000_012, SyncSlotMode::Catchup);
        assert_eq!(catchup, BundleTarget::NextBlock);
        assert_eq!(historical, BundleTarget::ExactBlock { block: 101 });

        let (steady, historical) =
            bundle_targets_for_slot(Some(102), 1_700_000_024, SyncSlotMode::Steady);
        assert_eq!(
            steady,
            BundleTarget::Exact {
                block: 102,
                timestamp: 1_700_000_024,
            }
        );
        assert_eq!(historical, BundleTarget::ExactBlock { block: 102 });
    }

    #[test]
    fn submission_identity_must_match_post_batch_signer() {
        let poster = Address::repeat_byte(0xa);
        assert!(ensure_submission_identity(poster, poster).is_ok());

        let err = ensure_submission_identity(poster, Address::repeat_byte(0xb)).unwrap_err();
        assert!(matches!(
            err,
            ComposerConfigError::SubmissionIdentityMismatch {
                submitter_poster,
                post_batch_signer,
            } if submitter_poster == poster && post_batch_signer == Address::repeat_byte(0xb)
        ));
    }

    #[test]
    fn eip7623_floor_matches_hand_computed_vector() {
        // 21000 + 10 * (zeros + 4*nonzeros).
        assert_eq!(calldata_floor_gas(&[]), 21_000);
        // 3 zeros (3 tokens) + 2 non-zeros (8 tokens) = 11 tokens = 110 gas.
        assert_eq!(calldata_floor_gas(&[0, 0xab, 0, 0xcd, 0]), 21_110);
        // A realistic batch's floor alone outgrows a multi-million fixed limit.
        assert!(calldata_floor_gas(&vec![0xff; 200 * 1024]) > 4_000_000);
    }

    /// K = 6 with a terminal metadata change to exercise both run encodings.
    fn drain_budget_range(with_traffic: bool) -> Vec<eez_payload_codec::SpanBlock> {
        let mut blocks = vec![
            eez_payload_codec::SpanBlock {
                beneficiary: [0x11; 20],
                extra_data: vec![0x22; 4],
                transactions: if with_traffic {
                    vec![vec![0x42; 200]]
                } else {
                    Vec::new()
                },
            };
            5
        ];
        blocks.push(eez_payload_codec::SpanBlock {
            beneficiary: [0x33; 20],
            extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
            transactions: Vec::new(),
        });
        blocks
    }

    const ONE_EMPTY_BLOB: BlobBytes = BlobBytes {
        limit: eez_payload_codec::LOGICAL_BYTES_PER_BLOB,
        used: 0,
        floor: 0,
    };

    fn blob_budget(blob: BlobBytes) -> PostBatchGasBudget {
        PostBatchGasBudget {
            blob: Some(blob),
            ..PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1)
        }
    }

    #[test]
    fn blob_budget_defers_behind_interval_traffic_and_evicts_only_the_oversized() {
        let floor = blob_stream_len(1, &drain_budget_range(false), &[]).unwrap();
        let base = blob_stream_len(1, &drain_budget_range(true), &[]).unwrap();
        let limit = 4096;
        let fit = |used, size| {
            blob_budget(BlobBytes { limit, used, floor })
                .try_accept_da(1, &[], Some(&Bytes::from(vec![0x42; size])))
                .unwrap()
        };
        assert_eq!(fit(floor, 3500), BlockGasFit::Accept);
        assert_eq!(fit(base, 3500), BlockGasFit::Defer);
        assert_eq!(fit(floor, limit), BlockGasFit::Unfittable);
    }

    #[test]
    fn blob_budget_matches_single_transaction_capacity() {
        let capacity = eez_payload_codec::LOGICAL_BYTES_PER_BLOB;
        for length in [capacity - 100, capacity - 40, capacity, capacity + 1] {
            let user = Bytes::from(vec![0x42; length]);
            let blocks = [eez_payload_codec::SpanBlock {
                transactions: vec![user.to_vec()],
                extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
                ..Default::default()
            }];
            let fits = eez_payload_codec::encode_blob_container(1, &blocks, &[])
                .unwrap()
                .len()
                <= 1;
            let mut budget = blob_budget(ONE_EMPTY_BLOB);
            assert_eq!(
                budget.try_accept_da(1, &[], Some(&user)).unwrap(),
                if fits {
                    BlockGasFit::Accept
                } else {
                    BlockGasFit::Unfittable
                },
                "{length} bytes"
            );
        }
    }

    #[test]
    fn blob_budget_projection_covers_the_real_stream() {
        for outbound in [false, true] {
            let action = eez_payload_codec::Action {
                source_rollup_id: u64::from(outbound),
                target_rollup_id: u64::from(!outbound),
                success: true,
                data: vec![0x42; 100],
                return_data: vec![0x24; 100],
                ..Default::default()
            };
            let entry = eez_protocol::entries::manifest::entry_from_action(
                &action,
                eez_protocol::RollupId(1),
            )
            .unwrap();
            let user = Bytes::from(vec![0x42; 100]);
            // Seeded with committed traffic, so an unseeded base under-counts.
            let mut blocks = drain_budget_range(true);
            let mut budget = blob_budget(BlobBytes {
                used: blob_stream_len(1, &blocks, &[]).unwrap(),
                ..ONE_EMPTY_BLOB
            });
            let mut actions = Vec::new();
            while budget
                .try_accept_da(1, std::slice::from_ref(&entry), outbound.then_some(&user))
                .unwrap()
                == BlockGasFit::Accept
            {
                actions.push(
                    eez_protocol::entries::manifest::action_from_entry(
                        &entry,
                        eez_protocol::RollupId(1),
                    )
                    .unwrap(),
                );
                if outbound {
                    blocks.last_mut().unwrap().transactions.push(user.to_vec());
                }
                let bytes = blob_stream_len(1, &blocks, &actions).unwrap();
                assert!(bytes <= budget.blob.unwrap().used);
            }
            assert!(
                actions.len() > 128,
                "exercise multibyte count and operations-length prefixes"
            );
            assert_eq!(
                eez_payload_codec::encode_blob_container(1, &blocks, &actions)
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[test]
    fn gas_budget_out_of_range_clamps_to_default() {
        // In range → honoured.
        assert_eq!(clamp_max_postbatch_gas(12_000_000, 1), 12_000_000);
        assert_eq!(
            clamp_max_postbatch_gas(min_viable_postbatch_gas(1), 1),
            min_viable_postbatch_gas(1)
        );
        assert_eq!(
            clamp_max_postbatch_gas(min_viable_postbatch_gas(1) - 1, 1),
            DEFAULT_MAX_POSTBATCH_GAS
        );
        assert_eq!(
            clamp_max_postbatch_gas(DEFAULT_MAX_POSTBATCH_GAS, 1),
            DEFAULT_MAX_POSTBATCH_GAS
        );
        assert_eq!(clamp_max_postbatch_gas(1, 1), DEFAULT_MAX_POSTBATCH_GAS);
        // Above the EIP-7825 tx gas cap no tx is valid at any block limit.
        assert_eq!(
            clamp_max_postbatch_gas(DEFAULT_MAX_POSTBATCH_GAS + 1, 1),
            DEFAULT_MAX_POSTBATCH_GAS
        );
    }

    /// The forge pin test re-declares the constants as literals, so a change
    /// here would leave it measuring the old value in silence. Bind them.
    #[test]
    fn solidity_pin_test_mirrors_the_rust_gas_pins() {
        let sol = include_str!("../../../../contracts/test/PostBatchGasPins.t.sol");
        for (name, rust) in [
            ("POSTBATCH_BASE_GAS_PIN", POSTBATCH_BASE_GAS_PIN),
            ("POSTBATCH_ENTRY_GAS_PIN", POSTBATCH_ENTRY_GAS_PIN),
            (
                "POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN",
                POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN,
            ),
        ] {
            let decl = format!("uint256 private constant {name} = ");
            let rest = sol
                .split(&decl)
                .nth(1)
                .unwrap_or_else(|| panic!("{name} is not declared in PostBatchGasPins.t.sol"));
            let literal: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '_')
                .collect();
            let sol_value: u64 = literal
                .replace('_', "")
                .parse()
                .unwrap_or_else(|e| panic!("{name} literal {literal:?}: {e}"));
            assert_eq!(
                sol_value, rust,
                "{name} drifted: Solidity pins {sol_value}, Rust projects {rust}",
            );
        }
    }

    /// Any ceiling the clamp accepts must let the drain take at least one tx.
    #[test]
    fn the_lowest_accepted_ceiling_still_admits_one_held_tx() {
        for attesters in 1..=MAX_ATTESTERS as u64 {
            let lowest = min_viable_postbatch_gas(attesters);
            assert_eq!(clamp_max_postbatch_gas(lowest, attesters), lowest);
            let mut budget = PostBatchGasBudget::new(lowest, attesters);
            assert!(
                budget.try_accept(TxL1Gas {
                    entries: 1,
                    calldata_gas: 0,
                    target_gas: 0,
                }),
                "with {attesters} attesters the lowest accepted ceiling admits no tx — every \
                 drain would evict its first held tx as poison",
            );
        }
    }

    /// One attester adds no attestation gas to the floor.
    #[test]
    fn a_single_attester_keeps_the_single_proof_floor() {
        assert_eq!(
            min_viable_postbatch_gas(1),
            projected_postbatch_gas(2, 0, 0) + POSTBATCH_DRAIN_MARGIN
        );
        assert!(min_viable_postbatch_gas(7) > min_viable_postbatch_gas(1));
    }

    /// The pre-prove gate sizes the batch with a `MAX_PROOF_BYTES` stand-in for
    /// the proof it does not have yet. That is only safe if the stand-in is a
    /// worst case: longer than any real proof, and priced at the dearer
    /// non-zero-byte rate. Both halves are pinned here — if a proof system ever
    /// returns something bigger, this fails before the gate turns optimistic and
    /// the signing-side ceiling has to catch it instead.
    #[test]
    fn proof_placeholder_is_a_worst_case() {
        /// r‖s‖v — the attestation `ECDSAProofSystem` verifies.
        const ECDSA_PROOF_BYTES: usize = 65;
        const { assert!(MAX_PROOF_BYTES >= ECDSA_PROOF_BYTES) };
        // Non-zero bytes are 4 EIP-7623 tokens, zeros are 1, so an all-0xff
        // stand-in prices above any real proof of the same length.
        let placeholder = [0xffu8; MAX_PROOF_BYTES];
        assert!(
            calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0x11u8; ECDSA_PROOF_BYTES])
        );
        assert!(calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0u8; MAX_PROOF_BYTES]));
        // Monotonic in length, so a shorter real proof can never cost more.
        assert!(calldata_floor_gas(&[0xffu8; 96]) <= calldata_floor_gas(&placeholder));
    }

    #[test]
    fn empty_candidate_grid_is_highest_first() {
        // The pure half of boundary selection: which on-grid offsets below the
        // snapped height are empty. Regression: an earlier revision folded a
        // per-block framing allowance into the same vector that carried the
        // emptiness flag, so NO candidate ever qualified and emission died.
        let pick = |empty: &[bool], k: usize| -> Vec<usize> {
            (0..empty.len()).step_by(k).filter(|&i| empty[i]).collect()
        };
        assert_eq!(pick(&[true; 12], 5), vec![0, 5, 10]);
        // Snapped height carries txs → the next K down leads.
        let mut e = [true; 12];
        e[0] = false;
        assert_eq!(pick(&e, 5), vec![5, 10]);
        // Only off-grid blocks empty → nothing to settle at.
        let mut e = [false; 12];
        e[3] = true;
        assert!(pick(&e, 5).is_empty());
        // Range shorter than one step still offers the snapped height.
        assert_eq!(pick(&[true, true, true], 5), vec![0]);
    }

    #[test]
    fn grid_aligned_cap_rounds_down_to_k_multiples() {
        // Already aligned → untouched. The default (300) divides by every K a
        // sane deployment uses (12s/2s→6, 12s/3s→4, 5s/1s→5), so it rarely moves.
        assert_eq!(grid_aligned_cap(300, 5), 300);
        assert_eq!(grid_aligned_cap(300, 6), 300);
        assert_eq!(grid_aligned_cap(300, 4), 300);
        // Not aligned → the largest multiple below, since a boundary can only
        // land on the grid; the remainder was never spendable.
        assert_eq!(grid_aligned_cap(300, 7), 294);
        assert_eq!(grid_aligned_cap(102, 5), 100);
        // Below one K there is no reachable grid height above the cursor.
        assert_eq!(grid_aligned_cap(3, 5), 5);
        assert_eq!(grid_aligned_cap(5, 5), 5);
    }

    /// Gas one outbound target call used on L1, measured on a kurtosis devnet
    /// (the same run measured ~336k marginal per outbound entry in total).
    const MEASURED_TARGET_GAS: u64 = 47_763;

    /// An outbound settlement entry of realistic size: one L1 call with a
    /// 4-byte selector plus two words of arguments.
    fn outbound_entry() -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
            l2ToL1Calls: vec![eez_protocol::abi::L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: Address::repeat_byte(0x11),
                sourceRollupId: 1,
                targetAddress: Address::repeat_byte(0x22),
                value: U256::ZERO,
                data: Bytes::from(vec![0x55u8; 68]),
            }],
            destinationRollupId: 1,
            success: true,
            ..Default::default()
        }
    }

    #[test]
    fn projection_charges_the_pins_the_calldata_and_the_probed_target() {
        // The cheapest postBatch: tx base + base pin + one leading entry.
        assert_eq!(
            projected_postbatch_gas(1, 0, 0),
            21_000 + POSTBATCH_BASE_GAS_PIN + POSTBATCH_ENTRY_GAS_PIN
        );
        // Entries are linear in the pin; calldata and probed target gas add raw.
        assert_eq!(
            projected_postbatch_gas(4, 500, 100),
            21_000 + POSTBATCH_BASE_GAS_PIN + 4 * POSTBATCH_ENTRY_GAS_PIN + 600
        );
        // Standard EIP-7623 rates, well under the 10-per-token floor the
        // signing path refuses on.
        assert_eq!(calldata_gas(&[]), 0);
        assert_eq!(calldata_gas(&[0, 0xab, 0, 0xcd, 0]), 3 * 4 + 2 * 16);
        // The bug this replaces: at a 50-tx bundle cap the real cost passes the
        // EIP-7825 per-tx cap, so no gas limit makes the batch postable.
        assert!(
            projected_postbatch_gas(51, 0, 50 * MEASURED_TARGET_GAS) > DEFAULT_MAX_POSTBATCH_GAS,
            "50 outbound entries must be recognised as unpostable",
        );
    }

    /// Every proof past the first is a verify call the batch pays for, so the
    /// drain starts that much further into its budget.
    #[test]
    fn drain_budget_charges_every_attester_past_the_first() {
        let single = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        let seven = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 7);
        assert_eq!(
            seven.projected - single.projected,
            6 * POSTBATCH_ECDSA_PROOF_SYSTEM_GAS_PIN
        );
        assert_eq!(attestation_gas(0), 0);
        assert_eq!(attestation_gas(1), 0);
    }

    #[test]
    fn blob_budget_charges_no_calldata_for_da_copies() {
        let entries = [outbound_entry()];
        let raw_tx = vec![0x77u8; 180];
        let calldata = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        let blob = blob_budget(ONE_EMPTY_BLOB);
        let in_calldata = calldata.tx_cost(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        let in_blobs = blob.tx_cost(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        let batch_only = projected_tx_l1_gas(&entries, &[], &[], MEASURED_TARGET_GAS);
        assert_eq!(in_blobs.calldata_gas, batch_only.calldata_gas);
        assert!(in_calldata.calldata_gas > batch_only.calldata_gas + calldata_gas(&raw_tx));
    }

    #[test]
    fn drain_budget_cuts_the_accepted_prefix_below_the_cap() {
        let entries = [outbound_entry()];
        let raw_tx = vec![0x77u8; 180];
        let cost = projected_tx_l1_gas(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        // Every term is charged: one entry, the inline call, and the calldata for
        // the entry in `batch.entries` plus its DA copy and the raw tx.
        assert_eq!(cost.entries, 1);
        assert_eq!(cost.target_gas, MEASURED_TARGET_GAS);
        let encoded = {
            use alloy_sol_types::SolValue as _;
            calldata_gas(&entries[0].abi_encode())
        };
        assert_eq!(cost.calldata_gas, 2 * encoded + calldata_gas(&raw_tx));

        let mut budget = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1);
        // The drain leaves the belt's margin below the cap, or the belt refuses
        // what the drain accepted and the same set requeues forever.
        assert_eq!(
            budget.cap,
            DEFAULT_MAX_POSTBATCH_GAS - POSTBATCH_DRAIN_MARGIN
        );
        let per_tx = POSTBATCH_ENTRY_GAS_PIN + cost.calldata_gas + cost.target_gas;
        let expected = (budget.cap - budget.projected) / per_tx;
        let mut accepted = 0u64;
        // The drain never offers more than the per-bundle cap.
        while accepted < 50 && budget.try_accept(cost) {
            accepted += 1;
        }
        assert_eq!(accepted, expected);
        // The cut is what makes the batch postable at all.
        assert!(accepted < 50);
        assert!(budget.projected <= DEFAULT_MAX_POSTBATCH_GAS);
        // A rejected tx leaves the projection untouched, so the accepted prefix
        // is exactly what the batch will carry.
        let projected = budget.projected;
        assert!(!budget.try_accept(cost));
        assert_eq!(budget.projected, projected);
        // The drain and the prepare-time belt agree on the same entries: the
        // leading immediate plus one per accepted tx.
        assert_eq!(
            projected,
            projected_postbatch_gas(
                accepted + 1,
                accepted * cost.calldata_gas,
                accepted * MEASURED_TARGET_GAS,
            )
        );
    }
}
