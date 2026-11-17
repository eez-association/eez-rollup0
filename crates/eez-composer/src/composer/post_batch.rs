//! The steps of `Composer::prepare_post_batch_raw`: batch assembly,
//! settlement stitching, DA payload, gas projection, blob carriage and
//! proof attachment. Free functions, so each step reads on its own.

use std::collections::HashMap;

use alloy_primitives::aliases::I192;
use alloy_primitives::{B256, Bytes, U256};
use alloy_sol_types::SolCall;
use eez_protocol::abi::ExecutionEntrySol;
use reth_primitives_traits::AlloyBlockHeader;
use tracing::{Level, event};

use super::blob::PhysicalBlobs;
use super::{
    CrossChainExecCtx, MAX_PROOF_BYTES, PreparePostBatchError, ProverError, SyncCandidates,
    assign_proof_systems, attestation_gas, calldata_floor_gas, calldata_gas,
    ensure_batch_registry_native, projected_postbatch_gas, target_blob_limit,
    validate_actionable_prover_failure,
};
use crate::attestation_quorum::Attestation;

/// Assemble the ordered entries and finalize their settlement updates.
pub(super) fn build_settlement_batch(
    rollup_id: u64,
    compositions: &[&eez_protocol::Composition],
    outbound_entries: &[ExecutionEntrySol],
    pre_block_hash: B256,
    candidates: &SyncCandidates,
    sync_block_hash: B256,
    live_l1_context: bool,
) -> Result<eez_protocol::EvmBatch, String> {
    let mut batch = assemble_batch(
        rollup_id,
        compositions,
        outbound_entries,
        pre_block_hash,
        candidates.anchor,
        live_l1_context,
    );
    let inbound_ether = inbound_ether_map(compositions);
    stitch_settlement(
        &mut batch,
        rollup_id,
        &candidates.per_effect,
        &inbound_ether,
        sync_block_hash,
    )?;

    // The contract drains the leading contiguous `proxyEntryHash == 0` run
    // inline: one anchor plus the outbound immediate entries.
    // Inbound deferred entries (proxyEntryHash != 0) queue for
    // `executeCrossChainCall` consumption. N=0 for inbound-only → 1.
    batch.immediateEntryCount = U256::from(1 + outbound_entries.len() as u64);

    // Registry-id settlement gate: refuse a batch carrying any non-registry
    // destinationRollupId (e.g. an un-rewritten MAINNET(0) outbound entry).
    ensure_batch_registry_native(&batch, rollup_id)?;
    Ok(batch)
}

/// Merge the slot's compositions into ONE batch, prepend the leading
/// immediate (anchor) entry, and splice the outbound settlement entries
/// after it — entry order `[anchor | outbound | inbound]`.
fn assemble_batch(
    rollup_id: u64,
    compositions: &[&eez_protocol::Composition],
    outbound_entries: &[ExecutionEntrySol],
    anchor_current_root: B256,
    anchor_new_root: B256,
    live_l1_context: bool,
) -> eez_protocol::EvmBatch {
    // Merge inbound compositions' L1 source batches, or start empty so an
    // idle Sync slot can carry only the leading immediate entry that
    // advances L1's stored root.
    let mut batch = if compositions.is_empty() {
        eez_protocol::EvmBatch::default()
    } else {
        let mut b = compositions[0].source.batch.clone();
        for c in &compositions[1..] {
            b.entries.extend(c.source.batch.entries.iter().cloned());
            b.staticEntries
                .extend(c.source.batch.staticEntries.iter().cloned());
        }
        b
    };
    batch.blockNumber = if live_l1_context { u64::MAX } else { 0 };

    // Prepend one leading immediate entry (`proxyEntryHash == 0`)
    // covering all L2 effects before the sync block — EEZ.sol drains
    // it inline during postAndVerifyBatch, applying its state update
    // against L1's recorded root.
    //
    // `currentRoot` = hash(posted), the L1-confirmed cursor block — must
    // equal L1.config.root at postBatch time so the deriver's
    // check_claimed_state agrees. `newRoot` = the Sync block sealed over no
    // transactions, so the anchor already sits at the Sync block's height
    // and later effect entries chain from it. With no effects that block IS
    // the Sync block, so it is also the required endpoint.
    let immediate_entry = ExecutionEntrySol {
        rollupUpdates: vec![eez_protocol::abi::RollupUpdateSol {
            rollupId: rollup_id,
            currentRoot: anchor_current_root,
            newRoot: anchor_new_root,
            etherDelta: I192::ZERO,
        }],
        proxyEntryHash: B256::ZERO,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        rollingHash: B256::ZERO,
        destinationRollupId: rollup_id,
        success: true,
        returnData: Bytes::new(),
    };
    batch.entries.insert(0, immediate_entry);

    // Splice OUTBOUND settlement entries after the leading anchor; its state
    // update is attached below. The contract drains the contiguous
    // `proxyEntryHash==0` run inline, so order must be
    // `[anchor | outbound | inbound]`. `dest=rid` is the settlement's source
    // rollup (not the call's MAINNET target); `_validateStructure` checks it.
    for (k, oe) in outbound_entries.iter().enumerate() {
        let mut entry = oe.clone();
        entry.destinationRollupId = rollup_id;
        batch.entries.insert(1 + k, entry);
    }
    batch
}

/// Map `proxyEntryHash → +V` for the value-bearing inbound deferred entries
/// of the slot's compositions.
fn inbound_ether_map(compositions: &[&eez_protocol::Composition]) -> HashMap<B256, I192> {
    // Deposit value for inbound deferred entries: the lean on-chain entry binds
    // V only in its `proxyEntryHash` preimage, so read V from the DA sidecar
    // (`targets[].batch`, same `proxyEntryHash`). Value-free → absent → 0.
    compositions
        .iter()
        .flat_map(|c| c.targets.iter())
        .flat_map(|t| t.batch.entries.iter())
        .filter_map(|e| {
            let v = e.l2ToL1Calls.first()?.value;
            if v.is_zero() {
                return None;
            }
            eez_protocol::abi::u256_to_i192(v).map(|d| (e.proxyEntryHash, d))
        })
        .collect()
}

/// Attach one chained settlement state update to each cross-chain effect
/// entry, stitch the per-rollup `currentRoot` chain so it ends at the Sync
/// block, then finalize the L1 rolling hashes over the stitched updates.
fn stitch_settlement(
    batch: &mut eez_protocol::EvmBatch,
    rollup_id: u64,
    per_effect: &[B256],
    inbound_ether: &HashMap<B256, I192>,
    sync_block_hash: B256,
) -> Result<(), String> {
    // Cross-chain entries arrive with EMPTY `rollupUpdates`; attach one chained
    // settlement state update to each (the anchor already has its own) — else
    // `_applyRollupUpdates` no-ops and the L2 root never settles. Direction by
    // `proxyEntryHash`: outbound (== 0) → `-V` (via `outbound_ether_out`; None =
    // multi-call-with-value, unsupported → reject); inbound (!= 0) → `+V` deposit.
    // Value-free → 0.
    // `newRoot` = effect `k`'s per-effect root `candidates.per_effect[k]`; entries are
    // ordered `[outbound… | inbound…]`, matching the Sync block's pair-ends.
    // The prover requires this exact per-entry value. `currentRoot` is fixed
    // by the stitch below.
    let mut effect_k = 0usize;
    for entry in &mut batch.entries {
        // Preserve the anchor's existing state update and fill only the
        // cross-chain effect entries, which arrive empty.
        if !entry.rollupUpdates.is_empty() {
            continue;
        }
        let ether_delta = if entry.proxyEntryHash == B256::ZERO {
            let v = eez_protocol::entries::outbound_ether_out(entry).ok_or_else(|| {
                format!(
                    "outbound entry: multi-call value not supported \
                     (l2ToL1Calls={})",
                    entry.l2ToL1Calls.len(),
                )
            })?;
            if v.is_zero() {
                I192::ZERO
            } else {
                -eez_protocol::abi::u256_to_i192(v)
                    .ok_or_else(|| format!("outbound etherOut {v} overflows int192"))?
            }
        } else {
            inbound_ether
                .get(&entry.proxyEntryHash)
                .copied()
                .unwrap_or(I192::ZERO)
        };
        let new_root = *per_effect.get(effect_k).ok_or_else(|| {
            format!(
                "settlement stitch: effect entry {effect_k} has no per-effect root \
                 (only {} pair-end roots — pair-end/entry misalignment)",
                per_effect.len(),
            )
        })?;
        entry.rollupUpdates = vec![eez_protocol::abi::RollupUpdateSol {
            rollupId: rollup_id,
            currentRoot: B256::ZERO,
            newRoot: new_root,
            etherDelta: ether_delta,
        }];
        effect_k += 1;
    }
    if effect_k != per_effect.len() {
        return Err(format!(
            "settlement stitch: {effect_k} effect entries but {} per-effect roots \
             (pair-end/entry misalignment)",
            per_effect.len(),
        ));
    }

    // Stitch the per-rollup state-update chain: EEZ.sol `_applyRollupUpdates`
    // enforces `config.root == update.currentRoot` then sets it to
    // `newRoot`, so each entry's `currentRoot` must chain to the prior
    // entry's `newRoot`. This chains `posted → E → R_0 → … → R_last`, with
    // `E` the empty prefix and `R_last` the terminal, satisfying both EEZ.sol
    // and the prover's effect-prefix gate.
    let mut running_roots: HashMap<u64, B256> = HashMap::new();
    for entry in &mut batch.entries {
        for update in &mut entry.rollupUpdates {
            if let Some(prev_new) = running_roots.get(&update.rollupId).copied() {
                update.currentRoot = prev_new;
            }
            running_roots.insert(update.rollupId, update.newRoot);
        }
    }

    // The validating proof path enforces that the chain ends at the Sync
    // block's final root. Debug builds also check the local stitching
    // invariant here before the proof signer independently validates it.
    debug_assert_eq!(
        batch
            .entries
            .last()
            .and_then(|entry| entry.rollupUpdates.last())
            .map(|update| update.newRoot),
        Some(sync_block_hash),
        "settlement chain must end at the Sync-block state root",
    );

    // The L1 rolling seed commits the finalized ordered state updates, so
    // it can only be computed after the stitch above has set every
    // `currentRoot`.
    eez_protocol::entries::finalize_l1_rolling_hashes(batch)
        .map_err(|error| format!("finalize L1 rolling hashes: {error}"))
}

/// The L2 block range `(from, to, span)` the batch covers: from the first
/// unposted block after `posted` to the Sync block at `parent_number + 1`.
pub(super) fn batch_range(
    posted: u64,
    parent_number: u64,
    max_blocks: u64,
) -> Result<(u64, u64, u64), String> {
    let from = posted + 1;
    let sync_block_number = parent_number + 1;
    if sync_block_number < from {
        return Err(format!(
            "sync block {sync_block_number} <= L1-confirmed cursor {posted}; \
             composer is behind its own posted batches"
        ));
    }
    // Invariant 7: `compose_sync_slot` bounds every range, so reaching this
    // is a decision-layer bug, not an operating condition.
    let span = sync_block_number - from + 1;
    if span > max_blocks {
        return Err(format!(
            "batch range {from}..={sync_block_number} spans {span} blocks, over \
             MAX_BLOCKS_PER_BATCH ({max_blocks}) — emission bug",
        ));
    }
    Ok((from, sync_block_number, span))
}

/// The DA payload: the committed intermediate blocks plus the Sync block,
/// and the published actions of the outbound and inbound entries.
pub(super) fn build_da_payload(
    rollup_id: u64,
    mut blocks: Vec<eez_payload_codec::SpanBlock>,
    terminal_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
    outbound_user_txs: &[Bytes],
    compositions: &[&eez_protocol::Composition],
    outbound_entries: &[ExecutionEntrySol],
) -> Result<
    (
        Vec<eez_payload_codec::SpanBlock>,
        Vec<eez_payload_codec::Action>,
    ),
    String,
> {
    // Outbound user txs aren't reconstructible from the entries (only the load
    // is), so they travel in the Sync-block DA here; the deriver interleaves
    // them with the rebuilt loads. Inbound-only → empty.
    blocks.push(eez_payload_codec::SpanBlock {
        // Read off the terminal header.
        beneficiary: terminal_header.beneficiary().into(),
        extra_data: terminal_header.extra_data().to_vec(),
        transactions: outbound_user_txs.iter().map(|b| b.to_vec()).collect(),
    });
    // Outbound entries describe L1 settlement and L2 loads; inbound target
    // batches carry the delivery inputs. Published as actions in that order
    // — a lean inbound entry binds its call only via `proxyEntryHash`, so
    // this is its only published copy.
    let actions: Vec<eez_payload_codec::Action> = outbound_entries
        .iter()
        .chain(
            compositions
                .iter()
                .flat_map(|c| c.targets.iter())
                .flat_map(|t| t.batch.entries.iter()),
        )
        .map(|entry| {
            eez_protocol::entries::manifest::action_from_entry(
                entry,
                eez_protocol::RollupId(rollup_id),
            )
            .map_err(|e| format!("DA action for entry: {e}"))
        })
        .collect::<Result<_, String>>()?;
    // A failed action is never emitted: an unsuccessful call is rejected
    // as unsupported long before it becomes an entry.
    if let Some(at) = actions.iter().position(|a| !a.success) {
        return Err(format!(
            "action {at} of {} failed; we do not emit failed actions",
            actions.len(),
        ));
    }
    Ok((blocks, actions))
}

/// Encode the DA payload into the batch: blobs when blob posting is
/// configured (returned for the budget check and the sidecar), else calldata.
pub(super) async fn encode_da<'a>(
    ctx: &'a CrossChainExecCtx,
    l1_block_time: std::time::Duration,
    bundle_target: eez_l1::BundleTarget,
    rollup_id: u64,
    blocks: &[eez_payload_codec::SpanBlock],
    actions: &[eez_payload_codec::Action],
    batch: &mut eez_protocol::EvmBatch,
) -> Result<Option<PhysicalBlobs<'a>>, String> {
    // Blob settlement carries the stream in blobs sized for the target's fork.
    if let Some(config) = &ctx.blob_posting {
        let limit =
            target_blob_limit(&ctx.l1_provider, config, l1_block_time, bundle_target).await?;
        let blobs = eez_payload_codec::encode_blob_container(rollup_id, blocks, actions)
            .map_err(|e| format!("eez_payload_codec::encode_blob_container: {e}"))?;
        batch.callData = Bytes::new();
        batch.blobIndices = (0..blobs.len()).map(U256::from).collect();
        Ok(Some(PhysicalBlobs {
            config,
            blobs,
            limit,
        }))
    } else {
        let payload = eez_payload_codec::encode_container(rollup_id, blocks, actions)
            .map_err(|e| format!("eez_payload_codec::encode_container: {e}"))?;
        batch.callData = Bytes::from(payload);
        Ok(None)
    }
}

/// Projected `postAndVerifyBatch` gas of the batch sized with a worst-case
/// proof from each of `proof_count` attesters, and that call's byte length.
pub(super) fn project_postbatch_gas(
    batch: &eez_protocol::EvmBatch,
    proof_count: usize,
    outbound_target_gas: u64,
    attesters: u64,
) -> (u64, usize) {
    // Sized for every configured attester: the grace window can collect a
    // proof from each of them.
    let mut sized = batch.clone();
    sized.proofs = vec![Bytes::from(vec![0xffu8; MAX_PROOF_BYTES]); proof_count];
    let projected = eez_protocol::abi::postAndVerifyBatchCall { batch: sized }.abi_encode();
    // EIP-7623 charges max(standard, floor). Standard now covers the bytes the
    // drain could not see; floor is what a fat, entry-light batch hits.
    let projected_gas = projected_postbatch_gas(
        batch.entries.len() as u64,
        calldata_gas(&projected),
        outbound_target_gas,
    )
    .saturating_add(attestation_gas(attesters))
    .max(calldata_floor_gas(&projected));
    (projected_gas, projected.len())
}

/// Classify an attestation failure: an actionable rejection only when its
/// details match this batch, else an opaque prover error.
pub(super) fn attestation_error(
    rollup_id: u64,
    error: ProverError,
    batch: &eez_protocol::EvmBatch,
    sync_block: Option<&reth_primitives_traits::RecoveredBlock<eez_primitives::Block>>,
) -> PreparePostBatchError {
    let Some(failure) = error.actionable_failure() else {
        return PreparePostBatchError::Prover(error);
    };
    if let Err(validation_error) = validate_actionable_prover_failure(failure, batch, sync_block) {
        event!(
            name: "eez.composer.prover.actionable_invalid",
            target: "eez_composer::composer",
            Level::ERROR,
            rollup_id,
            failure = %failure,
            error = %validation_error,
            "prover supplied actionable failure details that do not match the current request; treating the rejection as opaque",
        );
        return PreparePostBatchError::Prover(error);
    }
    PreparePostBatchError::Actionable(failure)
}

/// Name exactly the proof systems that attested and carry their proofs.
pub(super) fn apply_attestations(
    batch: &mut eez_protocol::EvmBatch,
    rollup_id: u64,
    attestations: Vec<Attestation>,
) {
    assign_proof_systems(
        batch,
        rollup_id,
        attestations.iter().map(|a| a.proof_system).collect(),
    );
    batch.proofs = attestations.into_iter().map(|a| a.proof).collect();
}
