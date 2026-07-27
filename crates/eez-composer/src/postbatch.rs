//! postBatch assembly pipeline — the standalone steps behind
//! `Composer::prepare_post_batch_raw` (see `composer.rs`).
//!
//! Free functions with no `Composer` state: batch merge + anchor +
//! outbound splice ([`assemble_batch`]), settlement stitching
//! ([`stitch_settlement`]), proof-carrier attachment
//! ([`attach_proof_carriers`]), DA payload encoding
//! ([`build_da_payload`]) and proving-window witness collection
//! ([`collect_window_witnesses`]). The orchestrator in `composer.rs`
//! sequences them and keeps the store reads / prover call.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use eez_driver::witness::{ExecutionWitnessMode, block_witness};
use eez_prover::BlockWitness;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{
    BlockReader, BlockSource, HeaderProvider, StateProviderFactory, TransactionsProvider,
};

/// Merge the slot's compositions into ONE batch, prepend the leading
/// immediate (anchor) entry, and splice the outbound settlement entries
/// after it — entry order `[anchor | outbound | inbound]`.
pub(crate) fn assemble_batch(
    compositions: &[&eez_protocol::Composition],
    rollup_id_u256: U256,
    pre_state_root: B256,
    pre_sync_state_root: B256,
    outbound_entries: &[eez_protocol::abi::ExecutionEntrySol],
) -> eez_protocol::EvmBatch {
    // Empty compositions is a VALID case: an empty HeldPool Sync
    // slot still emits a postBatch carrying just the leading
    // immediate entry so L1's stored stateRoot tracks the L2
    // progression. We build the batch from scratch (no per-tx
    // batch to merge from); only the leading immediate entry +
    // proof-system metadata go in.

    // Take the first composition's batch as the template, then
    // merge every later composition's entries + lookupCalls into
    // it. Empty compositions → build a fresh empty batch shell;
    // the leading immediate entry below is the entire payload.
    let mut batch = if compositions.is_empty() {
        eez_protocol::EvmBatch::default()
    } else {
        let mut b = compositions[0].source.batch.clone();
        for c in &compositions[1..] {
            b.entries.extend(c.source.batch.entries.iter().cloned());
            b.l1ToL2lookupCalls
                .extend(c.source.batch.l1ToL2lookupCalls.iter().cloned());
        }
        b
    };

    // Prepend ONE leading immediate entry (`proxyEntryHash == 0`)
    // covering all L2 effects before the sync block — EEZ.sol drains
    // it inline during postAndVerifyBatch, applying its stateDelta
    // against L1's recorded root.
    //
    // `currentState` = L2.stateRoot(posted) (the L1-confirmed cursor)
    // — must equal L1.config.stateRoot at postBatch time so the
    // deriver's check_claimed_state agrees. `newState` = L2 at
    // sync_block-1 (`parent_header.state_root()`), lumping every
    // pre-sync block's effects into one stateDelta.
    let immediate_entry = eez_protocol::abi::ExecutionEntrySol {
        stateDeltas: vec![eez_protocol::abi::StateDeltaSol {
            rollupId: rollup_id_u256,
            currentState: pre_state_root,
            newState: pre_sync_state_root,
            etherDelta: alloy_primitives::I256::ZERO,
        }],
        proxyEntryHash: B256::ZERO,
        destinationRollupId: rollup_id_u256,
        l2ToL1Calls: Vec::new(),
        expectedL1ToL2Calls: Vec::new(),
        expectedLookups: Vec::new(),
        callCount: U256::ZERO,
        returnData: Bytes::new(),
        rollingHash: B256::ZERO,
    };
    batch.entries.insert(0, immediate_entry);

    // Splice OUTBOUND settlement entries after the leading anchor (delta
    // attached below). The contract drains the contiguous `proxyEntryHash==0`
    // run inline, so order must be `[anchor | outbound | inbound]`. `dest=rid`
    // is the settlement's source rollup (not the call's MAINNET target);
    // `_validateStructure` membership-checks it.
    for (k, oe) in outbound_entries.iter().enumerate() {
        let mut entry = oe.clone();
        entry.destinationRollupId = rollup_id_u256;
        batch.entries.insert(1 + k, entry);
    }

    batch
}

/// Map `proxyEntryHash → +V` for the value-bearing inbound deferred
/// entries of the slot's compositions.
pub(crate) fn inbound_ether_map(
    compositions: &[&eez_protocol::Composition],
) -> HashMap<B256, alloy_primitives::I256> {
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
            alloy_primitives::I256::try_from(v)
                .ok()
                .map(|d| (e.proxyEntryHash, d))
        })
        .collect()
}

/// Attach one chained settlement `stateDelta` to each cross-chain effect
/// entry (ether delta by direction, `newState` from `pair_roots`), then
/// stitch the per-rollup `currentState` chain so it ends at the Sync
/// block's final root.
pub(crate) fn stitch_settlement(
    batch: &mut eez_protocol::EvmBatch,
    rollup_id_u256: U256,
    pair_roots: &[B256],
    inbound_ether: &HashMap<B256, alloy_primitives::I256>,
    sync_block_state_root: B256,
) -> Result<(), String> {
    // Cross-chain entries arrive with EMPTY `stateDeltas`; attach one chained
    // settlement delta to each (the anchor already has its own) — else
    // `_applyStateDeltas` no-ops and the L2 root never settles. Direction by
    // `proxyEntryHash`: outbound (== 0) → `-V` (via `outbound_ether_out`; None =
    // multi-call-with-value, unsupported → reject); inbound (!= 0) → `+V` deposit.
    // Value-free → 0.
    // `newState` = effect `k`'s per-effect root `pair_roots[k]`; entries are
    // ordered `[outbound… | inbound…]`, matching the Sync block's pair-ends.
    // The prover requires this exact per-entry value. `currentState` is fixed
    // by the stitch below.
    let mut effect_k = 0usize;
    for entry in &mut batch.entries {
        // Skip entries that already carry a delta (the anchor); fill only the
        // cross-chain effect entries, which arrive empty.
        if !entry.stateDeltas.is_empty() {
            continue;
        }
        let ether_delta = if entry.proxyEntryHash == B256::ZERO {
            let v = eez_protocol::entries::outbound_ether_out(entry).ok_or_else(|| {
                format!(
                    "outbound entry: multi-call value not supported \
                     (callCount={}, l2ToL1Calls={})",
                    entry.callCount,
                    entry.l2ToL1Calls.len(),
                )
            })?;
            if v.is_zero() {
                alloy_primitives::I256::ZERO
            } else {
                -alloy_primitives::I256::try_from(v)
                    .map_err(|e| format!("outbound etherOut {v} overflows I256: {e}"))?
            }
        } else {
            inbound_ether
                .get(&entry.proxyEntryHash)
                .copied()
                .unwrap_or(alloy_primitives::I256::ZERO)
        };
        let new_state = *pair_roots.get(effect_k).ok_or_else(|| {
            format!(
                "settlement stitch: effect entry {effect_k} has no per-effect root \
                 (only {} pair-end roots — pair-end/entry misalignment)",
                pair_roots.len(),
            )
        })?;
        entry.stateDeltas = vec![eez_protocol::abi::StateDeltaSol {
            rollupId: rollup_id_u256,
            currentState: B256::ZERO,
            newState: new_state,
            etherDelta: ether_delta,
        }];
        effect_k += 1;
    }
    if effect_k != pair_roots.len() {
        return Err(format!(
            "settlement stitch: {effect_k} effect entries but {} per-effect roots \
             (pair-end/entry misalignment)",
            pair_roots.len(),
        ));
    }

    // Stitch the per-rollup stateDelta chain: EEZ.sol `_applyStateDeltas`
    // enforces `config.stateRoot == delta.currentState` then sets it to
    // `newState`, so each entry's `currentState` must chain to the prior
    // entry's `newState`. This chains `pre_sync → R_0 → … → R_last (final
    // root)`, satisfying both EEZ.sol and the prover's effect-prefix gate.
    let mut running_roots: HashMap<U256, B256> = HashMap::new();
    for entry in &mut batch.entries {
        for delta in &mut entry.stateDeltas {
            if let Some(prev_new) = running_roots.get(&delta.rollupId).copied() {
                delta.currentState = prev_new;
            }
            running_roots.insert(delta.rollupId, delta.newState);
        }
    }

    // Anchor-only batch (no effects): the immediate is the last entry, so it
    // must carry the final root. An empty Sync block still mutates state
    // (EIP-2935 / EIP-4788 system writes), so `parent.stateRoot` differs from
    // the re-executed final root and the endpoint gate would fail. With
    // effects, the last effect's root already is the final root.
    if pair_roots.is_empty() {
        if let Some(last) = batch.entries.last_mut() {
            for delta in last.stateDeltas.iter_mut().rev() {
                if delta.rollupId == rollup_id_u256 {
                    delta.newState = sync_block_state_root;
                    break;
                }
            }
        }
    }

    // The chain must end at the Sync block's final root. The prover enforces
    // this (gates.rs); assert locally so a stitch bug fails fast here.
    debug_assert_eq!(
        batch
            .entries
            .last()
            .and_then(|e| e.stateDeltas.last())
            .map(|d| d.newState),
        Some(sync_block_state_root),
        "settlement chain must end at the Sync-block state root",
    );

    Ok(())
}

/// Set the transient-prefix count and the proof-system carriers on the
/// batch, gating on registry-native ids first.
pub(crate) fn attach_proof_carriers(
    batch: &mut eez_protocol::EvmBatch,
    rollup_id_u256: U256,
    outbound_count: usize,
    ecdsa_proof_system_address: Address,
    l2_rollup_id: u64,
) -> Result<(), String> {
    use eez_protocol::abi::RollupIdWithProofSystemsSol;

    // The contract drains the leading contiguous `proxyEntryHash==0` run
    // inline (`EEZ.sol:387`): 1 anchor immediate + N outbound immediates.
    // Inbound deferred entries (proxyEntryHash != 0) queue for
    // `executeCrossChainCall` consumption. N=0 for inbound-only → 1.
    batch.transientExecutionEntryCount = U256::from(1 + outbound_count as u64);

    // Registry-id settlement gate: refuse a batch carrying any non-registry
    // destinationRollupId (e.g. an un-rewritten MAINNET(0) outbound entry).
    assert_batch_registry_native(batch, rollup_id_u256)?;
    batch.proofSystems = vec![ecdsa_proof_system_address];
    batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
        rollupId: U256::from(l2_rollup_id),
        proofSystemIndex: vec![0u64],
    }];
    Ok(())
}

/// Refuse to settle a batch carrying any `destinationRollupId` / `sourceRollupId`
/// that isn't this rollup's registry id — a wiring bug (e.g. an outbound entry
/// whose `dest` stayed at the call's MAINNET(0) target) that L1 would misattribute
/// and that folds into the `publicInputsHash`. Guards the outbound `dest=rid` rewrite.
fn assert_batch_registry_native(batch: &eez_protocol::EvmBatch, rid: U256) -> Result<(), String> {
    for (i, entry) in batch.entries.iter().enumerate() {
        if entry.destinationRollupId != rid {
            return Err(format!(
                "entry[{i}].destinationRollupId = {} is not the configured registry id {rid} — \
                 a non-registry id reached the settlement batch (composition must be registry-native)",
                entry.destinationRollupId,
            ));
        }
        for (j, call) in entry.l2ToL1Calls.iter().enumerate() {
            if call.sourceRollupId != rid {
                return Err(format!(
                    "entry[{i}].l2ToL1Calls[{j}].sourceRollupId = {} is not the configured registry id {rid}",
                    call.sourceRollupId,
                ));
            }
        }
    }
    for (i, lookup) in batch.l1ToL2lookupCalls.iter().enumerate() {
        if lookup.destinationRollupId != rid {
            return Err(format!(
                "l1ToL2lookupCalls[{i}].destinationRollupId = {} is not the configured registry id {rid}",
                lookup.destinationRollupId,
            ));
        }
    }
    Ok(())
}

/// Serialize the covered L2 block range (walked parent-hash-first from
/// `parent_header`) plus the L2-shape entry sidecar into the DA payload
/// destined for `batch.callData` (codec v2).
pub(crate) fn build_da_payload<L2>(
    l2_provider: &L2,
    parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
    from: u64,
    sync_block_number: u64,
    system_signer_address: Address,
    compositions: &[&eez_protocol::Composition],
    outbound_entries: &[eez_protocol::abi::ExecutionEntrySol],
    outbound_user_txs: &[Bytes],
) -> Result<Vec<u8>, String>
where
    L2: BlockReader<Header = alloy_consensus::Header>,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    let span_len = usize::try_from(sync_block_number - from + 1)
        .map_err(|e| format!("batch span overflow: {e}"))?;
    let mut blocks_rev: Vec<Vec<Vec<u8>>> = Vec::with_capacity(span_len.saturating_sub(1));
    let mut cursor_hash = parent_header.hash();
    let mut cursor_number = parent_header.number();
    while cursor_number >= from {
        // `BlockSource::Any` so the lookup finds the parent even
        // while it's still "pending" in reth: at compose_sync_slot
        // time the Sequencer has done `newPayload(parent)` but the
        // promoting FCU fires on the next commit, so the parent is in
        // reth's tree but not yet canonical-head. Deeper ancestors
        // are already canonical, so `Any` finds them too.
        let block = l2_provider
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
        // Refuse-to-emit guard (invariant 7): intermediates carry
        // ONLY user txs (system txs live exclusively in the Sync
        // block, reconstructed deriver-side — Rollup-1 §8). A
        // failed-but-not-yet-recovered optimistic Sync block in this
        // range still holds its system txs; serializing it here would
        // launder phantom cross-chain effects into L1-accepted
        // history. Detect both framings (type-0x7E per Rollup-1 §5.3,
        // and the SYSTEM_ADDRESS legacy framing) and degrade — the
        // slot commits without emission.
        for enc in &tx_bytes {
            let is_system = if enc.first() == Some(&0x7E) {
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
                signer == system_signer_address
            };
            if is_system {
                return Err(format!(
                    "intermediate block {cursor_number} carries type-0x7E system txs — \
                     un-recovered failed Sync block in range; emission blocked until \
                     recovery (invariant 7)"
                ));
            }
        }
        blocks_rev.push(tx_bytes);
        if cursor_number == 0 {
            break;
        }
        cursor_hash = block.header().parent_hash();
        cursor_number -= 1;
    }
    blocks_rev.reverse();
    let mut blocks = blocks_rev;
    // Outbound user txs aren't reconstructible from the entries (only the load
    // is), so they travel in the Sync-block DA here; the deriver interleaves
    // them with the rebuilt loads. Inbound-only → empty.
    blocks.push(outbound_user_txs.iter().map(|b| b.to_vec()).collect());
    // L2-shape entries for system-tx reconstruction by external
    // followers. The L1 batch's `entries[]` carries the DEPOSIT-
    // shape entries (callCount=0, no L2ToL1Calls) for value-bearing
    // calls; those don't carry the inbound call params the L2
    // system tx needs. The L2-shape entries live in
    // `composition.targets[].batch` (built by
    // `protocol.build_batch(source=L2)`). We serialize each via
    // `SolValue::abi_encode` and ship them through codec v2 in
    // `batch.callData` — the contract treats callData as opaque
    // (only hashes it for proof binding, `EEZ.sol:596`), so this
    // is a follower-only DA channel.
    use alloy_sol_types::SolValue as _;
    // DA sidecar = the full derivation entry set in canonical order: OUTBOUND
    // settlement entries (proxyEntryHash==0, populated l2ToL1Calls) FIRST, then
    // inbound deferred entries — outbound-first matches the deriver's prefix split.
    let l2_entries_bytes: Vec<Vec<u8>> = outbound_entries
        .iter()
        .map(eez_protocol::abi::ExecutionEntrySol::abi_encode)
        .chain(
            compositions
                .iter()
                .flat_map(|c| c.targets.iter())
                .flat_map(|t| t.batch.entries.iter())
                .map(eez_protocol::abi::ExecutionEntrySol::abi_encode),
        )
        .collect();
    eez_payload_codec::encode(&blocks, &l2_entries_bytes)
        .map_err(|e| format!("eez_payload_codec::encode: {e}"))
}

/// Collect per-block witnesses for the proving window `[from..=sync]`:
/// store reads for the committed intermediates, in-memory capture for
/// the just-built endpoint. Mock mode (`witness_source` = `None`) →
/// empty.
pub(crate) async fn collect_window_witnesses<L2>(
    witness_source: Option<&Arc<dyn eez_prover::ProvingWitnessSource>>,
    l2_provider: &Arc<L2>,
    evm_config: &EthEvmConfig,
    from: u64,
    sync_block_number: u64,
    sync_block: &reth_primitives_traits::RecoveredBlock<reth_ethereum_primitives::Block>,
) -> Result<Vec<BlockWitness>, String>
where
    L2: StateProviderFactory
        + HeaderProvider<Header = alloy_consensus::Header>
        + Send
        + Sync
        + 'static,
{
    let block_witnesses = match witness_source {
        // Remote-prover mode. Intermediate blocks `[from..sync)` are committed
        // (served by the witness store); the just-built endpoint isn't, so
        // capture it here from the in-memory block.
        Some(src) => {
            // Witness generation is a CPU-heavy trie walk / re-exec. Run it on
            // the blocking pool so it can't stall async worker threads on the
            // settlement path. (Store hits are cheap; the rare store miss and
            // the endpoint capture are the heavy parts.)
            let src = Arc::clone(src);
            let l2_provider = Arc::clone(l2_provider);
            let evm_config = evm_config.clone();
            let terminal_block = sync_block.clone();
            tokio::task::spawn_blocking(move || -> Result<Vec<BlockWitness>, String> {
                let mut ws = (from..sync_block_number)
                    .map(|n| src.block_witness(n))
                    .collect::<Result<Vec<_>, String>>()
                    .map_err(|e| format!("witness_source: {e}"))?;
                // Endpoint (the just-built, uncommitted Sync block) is captured
                // in-memory — no store or provider can serve an uncommitted block.
                ws.push(
                    block_witness(
                        l2_provider.as_ref(),
                        &evm_config,
                        &terminal_block,
                        ExecutionWitnessMode::Legacy,
                    )
                    .map_err(|e| {
                        format!(
                            "terminal-block witness (block {}): {e}",
                            terminal_block.header().number()
                        )
                    })?,
                );
                Ok(ws)
            })
            .await
            .map_err(|e| format!("witness spawn_blocking join: {e}"))??
        }
        // Mock mode: the mock prover ignores per-block witnesses.
        None => Vec::new(),
    };
    Ok(block_witnesses)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::I256;
    use eez_protocol::{
        Composition, RollupId, SourceComposition, TargetComposition, rolling_hash::EntryRollingHash,
    };

    use super::*;

    const RID: u64 = 7;

    fn rid() -> U256 {
        U256::from(RID)
    }

    fn root(tag: u8) -> B256 {
        B256::repeat_byte(tag)
    }

    /// Minimal cross-chain effect entry: empty `stateDeltas`, direction
    /// picked by `proxy_entry_hash` (zero = outbound settlement, non-zero
    /// = inbound deferred).
    fn effect_entry(proxy_entry_hash: B256) -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: proxy_entry_hash,
            destinationRollupId: rid(),
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    fn value_call(value: U256) -> eez_protocol::abi::L2ToL1CallSol {
        eez_protocol::abi::L2ToL1CallSol {
            targetAddress: Address::ZERO,
            value,
            data: Bytes::new(),
            sourceAddress: Address::ZERO,
            sourceRollupId: rid(),
            revertSpan: U256::ZERO,
        }
    }

    /// Outbound settlement entry carrying value `v` as the supported
    /// single top-level call, with the rolling hash
    /// `outbound_ether_out` recovers the success flag from.
    fn outbound_value_entry(v: U256, success: bool) -> eez_protocol::abi::ExecutionEntrySol {
        let mut e = effect_entry(B256::ZERO);
        e.l2ToL1Calls = vec![value_call(v)];
        e.callCount = U256::from(1u8);
        let mut rolling = EntryRollingHash::new();
        rolling.call_begin(1);
        rolling.call_end(1, success, &e.returnData);
        e.rollingHash = B256::from(rolling.current());
        e
    }

    fn batch_with_entries(
        entries: Vec<eez_protocol::abi::ExecutionEntrySol>,
    ) -> eez_protocol::EvmBatch {
        eez_protocol::EvmBatch {
            entries,
            ..Default::default()
        }
    }

    /// Composition whose source batch carries `source_entries` (the lean
    /// L1-shape entries) and whose single target batch carries
    /// `target_entries` (the L2-shape DA sidecar).
    fn composition(
        source_entries: Vec<eez_protocol::abi::ExecutionEntrySol>,
        target_entries: Vec<eez_protocol::abi::ExecutionEntrySol>,
    ) -> Composition {
        Composition {
            source: SourceComposition {
                rollup_id: RollupId(RID),
                batch: batch_with_entries(source_entries),
                entry_payload: Vec::new(),
            },
            targets: vec![TargetComposition {
                rollup_id: RollupId(RID),
                batch: batch_with_entries(target_entries),
                load_table_payload: Vec::new(),
                execute_payload: Vec::new(),
            }],
        }
    }

    // ── assemble_batch ──────────────────────────────────────────────

    #[test]
    fn assemble_empty_compositions_is_anchor_only() {
        let batch = assemble_batch(&[], rid(), root(0xAA), root(0xBB), &[]);
        assert_eq!(batch.entries.len(), 1);
        let anchor = &batch.entries[0];
        assert_eq!(anchor.proxyEntryHash, B256::ZERO);
        assert_eq!(anchor.destinationRollupId, rid());
        assert_eq!(anchor.stateDeltas.len(), 1);
        let delta = &anchor.stateDeltas[0];
        assert_eq!(delta.rollupId, rid());
        assert_eq!(delta.currentState, root(0xAA));
        assert_eq!(delta.newState, root(0xBB));
        assert_eq!(delta.etherDelta, I256::ZERO);
        assert!(batch.l1ToL2lookupCalls.is_empty());
    }

    #[test]
    fn assemble_merges_compositions_in_order() {
        let c0 = composition(vec![effect_entry(root(0x01))], vec![]);
        let c1 = composition(
            vec![effect_entry(root(0x02)), effect_entry(root(0x03))],
            vec![],
        );
        let batch = assemble_batch(&[&c0, &c1], rid(), root(0xAA), root(0xBB), &[]);
        let hashes: Vec<B256> = batch.entries.iter().map(|e| e.proxyEntryHash).collect();
        assert_eq!(hashes, vec![B256::ZERO, root(0x01), root(0x02), root(0x03)]);
    }

    #[test]
    fn assemble_splices_outbound_after_anchor_and_stamps_dest() {
        let c = composition(
            vec![effect_entry(root(0x01)), effect_entry(root(0x02))],
            vec![],
        );
        // Outbound entries arrive with the call's MAINNET(0) destination;
        // the splice must rewrite it to this rollup's registry id.
        let mut out0 = effect_entry(B256::ZERO);
        out0.destinationRollupId = U256::ZERO;
        out0.returnData = Bytes::from(vec![0x42]); // distinguishes out0 from out1
        let mut out1 = effect_entry(B256::ZERO);
        out1.destinationRollupId = U256::ZERO;
        let batch = assemble_batch(&[&c], rid(), root(0xAA), root(0xBB), &[out0, out1]);
        // Exact order: `[anchor | outbound… | inbound…]`.
        assert_eq!(batch.entries.len(), 5);
        assert!(!batch.entries[0].stateDeltas.is_empty(), "anchor first");
        assert_eq!(batch.entries[1].proxyEntryHash, B256::ZERO);
        assert_eq!(batch.entries[1].returnData, Bytes::from(vec![0x42]));
        assert_eq!(batch.entries[2].proxyEntryHash, B256::ZERO);
        assert_eq!(batch.entries[3].proxyEntryHash, root(0x01));
        assert_eq!(batch.entries[4].proxyEntryHash, root(0x02));
        // dest stamped to the registry id on both spliced entries.
        assert_eq!(batch.entries[1].destinationRollupId, rid());
        assert_eq!(batch.entries[2].destinationRollupId, rid());
    }

    // ── inbound_ether_map ───────────────────────────────────────────

    #[test]
    fn inbound_ether_map_reads_target_sidecar_values() {
        let mut valued = effect_entry(root(0x01));
        valued.l2ToL1Calls = vec![value_call(U256::from(9u64))];
        let value_free = effect_entry(root(0x02));
        let c = composition(vec![], vec![valued, value_free]);
        let map = inbound_ether_map(&[&c]);
        assert_eq!(map.len(), 1);
        assert_eq!(map[&root(0x01)], I256::try_from(U256::from(9u64)).unwrap());
    }

    // ── stitch_settlement ───────────────────────────────────────────

    /// `[anchor | outbound | inbound]` batch stitched with per-effect
    /// roots `[R0, R1]` where `R1` doubles as the final Sync-block root.
    fn stitched_three_entry_batch() -> eez_protocol::EvmBatch {
        let c = composition(vec![effect_entry(root(0x01))], vec![]);
        let out = effect_entry(B256::ZERO);
        let mut batch = assemble_batch(&[&c], rid(), root(0xAA), root(0xBB), &[out]);
        stitch_settlement(
            &mut batch,
            rid(),
            &[root(0x51), root(0x52)],
            &HashMap::new(),
            root(0x52),
        )
        .unwrap();
        batch
    }

    #[test]
    fn stitch_fills_new_state_from_pair_roots_in_order() {
        let batch = stitched_three_entry_batch();
        assert_eq!(batch.entries[1].stateDeltas[0].newState, root(0x51));
        assert_eq!(batch.entries[2].stateDeltas[0].newState, root(0x52));
    }

    #[test]
    fn stitch_chains_current_state_and_ends_at_final_root() {
        let batch = stitched_three_entry_batch();
        // The anchor keeps its own pre-state anchor…
        assert_eq!(batch.entries[0].stateDeltas[0].currentState, root(0xAA));
        assert_eq!(batch.entries[0].stateDeltas[0].newState, root(0xBB));
        // …each later entry chains to the prior entry's newState…
        assert_eq!(batch.entries[1].stateDeltas[0].currentState, root(0xBB));
        assert_eq!(batch.entries[2].stateDeltas[0].currentState, root(0x51));
        // …and the chain ends at the final Sync-block root.
        let last = batch.entries.last().unwrap();
        assert_eq!(last.stateDeltas.last().unwrap().newState, root(0x52));
    }

    #[test]
    fn stitch_rejects_more_effect_entries_than_roots() {
        let c = composition(
            vec![effect_entry(root(0x01)), effect_entry(root(0x02))],
            vec![],
        );
        let mut batch = assemble_batch(&[&c], rid(), root(0xAA), root(0xBB), &[]);
        let err = stitch_settlement(
            &mut batch,
            rid(),
            &[root(0x51)],
            &HashMap::new(),
            root(0x51),
        )
        .unwrap_err();
        assert!(err.contains("pair-end/entry misalignment"), "{err}");
    }

    #[test]
    fn stitch_rejects_more_roots_than_effect_entries() {
        let c = composition(vec![effect_entry(root(0x01))], vec![]);
        let mut batch = assemble_batch(&[&c], rid(), root(0xAA), root(0xBB), &[]);
        let err = stitch_settlement(
            &mut batch,
            rid(),
            &[root(0x51), root(0x52)],
            &HashMap::new(),
            root(0x52),
        )
        .unwrap_err();
        assert!(
            err.contains("1 effect entries but 2 per-effect roots"),
            "{err}"
        );
    }

    #[test]
    fn stitch_anchor_only_fallback_writes_final_root() {
        let mut batch = assemble_batch(&[], rid(), root(0xAA), root(0xBB), &[]);
        stitch_settlement(&mut batch, rid(), &[], &HashMap::new(), root(0xCC)).unwrap();
        let delta = &batch.entries[0].stateDeltas[0];
        assert_eq!(delta.currentState, root(0xAA));
        // parent.stateRoot (0xBB) is replaced by the re-executed final root.
        assert_eq!(delta.newState, root(0xCC));
    }

    #[test]
    fn stitch_ether_deltas_by_direction() {
        // Outbound value V with a successful single call → −V; inbound
        // with a sidecar-map hit → +V; inbound without → 0.
        let five = U256::from(5u64);
        let c = composition(
            vec![effect_entry(root(0x01)), effect_entry(root(0x02))],
            vec![],
        );
        let out = outbound_value_entry(five, true);
        let mut batch = assemble_batch(&[&c], rid(), root(0xAA), root(0xBB), &[out]);
        let inbound_ether = HashMap::from([(root(0x01), I256::try_from(five).unwrap())]);
        stitch_settlement(
            &mut batch,
            rid(),
            &[root(0x51), root(0x52), root(0x53)],
            &inbound_ether,
            root(0x53),
        )
        .unwrap();
        assert_eq!(
            batch.entries[1].stateDeltas[0].etherDelta,
            -I256::try_from(five).unwrap()
        );
        assert_eq!(
            batch.entries[2].stateDeltas[0].etherDelta,
            I256::try_from(five).unwrap()
        );
        assert_eq!(batch.entries[3].stateDeltas[0].etherDelta, I256::ZERO);
    }

    #[test]
    fn stitch_rejects_multi_call_value_outbound() {
        let mut out = effect_entry(B256::ZERO);
        out.l2ToL1Calls = vec![value_call(U256::from(1u64)), value_call(U256::from(2u64))];
        out.callCount = U256::from(2u8);
        let mut batch = assemble_batch(&[], rid(), root(0xAA), root(0xBB), &[out]);
        let err = stitch_settlement(
            &mut batch,
            rid(),
            &[root(0x51)],
            &HashMap::new(),
            root(0x51),
        )
        .unwrap_err();
        assert!(err.contains("multi-call value not supported"), "{err}");
    }

    // ── attach_proof_carriers ───────────────────────────────────────

    #[test]
    fn attach_proof_carriers_sets_transient_count_and_carriers() {
        let addr = Address::repeat_byte(0x11);
        let mut batch = assemble_batch(&[], rid(), root(0xAA), root(0xBB), &[]);
        attach_proof_carriers(&mut batch, rid(), 2, addr, RID).unwrap();
        // 1 anchor immediate + N outbound immediates.
        assert_eq!(batch.transientExecutionEntryCount, U256::from(3u64));
        assert_eq!(batch.proofSystems, vec![addr]);
        assert_eq!(batch.rollupIdsWithProofSystems.len(), 1);
        assert_eq!(batch.rollupIdsWithProofSystems[0].rollupId, rid());
        assert_eq!(
            batch.rollupIdsWithProofSystems[0].proofSystemIndex,
            vec![0u64]
        );
    }

    #[test]
    fn attach_proof_carriers_rejects_non_registry_dest() {
        let mut foreign = effect_entry(root(0x01));
        foreign.destinationRollupId = U256::from(999u64);
        let mut batch = batch_with_entries(vec![foreign]);
        let err = attach_proof_carriers(&mut batch, rid(), 0, Address::ZERO, RID).unwrap_err();
        assert!(err.contains("not the configured registry id"), "{err}");
    }
}
