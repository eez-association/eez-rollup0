use std::collections::{HashMap, HashSet};

use alloy_consensus::Transaction;
use alloy_consensus::transaction::TxHashRef;
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent};
use eez_protocol::abi::{
    BatchPosted, ExecutionConsumed, L2ExecutionPerformed, L2TxSkipped,
    ProofSystemBatchPerVerificationEntriesSol, postAndVerifyBatchCall,
};
use tracing::{Level, event};

use crate::error::{L1Error, L1Result};

/// Initial block span for historical log scans. Wide catch-up gaps are
/// split before hitting RPCs that reject long `eth_getLogs` ranges.
pub(crate) const LOG_SCAN_CHUNK_BLOCKS: u64 = 100_000;

/// One log payload, tagged with where it sat in the L1 block: two composers can
/// post for one rollup in a block, so position decides which batch owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Positioned<T> {
    tx_index: u64,
    log_index: u64,
    block_hash: B256,
    payload: T,
}

/// A root L1 stored for our rollup.
type SettledRoot = Positioned<B256>;

/// A deferred entry L1 consumed, by its position in the rollup's entry queue.
type ConsumedEntry = Positioned<(u64, B256)>;

/// A decoded `BatchPosted` log, before settlement attribution. Held between the
/// scan's two passes: windows need every batch in the block decoded first.
#[derive(Debug)]
struct DecodedBatchLog {
    l1_block_number: u64,
    l1_block_hash: B256,
    tx_hash: B256,
    tx_index: u64,
    submitter: Address,
    /// This batch lists our rollup, so it wiped our queue — a window boundary.
    verifies_our_rollup: bool,
    /// Kept whole — it carries the calldata too, so no second copy is held.
    /// Attribution classifies entries by role and maps queue slots back to them.
    batch: ProofSystemBatchPerVerificationEntriesSol,
}

/// Stateful `BatchPosted` log chunks. Callers own when scanned ranges are
/// committed to their local cursors.
#[derive(Debug)]
pub struct BatchLogChunks {
    to_block: u64,
    ranges: Vec<(u64, u64)>,
}

impl BatchLogChunks {
    pub(crate) fn new(from_block: u64, to_block: u64) -> Self {
        let ranges = if from_block > to_block {
            Vec::new()
        } else {
            initial_log_scan_ranges(from_block, to_block)
        };
        Self { to_block, ranges }
    }

    /// L1 block these chunks were bounded to when created.
    #[must_use]
    pub const fn to_block(&self) -> u64 {
        self.to_block
    }

    /// Returns true when no scan chunks remain.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

/// One decoded `BatchPosted` log: winner flag plus the claimed state
/// roots from our rollup's `StateUpdate`. The Deriver's catch-up scan and
/// the live [`L1Watcher`](crate::L1Watcher) poll consume the same shape.
#[derive(Debug, Clone)]
pub struct ScannedBatch {
    pub l1_block_number: u64,
    /// Hash of the L1 block the batch landed in — canonicality probe
    /// for the resync anchor walk.
    pub l1_block_hash: B256,
    pub tx_hash: B256,
    pub submitter: Address,
    pub call_data: Bytes,
    pub state_applied: bool,
    /// Which of this batch's claimed steps L1 actually ran. See [`Settlement`].
    pub settlement: Settlement,
}

/// What an applied entry contributes to the Sync block. `ordinal` ranks within
/// its own direction — the DA layout — so a skipped entry never shifts the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryRole {
    /// State-only: moves the commitment but signs no system tx.
    Anchor,
    /// L2→L1 settlement — its DA slot and its Sync-block user tx.
    Outbound { ordinal: usize },
    /// L1→L2 delivery.
    Inbound { ordinal: usize },
}

/// One entry L1 applied, with what it contributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedEntry {
    /// Index into the on-chain `batch.entries`.
    pub entry_index: usize,
    pub role: EntryRole,
}

/// Classify every entry in array order, counting ordinals per direction.
/// `proxyEntryHash` must be tested first: inbound entries carry no `l2ToL1Calls`.
fn classify_entries(batch: &ProofSystemBatchPerVerificationEntriesSol) -> Vec<EntryRole> {
    let mut outbound = 0;
    let mut inbound = 0;
    batch
        .entries
        .iter()
        .map(|entry| {
            if entry.proxyEntryHash != B256::ZERO {
                let ordinal = inbound;
                inbound += 1;
                EntryRole::Inbound { ordinal }
            } else if entry.l2ToL1Calls.is_empty() {
                EntryRole::Anchor
            } else {
                let ordinal = outbound;
                outbound += 1;
                EntryRole::Outbound { ordinal }
            }
        })
        .collect()
}

/// End of the leading `proxyEntryHash == 0` run: the entries EEZ.sol runs
/// inline, and so the only ones that can emit `L2TxSkipped`.
fn immediate_run_end(batch: &ProofSystemBatchPerVerificationEntriesSol, immediate: usize) -> usize {
    batch
        .entries
        .iter()
        .take(immediate)
        .take_while(|entry| entry.proxyEntryHash == B256::ZERO)
        .count()
}

/// Queue slot -> entry index: `ExecutionConsumed.entryQueueIndex` indexes the
/// rollup's queue, not `batch.entries`. That queue is per-batch despite being
/// storage: every verify wipes it and zeroes the cursor before the batch pushes,
/// so slot 0 is this batch's first queued entry. `_saveRemainderEntries` starts
/// at `immediateEntryCount`, so the prefix never appears here.
fn deferred_queue_map(
    batch: &ProofSystemBatchPerVerificationEntriesSol,
    rollup_id: u64,
    immediate: usize,
) -> Vec<usize> {
    batch
        .entries
        .iter()
        .enumerate()
        .skip(immediate)
        .filter(|(_, entry)| entry.destinationRollupId == rollup_id)
        .map(|(index, _)| index)
        .collect()
}

/// Transient slot -> entry index. A contract poster trips EEZ.sol's meta-hook
/// (`i < immediateEntryCount && msg.sender.code.length > 0`), which loads
/// `entries[run_end..immediate]` into `_transientEntries` and routes consumption
/// through THAT table. Unfiltered, because the transient cursor is global: other
/// rollups' entries occupy slots too.
fn transient_prefix_index(run_end: usize, immediate: usize, slot: usize) -> Option<usize> {
    let index = run_end.checked_add(slot)?;
    (index < immediate).then_some(index)
}

/// What L1 emitted inside one batch's window, before it is matched to entries.
#[derive(Debug, Default)]
struct ConsumptionEvidence {
    /// `L2ExecutionPerformed.newState`, in emission order.
    observed: Vec<B256>,
    /// `(entryQueueIndex, crossChainCallHash)` per consumed deferred entry.
    consumed: Vec<(u64, B256)>,
    /// Entry indices of inline immediates L1 skipped.
    skipped_immediates: Vec<usize>,
}

/// Which of a batch's ENTRIES L1 applied, read from its own consumption events
/// rather than inferred from the roots it emitted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Settlement {
    /// Ascending entry index, which is application order: both cursors only move
    /// forward, and the three windows run in index order — inline immediates,
    /// then the transient prefix, then the queue, which this batch does not even
    /// populate until after its own meta-hook has returned.
    applied: Vec<AppliedEntry>,
    /// `newState` of the last entry that applied — L1's actual commitment, and
    /// the only valid reconciliation endpoint under partial consumption.
    pub final_state: Option<B256>,
    /// The stored commitment the applied run STARTED from. On a mid-chain
    /// resume this is a competing batch's endpoint, not this batch's claim.
    pub entry_state: Option<B256>,
}

impl Settlement {
    /// Nothing of this batch applied on L1.
    pub const NONE: Self = Self {
        applied: Vec::new(),
        final_state: None,
        entry_state: None,
    };

    /// Build from an ascending applied-entry list.
    #[must_use]
    pub fn new(
        applied: Vec<AppliedEntry>,
        final_state: Option<B256>,
        entry_state: Option<B256>,
    ) -> Self {
        debug_assert!(
            applied
                .windows(2)
                .all(|w| w[0].entry_index < w[1].entry_index),
            "applied entry indices must be strictly ascending: {applied:?}",
        );
        Self {
            applied,
            final_state,
            entry_state,
        }
    }

    /// True when L1 ran none of this batch's entries — the claimed commitments
    /// are phantoms and the batch must be skipped entirely.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    /// The applied entries, in application order.
    #[must_use]
    pub fn applied(&self) -> &[AppliedEntry] {
        &self.applied
    }

    /// Just the applied entry indices, for assertions and diagnostics.
    #[must_use]
    pub fn applied_indices(&self) -> Vec<usize> {
        self.applied.iter().map(|entry| entry.entry_index).collect()
    }

    /// Lowest applied entry index. Diagnostics; see [`Settlement::resumed`].
    #[must_use]
    pub fn first_applied(&self) -> Option<usize> {
        self.applied.first().map(|entry| entry.entry_index)
    }

    /// A mid-chain resume: entries applied but not index 0. Positional, not
    /// role-derived — a peer batch need not lead with an anchor.
    #[must_use]
    pub fn resumed(&self) -> bool {
        self.first_applied().is_some_and(|first| first > 0)
    }

    /// L1 ran at least one entry that puts content in the Sync block. The anchor
    /// claims the block BEFORE it, so an anchor-only prefix is reorged, not kept.
    #[must_use]
    pub fn applied_an_effect(&self) -> bool {
        self.applied
            .iter()
            .any(|entry| entry.role != EntryRole::Anchor)
    }
}

fn initial_log_scan_ranges(from_block: u64, to_block: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut from = from_block;
    loop {
        let to = from
            .saturating_add(LOG_SCAN_CHUNK_BLOCKS.saturating_sub(1))
            .min(to_block);
        ranges.push((from, to));
        if to == to_block {
            break;
        }
        from = to + 1;
    }
    ranges.reverse();
    ranges
}

/// True when a `get_logs` failure means "matched too much", not "was wrong".
/// Providers cap RESULT COUNT, which no fixed block span can respect. Detected by
/// message, since the wording is client-specific but the remedy — narrow — isn't.
fn is_range_too_wide(err: &L1Error) -> bool {
    let L1Error::Provider(msg) = err else {
        return false;
    };
    let m = msg.to_ascii_lowercase();
    m.contains("exceeds max results")
        || m.contains("query returned more than")
        || m.contains("more than 10000 results")
        || m.contains("log response size exceeded")
        || m.contains("response size exceeded")
        || m.contains("query timeout exceeded")
        || m.contains("range is too large")
        || m.contains("block range too large")
        || m.contains("too many results")
}

/// Scan `[from, to]`, halving the upper bound until the provider accepts. Returns
/// the batches plus the block actually REACHED, which may be `< to` — callers must
/// advance their cursor by that, not by `to`. Propagating the refusal instead makes
/// no progress: the Watcher would retry the identical over-wide range forever.
pub(crate) async fn scan_batch_logs_range_adaptive(
    provider: &impl Provider,
    eez: Address,
    rollup_id: u64,
    from: u64,
    to: u64,
) -> L1Result<(Vec<ScannedBatch>, u64)> {
    let mut hi = to;
    loop {
        match scan_batch_logs_range(provider, eez, rollup_id, from, hi).await {
            Ok(scanned) => return Ok((scanned, hi)),
            Err(err) if is_range_too_wide(&err) && hi > from => {
                let mid = from + (hi - from) / 2;
                event!(
                    name: "eez.l1.scan_batch_logs.chunk_narrowed",
                    Level::WARN,
                    from,
                    requested_to = hi,
                    narrowed_to = mid,
                    error = %err,
                    "get_logs matched more than the provider serves; halving the range and retrying",
                );
                hi = mid;
            }
            // A single block that still exceeds the cap is genuinely unservable.
            Err(err) => return Err(err),
        }
    }
}

pub(crate) async fn scan_next_batch_log_chunk(
    provider: &impl Provider,
    eez: Address,
    rollup_id: u64,
    chunks: &mut BatchLogChunks,
) -> L1Result<Option<Vec<ScannedBatch>>> {
    let Some(&(from, to)) = chunks.ranges.last() else {
        return Ok(None);
    };

    let (scanned, reached) =
        scan_batch_logs_range_adaptive(provider, eez, rollup_id, from, to).await?;
    chunks.ranges.pop();
    if reached < to {
        // Narrowed: re-queue the tail so no range is silently skipped.
        chunks.ranges.push((reached + 1, to));
    }
    Ok(Some(scanned))
}

/// Fetch every `BatchPosted` log in `[from_block, to_block]` and cross-
/// reference each against `L2ExecutionPerformed` for our rollup — present
/// ⇔ this batch's state delta applied (winner; losers emit `BatchPosted`
/// only). For each, decode the originating tx for the submitter, callData
/// and our rollup's claimed state roots.
pub(crate) async fn scan_batch_logs_range(
    provider: &impl Provider,
    eez: Address,
    rollup_id: u64,
    from_block: u64,
    to_block: u64,
) -> L1Result<Vec<ScannedBatch>> {
    let filter = Filter::new()
        .address(eez)
        .event_signature(BatchPosted::SIGNATURE_HASH)
        .from_block(from_block)
        .to_block(BlockNumberOrTag::Number(to_block));
    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(BatchPosted): {e}")))?;

    let winners_filter = Filter::new()
        .address(eez)
        .event_signature(L2ExecutionPerformed::SIGNATURE_HASH)
        .topic1(U256::from(rollup_id))
        .from_block(from_block)
        .to_block(BlockNumberOrTag::Number(to_block));
    let winner_logs = provider
        .get_logs(&winners_filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(L2ExecutionPerformed): {e}")))?;
    // Pinned by block hash too: the same tx can sit on both sides of a fork.
    let winner_tx_hashes: HashSet<(B256, B256)> = winner_logs
        .iter()
        .filter_map(|l| Some((l.block_hash?, l.transaction_hash?)))
        .collect();
    // Settled roots per L1 block, in emission order and tagged with the emitting
    // tx index. Per-TX-INDEX because two composers can post for the same rollup
    // in one L1 block: crediting a batch with the whole block's roots would hand
    // a loser its rival's settlement.
    let mut settled_by_block: HashMap<u64, Vec<SettledRoot>> = HashMap::new();
    for l in &winner_logs {
        let (Some(bn), Some(tx_index)) = (l.block_number, l.transaction_index) else {
            continue;
        };
        let block_hash = l.block_hash.ok_or_else(|| {
            L1Error::Provider("L2ExecutionPerformed log missing block_hash".into())
        })?;
        let data = l.data().data.as_ref();
        if data.len() == 32 {
            settled_by_block.entry(bn).or_default().push(SettledRoot {
                tx_index,
                log_index: l.log_index.unwrap_or_default(),
                block_hash,
                payload: B256::from_slice(data),
            });
        }
    }
    for roots in settled_by_block.values_mut() {
        roots.sort_by_key(|r| (r.tx_index, r.log_index));
    }

    // Both topics in ONE `get_logs`, so the two readings can never straddle
    // different chain views.
    let consumption_filter = Filter::new()
        .address(eez)
        .event_signature(vec![
            ExecutionConsumed::SIGNATURE_HASH,
            L2TxSkipped::SIGNATURE_HASH,
        ])
        .from_block(from_block)
        .to_block(BlockNumberOrTag::Number(to_block));
    let consumption_logs = provider
        .get_logs(&consumption_filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(consumption): {e}")))?;

    let mut consumed_by_block: HashMap<u64, Vec<ConsumedEntry>> = HashMap::new();
    // `L2TxSkipped(i)` labels the posting tx's OWN `batch.entries` (EEZ.sol
    // step 5), so a rival's indices name different entries and must not merge.
    let mut skipped_by_tx: HashMap<(B256, B256), HashSet<usize>> = HashMap::new();
    for l in &consumption_logs {
        let (Some(bn), Some(tx_index), Some(block_hash), Some(tx_hash)) = (
            l.block_number,
            l.transaction_index,
            l.block_hash,
            l.transaction_hash,
        ) else {
            continue;
        };
        match l.topic0() {
            Some(t) if *t == ExecutionConsumed::SIGNATURE_HASH => {
                let decoded = ExecutionConsumed::decode_log(&alloy_primitives::Log {
                    address: l.address(),
                    data: l.data().clone(),
                })
                .map_err(|e| L1Error::Decode(format!("decode ExecutionConsumed: {e}")))?;
                if decoded.rollupId != rollup_id {
                    continue;
                }
                let slot = u64::try_from(decoded.entryQueueIndex).map_err(|_| {
                    L1Error::Decode("ExecutionConsumed queue index overflows u64".into())
                })?;
                consumed_by_block
                    .entry(bn)
                    .or_default()
                    .push(ConsumedEntry {
                        tx_index,
                        log_index: l.log_index.unwrap_or_default(),
                        block_hash,
                        payload: (slot, decoded.crossChainCallHash),
                    });
            }
            Some(t) if *t == L2TxSkipped::SIGNATURE_HASH => {
                let decoded = L2TxSkipped::decode_log(&alloy_primitives::Log {
                    address: l.address(),
                    data: l.data().clone(),
                })
                .map_err(|e| L1Error::Decode(format!("decode L2TxSkipped: {e}")))?;
                let index = usize::try_from(decoded.transientIdx).map_err(|_| {
                    L1Error::Decode("L2TxSkipped transient index overflows usize".into())
                })?;
                skipped_by_tx
                    .entry((block_hash, tx_hash))
                    .or_default()
                    .insert(index);
            }
            _ => {}
        }
    }
    for entries in consumed_by_block.values_mut() {
        entries.sort_by_key(|c| (c.tx_index, c.log_index));
    }

    // Pass 1: decode every postBatch. Windows can't be computed until we know
    // which OTHER batches in the same block touch our rollup — each of those
    // wipes our queue (`EEZ.sol:_markVerifiedBlockPerRollup`), ending the
    // previous batch's consumption window.
    let mut decoded_batches: Vec<DecodedBatchLog> = Vec::with_capacity(logs.len());
    for log in &logs {
        let l1_block_number = log
            .block_number
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing block_number".into()))?;
        let l1_block_hash = log
            .block_hash
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing block_hash".into()))?;
        let tx_hash = log
            .transaction_hash
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing tx_hash".into()))?;
        // Fetch the postBatch tx by (block hash, index) — a minimal node has no
        // tx-hash index, and (block NUMBER, index) alone can straddle a reorg.
        let tx_index = log
            .transaction_index
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing transaction_index".into()))?;
        let tx = fetch_log_transaction(provider, l1_block_number, l1_block_hash, tx_index, tx_hash)
            .await?;
        let submitter = tx.inner.signer();
        let input = tx.inner.input();
        // `BatchPosted` carries rollupCount, not rollupId, so we decode every
        // rollup's batch — and a peer posting via a router yields undecodable
        // input. Skip unless it settled OUR rollup (invariant 8).
        let decoded = match postAndVerifyBatchCall::abi_decode(input) {
            Ok(decoded) => decoded,
            Err(e) if !winner_tx_hashes.contains(&(l1_block_hash, tx_hash)) => {
                event!(
                    name: "eez.l1_scan.foreign_batch_undecodable",
                    Level::WARN,
                    l1_block_number,
                    tx_hash = %tx_hash,
                    error = %e,
                    "skipping an undecodable postBatch that did not settle our rollup",
                );
                continue;
            }
            Err(e) => {
                return Err(L1Error::Decode(format!("decode postBatch({tx_hash}): {e}")));
            }
        };
        let _decoded_event = BatchPosted::decode_log(&alloy_primitives::Log {
            address: log.address(),
            data: log.data().clone(),
        })
        .map_err(|e| L1Error::Decode(format!("decode BatchPosted({tx_hash}): {e}")))?;
        decoded_batches.push(DecodedBatchLog {
            l1_block_number,
            l1_block_hash,
            tx_hash,
            tx_index,
            submitter,
            // A batch verifies (and therefore wipes) our rollup iff it lists it
            // — the same array `postAndVerifyBatch` loops over to mark verified.
            verifies_our_rollup: decoded
                .batch
                .rollupIdsWithProofSystems
                .iter()
                .any(|r| r.rollupId == rollup_id),
            batch: decoded.batch,
        });
    }

    // Window boundaries: tx indices, per block, of the postBatches that verify
    // our rollup. A batch owns the settled roots emitted from its own tx up to
    // (excluding) the next such boundary — its entries are provably dead past
    // that point, queue wiped.
    let mut boundaries: HashMap<(u64, B256), Vec<u64>> = HashMap::new();
    for b in &decoded_batches {
        if b.verifies_our_rollup {
            boundaries
                .entry((b.l1_block_number, b.l1_block_hash))
                .or_default()
                .push(b.tx_index);
        }
    }
    for idxs in boundaries.values_mut() {
        idxs.sort_unstable();
    }

    // Pass 2: attribute each batch its own window, then match steps.
    let mut out: Vec<ScannedBatch> = Vec::with_capacity(decoded_batches.len());
    for b in decoded_batches {
        let window_end = boundaries
            .get(&(b.l1_block_number, b.l1_block_hash))
            .and_then(|idxs| idxs.iter().copied().find(|&i| i > b.tx_index))
            .unwrap_or(u64::MAX);
        // Roots at this height on a DIFFERENT hash mean the two log queries
        // straddled a reorg; retry rather than silently settling empty.
        let block_roots = settled_by_block.get(&b.l1_block_number);
        if block_roots.is_some_and(|roots| roots.iter().all(|r| r.block_hash != b.l1_block_hash)) {
            return Err(L1Error::SourceIncomplete {
                block: b.l1_block_number,
                tx_hash: b.tx_hash,
                detail: "settlement logs are from another fork of this block; retry".into(),
            });
        }
        // Consumption is a SEPARATE `get_logs`: silently dropping its off-fork
        // records leaves a terminal count mismatch from a transient cause.
        let block_consumed = consumed_by_block.get(&b.l1_block_number);
        if block_consumed.is_some_and(|cs| cs.iter().all(|c| c.block_hash != b.l1_block_hash)) {
            return Err(L1Error::SourceIncomplete {
                block: b.l1_block_number,
                tx_hash: b.tx_hash,
                detail: "consumption logs are from another fork of this block; retry".into(),
            });
        }
        // One window serves every event family: from this batch's own tx up to
        // the next postBatch that verifies our rollup, pinned to this fork.
        let evidence = ConsumptionEvidence {
            observed: block_roots
                .map(|roots| in_window(roots, b.tx_index, window_end, b.l1_block_hash))
                .unwrap_or_default(),
            consumed: block_consumed
                .map(|entries| in_window(entries, b.tx_index, window_end, b.l1_block_hash))
                .unwrap_or_default(),
            skipped_immediates: skipped_by_tx
                .get(&(b.l1_block_hash, b.tx_hash))
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default(),
        };
        // Every attribution failure halts the node, so name the locus: the
        // counts alone leave an operator nothing to grep L1 for.
        let settlement =
            attribute_settlement(&b.batch, rollup_id, &evidence).map_err(|e| match e {
                L1Error::Decode(msg) => L1Error::Decode(format!(
                    "{msg} (L1 block {} {}, postBatch {})",
                    b.l1_block_number, b.l1_block_hash, b.tx_hash,
                )),
                other => other,
            })?;
        out.push(ScannedBatch {
            l1_block_number: b.l1_block_number,
            l1_block_hash: b.l1_block_hash,
            tx_hash: b.tx_hash,
            submitter: b.submitter,
            call_data: b.batch.callData,
            state_applied: winner_tx_hashes.contains(&(b.l1_block_hash, b.tx_hash)),
            settlement,
        });
    }
    Ok(out)
}

/// Payloads inside this batch's tx-index window and on its own block hash.
/// `window_end` is the next postBatch verifying us, which wipes the queue.
fn in_window<T: Copy>(
    events: &[Positioned<T>],
    tx_index: u64,
    window_end: u64,
    block_hash: B256,
) -> Vec<T> {
    events
        .iter()
        .filter(|e| e.tx_index >= tx_index && e.tx_index < window_end && e.block_hash == block_hash)
        .map(|e| e.payload)
        .collect()
}

/// Which entries L1 applied, read from the events inside this batch's window.
/// Errors when the evidence names an entry the batch does not carry.
fn attribute_settlement(
    batch: &ProofSystemBatchPerVerificationEntriesSol,
    rollup_id: u64,
    evidence: &ConsumptionEvidence,
) -> L1Result<Settlement> {
    if evidence.observed.is_empty() {
        return Ok(Settlement::NONE);
    }
    let roles = classify_entries(batch);
    let immediate = usize::try_from(batch.immediateEntryCount).unwrap_or(usize::MAX);
    let run_end = immediate_run_end(batch, immediate);
    let queue = deferred_queue_map(batch, rollup_id, immediate);

    // Every immediate in the leading run ran except those L1 reported skipped.
    let mut applied: Vec<usize> = (0..run_end)
        .filter(|index| !evidence.skipped_immediates.contains(index))
        .collect();

    // Deferred entries: each consumption names a slot, but in one of two index
    // spaces — the rollup's persistent queue, or a contract poster's transient
    // prefix. The entry's own key picks the space, mirroring `_entryMatches`
    // on-chain. At most one candidate can match: a consumption always carries a
    // real call hash, and those are unique per entry (invariant 5).
    for &(slot, call_hash) in &evidence.consumed {
        let slot = usize::try_from(slot).unwrap_or(usize::MAX);
        let entry_index = [
            queue.get(slot).copied(),
            transient_prefix_index(run_end, immediate, slot),
        ]
        .into_iter()
        .flatten()
        .find(|&index| {
            batch.entries[index].proxyEntryHash == call_hash
                && batch.entries[index].destinationRollupId == rollup_id
        })
        .ok_or_else(|| {
            // No reading of the slot lands on an entry the event could name, so
            // the window picked up a rival's consumption and the whole
            // attribution is suspect.
            L1Error::Decode(format!(
                "ExecutionConsumed slot {slot} names call hash {call_hash}, which matches no \
                 entry of rollup {rollup_id} in this batch ({} queued, {} in the transient \
                 prefix)",
                queue.len(),
                immediate.saturating_sub(run_end),
            ))
        })?;
        applied.push(entry_index);
    }
    applied.sort_unstable();

    // Observed roots count exactly the entries that move OUR commitment, which
    // cross-checks the event evidence against the root evidence.
    let ours: Vec<usize> = applied
        .into_iter()
        .filter(|&index| {
            batch.entries[index]
                .stateUpdates
                .iter()
                .any(|update| update.rollupId == rollup_id)
        })
        .collect();
    // EEZ.sol rejects duplicate rollups within an entry's `stateUpdates`
    // (`StateUpdatesNotStrictlyIncreasing`), so one applied entry emits exactly
    // one root for us — a disagreement means the window was mis-attributed.
    //
    // Terminal like the other two evidence disagreements: the Deriver stops
    // rather than rebuild a block from a reading it cannot trust. It does NOT
    // spare the optimistic height — the composer's observer maps every error to
    // a failed bundle and rolls back regardless.
    if ours.len() != evidence.observed.len() {
        return Err(L1Error::Decode(format!(
            "consumption events name {} applied entries but {} roots settled for rollup {rollup_id}",
            ours.len(),
            evidence.observed.len(),
        )));
    }

    let applied: Vec<AppliedEntry> = ours
        .iter()
        .map(|&entry_index| AppliedEntry {
            entry_index,
            role: roles[entry_index],
        })
        .collect();
    // The run began at the commitment the first applied entry expected — its
    // own claim when it leads, else whatever a competing batch had reached.
    // `ours` only holds entries carrying an update for us, so this always finds.
    let entry_state = applied.first().and_then(|first| {
        batch.entries[first.entry_index]
            .stateUpdates
            .iter()
            .find(|update| update.rollupId == rollup_id)
            .map(|update| update.currentState)
    });
    Ok(Settlement::new(
        applied,
        evidence.observed.last().copied(),
        entry_state,
    ))
}

/// Fetches the postBatch tx by (block hash, index) — never by tx hash, which
/// a minimal node can't serve. The fetched tx's own hash must match the
/// log's, else a reorg swapped the block between the log fetch and this call.
async fn fetch_log_transaction(
    provider: &impl Provider,
    l1_block_number: u64,
    l1_block_hash: B256,
    tx_index: u64,
    tx_hash: B256,
) -> L1Result<alloy_rpc_types_eth::Transaction> {
    let Some(tx) = provider
        .get_transaction_by_block_hash_and_index(l1_block_hash, tx_index as usize)
        .await
        .map_err(|e| {
            L1Error::Provider(format!(
                "get_tx({l1_block_hash}#{tx_index} for {tx_hash}): {e}"
            ))
        })?
    else {
        return Err(L1Error::SourceIncomplete {
            block: l1_block_number,
            tx_hash,
            detail: format!("block-hash/index lookup returned null at tx index {tx_index}"),
        });
    };

    if *tx.inner.tx_hash() != tx_hash {
        return Err(L1Error::SourceIncomplete {
            block: l1_block_number,
            tx_hash,
            detail: format!(
                "tx at ({l1_block_hash}, {tx_index}) does not match the log's tx hash — reorg during scan; retry"
            ),
        });
    }
    Ok(tx)
}

#[cfg(test)]
mod tests {
    use super::{
        BatchLogChunks, EntryRole, LOG_SCAN_CHUNK_BLOCKS, SettledRoot, Settlement,
        attribute_settlement, classify_entries, fetch_log_transaction, in_window,
        initial_log_scan_ranges, scan_batch_logs_range, scan_next_batch_log_chunk,
    };
    use crate::error::L1Error;
    use alloy_consensus::transaction::TxHashRef;
    use alloy_primitives::{Address, B256, Bytes, I256, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_sol_types::{SolCall, SolEvent};
    use alloy_transport::mock::Asserter;
    use eez_protocol::abi::{
        BatchPosted, ExecutionEntrySol, L2ExecutionPerformed,
        ProofSystemBatchPerVerificationEntriesSol, RollupIdWithProofSystemsSol, StateUpdateSol,
        postAndVerifyBatchCall,
    };

    #[test]
    fn initial_log_scan_ranges_stack_order() {
        let c = LOG_SCAN_CHUNK_BLOCKS;
        struct Case {
            name: &'static str,
            from: u64,
            to: u64,
            stored_stack: Vec<(u64, u64)>,
            pop_order: Vec<(u64, u64)>,
        }

        let cases = vec![
            Case {
                name: "single block",
                from: 10,
                to: 10,
                stored_stack: vec![(10, 10)],
                pop_order: vec![(10, 10)],
            },
            Case {
                name: "exactly one chunk",
                from: 1,
                to: c,
                stored_stack: vec![(1, c)],
                pop_order: vec![(1, c)],
            },
            Case {
                name: "one block past a chunk",
                from: 1,
                to: c + 1,
                stored_stack: vec![(c + 1, c + 1), (1, c)],
                pop_order: vec![(1, c), (c + 1, c + 1)],
            },
            Case {
                name: "nonzero start exact chunks",
                from: 10,
                to: 10 + 2 * c - 1,
                stored_stack: vec![(10 + c, 10 + 2 * c - 1), (10, 10 + c - 1)],
                pop_order: vec![(10, 10 + c - 1), (10 + c, 10 + 2 * c - 1)],
            },
            Case {
                name: "multiple chunks with partial tail",
                from: 42,
                to: 42 + 2 * c + 6,
                stored_stack: vec![
                    (42 + 2 * c, 42 + 2 * c + 6),
                    (42 + c, 42 + 2 * c - 1),
                    (42, 42 + c - 1),
                ],
                pop_order: vec![
                    (42, 42 + c - 1),
                    (42 + c, 42 + 2 * c - 1),
                    (42 + 2 * c, 42 + 2 * c + 6),
                ],
            },
            Case {
                name: "near u64 max does not overflow",
                from: u64::MAX - 1,
                to: u64::MAX,
                stored_stack: vec![(u64::MAX - 1, u64::MAX)],
                pop_order: vec![(u64::MAX - 1, u64::MAX)],
            },
        ];

        for case in cases {
            let mut ranges = initial_log_scan_ranges(case.from, case.to);
            assert_eq!(ranges, case.stored_stack, "{}", case.name);

            let mut pop_order = Vec::new();
            while let Some(range) = ranges.pop() {
                pop_order.push(range);
            }
            assert_eq!(pop_order, case.pop_order, "{}", case.name);
        }
    }

    const TEST_ROLLUP: u64 = 1;

    fn dummy_call() -> eez_protocol::abi::L2ToL1CallSol {
        eez_protocol::abi::L2ToL1CallSol {
            revertNextNCalls: 0,
            isStatic: false,
            gas: 0,
            sourceAddress: Address::ZERO,
            sourceRollupId: TEST_ROLLUP,
            targetAddress: Address::ZERO,
            value: U256::ZERO,
            data: Bytes::new(),
        }
    }

    /// Entry 0 is the state-only anchor; entries below `immediate_count` run
    /// inline, the rest are queued deliveries.
    fn batch_chain(
        pre: B256,
        roots: &[B256],
        immediate_count: usize,
    ) -> ProofSystemBatchPerVerificationEntriesSol {
        let entries = roots
            .iter()
            .enumerate()
            .map(|(i, &new_state)| ExecutionEntrySol {
                stateUpdates: vec![StateUpdateSol {
                    rollupId: TEST_ROLLUP,
                    currentState: if i == 0 { pre } else { roots[i - 1] },
                    newState: new_state,
                    etherDelta: I256::ZERO,
                }],
                proxyEntryHash: if i < immediate_count {
                    B256::ZERO
                } else {
                    B256::repeat_byte(0x80 + u8::try_from(i).unwrap())
                },
                // Entry 0 is the state-only anchor; every later entry produces.
                l2ToL1Calls: if i == 0 {
                    Vec::new()
                } else {
                    vec![dummy_call()]
                },
                destinationRollupId: TEST_ROLLUP,
                success: true,
                ..Default::default()
            })
            .collect();
        ProofSystemBatchPerVerificationEntriesSol {
            entries,
            immediateEntryCount: U256::from(immediate_count),
            ..Default::default()
        }
    }

    /// The evidence L1 would emit for a batch where exactly `applied` ran.
    fn evidence_for(
        batch: &ProofSystemBatchPerVerificationEntriesSol,
        applied: &[usize],
    ) -> super::ConsumptionEvidence {
        let immediate = usize::try_from(batch.immediateEntryCount).unwrap();
        let run_end = super::immediate_run_end(batch, immediate);
        let queue = super::deferred_queue_map(batch, TEST_ROLLUP, immediate);
        super::ConsumptionEvidence {
            observed: applied
                .iter()
                .map(|&i| batch.entries[i].stateUpdates[0].newState)
                .collect(),
            consumed: applied
                .iter()
                .filter(|&&i| i >= run_end)
                .map(|&i| {
                    let slot = queue.iter().position(|&e| e == i).expect("queued entry");
                    (
                        u64::try_from(slot).unwrap(),
                        batch.entries[i].proxyEntryHash,
                    )
                })
                .collect(),
            skipped_immediates: (0..run_end).filter(|i| !applied.contains(i)).collect(),
        }
    }

    fn attribute(
        batch: &ProofSystemBatchPerVerificationEntriesSol,
        applied: &[usize],
    ) -> crate::error::L1Result<Settlement> {
        attribute_settlement(batch, TEST_ROLLUP, &evidence_for(batch, applied))
    }

    /// Idle `A→A` and rich `A→B` share an L1 block; each is judged against the
    /// evidence in ITS OWN window, not the block's last root.
    #[test]
    fn same_block_batches_attributed_per_window_not_block_last() {
        let pre = B256::repeat_byte(0x01);
        for root in [B256::repeat_byte(0xAA), B256::repeat_byte(0xBB)] {
            let batch = batch_chain(pre, &[root], 1);
            let settlement = attribute(&batch, &[0]).unwrap();
            assert_eq!(settlement.applied_indices(), &[0]);
            assert_eq!(settlement.final_state, Some(root));
            assert_eq!(settlement.entry_state, Some(pre));
        }
    }

    /// Nothing settled in our window means the batch is not ours to derive.
    #[test]
    fn an_empty_window_settles_nothing() {
        let batch = batch_chain(B256::repeat_byte(0x01), &[B256::repeat_byte(0xAA)], 1);
        assert_eq!(attribute(&batch, &[]).unwrap(), Settlement::NONE);
    }

    /// A partial settlement is READ, not inferred: entry 2 is absent and the
    /// endpoint is entry 1's commitment.
    #[test]
    fn a_partial_settlement_names_the_entries_that_ran() {
        let pre = B256::repeat_byte(0x01);
        let (r1, r2) = (B256::repeat_byte(0x11), B256::repeat_byte(0x12));
        let batch = batch_chain(pre, &[r1, r2], 2);
        let settlement = attribute(&batch, &[0]).unwrap();
        assert_eq!(settlement.applied_indices(), &[0]);
        assert_eq!(settlement.final_state, Some(r1));
        assert!(!settlement.applied_an_effect(), "anchor-only ran no effect");
    }

    /// Inbound entries carry no `l2ToL1Calls`, so classifying on that field
    /// alone makes every delivery an anchor. `proxyEntryHash` decides first.
    #[test]
    fn an_inbound_entry_without_l2_to_l1_calls_is_not_an_anchor() {
        let pre = B256::repeat_byte(0x01);
        let (r1, r2) = (B256::repeat_byte(0x11), B256::repeat_byte(0x12));
        let mut batch = batch_chain(pre, &[r1, r2], 1);
        batch.entries[1].l2ToL1Calls = Vec::new();
        assert_ne!(batch.entries[1].proxyEntryHash, B256::ZERO);

        let roles = classify_entries(&batch);
        assert_eq!(roles[0], EntryRole::Anchor, "only entry 0 is state-only");
        assert_eq!(
            roles[1],
            EntryRole::Inbound { ordinal: 0 },
            "a zero-call entry with a non-zero proxyEntryHash is a DELIVERY",
        );
        assert!(attribute(&batch, &[0, 1]).unwrap().applied_an_effect());
    }

    /// Outbound and anchor are both zero-hash; only the calls tell them apart.
    #[test]
    fn outbound_and_anchor_are_told_apart_by_their_calls() {
        let pre = B256::repeat_byte(0x01);
        let roots: Vec<B256> = (1..=3).map(B256::repeat_byte).collect();
        let roles = classify_entries(&batch_chain(pre, &roots, 3));
        assert_eq!(roles[0], EntryRole::Anchor);
        assert_eq!(roles[1], EntryRole::Outbound { ordinal: 0 });
        assert_eq!(roles[2], EntryRole::Outbound { ordinal: 1 });
    }

    /// Resume is positional: the batch's FIRST entry did not apply. Roles say
    /// what an entry contributes, not whether we joined mid-chain.
    #[test]
    fn resumed_is_true_exactly_when_the_first_entry_did_not_apply() {
        let pre = B256::repeat_byte(0x01);
        let roots: Vec<B256> = (0x11..=0x13).map(B256::repeat_byte).collect();
        let batch = batch_chain(pre, &roots, 1);

        assert!(!attribute(&batch, &[0, 1, 2]).unwrap().resumed());
        assert!(!attribute(&batch, &[0]).unwrap().resumed());

        let resumed = attribute(&batch, &[1, 2]).unwrap();
        assert!(resumed.resumed());
        assert_eq!(
            resumed.entry_state,
            Some(roots[0]),
            "the run resumes at the rival's endpoint",
        );
    }

    /// Nothing applied is a total loss, not a resume.
    #[test]
    fn a_settlement_that_applied_nothing_is_not_resumed() {
        assert!(!Settlement::NONE.resumed());
        assert!(!Settlement::NONE.applied_an_effect());
    }

    /// A consumption naming a slot the batch lacks means the window was
    /// mis-attributed, so it is terminal rather than guessed.
    #[test]
    fn a_queue_slot_beyond_the_batch_is_terminal() {
        let pre = B256::repeat_byte(0x01);
        let batch = batch_chain(pre, &[B256::repeat_byte(0x11), B256::repeat_byte(0x12)], 1);
        let mut evidence = evidence_for(&batch, &[0, 1]);
        evidence.consumed = vec![(99, batch.entries[1].proxyEntryHash)];
        let error = attribute_settlement(&batch, TEST_ROLLUP, &evidence)
            .expect_err("an out-of-range queue slot must be terminal");
        assert!(
            matches!(error, L1Error::Decode(ref m) if m.contains("slot 99")),
            "got {error}",
        );
    }

    /// A contract poster trips EEZ.sol's meta-hook, which runs the immediates
    /// past the leading L2Tx run out of `_transientEntries` — a table the queue
    /// map cannot address, since `_saveRemainderEntries` never queues them. We
    /// do not post this shape, but a peer may, and misreading it would diverge.
    #[test]
    fn a_meta_hook_consumption_resolves_against_the_transient_prefix() {
        let pre = B256::repeat_byte(0x01);
        let roots: Vec<B256> = (0x11..=0x12).map(B256::repeat_byte).collect();
        let mut batch = batch_chain(pre, &roots, 1);
        // Entry 1 carries a call hash yet sits inside the immediate prefix, so
        // the leading run ends at 1 and entry 1 goes to the transient table.
        batch.immediateEntryCount = U256::from(2u64);
        assert!(
            super::deferred_queue_map(&batch, TEST_ROLLUP, 2).is_empty(),
            "the transient prefix must be unreachable through the queue",
        );

        let evidence = super::ConsumptionEvidence {
            observed: roots.clone(),
            consumed: vec![(0, batch.entries[1].proxyEntryHash)],
            skipped_immediates: Vec::new(),
        };
        let settlement = attribute_settlement(&batch, TEST_ROLLUP, &evidence)
            .expect("transient slot 0 names entry 1");
        assert_eq!(settlement.applied_indices(), &[0, 1]);
        assert_eq!(settlement.final_state, Some(roots[1]));

        // The hash decides, so a slot inside the window that no entry answers
        // for stays terminal instead of resolving positionally.
        let bogus = super::ConsumptionEvidence {
            consumed: vec![(0, B256::repeat_byte(0xEE))],
            ..evidence
        };
        attribute_settlement(&batch, TEST_ROLLUP, &bogus)
            .expect_err("a call hash no entry carries must be terminal");
    }

    /// The queue map and the call hash are independent readings of the same
    /// fact; disagreement means the window picked up a rival's consumption.
    #[test]
    fn queue_slot_and_call_hash_must_agree() {
        let pre = B256::repeat_byte(0x01);
        let batch = batch_chain(pre, &[B256::repeat_byte(0x11), B256::repeat_byte(0x12)], 1);
        let mut evidence = evidence_for(&batch, &[0, 1]);
        evidence.consumed = vec![(0, B256::repeat_byte(0xEE))];
        assert!(
            attribute_settlement(&batch, TEST_ROLLUP, &evidence).is_err(),
            "a call hash naming another entry must not be attributed to this one",
        );
    }

    /// A root logged against a DIFFERENT fork of the same block NUMBER (hash B,
    /// not this batch's hash A) must not be attributed.
    #[test]
    fn in_window_excludes_a_different_forks_record_at_the_same_block_number() {
        let hash_a = B256::repeat_byte(0xA1);
        let hash_b = B256::repeat_byte(0xB2);
        let root = B256::repeat_byte(0x0D);
        let roots = [SettledRoot {
            tx_index: 0,
            log_index: 0,
            block_hash: hash_b,
            payload: root,
        }];
        assert!(in_window(&roots, 0, u64::MAX, hash_a).is_empty());
        assert_eq!(in_window(&roots, 0, u64::MAX, hash_b), vec![root]);
        // Half-open: a record AT `window_end` belongs to the next batch, whose
        // verify already wiped our queue.
        assert!(in_window(&roots, 0, 0, hash_b).is_empty());
    }

    /// A minimal, serializable RPC transaction for mocked provider
    /// responses. Signed with a fixed test vector, so its hash is
    /// deterministic — the identity check compares against it.
    fn mock_rpc_transaction() -> alloy_rpc_types_eth::Transaction {
        use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy, transaction::Recovered};
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1,
            gas_limit: 21_000,
            to: alloy_primitives::TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let signed = tx.into_signed(alloy_primitives::Signature::test_signature());
        alloy_rpc_types_eth::Transaction {
            inner: Recovered::new_unchecked(TxEnvelope::Legacy(signed), Address::ZERO),
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_index: None,
            effective_gas_price: None,
        }
    }

    /// A `postAndVerifyBatch` tx carrying `input`, signed with a fixed test
    /// vector — its hash is a deterministic function of `input` alone, so
    /// distinct batches naturally get distinct hashes.
    /// A postBatch tx for `rollup_id`, optionally claiming one
    /// `current -> new` step. `salt` varies the encoding so each call yields a
    /// distinct tx hash.
    fn batch_tx(
        rollup_id: u64,
        step: Option<(B256, B256)>,
        salt: u64,
    ) -> alloy_rpc_types_eth::Transaction {
        let batch = ProofSystemBatchPerVerificationEntriesSol {
            rollupIdsWithProofSystems: vec![RollupIdWithProofSystemsSol {
                rollupId: rollup_id,
                proofSystemIndexes: vec![],
            }],
            // Attribution reads the applied set from the events an INLINE entry
            // emits, so a queued fixture would settle as empty.
            immediateEntryCount: U256::from(u64::from(step.is_some())),
            entries: step
                .map(|(current, new)| {
                    vec![ExecutionEntrySol {
                        stateUpdates: vec![StateUpdateSol {
                            rollupId: rollup_id,
                            currentState: current,
                            newState: new,
                            etherDelta: I256::ZERO,
                        }],
                        destinationRollupId: rollup_id,
                        ..Default::default()
                    }]
                })
                .unwrap_or_default(),
            // Varies the encoded input so each fixture tx gets its own hash.
            blockNumber: salt,
            ..Default::default()
        };
        mock_post_batch_tx(postAndVerifyBatchCall { batch }.abi_encode())
    }

    fn mock_post_batch_tx(input: Vec<u8>) -> alloy_rpc_types_eth::Transaction {
        use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy, transaction::Recovered};
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1,
            gas_limit: 21_000,
            to: alloy_primitives::TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::from(input),
        };
        let signed = tx.into_signed(alloy_primitives::Signature::test_signature());
        alloy_rpc_types_eth::Transaction {
            inner: Recovered::new_unchecked(TxEnvelope::Legacy(signed), Address::ZERO),
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_index: None,
            effective_gas_price: None,
        }
    }

    /// A `BatchPosted` log at the given (block, tx) coordinates.
    fn batch_posted_log(
        block_number: u64,
        block_hash: B256,
        tx_hash: B256,
        tx_index: u64,
    ) -> alloy_rpc_types_eth::Log {
        alloy_rpc_types_eth::Log {
            inner: alloy_primitives::Log {
                address: Address::ZERO,
                data: BatchPosted {
                    rollupCount: U256::from(1),
                }
                .encode_log_data(),
            },
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(tx_index),
            log_index: Some(0),
            removed: false,
        }
    }

    /// An `L2ExecutionPerformed` log settling `root` for `rollup_id` at the
    /// given (block, tx, log) coordinates.
    fn settled_root_log(
        rollup_id: u64,
        root: B256,
        block_number: u64,
        block_hash: B256,
        tx_hash: B256,
        tx_index: u64,
        log_index: u64,
    ) -> alloy_rpc_types_eth::Log {
        alloy_rpc_types_eth::Log {
            inner: alloy_primitives::Log {
                address: Address::ZERO,
                data: L2ExecutionPerformed {
                    rollupId: rollup_id,
                    newState: root,
                }
                .encode_log_data(),
            },
            block_hash: Some(block_hash),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(tx_index),
            log_index: Some(log_index),
            removed: false,
        }
    }

    /// The boot-crash fix's linchpin: a tx the L1 serves at (block hash, index) is
    /// returned when its hash matches the log's; a null lookup classifies as
    /// `SourceIncomplete` (retryable) rather than a fatal provider error.
    ///
    /// The mock is a method-agnostic FIFO, so this pins response consumption
    /// counts and the classification behavior — not which RPC method was used.
    #[tokio::test]
    async fn tx_lookup_hits_by_hash_index_then_classifies_null_source_incomplete() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let real_hash = *mock_rpc_transaction().inner.tx_hash();

        // (a) by-(block hash, index) hit, hash matches the log: accepted directly.
        asserter.push_success(&mock_rpc_transaction());
        fetch_log_transaction(&provider, 14, B256::ZERO, 0, real_hash)
            .await
            .expect("index lookup hit");

        // (b) null lookup → retryable SourceIncomplete carrying the block and
        // tx hash context.
        asserter.push_success(&serde_json::Value::Null);
        let err = fetch_log_transaction(&provider, 14, B256::ZERO, 7, real_hash)
            .await
            .expect_err("null lookup must not yield a tx");
        assert!(err.is_source_incomplete(), "unexpected error: {err}");
        match err {
            L1Error::SourceIncomplete {
                block, tx_hash: h, ..
            } => {
                assert_eq!(block, 14);
                assert_eq!(h, real_hash);
            }
            other => panic!("expected SourceIncomplete, got {other}"),
        }
    }

    /// A one-block reorg between the log fetch and this call can return a
    /// DIFFERENT tx at the same (block hash, index) slot; the tx's own hash must
    /// still match the log's, or it must be rejected — not laundered into the batch.
    #[tokio::test]
    async fn tx_lookup_rejects_a_tx_whose_hash_does_not_match_the_log() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let real_hash = *mock_rpc_transaction().inner.tx_hash();
        let claimed_hash = B256::with_last_byte(0x51);
        assert_ne!(claimed_hash, real_hash, "test vector must actually differ");

        asserter.push_success(&mock_rpc_transaction());
        let err = fetch_log_transaction(&provider, 14, B256::ZERO, 0, claimed_hash)
            .await
            .expect_err("mismatched tx must be rejected, not accepted");
        assert!(err.is_source_incomplete(), "unexpected error: {err}");
    }

    /// A result-count refusal must NARROW the range, not abort the scan: the
    /// Watcher's catch-up aborts its tick before advancing its ring, so
    /// propagating means it retries the identical over-wide range forever, and
    /// boot `catch_up` propagating means the node never starts.
    #[tokio::test]
    async fn result_limit_refusal_narrows_and_requeues_the_tail() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let mut chunks = BatchLogChunks::new(1_288, 33_632);
        assert_eq!(chunks.ranges.len(), 1, "one chunk (span < 100k)");

        // reth's wording; the retry of the narrowed half then succeeds.
        asserter
            .push_failure_msg("query exceeds max results 20000, retry with the range 1288-33632");
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&serde_json::json!([]));
        let scanned = scan_next_batch_log_chunk(&provider, Address::ZERO, 1, &mut chunks)
            .await
            .expect("must NOT propagate — narrowing is the remedy")
            .expect("chunk yielded");
        assert!(scanned.is_empty());

        // Covered only the lower half; the tail is re-queued so nothing is skipped.
        let mid = 1_288 + (33_632 - 1_288) / 2;
        assert_eq!(chunks.ranges.len(), 1, "tail re-queued");
        assert_eq!(*chunks.ranges.last().expect("tail"), (mid + 1, 33_632));
    }

    /// A single-block chunk that still exceeds the cap is genuinely unservable —
    /// propagate rather than split forever.
    #[tokio::test]
    async fn unsplittable_single_block_refusal_propagates() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let mut chunks = BatchLogChunks::new(500, 500);
        asserter.push_failure_msg("query exceeds max results 20000");
        let err = scan_next_batch_log_chunk(&provider, Address::ZERO, 1, &mut chunks)
            .await
            .expect_err("a one-block range cannot be narrowed further");
        assert!(
            err.to_string().contains("exceeds max results"),
            "got: {err}"
        );
        assert_eq!(chunks.ranges.len(), 1, "range preserved for retry");
    }

    /// A failed chunk scan must NOT consume the range: the same range is
    /// retried on the next call. A successful scan consumes exactly the
    /// oldest range. This is the invariant the watcher's per-chunk
    /// catch-up (and the deriver's retry loop) rely on.
    #[tokio::test]
    async fn failed_chunk_scan_preserves_range() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        // Two ranges: [(0, 99_999), (100_000, 150_000)] stored reversed.
        let mut chunks = BatchLogChunks::new(0, 150_000);
        assert_eq!(chunks.ranges.len(), 2);

        // First get_logs of the oldest chunk fails.
        asserter.push_failure_msg("injected: range scan failed");
        let err = scan_next_batch_log_chunk(&provider, Address::ZERO, 1, &mut chunks)
            .await
            .expect_err("injected failure must propagate");
        assert!(
            err.to_string().contains("injected"),
            "unexpected error: {err}"
        );
        // Range NOT consumed.
        assert_eq!(chunks.ranges.len(), 2);
        assert_eq!(
            *chunks.ranges.last().expect("oldest range intact"),
            (0, 99_999)
        );

        // Retry succeeds (BatchPosted logs + winners logs, both empty).
        asserter.push_success(&serde_json::json!([]));
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&serde_json::json!([]));
        let scanned = scan_next_batch_log_chunk(&provider, Address::ZERO, 1, &mut chunks)
            .await
            .expect("retry succeeds")
            .expect("chunk yielded");
        assert!(scanned.is_empty());
        // Exactly the oldest range consumed; newer range still queued.
        assert_eq!(chunks.ranges.len(), 1);
        assert_eq!(
            *chunks.ranges.last().expect("tail range"),
            (100_000, 150_000)
        );
    }

    /// `state_applied` is pinned by `(block_hash, tx_hash)` — a settlement log
    /// carrying the SAME tx hash but a DIFFERENT block hash (as a reorg would
    /// produce) must not credit the batch.
    #[tokio::test]
    async fn state_applied_is_pinned_by_block_hash_not_tx_hash_alone() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let hash_a = B256::with_last_byte(0xA1);
        let hash_b = B256::with_last_byte(0xB2);
        // An entry, because the root on our own hash below implies one applied.
        let tx = batch_tx(
            1,
            Some((B256::repeat_byte(0x50), B256::repeat_byte(0xD4))),
            0,
        );
        let tx_hash = *tx.inner.tx_hash();

        asserter.push_success(&vec![batch_posted_log(100, hash_a, tx_hash, 0)]);
        asserter.push_success(&vec![
            // Unrelated settlement on our OWN hash — keeps this distinct from
            // the all-different-fork case (SourceIncomplete; tested separately).
            settled_root_log(
                1,
                B256::repeat_byte(0xD4),
                100,
                hash_a,
                B256::with_last_byte(0xC3),
                1,
                0,
            ),
            // Same tx hash as our postBatch tx, but a DIFFERENT block hash.
            settled_root_log(1, B256::repeat_byte(0xE5), 100, hash_b, tx_hash, 2, 0),
        ]);
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&tx);

        let scanned = scan_batch_logs_range(&provider, Address::ZERO, 1, 100, 100)
            .await
            .expect("scan succeeds");
        assert_eq!(scanned.len(), 1);
        assert!(
            !scanned[0].state_applied,
            "same tx hash on a different block hash must not mark state_applied"
        );
    }

    /// Window boundaries are keyed by `(block_number, block_hash)` — a rival
    /// batch on a DIFFERENT fork of the same block number must not truncate
    /// our window.
    #[tokio::test]
    async fn boundaries_are_keyed_by_block_hash_not_number_alone() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let (hash_a, hash_b) = (B256::with_last_byte(0xA1), B256::with_last_byte(0xB2));
        let (c0, c1) = (B256::repeat_byte(0x50), B256::repeat_byte(0x51));

        let ours = batch_tx(1, Some((c0, c1)), 0);
        // An entry, because its decoy root below implies one applied: a root
        // with no entry is unreachable on-chain.
        let rival = batch_tx(1, Some((c0, B256::repeat_byte(0x99))), 1);
        let boundary = batch_tx(1, None, 2); // our real next boundary, hash A
        let (ours_hash, rival_hash, boundary_hash) = (
            *ours.inner.tx_hash(),
            *rival.inner.tx_hash(),
            *boundary.inner.tx_hash(),
        );

        asserter.push_success(&vec![
            batch_posted_log(500, hash_a, ours_hash, 3),
            batch_posted_log(500, hash_b, rival_hash, 5),
            batch_posted_log(500, hash_a, boundary_hash, 9),
        ]);
        asserter.push_success(&vec![
            // tx_index 7: past the rival's 5, before our real boundary at 9.
            settled_root_log(1, c1, 500, hash_a, B256::with_last_byte(0xF0), 7, 0),
            // Decoy on the rival's hash so its own pass doesn't trip the
            // all-roots-on-a-foreign-fork guard first.
            settled_root_log(
                1,
                B256::repeat_byte(0x99),
                500,
                hash_b,
                B256::with_last_byte(0xF1),
                5,
                0,
            ),
        ]);
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&ours);
        asserter.push_success(&rival);
        asserter.push_success(&boundary);

        let scanned = scan_batch_logs_range(&provider, Address::ZERO, 1, 500, 500)
            .await
            .expect("scan succeeds");
        let ours = scanned
            .iter()
            .find(|b| b.tx_hash == ours_hash)
            .expect("our batch scanned");
        assert_eq!(
            ours.settlement.applied_indices(),
            &[0],
            "the rival on another fork must not cut our window at its tx_index"
        );
        assert_eq!(ours.settlement.final_state, Some(c1));
        assert_eq!(ours.settlement.entry_state, Some(c0));
    }

    #[tokio::test]
    async fn cross_fork_settlement_logs_are_source_incomplete_not_empty() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

        let hash_a = B256::with_last_byte(0xA1);
        let hash_b = B256::with_last_byte(0xB2);
        let tx = batch_tx(1, None, 0);
        let tx_hash = *tx.inner.tx_hash();

        asserter.push_success(&vec![batch_posted_log(700, hash_a, tx_hash, 0)]);
        asserter.push_success(&vec![settled_root_log(
            1,
            B256::repeat_byte(0x77),
            700,
            hash_b,
            B256::with_last_byte(0xC3),
            1,
            0,
        )]);
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&tx);

        let err = scan_batch_logs_range(&provider, Address::ZERO, 1, 700, 700)
            .await
            .expect_err("all settlement roots on a different fork must not settle empty");
        assert!(err.is_source_incomplete(), "unexpected error: {err}");
    }

    /// `BatchPosted` carries rollupCount, not rollupId, so a peer posting via a
    /// router yields an input we cannot decode. That must not halt us — unless
    /// the same tx settled OUR rollup, which we then genuinely cannot derive.
    #[tokio::test]
    async fn undecodable_foreign_batch_is_skipped_but_one_that_settled_us_is_fatal() {
        let block_hash = B256::with_last_byte(0xA1);
        let tx = mock_post_batch_tx(b"not a postAndVerifyBatch call".to_vec());
        let tx_hash = *tx.inner.tx_hash();

        // Did not settle our rollup: skipped, scan still succeeds.
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        asserter.push_success(&vec![batch_posted_log(700, block_hash, tx_hash, 0)]);
        asserter.push_success(&Vec::<alloy_rpc_types_eth::Log>::new());
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&tx);
        let scanned = scan_batch_logs_range(&provider, Address::ZERO, 1, 700, 700)
            .await
            .expect("a foreign undecodable batch must not fail the scan");
        assert!(scanned.is_empty());

        // Settled our rollup: we cannot derive it, so fail loudly.
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        asserter.push_success(&vec![batch_posted_log(700, block_hash, tx_hash, 0)]);
        asserter.push_success(&vec![settled_root_log(
            1,
            B256::repeat_byte(0x77),
            700,
            block_hash,
            tx_hash,
            0,
            0,
        )]);
        asserter.push_success(&serde_json::json!([])); // consumption events
        asserter.push_success(&tx);
        let err = scan_batch_logs_range(&provider, Address::ZERO, 1, 700, 700)
            .await
            .expect_err("an undecodable batch that moved our root must not be skipped");
        assert!(err.is_terminal(), "unexpected error: {err}");
    }
}
