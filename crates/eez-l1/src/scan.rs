use std::collections::{HashMap, HashSet};

use alloy_consensus::Transaction;
use alloy_consensus::transaction::TxHashRef;
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::SolEvent;
use eez_protocol::abi::{
    BatchPosted, L2ExecutionPerformed, ProofSystemBatchPerVerificationEntriesSol,
};
use tracing::{Level, event};

use crate::error::{L1Error, L1Result};

/// Initial block span for historical log scans. Wide catch-up gaps are
/// split before hitting RPCs that reject long `eth_getLogs` ranges.
pub(crate) const LOG_SCAN_CHUNK_BLOCKS: u64 = 100_000;

/// One `L2ExecutionPerformed.newState` for our rollup, tagged with where in the
/// L1 block it was emitted so it can be attributed to the owning batch.
#[derive(Debug, Clone, Copy)]
struct SettledRoot {
    tx_index: u64,
    log_index: u64,
    /// L1 block this root's log landed in — attribution requires it to
    /// match the batch's block hash (fork-pinning).
    block_hash: B256,
    root: B256,
}

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
    call_data: Bytes,
    post_batch_input: Bytes,
    claimed_current_state: Option<B256>,
    claimed_chain: Vec<B256>,
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
    /// The originating postBatch tx's full input (the `postAndVerifyBatch`
    /// calldata), captured from the tx fetched by (block, index). Carried
    /// so the Deriver's reconcile fallback decodes `batch.entries`
    /// from these bytes instead of re-fetching the tx by hash — that lookup
    /// fails on a pruned or still-resyncing embedded L1 and crashed boot
    /// catch_up on restart-after-post.
    pub post_batch_input: Bytes,
    pub state_applied: bool,
    /// Which of this batch's claimed steps L1 actually ran. See [`Settlement`].
    pub settlement: Settlement,
    pub claimed_current_state: Option<B256>,
    pub claimed_new_state: Option<B256>,
}

/// Which of a batch's claimed steps L1 actually ran. Each step moves the stored
/// root one hop and emits one `L2ExecutionPerformed`, in order, so the observed
/// roots identify them. The run is always contiguous — a skipped step stops the
/// root advancing, failing every later `currentState` check
/// (`EEZ.sol:_applyStateDeltas`) — so `(start, len)` describes it fully.
///
/// `start > 0` = leading steps skipped: a competing same-block batch already made
/// those hops (routine with two composers on a shared tx stream, since identical
/// txs produce identical roots).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Settlement {
    /// Index into the claimed chain where the applied run starts. Meaningless
    /// when `len == 0`.
    pub start: usize,
    /// How many consecutive steps ran. `0` = nothing settled → skip the batch.
    pub len: usize,
    /// `newState` of the last step that ran — L1's ACTUAL stored root after this
    /// batch (a prefix endpoint under partial consumption). Reconciliation
    /// validates against THIS, never the claimed full-chain end.
    pub final_state: Option<B256>,
    /// The stored root the applied run STARTED from — what reconciliation compares
    /// its local cursor against. The claimed `currentState` when `start == 0`,
    /// else `claimed_chain[start - 1]`; using the claimed value on a mid-chain
    /// resume reports a divergence that isn't one.
    pub entry_state: Option<B256>,
}

impl Settlement {
    /// Nothing of this batch applied on L1.
    pub const NONE: Self = Self {
        start: 0,
        len: 0,
        final_state: None,
        entry_state: None,
    };

    /// True when L1 ran none of this batch's steps — the claimed roots are
    /// phantoms and the batch must be skipped entirely.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Which "producing" entries ran, as `(skip, take)` over that list. Producing
    /// entries carry L2 calls and map 1:1 to reconstructed system txs; claimed
    /// index 0 is the anchor (state-only, signs no system tx), so claimed `i` is
    /// producing `i - 1`:
    /// - `start == 0` — anchor ran → `(0, len - 1)`.
    /// - `start >= 1` — anchor plus `start - 1` producing entries skipped →
    ///   `(start - 1, len)`.
    ///
    /// `start > 1` is not an error: composer 1 posting `A→B→C` and composer 2
    /// posting `A→B→C→D` yield an identical `C`, so composer 2's anchor and `B→C`
    /// are refused as redundant while `C→D` still runs. Refusing to reconstruct
    /// that step would stall the cursor at `C` forever.
    #[must_use]
    pub const fn producing_slice(&self) -> (usize, usize) {
        match self.start {
            0 => (0, self.len.saturating_sub(1)),
            s => (s - 1, self.len),
        }
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
    // a loser its rival's settlement. Ordered (not a set) because attribution
    // matches steps positionally.
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
                root: B256::from_slice(data),
            });
        }
    }
    for roots in settled_by_block.values_mut() {
        roots.sort_by_key(|r| (r.tx_index, r.log_index));
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
        // A peer can post through a ROUTER: the top-level selector is the
        // router's, with the real `postAndVerifyBatch` calldata forwarded
        // verbatim as a `bytes` arg. Decode through that wrapper so a routed
        // batch that DID settle our rollup still derives (invariant 1).
        let decoded = match eez_protocol::entries::decode_postbatch_input(input) {
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
        let (claimed_current_state, claimed_chain) = our_state_chain(&decoded, rollup_id);
        decoded_batches.push(DecodedBatchLog {
            l1_block_number,
            l1_block_hash,
            tx_hash,
            tx_index,
            submitter,
            // A batch verifies (and therefore wipes) our rollup iff it lists it
            // — the same array `postAndVerifyBatch` loops over to mark verified.
            verifies_our_rollup: decoded
                .rollupIdsWithProofSystems
                .iter()
                .any(|r| r.rollupId == rollup_id),
            call_data: decoded.callData,
            post_batch_input: input.clone(),
            claimed_current_state,
            claimed_chain,
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
        let observed: Vec<B256> = block_roots
            .map(|roots| window_roots(roots, b.tx_index, window_end, b.l1_block_hash))
            .unwrap_or_default();
        let settlement = attribute_settlement(b.claimed_current_state, &b.claimed_chain, &observed);
        out.push(ScannedBatch {
            l1_block_number: b.l1_block_number,
            l1_block_hash: b.l1_block_hash,
            tx_hash: b.tx_hash,
            submitter: b.submitter,
            call_data: b.call_data,
            post_batch_input: b.post_batch_input,
            state_applied: winner_tx_hashes.contains(&(b.l1_block_hash, b.tx_hash)),
            settlement,
            claimed_current_state: b.claimed_current_state,
            claimed_new_state: b.claimed_chain.last().copied(),
        });
    }
    Ok(out)
}

/// Our rollup's state-update chain in a batch: the first update's
/// `currentState` (pre-batch root) and the ordered per-update `newState` roots.
fn our_state_chain(
    batch: &ProofSystemBatchPerVerificationEntriesSol,
    rollup_id: u64,
) -> (Option<B256>, Vec<B256>) {
    let mut first_curr: Option<B256> = None;
    let mut new_states: Vec<B256> = Vec::new();
    for entry in &batch.entries {
        for update in &entry.stateUpdates {
            if update.rollupId == rollup_id {
                if first_curr.is_none() {
                    first_curr = Some(update.currentState);
                }
                new_states.push(update.newState);
            }
        }
    }
    (first_curr, new_states)
}

/// Roots settled inside this batch's tx-index window AND on this batch's own
/// L1 block hash — a same-numbered root from a different fork never attributes.
fn window_roots(
    roots: &[SettledRoot],
    tx_index: u64,
    window_end: u64,
    block_hash: B256,
) -> Vec<B256> {
    roots
        .iter()
        .filter(|r| r.tx_index >= tx_index && r.tx_index < window_end && r.block_hash == block_hash)
        .map(|r| r.root)
        .collect()
}

/// Which of this batch's claimed steps L1 ran (see [`Settlement`]). `observed` is
/// the ordered `newState` sequence emitted inside this batch's window, located as
/// a consecutive slice of `claimed_chain` — positional, not set membership, so
/// duplicate roots stay distinct and a coincidental match can't inflate the count.
/// [`Settlement::NONE`] when nothing matches (empty window or phantom roots).
fn attribute_settlement(
    claimed_current: Option<B256>,
    claimed_chain: &[B256],
    observed: &[B256],
) -> Settlement {
    if observed.is_empty() || claimed_chain.is_empty() {
        return Settlement::NONE;
    }
    // The run must appear as a consecutive slice of the claimed chain.
    if observed.len() > claimed_chain.len() {
        return Settlement::NONE;
    }
    let Some(start) = (0..=(claimed_chain.len() - observed.len()))
        .find(|&s| claimed_chain[s..].starts_with(observed))
    else {
        // The window settled roots this batch never claimed. Not attributable
        // to it — treat as "nothing of ours applied" and let the caller skip;
        // the cursor guard catches any real misalignment loudly.
        event!(
            name: "eez.l1.scan_batch_logs.settlement_unmatched",
            Level::WARN,
            observed = observed.len(),
            claimed = claimed_chain.len(),
            "settled roots in this batch's window match no consecutive run of its claimed chain",
        );
        return Settlement::NONE;
    };
    Settlement {
        start,
        len: observed.len(),
        final_state: observed.last().copied(),
        // Leading steps skipped ⇒ the run began at the last skipped step's
        // `newState` (a competing batch had already made that hop).
        entry_state: if start == 0 {
            claimed_current
        } else {
            claimed_chain.get(start - 1).copied()
        },
    }
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
        BatchLogChunks, LOG_SCAN_CHUNK_BLOCKS, SettledRoot, Settlement, attribute_settlement,
        fetch_log_transaction, initial_log_scan_ranges, scan_batch_logs_range,
        scan_next_batch_log_chunk, window_roots,
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

    /// Idle `A→A` and rich `A→B` share an L1 block; each is judged against the
    /// roots settled in ITS OWN window, not the block's last root.
    #[test]
    fn same_block_batches_attributed_per_window_not_block_last() {
        let a = B256::repeat_byte(0xAA);
        let b = B256::repeat_byte(0xBB);
        assert_eq!(
            attribute_settlement(None, &[a], &[a]),
            Settlement {
                start: 0,
                len: 1,
                final_state: Some(a),
                entry_state: None
            },
        );
        assert_eq!(
            attribute_settlement(None, &[b], &[b]),
            Settlement {
                start: 0,
                len: 1,
                final_state: Some(b),
                entry_state: None
            },
        );
    }

    /// A loser gets an EMPTY window (its rival's events belong to the rival's
    /// window), so nothing is attributed to it ⇒ the deriver skips it. Crediting
    /// the loser with the winner's root let its phantom `currentState` reach the
    /// cursor guard and fail boot catch_up.
    #[test]
    fn loser_with_empty_window_is_skipped() {
        let b = B256::repeat_byte(0xBB);
        let y = B256::repeat_byte(0xCC);
        assert_eq!(attribute_settlement(None, &[y], &[]), Settlement::NONE);
        // Even when the loser's chain SHARES a root with the winner's (overlapping
        // ranges), an empty window keeps it unattributed.
        assert_eq!(attribute_settlement(None, &[y, b], &[]), Settlement::NONE);
    }

    /// Partial consumption: only a prefix ran → endpoint is the last root that
    /// ran, not the claimed end.
    #[test]
    fn partial_consumption_uses_last_root_that_ran() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let d = B256::repeat_byte(0x0D);
        assert_eq!(
            attribute_settlement(None, &[b, c, d], &[b, c]),
            Settlement {
                start: 0,
                len: 2,
                final_state: Some(c),
                entry_state: None
            },
        );
    }

    /// Full consumption: the claimed end ran → it's the endpoint.
    #[test]
    fn full_consumption_uses_claimed_end() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        assert_eq!(
            attribute_settlement(None, &[b, c], &[b, c]),
            Settlement {
                start: 0,
                len: 2,
                final_state: Some(c),
                entry_state: None
            },
        );
    }

    /// Empty window (nothing settled for our rollup) → unsettled.
    #[test]
    fn empty_window_is_unsettled() {
        assert_eq!(
            attribute_settlement(None, &[B256::repeat_byte(1)], &[]),
            Settlement::NONE,
        );
    }

    /// Two composers in ONE L1 block: pb1 claims A→B and lands; pb2 claims
    /// A→B→C, so its A→B is refused as redundant and only B→C runs. pb2 must be
    /// credited with C alone and aligned on B — the claimed A reported a false
    /// `local_diverged`.
    #[test]
    fn two_composers_second_batch_resumes_after_first_hop() {
        let (a, b, c) = (
            B256::repeat_byte(0x0A),
            B256::repeat_byte(0x0B),
            B256::repeat_byte(0x0C),
        );

        let s1 = attribute_settlement(Some(a), &[b], &[b]);
        assert_eq!(
            s1,
            Settlement {
                start: 0,
                len: 1,
                final_state: Some(b),
                entry_state: Some(a),
            },
        );

        let s2 = attribute_settlement(Some(a), &[b, c], &[c]);
        assert_eq!(
            s2,
            Settlement {
                start: 1,
                len: 1,
                final_state: Some(c),
                entry_state: Some(b),
            },
        );
        assert_ne!(s2.entry_state, Some(a));
        assert_eq!(s2.producing_slice(), (0, 1));

        // Per-block attribution hands pb2 both roots, claiming pb1's hop as its
        // own — that disagreement was the bug.
        let per_block = attribute_settlement(Some(a), &[b, c], &[b, c]);
        assert_eq!((per_block.start, per_block.len), (0, 2));
        assert_ne!((per_block.start, per_block.len), (s2.start, s2.len));
    }

    /// Anchor skipped: a competing same-block batch already made the anchor's
    /// hop (A→B), so ours was refused as redundant while the producing steps
    /// still ran. The run starts at index 1 — counting producing entries as
    /// `len - 1` would under-count them and truncate a system tx.
    #[test]
    fn anchor_skipped_run_starts_at_one() {
        let b = B256::repeat_byte(0x0B); // anchor's newState
        let c = B256::repeat_byte(0x0C);
        let d = B256::repeat_byte(0x0D);
        let s = attribute_settlement(None, &[b, c, d], &[c, d]);
        assert_eq!(
            s,
            Settlement {
                start: 1,
                len: 2,
                final_state: Some(d),
                entry_state: Some(b)
            }
        );
        // Both producing steps ran — NOT `len - 1`.
        assert_eq!(s.producing_slice(), (0, 2));
    }

    /// Anchor ran: producing entries are `len - 1`.
    #[test]
    fn anchor_applied_excludes_itself_from_producing_count() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let s = attribute_settlement(None, &[b, c], &[b, c]);
        assert_eq!(s.start, 0);
        assert_eq!(s.producing_slice(), (0, 1));
    }

    /// Two composers on a SHARED tx stream: composer 1 posts `A→B→C` and lands
    /// first, composer 2 posts `A→B→C→D` over the same txs. Composer 2's anchor
    /// and `B→C` are refused as redundant (root is already `C`), but `C→D`
    /// matches the live root and RUNS. The run resumes mid-chain and must be
    /// reconstructed — refusing would stall the cursor at `C` forever.
    #[test]
    fn shared_tx_stream_run_resumes_mid_chain() {
        let b = B256::repeat_byte(0x0B); // anchor A→B
        let c = B256::repeat_byte(0x0C); // tx-1   B→C  (competitor already made this hop)
        let d = B256::repeat_byte(0x0D); // tx-2   C→D  (only this one ran)
        let s = attribute_settlement(None, &[b, c, d], &[d]);
        assert_eq!(
            s,
            Settlement {
                start: 2,
                len: 1,
                final_state: Some(d),
                entry_state: Some(c)
            }
        );
        // Skip the producing entry the competitor settled (tx-1), take tx-2.
        assert_eq!(s.producing_slice(), (1, 1));
        // Reconciliation compares its cursor against `C` — the root the run began
        // at — not the claimed `currentState` (`A`), which would look diverged.
        assert_eq!(s.entry_state, Some(c));
    }

    /// Deeper resume: a competitor supplied the anchor plus TWO producing hops.
    #[test]
    fn producing_slice_skips_every_step_before_the_run() {
        let r: Vec<B256> = (0x0Bu8..=0x0F).map(B256::repeat_byte).collect();
        let s = attribute_settlement(None, &r, &r[3..]);
        assert_eq!((s.start, s.len), (3, 2));
        // claimed [3,4] → producing [2,3] → skip 2, take 2
        assert_eq!(s.producing_slice(), (2, 2));
    }

    /// Matching is positional, so a duplicate root can't inflate the run and an
    /// out-of-order coincidence isn't credited.
    #[test]
    fn matching_is_positional_not_set_membership() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        // Repeated root: the run is located, not counted twice.
        assert_eq!(
            attribute_settlement(None, &[b, b, c], &[b, b]),
            Settlement {
                start: 0,
                len: 2,
                final_state: Some(b),
                entry_state: None
            },
        );
        // Out of order → no consecutive run matches → unattributed.
        assert_eq!(
            attribute_settlement(None, &[b, c], &[c, b]),
            Settlement::NONE
        );
    }

    /// Roots this batch never claimed (window contamination) are not attributed.
    #[test]
    fn unclaimed_roots_are_not_attributed() {
        let b = B256::repeat_byte(0x0B);
        let z = B256::repeat_byte(0x7A);
        assert_eq!(attribute_settlement(None, &[b], &[z]), Settlement::NONE);
    }

    /// A root logged against a DIFFERENT fork of the same block NUMBER (hash B,
    /// not this batch's hash A) must not be attributed — it settles as empty,
    /// exactly like any other non-matching root.
    #[test]
    fn window_roots_excludes_a_different_forks_root_at_the_same_block_number() {
        let hash_a = B256::repeat_byte(0xA1);
        let hash_b = B256::repeat_byte(0xB2);
        let root = B256::repeat_byte(0x0D);
        let roots = [SettledRoot {
            tx_index: 0,
            log_index: 0,
            block_hash: hash_b,
            root,
        }];
        assert!(window_roots(&roots, 0, u64::MAX, hash_a).is_empty());
        assert_eq!(window_roots(&roots, 0, u64::MAX, hash_b), vec![root]);
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
            immediateEntryCount: U256::from(salt),
            entries: step
                .map(|(current, new)| {
                    vec![ExecutionEntrySol {
                        stateUpdates: vec![StateUpdateSol {
                            rollupId: rollup_id,
                            currentState: current,
                            newState: new,
                            etherDelta: I256::ZERO,
                        }],
                        ..Default::default()
                    }]
                })
                .unwrap_or_default(),
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
        let tx = batch_tx(1, None, 0);
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
        let rival = batch_tx(1, None, 1); // same block NUMBER, different HASH
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
            ours.settlement,
            Settlement {
                start: 0,
                len: 1,
                final_state: Some(c1),
                entry_state: Some(c0),
            },
            "the rival on another fork must not cut our window at its tx_index"
        );
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
        asserter.push_success(&tx);
        let err = scan_batch_logs_range(&provider, Address::ZERO, 1, 700, 700)
            .await
            .expect_err("an undecodable batch that moved our root must not be skipped");
        assert!(err.is_terminal(), "unexpected error: {err}");
    }
}
