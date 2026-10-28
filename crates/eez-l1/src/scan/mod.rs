//! L1 batch scan (`docs/l1-batch-scan.md`). One `get_logs` per range; per
//! block, only the txs of posts listing our rollup are fetched, and the block
//! is traced only when one of them is a contract post.

use std::collections::BTreeMap;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, Log, Transaction};
use eez_protocol::abi::SCANNED_TOPICS;
use tracing::{Level, event};

use crate::error::{L1Error, L1Result};
use crate::fetch::trace_block;

mod model;
pub mod types;

pub use types::{AppliedEntry, EntryRole, Post, PostId, Settlement};

use self::model::{Event, ScannedLog, calls_in_tx, direct_call, group_blocks, read_block};

/// Initial block span for historical log scans, so wide catch-up gaps are
/// split before RPCs that reject long `eth_getLogs` ranges.
pub(crate) const LOG_SCAN_CHUNK_BLOCKS: u64 = 100_000;

/// Stateful `BatchPosted` log chunks. Callers own when scanned ranges are
/// committed to their local cursors.
#[derive(Debug)]
pub struct BatchLogChunks {
    to_block: u64,
    /// Pending ranges, oldest last.
    ranges: Vec<(u64, u64)>,
}

impl BatchLogChunks {
    /// Splits `[from_block, to_block]` into `LOG_SCAN_CHUNK_BLOCKS` ranges, oldest first out.
    pub(crate) fn new(from_block: u64, to_block: u64) -> Self {
        let span = LOG_SCAN_CHUNK_BLOCKS - 1;
        let chunk = |from: u64| (from, from.saturating_add(span).min(to_block));
        let starts = (from_block..=to_block).step_by(LOG_SCAN_CHUNK_BLOCKS as usize);
        let mut ranges: Vec<_> = starts.map(chunk).collect();
        ranges.reverse();
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

/// True when a `get_logs` failure means "matched too much", not "was wrong".
/// Providers cap the result count, which no fixed span respects; the wording
/// is client-specific but the remedy, narrowing, is not.
fn is_range_too_wide(err: &L1Error) -> bool {
    let L1Error::Provider(msg) = err else {
        return false;
    };
    let msg = msg.to_ascii_lowercase();
    [
        "exceeds max results",
        "query returned more than",
        "more than 10000 results",
        "response size exceeded",
        "query timeout exceeded",
        "range is too large",
        "block range too large",
        "too many results",
    ]
    .iter()
    .any(|cap| msg.contains(cap))
}

/// Scans `[from, to]`, halving the upper bound until `get_logs` is served.
/// Returns the posts and the block actually reached, which may be `< to`:
/// callers advance their cursor by that. Only a `get_logs` refusal narrows.
pub(crate) async fn scan_batch_logs_range_adaptive(
    logs: &impl Provider,
    trace: &impl Provider,
    eez: Address,
    rollup_id: u64,
    from: u64,
    to: u64,
) -> L1Result<(Vec<Post>, u64)> {
    let mut hi = to;
    let raw = loop {
        match fetch_scanned_logs(logs, eez, from, hi).await {
            Ok(raw) => break raw,
            // A single block that still exceeds the cap is unservable.
            Err(err) if is_range_too_wide(&err) && hi > from => {
                let mid = from + (hi - from) / 2;
                event!(
                    name: "eez.l1.scan_batch_logs.chunk_narrowed",
                    Level::WARN,
                    from,
                    requested_to = hi,
                    narrowed_to = mid,
                    error = %err,
                    "get_logs matched more than the provider serves; halving the range",
                );
                hi = mid;
            }
            Err(err) => return Err(err),
        }
    };
    Ok((read_logs(logs, trace, eez, rollup_id, &raw).await?, hi))
}

/// Scans the oldest pending chunk; `None` once every chunk is scanned.
pub(crate) async fn scan_next_batch_log_chunk(
    logs: &impl Provider,
    trace: &impl Provider,
    eez: Address,
    rollup_id: u64,
    chunks: &mut BatchLogChunks,
) -> L1Result<Option<Vec<Post>>> {
    let Some(&(from, to)) = chunks.ranges.last() else {
        return Ok(None);
    };
    let (posts, reached) =
        scan_batch_logs_range_adaptive(logs, trace, eez, rollup_id, from, to).await?;
    chunks.ranges.pop();
    if reached < to {
        // Narrowed: re-queue the tail so no range is skipped.
        chunks.ranges.push((reached + 1, to));
    }
    Ok(Some(posts))
}

/// Every post in `[from, to]` that lists `rollup_id`, in L1 position order.
/// `logs` serves logs and txs; `trace` serves block traces, pinned by hash.
pub(crate) async fn scan_range(
    logs: &impl Provider,
    trace: &impl Provider,
    eez: Address,
    rollup_id: u64,
    from: u64,
    to: u64,
) -> L1Result<Vec<Post>> {
    let raw = fetch_scanned_logs(logs, eez, from, to).await?;
    read_logs(logs, trace, eez, rollup_id, &raw).await
}

/// One `get_logs` for every scanned event: separate calls could answer from
/// different forks.
async fn fetch_scanned_logs(
    logs: &impl Provider,
    eez: Address,
    from: u64,
    to: u64,
) -> L1Result<Vec<Log>> {
    let filter = Filter::new()
        .address(eez)
        .event_signature(SCANNED_TOPICS.to_vec())
        .from_block(from)
        .to_block(BlockNumberOrTag::Number(to));
    logs.get_logs(&filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs({from}..={to}): {e}")))
}

/// Reads blocks in ascending order, each in log order. Per block, fetches the
/// txs whose `BatchPosted` lists `rollup_id`: a direct post is its own call,
/// and any other makes the scan trace the block once.
async fn read_logs(
    logs: &impl Provider,
    trace: &impl Provider,
    eez: Address,
    rollup_id: u64,
    raw: &[Log],
) -> L1Result<Vec<Post>> {
    let lists =
        |log: &ScannedLog| matches!(&log.event, Event::Posted(_, ids) if ids.contains(&rollup_id));
    let mut posts = Vec::new();
    for mut block in group_blocks(raw)? {
        let header = logs
            .get_block_by_hash(block.hash)
            .await
            .map_err(|error| L1Error::Provider(format!("fetch batch inclusion header: {error}")))?
            .ok_or_else(|| L1Error::Provider("batch inclusion header unavailable".into()))?;
        if header.header.number != block.number || header.header.hash != block.hash {
            return Err(L1Error::Provider(
                "batch inclusion header changed during scan".into(),
            ));
        }
        block.context = Some(eez_protocol::l1_context::LiveSettlementContext {
            parent_hash: header.header.parent_hash,
            timestamp: header.header.timestamp,
        });
        let number = block.number;
        let block_hash = block.hash;
        let mut calls_by_tx = BTreeMap::new();
        let mut traced = Vec::new();
        // Only txs with a post listing us are fetched; a direct post's call is the tx itself.
        for tx_logs in block.logs.chunk_by(|a, b| a.tx_index == b.tx_index) {
            let index = tx_logs[0].tx_index;
            let hash = tx_logs[0].tx_hash;
            let count = tx_logs.len();
            if tx_logs.iter().any(lists) {
                let tx = fetch_log_transaction(logs, number, block_hash, index, hash).await?;
                if let Some(call) = direct_call(&tx, eez, count) {
                    calls_by_tx.insert(index, (vec![call], count));
                } else {
                    traced.push((index, hash));
                }
            }
        }
        if !traced.is_empty() {
            event!(
                name: "eez.l1_scan.contract_post",
                Level::INFO,
                block = number,
                %block_hash,
                txs = traced.len(),
                "tracing a block whose contract post lists our rollup",
            );
            // One trace of the block gives every other such tx's committed calls.
            let hashes: Vec<_> = traced.iter().map(|&(_, hash)| hash).collect();
            let frames = trace_block(trace, number, block_hash, &hashes).await?;
            for ((index, hash), frame) in traced.into_iter().zip(frames) {
                let here = format!("block {number} tx {hash}");
                calls_by_tx.insert(index, calls_in_tx(&frame, eez, &here)?);
            }
        }
        posts.extend(read_block(&block, &calls_by_tx, rollup_id)?);
    }
    Ok(posts)
}

/// Fetches a post's tx by (block hash, index), never by tx hash, which a
/// minimal node can't serve. A block pinned by hash cannot change, so a
/// missing tx only means the node lags or pruned it.
async fn fetch_log_transaction(
    provider: &impl Provider,
    block: u64,
    block_hash: B256,
    tx_index: u64,
    tx_hash: B256,
) -> L1Result<Transaction> {
    let tx = provider
        .get_transaction_by_block_hash_and_index(block_hash, tx_index as usize)
        .await
        .map_err(|e| L1Error::Provider(format!("get_tx({block_hash}#{tx_index}): {e}")))?;
    tx.ok_or_else(|| L1Error::SourceIncomplete {
        block,
        detail: format!("tx {tx_hash} at ({block_hash}, {tx_index}) not served yet; retry"),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy_consensus::transaction::{Recovered, TxHashRef};
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_primitives::aliases::I192;
    use alloy_primitives::{Address, B256, Bytes, LogData, Signature, TxKind, U256, keccak256};
    use alloy_provider::{Provider, ProviderBuilder};
    use alloy_rpc_types_eth::{Block, Log, Transaction};
    use alloy_rpc_types_trace::geth::{CallFrame, CallLogFrame};
    use alloy_sol_types::{SolCall, SolEvent, SolValue};
    use alloy_transport::mock::Asserter;
    use eez_protocol::abi::{
        BatchPosted, EntryExecuted, EvmBatch, ExecutionConsumed, ExecutionEntrySol,
        L2ExecutionPerformed, L2TxSkipped, RollupIdWithProofSystemsSol, RollupUpdateSol,
        executeL2TxsCall, postAndVerifyBatchCall,
    };
    use eez_protocol::entries::EntryRole::{self, Anchor as A, Inbound};
    use eez_protocol::public_inputs::batch_shared_public_input;
    use serde_json::{Value, json};

    use super::model::{Call, calls_in_tx, direct_call, group_blocks, read_block};
    use super::{AppliedEntry, BatchLogChunks, Post, PostId, Settlement};
    use super::{scan_next_batch_log_chunk, scan_range};
    use crate::error::{L1Error, L1Result};

    const EEZ: Address = Address::repeat_byte(0xEE);
    const EOA: Address = Address::repeat_byte(0x05);
    const ROUTER: Address = Address::repeat_byte(0x0A);
    const US: u64 = 1;
    const BLOCK: u64 = 100;
    const HASH: B256 = B256::repeat_byte(0xB1);
    const H: B256 = B256::repeat_byte(0xC1);
    const H2: B256 = B256::repeat_byte(0xC2);
    const I0: EntryRole = Inbound { ordinal: 0 };
    const I1: EntryRole = Inbound { ordinal: 1 };

    fn root(n: u8) -> B256 {
        B256::repeat_byte(n)
    }

    /// An entry applying `(rollup, from, to)` updates, consumed as `call` from
    /// queue `queue`, with a rolling hash unique to its content.
    fn entry(updates: &[(u64, u8, u8)], call: B256, queue: u64) -> ExecutionEntrySol {
        let update = |&(id, from, to): &(u64, u8, u8)| RollupUpdateSol {
            rollupId: id,
            etherDelta: I192::ZERO,
            currentRoot: root(from),
            newRoot: root(to),
        };
        let mut entry = ExecutionEntrySol {
            rollupUpdates: updates.iter().map(update).collect(),
            proxyEntryHash: call,
            destinationRollupId: queue,
            success: true,
            ..Default::default()
        };
        entry.rollingHash = keccak256(entry.abi_encode());
        entry
    }

    fn anchor(from: u8, to: u8) -> ExecutionEntrySol {
        entry(&[(US, from, to)], B256::ZERO, US)
    }

    fn inbound(call: B256, from: u8, to: u8) -> ExecutionEntrySol {
        entry(&[(US, from, to)], call, US)
    }

    fn theirs() -> ExecutionEntrySol {
        entry(&[(2, 0x20, 0x21)], B256::ZERO, 2)
    }

    fn batch(ids: &[u64], entries: Vec<ExecutionEntrySol>, immediate: u64) -> EvmBatch {
        let listed = |&id: &u64| RollupIdWithProofSystemsSol {
            rollupId: id,
            proofSystemIndexes: Vec::new(),
        };
        EvmBatch {
            entries,
            immediateEntryCount: U256::from(immediate),
            rollupIdsWithProofSystems: ids.iter().map(listed).collect(),
            callData: vec![0xDA].into(),
            ..Default::default()
        }
    }

    /// An anchor `n -> n+1`, then an inbound `n+1 -> n+2` answering `call`.
    fn ours(n: u8, call: B256) -> EvmBatch {
        let entries = vec![anchor(n, n + 1), inbound(call, n + 1, n + 2)];
        batch(&[US], entries, 1)
    }

    fn call(batch: &EvmBatch, from: Address, logs: usize) -> Call {
        let batch = batch.clone();
        let input = postAndVerifyBatchCall { batch }.abi_encode().into();
        Call { input, from, logs }
    }

    fn root_log(rollup: u64, root: B256) -> LogData {
        let event = L2ExecutionPerformed {
            rollupId: rollup,
            newRoot: root,
            etherBalance: U256::ZERO,
        };
        event.encode_log_data()
    }

    fn exec(index: u64, rolling_hash: B256) -> LogData {
        let index = U256::from(index);
        let event = EntryExecuted {
            entryIndex: index,
            rollingHash: rolling_hash,
        };
        event.encode_log_data()
    }

    fn consumed(call: B256, queue: u64, slot: u64) -> LogData {
        let event = ExecutionConsumed {
            crossChainCallHash: call,
            rollupId: queue,
            entryQueueIndex: U256::from(slot),
        };
        event.encode_log_data()
    }

    fn skip(index: u64) -> S {
        let (index, data) = (U256::from(index), Bytes::new());
        R(L2TxSkipped {
            entryIndex: index,
            revertData: data,
        }
        .encode_log_data())
    }

    /// The `BatchPosted` EEZ emits for `batch` posted by `sender`.
    fn posted(batch: &EvmBatch, sender: Address) -> LogData {
        let spi = batch_shared_public_input(batch, sender).expect("supported");
        let ids = batch.rollupIdsWithProofSystems.iter().map(|r| r.rollupId);
        BatchPosted {
            sharedPublicInput: spi,
            rollupIds: ids.collect(),
        }
        .encode_log_data()
    }

    /// One step of `BLOCK`, in log order. `Tx(i)`: later logs come from tx `i`.
    /// `L(b, e)`: entry `e` of batch `b` runs in its post's leading run;
    /// `C(b, e, s)`: it is consumed from slot `s`. `O`: a router `CALL` into
    /// EEZ starts, and the next post ends it; a tx without one posts directly.
    /// `P(b)`: the `BatchPosted` of batch `b`; `Px(b, c)`: its call carried
    /// batch `c` instead. `R(log)`: a raw log. A post without an `O` in a tx
    /// that already has a call adds none; a direct call's log count is
    /// `usize::MAX` until the whole tx is counted.
    #[derive(Clone)]
    enum S {
        Tx(u64),
        L(usize, usize),
        C(usize, usize, u64),
        O,
        P(usize),
        Px(usize, usize),
        R(LogData),
    }
    use S::{C, L, O, P, Px, R, Tx};

    type Calls = BTreeMap<u64, (Vec<Call>, usize)>;

    /// The logs `steps` emit, and the calls the fetch layer reads for them.
    fn build(batches: &[EvmBatch], steps: &[S]) -> (Vec<Log>, Calls) {
        let (mut tx, mut logs, mut calls) = (0, Vec::<Log>::new(), Calls::new());
        let (mut open, mut listing) = (None, Vec::new());
        for step in steps {
            let ran = |b: usize, e: usize, slot: Option<u64>| {
                let entry = &batches[b].entries[e];
                let (call, queue) = (entry.proxyEntryHash, entry.destinationRollupId);
                let roots = entry.rollupUpdates.iter().map(|u| (u.rollupId, u.newRoot));
                let took = slot.map(|slot| consumed(call, queue, slot)).into_iter();
                let roots = roots.map(|(rollup, root)| root_log(rollup, root));
                let done = exec(slot.unwrap_or(0), entry.rollingHash);
                took.chain(roots).chain([done]).collect()
            };
            let emitted = match *step {
                Tx(i) => {
                    tx = i;
                    Vec::new()
                }
                L(b, e) => ran(b, e, None),
                C(b, e, slot) => ran(b, e, Some(slot)),
                R(ref log) => vec![log.clone()],
                O => {
                    open = Some(logs.len());
                    let tx_calls = &mut calls.entry(tx).or_insert((Vec::new(), 0)).0;
                    tx_calls.push(call(&batches[0], ROUTER, 0));
                    Vec::new()
                }
                P(b) | Px(b, _) => {
                    let carried = &batches[if let Px(_, c) = *step { c } else { b }];
                    let ids = &batches[b].rollupIdsWithProofSystems;
                    listing.extend(ids.iter().any(|r| r.rollupId == US).then_some(tx));
                    let sender = if open.is_some() { ROUTER } else { EOA };
                    let tx_calls = &mut calls.entry(tx).or_insert((Vec::new(), 0)).0;
                    match (open.take(), tx_calls.last_mut()) {
                        (Some(start), Some(last)) => {
                            *last = call(carried, ROUTER, logs.len() + 1 - start);
                        }
                        (None, None) => tx_calls.push(call(carried, EOA, usize::MAX)),
                        _ => {}
                    }
                    vec![posted(&batches[b], sender)]
                }
            };
            for data in emitted {
                logs.push(rpc_log(tx, logs.len() as u64, data));
            }
        }
        calls.retain(|tx, _| listing.contains(tx));
        for (&tx, (calls, count)) in &mut calls {
            *count = logs
                .iter()
                .filter(|log| log.transaction_index == Some(tx))
                .count();
            let direct = calls.iter_mut().filter(|call| call.logs == usize::MAX);
            direct.for_each(|call| call.logs = *count);
        }
        (logs, calls)
    }

    fn rpc_log(tx: u64, log_index: u64, data: LogData) -> Log {
        Log {
            inner: alloy_primitives::Log { address: EEZ, data },
            block_hash: Some(HASH),
            block_number: Some(BLOCK),
            transaction_hash: Some(B256::from(U256::from(0x1000 + tx))),
            transaction_index: Some(tx),
            log_index: Some(log_index),
            ..Default::default()
        }
    }

    fn read(batches: &[EvmBatch], steps: &[S]) -> L1Result<Vec<Post>> {
        let (logs, calls) = build(batches, steps);
        read_block(&group_blocks(&logs)?.remove(0), &calls, US)
    }

    /// What a post settled: `applied` in L1's order, moving us `from -> to`.
    fn s(applied: &[(usize, EntryRole)], from: u8, to: u8) -> Settlement {
        resumed(applied, from, to, false)
    }

    fn resumed(applied: &[(usize, EntryRole)], from: u8, to: u8, resumed: bool) -> Settlement {
        let applied = applied
            .iter()
            .map(|&(entry_index, role)| AppliedEntry { entry_index, role });
        Settlement::new(applied.collect(), Some(root(to)), Some(root(from)), resumed)
    }

    #[test]
    fn a_malformed_get_logs_answer_fails_with_its_class() {
        let ours = posted(&ours(0, H), EOA);
        let mut forked = rpc_log(1, 1, ours.clone());
        forked.block_hash = Some(B256::ZERO);
        let mut unplaced = rpc_log(1, 1, ours.clone());
        unplaced.log_index = None;
        let short = LogData::new_unchecked(vec![BatchPosted::SIGNATURE_HASH], Bytes::new());
        let provider: fn(&L1Error) -> bool = |e| matches!(e, L1Error::Provider(_));
        #[rustfmt::skip]
        let cases: [(&str, Log, fn(&L1Error) -> bool); 3] = [
            ("two block hashes", forked, |e| matches!(e, L1Error::SourceIncomplete { .. })),
            ("a log without its position", unplaced, provider),
            ("an undecodable log", rpc_log(1, 1, short), |e| matches!(e, L1Error::Decode(_))),
        ];
        for (name, log, class) in cases {
            let err = group_blocks(&[rpc_log(1, 0, ours.clone()), log]).expect_err(name);
            assert!(class(&err), "{name}: {err}");
        }
    }

    #[test]
    fn each_post_settles_exactly_what_l1_applied_of_it() {
        let shared = |entries, immediate| batch(&[US, 2], entries, immediate);
        let solo = |entries, immediate| batch(&[US], entries, immediate);
        let mut bound = ours(0, H);
        bound.bindMsgSenderInPublicInput = true;
        #[rustfmt::skip]
        let cases: Vec<(&str, Vec<EvmBatch>, Vec<S>, Vec<Settlement>)> = vec![
            ("a direct post's queue, consumed by a later tx", vec![ours(0, H)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0)], vec![s(&[(0, A), (1, I0)], 0, 2)]),
            // #175: the rival's BatchPosted reset the queue after ours.
            ("the latest post listing us owns later consumption",
                vec![ours(0, H), solo(vec![inbound(H, 1, 2)], 0)],
                vec![L(0, 0), P(0), Tx(1), O, P(1), Tx(2), C(1, 0, 0)],
                vec![s(&[(0, A)], 0, 1), s(&[(0, I0)], 1, 2)]),
            ("shape (a): a router posts, then consumes its own queue", vec![ours(0, H)],
                vec![O, L(0, 0), P(0), C(0, 1, 0)], vec![s(&[(0, A), (1, I0)], 0, 2)]),
            ("shape (b): consumption before a post belongs to the earlier one",
                vec![ours(0, H), ours(2, H2)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0), O, L(1, 0), P(1)],
                vec![s(&[(0, A), (1, I0)], 0, 2), s(&[(0, A)], 2, 3)]),
            ("shape (c): of two posts in one tx, the latest lister owns the queue",
                vec![ours(0, H), ours(1, H)],
                vec![O, L(0, 0), P(0), O, L(1, 0), P(1), Tx(1), C(1, 1, 0)],
                vec![s(&[(0, A)], 0, 1), s(&[(0, A), (1, I0)], 1, 3)]),
            // Both log `ExecutionConsumed(H, us, 0)`; only the call boundary differs.
            ("the trace separates a deferred entry from a meta-hook one",
                vec![ours(0, H), solo(vec![inbound(H, 2, 3)], 1)],
                vec![O, L(0, 0), P(0), C(0, 1, 0), O, C(1, 0, 0), P(1)],
                vec![s(&[(0, A), (1, I0)], 0, 2), s(&[(0, I0)], 2, 3)]),
            // H8
            ("each call's skips name its own entries",
                vec![solo(vec![anchor(0, 1), anchor(0, 2)], 2),
                    solo(vec![anchor(2, 3), anchor(3, 4)], 2)],
                vec![O, skip(0), L(0, 1), P(0), O, L(1, 0), skip(1), P(1)],
                vec![resumed(&[(1, A)], 0, 2, true), s(&[(0, A)], 2, 3)]),
            // H11: the queue, not the roots, names the owner.
            ("an entry for us queued under another rollup",
                vec![shared(vec![anchor(0, 1), entry(&[(US, 1, 2), (2, 32, 33)], H, 2)], 1)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0)], vec![s(&[(0, A), (1, I0)], 0, 2)]),
            // H12
            ("another rollup's entries shift neither settlement nor ordinals",
                vec![shared(vec![anchor(0, 1), entry(&[(2, 32, 33)], H2, 2), inbound(H, 1, 2)], 1)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0), C(0, 2, 0)],
                vec![s(&[(0, A), (2, I0)], 0, 2)]),
            // #99: consumption may skip entries; the slot still names one.
            ("a slot past a gap",
                vec![solo(vec![anchor(0, 1), inbound(H, 1, 2), inbound(H2, 1, 3)], 1)],
                vec![L(0, 0), P(0), Tx(1), C(0, 2, 1)], vec![s(&[(0, A), (2, I1)], 0, 3)]),
            ("repeated roots settle by slot",
                vec![solo(vec![anchor(0, 1), inbound(H, 1, 2), inbound(H, 2, 1)], 1)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0), C(0, 2, 1)],
                vec![s(&[(0, A), (1, I0), (2, I1)], 0, 1)]),
            ("a partial prefix",
                vec![solo(vec![anchor(0, 1), inbound(H, 1, 2), inbound(H2, 2, 3)], 1)],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 0)], vec![s(&[(0, A), (1, I0)], 0, 2)]),
            ("anchor only", vec![ours(0, H)], vec![L(0, 0), P(0)], vec![s(&[(0, A)], 0, 1)]),
            ("a run from our first entry, behind another rollup's, is not resumed",
                vec![shared(vec![theirs(), anchor(0, 1)], 2)],
                vec![L(0, 0), L(0, 1), P(0)], vec![s(&[(1, A)], 0, 1)]),
            ("a run past our skipped first entry is resumed",
                vec![shared(vec![theirs(), anchor(0, 1), anchor(0, 2)], 3)],
                vec![L(0, 0), skip(1), L(0, 2), P(0)], vec![resumed(&[(2, A)], 0, 2, true)]),
            ("a post listing us that applies nothing", vec![solo(vec![inbound(H, 0, 1)], 0)],
                vec![P(0)], vec![Settlement::NONE]),
            // Halting here would stop every node on another rollup's skip.
            ("another rollup's skips, in its post's call or an untraced tx, leave ours alone",
                vec![ours(0, H), batch(&[2], vec![theirs()], 1)],
                vec![O, skip(0), P(1), O, L(0, 0), P(0), Tx(1), skip(0), P(1)],
                vec![s(&[(0, A)], 0, 1)]),
            ("a sender-bound post verifies against its frame's sender", vec![bound],
                vec![O, L(0, 0), P(0)], vec![s(&[(0, A)], 0, 1)]),
        ];
        for (name, batches, steps, expected) in cases {
            let posts = read(&batches, &steps).expect(name);
            let settlements: Vec<_> = posts.into_iter().map(|post| post.settlement).collect();
            assert_eq!(settlements, expected, "{name}");
        }
    }

    #[test]
    fn contradictions_fail_with_their_class() {
        let x = ours(0, H);
        let r2 = R(root_log(2, H));
        let with = |mut batch: EvmBatch, change: fn(&mut EvmBatch)| {
            change(&mut batch);
            batch
        };
        let other = with(x.clone(), |b| b.callData = vec![0xDB].into());
        let blobs = with(x.clone(), |b| b.blobIndices = vec![Default::default()]);
        let numbered = with(x.clone(), |b| b.blockNumber = 7);
        let wrong_root = with(x.clone(), |b| b.entries[0].rollupUpdates[0].newRoot = H);
        let duo = batch(&[US, 2], x.entries.clone(), 1);
        let duo_other = with(duo.clone(), |b| b.callData = vec![0xDB].into());
        let foreign = batch(&[2], vec![theirs()], 1);
        let queued_in_2 = batch(&[US, 2], vec![entry(&[(US, 1, 2), (2, 32, 33)], H, 2)], 0);
        let twins = batch(&[US], vec![anchor(0, 1), anchor(0, 1)], 2);
        let leading_and_table = batch(&[US], vec![anchor(0, 1), inbound(H, 1, 2)], 2);
        let table = batch(&[US], vec![inbound(H, 0, 1), inbound(H2, 1, 2)], 2);
        let two_queued = batch(
            &[US],
            vec![anchor(0, 1), inbound(H, 1, 2), inbound(H2, 2, 3)],
            1,
        );
        let decode: fn(&L1Error) -> bool = |e| matches!(e, L1Error::Decode(_));
        let unsupported: fn(&L1Error) -> bool = |e| matches!(e, L1Error::Unsupported { .. });
        #[rustfmt::skip]
        let cases: Vec<(&str, Vec<EvmBatch>, Vec<S>, fn(&L1Error) -> bool)> = vec![
            // Grammar, on rollup 2's logs, which the scan would otherwise ignore.
            ("EntryExecuted with no entry open", vec![], vec![R(exec(0, H))], decode),
            ("an interrupted entry", vec![], vec![r2.clone(), skip(0)], decode),
            ("an entry open at its tx's end", vec![], vec![r2.clone()], decode),
            ("an entry applying no update", vec![],
                vec![R(consumed(H, 2, 0)), R(exec(0, H))], decode),
            ("EntryExecuted not repeating the consumed slot", vec![],
                vec![R(consumed(H, 2, 2)), r2.clone(), R(exec(1, H))], decode),
            ("roots not ascending by rollup", vec![],
                vec![r2.clone(), r2.clone(), R(exec(0, H))], decode),
            // Labelling.
            ("fewer calls than BatchPosted", vec![x.clone()], vec![O, P(0), P(0)], unsupported),
            ("more calls than BatchPosted", vec![x.clone()], vec![O, L(0, 0), P(0), O], decode),
            // Only the boundary is wrong: rollup 2's entry, outside every call, would be ignored.
            ("a record straddling a call boundary", vec![x.clone()],
                vec![R(consumed(H, 2, 0)), O, R(root_log(2, H)), R(exec(0, H)), L(0, 0), P(0)],
                decode),
            // Verification: the call carries batch 1, whose entries are batch 0's.
            ("other rollupIds than BatchPosted's", vec![duo.clone(), x.clone()],
                vec![L(0, 0), Px(0, 1)], decode),
            ("a commitment mismatch, us alone", vec![x.clone(), other],
                vec![L(0, 0), Px(0, 1)], decode),
            ("a commitment mismatch, co-listed", vec![duo, duo_other],
                vec![L(0, 0), Px(0, 1)], unsupported),
            ("blobs", vec![x.clone(), blobs], vec![L(0, 0), Px(0, 1)], unsupported),
            ("a nonzero blockNumber", vec![x.clone(), numbered], vec![L(0, 0), Px(0, 1)],
                unsupported),
            // Settlement.
            ("a queue no post listed", vec![x.clone()], vec![C(0, 1, 0)], decode),
            ("a queue whose owner does not list us", vec![foreign.clone(), queued_in_2],
                vec![L(0, 0), P(0), Tx(1), C(1, 0, 0)], decode),
            ("our entry in a foreign post's call", vec![x.clone(), foreign],
                vec![O, L(0, 0), P(0), O, L(0, 0), P(1)], decode),
            ("a rolling hash mismatch", vec![x.clone()],
                vec![R(root_log(US, root(1))), R(exec(0, H)), P(0)], decode),
            ("a root mismatch", vec![x.clone(), wrong_root], vec![L(1, 0), P(0)], decode),
            ("a missing queue slot", vec![x.clone()],
                vec![L(0, 0), P(0), Tx(1), C(0, 1, 5)], decode),
            ("a consumed call hash unlike the entry's", vec![x.clone()],
                vec![L(0, 0), P(0), Tx(1), R(consumed(H2, US, 0)), R(root_log(US, root(2))),
                    R(exec(0, x.entries[1].rollingHash))], decode),
            // Each order check alone: the same logs in order would settle.
            ("a post ending before its leading run reported", vec![x.clone()], vec![P(0)], decode),
            ("a skip naming another entry than the next", vec![twins],
                vec![skip(1), L(0, 0), P(0)], decode),
            ("a leading-run result after the meta hook", vec![leading_and_table],
                vec![C(0, 1, 0), L(0, 0), P(0)], decode),
            ("a meta-hook slot moving backwards", vec![table],
                vec![C(0, 1, 1), C(0, 0, 0), P(0)], decode),
            ("a queue slot moving backwards", vec![two_queued],
                vec![L(0, 0), P(0), Tx(1), C(0, 2, 1), C(0, 1, 0)], decode),
            ("a skip between calls", vec![x.clone()], vec![O, L(0, 0), P(0), skip(0)], decode),
            ("a leading-run entry between calls", vec![x],
                vec![O, L(0, 0), P(0), r2, R(exec(0, H))], decode),
        ];
        for (name, batches, steps, class) in cases {
            let err = read(&batches, &steps).expect_err(name);
            assert!(class(&err), "{name}: {err}");
        }
    }

    /// A committed frame from `ROUTER` emitting `logs` scanned EEZ logs.
    fn frame(
        typ: &str,
        to: Address,
        input: &Bytes,
        logs: usize,
        calls: Vec<CallFrame>,
    ) -> CallFrame {
        let topics = Some(vec![BatchPosted::SIGNATURE_HASH]);
        let log = CallLogFrame {
            address: Some(EEZ),
            topics,
            ..Default::default()
        };
        let (typ, to, input, logs) = (typ.into(), Some(to), input.clone(), vec![log; logs]);
        CallFrame {
            typ,
            from: ROUTER,
            to,
            input,
            logs,
            calls,
            ..Default::default()
        }
    }

    #[test]
    fn the_walk_reads_committed_post_calls_only() {
        let (input, other) = (
            call(&ours(0, H), EOA, 0).input,
            executeL2TxsCall::SELECTOR.into(),
        );
        let post = |logs, calls| frame("CALL", EEZ, &input, logs, calls);
        let other_call = |logs| frame("CALL", EEZ, &other, logs, Vec::new());
        let router = |calls| frame("CALL", ROUTER, &Bytes::new(), 0, calls);
        let failed = |mut frame: CallFrame| {
            frame.error = Some("execution reverted".into());
            frame
        };
        #[rustfmt::skip]
        let cases: Vec<(&str, CallFrame, Option<(Vec<usize>, usize)>)> = vec![
            ("a top-level post", post(2, vec![]), Some((vec![2], 2))),
            ("a routed post", router(vec![post(1, vec![]), other_call(1)]), Some((vec![1], 2))),
            ("logs under a post", post(1, vec![other_call(1)]), Some((vec![2], 2))),
            ("a post under a reverted ancestor",
                router(vec![failed(router(vec![post(1, vec![])])), post(1, vec![])]),
                Some((vec![1], 1))),
            ("a reverted post", router(vec![failed(post(1, vec![]))]), Some((vec![], 0))),
            ("logs on a failed child", post(1, vec![failed(other_call(1))]), Some((vec![1], 1))),
            ("a DELEGATECALL into EEZ", router(vec![frame("DELEGATECALL", EEZ, &input, 0, vec![])]),
                Some((vec![], 0))),
            ("another EEZ selector", router(vec![other_call(0)]), Some((vec![], 0))),
            ("a nested committed post", post(1, vec![post(1, vec![])]), None),
        ];
        for (name, root, expected) in cases {
            let walked = calls_in_tx(&root, EEZ, name).ok();
            let counts = walked.map(|(calls, n)| (calls.iter().map(|c| c.logs).collect(), n));
            assert_eq!(counts, expected, "{name}");
        }
        let cases = [
            (EEZ, &input, true),
            (ROUTER, &input, false),
            (EEZ, &other, false),
        ];
        for (to, input, direct) in cases {
            let tx = signed_tx(to, input.clone(), 0);
            let expected = direct.then(|| call(&ours(0, H), EOA, 4));
            assert_eq!(direct_call(&tx, EEZ, 4), expected, "{to} {input}");
        }
    }

    fn mock() -> (Asserter, impl Provider) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        (asserter, provider)
    }

    /// A tx `EOA` sent to `to`; its hash depends on `input` and `nonce`.
    fn signed_tx(to: Address, input: Bytes, nonce: u64) -> Transaction {
        let to = TxKind::Call(to);
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            to,
            input,
            ..Default::default()
        };
        let tx = TxEnvelope::Legacy(tx.into_signed(Signature::test_signature()));
        Transaction::from_transaction(Recovered::new_unchecked(tx, EOA), Default::default())
    }

    /// `logs` with tx `index` served as `tx`.
    fn served_as(mut logs: Vec<Log>, index: u64, tx: &Transaction) -> Vec<Log> {
        for log in logs
            .iter_mut()
            .filter(|log| log.transaction_index == Some(index))
        {
            log.transaction_hash = Some(*tx.inner.tx_hash());
        }
        logs
    }

    /// A router post listing us at tx 0, and its tx.
    fn routed() -> (Vec<Log>, Transaction) {
        let tx = signed_tx(ROUTER, Bytes::new(), 0);
        let logs = build(&[ours(0, H)], &[O, L(0, 0), P(0)]).0;
        (served_as(logs, 0, &tx), tx)
    }

    fn serve_inclusion_header(asserter: &Asserter, number: u64, hash: B256) {
        let mut block = Block::<Transaction>::default();
        block.header.inner.number = number;
        block.header.hash = hash;
        block.header.inner.parent_hash = B256::repeat_byte(0x99);
        block.header.inner.timestamp = 42;
        asserter.push_success(&block);
    }

    /// The trace endpoint knows the block, then traces `frames`.
    fn serve_trace(asserter: &Asserter, frames: &[(B256, CallFrame)]) {
        asserter.push_success(&Block::<Transaction>::default());
        let trace = |(tx, result): &(B256, CallFrame)| json!({"txHash": tx, "result": result});
        asserter.push_success(&frames.iter().map(trace).collect::<Vec<_>>());
    }

    /// Blocks are read in ascending order even when `get_logs` answers out of
    /// order: each block's txs are fetched in turn from one FIFO.
    #[tokio::test]
    async fn direct_posts_are_read_from_their_txs_and_a_foreign_post_is_never_fetched() {
        let (x, y) = (ours(0, H), ours(2, H2));
        let tx_x = signed_tx(EEZ, call(&x, EOA, 0).input, 0);
        let tx_y = signed_tx(EEZ, call(&y, EOA, 0).input, 1);
        let steps = [L(0, 0), P(0), Tx(1), L(1, 0), P(1)];
        let first = build(&[x, batch(&[2], vec![theirs()], 1)], &steps).0;
        let mut next = served_as(build(&[y], &[L(0, 0), P(0)]).0, 0, &tx_y);
        for log in &mut next {
            log.block_number = Some(BLOCK + 1);
            log.block_hash = Some(B256::repeat_byte(0xB2));
        }
        let ((asserter, rpc), (_, no_trace)) = (mock(), mock());
        asserter.push_success(&[next, served_as(first, 0, &tx_x)].concat());
        serve_inclusion_header(&asserter, BLOCK, HASH);
        asserter.push_success(&tx_x);
        serve_inclusion_header(&asserter, BLOCK + 1, B256::repeat_byte(0xB2));
        asserter.push_success(&tx_y);
        // Any other call meets an empty FIFO and fails the scan.
        let posts = scan_range(&rpc, &no_trace, EEZ, US, BLOCK, BLOCK + 1).await;
        let posts = posts.expect("read");
        let id = PostId {
            l1_block: BLOCK,
            l1_block_hash: HASH,
            log_index: 2,
        };
        let first = (posts[0].id, posts[0].sender, &posts[0].settlement);
        assert_eq!(first, (id, EOA, &s(&[(0, A)], 0, 1)));
        let next = (posts[1].id.l1_block, posts[1].tx_hash);
        assert_eq!(next, (BLOCK + 1, *tx_y.inner.tx_hash()));
        assert!(asserter.read_q().is_empty());
    }

    #[tokio::test]
    async fn a_contract_post_traces_its_block_once_and_reads_the_frame() {
        let (x, y) = (ours(0, H), ours(1, H2));
        let direct = signed_tx(EEZ, call(&x, EOA, 0).input, 0);
        let router = signed_tx(ROUTER, Bytes::new(), 1);
        let logs = build(&[x, y.clone()], &[L(0, 0), P(0), Tx(1), O, L(1, 0), P(1)]).0;
        let into_eez = frame("CALL", EEZ, &call(&y, ROUTER, 0).input, 3, Vec::new());
        let traced = frame("CALL", ROUTER, &Bytes::new(), 0, vec![into_eez]);
        // One FIFO pins the order: logs, the txs, then one trace.
        let (asserter, rpc) = mock();
        asserter.push_success(&served_as(served_as(logs, 0, &direct), 1, &router));
        serve_inclusion_header(&asserter, BLOCK, HASH);
        asserter.push_success(&direct);
        asserter.push_success(&router);
        serve_trace(&asserter, &[(*router.inner.tx_hash(), traced)]);
        let posts = scan_range(&rpc, &rpc, EEZ, US, BLOCK, BLOCK)
            .await
            .expect("read");
        assert!(asserter.read_q().is_empty());
        assert_eq!(posts[1].sender, ROUTER, "EEZ's msg.sender");
        assert_eq!(
            posts[1].batch.abi_encode(),
            y.abi_encode(),
            "the frame's bytes"
        );
        let settled = [&posts[0].settlement, &posts[1].settlement];
        assert_eq!(settled, [&s(&[(0, A)], 0, 1), &s(&[(0, A)], 1, 2)]);
    }

    #[tokio::test]
    async fn an_unreadable_source_fails_the_scan_with_its_class() {
        let (logs, tx) = routed();
        let lagging = |e: &L1Error| matches!(e, L1Error::SourceIncomplete { block: BLOCK, .. });
        let provider = |e: &L1Error| matches!(e, L1Error::Provider(_));
        let decode = |e: &L1Error| matches!(e, L1Error::Decode(_));
        // The trace commits one EEZ log more than get_logs returned, outside the post.
        let input = call(&ours(0, H), ROUTER, 0).input;
        let into_eez = frame("CALL", EEZ, &input, 3, vec![]);
        let traced = frame("CALL", ROUTER, &Bytes::new(), 1, vec![into_eez]);
        let extra = json!([{"txHash": tx.inner.tx_hash(), "result": traced}]);
        let (tx, block) = (json!(tx), json!(Block::<Transaction>::default()));
        // The trace's own failures are classified in `fetch`; one shows they propagate.
        #[rustfmt::skip]
        let cases: [(&str, Vec<Value>, fn(&L1Error) -> bool); 3] = [
            ("no tx at the post's index", vec![json!(null)], lagging),
            ("a trace without the tx", vec![tx.clone(), block.clone(), json!([])], provider),
            ("a trace committing other logs than get_logs", vec![tx, block, extra], decode),
        ];
        for (name, answers, class) in cases {
            let (asserter, rpc) = mock();
            asserter.push_success(&logs);
            serve_inclusion_header(&asserter, BLOCK, HASH);
            for answer in &answers {
                asserter.push_success(answer);
            }
            let err = scan_range(&rpc, &rpc, EEZ, US, BLOCK, BLOCK)
                .await
                .expect_err(name);
            assert!(class(&err), "{name}: {err}");
            assert!(asserter.read_q().is_empty(), "{name}: every answer read");
        }
    }

    /// A refused `get_logs` narrows and re-queues the tail, down to one block,
    /// which then fails; a trace failure worded like a result cap propagates
    /// and keeps the range whole.
    #[tokio::test]
    async fn only_a_get_logs_refusal_narrows_the_range() {
        let (asserter, rpc) = mock();
        let mut chunks = BatchLogChunks::new(0, 150_000);
        assert_eq!(chunks.ranges, [(100_000, 150_000), (0, 99_999)]);
        asserter.push_failure_msg("query exceeds max results 20000");
        asserter.push_success(&Vec::<Log>::new());
        let posts = scan_next_batch_log_chunk(&rpc, &rpc, EEZ, US, &mut chunks).await;
        assert!(posts.expect("narrowed").expect("a chunk").is_empty());
        assert_eq!(chunks.ranges, [(100_000, 150_000), (50_000, 99_999)]);

        let (asserter, rpc) = mock();
        let mut chunks = BatchLogChunks::new(BLOCK, BLOCK);
        asserter.push_failure_msg("query exceeds max results 20000");
        // Read only by a retry of the same one-block range.
        asserter.push_success(&Vec::<Log>::new());
        let err = scan_next_batch_log_chunk(&rpc, &rpc, EEZ, US, &mut chunks).await;
        assert!(matches!(err, Err(L1Error::Provider(_))), "{err:?}");
        let left = (chunks.ranges, asserter.read_q().len());
        assert_eq!(left, (vec![(BLOCK, BLOCK)], 1));

        let (asserter, rpc) = mock();
        let (logs, tx) = routed();
        let mut chunks = BatchLogChunks::new(BLOCK, BLOCK + 1);
        asserter.push_success(&logs);
        serve_inclusion_header(&asserter, BLOCK, HASH);
        asserter.push_success(&tx);
        asserter.push_success(&Block::<Transaction>::default());
        asserter.push_failure_msg("query timeout exceeded");
        // Read only by a wrongly narrowed retry.
        asserter.push_success(&Vec::<Log>::new());
        let err = scan_next_batch_log_chunk(&rpc, &rpc, EEZ, US, &mut chunks).await;
        assert!(matches!(err, Err(L1Error::Provider(_))), "{err:?}");
        let left = (chunks.ranges, asserter.read_q().len());
        assert_eq!(left, (vec![(BLOCK, BLOCK + 1)], 1));
    }
}
