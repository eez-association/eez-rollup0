//! The pure per-block model of `docs/l1-batch-scan.md`: logs parse into items,
//! calls label them, queues name owners, and each record restates one entry of
//! its owner's verified bytes.

use std::collections::BTreeMap;

use alloy_consensus::Transaction as _;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{Log, Transaction};
use alloy_rpc_types_trace::geth::CallFrame;
use alloy_sol_types::{SolCall, SolEvent};
use eez_protocol::Rollup0Deployment;
use eez_protocol::abi::{
    BatchPosted, EntryExecuted, EvmBatch, ExecutionConsumed, ExecutionEntrySol,
    L2ExecutionPerformed, L2TxSkipped, SCANNED_TOPICS, postAndVerifyBatchCall,
};
use eez_protocol::entries::{EntryRole, entry_roles};
use eez_protocol::public_inputs::{
    SettlementContext, batch_shared_public_input, batch_shared_public_input_with,
};

use super::types::{AppliedEntry, Post, PostId, Settlement};
use crate::error::{L1Error, L1Result};

/// Returns `Decode` unless `$ok`: the logs contradict EEZ or the post's bytes.
macro_rules! ensure {
    ($ok:expr, $($msg:tt)+) => {
        if !$ok {
            return Err(L1Error::Decode(format!($($msg)+)));
        }
    };
}

/// One L1 block's scanned EEZ logs, in log order.
#[derive(Debug)]
pub(super) struct Block {
    pub number: u64,
    pub hash: B256,
    pub logs: Vec<ScannedLog>,
}

#[derive(Debug)]
pub(super) struct ScannedLog {
    pub tx_index: u64,
    pub tx_hash: B256,
    pub log_index: u64,
    pub event: Event,
}

/// The five scanned events, in `SCANNED_TOPICS` order.
#[derive(Debug)]
pub(super) enum Event {
    /// `BatchPosted(sharedPublicInput, rollupIds)`: a post's last log; it reset these queues.
    Posted(B256, Vec<u64>),
    Consumed(Consumed),
    /// `L2ExecutionPerformed(rollupId, newRoot)`.
    Root(u64, B256),
    /// `EntryExecuted(entryIndex, rollingHash)`.
    Executed(u64, B256),
    /// `L2TxSkipped(entryIndex)`.
    Skipped(u64),
}

/// `ExecutionConsumed`: slot `index` of queue `queue` or of the meta-hook table (EEZ.sol:933-949).
#[derive(Debug, Clone, Copy)]
pub(super) struct Consumed {
    hash: B256,
    queue: u64,
    index: u64,
}

/// A committed `postAndVerifyBatch` call into EEZ: the exact bytes, EEZ's
/// `msg.sender`, the scanned logs of its committed subtree, and the versioned
/// hashes of its tx, which `BLOBHASH` reads for every call in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Call {
    pub input: Bytes,
    pub from: Address,
    pub logs: usize,
    pub blob_hashes: Vec<B256>,
}

/// What a blob post binds beyond its bytes: the manager
/// deployment, and the block whose timestamp and parent `getCustomData` read.
#[derive(Debug, Clone, Copy)]
pub(super) struct CandidateBlock<'a> {
    pub deployment: &'a Rollup0Deployment,
    pub timestamp: u64,
    pub parent_hash: B256,
}

fn at(block: u64, log: &ScannedLog) -> String {
    format!("block {block} tx {} log {}", log.tx_hash, log.log_index)
}

/// A log whose position checked out, before it is decoded.
#[derive(Debug)]
struct Placed<'a> {
    log_index: u64,
    tx_index: u64,
    tx_hash: B256,
    log: &'a Log,
}

/// Groups a `get_logs` answer into ascending blocks in log order. Position
/// faults are found before any log is decoded, so they are retried.
pub(super) fn group_blocks(raw: &[Log]) -> L1Result<Vec<Block>> {
    let mut blocks: BTreeMap<u64, (B256, Vec<Placed<'_>>)> = BTreeMap::new();
    for log in raw {
        let (Some(block), Some(hash), Some(tx_index), Some(tx_hash), Some(log_index)) = (
            log.block_number,
            log.block_hash,
            log.transaction_index,
            log.transaction_hash,
            log.log_index,
        ) else {
            // Mined logs always carry these; dropping one could hide an entry.
            return Err(L1Error::Provider(format!(
                "EEZ log without its position: {log:?}"
            )));
        };
        let (first_hash, placed) = blocks.entry(block).or_insert((hash, Vec::new()));
        if *first_hash != hash {
            // #184: an answer from two forks of one block is retried, never merged.
            let detail = format!("logs carry block hashes {first_hash} and {hash}");
            return Err(L1Error::SourceIncomplete { block, detail });
        }
        placed.push(Placed {
            log_index,
            tx_index,
            tx_hash,
            log,
        });
    }
    for (block, (_, placed)) in &mut blocks {
        placed.sort_unstable_by_key(|placed| placed.log_index);
        for pair in placed.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            let same_tx = a.tx_index == b.tx_index;
            let duplicate = a.log_index == b.log_index;
            if duplicate || b.tx_index < a.tx_index || (same_tx && a.tx_hash != b.tx_hash) {
                let detail = format!(
                    "block {block}: log {} (tx {} {}) and log {} (tx {} {}) are out of order",
                    a.log_index, a.tx_index, a.tx_hash, b.log_index, b.tx_index, b.tx_hash
                );
                return Err(L1Error::Provider(detail));
            }
        }
    }
    let mut decoded = Vec::with_capacity(blocks.len());
    for (number, (hash, placed)) in blocks {
        let mut logs = Vec::with_capacity(placed.len());
        for Placed {
            log_index,
            tx_index,
            tx_hash,
            log,
        } in placed
        {
            let event = decode(log, &format!("block {number} tx {tx_hash} log {log_index}"))?;
            logs.push(ScannedLog {
                tx_index,
                tx_hash,
                log_index,
                event,
            });
        }
        decoded.push(Block { number, hash, logs });
    }
    Ok(decoded)
}

/// Decodes one scanned EEZ log into its `Event`.
fn decode(log: &Log, here: &str) -> L1Result<Event> {
    fn event<E: SolEvent>(log: &Log, here: &str) -> L1Result<E> {
        let decoded = E::decode_log_data_validate(log.data());
        decoded.map_err(|e| L1Error::Decode(format!("{here}: undecodable log: {e}")))
    }
    let index = |v: U256| {
        u64::try_from(v).map_err(|_| L1Error::Decode(format!("{here}: index {v} overflows")))
    };
    Ok(match log.topic0() {
        Some(&BatchPosted::SIGNATURE_HASH) => {
            let e: BatchPosted = event(log, here)?;
            Event::Posted(e.sharedPublicInput, e.rollupIds)
        }
        Some(&ExecutionConsumed::SIGNATURE_HASH) => {
            let e: ExecutionConsumed = event(log, here)?;
            Event::Consumed(Consumed {
                hash: e.crossChainCallHash,
                queue: e.rollupId,
                index: index(e.entryQueueIndex)?,
            })
        }
        Some(&L2ExecutionPerformed::SIGNATURE_HASH) => {
            let e: L2ExecutionPerformed = event(log, here)?;
            Event::Root(e.rollupId, e.newRoot)
        }
        Some(&EntryExecuted::SIGNATURE_HASH) => {
            let e: EntryExecuted = event(log, here)?;
            Event::Executed(index(e.entryIndex)?, e.rollingHash)
        }
        Some(&L2TxSkipped::SIGNATURE_HASH) => {
            let e: L2TxSkipped = event(log, here)?;
            Event::Skipped(index(e.entryIndex)?)
        }
        topic => {
            let detail = format!("{here}: get_logs answered topic {topic:?}, outside the filter");
            return Err(L1Error::Provider(detail));
        }
    })
}

/// One applied entry, read from its logs. `consumed` is `None` for a
/// leading-run entry, which runs without being consumed (EEZ.sol:414-436).
#[derive(Debug)]
struct Record {
    consumed: Option<Consumed>,
    roots: Vec<(u64, B256)>,
    rolling_hash: B256,
}

#[derive(Debug)]
enum Kind {
    Posted(B256, Vec<u64>),
    Skipped(u64),
    Record(Record),
}

/// An item and the positions of its first and last log among its tx's logs.
#[derive(Debug)]
struct Item {
    first: usize,
    last: usize,
    kind: Kind,
}

/// One tx's scanned logs and the items they parse into.
#[derive(Debug)]
struct Tx<'a> {
    logs: &'a [ScannedLog],
    items: Vec<Item>,
}

/// Parses one tx's logs as `Posted | Skipped | Consumed? Root+ Executed`,
/// exact because posts and entries never nest (EEZ.sol:380, :871-881, :896).
fn parse_tx(block: u64, logs: &[ScannedLog]) -> L1Result<Tx<'_>> {
    let mut items = Vec::new();
    // The entry being read: its first log, its `ExecutionConsumed` if any, its roots so far.
    let mut open: Option<(usize, Option<Consumed>, Vec<(u64, B256)>)> = None;
    for (position, log) in logs.iter().enumerate() {
        let here = at(block, log);
        let (first, kind) = match (&log.event, open.take()) {
            // A root opens an unconsumed (leading-run) entry, or extends the open one.
            (&Event::Root(rollup, root), opened) => {
                let (first, consumed, mut roots) = opened.unwrap_or((position, None, Vec::new()));
                // Updates ascend strictly by rollup, above L1's id 0 (EEZ.sol:578-584).
                let after = roots.last().map_or(0, |&(id, _)| id);
                ensure!(rollup > after, "{here}: root for {rollup} after {after}");
                roots.push((rollup, root));
                open = Some((first, consumed, roots));
                continue;
            }
            // A consumption opens an entry; its roots and `EntryExecuted` follow (EEZ.sol:949-951).
            (&Event::Consumed(consumed), None) => {
                open = Some((position, Some(consumed), Vec::new()));
                continue;
            }
            // `EntryExecuted` closes the open entry into one record.
            (&Event::Executed(index, rolling_hash), Some((first, consumed, roots))) => {
                // Every entry has an update (EEZ.sol:569).
                ensure!(!roots.is_empty(), "{here}: entry applied no rollup update");
                // It repeats the consumed slot, or 0 when unconsumed (EEZ.sol:495-496, :951).
                let slot = consumed.map_or(0, |consumed| consumed.index);
                ensure!(
                    index == slot,
                    "{here}: EntryExecuted({index}) for slot {slot}"
                );
                let record = Record {
                    consumed,
                    roots,
                    rolling_hash,
                };
                (first, Kind::Record(record))
            }
            (Event::Posted(spi, ids), None) => (position, Kind::Posted(*spi, ids.clone())),
            (&Event::Skipped(index), None) => (position, Kind::Skipped(index)),
            (event, None) => {
                return Err(L1Error::Decode(format!(
                    "{here}: {event:?} with no entry open"
                )));
            }
            // Anything else inside an open entry: entries never nest (EEZ.sol:871-881).
            (event, Some(_)) => {
                return Err(L1Error::Decode(format!(
                    "{here}: {event:?} inside an open entry"
                )));
            }
        };
        items.push(Item {
            first,
            last: position,
            kind,
        });
    }
    if let Some((first, ..)) = open {
        let here = at(block, &logs[first]);
        return Err(L1Error::Decode(format!("{here}: entry never closes")));
    }
    Ok(Tx { logs, items })
}

/// The call of a tx sent straight to EEZ with the selector: the whole tx, sent by its signer.
pub(super) fn direct_call(tx: &Transaction, eez: Address, logs: usize) -> Option<Call> {
    let input = tx.input();
    let direct = tx.to() == Some(eez) && input.starts_with(&postAndVerifyBatchCall::SELECTOR);
    direct.then(|| Call {
        input: input.clone(),
        from: tx.inner.signer(),
        logs,
        blob_hashes: tx.blob_versioned_hashes().unwrap_or_default().to_vec(),
    })
}

/// A traced tx's committed post calls in execution order, and its committed
/// scanned-log count. Only a `CALL` posts (a DELEGATECALL's logs carry the
/// caller's address), and nothing under a failed frame commits.
pub(super) fn calls_in_tx(
    root: &CallFrame,
    eez: Address,
    here: &str,
) -> L1Result<(Vec<Call>, usize)> {
    type Out = (Vec<Call>, usize);
    /// False when a committed post nests in another, which EEZ.sol:380 reverts.
    fn walk(frame: &CallFrame, eez: Address, inside: Option<usize>, out: &mut Out) -> bool {
        // A failed frame and everything under it rolled back.
        if frame.error.is_some() || frame.revert_reason.is_some() {
            return true;
        }
        let selector = &postAndVerifyBatchCall::SELECTOR;
        let post = frame.is_call() && frame.to == Some(eez) && frame.input.starts_with(selector);
        if post && inside.is_some() {
            return false;
        }
        let inside = if post { Some(out.0.len()) } else { inside };
        if post {
            out.0.push(Call {
                input: frame.input.clone(),
                from: frame.from,
                logs: 0,
                blob_hashes: Vec::new(),
            });
        }
        let scanned = frame.logs.iter().filter(|log| {
            let topic0 = log.topics.as_ref().and_then(|topics| topics.first());
            log.address == Some(eez) && topic0.is_some_and(|topic| SCANNED_TOPICS.contains(topic))
        });
        let scanned = scanned.count();
        out.1 += scanned;
        if let Some(k) = inside {
            out.0[k].logs += scanned;
        }
        frame.calls.iter().all(|call| walk(call, eez, inside, out))
    }
    let mut out = (Vec::new(), 0);
    let flat = walk(root, eez, None, &mut out);
    ensure!(flat, "{here}: trace nests a committed post");
    Ok(out)
}

/// Where an item ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// Inside the call of `posts[i]`, which lists us.
    Ours(usize),
    /// Inside the call of a post that does not list us.
    Foreign,
    /// Between the calls of a tx that posts for us.
    Between,
    /// In a tx that posts nothing for us, whose calls are not read.
    Untraced,
}

/// Every post in `block` that lists `us`, in log order. `calls` holds the
/// committed calls and scanned-log count of each tx whose `BatchPosted` lists
/// `us`; `candidate` is set when the block may hold a candidate-V1 post.
pub(super) fn read_block(
    block: &Block,
    calls: &BTreeMap<u64, (Vec<Call>, usize)>,
    us: u64,
    candidate: Option<&CandidateBlock<'_>>,
) -> L1Result<Vec<Post>> {
    let mut txs = Vec::new();
    for logs in block.logs.chunk_by(|a, b| a.tx_index == b.tx_index) {
        txs.push(parse_tx(block.number, logs)?);
    }
    let mut posts = Vec::new();
    let mut places = Vec::with_capacity(txs.len());
    for tx in &txs {
        let calls = calls.get(&tx.logs[0].tx_index);
        places.push(label(block, tx, calls, us, candidate, &mut posts)?);
    }
    // A queue belongs to the latest post in the block that listed it (EEZ.sol:804-811, :1578-1582).
    let mut owners = BTreeMap::new();
    for (tx, places) in txs.iter().zip(&places) {
        for (item, &place) in tx.items.iter().zip(places) {
            let here = at(block.number, &tx.logs[item.last]);
            let ours = |record: &Record| record.roots.iter().any(|&(rollup, _)| rollup == us);
            match (&item.kind, place) {
                // Our post ends: it reported its whole leading run, and owns the queues it listed.
                (Kind::Posted(_, ids), Place::Ours(i)) => {
                    // By its `BatchPosted` a post reported its whole leading run (EEZ.sol:414-436).
                    let reported = posts[i].next_leading == posts[i].run;
                    ensure!(reported, "{here}: leading run unreported");
                    owners.extend(ids.iter().map(|&queue| (queue, Some(i))));
                }
                (Kind::Posted(_, ids), _) => owners.extend(ids.iter().map(|&queue| (queue, None))),
                (&Kind::Skipped(index), Place::Ours(i)) => {
                    let next = posts[i].leading(&here)?;
                    ensure!(index == next as u64, "{here}: skip of {index}, not {next}");
                }
                // Only a post's own leading run skips or runs unconsumed (EEZ.sol:414-436).
                (Kind::Skipped(_), place) => {
                    ensure!(place != Place::Between, "{here}: skip outside a call");
                }
                // An entry run inside our post's call: its leading run or meta hook.
                (Kind::Record(record), Place::Ours(i)) => posts[i].in_call(record, &here)?,
                // An entry updates only its batch's rollups (EEZ.sol:581).
                (Kind::Record(record), Place::Foreign) => {
                    ensure!(!ours(record), "{here}: our entry in a foreign post");
                }
                (Kind::Record(record), place) => match record.consumed {
                    None => {
                        let fine = place == Place::Untraced && !ours(record);
                        ensure!(fine, "{here}: leading-run entry outside a call");
                    }
                    // A consumption that updates us: the queue's owner applied it.
                    Some(consumed) if ours(record) => {
                        let Some(&Some(i)) = owners.get(&consumed.queue) else {
                            let queue = consumed.queue;
                            let why = format!("{here}: no post listing {us} owns queue {queue}");
                            return Err(L1Error::Decode(why));
                        };
                        posts[i].deferred(record, consumed, &here)?;
                    }
                    // Another rollup's consumption.
                    Some(_) => {}
                },
            }
        }
    }
    Ok(posts.into_iter().map(|post| post.finish(us)).collect())
}

/// Labels each item of `tx` with the call it ran in, and starts reading each
/// of its posts that lists `us`. The k-th call made the k-th `BatchPosted`,
/// and a call's logs are contiguous and end with it (EEZ.sol:380, :470-475).
fn label(
    block: &Block,
    tx: &Tx<'_>,
    calls: Option<&(Vec<Call>, usize)>,
    us: u64,
    candidate: Option<&CandidateBlock<'_>>,
    posts: &mut Vec<Reading>,
) -> L1Result<Vec<Place>> {
    let Some((calls, committed)) = calls else {
        return Ok(vec![Place::Untraced; tx.items.len()]);
    };
    let head = &tx.logs[0];
    let scanned = tx.logs.len();
    let site = |item: &Item| at(block.number, &tx.logs[item.last]);
    // The tx's `BatchPosted` items: each ends one post.
    let ends: Vec<_> = (tx.items.iter())
        .filter_map(|item| match &item.kind {
            Kind::Posted(spi, ids) => Some((item, *spi, ids)),
            _ => None,
        })
        .collect();
    let made = calls.len();
    let posted = ends.len();
    let counts =
        format!("{made} calls, {committed} logs traced; {posted} BatchPosted, {scanned} logs");
    let tx_at = at(block.number, head);
    // The trace and get_logs agree on the tx's logs, and every committed call posted.
    ensure!(*committed == scanned && made <= posted, "{tx_at}: {counts}");
    if made < posted {
        // A BatchPosted without its CALL: posted by DELEGATECALL or upgradeToAndCall.
        return Err(L1Error::Unsupported {
            block: block.number,
            log_index: tx.logs[ends[made].0.last].log_index,
            detail: format!("tx {}: {counts}", head.tx_hash),
        });
    }
    // Call k owns the `call.logs` logs ending at the k-th `BatchPosted`; calls don't overlap.
    let mut labels = vec![None; scanned];
    let mut floor = 0;
    for (k, (call, (end, ..))) in calls.iter().zip(&ends).enumerate() {
        let start = (end.last + 1).checked_sub(call.logs).unwrap_or(usize::MAX);
        let fits = floor <= start && start <= end.last;
        ensure!(fits, "{}: call {k} claims {} logs", site(end), call.logs);
        labels[start..=end.last].fill(Some(k));
        floor = end.last + 1;
    }
    // An entry runs inside one call or between calls, never across a boundary.
    for item in &tx.items {
        let span = &labels[item.first..=item.last];
        let one_call = span.iter().all(|&label| label == span[0]);
        ensure!(one_call, "{}: entry straddles a call boundary", site(item));
    }
    // Start reading each post that lists us; another rollup's post is foreign.
    let mut by_call = Vec::with_capacity(made);
    for (call, &(end, spi, ids)) in calls.iter().zip(&ends) {
        if !ids.contains(&us) {
            by_call.push(Place::Foreign);
            continue;
        }
        let here = site(end);
        let id = PostId {
            l1_block: block.number,
            l1_block_hash: block.hash,
            log_index: tx.logs[end.last].log_index,
        };
        let (batch, blob_hashes) = verify(call, spi, ids, &id, &here, candidate)?;
        let post = Post {
            id,
            tx_hash: head.tx_hash,
            sender: call.from,
            batch,
            blob_hashes,
            settlement: Settlement::NONE,
        };
        by_call.push(Place::Ours(posts.len()));
        posts.push(Reading::new(post, us, &here)?);
    }
    // An item outside every call ran between them.
    let place = |item: &Item| labels[item.first].map_or(Place::Between, |k| by_call[k]);
    Ok(tx.items.iter().map(place).collect())
}

/// The batch `call` carried, checked against its own `BatchPosted`, and the
/// versioned hashes it selected (none for the calldata profile).
fn verify(
    call: &Call,
    spi: B256,
    ids: &[u64],
    id: &PostId,
    here: &str,
    candidate: Option<&CandidateBlock<'_>>,
) -> L1Result<(EvmBatch, Vec<B256>)> {
    let decoded = postAndVerifyBatchCall::abi_decode_validate(&call.input);
    let decoded = decoded.map_err(|e| L1Error::Decode(format!("{here}: bad call: {e}")))?;
    let batch = decoded.batch;
    // EEZ emits the batch's own rollup list (EEZ.sol:471-475).
    let listed = batch.rollupIdsWithProofSystems.iter().map(|r| r.rollupId);
    let same = listed.eq(ids.iter().copied());
    ensure!(same, "{here}: call lists other rollups");
    let unsupported = |detail| L1Error::Unsupported {
        block: id.l1_block,
        log_index: id.log_index,
        detail,
    };
    let (commitment, blob_hashes) = if batch.blockNumber == 0 && batch.blobIndices.is_empty() {
        let commitment = batch_shared_public_input(&batch, call.from);
        (
            commitment.map_err(|e| unsupported(format!("{here}: {e}")))?,
            Vec::new(),
        )
    } else {
        // Any other blockNumber's customData may read any state (IRollup.sol:41-54);
        // only the configured candidate-V1 domain is reproducible.
        let Some(candidate) = candidate.filter(|c| ids == [c.deployment.eez_rollup_id]) else {
            let detail = format!("{here}: an L1-bound post outside the configured candidate V1");
            return Err(unsupported(detail));
        };
        candidate_commitment(&batch, call, candidate, here)?
    };
    let mismatch = format!("{here}: call hashes to {commitment}, not {spi}");
    // Only a co-listed rollup's block-zero customData can explain a mismatch (EEZ.sol:753-762).
    ensure!(commitment == spi || ids.len() > 1, "{mismatch}");
    if commitment != spi {
        return Err(unsupported(mismatch));
    }
    Ok((batch, blob_hashes))
}

/// The commitment of a post admitted by the settlement wrapper:
/// latest-context sentinel, no calldata tail, and every blob of the tx selected
/// once, in order. A committed Rollup0 post cannot break these, so any break is `Decode`.
fn candidate_commitment(
    batch: &EvmBatch,
    call: &Call,
    candidate: &CandidateBlock<'_>,
    here: &str,
) -> L1Result<(B256, Vec<B256>)> {
    let selected = (0..call.blob_hashes.len()).map(U256::from);
    let canonical = !call.blob_hashes.is_empty()
        && batch.blobIndices.iter().copied().eq(selected)
        && batch.callData.is_empty();
    ensure!(
        batch.blockNumber == u64::MAX && canonical,
        "{here}: not a canonical candidate-V1 carrier"
    );
    let custom_data = candidate
        .deployment
        .custom_data(candidate.timestamp, candidate.parent_hash);
    let context = SettlementContext {
        blob_hashes: &call.blob_hashes,
        custom_data: std::slice::from_ref(&custom_data),
    };
    let commitment = batch_shared_public_input_with(batch, call.from, context)
        .map_err(|e| L1Error::Decode(format!("{here}: {e}")))?;
    Ok((commitment, call.blob_hashes.clone()))
}

/// A post listing us, while its block is read.
#[derive(Debug)]
struct Reading {
    post: Post,
    roles: Vec<Option<EntryRole>>,
    /// `immediateEntryCount`, and the leading run: the entries before the first
    /// with a non-zero `proxyEntryHash` (EEZ.sol:414-431).
    immediate: usize,
    run: usize,
    next_leading: usize,
    last_transient: Option<usize>,
    last_queued: BTreeMap<u64, usize>,
    applied: Vec<AppliedEntry>,
}

impl Reading {
    /// Starts reading `post`: our entries' roles and its leading-run bounds.
    fn new(post: Post, us: u64, here: &str) -> L1Result<Self> {
        let entries = &post.batch.entries;
        let immediate = usize::try_from(post.batch.immediateEntryCount).unwrap_or(usize::MAX);
        // A committed post has immediateEntryCount <= entries.length (EEZ.sol:637).
        ensure!(immediate <= entries.len(), "{here}: too many immediates");
        let unconsumed = |entry: &&ExecutionEntrySol| entry.proxyEntryHash == B256::ZERO;
        Ok(Self {
            run: entries[..immediate].iter().take_while(unconsumed).count(),
            roles: entry_roles(entries, us),
            immediate,
            next_leading: 0,
            last_transient: None,
            last_queued: BTreeMap::new(),
            applied: Vec::new(),
            post,
        })
    }

    /// The leading run reports each entry in order, before the meta hook (EEZ.sol:414-451).
    fn leading(&mut self, here: &str) -> L1Result<usize> {
        let fine = self.last_transient.is_none() && self.next_leading < self.run;
        ensure!(fine, "{here}: result past the leading run");
        self.next_leading += 1;
        Ok(self.next_leading - 1)
    }

    /// A leading-run entry, or one the meta hook consumed from this post's
    /// table `entries[run..immediate]`, moving forward (EEZ.sol:443-451, :933-938).
    fn in_call(&mut self, record: &Record, here: &str) -> L1Result<()> {
        let entry_index = match record.consumed {
            None => self.leading(here)?,
            Some(consumed) => {
                let slot = usize::try_from(consumed.index).unwrap_or(usize::MAX);
                let index = self.run.saturating_add(slot);
                let forward = self.last_transient.is_none_or(|last| index > last);
                let fine = index < self.immediate && forward;
                ensure!(fine, "{here}: bad meta-hook slot {slot}");
                self.last_transient = Some(index);
                index
            }
        };
        self.apply(entry_index, record, here)
    }

    /// Slot `k` of a queue is the k-th entry of `entries[immediate..]` routed to
    /// it, and the queue cursor only moves forward (EEZ.sol:816-821, :939-947).
    fn deferred(&mut self, record: &Record, consumed: Consumed, here: &str) -> L1Result<()> {
        let entries = &self.post.batch.entries;
        let queue = consumed.queue;
        let routed = |&i: &usize| entries[i].destinationRollupId == queue;
        let slot = usize::try_from(consumed.index).unwrap_or(usize::MAX);
        let index = (self.immediate..entries.len()).filter(routed).nth(slot);
        let last = self.last_queued.get(&queue);
        let Some(entry_index) = index.filter(|index| last.is_none_or(|last| index > last)) else {
            return Err(L1Error::Decode(format!(
                "{here}: queue {queue} has no next slot {slot}"
            )));
        };
        self.last_queued.insert(queue, entry_index);
        self.apply(entry_index, record, here)
    }

    /// The logs restate the entry: its call and queue (EEZ.sol:1304-1320), its
    /// rolling hash (:1026, :1034) and each update's `(rollupId, newRoot)` (:1327-1345).
    fn apply(&mut self, entry_index: usize, record: &Record, here: &str) -> L1Result<()> {
        let entry = &self.post.batch.entries[entry_index];
        let call = (entry.proxyEntryHash, entry.destinationRollupId);
        let consumed = record.consumed.is_none_or(|c| (c.hash, c.queue) == call);
        let updates = entry.rollupUpdates.iter().map(|u| (u.rollupId, u.newRoot));
        let roots = updates.eq(record.roots.iter().copied());
        let restated = consumed && roots && record.rolling_hash == entry.rollingHash;
        ensure!(restated, "{here}: logs do not restate entry {entry_index}");
        if let Some(role) = self.roles[entry_index] {
            self.applied.push(AppliedEntry { entry_index, role });
        }
        Ok(())
    }

    /// The post with its settlement: the entries applied, in order, and their end states.
    fn finish(mut self, us: u64) -> Post {
        let entries = &self.post.batch.entries;
        let roots = |applied: &AppliedEntry| {
            let updates = &entries[applied.entry_index].rollupUpdates;
            let update = updates.iter().find(|update| update.rollupId == us)?;
            Some((update.currentRoot, update.newRoot))
        };
        // L1 checked where the run started against its stored root (EEZ.sol:1011-1016).
        let entry_state = self.applied.first().and_then(roots).map(|(from, _)| from);
        let final_state = self.applied.last().and_then(roots).map(|(_, to)| to);
        let first_ours = self.roles.iter().position(Option::is_some);
        let first = self.applied.first().map(|applied| applied.entry_index);
        let resumed = first.is_some() && first != first_ours;
        self.post.settlement = Settlement::new(self.applied, final_state, entry_state, resumed);
        self.post
    }
}
