use std::collections::{HashMap, HashSet};

use alloy_consensus::Transaction;
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent};
use eez_protocol::abi::{
    BatchPosted, L2ExecutionPerformed, ProofSystemBatchPerVerificationEntriesSol,
    postAndVerifyBatchCall,
};
use tracing::{Level, event};

use crate::error::{L1Error, L1Result};

/// Initial block span for historical log scans. Wide catch-up gaps are
/// split before hitting RPCs that reject long `eth_getLogs` ranges.
pub(crate) const LOG_SCAN_CHUNK_BLOCKS: u64 = 100_000;

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
    /// How many of this batch's claimed roots L1 settled (0 = skip). See
    /// [`attribute_settlement`].
    pub settled_count: usize,
    /// Deepest claimed root L1 settled — this batch's actual post-batch endpoint.
    pub settled_final_state: Option<B256>,
    pub claimed_current_state: Option<B256>,
    pub claimed_new_state: Option<B256>,
}

#[derive(Debug, Clone, Copy)]
struct OrderedSettlement {
    transaction_index: u64,
    log_index: u64,
    new_state: B256,
}

#[derive(Debug)]
struct PendingBatch {
    scanned: ScannedBatch,
    transaction_index: u64,
    log_index: u64,
    touches_rollup: bool,
    claimed_chain: Vec<B256>,
}

/// Finds transactions containing multiple batches for the observed rollup.
/// Their event streams cannot be separated by transaction index alone.
fn ambiguous_batch_transactions(
    batches: impl IntoIterator<Item = (u64, u64, bool)>,
) -> HashSet<(u64, u64)> {
    let mut seen = HashSet::new();
    let mut ambiguous = HashSet::new();
    for (block_number, transaction_index, touches_rollup) in batches {
        if touches_rollup && !seen.insert((block_number, transaction_index)) {
            ambiguous.insert((block_number, transaction_index));
        }
    }
    ambiguous
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

pub(crate) async fn scan_next_batch_log_chunk(
    provider: &impl Provider,
    eez: Address,
    rollup_id: u64,
    chunks: &mut BatchLogChunks,
) -> L1Result<Option<Vec<ScannedBatch>>> {
    let Some(&(from, to)) = chunks.ranges.last() else {
        return Ok(None);
    };

    let scanned = scan_batch_logs_range(provider, eez, rollup_id, from, to).await?;
    chunks.ranges.pop();
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
    let winner_tx_hashes: HashSet<B256> = winner_logs
        .iter()
        .filter_map(|log| log.transaction_hash)
        .collect();
    // The contract emits one event per applied StateUpdate, in update order.
    // Keep both that order and duplicate roots: the deriver consumes an applied
    // count as a prefix of the batch's claimed chain.
    let mut settled_by_block: HashMap<u64, Vec<OrderedSettlement>> = HashMap::new();
    for log in &winner_logs {
        let block_number = log.block_number.ok_or_else(|| {
            L1Error::Provider("L2ExecutionPerformed log missing block_number".into())
        })?;
        let transaction_index = log.transaction_index.ok_or_else(|| {
            L1Error::Provider("L2ExecutionPerformed log missing transaction_index".into())
        })?;
        let log_index = log.log_index.ok_or_else(|| {
            L1Error::Provider("L2ExecutionPerformed log missing log_index".into())
        })?;
        let decoded = L2ExecutionPerformed::decode_log(&log.inner).map_err(|error| {
            L1Error::Provider(format!("decode L2ExecutionPerformed log: {error}"))
        })?;
        settled_by_block
            .entry(block_number)
            .or_default()
            .push(OrderedSettlement {
                transaction_index,
                log_index,
                new_state: decoded.newState,
            });
    }
    for settlements in settled_by_block.values_mut() {
        settlements.sort_unstable_by_key(|settlement| {
            (settlement.transaction_index, settlement.log_index)
        });
    }

    let mut pending: Vec<PendingBatch> = Vec::with_capacity(logs.len());
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
        // Fetch the postBatch tx by (block, index), NOT by hash.
        // Helps use pruned nodes.
        let tx_index = log
            .transaction_index
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing transaction_index".into()))?;
        let log_index = log
            .log_index
            .ok_or_else(|| L1Error::Provider("BatchPosted log missing log_index".into()))?;
        let tx = fetch_log_transaction(provider, l1_block_number, tx_index, tx_hash).await?;
        let submitter = tx.inner.signer();
        let input = tx.inner.input();
        let decoded = postAndVerifyBatchCall::abi_decode(input)
            .map_err(|e| L1Error::Provider(format!("decode postBatch({tx_hash}): {e}")))?;
        let _decoded_event = BatchPosted::decode_log(&alloy_primitives::Log {
            address: log.address(),
            data: log.data().clone(),
        })
        .map_err(|e| L1Error::Provider(format!("decode BatchPosted({tx_hash}): {e}")))?;
        let (claimed_current_state, claimed_chain) = our_state_chain(&decoded.batch, rollup_id);
        let claimed_new_state = claimed_chain.last().copied();
        let touches_rollup = decoded
            .batch
            .rollupIdsWithProofSystems
            .iter()
            .any(|rollup| rollup.rollupId == rollup_id);
        pending.push(PendingBatch {
            scanned: ScannedBatch {
                l1_block_number,
                l1_block_hash,
                tx_hash,
                submitter,
                call_data: decoded.batch.callData,
                post_batch_input: input.clone(),
                state_applied: winner_tx_hashes.contains(&tx_hash),
                settled_count: 0,
                settled_final_state: None,
                claimed_current_state,
                claimed_new_state,
            },
            transaction_index: tx_index,
            log_index,
            touches_rollup,
            claimed_chain,
        });
    }

    pending.sort_by_key(|batch| {
        (
            batch.scanned.l1_block_number,
            batch.transaction_index,
            batch.log_index,
        )
    });
    let ambiguous_transactions = ambiguous_batch_transactions(pending.iter().map(|batch| {
        (
            batch.scanned.l1_block_number,
            batch.transaction_index,
            batch.touches_rollup,
        )
    }));
    for index in 0..pending.len() {
        let (current_and_previous, following) = pending.split_at_mut(index + 1);
        let current = &mut current_and_previous[index];
        // Sequential post calls can interleave each call's immediate events
        // before its BatchPosted marker. Without a call-start boundary, leave
        // every same-transaction batch unattributed rather than guess.
        if !current.touches_rollup
            || ambiguous_transactions
                .contains(&(current.scanned.l1_block_number, current.transaction_index))
        {
            continue;
        }

        let block_number = current.scanned.l1_block_number;
        // A later batch touching this rollup replaces its queues. Events in
        // that later post transaction (including its immediate updates) belong
        // to the later batch, so its transaction index is the exclusive bound.
        let next_batch_tx = following
            .iter()
            .find(|batch| batch.scanned.l1_block_number == block_number && batch.touches_rollup)
            .map(|batch| batch.transaction_index);
        let settlements = settled_by_block
            .get(&block_number)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let settlements = settlement_window(settlements, current.transaction_index, next_batch_tx);
        (
            current.scanned.settled_count,
            current.scanned.settled_final_state,
        ) = attribute_settlement(
            &current.claimed_chain,
            settlements.iter().map(|settlement| &settlement.new_state),
        );
    }

    Ok(pending.into_iter().map(|batch| batch.scanned).collect())
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

/// Returns the ordered settlement events belonging to one batch. A subsequent
/// batch for the same rollup supersedes its queues at the start of its post
/// transaction, making that transaction the exclusive upper bound.
fn settlement_window(
    block_settlements: &[OrderedSettlement],
    batch_transaction_index: u64,
    next_batch_transaction_index: Option<u64>,
) -> &[OrderedSettlement] {
    let start = block_settlements
        .partition_point(|settlement| settlement.transaction_index < batch_transaction_index);
    let end = next_batch_transaction_index.map_or(block_settlements.len(), |transaction_index| {
        block_settlements
            .partition_point(|settlement| settlement.transaction_index < transaction_index)
    });
    &block_settlements[start..end]
}

/// How much of this batch L1 actually settled. Only an exact leading prefix of
/// the claimed `newState` chain is safe for the deriver to replay: every event
/// in the batch's window must match, with no surplus observations. Comparing
/// the ordered streams also preserves repeated roots as distinct state updates.
fn attribute_settlement<'a>(
    claimed_chain: &[B256],
    settled_roots: impl ExactSizeIterator<Item = &'a B256>,
) -> (usize, Option<B256>) {
    let count = settled_roots.len();
    if count > claimed_chain.len() || !claimed_chain[..count].iter().eq(settled_roots) {
        return (0, None);
    }
    let final_state = count.checked_sub(1).map(|index| claimed_chain[index]);
    (count, final_state)
}

async fn fetch_log_transaction(
    provider: &impl Provider,
    l1_block_number: u64,
    tx_index: u64,
    tx_hash: B256,
) -> L1Result<alloy_rpc_types_eth::Transaction> {
    if let Some(tx) = provider
        .get_transaction_by_block_number_and_index(
            BlockNumberOrTag::Number(l1_block_number),
            tx_index as usize,
        )
        .await
        .map_err(|e| {
            L1Error::Provider(format!(
                "get_tx({l1_block_number}#{tx_index} for {tx_hash}): {e}"
            ))
        })?
    {
        return Ok(tx);
    }

    event!(
        name: "eez.l1.scan_batch_logs.tx_by_index_missing",
        Level::WARN,
        l1_block_number,
        tx_index,
        tx_hash = %tx_hash,
        "postBatch tx missing by block/index; retrying same provider by hash",
    );

    provider
        .get_transaction_by_hash(tx_hash)
        .await
        .map_err(|e| L1Error::Provider(format!("get_tx({tx_hash}): {e}")))?
        .ok_or_else(|| L1Error::SourceIncomplete {
            block: l1_block_number,
            tx_hash,
            detail: format!(
                "block/index lookup returned null at tx index {tx_index}; tx-hash lookup also returned null"
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::{
        BatchLogChunks, LOG_SCAN_CHUNK_BLOCKS, OrderedSettlement, ambiguous_batch_transactions,
        attribute_settlement, fetch_log_transaction, initial_log_scan_ranges,
        scan_next_batch_log_chunk, settlement_window,
    };
    use crate::error::L1Error;
    use alloy_primitives::{Address, B256, Bytes, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;

    fn settlement(transaction_index: u64, log_index: u64, new_state: B256) -> OrderedSettlement {
        OrderedSettlement {
            transaction_index,
            log_index,
            new_state,
        }
    }

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

    /// The bug this fix closes: idle `A→A` and rich `A→B` share an L1 block;
    /// each gets its OWN root, not the block's last (`B`).
    #[test]
    fn same_block_batches_attributed_per_chain_not_block_last() {
        let a = B256::repeat_byte(0xAA);
        let b = B256::repeat_byte(0xBB);
        let block = [settlement(3, 1, a), settlement(7, 2, b)];
        let first = settlement_window(&block, 3, Some(7));
        let second = settlement_window(&block, 7, None);

        assert_eq!(
            attribute_settlement(&[a], first.iter().map(|event| &event.new_state)),
            (1, Some(a))
        );
        assert_eq!(
            attribute_settlement(&[b], second.iter().map(|event| &event.new_state)),
            (1, Some(b))
        );
    }

    #[test]
    fn same_transaction_rollup_batches_are_ambiguous() {
        let ambiguous = ambiguous_batch_transactions([
            (10, 3, true),
            (10, 3, true),
            (10, 7, true),
            (11, 3, true),
            // An unrelated batch in the same transaction is not a second
            // attribution candidate for this rollup.
            (12, 4, false),
            (12, 4, true),
        ]);

        assert_eq!(ambiguous.len(), 1);
        assert!(ambiguous.contains(&(10, 3)));
    }

    /// A loser is not credited with an equal root emitted by the next batch.
    #[test]
    fn later_batch_settlement_does_not_credit_loser() {
        let b = B256::repeat_byte(0xBB);
        let block = [settlement(7, 2, b)];
        let loser = settlement_window(&block, 3, Some(7));
        let winner = settlement_window(&block, 7, None);

        assert_eq!(
            attribute_settlement(&[b], loser.iter().map(|event| &event.new_state)),
            (0, None)
        );
        assert_eq!(
            attribute_settlement(&[b], winner.iter().map(|event| &event.new_state)),
            (1, Some(b))
        );
    }

    /// Partial consumption: only a prefix settled → endpoint is the deepest
    /// settled root, not the claimed end.
    #[test]
    fn partial_consumption_uses_deepest_settled_root() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let d = B256::repeat_byte(0x0D);
        let settled = [b, c];
        assert_eq!(
            attribute_settlement(&[b, c, d], settled.iter()),
            (2, Some(c)),
        );
    }

    /// Full consumption: the claimed end settled → it's the endpoint.
    #[test]
    fn full_consumption_uses_claimed_end() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let settled = [b, c];
        assert_eq!(attribute_settlement(&[b, c], settled.iter()), (2, Some(c)),);
    }

    /// Event order is consensus order; membership alone must not turn a
    /// reordered chain into a settled prefix.
    #[test]
    fn out_of_order_roots_do_not_form_a_prefix() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let settled = [c, b];

        assert_eq!(attribute_settlement(&[b, c], settled.iter()), (0, None));
    }

    /// A later mismatch invalidates the complete observation; reporting the
    /// matching portion would make the deriver replay a prefix L1 did not emit.
    #[test]
    fn later_mismatch_rejects_the_observed_slice() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let unexpected = B256::repeat_byte(0xEE);
        let settled = [b, unexpected];

        assert_eq!(attribute_settlement(&[b, c], settled.iter()), (0, None));
    }

    /// Every event in the batch's settlement window must be explained by its
    /// claimed chain; zip truncation must not hide surplus observations.
    #[test]
    fn surplus_settlement_event_rejects_the_observed_slice() {
        let b = B256::repeat_byte(0x0B);
        let unexpected = B256::repeat_byte(0xEE);
        let settled = [b, unexpected];

        assert_eq!(attribute_settlement(&[b], settled.iter()), (0, None));
    }

    /// Repeated roots represent repeated updates and therefore require one
    /// event each; a set would incorrectly credit both from a single event.
    #[test]
    fn repeated_root_requires_repeated_settlement_event() {
        let b = B256::repeat_byte(0x0B);
        let one_event = [b];
        let two_events = [b, b];

        assert_eq!(
            attribute_settlement(&[b, b], one_event.iter()),
            (1, Some(b))
        );
        assert_eq!(
            attribute_settlement(&[b, b], two_events.iter()),
            (2, Some(b))
        );
    }

    /// No settlement for our rollup in the block at all → unsettled.
    #[test]
    fn no_block_settlements_is_unsettled() {
        assert_eq!(
            attribute_settlement(&[B256::repeat_byte(1)], std::iter::empty::<&B256>()),
            (0, None)
        );
    }

    /// A minimal, serializable RPC transaction for mocked provider
    /// responses. The signature is a fixed test vector — none of the
    /// scan paths validate it.
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

    /// The boot-crash fix's linchpin: a tx the L1 serves by (block,
    /// index) is returned directly; one missing by index falls back to
    /// the by-hash lookup; missing by BOTH lookups classifies as
    /// `SourceIncomplete` (retryable) rather than a fatal provider error.
    ///
    /// The mock is a method-agnostic FIFO, so this pins response
    /// consumption counts and the fallback/classification behavior —
    /// not which RPC method each lookup used.
    #[tokio::test]
    async fn tx_lookup_falls_back_by_hash_then_classifies_source_incomplete() {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let tx_hash = B256::with_last_byte(0x51);

        // (a) by-(block, index) hit: returned directly, no fallback call.
        asserter.push_success(&mock_rpc_transaction());
        fetch_log_transaction(&provider, 14, 0, tx_hash)
            .await
            .expect("index lookup hit");

        // (b) index lookup null → by-hash fallback hit.
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_success(&mock_rpc_transaction());
        fetch_log_transaction(&provider, 14, 0, tx_hash)
            .await
            .expect("hash fallback hit");

        // (c) both lookups null → retryable SourceIncomplete carrying
        // the block and tx hash context.
        asserter.push_success(&serde_json::Value::Null);
        asserter.push_success(&serde_json::Value::Null);
        let err = fetch_log_transaction(&provider, 14, 7, tx_hash)
            .await
            .expect_err("both lookups null must not yield a tx");
        assert!(err.is_source_incomplete(), "unexpected error: {err}");
        match err {
            L1Error::SourceIncomplete {
                block, tx_hash: h, ..
            } => {
                assert_eq!(block, 14);
                assert_eq!(h, tx_hash);
            }
            other => panic!("expected SourceIncomplete, got {other}"),
        }
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
}
