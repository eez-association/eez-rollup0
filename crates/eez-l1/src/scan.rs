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
    /// How many of this batch's claimed roots L1 settled (0 = skip). Computed
    /// by the scanner's settlement-attribution pass.
    pub settled_count: usize,
    /// Deepest claimed root L1 settled — this batch's actual post-batch endpoint.
    pub settled_final_state: Option<B256>,
    pub claimed_current_state: Option<B256>,
    pub claimed_new_state: Option<B256>,
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
        .filter_map(|l| l.transaction_hash)
        .collect();
    // `L2ExecutionPerformed.newState` roots L1 settled, per L1 block (per-block
    // not per-tx: deferred entries emit from the bundled user_tx). Each batch is
    // credited only its own subset later — see [`attribute_settlement`].
    let mut settled_by_block: HashMap<u64, HashSet<B256>> = HashMap::new();
    for l in &winner_logs {
        if let Some(bn) = l.block_number {
            let data = l.data().data.as_ref();
            if data.len() == 32 {
                settled_by_block
                    .entry(bn)
                    .or_default()
                    .insert(B256::from_slice(data));
            }
        }
    }

    let mut out: Vec<ScannedBatch> = Vec::with_capacity(logs.len());
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
        let (settled_count, settled_final_state) =
            attribute_settlement(&claimed_chain, settled_by_block.get(&l1_block_number));
        out.push(ScannedBatch {
            l1_block_number,
            l1_block_hash,
            tx_hash,
            submitter,
            call_data: decoded.batch.callData,
            post_batch_input: input.clone(),
            state_applied: winner_tx_hashes.contains(&tx_hash),
            settled_count,
            settled_final_state,
            claimed_current_state,
            claimed_new_state,
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

/// How much of this batch L1 actually settled: matches the batch's claimed
/// `newState` roots against the roots settled in its L1 block, returning the
/// match count and the deepest match (the batch's true post-batch root), or
/// `(0, None)` if none match — in which case the deriver skips the batch.
///
/// Matched per batch, not by taking the block's last settled root, because two
/// postBatches for the same rollup can land in one L1 block: an empty `A→A`
/// batch must not be judged against a rich `A→B` batch's `B` in that block.
fn attribute_settlement(
    claimed_chain: &[B256],
    block_settled: Option<&HashSet<B256>>,
) -> (usize, Option<B256>) {
    let Some(settled) = block_settled else {
        return (0, None);
    };
    let count = claimed_chain
        .iter()
        .filter(|&&root| settled.contains(&root))
        .count();
    let final_state = claimed_chain
        .iter()
        .rev()
        .find(|&&root| settled.contains(&root))
        .copied();
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
        BatchLogChunks, LOG_SCAN_CHUNK_BLOCKS, attribute_settlement, fetch_log_transaction,
        initial_log_scan_ranges, scan_next_batch_log_chunk,
    };
    use crate::error::L1Error;
    use alloy_primitives::{Address, B256, Bytes, U256};
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use std::collections::HashSet;

    fn settled(roots: &[B256]) -> HashSet<B256> {
        roots.iter().copied().collect()
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
        let block = settled(&[a, b]);
        assert_eq!(attribute_settlement(&[a], Some(&block)), (1, Some(a)));
        assert_eq!(attribute_settlement(&[b], Some(&block)), (1, Some(b)));
    }

    /// A loser whose claimed root never settled → `(0, None)` ⇒ deriver skips it.
    #[test]
    fn unsettled_loser_is_skipped() {
        let b = B256::repeat_byte(0xBB);
        let y = B256::repeat_byte(0xCC);
        assert_eq!(attribute_settlement(&[y], Some(&settled(&[b]))), (0, None));
    }

    /// Partial consumption: only a prefix settled → endpoint is the deepest
    /// settled root, not the claimed end.
    #[test]
    fn partial_consumption_uses_deepest_settled_root() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        let d = B256::repeat_byte(0x0D);
        assert_eq!(
            attribute_settlement(&[b, c, d], Some(&settled(&[b, c]))),
            (2, Some(c)),
        );
    }

    /// Full consumption: the claimed end settled → it's the endpoint.
    #[test]
    fn full_consumption_uses_claimed_end() {
        let b = B256::repeat_byte(0x0B);
        let c = B256::repeat_byte(0x0C);
        assert_eq!(
            attribute_settlement(&[b, c], Some(&settled(&[b, c]))),
            (2, Some(c)),
        );
    }

    /// No settlement for our rollup in the block at all → unsettled.
    #[test]
    fn no_block_settlements_is_unsettled() {
        assert_eq!(
            attribute_settlement(&[B256::repeat_byte(1)], None),
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
