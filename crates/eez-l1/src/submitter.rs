//! Thin L1-interaction primitive: sends `postAndVerifyBatch` via
//! `eth_sendBundle` and reads past `BatchPosted` events. Stateless —
//! the [`Composer`](crate::Composer) owns cursors, batch construction,
//! and prover orchestration.
//!
//! `eth_sendBundle` pins inclusion to one L1 block. If the bundle
//! isn't in that block we report [`SendOutcome::Dropped`] and the
//! Composer rebuilds on the next tick with a fresh target + nonce.
//! Real Flashbots-style relays don't consume the nonce on miss; the
//! anvil-side `scripts/builder-stub.py` does (it forwards via
//! `eth_sendRawTransaction`), but the cursor-race guard +
//! `pending` nonce read keep both paths correct.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::Transaction;
use alloy_eips::{BlockNumberOrTag, Decodable2718};
use alloy_primitives::{TxHash, U256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent};
use eez_evm::types::{
    BatchPosted, L2ExecutionPerformed, ProofSystemBatchPerVerificationEntriesSol,
    postAndVerifyBatchCall,
};
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Wall-clock cap on the target-block + inclusion check.
const TARGET_WAIT_BUDGET: Duration = Duration::from_secs(30);
const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Initial block span for historical log scans. Wide catch-up gaps are
/// split before hitting RPCs that reject long `eth_getLogs` ranges.
const LOG_SCAN_CHUNK_BLOCKS: u64 = 100_000;

/// L1 block offset for [`BundleTarget::NextBlock`]. slack=2 (over the
/// minimal latest+1) gives a one-block cushion for when our local
/// `latest` is stale by the time the relay sees the bundle.
const NEXT_BLOCK_SLACK: u64 = 2;

/// Which L1 block the bundle should land in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleTarget {
    /// Resolved to `latest + NEXT_BLOCK_SLACK` at send time. Unpinned —
    /// used for off-cadence catch-up.
    NextBlock,
    /// Land in exactly L1 block `block`, and only if its timestamp equals
    /// `timestamp`. The timestamp pin makes the settlement slot match the
    /// L2 Sync block's anchored slot by construction: a gnosis block's
    /// timestamp IS its slot time, so a skipped slot (block lands a slot
    /// late) won't match — the bundle drops instead of settling with a
    /// drifted L2 timestamp.
    Exact { block: u64, timestamp: u64 },
}

/// One bundle attempt's outcome. `Dropped` is the expected miss path —
/// caller rebuilds on the next tick.
#[derive(Debug, Clone)]
pub enum SendOutcome {
    Included {
        tx_hash: TxHash,
        l1_block: u64,
        /// Same L1 tx emitted `L2ExecutionPerformed` for our rollup,
        /// i.e. the contract advanced its state root.
        state_applied: bool,
    },
    Dropped {
        tx_hash: TxHash,
        target_block: u64,
    },
}

/// Thin L1-interaction primitive. Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct Submitter {
    inner: Arc<Inner>,
}

/// Stateful `BatchPosted` log chunks. The submitter owns chunk boundaries;
/// callers consume chunks and decide when to commit their own progress.
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

struct Inner {
    config: SubmitterConfig,
    http: reqwest::Client,
}

impl std::fmt::Debug for Submitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submitter")
            .field("config", &self.inner.config)
            .finish()
    }
}

impl Submitter {
    /// Build a Submitter from its config.
    #[must_use]
    pub fn new(config: SubmitterConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                http: reqwest::Client::new(),
            }),
        }
    }

    /// L1 EOA address this Submitter sends from. Used by the Composer
    /// to identify "our own" `L1Event::BatchPosted` events vs.
    /// external ones in a multi-composer based-rollup deployment.
    #[must_use]
    pub fn poster_address(&self) -> alloy_primitives::Address {
        self.inner.config.poster.address()
    }

    /// Configured L1 RPC URL. Exposed so callers (e.g. the deriver) can
    /// build their own provider against the same node to fetch a tx by
    /// hash and re-decode it under a different `sol!` view.
    #[must_use]
    pub fn rpc_url(&self) -> reqwest::Url {
        self.inner.config.rpc_url.clone()
    }

    /// Atomically bundle already-signed, 2718-encoded `raw_txs`
    /// (`[postBatch, user_tx_1, …]`, signed upstream) into one
    /// `eth_sendBundle` POST. Flashbots ordering: the bundle lands in
    /// `target_block` as one atomic insert or is dropped — no subset.
    ///
    /// Observation is best-effort: only `raw_txs[0]` (the postBatch) is
    /// checked for inclusion; if it's in, the bundle either succeeded or
    /// a proposer broke atomicity (their bug, not ours).
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (block fetch, log fetch).
    /// - [`L1Error::Submission`] if the relay rejects the bundle.
    pub async fn send_bundle(
        &self,
        raw_txs: &[alloy_primitives::Bytes],
        target: BundleTarget,
        expected_final_state: Option<alloy_primitives::B256>,
    ) -> L1Result<SendOutcome> {
        fail::fail_point!("submitter::send::start", |_| Err(L1Error::Submission(
            "injected failpoint: submitter::send::start".into()
        )));
        if raw_txs.is_empty() {
            return Err(L1Error::Submission(
                "send_bundle called with no txs".to_string(),
            ));
        }
        // Exact targets carry the L2 Sync block's anchored timestamp; pin
        // the bundle to land only in a block at that exact time. NextBlock
        // (catch-up) is off-cadence, so it stays unpinned.
        let pin_timestamp = match target {
            BundleTarget::Exact { timestamp, .. } => Some(timestamp),
            BundleTarget::NextBlock => None,
        };
        let target_block = match target {
            BundleTarget::Exact { block, .. } => block,
            BundleTarget::NextBlock => {
                let target_provider = self.inner.build_target_provider();
                target_provider
                    .get_block_number()
                    .await
                    .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?
                    + NEXT_BLOCK_SLACK
            }
        };
        let post_batch_envelope =
            alloy_consensus::TxEnvelope::decode_2718(&mut raw_txs[0].as_ref()).map_err(|e| {
                L1Error::Submission(format!("send_bundle: decode postBatch envelope: {e}"))
            })?;
        let post_batch_hash = *post_batch_envelope.tx_hash();
        // One bundle, one target block. Atomic bundle semantics:
        // rbuilder either includes the whole bundle in `target_block`
        // in the specified order, or drops it. Relays without a bundle
        // API degrade to ordered mempool submission inside
        // `dispatch_and_observe`.
        self.inner
            .dispatch_and_observe(
                raw_txs,
                post_batch_hash,
                target_block,
                pin_timestamp,
                expected_final_state,
            )
            .await
    }

    /// `true` iff L1 already has a receipt for `tx_hash` (included,
    /// regardless of status). Used by the composer's bundle-failure
    /// path to avoid re-queueing user_txs whose nonce was burned by a
    /// reverted-but-included execution.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn receipt_exists(&self, tx_hash: TxHash) -> L1Result<bool> {
        // Target-tip RPC, not the embedded L1 — the embedded node can
        // lag the canonical tip by 2-3 blocks, and a missed receipt
        // here re-queues a burned-nonce tx that poisons the next
        // bundle's simulation.
        let provider = self.inner.build_target_provider();
        Ok(provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_transaction_receipt: {e}")))?
            .is_some())
    }

    /// Creates bounded `BatchPosted` log chunks from `from_block` to the
    /// current L1 head. Call [`Self::next_batch_log_chunk`] to consume it.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn batch_log_chunks(&self, from_block: u64) -> L1Result<BatchLogChunks> {
        let provider = self.inner.build_provider();
        let latest = provider
            .get_block_number()
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?;
        Ok(BatchLogChunks::new(from_block, latest))
    }

    /// Scans and returns the next `BatchPosted` log chunk, or `None` when
    /// the chunks are exhausted.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure (log fetch, tx fetch).
    pub async fn next_batch_log_chunk(
        &self,
        chunks: &mut BatchLogChunks,
    ) -> L1Result<Option<Vec<ScannedBatch>>> {
        let provider = self.inner.build_provider();
        scan_next_batch_log_chunk(
            &provider,
            self.inner.config.eez,
            self.inner.config.rollup_id,
            chunks,
        )
        .await
    }

    /// Hash of the canonical L1 block at `number`, or `None` if none. Used by
    /// the Deriver's resync to check whether an indexed batch is still canonical.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn canonical_l1_hash(&self, number: u64) -> L1Result<Option<alloy_primitives::B256>> {
        let provider = self.inner.build_provider();
        Ok(provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({number}): {e}")))?
            .map(|b| b.header.hash))
    }

    /// Timestamp of an L1 block on the target chain, or `None` if absent.
    /// Distinguishes a skipped-slot drop from a genuine exclusion.
    pub async fn block_timestamp(&self, number: u64) -> L1Result<Option<u64>> {
        let provider = self.inner.build_target_provider();
        Ok(provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({number}): {e}")))?
            .map(|b| b.header.timestamp))
    }
}

impl Inner {
    fn build_provider(&self) -> impl Provider + use<> {
        // No wallet: writes go through the builder relay, reads
        // don't need signing. Pre-sim sets `from` explicitly.
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(self.config.rpc_url.clone())
    }

    /// Provider used ONLY for target-block discovery on
    /// `BundleTarget::NextBlock`. Falls back to the main RPC when no
    /// override URL is set. See `SubmitterConfig::target_rpc_url`.
    fn build_target_provider(&self) -> impl Provider + use<> {
        let url = self
            .config
            .target_rpc_url
            .clone()
            .unwrap_or_else(|| self.config.rpc_url.clone());
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(url)
    }

    /// Submit `raw_txs` (postBatch first) and observe the outcome. Try
    /// the relay's `eth_sendBundle`; if it's a plain execution RPC
    /// ([`L1Error::BundleRpcUnsupported`]), degrade to ordered mempool
    /// submission — on dev reth / anvil that lands every tx in the next
    /// block, postBatch first by priority-fee, minus the all-or-nothing
    /// guarantee (irrelevant without a competing builder).
    async fn dispatch_and_observe(
        &self,
        raw_txs: &[alloy_primitives::Bytes],
        post_batch_hash: TxHash,
        target_block: u64,
        pin_timestamp: Option<u64>,
        expected_final_state: Option<alloy_primitives::B256>,
    ) -> L1Result<SendOutcome> {
        let hexes: Vec<String> = raw_txs
            .iter()
            .map(|t| hex::encode_prefixed(t.as_ref()))
            .collect();
        let hex_refs: Vec<&str> = hexes.iter().map(String::as_str).collect();
        match post_bundle(
            &self.http,
            self.config.builder_rpc_url.as_str(),
            &hex_refs,
            target_block,
            pin_timestamp,
        )
        .await
        {
            Ok(()) => {
                self.observe(post_batch_hash, target_block, expected_final_state)
                    .await
            }
            Err(L1Error::BundleRpcUnsupported) => {
                event!(
                    name: "eez.submitter.bundle.mempool_fallback",
                    Level::INFO,
                    target_block,
                    tx_count = raw_txs.len(),
                    "relay has no eth_sendBundle; submitting txs via mempool in order",
                );
                let target_provider = self.build_target_provider();
                for (idx, raw) in raw_txs.iter().enumerate() {
                    if let Err(err) = alloy_provider::Provider::send_raw_transaction(
                        &target_provider,
                        raw.as_ref(),
                    )
                    .await
                    {
                        // postBatch rejection is fatal for the slot;
                        // a rejected user_tx (stale nonce after a
                        // re-push) is logged and skipped — the rest of
                        // the submission stands on its own.
                        if idx == 0 {
                            return Err(L1Error::Submission(format!(
                                "mempool fallback: postBatch rejected: {err}"
                            )));
                        }
                        event!(
                            name: "eez.submitter.mempool.user_tx_rejected",
                            Level::WARN,
                            tx_idx = idx,
                            error = %err,
                            "mempool fallback: user_tx rejected; continuing without it",
                        );
                    }
                }
                self.observe(post_batch_hash, target_block, expected_final_state)
                    .await
            }
            Err(other) => Err(other),
        }
    }

    /// Bundle observation, transport-agnostic: poll for the postBatch
    /// receipt (no block pinning — a builder may land the bundle a block
    /// late and the embedded L1 may lag the tip, so pinning to the exact
    /// target produced false `Dropped` verdicts), then derive
    /// `state_applied` from the inclusion block's `L2ExecutionPerformed`
    /// events via [`Self::settlement_in_block`].
    async fn observe(
        &self,
        tx_hash: TxHash,
        target_block: u64,
        expected_final_state: Option<alloy_primitives::B256>,
    ) -> L1Result<SendOutcome> {
        // Failure must mean PROVABLY DEAD, not merely slow. A bundle is
        // pinned to `target_block` — included in order there or never —
        // so death = the chain passed the target (head > target) without
        // our tx. A wall-clock-only budget here gave false FAILED
        // verdicts for bundles that later landed: recovery reorged + re-
        // emitted, the original settled anyway, and the two timelines
        // fought forever (StateRootMismatch, phantom L2 effects).
        // Receipt, head, and settlement logs all read from the SAME
        // tip provider — mixing the lagging embedded L1 for receipts
        // would re-open the false-death window. If the tip later reorgs
        // the bundle in, downstream converges it (Watcher → Deriver →
        // recovery cursor re-check drops the stale verdict).
        let start = tokio::time::Instant::now();
        let target_provider = self.build_target_provider();
        let mut slow_logged = false;
        loop {
            // Transient RPC failures are retried, never escalated to a
            // failure verdict — while polling, the ledger stays Pending
            // and the composer's gate stays closed.
            match target_provider.get_transaction_receipt(tx_hash).await {
                Ok(Some(receipt)) => {
                    let l1_block = receipt.block_number.ok_or_else(|| {
                        L1Error::Provider("receipt present but block_number missing".into())
                    })?;
                    let state_applied = self
                        .settlement_in_block(&target_provider, l1_block, expected_final_state)
                        .await?;
                    return Ok(SendOutcome::Included {
                        tx_hash,
                        l1_block,
                        state_applied,
                    });
                }
                Ok(None) => match target_provider.get_block_number().await {
                    Ok(head) if head > target_block => {
                        return Ok(dropped(
                            tx_hash,
                            target_block,
                            "target block passed without inclusion",
                        ));
                    }
                    Ok(_) => {}
                    Err(err) => event!(
                        name: "eez.submitter.observe.head_read_failed",
                        Level::WARN,
                        tx_hash = %tx_hash,
                        error = %err,
                        "head read failed during bundle observation; retrying",
                    ),
                },
                Err(err) => event!(
                    name: "eez.submitter.observe.receipt_read_failed",
                    Level::WARN,
                    tx_hash = %tx_hash,
                    error = %err,
                    "receipt read failed during bundle observation; retrying",
                ),
            }
            if !slow_logged && start.elapsed() >= TARGET_WAIT_BUDGET {
                slow_logged = true;
                // Sanity breadcrumb only — NOT a verdict. The loop
                // keeps going; death is decided solely by the chain
                // passing the target block.
                event!(
                    name: "eez.submitter.observe.slow",
                    Level::WARN,
                    tx_hash = %tx_hash,
                    target_block,
                    elapsed_secs = start.elapsed().as_secs(),
                    "bundle observation exceeding budget; still polling (verdict requires inclusion or target passage)",
                );
            }
            tokio::time::sleep(TARGET_POLL_INTERVAL).await;
        }
    }

    /// Did L1's stored stateRoot for our rollup reach the claimed state
    /// in `l1_block`?
    ///
    /// - `Some(root)`: true iff some `L2ExecutionPerformed(rollupId,
    ///   newState)` in the block has `newState == root`. The leading
    ///   immediate entry always emits this event, so matching "any
    ///   event" would report settled even with every deferred entry
    ///   unconsumed; requiring the FINAL root means L1 reached exactly
    ///   the state the Sync block claims.
    /// - `None`: any event for our rollupId.
    async fn settlement_in_block<P: Provider>(
        &self,
        provider: &P,
        l1_block: u64,
        expected_final_state: Option<alloy_primitives::B256>,
    ) -> L1Result<bool> {
        let winners = Filter::new()
            .address(self.config.eez)
            .event_signature(L2ExecutionPerformed::SIGNATURE_HASH)
            .topic1(U256::from(self.config.rollup_id))
            .from_block(l1_block)
            .to_block(l1_block);
        let logs = provider
            .get_logs(&winners)
            .await
            .map_err(|e| L1Error::Provider(format!("get_logs(L2ExecutionPerformed): {e}")))?;
        Ok(match expected_final_state {
            Some(root) => logs
                .iter()
                .any(|l| l.data().data.as_ref() == root.as_slice()),
            None => !logs.is_empty(),
        })
    }
}

/// POST a multi-tx bundle to a Flashbots-style `eth_sendBundle` relay.
/// Returns `Ok(())` if the relay returns a successful JSON-RPC reply
/// (any non-error `result` — most relays use `{"result": {"bundleHash":
/// "0x..."}}`, the chiado builder returns `{"result": null}`). Errors
/// surface relay-side rejections as [`L1Error::Submission`].
///
/// `target_block` is encoded as `0x{hex}` in the bundle params. Caller
/// is responsible for picking a not-yet-proposed block — the relay
/// silently drops bundles aimed at already-built blocks.
///
/// # Errors
///
/// - [`L1Error::Provider`] on transport (DNS, TCP, JSON decode) failure.
/// - [`L1Error::Submission`] when the relay HTTP status is non-2xx OR
///   the JSON body carries an `error` field.
pub async fn post_bundle(
    http: &reqwest::Client,
    builder_rpc_url: &str,
    raw_tx_hexes: &[&str],
    target_block: u64,
    pin_timestamp: Option<u64>,
) -> L1Result<()> {
    // STRICT all-or-nothing: `revertingTxHashes`/`droppingTxHashes`
    // empty. Per rbuilder's order commit, txs execute in submitted order
    // and any revert outside those whitelists fails the WHOLE bundle.
    // Whitelisting user_txs in `revertingTxHashes` instead lets the
    // relay silently DROP a reverting one (observed on chiado: block
    // 21566886 landed postBatch + 2 of 3 user_txs), advancing L1 to a
    // mid-chain prefix root and desyncing the composer. Sim is in-order
    // too, so user_txs simulate after the postBatch — no whitelist
    // needed for a "pre-postBatch state" sim.
    let mut bundle_params = serde_json::json!({
        "txs": raw_tx_hexes,
        "blockNumber": format!("0x{target_block:x}"),
    });
    if let Some(ts) = pin_timestamp {
        // Pin inclusion to the exact L1 slot the L2 Sync block anchored
        // to. The builder enforces min/maxTimestamp against block.timestamp
        // (verified on chiado), so a skipped slot won't match and the
        // bundle drops rather than settling with a drifted L2 timestamp.
        bundle_params["minTimestamp"] = serde_json::json!(ts);
        bundle_params["maxTimestamp"] = serde_json::json!(ts);
    }
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_sendBundle",
        "params": [bundle_params],
    });
    let resp: serde_json::Value = http
        .post(builder_rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| L1Error::Provider(format!("eth_sendBundle POST: {e}")))?
        .error_for_status()
        .map_err(|e| L1Error::Submission(format!("eth_sendBundle HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| L1Error::Provider(format!("eth_sendBundle decode: {e}")))?;
    event!(
        name: "eez.submitter.bundle.sent",
        Level::INFO,
        target_block,
        tx_count = raw_tx_hexes.len(),
        response = %resp,
        "eth_sendBundle response received",
    );
    if let Some(err) = resp.get("error") {
        // JSON-RPC -32601 = method not found: the configured relay is a
        // plain execution RPC (dev reth, anvil) with no bundle API.
        // Typed so `send_bundle` can degrade to mempool submission.
        if err.get("code").and_then(serde_json::Value::as_i64) == Some(-32601) {
            return Err(L1Error::BundleRpcUnsupported);
        }
        return Err(L1Error::Submission(format!("eth_sendBundle: {err}")));
    }
    Ok(())
}

fn dropped(tx_hash: TxHash, target_block: u64, reason: &'static str) -> SendOutcome {
    event!(
        name: "eez.submitter.bundle.dropped",
        Level::WARN,
        target_block,
        tx_hash = %tx_hash,
        reason,
        "bundle dropped; composer will retry on next tick",
    );
    SendOutcome::Dropped {
        tx_hash,
        target_block,
    }
}

/// Our rollup's stateDelta chain in a batch: the first delta's `currentState`
/// (pre-batch root) and the ordered per-delta `newState` roots.
pub(crate) fn our_state_chain(
    batch: &ProofSystemBatchPerVerificationEntriesSol,
    rollup_id: u64,
) -> (Option<alloy_primitives::B256>, Vec<alloy_primitives::B256>) {
    let rid = U256::from(rollup_id);
    let mut first_curr: Option<alloy_primitives::B256> = None;
    let mut new_states: Vec<alloy_primitives::B256> = Vec::new();
    for entry in &batch.entries {
        for delta in &entry.stateDeltas {
            if delta.rollupId == rid {
                if first_curr.is_none() {
                    first_curr = Some(delta.currentState);
                }
                new_states.push(delta.newState);
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
    claimed_chain: &[alloy_primitives::B256],
    block_settled: Option<&std::collections::HashSet<alloy_primitives::B256>>,
) -> (usize, Option<alloy_primitives::B256>) {
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

/// One decoded `BatchPosted` log: winner flag plus the claimed state
/// roots from our rollup's `StateDelta`. The Deriver's catch-up scan and
/// the live [`L1Watcher`](crate::L1Watcher) poll consume the same shape.
#[derive(Debug, Clone)]
pub struct ScannedBatch {
    pub l1_block_number: u64,
    /// Hash of the L1 block the batch landed in — canonicality probe
    /// for the resync anchor walk.
    pub l1_block_hash: alloy_primitives::B256,
    pub tx_hash: alloy_primitives::B256,
    pub submitter: alloy_primitives::Address,
    pub rollup_count: alloy_primitives::U256,
    pub call_data: alloy_primitives::Bytes,
    pub state_applied: bool,
    /// How many of this batch's claimed roots L1 settled (0 = skip). See
    /// [`attribute_settlement`].
    pub settled_count: usize,
    /// Deepest claimed root L1 settled — this batch's actual post-batch endpoint.
    pub settled_final_state: Option<alloy_primitives::B256>,
    pub claimed_current_state: Option<alloy_primitives::B256>,
    pub claimed_new_state: Option<alloy_primitives::B256>,
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
    eez: alloy_primitives::Address,
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
async fn scan_batch_logs_range(
    provider: &impl Provider,
    eez: alloy_primitives::Address,
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
        .topic1(alloy_primitives::U256::from(rollup_id))
        .from_block(from_block)
        .to_block(BlockNumberOrTag::Number(to_block));
    let winner_logs = provider
        .get_logs(&winners_filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(L2ExecutionPerformed): {e}")))?;
    let winner_tx_hashes: std::collections::HashSet<alloy_primitives::B256> = winner_logs
        .iter()
        .filter_map(|l| l.transaction_hash)
        .collect();
    // `L2ExecutionPerformed.newState` roots L1 settled, per L1 block (per-block
    // not per-tx: deferred entries emit from the bundled user_tx). Each batch is
    // credited only its own subset later — see [`attribute_settlement`].
    let mut settled_by_block: std::collections::HashMap<
        u64,
        std::collections::HashSet<alloy_primitives::B256>,
    > = std::collections::HashMap::new();
    for l in &winner_logs {
        if let Some(bn) = l.block_number {
            let data = l.data().data.as_ref();
            if data.len() == 32 {
                settled_by_block
                    .entry(bn)
                    .or_default()
                    .insert(alloy_primitives::B256::from_slice(data));
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
        let decoded_event = BatchPosted::decode_log(&alloy_primitives::Log {
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
            rollup_count: decoded_event.rollupCount,
            call_data: decoded.batch.callData,
            state_applied: winner_tx_hashes.contains(&tx_hash),
            settled_count,
            settled_final_state,
            claimed_current_state,
            claimed_new_state,
        });
    }
    Ok(out)
}

async fn fetch_log_transaction(
    provider: &impl Provider,
    l1_block_number: u64,
    tx_index: u64,
    tx_hash: alloy_primitives::B256,
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
    use super::{LOG_SCAN_CHUNK_BLOCKS, attribute_settlement, initial_log_scan_ranges};
    use alloy_primitives::B256;
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
}
