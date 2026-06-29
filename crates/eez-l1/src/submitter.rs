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

use alloy_consensus::{SignableTransaction, Transaction, TxEip1559};
use alloy_eips::eip2718::Encodable2718;
use alloy_eips::{BlockNumberOrTag, Decodable2718};
use alloy_network::TxSignerSync;
use alloy_primitives::{Bytes, TxHash, TxKind, U256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent};
use eez_evm::types::{
    BatchPosted, ExecutionConsumed, L2ExecutionPerformed,
    ProofSystemBatchPerVerificationEntriesSol, postAndVerifyBatchCall,
};
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Wall-clock cap on the target-block + inclusion check.
const TARGET_WAIT_BUDGET: Duration = Duration::from_secs(30);
const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// L1 block offset for [`BundleTarget::NextBlock`] — the MINIMAL viable
/// target, `latest + 1` (the next block to be built). A bundle must target a
/// not-yet-built block; targeting the already-mined `latest` (offset 0) can
/// never be included, so 1 is the floor.
///
/// There is NO slack cushion. The old `+2` traded a constant extra L1 block
/// (~5s) of settlement latency to absorb a STALE local `latest`, but a synced
/// embedded L1 reports the true tip (measured: the bundle landed EXACTLY at
/// target, Δblocks=+0 every cycle), and the submitter's retry path already
/// backstops the rare stale case — so the cushion was pure added latency.
/// Removing it lets the settlement round-trip fit inside one 5s slot so the
/// composer's one-in-flight gate can re-open every slot instead of every
/// other (≈5s cadence instead of ≈10s).
const NEXT_BLOCK_OFFSET: u64 = 1;

/// Which L1 block the bundle should land in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BundleTarget {
    /// Resolved to `latest + NEXT_BLOCK_OFFSET` (`latest + 1`) at send time.
    NextBlock,
    /// Caller-picked exact L1 block.
    Exact(u64),
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
        let target_block = match target {
            BundleTarget::Exact(n) => n,
            BundleTarget::NextBlock => {
                let target_provider = self.inner.build_target_provider();
                target_provider
                    .get_block_number()
                    .await
                    .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?
                    + NEXT_BLOCK_OFFSET
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
            .dispatch_and_observe(raw_txs, post_batch_hash, target_block, expected_final_state)
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

    /// `true` iff `tx_hash` has an L1 receipt and its inclusion block
    /// advanced this rollup to `expected_final_state`.
    ///
    /// Used by optimistic recovery to distinguish a stale false-failure
    /// verdict from a real competition loss: in based competition,
    /// another composer may advance the L2 cursor past our Sync height
    /// with a different state root, so cursor height alone is not
    /// evidence that our local optimistic block settled.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC/log lookup failure.
    pub async fn receipt_reached_state(
        &self,
        tx_hash: TxHash,
        expected_final_state: alloy_primitives::B256,
    ) -> L1Result<bool> {
        let provider = self.inner.build_target_provider();
        let Some(receipt) = provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_transaction_receipt: {e}")))?
        else {
            return Ok(false);
        };
        let l1_block = receipt
            .block_number
            .ok_or_else(|| L1Error::Provider("receipt present but block_number missing".into()))?;
        self.inner
            .settlement_in_block(&provider, l1_block, Some(expected_final_state))
            .await
    }

    /// Walks every past `BatchPosted` event from `deploy_block` to L1
    /// head and returns one [`HistoricalBatch`] per event, with enough
    /// metadata (L1 block, tx hash, submitter, raw `callData`) for the
    /// Deriver's catch-up + reorg-walkback paths to replay them.
    ///
    /// Called once at deriver startup (catch-up) and on L1 reorg
    /// (walkback); not on the per-tick hot path.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (log fetch, tx fetch).
    /// - [`L1Error::Codec`] only if `block_count` is later called on a
    ///   malformed batch.
    pub async fn scan_batches(&self, deploy_block: u64) -> L1Result<Vec<HistoricalBatch>> {
        let provider = self.inner.build_provider();
        let scanned = scan_batch_logs(
            &provider,
            self.inner.config.eez,
            self.inner.config.rollup_id,
            deploy_block,
            BlockNumberOrTag::Latest,
        )
        .await?;
        Ok(scanned
            .into_iter()
            .map(|b| HistoricalBatch {
                l1_block_number: b.l1_block_number,
                l1_block_hash: b.l1_block_hash,
                tx_hash: b.tx_hash,
                submitter: b.submitter,
                call_data: b.call_data,
                state_applied: b.state_applied,
                settled_count: b.settled_count,
                consumed_count: b.consumed_count,
                settled_final_state: b.settled_final_state,
                claimed_current_state: b.claimed_current_state,
                claimed_new_state: b.claimed_new_state,
            })
            .collect())
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
}

/// One past `BatchPosted` event, with enough context for the Deriver
/// to replay (or skip) it during catch-up.
#[derive(Debug, Clone)]
pub struct HistoricalBatch {
    pub l1_block_number: u64,
    /// Hash of the L1 block the batch landed in — canonicality probe
    /// for the resync anchor walk.
    pub l1_block_hash: alloy_primitives::B256,
    pub tx_hash: alloy_primitives::B256,
    pub submitter: alloy_primitives::Address,
    pub call_data: alloy_primitives::Bytes,
    /// Winner flag: same L1 tx emitted `L2ExecutionPerformed`.
    /// See [`L1Event::BatchPosted::state_applied`].
    pub state_applied: bool,
    /// Entries that actually applied. See
    /// [`L1Event::BatchPosted::settled_count`].
    pub settled_count: usize,
    /// DEFERRED (inbound) entries consumed = `ExecutionConsumed` count for
    /// our rollupId. See [`L1Event::BatchPosted::consumed_count`].
    pub consumed_count: usize,
    /// L1's actual stored root after this batch — the reconciliation
    /// endpoint. See [`L1Event::BatchPosted::settled_final_state`].
    pub settled_final_state: Option<alloy_primitives::B256>,
    /// FIRST stateDelta's `currentState` for our rollup — L1's
    /// pre-batch stored root (Deriver compares to L2's actual at
    /// `from_block - 1`).
    pub claimed_current_state: Option<alloy_primitives::B256>,
    /// LAST stateDelta's `newState` — the composer's claimed full-chain
    /// endpoint. Diagnostics only; reconcile against `settled_final_state`.
    pub claimed_new_state: Option<alloy_primitives::B256>,
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
        expected_final_state: Option<alloy_primitives::B256>,
    ) -> L1Result<SendOutcome> {
        let hexes: Vec<String> = raw_txs
            .iter()
            .map(|t| hex::encode_prefixed(t.as_ref()))
            .collect();
        let hex_refs: Vec<&str> = hexes.iter().map(String::as_str).collect();
        let post_batch_envelope =
            alloy_consensus::TxEnvelope::decode_2718(&mut raw_txs[0].as_ref()).map_err(|e| {
                L1Error::Submission(format!("send_bundle: decode postBatch envelope: {e}"))
            })?;
        // rbuilder-chiado DROPS 1-tx `eth_sendBundle` bundles but accepts
        // >=2-tx bundles. A lone minimal postBatch therefore gets a harmless
        // poster self-transfer appended, preserving target-block bundle
        // semantics. Do NOT use bare eth_sendRawTransaction here: the relay can
        // return a hash without propagating the tx publicly, and a raw mempool tx
        // is not target-pinned, so `head > target` is not a proof of death.
        if let [only] = hex_refs.as_slice() {
            let filler = self.sign_single_postbatch_filler(&post_batch_envelope)?;
            let filler_hex = hex::encode_prefixed(filler.as_ref());
            let padded = [*only, filler_hex.as_str()];
            event!(
                name: "eez.submitter.single_postbatch.padded",
                Level::INFO,
                target_block,
                post_batch_hash = %post_batch_hash,
                filler_nonce = post_batch_envelope.nonce().saturating_add(1),
                "single postBatch padded with poster self-transfer for target-pinned bundle submission",
            );
            return match post_bundle(
                &self.http,
                self.config.builder_rpc_url.as_str(),
                &padded,
                target_block,
            )
            .await
            {
                Ok(()) => {
                    self.observe(post_batch_hash, target_block, expected_final_state)
                        .await
                }
                Err(L1Error::BundleRpcUnsupported) => {
                    event!(
                        name: "eez.submitter.single_postbatch.mempool_fallback",
                        Level::INFO,
                        target_block,
                        post_batch_hash = %post_batch_hash,
                        "relay has no eth_sendBundle; falling back to raw postBatch submission",
                    );
                    post_raw_transaction(&self.http, self.config.builder_rpc_url.as_str(), only)
                        .await?;
                    self.observe(post_batch_hash, target_block, expected_final_state)
                        .await
                }
                Err(e) => Err(e),
            };
        }
        match post_bundle(
            &self.http,
            self.config.builder_rpc_url.as_str(),
            &hex_refs,
            target_block,
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

    fn sign_single_postbatch_filler(
        &self,
        post_batch: &alloy_consensus::TxEnvelope,
    ) -> L1Result<Bytes> {
        let chain_id = post_batch.chain_id().ok_or_else(|| {
            L1Error::Submission("postBatch tx has no chain_id; cannot sign filler".into())
        })?;
        let nonce = post_batch.nonce().checked_add(1).ok_or_else(|| {
            L1Error::Submission("postBatch nonce overflow; cannot sign filler".into())
        })?;
        let max_fee_per_gas = post_batch.max_fee_per_gas();
        let max_priority_fee_per_gas = post_batch
            .max_priority_fee_per_gas()
            .unwrap_or(max_fee_per_gas);
        let poster = self.config.poster.address();
        let mut tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 21_000,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(poster),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let sig = self
            .config
            .poster
            .sign_transaction_sync(&mut tx)
            .map_err(|e| L1Error::Submission(format!("sign postBatch filler tx: {e}")))?;
        let signed = tx.into_signed(sig);
        let mut buf = Vec::with_capacity(128);
        signed.encode_2718(&mut buf);
        Ok(Bytes::from(buf))
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
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_sendBundle",
        "params": [{
            "txs": raw_tx_hexes,
            "blockNumber": format!("0x{target_block:x}"),
        }],
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

/// POST a single raw tx via `eth_sendRawTransaction` to the builder relay.
///
/// rbuilder-chiado DROPS 1-tx `eth_sendBundle` bundles but ingests bare
/// raw txs and >=2-tx bundles (verified live 2026-06-23: a lone postBatch
/// in a 1-tx bundle is dropped even at 2x priority fee, while a 2-tx
/// bundle and a bare `eth_sendRawTransaction` both land in the next
/// block). So a lone postBatch — the bundle whenever the held-tx pool is
/// empty — must be submitted as a raw tx, not a 1-tx bundle.
///
/// Idempotent across re-posts: the composer re-submits the same nonce
/// every slot until it lands, so an "already known" / "nonce too low" /
/// "replacement" reply is benign (the tx is already in the relay's pool);
/// only a genuine rejection surfaces as an error. Inclusion is decided by
/// the caller's `observe` poll, not this reply.
///
/// # Errors
///
/// - [`L1Error::Provider`] on transport (DNS, TCP, JSON decode) failure.
/// - [`L1Error::Submission`] on a genuine relay-side rejection.
pub async fn post_raw_transaction(
    http: &reqwest::Client,
    builder_rpc_url: &str,
    raw_tx_hex: &str,
) -> L1Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction",
        "params": [raw_tx_hex],
    });
    let resp: serde_json::Value = http
        .post(builder_rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| L1Error::Provider(format!("eth_sendRawTransaction POST: {e}")))?
        .error_for_status()
        .map_err(|e| L1Error::Submission(format!("eth_sendRawTransaction HTTP: {e}")))?
        .json()
        .await
        .map_err(|e| L1Error::Provider(format!("eth_sendRawTransaction decode: {e}")))?;
    event!(
        name: "eez.submitter.rawtx.sent",
        Level::INFO,
        response = %resp,
        "eth_sendRawTransaction response received",
    );
    if let Some(err) = resp.get("error") {
        let msg = err.to_string().to_ascii_lowercase();
        // Benign re-post echoes: the same nonce is already in the pool.
        if msg.contains("known")
            || msg.contains("nonce too low")
            || msg.contains("already")
            || msg.contains("replacement")
        {
            return Ok(());
        }
        return Err(L1Error::Submission(format!(
            "eth_sendRawTransaction: {err}"
        )));
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
/// roots from our rollup's `StateDelta`. Produced by [`scan_batch_logs`];
/// the Deriver's catch-up scan and the live
/// [`L1Watcher`](crate::L1Watcher) poll each project it into their own
/// type.
pub(crate) struct ScannedBatch {
    pub l1_block_number: u64,
    pub l1_block_hash: alloy_primitives::B256,
    pub tx_hash: alloy_primitives::B256,
    pub submitter: alloy_primitives::Address,
    pub rollup_count: alloy_primitives::U256,
    pub call_data: alloy_primitives::Bytes,
    pub state_applied: bool,
    /// How many of this batch's claimed roots L1 settled (0 = skip). See
    /// [`attribute_settlement`].
    pub settled_count: usize,
    /// `ExecutionConsumed` events for our rollupId in the postBatch's L1
    /// block = the number of DEFERRED (inbound) entries actually consumed.
    /// The AUTHORITATIVE consumed-deferred count (mirrors based-rollup's
    /// per-entry ExecutionConsumed signal); used by the deriver instead of
    /// `settled_count - (1 + outbound)`, which deflates if an outbound
    /// immediate is skipped. See `scan_batch_logs`.
    pub consumed_count: usize,
    /// Deepest claimed root L1 settled — this batch's actual post-batch
    /// endpoint and the reconciliation endpoint. See [`attribute_settlement`].
    pub settled_final_state: Option<alloy_primitives::B256>,
    pub claimed_current_state: Option<alloy_primitives::B256>,
    pub claimed_new_state: Option<alloy_primitives::B256>,
}

/// Tally items by their (optional) L1 block number, dropping those without
/// one. Counts `ExecutionConsumed` logs (already rollup-filtered at the RPC
/// layer via `topic2`) per block — one per consumed inbound deferred entry,
/// the authoritative `consumed_count`. Pure + iterator-shaped so the
/// partial-consumption / skipped-immediate invariant is unit-testable
/// without a live provider (see the tests below).
fn tally_by_block(
    blocks: impl IntoIterator<Item = Option<u64>>,
) -> std::collections::HashMap<u64, usize> {
    let mut by_block: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
    for bn in blocks.into_iter().flatten() {
        *by_block.entry(bn).or_default() += 1;
    }
    by_block
}

/// Fetch every `BatchPosted` log in `[from_block, to_block]` and cross-
/// reference each against `L2ExecutionPerformed` for our rollup — present
/// ⇔ this batch's state delta applied (winner; losers emit `BatchPosted`
/// only). For each, decode the originating tx for the submitter, callData
/// and our rollup's claimed state roots. Shared by
/// [`Submitter::scan_batches`] (catch-up) and
/// [`L1Watcher::scan_batch_posted`](crate::L1Watcher) (live poll).
pub(crate) async fn scan_batch_logs(
    provider: &impl Provider,
    eez: alloy_primitives::Address,
    rollup_id: u64,
    from_block: u64,
    to_block: BlockNumberOrTag,
) -> L1Result<Vec<ScannedBatch>> {
    let filter = Filter::new()
        .address(eez)
        .event_signature(BatchPosted::SIGNATURE_HASH)
        .from_block(from_block)
        .to_block(to_block);
    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(BatchPosted): {e}")))?;

    let winners_filter = Filter::new()
        .address(eez)
        .event_signature(L2ExecutionPerformed::SIGNATURE_HASH)
        .topic1(alloy_primitives::U256::from(rollup_id))
        .from_block(from_block)
        .to_block(to_block);
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

    // `ExecutionConsumed` events for our rollupId, per L1 block — the
    // AUTHORITATIVE count of DEFERRED (inbound) entries consumed by their
    // bundled user txs (EEZ.sol:903). Unlike `settled_count` (which folds
    // the leading immediate + every outbound immediate + consumed deferred
    // into one number, and so deflates if an outbound immediate is
    // skipped), this is the direct per-entry consumption signal the deriver
    // needs to truncate the inbound-delivery FIFO. rollupId is the SECOND
    // indexed param of `ExecutionConsumed` ⇒ `topic2`.
    let consumed_filter = Filter::new()
        .address(eez)
        .event_signature(ExecutionConsumed::SIGNATURE_HASH)
        .topic2(alloy_primitives::U256::from(rollup_id))
        .from_block(from_block)
        .to_block(to_block);
    let consumed_logs = provider
        .get_logs(&consumed_filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(ExecutionConsumed): {e}")))?;
    let consumed_by_block = tally_by_block(consumed_logs.iter().map(|l| l.block_number));

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
        let tx = provider
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
            .ok_or_else(|| {
                L1Error::Provider(format!(
                    "tx {tx_hash} (block {l1_block_number} idx {tx_index}) not found"
                ))
            })?;
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
            consumed_count: consumed_by_block
                .get(&l1_block_number)
                .copied()
                .unwrap_or(0),
            settled_final_state,
            claimed_current_state,
            claimed_new_state,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{attribute_settlement, tally_by_block};
    use alloy_primitives::B256;
    use std::collections::HashSet;

    fn settled(roots: &[B256]) -> HashSet<B256> {
        roots.iter().copied().collect()
    }

    #[test]
    fn tally_by_block_counts_per_block_and_drops_none() {
        // Three ExecutionConsumed logs in block 10, one in 11, one with no
        // block number (pending/dropped) — the last is ignored.
        let blocks = [Some(10u64), Some(10), Some(10), Some(11), None];
        let t = tally_by_block(blocks);
        assert_eq!(t.get(&10).copied(), Some(3));
        assert_eq!(t.get(&11).copied(), Some(1));
        assert_eq!(
            t.get(&12).copied(),
            None,
            "absent block ⇒ no entry (deriver reads 0)"
        );
        assert_eq!(t.values().sum::<usize>(), 4, "the None is not counted");
    }

    /// The crux of Change-4: under a SKIPPED outbound immediate, the
    /// authoritative `ExecutionConsumed` count (`consumed_count`) stays
    /// correct, whereas the legacy `settled_count - (1 + outbound)` formula
    /// deflates by the skip and would truncate a real inbound delivery.
    ///
    /// Scenario — one batch in L1 block 7: 1 anchor + 2 outbound immediates
    /// (ONE skipped) + 2 inbound deferred (BOTH consumed).
    ///   * `L2ExecutionPerformed` fires for: anchor + the 1 settled outbound
    ///     + the 2 consumed inbound = 4. The SKIPPED outbound emits NONE
    ///     (it took the `ImmediateEntrySkipped` catch path), so
    ///     `settled_count == 4` — deflated by the skip.
    ///   * `ExecutionConsumed` fires once per consumed inbound deferred = 2.
    ///     Neither the anchor nor any immediate emits it.
    #[test]
    fn consumed_count_survives_skipped_immediate_where_settled_subtraction_fails() {
        let block = 7u64;
        // The real counting helper over the rollup-filtered ExecutionConsumed
        // logs (two, both in block 7).
        let consumed_count = *tally_by_block([Some(block), Some(block)])
            .get(&block)
            .expect("block 7 present");
        assert_eq!(
            consumed_count, 2,
            "ExecutionConsumed count = consumed inbound deferred"
        );

        // Model the skip-deflated settled_count and the static outbound count.
        let settled_count_with_skip = 4usize; // anchor + 1 settled outbound + 2 inbound
        let outbound_entries = 2usize; // 2 emitted, but only 1 settled

        // Legacy formula (deriver.rs, pre-Change-4): deflates by the skip.
        let legacy = settled_count_with_skip.saturating_sub(1 + outbound_entries);
        assert_eq!(
            legacy, 1,
            "legacy `settled - (1 + outbound)` under-counts by the skip"
        );

        // Change-4: the deriver now truncates the inbound FIFO to
        // `consumed_count` directly — correct, independent of outbound/skip.
        assert_eq!(
            consumed_count, 2,
            "consumed_count is the true inbound-consumed count"
        );
        assert_ne!(
            legacy, consumed_count,
            "Change-4 fixes exactly this divergence: legacy would drop one real inbound delivery",
        );
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
