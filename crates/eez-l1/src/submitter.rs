//! Thin signed L1-submission primitive: sends `postAndVerifyBatch` via
//! `eth_sendBundle`. Read-only canonical-chain scans live in
//! [`L1Reader`].
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

use alloy_eips::{BlockNumberOrTag, Decodable2718};
use alloy_primitives::{TxHash, U256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::SolEvent;
use eez_protocol::abi::L2ExecutionPerformed;
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};
use crate::l1_reader::L1Reader;

/// Wall-clock cap on the target-block + inclusion check.
const TARGET_WAIT_BUDGET: Duration = Duration::from_secs(30);
const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(500);
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

struct Inner {
    config: SubmitterConfig,
    reader: L1Reader,
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
        let reader = L1Reader::new(config.reader.clone());
        Self {
            inner: Arc::new(Inner {
                config,
                reader,
                // Bounded timeout so a hanging/unreachable builder relay cannot
                // stall the one-in-flight gate forever: a timed-out `eth_sendBundle`
                // returns Err, which `observe_bundle_outcome` treats as a drop →
                // the gate reopens and the next slot retries. `Client::new()` has
                // NO timeout, so a relay that accepts the TCP connection but never
                // responds freezes posting permanently (cursor stuck at 0).
                http: reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new()),
            }),
        }
    }

    /// Clone the read-only canonical L1 client contained by this Submitter.
    /// The Deriver uses it without gaining access to submission credentials or
    /// builder routing.
    #[must_use]
    pub fn reader(&self) -> L1Reader {
        self.inner.reader.clone()
    }

    /// L1 EOA address this Submitter sends from. Used by the Composer
    /// to identify "our own" `L1Event::BatchPosted` events vs.
    /// external ones in a multi-composer based-rollup deployment.
    #[must_use]
    pub fn poster_address(&self) -> alloy_primitives::Address {
        self.inner.config.poster.address()
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
        // Dispatch breadcrumb: correlate this postBatch tx to the L1 block we
        // aim it at. `SendOutcome::Included` later carries the ACTUAL inclusion
        // block, so joining on tx_hash gives the N+1 next-slot targeting hit-rate.
        event!(
            name: "eez.submitter.bundle.dispatch",
            Level::INFO,
            tx_hash = %post_batch_hash,
            target_block,
            exact = matches!(target, BundleTarget::Exact { .. }),
            tx_count = raw_txs.len(),
            "dispatching bundle to builder",
        );
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
    /// Provider used ONLY for target-block discovery on
    /// `BundleTarget::NextBlock`. Falls back to the main RPC when no
    /// override URL is set. See `SubmitterConfig::target_rpc_url`.
    fn build_target_provider(&self) -> impl Provider + use<> {
        let url = self
            .config
            .target_rpc_url
            .clone()
            .unwrap_or_else(|| self.reader.rpc_url());
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
                // Relay path: the builder honors the min/max timestamp pin, so
                // an Exact target's `pin_timestamp` unlocks the early verdict.
                self.observe(
                    post_batch_hash,
                    target_block,
                    pin_timestamp,
                    expected_final_state,
                )
                .await
            }
            Err(L1Error::BundleRpcUnsupported) => {
                // A multi-tx bundle needs its order kept: the deferred entries
                // chain, so a reordered user tx finds no entry and reverts.
                if raw_txs.len() > 1 {
                    event!(
                        name: "eez.submitter.bundle.mempool_fallback",
                        Level::WARN,
                        event_name = "eez.submitter.bundle.mempool_fallback",
                        target_block,
                        tx_count = raw_txs.len(),
                        "relay has no eth_sendBundle; the mempool does NOT guarantee bundle order, so bundled user txs may revert",
                    );
                } else {
                    event!(
                        name: "eez.submitter.bundle.mempool_fallback",
                        Level::INFO,
                        event_name = "eez.submitter.bundle.mempool_fallback",
                        target_block,
                        tx_count = raw_txs.len(),
                        "relay has no eth_sendBundle; submitting the lone postBatch via mempool",
                    );
                }
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
                // No bundle API, no timestamp pin: these txs can genuinely land
                // in a later block, so the conservative rule is the only one.
                self.observe(post_batch_hash, target_block, None, expected_final_state)
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
    ///
    /// `pinned` is the timestamp pin, set only for an [`BundleTarget::Exact`]
    /// bundle the relay accepted — those get the early verdict below.
    async fn observe(
        &self,
        tx_hash: TxHash,
        target_block: u64,
        pin_timestamp: Option<u64>,
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
        //
        // `pinned` buys a slot without weakening that: min == max timestamp
        // leaves one satisfiable height, so the block there decides it.
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
                Ok(None) => {
                    let verdict = match pin_timestamp {
                        Some(pin_ts) => {
                            pinned_slot_check(&target_provider, target_block, tx_hash, pin_ts).await
                        }
                        None => PinnedVerdict::Pending,
                    };
                    match verdict {
                        PinnedVerdict::Excluded => {
                            return Ok(dropped(
                                tx_hash,
                                target_block,
                                "pinned slot built without inclusion",
                            ));
                        }
                        PinnedVerdict::SlotSkipped => {
                            return Ok(dropped(tx_hash, target_block, "pinned slot skipped"));
                        }
                        // Our tx IS in the pinned block; only the receipt read
                        // trails it. Poll on, skipping the head rule.
                        PinnedVerdict::Included => {}
                        PinnedVerdict::Pending => match target_provider.get_block_number().await {
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
                    }
                }
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
            .address(self.reader.eez())
            .event_signature(L2ExecutionPerformed::SIGNATURE_HASH)
            .topic1(U256::from(self.reader.rollup_id()))
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
async fn post_bundle(
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

/// Verdict for a relay-submitted, timestamp-pinned bundle, read off the
/// canonical block at the pinned height.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinnedVerdict {
    /// Height not built (or not visible) yet — no verdict.
    Pending,
    /// Our postBatch is in it; the receipt follows.
    Included,
    /// Built without us; atomic bundles land in that block or nowhere.
    Excluded,
    /// Another timestamp filled the height. Timestamps strictly increase, so
    /// no later block can satisfy `minTimestamp == maxTimestamp == pin_ts`.
    SlotSkipped,
}

/// `block` is `(timestamp, contains our tx)` at the pinned height, `None` while
/// unobservable. Inclusion outranks a timestamp mismatch — in is in.
fn pinned_verdict(block: Option<(u64, bool)>, pin_ts: u64) -> PinnedVerdict {
    match block {
        None => PinnedVerdict::Pending,
        Some((_, true)) => PinnedVerdict::Included,
        Some((ts, false)) if ts == pin_ts => PinnedVerdict::Excluded,
        Some(_) => PinnedVerdict::SlotSkipped,
    }
}

/// Read the canonical block at the pinned height (hashes only) and apply
/// [`pinned_verdict`]. RPC failures yield `Pending`, never a verdict.
async fn pinned_slot_check<P: Provider>(
    provider: &P,
    target_block: u64,
    tx_hash: TxHash,
    pin_ts: u64,
) -> PinnedVerdict {
    match provider
        .get_block_by_number(BlockNumberOrTag::Number(target_block))
        .hashes()
        .await
    {
        Ok(block) => pinned_verdict(
            block.map(|b| {
                (
                    b.header.timestamp,
                    b.transactions.hashes().any(|h| h == tx_hash),
                )
            }),
            pin_ts,
        ),
        Err(err) => {
            event!(
                name: "eez.submitter.observe.pinned_block_read_failed",
                Level::WARN,
                tx_hash = %tx_hash,
                target_block,
                error = %err,
                "pinned block read failed during bundle observation; retrying",
            );
            PinnedVerdict::Pending
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{PinnedVerdict, pinned_verdict};

    const PIN: u64 = 1_700_000_012;

    #[test]
    fn pinned_verdict_covers_every_arm() {
        for (case, block, want) in [
            ("height unbuilt", None, PinnedVerdict::Pending),
            (
                "block carries our tx",
                Some((PIN, true)),
                PinnedVerdict::Included,
            ),
            (
                "pinned slot built without us",
                Some((PIN, false)),
                PinnedVerdict::Excluded,
            ),
            (
                "timestamp mismatch",
                Some((PIN + 5, false)),
                PinnedVerdict::SlotSkipped,
            ),
            // Inclusion is the stronger fact: a builder that ignored the pin
            // still settled us, so never call that height a skip.
            (
                "included despite mismatch",
                Some((PIN + 5, true)),
                PinnedVerdict::Included,
            ),
        ] {
            assert_eq!(pinned_verdict(block, PIN), want, "{case}");
        }
    }
}
