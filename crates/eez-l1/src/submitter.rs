//! Thin signed L1-submission primitive: sends `postAndVerifyBatch` via
//! `eth_sendBundle`. Read-only canonical-chain scans live in
//! [`L1Reader`].
//!
//! `eth_sendBundle` pins inclusion to one L1 block. Only L1 decides the
//! outcome: the post is in a block, or the chain passed its target
//! without it ([`SendOutcome::Dropped`]). The relay's answer is reported
//! as a [`RelayReply`] and never decides it, because a lost reply says
//! nothing about whether the relay took the bundle.

use std::sync::Arc;
use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{TxHash, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolEvent;
use eez_protocol::abi::BatchPosted;
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};
use crate::l1_reader::L1Reader;

/// Wall-clock cap on the target-block + inclusion check.
const TARGET_WAIT_BUDGET: Duration = Duration::from_secs(30);
/// Reads of the `NextBlock` target before giving up without sending.
const TARGET_READ_ATTEMPTS: u32 = 3;
/// How often a settlement read that keeps failing is logged again.
const SETTLEMENT_READ_WARN_EVERY: Duration = Duration::from_mins(1);
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
    /// Land in exactly L1 block `block`, without pinning its timestamp.
    /// Cap-triggered historical batches use this to target the slot that
    /// caused their composition while allowing a past L2 terminal timestamp.
    ExactBlock { block: u64 },
    /// Land in exactly L1 block `block`, and only if its timestamp equals
    /// `timestamp`. The timestamp pin makes the settlement slot match the
    /// L2 Sync block's anchored slot by construction: a gnosis block's
    /// timestamp IS its slot time, so a skipped slot (block lands a slot
    /// late) won't match — the bundle drops instead of settling with a
    /// drifted L2 timestamp.
    Exact { block: u64, timestamp: u64 },
}

/// What the relay answered to a bundle. It decides nothing about the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayReply {
    /// The relay has no bundle API and the post went through the mempool.
    Unsupported,
    /// The relay accepted the bundle.
    Accepted,
    /// The relay answered with an error.
    Rejected(String),
    /// No answer arrived, so the relay may have taken the bundle.
    Unanswered(String),
}

/// Why L1 shows the bundle did not land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Another timestamp filled the pinned height, so the pin cannot match.
    PinnedSlotSkipped,
    /// The pinned slot was built without the bundle.
    PinnedSlotExcluded,
    /// The chain passed the target block without the post.
    TargetPassed,
}

impl DropReason {
    const fn describe(self) -> &'static str {
        match self {
            Self::PinnedSlotSkipped => "pinned slot skipped",
            Self::PinnedSlotExcluded => "pinned slot built without inclusion",
            Self::TargetPassed => "target block passed without inclusion",
        }
    }
}

/// One sent bundle: what the relay answered and what L1 did.
#[derive(Debug, Clone)]
pub struct SentBundle {
    pub reply: RelayReply,
    pub outcome: SendOutcome,
}

/// What L1 did with one bundle. `Dropped` is the expected miss path —
/// caller rebuilds on the next tick.
#[derive(Debug, Clone)]
pub enum SendOutcome {
    Included {
        tx_hash: TxHash,
        l1_block: u64,
        /// Which entries L1 applied, from the same scan the deriver
        /// uses; `final_state` distinguishes "settled partially" from "nothing".
        settlement: crate::scan::Settlement,
    },
    Dropped {
        tx_hash: TxHash,
        target_block: u64,
        reason: DropReason,
    },
    /// The post landed, but reading how it settled failed for good. Not a
    /// failure by itself: the Deriver's cursor resolves the batch unless the
    /// post reverted or left L1.
    IncludedUnread {
        tx_hash: TxHash,
        l1_block: u64,
        error: String,
    },
}

/// Whether a bundled user_tx may revert without failing the bundle, so L1
/// settles partially. On by default; `EEZ_PARTIAL_CONSUMPTION=0` restores all-or-nothing.
///
/// Read once: flipping mid-run would make two bundles disagree.
fn partial_consumption_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        // Absent or unrecognised reads as ON; only an explicit off-value opts out.
        !std::env::var("EEZ_PARTIAL_CONSUMPTION")
            .is_ok_and(|raw| matches!(raw.trim(), "0" | "false" | "FALSE"))
    })
}

/// Bundle txs the relay may drop on revert. Index 0 is the postBatch and is
/// never whitelisted: if it reverts, nothing settled.
fn reverting_whitelist(raw_tx_hexes: &[&str], allow_partial: bool) -> Vec<String> {
    if !allow_partial {
        return Vec::new();
    }
    raw_tx_hexes
        .get(1..)
        .unwrap_or_default()
        .iter()
        .filter_map(|hex_tx| {
            let bytes = alloy_primitives::hex::decode(hex_tx).ok()?;
            Some(format!("{:#x}", alloy_primitives::keccak256(&bytes)))
        })
        .collect()
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
                // Bounded timeout so a relay that never answers cannot stall
                // the send: the bundle is then `Unanswered` and L1 decides,
                // by the target block at the latest. `Client::new()` has NO
                // timeout.
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
    /// A relay without `eth_sendBundle` gets only the postBatch, through the mempool.
    ///
    /// Once sent, the bundle is observed until L1 decides, whatever the
    /// relay answered. Only `raw_txs[0]` (the postBatch) is checked for
    /// inclusion; if it's in, the bundle either succeeded or a proposer
    /// broke atomicity (their bug, not ours).
    ///
    /// `on_dispatch` runs once, right before the bundle goes to the relay.
    ///
    /// # Errors
    ///
    /// Only when nothing was sent: an empty or undecodable bundle, or an
    /// unreadable `NextBlock` target.
    pub async fn send_bundle(
        &self,
        raw_txs: &[alloy_primitives::Bytes],
        target: BundleTarget,
        on_dispatch: impl FnOnce(),
    ) -> L1Result<SentBundle> {
        fail::fail_point!("submitter::send::start", |_| Err(L1Error::Submission(
            "injected failpoint: submitter::send::start".into()
        )));
        if raw_txs.is_empty() {
            return Err(L1Error::Submission(
                "send_bundle called with no txs".to_string(),
            ));
        }
        // Exact targets carry the L2 Sync block's anchored timestamp; pin
        // the bundle to land only in a block at that exact time. ExactBlock
        // and NextBlock deliberately stay unpinned.
        let pin_timestamp = match target {
            BundleTarget::Exact { timestamp, .. } => Some(timestamp),
            BundleTarget::NextBlock | BundleTarget::ExactBlock { .. } => None,
        };
        let target_block = match target {
            BundleTarget::Exact { block, .. } | BundleTarget::ExactBlock { block } => block,
            BundleTarget::NextBlock => self.inner.next_block_target().await?,
        };
        let post_batch_hash = eez_blob::post_batch_tx_hash(&raw_txs[0]).map_err(|e| {
            L1Error::Submission(format!("send_bundle: decode postBatch envelope: {e}"))
        })?;
        // Dispatch breadcrumb: correlate this postBatch tx to the L1 block we
        // aim it at. `SendOutcome::Included` later carries the ACTUAL inclusion
        // block, so joining on tx_hash gives the N+1 next-slot targeting hit-rate.
        event!(
            name: "eez.submitter.bundle.dispatch",
            Level::INFO,
            tx_hash = %post_batch_hash,
            target_block,
            exact = !matches!(target, BundleTarget::NextBlock),
            tx_count = raw_txs.len(),
            "dispatching bundle to builder",
        );
        on_dispatch();
        // One bundle, one target block. Atomic bundle semantics:
        // rbuilder either includes the whole bundle in `target_block`
        // in the specified order, or drops it. A relay without a bundle API
        // gets only the postBatch, through the mempool, inside
        // `dispatch_and_observe`.
        self.inner
            .dispatch_and_observe(raw_txs, post_batch_hash, target_block, pin_timestamp)
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
        Ok(self.receipt_status(tx_hash).await?.is_some())
    }

    /// Whether `tx_hash` succeeded on L1, or `None` without a receipt.
    ///
    /// # Errors
    ///
    /// [`L1Error::Provider`] on RPC failure.
    pub async fn receipt_status(&self, tx_hash: TxHash) -> L1Result<Option<bool>> {
        // Target-tip RPC, not the embedded L1 — the embedded node can
        // lag the canonical tip by 2-3 blocks, and a missed receipt
        // here re-queues a burned-nonce tx that poisons the next
        // bundle's simulation.
        let provider = self.inner.build_target_provider();
        Ok(provider
            .get_transaction_receipt(tx_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_transaction_receipt: {e}")))?
            .map(|receipt| receipt.status()))
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

    /// Whether L1 block `number` on the target chain holds `tx_hash`, or
    /// `None` if the block is absent.
    pub async fn block_has_tx(&self, number: u64, tx_hash: TxHash) -> L1Result<Option<bool>> {
        let provider = self.inner.build_target_provider();
        Ok(provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .hashes()
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({number}): {e}")))?
            .map(|b| b.transactions.hashes().any(|h| h == tx_hash)))
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

    /// `latest + NEXT_BLOCK_SLACK` from the target provider, read a few times.
    async fn next_block_target(&self) -> L1Result<u64> {
        let provider = self.build_target_provider();
        let mut attempt = 1;
        loop {
            match provider.get_block_number().await {
                Ok(head) => return Ok(head + NEXT_BLOCK_SLACK),
                Err(e) if attempt >= TARGET_READ_ATTEMPTS => {
                    return Err(L1Error::Provider(format!("get_block_number: {e}")));
                }
                Err(_) => attempt += 1,
            }
            tokio::time::sleep(TARGET_POLL_INTERVAL).await;
        }
    }

    /// Submit `raw_txs` (postBatch first) and observe the outcome. Try
    /// the relay's `eth_sendBundle`; if it's a plain execution RPC
    /// (JSON-RPC `-32601`), send only the postBatch, calldata or blob,
    /// through `eth_sendRawTransaction`. The mempool keeps no bundle order, so
    /// a user tx could run before its entry and revert: the user txs are never
    /// sent, and their entries stay unconsumed.
    async fn dispatch_and_observe(
        &self,
        raw_txs: &[alloy_primitives::Bytes],
        post_batch_hash: TxHash,
        target_block: u64,
        pin_timestamp: Option<u64>,
    ) -> L1Result<SentBundle> {
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
            PostReply::Relay(reply) => {
                if reply != RelayReply::Accepted {
                    event!(
                        name: "eez.submitter.bundle.not_accepted",
                        Level::WARN,
                        tx_hash = %post_batch_hash,
                        target_block,
                        relay_reply = ?reply,
                        "relay did not accept the bundle; L1 decides its outcome",
                    );
                }
                // Relay path: the builder honors the min/max timestamp pin, so
                // an Exact target's `pin_timestamp` unlocks the early verdict.
                let outcome = self
                    .observe(post_batch_hash, target_block, pin_timestamp)
                    .await;
                Ok(SentBundle { reply, outcome })
            }
            PostReply::Unsupported => {
                let unsent = raw_txs.len() - 1;
                if unsent > 0 {
                    event!(
                        name: "eez.submitter.bundle.mempool_fallback",
                        Level::ERROR,
                        event_name = "eez.submitter.bundle.mempool_fallback",
                        target_block,
                        tx_count = raw_txs.len(),
                        unsent_user_txs = unsent,
                        "the relay at EEZ_L1_BUILDER_RPC_URL has no eth_sendBundle; only the postBatch is sent; the bundle's {unsent} user transactions are not sent and their entries stay unconsumed",
                    );
                } else {
                    event!(
                        name: "eez.submitter.bundle.mempool_fallback",
                        Level::WARN,
                        event_name = "eez.submitter.bundle.mempool_fallback",
                        target_block,
                        tx_count = raw_txs.len(),
                        "the relay at EEZ_L1_BUILDER_RPC_URL has no eth_sendBundle; the postBatch goes through the mempool",
                    );
                }
                // No builder took the bundle, so it is never `Accepted`.
                let mut reply = RelayReply::Unsupported;
                if let Err(err) = alloy_provider::Provider::send_raw_transaction(
                    &self.build_target_provider(),
                    raw_txs[0].as_ref(),
                )
                .await
                {
                    // L1 still decides whether the postBatch landed.
                    let detail = format!("mempool fallback: postBatch send: {err}");
                    reply = if err.is_error_resp() {
                        RelayReply::Rejected(detail)
                    } else {
                        RelayReply::Unanswered(detail)
                    };
                    event!(
                        name: "eez.submitter.bundle.not_accepted",
                        Level::WARN,
                        tx_hash = %post_batch_hash,
                        target_block,
                        relay_reply = ?reply,
                        "mempool did not accept the postBatch; L1 decides its outcome",
                    );
                }
                // No bundle API, no timestamp pin: the postBatch can genuinely
                // land in a later block, so the conservative rule is the only one.
                let outcome = self.observe(post_batch_hash, target_block, None).await;
                Ok(SentBundle { reply, outcome })
            }
        }
    }

    /// Bundle observation, transport-agnostic: poll for the postBatch
    /// receipt (no block pinning — a builder may land the bundle a block
    /// late and the embedded L1 may lag the tip, so pinning to the exact
    /// target produced false `Dropped` verdicts), then attribute the applied
    /// entries from the inclusion block via [`Self::observe_settlement`].
    ///
    /// `pin_timestamp` is the pin of an [`BundleTarget::Exact`] bundle sent to
    /// the relay, whatever it answered; it gets the early verdict below.
    async fn observe(
        &self,
        tx_hash: TxHash,
        target_block: u64,
        pin_timestamp: Option<u64>,
    ) -> SendOutcome {
        self.observe_with_provider(
            &self.build_target_provider(),
            tx_hash,
            target_block,
            pin_timestamp,
        )
        .await
    }

    async fn observe_with_provider<P: Provider>(
        &self,
        target_provider: &P,
        tx_hash: TxHash,
        target_block: u64,
        pin_timestamp: Option<u64>,
    ) -> SendOutcome {
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
        let mut slow_logged = false;
        let mut settlement_read_warned: Option<tokio::time::Instant> = None;
        loop {
            // Transient RPC failures are retried, never escalated to a
            // failure verdict — while polling, the ledger stays Pending
            // and the composer's gate stays closed.
            match target_provider.get_transaction_receipt(tx_hash).await {
                // A receipt without a block number is read again, like a failed read.
                Ok(Some(receipt)) if receipt.block_number.is_none() => event!(
                    name: "eez.submitter.observe.receipt_read_failed",
                    Level::WARN,
                    tx_hash = %tx_hash,
                    error = "receipt present but block_number missing",
                    "receipt read failed during bundle observation; retrying",
                ),
                Ok(Some(receipt)) => {
                    let l1_block = receipt.block_number.unwrap_or_default();
                    // The receipt's own logs say whether a batch was posted: a
                    // call that succeeds can still post nothing, e.g. before
                    // the contract exists.
                    let posted = receipt.inner.logs().iter().any(|log| {
                        log.address() == self.reader.eez()
                            && log.topic0() == Some(&BatchPosted::SIGNATURE_HASH)
                    });
                    let read = self
                        .observe_settlement(target_provider, l1_block, tx_hash, posted)
                        .await;
                    match included_outcome(tx_hash, l1_block, read) {
                        Ok(outcome) => return outcome,
                        // If a reorg removes the block, the receipt goes with it.
                        Err(err) => {
                            if settlement_read_warned
                                .is_none_or(|at| at.elapsed() >= SETTLEMENT_READ_WARN_EVERY)
                            {
                                settlement_read_warned = Some(tokio::time::Instant::now());
                                event!(
                                    name: "eez.submitter.observe.settlement_read_failed",
                                    Level::WARN,
                                    tx_hash = %tx_hash,
                                    l1_block,
                                    error = %err,
                                    "settlement read failed after inclusion; retrying",
                                );
                            }
                        }
                    }
                }
                Ok(None) => {
                    let verdict = match pin_timestamp {
                        Some(pin_ts) => {
                            pinned_slot_check(target_provider, target_block, tx_hash, pin_ts).await
                        }
                        None => PinnedVerdict::Pending,
                    };
                    match verdict {
                        PinnedVerdict::Excluded => {
                            return dropped(tx_hash, target_block, DropReason::PinnedSlotExcluded);
                        }
                        PinnedVerdict::SlotSkipped => {
                            return dropped(tx_hash, target_block, DropReason::PinnedSlotSkipped);
                        }
                        // Our tx IS in the pinned block; only the receipt read
                        // trails it. Poll on, skipping the head rule.
                        PinnedVerdict::Included => {}
                        PinnedVerdict::Pending => match target_provider.get_block_number().await {
                            Ok(head) if head > target_block => {
                                return dropped(tx_hash, target_block, DropReason::TargetPassed);
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

    /// How far L1 got with our batch, via the shared scan in [`crate::scan`] —
    /// one reading of these logs, not a bespoke block scan.
    async fn observe_settlement<P: Provider>(
        &self,
        provider: &P,
        l1_block: u64,
        tx_hash: TxHash,
        posted: bool,
    ) -> L1Result<crate::scan::Settlement> {
        let posts = crate::scan::scan_range(
            provider,
            &self.reader.build_trace_provider(),
            self.reader.eez(),
            self.reader.rollup_id(),
            l1_block,
            l1_block,
        )
        .await?;
        settlement_of_our_post(posts, l1_block, tx_hash, posted)
    }
}

/// The relay's answer to one `eth_sendBundle` POST.
#[derive(Debug, PartialEq, Eq)]
enum PostReply {
    Relay(RelayReply),
    /// JSON-RPC `-32601`: a plain execution RPC with no bundle API.
    Unsupported,
}

/// POST a multi-tx bundle to a Flashbots-style `eth_sendBundle` relay.
/// Any non-error `result` is `Accepted` (most relays use `{"result":
/// {"bundleHash": "0x..."}}`, the chiado builder returns `{"result":
/// null}`). A non-2xx status or a JSON-RPC error is `Rejected`. A failed
/// send or an undecodable reply is `Unanswered`.
///
/// `target_block` is encoded as `0x{hex}` in the bundle params. Caller
/// is responsible for picking a not-yet-proposed block — the relay
/// silently drops bundles aimed at already-built blocks.
async fn post_bundle(
    http: &reqwest::Client,
    builder_rpc_url: &str,
    raw_tx_hexes: &[&str],
    target_block: u64,
    pin_timestamp: Option<u64>,
) -> PostReply {
    // Any revert OUTSIDE `revertingTxHashes` fails the bundle; whitelisting the
    // user_txs lets one revert while the postBatch lands, so L1 settles partially.
    let reverting = reverting_whitelist(raw_tx_hexes, partial_consumption_enabled());
    let mut bundle_params = serde_json::json!({
        "txs": raw_tx_hexes,
        "blockNumber": format!("0x{target_block:x}"),
    });
    if !reverting.is_empty() {
        bundle_params["revertingTxHashes"] = serde_json::json!(reverting);
    }
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
    let reply = match http.post(builder_rpc_url).json(&body).send().await {
        Ok(reply) => reply,
        Err(e) => {
            return PostReply::Relay(RelayReply::Unanswered(format!("eth_sendBundle POST: {e}")));
        }
    };
    let reply = match reply.error_for_status() {
        Ok(reply) => reply,
        Err(e) => {
            return PostReply::Relay(RelayReply::Rejected(format!("eth_sendBundle HTTP: {e}")));
        }
    };
    let resp: serde_json::Value = match reply.json().await {
        Ok(resp) => resp,
        Err(e) => {
            return PostReply::Relay(RelayReply::Unanswered(format!(
                "eth_sendBundle decode: {e}"
            )));
        }
    };
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
            return PostReply::Unsupported;
        }
        return PostReply::Relay(RelayReply::Rejected(format!("eth_sendBundle: {err}")));
    }
    PostReply::Relay(RelayReply::Accepted)
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

fn dropped(tx_hash: TxHash, target_block: u64, reason: DropReason) -> SendOutcome {
    event!(
        name: "eez.submitter.bundle.dropped",
        Level::WARN,
        event_name = "eez.submitter.bundle.dropped",
        target_block,
        tx_hash = %tx_hash,
        reason = reason.describe(),
        "bundle dropped; composer will retry on next tick",
    );
    SendOutcome::Dropped {
        tx_hash,
        target_block,
        reason,
    }
}

/// The outcome of a post whose receipt exists, or the retryable error to read
/// again on. The receipt proves the post landed, so no failed read makes it a
/// failure: a read that cannot succeed leaves the settlement unread.
fn included_outcome(
    tx_hash: TxHash,
    l1_block: u64,
    read: L1Result<crate::scan::Settlement>,
) -> L1Result<SendOutcome> {
    match read {
        Ok(settlement) => Ok(SendOutcome::Included {
            tx_hash,
            l1_block,
            settlement,
        }),
        Err(err) if !err.is_terminal() => Err(err),
        Err(err) => Ok(SendOutcome::IncludedUnread {
            tx_hash,
            l1_block,
            error: err.to_string(),
        }),
    }
}

/// Our tx posts directly, and the scan rejects a direct tx with two posts, so
/// at most one post carries our hash.
fn settlement_of_our_post(
    posts: Vec<crate::scan::Post>,
    l1_block: u64,
    tx_hash: TxHash,
    posted: bool,
) -> L1Result<crate::scan::Settlement> {
    match posts.into_iter().find(|post| post.tx_hash == tx_hash) {
        Some(post) => Ok(post.settlement),
        // The receipt shows a post the scan lacks: the logs lag the receipt,
        // not a post that applied nothing.
        None if posted => Err(L1Error::SourceIncomplete {
            block: l1_block,
            detail: format!("postBatch {tx_hash}: receipt lists a post the scan lacks; retry"),
        }),
        None => Ok(crate::scan::Settlement::NONE),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PinnedVerdict, PostReply, RelayReply, SendOutcome, included_outcome, pinned_verdict,
        post_bundle, reverting_whitelist, settlement_of_our_post,
    };
    use crate::error::L1Error;
    use crate::scan::{Post, PostId, Settlement};
    use alloy_primitives::{Address, B256, TxHash};

    const PIN: u64 = 1_700_000_012;

    #[tokio::test]
    async fn observe_returns_the_reason_for_each_drop_path() {
        use super::{DropReason, SendOutcome, Submitter, SubmitterConfig};
        use crate::L1ReaderConfig;
        use alloy_provider::{ProviderBuilder, mock::Asserter};
        use alloy_rpc_types_eth::{Block, Transaction};
        use alloy_signer_local::PrivateKeySigner;

        const TARGET: u64 = 10;
        let url = "http://127.0.0.1:1".parse::<url::Url>().unwrap();
        let submitter = Submitter::new(SubmitterConfig {
            builder_rpc_url: url.clone(),
            target_rpc_url: None,
            reader: L1ReaderConfig {
                rpc_url: url.clone(),
                trace_rpc_url: url,
                eez: Address::ZERO,
                rollup_id: 1,
                deploy_block: 0,
                beacon_url: None,
            },
            poster: PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap(),
        });
        for (pin, timestamp, expected) in [
            (Some(PIN), Some(PIN), DropReason::PinnedSlotExcluded),
            (Some(PIN), Some(PIN + 5), DropReason::PinnedSlotSkipped),
            (None, None, DropReason::TargetPassed),
            (Some(PIN), None, DropReason::TargetPassed),
        ] {
            let rpc = Asserter::new();
            rpc.push_success(&serde_json::Value::Null); // receipt
            if let Some(timestamp) = timestamp {
                let mut block = Block::<Transaction>::default();
                block.header.inner.timestamp = timestamp;
                rpc.push_success(&block);
            } else {
                if pin.is_some() {
                    rpc.push_success(&serde_json::Value::Null); // pinned height unavailable
                }
                rpc.push_success(&"0xb"); // head passed TARGET
            }
            let provider = ProviderBuilder::default().connect_mocked_client(rpc);
            let outcome = submitter
                .inner
                .observe_with_provider(&provider, TxHash::ZERO, TARGET, pin)
                .await;
            assert!(
                matches!(outcome, SendOutcome::Dropped { tx_hash: TxHash::ZERO, target_block: TARGET, reason } if reason == expected),
                "{outcome:?}"
            );
        }
    }

    /// The postBatch at index 0 is never whitelisted: if it reverts nothing
    /// settled, so letting the user_txs land would deliver calls with no state.
    #[test]
    fn reverting_whitelist_covers_every_arm() {
        // Hashes are keccak of the raw EIP-2718 envelope, which is what the
        // relay matches against.
        let hash = |raw: &str| {
            format!(
                "{:#x}",
                alloy_primitives::keccak256(alloy_primitives::hex::decode(raw).unwrap())
            )
        };
        for (case, raw, allow, want) in [
            (
                "postBatch excluded, user_txs whitelisted",
                &["0xaa00", "0xbb11", "0xcc22"][..],
                true,
                vec![hash("0xbb11"), hash("0xcc22")],
            ),
            (
                "opted out restores all-or-nothing",
                &["0xaa00", "0xbb11"][..],
                false,
                Vec::new(),
            ),
            ("lone postBatch", &["0xaa00"][..], true, Vec::new()),
            ("empty bundle", &[][..], true, Vec::new()),
        ] {
            assert_eq!(reverting_whitelist(raw, allow), want, "{case}");
        }
    }

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

    /// A relay that answers `status` with `body` to every request, or closes
    /// the connection unanswered when `reply` is `None`.
    async fn relay(reply: Option<(u16, &'static str)>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                // Read the whole request so a close is not a reset mid-send.
                let mut request = Vec::new();
                let mut chunk = [0_u8; 4096];
                while let Ok(read @ 1..) = socket.read(&mut chunk).await {
                    request.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                if let Some((status, body)) = reply {
                    let response = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            }
        });
        url
    }

    #[tokio::test]
    async fn post_bundle_classifies_every_reply() {
        let accepted = r#"{"jsonrpc":"2.0","id":1,"result":{"bundleHash":"0x01"}}"#;
        let error = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"no"}}"#;
        let unsupported = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no"}}"#;
        // A port that was bound and released refuses connections.
        let refused = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        let http = reqwest::Client::new();
        for (case, url, want) in [
            (
                "200 + result",
                relay(Some((200, accepted))).await,
                "Accepted",
            ),
            ("200 + error", relay(Some((200, error))).await, "Rejected"),
            (
                "-32601",
                relay(Some((200, unsupported))).await,
                "Unsupported",
            ),
            ("502", relay(Some((502, ""))).await, "Rejected"),
            ("closed unanswered", relay(None).await, "Unanswered"),
            ("refused", refused, "Unanswered"),
        ] {
            let got = match post_bundle(&http, &url, &["0xaa"], 7, None).await {
                PostReply::Relay(RelayReply::Accepted) => "Accepted",
                PostReply::Relay(RelayReply::Unsupported) => "Unsupported",
                PostReply::Relay(RelayReply::Rejected(_)) => "Rejected",
                PostReply::Relay(RelayReply::Unanswered(_)) => "Unanswered",
                PostReply::Unsupported => "Unsupported",
            };
            assert_eq!(got, want, "{case}");
        }
    }

    /// A landed post never fails on a settlement read: a terminal read leaves
    /// it unread, and a retryable one reads again.
    #[test]
    fn settlement_read_failure_after_inclusion_is_not_a_failure() {
        let tx = TxHash::repeat_byte(0x0A);
        let terminal = L1Error::TraceUnavailable {
            block: 7,
            detail: "no debug namespace".into(),
        };
        assert!(matches!(
            included_outcome(tx, 7, Err(terminal)),
            Ok(SendOutcome::IncludedUnread { l1_block: 7, .. })
        ));
        let retryable = L1Error::Provider("timeout".into());
        assert!(matches!(
            included_outcome(tx, 7, Err(retryable)),
            Err(L1Error::Provider(_))
        ));
        assert!(matches!(
            included_outcome(tx, 7, Ok(Settlement::NONE)),
            Ok(SendOutcome::Included { l1_block: 7, .. })
        ));
    }

    /// The post under our hash settles, and none is a lagging scan only when
    /// the receipt posted.
    #[test]
    fn our_tx_settles_through_its_post() {
        let ours = TxHash::repeat_byte(0x0A);
        let root = |byte| Some(B256::repeat_byte(byte));
        let post = |tx_hash, log_index: u8| Post {
            id: PostId {
                l1_block: 7,
                l1_block_hash: B256::ZERO,
                log_index: log_index.into(),
            },
            tx_hash,
            sender: Address::ZERO,
            l1_context: None,
            batch: eez_protocol::abi::EvmBatch::default(),
            blob_hashes: Vec::new(),
            settlement: Settlement::new(Vec::new(), root(log_index), None, false),
        };
        let theirs = post(TxHash::repeat_byte(0x0B), 1);
        for (posts, posted, want) in [
            (vec![theirs.clone(), post(ours, 3)], true, Ok(root(3))),
            (vec![theirs.clone()], true, Err("SourceIncomplete")),
            (vec![theirs], false, Ok(None)),
        ] {
            let got = settlement_of_our_post(posts, 7, ours, posted)
                .map(|settlement| settlement.final_state)
                .map_err(|err| match err {
                    L1Error::SourceIncomplete { .. } => "SourceIncomplete",
                    other => panic!("unexpected {other}"),
                });
            assert_eq!(got, want, "posted={posted}");
        }
    }
}
