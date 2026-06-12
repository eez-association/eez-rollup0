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

use alloy_consensus::{Transaction, TxEip1559, TxEnvelope, TypedTransaction};
use alloy_eips::{BlockNumberOrTag, Encodable2718};
use alloy_network::{Ethereum, EthereumWallet, NetworkWallet};
use alloy_primitives::{TxHash, TxKind, U256, hex};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::Filter;
use alloy_sol_types::{SolCall, SolEvent};
use eez_prover::{EezRegistry, ProofSystemBatchPerVerificationEntries, StateDelta};
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Buffer to absorb some variance (for contentious txs).
const GAS_BUFFER_NUM: u128 = 3;
const GAS_BUFFER_DEN: u128 = 2;

/// `max_fee_per_gas` headroom over `estimate_eip1559_fees`
const MAX_FEE_BUFFER_NUM: u128 = 2;
const MAX_FEE_BUFFER_DEN: u128 = 1;

/// Wall-clock cap on the target-block + inclusion check.
const TARGET_WAIT_BUDGET: Duration = Duration::from_secs(30);
const TARGET_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// L1 block offset for [`BundleTarget::NextBlock`].
const NEXT_BLOCK_SLACK: u64 = 2;

/// Which L1 block the bundle should land in. Stage-3 passes
/// [`BundleTarget::NextBlock`]; stage-4 will pick exact targets from
/// the sync-slot scheduler.
#[derive(Debug, Clone, Copy)]
pub enum BundleTarget {
    /// Resolved to `latest + NEXT_BLOCK_SLACK` at send time.
    NextBlock,
    /// Caller-picked exact L1 block.
    Exact(u64),
}

/// One bundle attempt's outcome. `Dropped` is the expected miss path —
/// caller rebuilds on the next tick.
#[derive(Debug, Clone, Copy)]
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

    /// Pre-simulate, sign, POST `eth_sendBundle`, observe target.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Submission`] on simulation revert or relay rejection.
    /// - [`L1Error::Provider`] on RPC failure.
    pub async fn send(
        &self,
        batch: ProofSystemBatchPerVerificationEntries,
        target: BundleTarget,
    ) -> L1Result<SendOutcome> {
        fail::fail_point!("submitter::send::start", |_| Err(L1Error::Submission(
            "injected failpoint: submitter::send::start".into()
        )));
        let provider = self.inner.build_provider();
        let from = self.inner.config.poster.address();
        let eez = EezRegistry::new(self.inner.config.eez, &provider);
        let call = eez.postAndVerifyBatch(batch).from(from);

        call.call()
            .await
            .map_err(|e| L1Error::Submission(format!("eth_call simulation reverted: {e}")))?;
        let estimated = call
            .estimate_gas()
            .await
            .map_err(|e| L1Error::Submission(format!("eth_estimateGas: {e}")))?;
        let gas_limit = u64::try_from(
            u128::from(estimated)
                .saturating_mul(GAS_BUFFER_NUM)
                .saturating_div(GAS_BUFFER_DEN),
        )
        .unwrap_or(u64::MAX);

        let target_block = match target {
            BundleTarget::Exact(n) => n,
            BundleTarget::NextBlock => {
                provider
                    .get_block_number()
                    .await
                    .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?
                    + NEXT_BLOCK_SLACK
            }
        };

        let (envelope, tx_hash) = self
            .inner
            .sign(&provider, call.calldata().clone(), gas_limit)
            .await?;
        self.inner
            .post_bundle(&hex::encode_prefixed(envelope.encoded_2718()), target_block)
            .await?;
        self.inner
            .observe_target(&provider, tx_hash, target_block)
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
                claimed_current_state: b.claimed_current_state,
                claimed_new_state: b.claimed_new_state,
            })
            .collect())
    }

    /// Hash of the canonical L1 block at `number`, or `None` if L1 has
    /// no block at that height. Used by the Deriver's resync to check
    /// whether an indexed batch's L1 block is still canonical.
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
    /// First `StateDelta.currentState`; diagnostic only —
    /// `state_applied` is the authoritative winner/loser signal.
    pub claimed_current_state: Option<alloy_primitives::B256>,
    /// First `StateDelta.newState`; deriver compares to local STF
    /// result at `to_block` to catch claimed-vs-derived divergence.
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

    async fn sign<P: Provider>(
        &self,
        provider: &P,
        input: alloy_primitives::Bytes,
        gas_limit: u64,
    ) -> L1Result<(TxEnvelope, TxHash)> {
        let from = self.config.poster.address();
        let nonce = provider
            .get_transaction_count(from)
            .pending()
            .await
            .map_err(|e| L1Error::Provider(format!("get_transaction_count: {e}")))?;
        let chain_id = provider
            .get_chain_id()
            .await
            .map_err(|e| L1Error::Provider(format!("get_chain_id: {e}")))?;
        let fees = provider
            .estimate_eip1559_fees()
            .await
            .map_err(|e| L1Error::Provider(format!("estimate_eip1559_fees: {e}")))?;

        let max_fee_per_gas = fees
            .max_fee_per_gas
            .saturating_mul(MAX_FEE_BUFFER_NUM)
            .saturating_div(MAX_FEE_BUFFER_DEN);
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
            to: TxKind::Call(self.config.eez),
            value: U256::ZERO,
            access_list: alloy_eips::eip2930::AccessList::default(),
            input,
        };
        let wallet = EthereumWallet::from(self.config.poster.clone());
        let envelope =
            NetworkWallet::<Ethereum>::sign_transaction(&wallet, TypedTransaction::Eip1559(tx))
                .await
                .map_err(|e| L1Error::Submission(format!("sign envelope: {e}")))?;
        let tx_hash = *envelope.tx_hash();
        Ok((envelope, tx_hash))
    }

    async fn post_bundle(&self, raw_tx_hex: &str, target_block: u64) -> L1Result<()> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_sendBundle",
            "params": [{ "txs": [raw_tx_hex], "blockNumber": format!("0x{target_block:x}") }],
        });
        let resp: serde_json::Value = self
            .http
            .post(self.config.builder_rpc_url.as_str())
            .json(&body)
            .send()
            .await
            .map_err(|e| L1Error::Provider(format!("eth_sendBundle POST: {e}")))?
            .error_for_status()
            .map_err(|e| L1Error::Submission(format!("eth_sendBundle HTTP: {e}")))?
            .json()
            .await
            .map_err(|e| L1Error::Provider(format!("eth_sendBundle decode: {e}")))?;
        if let Some(err) = resp.get("error") {
            return Err(L1Error::Submission(format!("eth_sendBundle: {err}")));
        }
        Ok(())
    }

    async fn observe_target<P: Provider>(
        &self,
        provider: &P,
        tx_hash: TxHash,
        target_block: u64,
    ) -> L1Result<SendOutcome> {
        let start = tokio::time::Instant::now();
        loop {
            let latest = provider
                .get_block_number()
                .await
                .map_err(|e| L1Error::Provider(format!("get_block_number: {e}")))?;
            if latest >= target_block {
                break;
            }
            if start.elapsed() >= TARGET_WAIT_BUDGET {
                return Ok(dropped(
                    tx_hash,
                    target_block,
                    "target block not produced within budget",
                ));
            }
            tokio::time::sleep(TARGET_POLL_INTERVAL).await;
        }

        // TODO: this reads the block at `target_block` height; if the
        // chain reorgs out the original target between sign and observe,
        // we'll inspect the replacement block — which may not contain
        // our tx even though it was included in the orphaned one. Stage-3
        // accepts the false-Dropped (composer rebuilds next tick); revisit
        // when the prover commits to a specific L1 anchor.
        let block = provider
            .get_block_by_number(BlockNumberOrTag::Number(target_block))
            .await
            .map_err(|e| L1Error::Provider(format!("get_block_by_number({target_block}): {e}")))?
            .ok_or_else(|| {
                L1Error::Provider(format!("block {target_block} missing after latest>=target"))
            })?;
        if !block.transactions.hashes().any(|h| h == tx_hash) {
            return Ok(dropped(
                tx_hash,
                target_block,
                "tx absent from target block",
            ));
        }

        let winners = Filter::new()
            .address(self.config.eez)
            .event_signature(EezRegistry::L2ExecutionPerformed::SIGNATURE_HASH)
            .topic1(U256::from(self.config.rollup_id))
            .from_block(target_block)
            .to_block(target_block);
        let state_applied = provider
            .get_logs(&winners)
            .await
            .map_err(|e| L1Error::Provider(format!("get_logs(L2ExecutionPerformed): {e}")))?
            .iter()
            .any(|l| l.transaction_hash == Some(tx_hash));

        Ok(SendOutcome::Included {
            tx_hash,
            l1_block: target_block,
            state_applied,
        })
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

/// First `StateDelta` whose `rollupId` matches ours. Multi-rollup batches
/// can carry deltas for several rollups; we only care about our own.
/// Linearity (`EEZ.sol:967`) makes "first match" well-defined — at most
/// one delta per rollup per batch.
pub(crate) fn our_state_delta(
    batch: &ProofSystemBatchPerVerificationEntries,
    rollup_id: u64,
) -> Option<&StateDelta> {
    let rid = U256::from(rollup_id);
    batch
        .entries
        .iter()
        .flat_map(|entry| entry.stateDeltas.iter())
        .find(|delta| delta.rollupId == rid)
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
    pub claimed_current_state: Option<alloy_primitives::B256>,
    pub claimed_new_state: Option<alloy_primitives::B256>,
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
        .event_signature(EezRegistry::BatchPosted::SIGNATURE_HASH)
        .from_block(from_block)
        .to_block(to_block);
    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| L1Error::Provider(format!("get_logs(BatchPosted): {e}")))?;

    let winners_filter = Filter::new()
        .address(eez)
        .event_signature(EezRegistry::L2ExecutionPerformed::SIGNATURE_HASH)
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
        let tx = provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| L1Error::Provider(format!("get_tx({tx_hash}): {e}")))?
            .ok_or_else(|| L1Error::Provider(format!("tx {tx_hash} not found")))?;
        let submitter = tx.inner.signer();
        let input = tx.inner.input();
        let decoded = EezRegistry::postAndVerifyBatchCall::abi_decode(input)
            .map_err(|e| L1Error::Provider(format!("decode postBatch({tx_hash}): {e}")))?;
        let decoded_event = EezRegistry::BatchPosted::decode_log(&alloy_primitives::Log {
            address: log.address(),
            data: log.data().clone(),
        })
        .map_err(|e| L1Error::Provider(format!("decode BatchPosted({tx_hash}): {e}")))?;
        let our_delta = our_state_delta(&decoded.batch, rollup_id);
        let claimed_current_state = our_delta.map(|d| d.currentState);
        let claimed_new_state = our_delta.map(|d| d.newState);
        out.push(ScannedBatch {
            l1_block_number,
            l1_block_hash,
            tx_hash,
            submitter,
            rollup_count: decoded_event.rollupCount,
            call_data: decoded.batch.callData,
            state_applied: winner_tx_hashes.contains(&tx_hash),
            claimed_current_state,
            claimed_new_state,
        });
    }
    Ok(out)
}
