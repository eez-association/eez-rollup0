//! Thin L1-interaction primitive: sends `postAndVerifyBatch` and reads
//! past `BatchPosted` events. Stateless — no cursors, no batch
//! construction, no prover orchestration. The [`Composer`](crate::Composer)
//! owns all of that and calls into here.
//!
//! Per-call: builds a fresh provider from
//! [`SubmitterConfig`](crate::SubmitterConfig), pre-simulates via
//! `eth_call` to surface typed reverts before broadcasting, sends, and
//! awaits one confirmation within [`RECEIPT_TIMEOUT`].

use std::sync::Arc;
use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_network::EthereumWallet;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, TransactionTrait};
use alloy_sol_types::{SolCall, SolEvent};
use eez_prover::{EezRegistry, ProofSystemBatchPerVerificationEntries, StateDelta};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Upper bound on how long we wait for a postBatch tx to confirm.
/// later we switch to bundler-routed submission.
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(45);

/// Buffer to absorb some variance (for contentious txs)
const GAS_BUFFER_NUM: u128 = 3;
const GAS_BUFFER_DEN: u128 = 2;

/// Outcome of a successful [`Submitter::send`].
#[derive(Debug, Clone, Copy)]
pub struct SendOutcome {
    /// Tx hash of the broadcasted postBatch.
    pub tx_hash: alloy_primitives::TxHash,
    /// L1 block in which the tx landed.
    pub l1_block: u64,
}

/// Thin L1-interaction primitive — send postBatch txs and read
/// past `BatchPosted` events. Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct Submitter {
    inner: Arc<Inner>,
}

struct Inner {
    config: SubmitterConfig,
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
            inner: Arc::new(Inner { config }),
        }
    }

    /// L1 EOA address this Submitter sends from. Used by the Composer
    /// to identify "our own" `L1Event::BatchPosted` events vs.
    /// external ones in a multi-composer based-rollup deployment.
    #[must_use]
    pub fn poster_address(&self) -> alloy_primitives::Address {
        self.inner.config.poster.address()
    }

    /// Pre-simulate, broadcast, and await one confirmation for
    /// `postAndVerifyBatch(batch)`.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Submission`] on simulation revert, send failure,
    ///   receipt-await timeout, or on-chain revert.
    pub async fn send(
        &self,
        batch: ProofSystemBatchPerVerificationEntries,
    ) -> L1Result<SendOutcome> {
        let provider = self.inner.build_provider();
        let eez = EezRegistry::new(self.inner.config.eez, &provider);

        let call_builder = eez.postAndVerifyBatch(batch);
        call_builder
            .call()
            .await
            .map_err(|e| L1Error::Submission(format!("eth_call simulation reverted: {e}")))?;

        // Apply the gas-limit buffer (see GAS_BUFFER_NUM/DEN docs above)
        // so a contention-time heavier branch doesn't OOG mid-execution.
        let estimated = call_builder
            .estimate_gas()
            .await
            .map_err(|e| L1Error::Submission(format!("eth_estimateGas: {e}")))?;
        let gas_limit = u64::try_from(
            u128::from(estimated)
                .saturating_mul(GAS_BUFFER_NUM)
                .saturating_div(GAS_BUFFER_DEN),
        )
        .unwrap_or(u64::MAX);
        let pending = call_builder
            .gas(gas_limit)
            .send()
            .await
            .map_err(|e| L1Error::Submission(format!("send: {e}")))?;
        let tx_hash = *pending.tx_hash();

        let receipt = pending
            .with_required_confirmations(1)
            .with_timeout(Some(RECEIPT_TIMEOUT))
            .get_receipt()
            .await
            .map_err(|e| L1Error::Submission(format!("await receipt {tx_hash}: {e}")))?;
        if !receipt.status() {
            return Err(L1Error::Submission(format!(
                "tx {tx_hash} reverted on-chain (gas used {})",
                receipt.gas_used
            )));
        }
        let l1_block = receipt.block_number.ok_or_else(|| {
            L1Error::Submission(format!("receipt {tx_hash} missing block_number"))
        })?;

        Ok(SendOutcome { tx_hash, l1_block })
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
        let filter = Filter::new()
            .address(self.inner.config.eez)
            .event_signature(EezRegistry::BatchPosted::SIGNATURE_HASH)
            .from_block(deploy_block)
            .to_block(BlockNumberOrTag::Latest);
        let logs = provider
            .get_logs(&filter)
            .await
            .map_err(|e| L1Error::Provider(format!("get_logs(BatchPosted): {e}")))?;

        // Winner filter: same logic as L1Watcher::scan_batch_posted —
        // `L2ExecutionPerformed` only fires when the state delta
        // actually applied. Losers emit `BatchPosted` only.
        let winners_filter = Filter::new()
            .address(self.inner.config.eez)
            .event_signature(EezRegistry::L2ExecutionPerformed::SIGNATURE_HASH)
            .topic1(alloy_primitives::U256::from(self.inner.config.rollup_id))
            .from_block(deploy_block)
            .to_block(BlockNumberOrTag::Latest);
        let winner_logs = provider
            .get_logs(&winners_filter)
            .await
            .map_err(|e| L1Error::Provider(format!("get_logs(L2ExecutionPerformed): {e}")))?;
        let winner_tx_hashes: std::collections::HashSet<alloy_primitives::B256> = winner_logs
            .iter()
            .filter_map(|l| l.transaction_hash)
            .collect();

        let mut out: Vec<HistoricalBatch> = Vec::with_capacity(logs.len());
        for log in &logs {
            let l1_block_number = log
                .block_number
                .ok_or_else(|| L1Error::Provider("BatchPosted log missing block_number".into()))?;
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
            let our_delta = our_state_delta(&decoded.batch, self.inner.config.rollup_id);
            let claimed_current_state = our_delta.map(|d| d.currentState);
            let claimed_new_state = our_delta.map(|d| d.newState);
            let state_applied = winner_tx_hashes.contains(&tx_hash);
            out.push(HistoricalBatch {
                l1_block_number,
                tx_hash,
                submitter,
                call_data: decoded.batch.callData,
                state_applied,
                claimed_current_state,
                claimed_new_state,
            });
        }
        Ok(out)
    }
}

/// One past `BatchPosted` event, with enough context for the Deriver
/// to replay (or skip) it during catch-up.
#[derive(Debug, Clone)]
pub struct HistoricalBatch {
    pub l1_block_number: u64,
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
        let wallet = EthereumWallet::from(self.config.poster.clone());
        ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(self.config.rpc_url.clone())
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
    let rid = alloy_primitives::U256::from(rollup_id);
    batch
        .entries
        .iter()
        .flat_map(|entry| entry.stateDeltas.iter())
        .find(|delta| delta.rollupId == rid)
}
