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
use eez_prover::{EezRegistry, ProofSystemBatchPerVerificationEntries};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Upper bound on how long we wait for a postBatch tx to confirm.
///
/// Stage-2 heuristic — picked to be > a few L1 blocks on chiado /
/// mainnet so a normally-priced tx has a fair chance to land, but
/// less than the typical composer interval so failed cycles don't
/// stack. Tuning this is a known imprecise dial; **stage 4 makes it
/// moot** by switching to bundler-routed submission (see plan doc
/// §5.4.2), where the success criterion becomes "`BatchPosted` event
/// observed at the target L1 block we asked the relay to include
/// us in" — no time-based timeout at all.
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(45);

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
        fail::fail_point!("submitter::send::start", |_| Err(L1Error::Submission(
            "injected failpoint: submitter::send::start".into()
        )));
        let provider = self.inner.build_provider();
        let eez = EezRegistry::new(self.inner.config.eez, &provider);

        let call_builder = eez.postAndVerifyBatch(batch);
        call_builder
            .call()
            .await
            .map_err(|e| L1Error::Submission(format!("eth_call simulation reverted: {e}")))?;

        let pending = call_builder
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

    /// Walk every past `BatchPosted` event from `deploy_block` to L1
    /// head, decode each tx's callData via [`eez_payload_codec::decode`],
    /// and return the total L2 block count the contract has accepted.
    /// Returns 0 when no batch has landed yet.
    ///
    /// Called once at composer startup; not on the per-tick hot path.
    ///
    /// # Errors
    ///
    /// - [`L1Error::Provider`] on RPC failure (log fetch, tx fetch,
    ///   abi decode).
    /// - [`L1Error::Codec`] if a past batch's payload is malformed.
    pub async fn scan_on_chain_head(&self, deploy_block: u64) -> L1Result<u64> {
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

        let mut head: u64 = 0;
        for log in &logs {
            let tx_hash = log
                .transaction_hash
                .ok_or_else(|| L1Error::Provider("BatchPosted log missing tx_hash".into()))?;
            let tx = provider
                .get_transaction_by_hash(tx_hash)
                .await
                .map_err(|e| L1Error::Provider(format!("get_tx({tx_hash}): {e}")))?
                .ok_or_else(|| L1Error::Provider(format!("tx {tx_hash} not found")))?;
            let input = tx.inner.input();
            let decoded = EezRegistry::postAndVerifyBatchCall::abi_decode(input)
                .map_err(|e| L1Error::Provider(format!("decode postBatch({tx_hash}): {e}")))?;
            let payload = decoded.batch.callData.as_ref();
            let batch = eez_payload_codec::decode(payload)?;
            head += batch.block_count() as u64;
        }
        Ok(head)
    }
}

impl Inner {
    fn build_provider(&self) -> impl Provider + use<> {
        let wallet = EthereumWallet::from(self.config.poster.clone());
        ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(self.config.rpc_url.clone())
    }
}
