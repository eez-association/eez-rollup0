//! Stage-2 Submitter: posts L2 batches to `EEZ.postVerifyAndExecuteOrSaveExecutionsFromBatch`.
//!
//! Construction:
//!
//! 1. Walk past `BatchPosted` logs from `EEZ_REGISTRY_DEPLOY_BLOCK` to L1
//!    head. Decode each tx's `callData` via [`eez_payload_codec::decode`]
//!    and sum `block_tx_counts.len()` across all batches. That sum is the
//!    L2 block height the contract has already accepted.
//! 2. Read the local L2 best block.
//! 3. If local < on-chain: refuse to start ([`L1Error::L2Behind`]).
//! 4. Otherwise: seed `posted_through = on_chain_head`. The next tick
//!    posts the gap between on-chain and local.
//!
//! Per-tick:
//!
//! - Read the local L2 best block.
//! - If `local > posted_through`: collect raw txs for blocks
//!   `(posted_through + 1 ..= local)`, encode the §8.1 payload, build the
//!   batch with our payload in `callData` (every cross-chain slot empty
//!   for stage 2), hand it to [`Prover::prove`], submit
//!   `postVerifyAndExecuteOrSaveExecutionsFromBatch`, wait for one
//!   confirmation. On success: `posted_through = local`.
//!
//! After startup `posted_through` lives in memory only — no on-chain
//! re-scan per tick. Stage 3's Deriver will own the L1-event-driven
//! catch-up + reorg path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_eips::{BlockNumberOrTag, Encodable2718};
use alloy_network::EthereumWallet;
use alloy_primitives::{B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::{Filter, TransactionTrait};
use alloy_sol_types::{SolCall, SolEvent};
use eez_prover::{
    EezRegistry, ProofSystemBatchPerVerificationEntries, Prover, ProvingContext,
    RollupIdWithProofSystems,
};
use reth_primitives_traits::{Block, BlockBody};
use reth_storage_api::{BlockReader, TransactionsProvider};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};

use crate::config::SubmitterConfig;
use crate::error::{L1Error, L1Result};

/// Upper bound on how long we wait for a postBatch tx to confirm.
///
/// Stage-2 heuristic — picked to be > a few L1 blocks on chiado /
/// mainnet so a normally-priced tx has a fair chance to land, but
/// less than the typical submitter interval so failed cycles don't
/// stack. Tuning this is a known imprecise dial; **stage 4 makes it
/// moot** by switching to bundler-routed submission (see plan doc
/// §5.4.2), where the success criterion becomes "`BatchPosted` event
/// observed at the target L1 block we asked the relay to include
/// us in" — no time-based timeout at all.
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(45);

/// Stage-2 Submitter task.
pub struct Submitter<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

struct Inner<L2: BlockReader> {
    config: SubmitterConfig,
    prover: Arc<dyn Prover>,
    l2_provider: Arc<L2>,
    /// Last L2 block we know the contract has accepted. Seeded from the
    /// on-chain log scan at startup; updated locally after each
    /// successful submission.
    posted_through: AtomicU64,
}

impl<L2: BlockReader> std::fmt::Debug for Submitter<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Submitter")
            .field("eez", &self.inner.config.eez)
            .field("rollup_id", &self.inner.config.rollup_id)
            .field("interval", &self.inner.config.interval)
            .field(
                "posted_through",
                &self.inner.posted_through.load(Ordering::Acquire),
            )
            .field("prover", &self.inner.prover)
            .finish()
    }
}

impl<L2> Submitter<L2>
where
    L2: BlockReader + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Constructs a Submitter by reconciling on-chain and local L2 heads.
    ///
    /// # Errors
    ///
    /// - [`L1Error::L2Source`]: local L2 provider lookup failed.
    /// - [`L1Error::Provider`]: L1 RPC call (log scan / tx fetch) failed.
    /// - [`L1Error::Codec`]: a past batch's payload couldn't be decoded.
    /// - [`L1Error::L2Behind`]: local L2 head is below what the contract
    ///   has already accepted — operator must reconcile before retrying.
    pub async fn new(
        config: SubmitterConfig,
        prover: Arc<dyn Prover>,
        l2_provider: Arc<L2>,
    ) -> L1Result<Self> {
        let wallet = EthereumWallet::from(config.poster.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(config.rpc_url.clone());

        let on_chain_head = scan_on_chain_head(&provider, config.eez, config.deploy_block).await?;
        let local_head = l2_provider
            .best_block_number()
            .map_err(|e| L1Error::L2Source(e.to_string()))?;

        if local_head < on_chain_head {
            return Err(L1Error::L2Behind {
                local: local_head,
                on_chain: on_chain_head,
            });
        }

        Ok(Self {
            inner: Arc::new(Inner {
                config,
                prover,
                l2_provider,
                posted_through: AtomicU64::new(on_chain_head),
            }),
        })
    }

    /// Run loop. One tick → at most one postBatch tx.
    pub async fn run(self) {
        let interval = self.inner.config.interval;
        let mut ticker = interval_at(Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        event!(
            name: "eez.submitter.started",
            Level::INFO,
            eez = %self.inner.config.eez,
            rollup_id = self.inner.config.rollup_id,
            poster = %self.inner.config.poster.address(),
            interval_secs = self.inner.config.interval.as_secs(),
            posted_through = self.inner.posted_through.load(Ordering::Acquire),
            "submitter loop started",
        );

        loop {
            ticker.tick().await;
            match self.inner.try_post_one_batch().await {
                Ok(Outcome::NothingToDo { local, posted }) => event!(
                    name: "eez.submitter.idle",
                    Level::DEBUG,
                    local_head = local,
                    posted_through = posted,
                    "no new L2 blocks to post",
                ),
                Ok(Outcome::Posted { from, to, tx_hash }) => event!(
                    name: "eez.submitter.batch.posted",
                    Level::INFO,
                    from, to, tx_hash = %tx_hash,
                    "posted batch [{{from}}, {{to}}]",
                ),
                Err(err) => event!(
                    name: "eez.submitter.cycle.failed",
                    Level::WARN,
                    error = %err,
                    "submission cycle failed; will retry next tick",
                ),
            }
        }
    }
}

#[derive(Debug)]
enum Outcome {
    NothingToDo {
        local: u64,
        posted: u64,
    },
    Posted {
        from: u64,
        to: u64,
        tx_hash: alloy_primitives::TxHash,
    },
}

impl<L2> Inner<L2>
where
    L2: BlockReader + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    async fn try_post_one_batch(&self) -> L1Result<Outcome> {
        let posted = self.posted_through.load(Ordering::Acquire);
        let local = self
            .l2_provider
            .best_block_number()
            .map_err(|e| L1Error::L2Source(e.to_string()))?;

        if local <= posted {
            return Ok(Outcome::NothingToDo { local, posted });
        }

        let from = posted + 1;
        let to = local;
        let capacity = usize::try_from(to - from + 1).unwrap_or(0);
        let mut blocks: Vec<Vec<Vec<u8>>> = Vec::with_capacity(capacity);
        for n in from..=to {
            let block = self
                .l2_provider
                .block_by_number(n)
                .map_err(|e| L1Error::L2Source(e.to_string()))?
                .ok_or_else(|| L1Error::L2Source(format!("L2 block {n} missing")))?;
            blocks.push(
                block
                    .body()
                    .transactions()
                    .iter()
                    .map(Encodable2718::encoded_2718)
                    .collect(),
            );
        }

        let payload = eez_payload_codec::encode(&blocks)?;
        let batch = self.build_batch(payload);

        let ctx = ProvingContext {
            batch: &batch,
            rollup_id: self.config.rollup_id,
        };
        let proof = self
            .prover
            .prove(ctx)
            .await
            .map_err(|e| L1Error::Prover(e.to_string()))?;
        let batch = ProofSystemBatchPerVerificationEntries {
            proofs: vec![proof],
            ..batch
        };

        let wallet = EthereumWallet::from(self.config.poster.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(self.config.rpc_url.clone());
        let eez = EezRegistry::new(self.config.eez, &provider);

        // Pre-simulate via eth_call to surface a typed revert before we
        // ever broadcast — saves gas on bugs and prevents stuck pending
        // txs from misformed batches.
        let call_builder = eez.postVerifyAndExecuteOrSaveExecutionsFromBatch(batch);
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

        self.posted_through.store(to, Ordering::Release);
        Ok(Outcome::Posted { from, to, tx_hash })
    }

    /// Build the stage-2 batch — our L2 payload in `callData`, every
    /// cross-chain slot empty. `proofs` is filled in by the caller after
    /// invoking the prover.
    fn build_batch(&self, payload: Vec<u8>) -> ProofSystemBatchPerVerificationEntries {
        ProofSystemBatchPerVerificationEntries {
            entries: Vec::new(),
            l1ToL2lookupCalls: Vec::new(),
            transientExecutionEntryCount: U256::ZERO,
            transientLookupCallCount: U256::ZERO,
            proofSystems: vec![self.config.proof_system],
            rollupIdsWithProofSystems: vec![RollupIdWithProofSystems {
                rollupId: U256::from(self.config.rollup_id),
                proofSystemIndex: vec![0],
            }],
            crossProofSystemInteractions: B256::ZERO,
            blobIndices: Vec::new(),
            callData: Bytes::from(payload),
            proofs: Vec::new(),
        }
    }
}

/// Walk every past `BatchPosted` event from `deploy_block` to L1 head;
/// decode each tx's `callData` via the codec; sum `block_tx_counts.len()`
/// across all of them. Returns the L2 block height the contract has
/// accepted so far. Returns 0 when no batch has landed yet.
///
/// Called once at startup; not on the per-tick hot path.
async fn scan_on_chain_head(
    provider: &impl Provider,
    eez: alloy_primitives::Address,
    deploy_block: u64,
) -> L1Result<u64> {
    let filter = Filter::new()
        .address(eez)
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
        let decoded =
            EezRegistry::postVerifyAndExecuteOrSaveExecutionsFromBatchCall::abi_decode(input)
                .map_err(|e| L1Error::Provider(format!("decode postBatch({tx_hash}): {e}")))?;
        let payload = decoded.batch.callData.as_ref();
        let batch = eez_payload_codec::decode(payload)?;
        head += batch.block_count() as u64;
    }
    Ok(head)
}
