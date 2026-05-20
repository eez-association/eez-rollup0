//! Stage-2 Composer: detects the gap between local L2 head and on-chain
//! `posted_through`, builds the batch payload, asks the [`Prover`] for
//! a proof, and hands the assembled batch to the [`Submitter`].
//!
//! Construction:
//!
//! 1. Ask the Submitter to scan past `BatchPosted` logs from
//!    `EEZ_REGISTRY_DEPLOY_BLOCK` to L1 head. The total L2 block count
//!    across all decoded batches is the head the contract has accepted.
//! 2. Read the local L2 best block.
//! 3. If local < on-chain: refuse to start ([`L1Error::L2Behind`]).
//! 4. Otherwise: seed `posted_through = on_chain_head`. The next tick
//!    posts the gap between on-chain and local.
//!
//! Per tick:
//!
//! - Read the local L2 best block.
//! - If `local > posted_through`: collect raw txs for blocks
//!   `(posted_through + 1 ..= local)`, encode the §8.1 payload, build the
//!   stage-2 batch (every cross-chain slot empty), hand it to
//!   [`Prover::prove`], pack the proof into the batch, and submit via
//!   [`Submitter::send`]. On success: `posted_through = local`.
//!
//! After startup `posted_through` lives in memory only — no on-chain
//! re-scan per tick. Stage 3's Deriver will own the L1-event-driven
//! catch-up + reorg path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_eips::Encodable2718;
use alloy_primitives::{B256, Bytes, U256};
use eez_prover::{
    ProofSystemBatchPerVerificationEntries, Prover, ProvingContext, RollupIdWithProofSystems,
};
use reth_primitives_traits::{Block, BlockBody};
use reth_storage_api::{BlockReader, TransactionsProvider};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};

use crate::config::ComposerConfig;
use crate::error::{L1Error, L1Result};
use crate::submitter::{SendOutcome, Submitter};

/// Stage-2 Composer task. Cheaply [`Clone`]able.
#[derive(Clone)]
pub struct Composer<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

struct Inner<L2: BlockReader> {
    config: ComposerConfig,
    prover: Arc<dyn Prover>,
    l2_provider: Arc<L2>,
    submitter: Submitter,
    /// Last L2 block we know the contract has accepted. Seeded from the
    /// on-chain log scan at startup; advanced after each successful
    /// [`Submitter::send`].
    posted_through: AtomicU64,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_id", &self.inner.config.rollup_id)
            .field("interval", &self.inner.config.interval)
            .field(
                "posted_through",
                &self.inner.posted_through.load(Ordering::Acquire),
            )
            .field("prover", &self.inner.prover)
            .field("submitter", &self.inner.submitter)
            .finish()
    }
}

impl<L2> Composer<L2>
where
    L2: BlockReader + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Construct a Composer, reconciling on-chain and local L2 heads.
    ///
    /// # Errors
    ///
    /// - [`L1Error::L2Source`]: local L2 provider lookup failed.
    /// - [`L1Error::Provider`]: L1 RPC call (log scan / tx fetch) failed.
    /// - [`L1Error::Codec`]: a past batch's payload couldn't be decoded.
    /// - [`L1Error::L2Behind`]: local L2 head is below what the contract
    ///   has already accepted — operator must reconcile before retrying.
    pub async fn new(
        config: ComposerConfig,
        prover: Arc<dyn Prover>,
        l2_provider: Arc<L2>,
        submitter: Submitter,
    ) -> L1Result<Self> {
        let on_chain_head = submitter.scan_on_chain_head(config.deploy_block).await?;
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
                submitter,
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
            name: "eez.composer.started",
            Level::INFO,
            rollup_id = self.inner.config.rollup_id,
            interval_secs = self.inner.config.interval.as_secs(),
            posted_through = self.inner.posted_through.load(Ordering::Acquire),
            "composer loop started",
        );

        loop {
            ticker.tick().await;
            match self.inner.try_compose_one_batch().await {
                Ok(Outcome::NothingToDo { local, posted }) => event!(
                    name: "eez.composer.idle",
                    Level::DEBUG,
                    local_head = local,
                    posted_through = posted,
                    "no new L2 blocks to post",
                ),
                Ok(Outcome::Posted {
                    from,
                    to,
                    tx_hash,
                    l1_block,
                }) => event!(
                    name: "eez.composer.batch.posted",
                    Level::INFO,
                    from,
                    to,
                    tx_hash = %tx_hash,
                    l1_block,
                    "posted batch [{{from}}, {{to}}] in L1 block {{l1_block}}",
                ),
                Err(err) => event!(
                    name: "eez.composer.cycle.failed",
                    Level::WARN,
                    error = %err,
                    "compose cycle failed; will retry next tick",
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
        l1_block: u64,
    },
}

impl<L2> Inner<L2>
where
    L2: BlockReader + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    async fn try_compose_one_batch(&self) -> L1Result<Outcome> {
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

        let proof = self
            .prover
            .prove(ProvingContext)
            .await
            .map_err(|e| L1Error::Prover(e.to_string()))?;
        let batch = ProofSystemBatchPerVerificationEntries {
            proofs: vec![proof],
            ..batch
        };

        let SendOutcome { tx_hash, l1_block } = self.submitter.send(batch).await?;
        self.posted_through.store(to, Ordering::Release);
        Ok(Outcome::Posted {
            from,
            to,
            tx_hash,
            l1_block,
        })
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
