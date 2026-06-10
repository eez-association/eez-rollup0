//! Composer: detects the gap between local L2 head and on-chain
//! `posted_through`, builds the batch payload, asks the [`Prover`]
//! for a proof, and hands the assembled batch to the [`Submitter`].
//!
//! `posted_through` is driven by [`L1Watcher`] events (own + external
//! batches go through the same code path). Per tick:
//! `from = posted_through + 1`,
//! `to = min(local, posted_through + MAX_BLOCKS_PER_BATCH)`,
//! encode payload, prove, submit. The `expect_external_batches` flag
//! only changes the log level on observed external batches (info vs error).

use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{B256, Bytes, U256};
use eez_prover::{
    ExecutionEntry, ProofSystemBatchPerVerificationEntries, Prover, ProvingContext,
    RollupIdWithProofSystems, StateDelta,
};
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{BlockReader, TransactionsProvider};
use tokio::sync::broadcast;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{Level, event};

use crate::config::ComposerConfig;
use crate::error::{L1Error, L1Result};
use crate::l1_canonical_head::L1CanonicalHead;
use crate::l1_watcher::{L1Event, L1Watcher};
use crate::submitter::{BundleTarget, SendOutcome, Submitter};

/// Maximum L2 block count any single postBatch tx may cover.
///
/// Bounds L1 calldata gas per submission so a long stall doesn't
/// produce an unbounded batch when the Composer finally catches up.
const MAX_BLOCKS_PER_BATCH: u64 = 300;

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
    l1_watcher: L1Watcher,
    /// Shared L1-confirmed L2 head. Composer is a read-only consumer
    /// (uses `cursor()` to compute the next batch's `from_block`); the
    /// Deriver writes on every `L1Event::BatchPosted / Reorg / Finalized`.
    l1_head: Arc<L1CanonicalHead>,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_id", &self.inner.config.rollup_id)
            .field("interval", &self.inner.config.interval)
            .field("posted_through", &self.inner.l1_head.cursor())
            .field("prover", &self.inner.prover)
            .field("submitter", &self.inner.submitter)
            .field("l1_watcher", &self.inner.l1_watcher)
            .finish()
    }
}

impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Constructs a Composer that reads its L1-confirmed L2 head from
    /// the shared [`L1CanonicalHead`]. The Deriver is the sole
    /// writer of that state; this Composer never owns the cursor.
    ///
    /// Synchronous — does no I/O.
    #[must_use]
    pub fn new(
        config: ComposerConfig,
        prover: Arc<dyn Prover>,
        l2_provider: Arc<L2>,
        submitter: Submitter,
        l1_watcher: L1Watcher,
        l1_head: Arc<L1CanonicalHead>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                prover,
                l2_provider,
                submitter,
                l1_watcher,
                l1_head,
            }),
        }
    }

    /// Run loop. Two event sources via `tokio::select!`:
    ///
    /// - Interval ticker: try to compose + submit one batch.
    /// - `L1Watcher` broadcast: advance `posted_through` from any
    ///   landed `BatchPosted` event (ours or external).
    pub async fn run(self) {
        let interval = self.inner.config.interval;
        let mut ticker = interval_at(Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut l1_events = self.inner.l1_watcher.subscribe();
        let our_address = self.inner.config_poster_address();

        event!(
            name: "eez.composer.started",
            Level::INFO,
            rollup_id = self.inner.config.rollup_id,
            interval_secs = self.inner.config.interval.as_secs(),
            posted_through = self.inner.l1_head.cursor(),
            expect_external_batches = self.inner.config.expect_external_batches,
            our_address = %our_address,
            "composer loop started",
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match self.inner.try_compose_one_batch().await {
                        Ok(Outcome::NothingToDo { local, posted }) => event!(
                            name: "eez.composer.idle",
                            Level::DEBUG,
                            local_head = local,
                            posted_through = posted,
                            "no new L2 blocks to post",
                        ),
                        Ok(Outcome::Posted { from, to, tx_hash, l1_block, state_applied }) => event!(
                            name: "eez.composer.batch.posted",
                            Level::INFO,
                            from, to,
                            tx_hash = %tx_hash,
                            l1_block,
                            state_applied,
                            "posted batch [{{from}}, {{to}}] in L1 block {{l1_block}}",
                        ),
                        Ok(Outcome::BundleDropped { from, to, tx_hash, target_block }) => event!(
                            name: "eez.composer.batch.bundle_dropped",
                            Level::WARN,
                            from, to,
                            tx_hash = %tx_hash,
                            target_block,
                            "bundle missed target block; will rebuild + retry next tick",
                        ),
                        Ok(Outcome::CursorRaced { from, to, expected_cursor, actual_cursor }) => event!(
                            name: "eez.composer.batch.cursor_raced",
                            Level::INFO,
                            from, to,
                            expected_cursor,
                            actual_cursor,
                            "cursor advanced under our batch build (peer's batch landed first); aborting submission, will rebuild next tick",
                        ),
                        Err(err) => event!(
                            name: "eez.composer.cycle.failed",
                            Level::WARN,
                            error = %err,
                            "compose cycle failed; will retry next tick",
                        ),
                    }
                }
                event = l1_events.recv() => {
                    self.inner.on_l1_event(&event, our_address);
                }
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
        state_applied: bool,
    },
    /// Target L1 block was produced without our tx in it; next tick
    /// re-reads the cursor and rebuilds with a fresh target + nonce.
    BundleDropped {
        from: u64,
        to: u64,
        tx_hash: alloy_primitives::TxHash,
        target_block: u64,
    },
    /// Our cursor advanced between batch-build and submit (a peer's
    /// batch landed and the Deriver indexed it). Our batch's payload
    /// is now stale w.r.t. `from_block`; abort without submitting.
    CursorRaced {
        from: u64,
        to: u64,
        expected_cursor: u64,
        actual_cursor: u64,
    },
}

impl<L2> Inner<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    fn config_poster_address(&self) -> alloy_primitives::Address {
        // The poster lives in SubmitterConfig (held by Submitter), not
        // ComposerConfig — derive from the prover/submitter the same
        // way deploy.sh derives it on the CLI side.
        self.submitter.poster_address()
    }

    async fn try_compose_one_batch(&self) -> L1Result<Outcome> {
        // Read the L1-confirmed cursor through the shared L1CanonicalHead.
        // Deriver is the sole writer; advances + retreats are visible
        // here immediately.
        let posted = self.l1_head.cursor();
        let local = self
            .l2_provider
            .best_block_number()
            .map_err(|e| L1Error::L2Source(e.to_string()))?;

        if local <= posted {
            return Ok(Outcome::NothingToDo { local, posted });
        }

        // Cap the batch's L2-block span at MAX_BLOCKS_PER_BATCH so a
        // long stall doesn't produce an unbounded postBatch tx. The
        // remainder lands on subsequent ticks.
        let from = posted + 1;
        let to = local.min(posted + MAX_BLOCKS_PER_BATCH);
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
        let batch = self.build_batch(posted, to, payload)?;

        let proof = self
            .prover
            .prove(ProvingContext)
            .await
            .map_err(|e| L1Error::Prover(e.to_string()))?;
        let batch = ProofSystemBatchPerVerificationEntries {
            proofs: vec![proof],
            ..batch
        };

        // Stale-cursor guard: payload doesn't encode `from_block`, so
        // if a peer's batch landed while we were building, our payload
        // would replay at the wrong L2 range. Abort and rebuild next tick.
        let cursor_now = self.l1_head.cursor();
        if cursor_now != posted {
            return Ok(Outcome::CursorRaced {
                from,
                to,
                expected_cursor: posted,
                actual_cursor: cursor_now,
            });
        }

        // posted_through advance happens via the L1Event::BatchPosted
        // subscriber so external + own batches share one code path.
        match self.submitter.send(batch, BundleTarget::NextBlock).await? {
            SendOutcome::Included {
                tx_hash,
                l1_block,
                state_applied,
            } => Ok(Outcome::Posted {
                from,
                to,
                tx_hash,
                l1_block,
                state_applied,
            }),
            SendOutcome::Dropped {
                tx_hash,
                target_block,
            } => Ok(Outcome::BundleDropped {
                from,
                to,
                tx_hash,
                target_block,
            }),
        }
    }

    /// Diagnostic-only handler for L1 events. State (cursor + reorg
    /// retreats) lives in the shared `L1CanonicalHead` written by the
    /// Deriver; here we just log own-vs-external batch attribution +
    /// log the `expect_external_batches=false` violation when in
    /// sequenced mode.
    fn on_l1_event(
        &self,
        event: &Result<L1Event, broadcast::error::RecvError>,
        our_address: alloy_primitives::Address,
    ) {
        match event {
            Ok(L1Event::BatchPosted {
                l1_block_number,
                tx_hash,
                submitter,
                ..
            }) => {
                let is_ours = *submitter == our_address;
                let cursor = self.l1_head.cursor();
                if is_ours {
                    event!(
                        name: "eez.composer.batch.confirmed",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        posted_through = cursor,
                        "our batch landed on L1",
                    );
                } else if self.config.expect_external_batches {
                    event!(
                        name: "eez.composer.batch.external",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        submitter = %submitter,
                        posted_through = cursor,
                        "external batch landed (based mode)",
                    );
                } else {
                    event!(
                        name: "eez.composer.batch.external.unexpected",
                        Level::ERROR,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        submitter = %submitter,
                        "external batch landed in sequenced-mode rollup — someone else is sequencing our chain",
                    );
                }
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                event!(
                    name: "eez.composer.l1_events.lagged",
                    Level::WARN,
                    skipped,
                    "L1 event stream lagged",
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                event!(
                    name: "eez.composer.l1_events.closed",
                    Level::ERROR,
                    "L1 event stream closed",
                );
            }
        }
    }

    /// Build the batch — one `ExecutionEntry` carrying a single
    /// `StateDelta` for our rollup, the L2 payload in `callData`,
    /// every cross-chain slot empty. `proofs` is filled in by the
    /// caller after invoking the prover.
    ///
    /// `current_state` is read from local reth at
    /// `posted_through_before_this_batch` — by the protocol invariant,
    /// that L2 state root matches what L1 stored at the last
    /// successful `postAndVerifyBatch`. If it doesn't, the on-chain
    /// `StateRootMismatch` revert is the loud-failure signal that
    /// catch-up or reorg-walkback has drifted.
    fn build_batch(
        &self,
        posted_through_before: u64,
        to_block: u64,
        payload: Vec<u8>,
    ) -> L1Result<ProofSystemBatchPerVerificationEntries> {
        let current_state = self.l2_state_root(posted_through_before)?;
        let new_state = self.l2_state_root(to_block)?;
        let rollup_id_u256 = U256::from(self.config.rollup_id);

        let entry = ExecutionEntry {
            stateDeltas: vec![StateDelta {
                rollupId: rollup_id_u256,
                currentState: current_state,
                newState: new_state,
                etherDelta: alloy_primitives::I256::ZERO,
            }],
            proxyEntryHash: B256::ZERO,
            destinationRollupId: rollup_id_u256,
            L2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            callCount: U256::ZERO,
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        };

        // `transientExecutionEntryCount = 1` so our pure-L2 entry runs
        // inline in step 5 of postAndVerifyBatch — the only path that
        // applies state deltas (EEZ.sol:298-300). With 0 it'd queue + never run.
        Ok(ProofSystemBatchPerVerificationEntries {
            entries: vec![entry],
            l1ToL2lookupCalls: Vec::new(),
            transientExecutionEntryCount: U256::from(1),
            transientLookupCallCount: U256::ZERO,
            proofSystems: vec![self.config.proof_system],
            rollupIdsWithProofSystems: vec![RollupIdWithProofSystems {
                rollupId: rollup_id_u256,
                proofSystemIndex: vec![0],
            }],
            crossProofSystemInteractions: B256::ZERO,
            blobIndices: Vec::new(),
            callData: Bytes::from(payload),
            proofs: Vec::new(),
        })
    }

    /// Read the post-state L2 state root at the given block number.
    fn l2_state_root(&self, block_number: u64) -> L1Result<B256> {
        let header = self
            .l2_provider
            .sealed_header(block_number)
            .map_err(|e| L1Error::L2Source(format!("sealed_header({block_number}): {e}")))?
            .ok_or_else(|| L1Error::L2Source(format!("L2 header at {block_number} missing")))?;
        Ok(header.state_root())
    }
}
