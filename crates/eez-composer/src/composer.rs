//! [`Composer`]: the umbrella that owns the per-rollup produce →
//! prove → submit loop.
//!
//! On each [`BatchCandidate`] arrival, the Composer:
//!
//! 1. Routes by `candidate.rollup_id` to the relevant
//!    [`RollupState`](crate::RollupState).
//! 2. Reads that rollup's L1-confirmed cursor + clamps the upper
//!    bound: `from = cursor + 1`, `to = min(candidate.to_block,
//!    cursor + MAX_BLOCKS_PER_BATCH)`.
//! 3. Encodes the user-tx payload from that rollup's local reth.
//! 4. Calls the shared [`Prover`] for a proof binding the range.
//! 5. Hands the assembled batch to the shared
//!    [`Submitter`](eez_l1::Submitter), which sends via the bundle relay.
//!
//! Coalesce-on-drain: between `recv` and `compose`, drains any other
//! queued candidates non-blockingly and uses the latest's `to_block`.
//! During catchup this turns N queued candidates into one submission
//! instead of N (with N-1 `NothingToDo` cycles); steady-state is a no-op.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{B256, Bytes, U256};
use eez_driver::BatchCandidate;
use eez_l1::{BundleTarget, L1Error, L1Event, L1Result, L1Watcher, SendOutcome, Submitter};
use eez_prover::{
    ExecutionEntry, ProofSystemBatchPerVerificationEntries, Prover, ProvingContext,
    RollupIdWithProofSystems, StateDelta,
};
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{BlockReader, TransactionsProvider};
use tokio::sync::{broadcast, mpsc};
use tracing::{Level, event};

use crate::rollup::RollupState;

/// Maximum L2 block count any single postBatch tx may cover.
///
/// Bounds L1 calldata gas per submission so a long stall doesn't
/// produce an unbounded batch when the Composer finally catches up.
/// Aligned with `MAX_BLOCKS_PER_CATCHUP` on the Sequencer side so one
/// catchup trigger's output fits in one submission.
const MAX_BLOCKS_PER_BATCH: u64 = 300;

/// Composer umbrella. Cheaply [`Clone`]able (`Arc<Inner>`).
#[derive(Clone)]
pub struct Composer<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

struct Inner<L2: BlockReader> {
    /// Per-rollup state. Single entry in S4.2; N entries in stage-N
    /// multi-L2. `HashMap` from day one to keep the routing shape
    /// future-proof.
    rollups: HashMap<u64, RollupState<L2>>,
    /// Shared across rollups: one prover, one submitter, one `L1Watcher`.
    prover: Arc<dyn Prover>,
    submitter: Submitter,
    l1_watcher: L1Watcher,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_ids", &self.inner.rollups.keys().collect::<Vec<_>>())
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
    /// Constructs the umbrella. Synchronous — does no I/O.
    #[must_use]
    pub fn new(
        rollups: HashMap<u64, RollupState<L2>>,
        prover: Arc<dyn Prover>,
        submitter: Submitter,
        l1_watcher: L1Watcher,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                rollups,
                prover,
                submitter,
                l1_watcher,
            }),
        }
    }

    /// Run loop. Two event sources via `tokio::select!`:
    ///
    /// - `batch_rx`: Sequencer-driven [`BatchCandidate`] arrivals
    ///   (drained coalesce-on-recv, dispatched by `rollup_id`).
    /// - `L1Watcher` broadcast: log own/external `BatchPosted` events
    ///   (cursor itself is owned by the Deriver via shared
    ///   `L1CanonicalHead`).
    ///
    /// When `batch_rx` closes, the upstream Sequencer task has exited;
    /// the run loop exits.
    pub async fn run(self, mut batch_rx: mpsc::Receiver<BatchCandidate>) {
        let mut l1_events = self.inner.l1_watcher.subscribe();
        let our_address = self.inner.submitter.poster_address();

        event!(
            name: "eez.composer.started",
            Level::INFO,
            rollup_count = self.inner.rollups.len(),
            our_address = %our_address,
            "composer umbrella loop started",
        );

        loop {
            tokio::select! {
                maybe_candidate = batch_rx.recv() => {
                    let Some(mut latest) = maybe_candidate else {
                        event!(
                            name: "eez.composer.batch_rx.closed",
                            Level::ERROR,
                            "batch candidate channel closed; composer task exiting",
                        );
                        break;
                    };
                    // Coalesce drain: collapse pending candidates per
                    // rollup_id, keep the highest `to_block` per rollup.
                    // S4.2 single-rollup pass-through: just take the
                    // latest. Multi-rollup needs per-rollup latest;
                    // simple `latest = next` works only because there's
                    // one rollup today.
                    while let Ok(next) = batch_rx.try_recv() {
                        latest = next;
                    }
                    self.inner.handle_candidate(&latest).await;
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
        candidate_to: u64,
        posted: u64,
    },
    Posted {
        from: u64,
        to: u64,
        tx_hash: alloy_primitives::TxHash,
        l1_block: u64,
        state_applied: bool,
    },
    /// Target L1 block was produced without our tx in it; next
    /// candidate re-reads the cursor and rebuilds with fresh nonce.
    BundleDropped {
        from: u64,
        to: u64,
        tx_hash: alloy_primitives::TxHash,
        target_block: u64,
    },
    /// Cursor advanced between batch-build and submit (a peer's batch
    /// landed and the Deriver indexed it). Our batch's payload is
    /// stale; abort without submitting.
    CursorRaced {
        from: u64,
        to: u64,
        expected_cursor: u64,
        actual_cursor: u64,
    },
    /// Candidate carried a `rollup_id` we don't manage. Should never
    /// happen in well-wired setups; logged at ERROR for visibility.
    UnknownRollup {
        rollup_id: u64,
    },
}

impl<L2> Inner<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    async fn handle_candidate(&self, candidate: &BatchCandidate) {
        match self.try_compose_one_batch(candidate).await {
            Ok(Outcome::NothingToDo {
                candidate_to,
                posted,
            }) => event!(
                name: "eez.composer.idle",
                Level::DEBUG,
                rollup_id = candidate.rollup_id,
                candidate.to = candidate_to,
                posted_through = posted,
                "candidate already covered by posted cursor; no new L2 blocks to post",
            ),
            Ok(Outcome::Posted {
                from,
                to,
                tx_hash,
                l1_block,
                state_applied,
            }) => event!(
                name: "eez.composer.batch.posted",
                Level::INFO,
                rollup_id = candidate.rollup_id,
                from, to,
                tx_hash = %tx_hash,
                l1_block,
                state_applied,
                "posted batch [{{from}}, {{to}}] in L1 block {{l1_block}}",
            ),
            Ok(Outcome::BundleDropped {
                from,
                to,
                tx_hash,
                target_block,
            }) => event!(
                name: "eez.composer.batch.bundle_dropped",
                Level::WARN,
                rollup_id = candidate.rollup_id,
                from, to,
                tx_hash = %tx_hash,
                target_block,
                "bundle missed target block; will rebuild + retry next candidate",
            ),
            Ok(Outcome::CursorRaced {
                from,
                to,
                expected_cursor,
                actual_cursor,
            }) => event!(
                name: "eez.composer.batch.cursor_raced",
                Level::INFO,
                rollup_id = candidate.rollup_id,
                from, to,
                expected_cursor,
                actual_cursor,
                "cursor advanced under our batch build (peer's batch landed first); aborting submission, will rebuild next candidate",
            ),
            Ok(Outcome::UnknownRollup { rollup_id }) => event!(
                name: "eez.composer.batch.unknown_rollup",
                Level::ERROR,
                rollup_id,
                "candidate carried unknown rollup_id; no `RollupState` registered for it",
            ),
            Err(err) => event!(
                name: "eez.composer.cycle.failed",
                Level::WARN,
                rollup_id = candidate.rollup_id,
                error = %err,
                "compose cycle failed; will retry next candidate",
            ),
        }
    }

    async fn try_compose_one_batch(&self, candidate: &BatchCandidate) -> L1Result<Outcome> {
        let Some(rollup) = self.rollups.get(&candidate.rollup_id) else {
            return Ok(Outcome::UnknownRollup {
                rollup_id: candidate.rollup_id,
            });
        };

        // Read the L1-confirmed cursor through the shared L1CanonicalHead.
        // Deriver is the sole writer; advances + retreats are visible
        // here immediately.
        let posted = rollup.l1_head.cursor();

        // Authoritative upper bound is the Sequencer's `to_block` —
        // not `best_block_number()`. The cross-chain proof's
        // `ExecutionEntry[]` is sync-slot-pinned (Rollup-1 §6 + §12),
        // so the Composer cannot legitimately extend past the
        // Sequencer's chosen sync-slot terminator. In catchup
        // (Live-only, no entries), partial extension up to `to_block`
        // is permitted per Rollup-1 §13.4.23.
        if candidate.to_block <= posted {
            return Ok(Outcome::NothingToDo {
                candidate_to: candidate.to_block,
                posted,
            });
        }

        // Cap the batch's L2-block span at MAX_BLOCKS_PER_BATCH so a
        // long stall doesn't produce an unbounded postBatch tx. The
        // remainder lands on subsequent candidates.
        let from = posted + 1;
        let to = candidate.to_block.min(posted + MAX_BLOCKS_PER_BATCH);
        let capacity = usize::try_from(to - from + 1).unwrap_or(0);
        let mut blocks: Vec<Vec<Vec<u8>>> = Vec::with_capacity(capacity);
        for n in from..=to {
            let block = rollup
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
        let batch = Self::build_batch(rollup, posted, to, payload)?;

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
        // would replay at the wrong L2 range. Abort and rebuild next
        // candidate.
        let cursor_now = rollup.l1_head.cursor();
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
    /// flag the `expect_external_batches=false` violation when in
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
                if is_ours {
                    event!(
                        name: "eez.composer.batch.confirmed",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        "our batch landed on L1",
                    );
                } else {
                    // Determine log level by finding any rollup whose
                    // config expects external batches. Single-rollup
                    // S4.2 means there's only one entry; multi-rollup
                    // stage-N can refine per-rollup attribution once
                    // the BatchPosted event carries rollup_id.
                    let any_expects_external = self
                        .rollups
                        .values()
                        .any(|r| r.config.expect_external_batches);
                    if any_expects_external {
                        event!(
                            name: "eez.composer.batch.external",
                            Level::INFO,
                            l1_block_number,
                            tx_hash = %tx_hash,
                            submitter = %submitter,
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
    /// `StateDelta` for the target rollup, the L2 payload in
    /// `callData`, every cross-chain slot empty (S4.2: no cross-chain
    /// content yet; S4.4+ populates entries).
    ///
    /// `current_state` is read from local reth at
    /// `posted_through_before_this_batch` — by the protocol invariant,
    /// that L2 state root matches what L1 stored at the last
    /// successful `postAndVerifyBatch`. If it doesn't, the on-chain
    /// `StateRootMismatch` revert is the loud-failure signal that
    /// catch-up or reorg-walkback has drifted.
    fn build_batch(
        rollup: &RollupState<L2>,
        posted_through_before: u64,
        to_block: u64,
        payload: Vec<u8>,
    ) -> L1Result<ProofSystemBatchPerVerificationEntries> {
        let current_state = Self::l2_state_root(rollup, posted_through_before)?;
        let new_state = Self::l2_state_root(rollup, to_block)?;
        let rollup_id_u256 = U256::from(rollup.config.rollup_id);

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
            proofSystems: vec![rollup.config.proof_system],
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
    fn l2_state_root(rollup: &RollupState<L2>, block_number: u64) -> L1Result<B256> {
        let header = rollup
            .l2_provider
            .sealed_header(block_number)
            .map_err(|e| L1Error::L2Source(format!("sealed_header({block_number}): {e}")))?
            .ok_or_else(|| L1Error::L2Source(format!("L2 header at {block_number} missing")))?;
        Ok(header.state_root())
    }
}
