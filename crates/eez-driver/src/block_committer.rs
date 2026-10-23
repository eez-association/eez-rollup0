//! Single-task actor that serializes every engine-API call to reth.
//!
//! Owns the `ConsensusEngineHandle`, the `PayloadBuilderHandle`,
//! the `last_header` watch, and the unsafe / safe / finalized hash cursors.
//! [`BlockCommitterHandle`] is a clone-cheap `mpsc::Sender` wrapper —
//! both the Sequencer and the Deriver hold clones, so all engine
//! traffic stays strictly ordered.
//!
//! Commands:
//! - `Sequence` — parent-bound Sequencer FCU+attrs → resolve payload → newPayload.
//! - `RefreshForkchoice` — bare FCU to keep reth's view alive during quiet periods.
//! - `AdvanceSafeFinalized` — Deriver pushes safe/finalized cursors forward.
//! - `AdvanceHead` — follower points unsafe head at a sequencer-served block.
//! - `Derive` — Deriver submits a pre-built `ExecutionPayload` via newPayload + head-FCU.
//!
//! Until the Deriver speaks, `safe` and `finalized` stay at the boot anchors
//! (reth's persisted safe/finalized, or genesis on a fresh chain).

use core::fmt;
use std::sync::Arc;

use alloy_primitives::B256;
use alloy_rpc_types_engine::{ForkchoiceState, ForkchoiceUpdateError, ForkchoiceUpdated};
use reth_engine_primitives::{BeaconForkChoiceUpdateError, ConsensusEngineHandle};
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{BuiltPayload, ExecutionPayload, PayloadKind, PayloadTypes};
use reth_primitives_traits::{HeaderTy, NodePrimitives, SealedHeader, SealedHeaderFor};
use reth_storage_api::{BlockIdReader, BlockReader};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{Level, event};

use crate::error::{DriverError, DriverResult};

/// Command-channel capacity. Small — both Sequencer and Deriver issue at
/// most one command per L1/L2 cycle and wait for the response; queueing
/// more than a couple at a time would indicate the engine is wedged
/// anyway.
const COMMAND_BUFFER: usize = 16;

/// Successful result of [`BlockCommitterHandle::commit_sequenced`].
pub struct CommitOutcome<T: PayloadTypes> {
    /// The newly canonical header.
    pub header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
}

impl<T: PayloadTypes> fmt::Debug for CommitOutcome<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitOutcome")
            .field("header", &self.header)
            .finish()
    }
}

/// Follower unsafe-head forkchoice result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkchoiceOutcome {
    /// Reth accepted the forkchoice as canonical.
    Valid,
    /// Reth accepted the target as a sync target and still needs to
    /// backfill/import blocks.
    Syncing,
    /// Reth rejected the forkchoice state as internally inconsistent.
    InvalidState,
}

enum CommitCommand<T: PayloadTypes> {
    Sequence {
        attrs: EthPayloadAttributes,
        response: oneshot::Sender<DriverResult<CommitOutcome<T>>>,
    },
    RefreshForkchoice {
        response: oneshot::Sender<DriverResult<()>>,
    },
    AdvanceSafeFinalized {
        safe: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        finalized: B256,
        response: oneshot::Sender<DriverResult<()>>,
    },
    AdvanceHead {
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        response: oneshot::Sender<DriverResult<ForkchoiceOutcome>>,
    },
    SyncTo {
        target: B256,
        response: oneshot::Sender<DriverResult<ForkchoiceOutcome>>,
    },
    AdoptSynced {
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        response: oneshot::Sender<DriverResult<()>>,
    },
    Derive {
        payload: <T as PayloadTypes>::ExecutionData,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        /// Emit this block to the prover witness feed. TRUE only for a PRODUCED
        /// Sync block (`commit_one_prebuilt`); FALSE for a follower / L1-reconcile
        /// re-derive, so the prover is never double-fed the same block.
        feed_witness: bool,
        response: oneshot::Sender<DriverResult<DeriveOutcome>>,
    },
    /// Roll the canonical head back to `target_header`. Used by the
    /// composer when a Sync block's bundle didn't settle on L1: per the
    /// "L1 root is the finality oracle" model that block is
    /// unfinalized and must be rebuilt.
    ReorgTo {
        target_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        response: oneshot::Sender<DriverResult<()>>,
    },
}

/// Successful result of [`BlockCommitterHandle::commit_derived`].
#[derive(Debug, Clone, Copy)]
pub struct DeriveOutcome {
    /// Hash of the block reth accepted as the new canonical head.
    pub block_hash: B256,
    /// Block number of the new canonical head.
    pub block_number: u64,
}

/// Clone-cheap handle to the spawned block-committer actor. Carries
/// a `watch` head mirror the actor writes on every head change
/// ([`Self::subscribe_head`] observes the latest) — fixes the `invalid payload attributes` race
/// (Sequencer reads the post-Derive head without `CanonStateNotification`
/// lag).
pub struct BlockCommitterHandle<T: PayloadTypes> {
    sender: mpsc::Sender<CommitCommand<T>>,
    last_header: watch::Receiver<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>>,
    /// Held by Deriver across a whole batch reconcile; blocks
    /// `commit_sequenced` so sequencer ticks can't interleave
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<T: PayloadTypes> Clone for BlockCommitterHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            last_header: self.last_header.clone(),
            reconcile_lock: Arc::clone(&self.reconcile_lock),
        }
    }
}

impl<T: PayloadTypes> fmt::Debug for BlockCommitterHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCommitterHandle")
            .finish_non_exhaustive()
    }
}

impl<T> BlockCommitterHandle<T>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes> + Send + Sync + 'static,
    <T::BuiltPayload as BuiltPayload>::Primitives: Send + Sync + 'static,
    SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Send,
{
    /// Spawn the actor. `safe_header` / `finalized_hash` seed the forkchoice
    /// anchors until the Deriver advances them; they must be durable canonical
    /// blocks (persisted safe/finalized or genesis), never the speculative
    /// head, which L1-derived replays can displace into a rejected FCU.
    #[must_use]
    pub fn spawn(
        initial_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        safe_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        finalized_hash: B256,
        to_engine: ConsensusEngineHandle<T>,
        payload_builder: PayloadBuilderHandle<T>,
        witness_sender: Option<mpsc::UnboundedSender<B256>>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(COMMAND_BUFFER);
        let initial_hash = initial_header.hash();
        let (head_tx, last_header) = watch::channel(initial_header);
        let actor = Actor {
            receiver,
            to_engine,
            payload_builder,
            head_tx,
            unsafe_head_hash: initial_hash,
            safe_header,
            finalized_hash,
            witness_sender,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            last_header,
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// [`Self::spawn`] with anchors read from `provider`: head from the best
    /// block, safe/finalized from reth's persisted forkchoice state, falling
    /// back to genesis when none is recorded yet.
    ///
    /// # Errors
    ///
    /// `provider` (lookup failure), `missing_header` (a referenced
    /// header is absent — brief startup race).
    pub fn spawn_from_provider<P>(
        provider: &P,
        to_engine: ConsensusEngineHandle<T>,
        payload_builder: PayloadBuilderHandle<T>,
        witness_sender: Option<mpsc::UnboundedSender<B256>>,
    ) -> DriverResult<Self>
    where
        <T::BuiltPayload as BuiltPayload>::Primitives: NodePrimitives,
        P: BlockReader<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>
            + BlockIdReader,
    {
        let best = provider
            .best_block_number()
            .map_err(|e| DriverError::Provider(e.to_string()))?;
        let initial_header = provider
            .sealed_header(best)
            .map_err(|e| DriverError::Provider(e.to_string()))?
            .ok_or(DriverError::MissingHeader { block_number: best })?;
        let genesis =
            || -> DriverResult<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>> {
                provider
                    .sealed_header(0)
                    .map_err(|e| DriverError::Provider(e.to_string()))?
                    .ok_or(DriverError::MissingHeader { block_number: 0 })
            };
        let safe_header = match provider
            .safe_block_num_hash()
            .map_err(|e| DriverError::Provider(e.to_string()))?
        {
            Some(safe) => provider
                .header(safe.hash)
                .map_err(|e| DriverError::Provider(e.to_string()))?
                .map(|header| SealedHeader::new(header, safe.hash))
                .ok_or(DriverError::MissingHeader {
                    block_number: safe.number,
                })?,
            None => genesis()?,
        };
        let finalized_hash = match provider
            .finalized_block_num_hash()
            .map_err(|e| DriverError::Provider(e.to_string()))?
        {
            Some(finalized) => finalized.hash,
            None => genesis()?.hash(),
        };
        Ok(Self::spawn(
            initial_header,
            safe_header,
            finalized_hash,
            to_engine,
            payload_builder,
            witness_sender,
        ))
    }

    /// Hold across a multi-block reconcile to suppress concurrent `commit_sequenced`
    pub async fn begin_reconcile(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.reconcile_lock).lock_owned().await
    }

    /// Clone of the actor's current canonical head mirror. Reflects the
    /// latest head write of any command — no broadcast lag.
    #[must_use]
    pub fn last_header(&self) -> SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>
    where
        SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Clone,
    {
        self.last_header.borrow().clone()
    }

    /// Receiver whose current value is the head now; `changed()` fires on
    /// every later head write. Keeps only the latest, so a slow subscriber never lags the actor.
    #[must_use]
    pub fn subscribe_head(
        &self,
    ) -> watch::Receiver<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>> {
        let mut rx = self.last_header.clone();
        // The clone inherits this handle's stale seen-version; start from now.
        rx.mark_unchanged();
        rx
    }

    /// Builds a payload with the given attributes if the actor's head
    /// still matches `parent_hash`, commits it via `newPayload`, and
    /// advances the head cursor. `safe` / `finalized` are unaffected —
    /// only [`Self::advance_safe_finalized`] moves those.
    ///
    /// `parent_hash` is the head the caller computed `attrs.timestamp`
    /// against; returns [`DriverError::StaleParent`] upon mismatch
    /// prevents timestamp-drifted block.
    ///
    /// # Errors
    ///
    /// - [`DriverError::StaleParent`] on snapshot mismatch.
    /// - Engine-API failures or `committer_closed`.
    pub async fn commit_sequenced(
        &self,
        parent_hash: B256,
        attrs: EthPayloadAttributes,
    ) -> DriverResult<CommitOutcome<T>> {
        // The reconcile_lock guards ONLY the parent-hash snapshot check
        // below — it stops the deriver advancing the head between the
        // snapshot read and the engine dispatch. Holding it across
        // `response_rx.await` (seconds, while the engine builds the
        // payload) starves `begin_reconcile()` callers, and reth then
        // evicts the issued id → "payload builder returned no payload
        // for issued id".
        {
            let _guard = self.reconcile_lock.lock().await;
            let current_head = self.last_header.borrow().hash();
            if current_head != parent_hash {
                return Err(DriverError::StaleParent {
                    expected: parent_hash,
                    actual: current_head,
                });
            }
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::Sequence {
                attrs,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Re-publishes the current forkchoice state without attaching
    /// payload attributes. Used by the Sequencer's slow refresh timer
    /// to keep reth's engine view alive during quiet periods.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::commit_sequenced`].
    pub async fn refresh_forkchoice(&self) -> DriverResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::RefreshForkchoice {
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Updates the `safe` and `finalized` cursors and sends a fresh FCU
    /// with the same head reth currently has. Used by the Deriver
    /// (stage 3) to push L1-derived safe/finalized advances into reth.
    /// Takes the full safe header so a head demotion triggered by the
    /// update can repair the `last_header` parenting mirror too.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::commit_sequenced`]. If reth rejects the
    /// updated triplet (e.g., the safe / finalized hashes aren't on its
    /// canonical chain), returns [`DriverError::InvalidForkchoice`].
    pub async fn advance_safe_finalized(
        &self,
        safe: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        finalized: B256,
    ) -> DriverResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::AdvanceSafeFinalized {
                safe,
                finalized,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Roll the canonical L2 head back to `target_header` (FCU with
    /// this hash, dropping any blocks past it). Two callers:
    ///
    /// - **L1 reorg retreat (`Deriver::on_l1_reorg`).** L1 rolled out a
    ///   confirmed postBatch, so the L2 head must follow. The caller
    ///   advances safe/finalized FIRST, then calls `reorg_to` — the
    ///   reverse order yields an FCU where safe ≠ ancestor(head), which
    ///   reth rejects.
    /// - **Bundle-failure retreat (Composer).** A just-committed Sync
    ///   block's L1 bundle didn't settle, so the block is rebuilt.
    ///   `safe`/`finalized` stay put — a dropped-bundle reorg never
    ///   invalidates an L1-confirmed block.
    ///
    /// # Errors
    ///
    /// - an engine-RPC error on transport failure.
    /// - an invalid-forkchoice error if reth rejects the
    ///   target hash (not on its canonical chain).
    /// - `committer_closed` if the actor is gone.
    pub async fn reorg_to(
        &self,
        target_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::ReorgTo {
                target_header,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Advances the unsafe head mirror through a bare FCU. Safe and
    /// finalized stay exactly where the committer last accepted them.
    ///
    /// This is used by follower nodes whose unsafe head comes from a
    /// sequencer RPC. A `SYNCING` response is returned as a successful
    /// [`ForkchoiceOutcome::Syncing`]; the engine has accepted the target
    /// and will backfill it through normal sync.
    ///
    /// # Errors
    ///
    /// Transport failures or `committer_closed`.
    pub async fn advance_unsafe_head(
        &self,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<ForkchoiceOutcome> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::AdvanceHead {
                header,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Sends FCU `{head, safe, finalized} = target` so reth syncs to it.
    /// Changes no mirror, anchor or `unsafe_head_hash`: on `SYNCING` nothing
    /// is left behind, so the caller can repeat the call until `Valid`.
    ///
    /// # Errors
    ///
    /// Transport failures, [`DriverError::InvalidForkchoice`] on `INVALID`,
    /// or `committer_closed`.
    pub async fn sync_to(&self, target: B256) -> DriverResult<ForkchoiceOutcome> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::SyncTo {
                target,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Points the head, safe and finalized mirrors at `header`, with no FCU.
    /// Call only after [`Self::sync_to`] returned `Valid` for this header: reth
    /// already holds head = safe = finalized = it.
    ///
    /// # Errors
    ///
    /// `committer_closed` if the actor is gone.
    pub async fn adopt_synced(
        &self,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::AdoptSynced {
                header,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }

    /// Commit a Deriver-built [`ExecutionData`] via `newPayloadV3` +
    /// head-FCU. `safe` / `finalized` are unchanged; move them via
    /// [`Self::advance_safe_finalized`].
    ///
    /// # Errors
    ///
    /// `invalid_payload`, `invalid_forkchoice`, or `committer_closed`.
    ///
    /// [`ExecutionData`]: reth_payload_primitives::PayloadTypes::ExecutionData
    pub async fn commit_derived(
        &self,
        payload: <T as PayloadTypes>::ExecutionData,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        feed_witness: bool,
    ) -> DriverResult<DeriveOutcome> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::Derive {
                payload,
                header,
                feed_witness,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::CommitterClosed)?;
        response_rx
            .await
            .map_err(|_| DriverError::CommitterClosed)?
    }
}

struct Actor<T: PayloadTypes> {
    receiver: mpsc::Receiver<CommitCommand<T>>,
    to_engine: ConsensusEngineHandle<T>,
    payload_builder: PayloadBuilderHandle<T>,
    /// Head mirror read by the handles. The actor is the sole writer, only via
    /// [`Self::set_head`].
    head_tx: watch::Sender<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>>,
    /// Last head accepted by forkchoice. May point at a syncing unsafe head
    /// before reth has imported the canonical header.
    unsafe_head_hash: B256,
    /// Current safe anchor. The full header (not just the hash) so a
    /// head demotion can rewrite the head mirror to a valid parent.
    safe_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    finalized_hash: B256,
    /// Prover-feed trigger (prover-chain P1): after a PRODUCED block
    /// canonicalizes, the actor hands its hash here so the out-of-loop witness
    /// task can re-execute it on parent state for the proof feed. `None` outside
    /// composer mode. The heavy re-exec is NOT done inline — that would stall the
    /// actor loop; only the trigger is emitted. Fired by `process_sequence` (live
    /// blocks) and by `process_derive` ONLY when `feed_witness` is set (a produced
    /// Sync block via `commit_one_prebuilt`); a follower / L1-reconcile re-derive
    /// passes `feed_witness=false` so the prover isn't double-fed the same block.
    witness_sender: Option<mpsc::UnboundedSender<B256>>,
}

impl<T> Actor<T>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes> + Send + Sync + 'static,
    <T::BuiltPayload as BuiltPayload>::Primitives: Send + Sync + 'static,
    SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Send,
{
    fn feed_witness(&self, block_hash: B256) {
        let Some(sender) = &self.witness_sender else {
            return;
        };
        if let Err(err) = sender.send(block_hash) {
            event!(
                name: "eez.committer.witness_feed.send_failed",
                Level::ERROR,
                block.hash = %block_hash,
                error = %err,
                "witness-feed channel closed; prover-capture task is gone — blocks will stop being proven and settlement will stall",
            );
        }
    }

    async fn run(mut self) {
        while let Some(cmd) = self.receiver.recv().await {
            match cmd {
                CommitCommand::Sequence { attrs, response } => {
                    let result = self.process_sequence(attrs).await;
                    let _ = response.send(result);
                }
                CommitCommand::RefreshForkchoice { response } => {
                    let result = self.process_refresh_forkchoice().await;
                    let _ = response.send(result);
                }
                CommitCommand::AdvanceSafeFinalized {
                    safe,
                    finalized,
                    response,
                } => {
                    let result = self.process_advance_safe_finalized(safe, finalized).await;
                    let _ = response.send(result);
                }
                CommitCommand::AdvanceHead { header, response } => {
                    let result = self.process_advance_head(header).await;
                    let _ = response.send(result);
                }
                CommitCommand::SyncTo { target, response } => {
                    let result = self.process_sync_to(target).await;
                    let _ = response.send(result);
                }
                CommitCommand::AdoptSynced { header, response } => {
                    self.finalized_hash = header.hash();
                    self.safe_header = header.clone();
                    self.set_head(header);
                    let _ = response.send(Ok(()));
                }
                CommitCommand::Derive {
                    payload,
                    header,
                    feed_witness,
                    response,
                } => {
                    let result = self.process_derive(payload, header, feed_witness).await;
                    let _ = response.send(result);
                }
                CommitCommand::ReorgTo {
                    target_header,
                    response,
                } => {
                    let result = self.process_reorg_to(target_header).await;
                    let _ = response.send(result);
                }
            }
        }
    }

    async fn process_reorg_to(
        &mut self,
        target_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<()> {
        let target_hash = target_header.hash();
        // safe/finalized unchanged — only L1 advances them, and a
        // dropped-bundle reorg can't invalidate an L1-confirmed block.
        let state = ForkchoiceState {
            head_block_hash: target_hash,
            safe_block_hash: self.safe_header.hash(),
            finalized_block_hash: self.finalized_hash,
        };
        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !res.is_valid() {
            return Err(DriverError::InvalidForkchoice(format!(
                "reorg_to({target_hash}): {res:?}"
            )));
        }
        // Mirror the head only AFTER reth accepted the FCU — writing it
        // first then erroring would leave every later compose building
        // on a head reth never adopted (permanent stale-parent loop).
        // `set_head` keeps `unsafe_head_hash` current: a bare
        // FCU (forkchoice_state reads `unsafe_head_hash`) fires on every
        // BatchPosted via `process_advance_safe_finalized`, and a stale
        // `unsafe_head_hash` would re-canonicalize the orphan we just
        // reorged away — silently undoing this recovery.
        self.set_head(target_header);
        Ok(())
    }

    /// The only head-mirror write: keeps `unsafe_head_hash` in lockstep and publishes.
    fn set_head(&mut self, header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>) {
        self.unsafe_head_hash = header.hash();
        self.head_tx.send_replace(header);
    }

    fn forkchoice_state(&self) -> ForkchoiceState {
        ForkchoiceState {
            head_block_hash: self.unsafe_head_hash,
            safe_block_hash: self.safe_header.hash(),
            finalized_block_hash: self.finalized_hash,
        }
    }

    /// Sends `state` via FCU; on an inconsistent-forkchoice rejection where
    /// the unsafe head (≠ safe) is the suspect, demote it to the safe anchor
    /// and retry once, rewriting both `unsafe_head_hash` and the head mirror so
    /// later ticks don't re-send the same bad triplet. `safe_header`'s hash
    /// must equal `state.safe_block_hash`.
    async fn forkchoice_or_demote_head(
        &mut self,
        mut state: ForkchoiceState,
        safe_header: &SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<ForkchoiceUpdated> {
        match self.to_engine.fork_choice_updated(state, None).await {
            Ok(res) => Ok(res),
            Err(BeaconForkChoiceUpdateError::ForkchoiceUpdateError(
                ForkchoiceUpdateError::InvalidState,
            )) if state.head_block_hash != state.safe_block_hash => {
                event!(
                    name: "eez.committer.unsafe_head.demoted",
                    Level::WARN,
                    head = %state.head_block_hash,
                    safe = %state.safe_block_hash,
                    "engine rejected forkchoice as inconsistent; demoting unsafe head to the safe anchor",
                );
                debug_assert_eq!(safe_header.hash(), state.safe_block_hash);
                self.set_head(safe_header.clone());
                state.head_block_hash = state.safe_block_hash;
                self.to_engine
                    .fork_choice_updated(state, None)
                    .await
                    .map_err(|e| DriverError::EngineRpc(e.to_string()))
            }
            Err(err) => Err(DriverError::EngineRpc(err.to_string())),
        }
    }

    async fn process_refresh_forkchoice(&mut self) -> DriverResult<()> {
        let state = self.forkchoice_state();
        let safe_header = self.safe_header.clone();
        let res = self.forkchoice_or_demote_head(state, &safe_header).await?;
        require_valid_forkchoice(&res)?;
        Ok(())
    }

    async fn process_advance_safe_finalized(
        &mut self,
        safe: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        finalized: B256,
    ) -> DriverResult<()> {
        let mut state = self.forkchoice_state();
        state.safe_block_hash = safe.hash();
        state.finalized_block_hash = finalized;
        let res = self.forkchoice_or_demote_head(state, &safe).await?;
        require_valid_forkchoice(&res)?;
        self.safe_header = safe;
        self.finalized_hash = finalized;
        Ok(())
    }

    async fn process_sync_to(&mut self, target: B256) -> DriverResult<ForkchoiceOutcome> {
        let state = ForkchoiceState {
            head_block_hash: target,
            safe_block_hash: target,
            finalized_block_hash: target,
        };
        let res = match self.to_engine.fork_choice_updated(state, None).await {
            Ok(res) => res,
            Err(BeaconForkChoiceUpdateError::ForkchoiceUpdateError(
                ForkchoiceUpdateError::InvalidState,
            )) => return Ok(ForkchoiceOutcome::InvalidState),
            Err(err) => return Err(DriverError::EngineRpc(err.to_string())),
        };
        classify_advance_head_response(&res)
    }

    async fn process_advance_head(
        &mut self,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<ForkchoiceOutcome> {
        let mut state = self.forkchoice_state();
        state.head_block_hash = header.hash();
        let res = match self.to_engine.fork_choice_updated(state, None).await {
            Ok(res) => res,
            Err(BeaconForkChoiceUpdateError::ForkchoiceUpdateError(
                ForkchoiceUpdateError::InvalidState,
            )) => return Ok(ForkchoiceOutcome::InvalidState),
            Err(err) => return Err(DriverError::EngineRpc(err.to_string())),
        };
        let outcome = classify_advance_head_response(&res)?;
        if outcome == ForkchoiceOutcome::Valid {
            self.set_head(header);
        } else if outcome == ForkchoiceOutcome::Syncing {
            self.unsafe_head_hash = header.hash();
        }
        Ok(outcome)
    }

    async fn process_derive(
        &mut self,
        payload: <T as PayloadTypes>::ExecutionData,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        feed_witness: bool,
    ) -> DriverResult<DeriveOutcome> {
        let block_hash = payload.block_hash();
        let block_number = payload.block_number();

        let np = self
            .to_engine
            .new_payload(payload)
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !np.is_valid() {
            return Err(DriverError::InvalidPayload(format!("{np:?}")));
        }

        // Advance reth's canonical head to the derived block. Safe /
        // finalized stay put; `advance_safe_finalized` is the only
        // thing that moves them.
        let mut state = self.forkchoice_state();
        state.head_block_hash = block_hash;
        let fcu = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !fcu.is_valid() {
            return Err(DriverError::InvalidForkchoice(format!("{fcu:?}")));
        }
        self.set_head(header);

        // Prover-feed (prover-chain P1): a PRODUCED Sync/settling block reaches
        // canonicalization via this path (commit_one_prebuilt → commit_derived),
        // not process_sequence. Emit ONLY when the caller is the producer
        // (`feed_witness`) — a follower / L1-reconcile re-derive passes
        // `feed_witness=false` so the prover isn't double-fed the same block.
        // The block is already canonical (the FCU above), so the witness task's
        // `recovered_block(hash)` resolves on the first try.
        if feed_witness {
            self.feed_witness(block_hash);
        }

        Ok(DeriveOutcome {
            block_hash,
            block_number,
        })
    }

    async fn process_sequence(
        &mut self,
        attrs: EthPayloadAttributes,
    ) -> DriverResult<CommitOutcome<T>> {
        let head_block_hash = self.head_tx.borrow().hash();
        let state = ForkchoiceState {
            head_block_hash,
            safe_block_hash: self.safe_header.hash(),
            finalized_block_hash: self.finalized_hash,
        };
        let fcu = self
            .to_engine
            .fork_choice_updated(state, Some(attrs))
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !fcu.is_valid() {
            return Err(DriverError::InvalidForkchoice(format!("{fcu:?}")));
        }
        let payload_id = fcu.payload_id.ok_or(DriverError::PayloadMissing)?;

        let payload = self
            .payload_builder
            .resolve_kind(payload_id, PayloadKind::WaitForPending)
            .await
            .ok_or(DriverError::PayloadMissing)?
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;

        let header: SealedHeader<_> = payload.block().sealed_header().clone();
        let exec_payload = T::block_to_payload(payload.block().clone(), None);

        let np = self
            .to_engine
            .new_payload(exec_payload)
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !np.is_valid() {
            return Err(DriverError::InvalidPayload(format!("{np:?}")));
        }

        // Canonicalize the just-built block immediately (head-FCU, mirroring
        // process_derive) so it is queryable BEFORE we emit to the prover feed.
        // Without this the block is only `new_payload`'d here and becomes canonical
        // at the NEXT slot's opening FCU, so a `recovered_block(hash)` in the
        // witness task would transiently miss it.
        let mut state = self.forkchoice_state();
        state.head_block_hash = header.hash();
        let fcu = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(|e| DriverError::EngineRpc(e.to_string()))?;
        if !fcu.is_valid() {
            return Err(DriverError::InvalidForkchoice(format!("{fcu:?}")));
        }

        self.set_head(header.clone());
        // Prover-feed trigger (prover-chain P1): the block is now canonical, so
        // the witness task's `recovered_block(hash)` resolves on the first try.
        self.feed_witness(header.hash());
        Ok(CommitOutcome { header })
    }
}

/// `SYNCING` applied nothing, so it must not move safe/finalized anchors.
fn require_valid_forkchoice(res: &ForkchoiceUpdated) -> DriverResult<()> {
    match classify_advance_head_response(res)? {
        ForkchoiceOutcome::Valid => Ok(()),
        _ => Err(DriverError::EngineNotReady(format!("{res:?}"))),
    }
}

fn classify_advance_head_response(res: &ForkchoiceUpdated) -> DriverResult<ForkchoiceOutcome> {
    if res.is_valid() {
        Ok(ForkchoiceOutcome::Valid)
    } else if res.is_syncing() {
        Ok(ForkchoiceOutcome::Syncing)
    } else {
        Err(DriverError::InvalidForkchoice(format!("{res:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Header;
    use reth_engine_primitives::BeaconEngineMessage;
    use reth_ethereum_engine_primitives::EthEngineTypes;
    use reth_ethereum_primitives::BlockBody;
    use reth_primitives_traits::SealedBlock;

    fn sealed(number: u64) -> SealedHeader {
        SealedHeader::seal_slow(Header {
            number,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn head_subscribers_see_every_head_write() {
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(msg) = engine_rx.recv().await {
                match msg {
                    BeaconEngineMessage::ForkchoiceUpdated { tx, .. } => {
                        let _ = tx.send(Ok(reth_engine_primitives::OnForkChoiceUpdated::valid(
                            alloy_rpc_types_engine::PayloadStatus::from_status(
                                alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                            ),
                        )));
                    }
                    BeaconEngineMessage::NewPayload { tx, .. } => {
                        let _ = tx.send(Ok(alloy_rpc_types_engine::PayloadStatus::from_status(
                            alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                        )));
                    }
                    _ => {}
                }
            }
        });

        let (h0, h1, h2) = (sealed(0), sealed(1), sealed(2));
        let handle = BlockCommitterHandle::<EthEngineTypes>::spawn(
            h0.clone(),
            h0,
            B256::ZERO,
            ConsensusEngineHandle::new(engine_tx),
            PayloadBuilderHandle::new(mpsc::unbounded_channel().0),
            None,
        );
        let mut rx = handle.subscribe_head();

        handle.reorg_to(h1.clone()).await.unwrap();
        rx.changed().await.unwrap();
        assert_eq!(rx.borrow_and_update().hash(), h1.hash());

        let block: SealedBlock<reth_ethereum_primitives::Block> =
            SealedBlock::from_sealed_parts(h2.clone(), BlockBody::default());
        let payload = <EthEngineTypes as PayloadTypes>::block_to_payload(block, None);
        handle
            .commit_derived(payload, h2.clone(), false)
            .await
            .unwrap();
        rx.changed().await.unwrap();
        assert_eq!(rx.borrow_and_update().hash(), h2.hash());
        assert_eq!(handle.last_header().hash(), h2.hash());
        assert!(!handle.subscribe_head().has_changed().unwrap());
    }

    type Seen = Arc<std::sync::Mutex<Vec<ForkchoiceState>>>;

    /// Mock engine answering every FCU with `syncing` or VALID, recording states.
    fn mock_engine(syncing: bool) -> (ConsensusEngineHandle<EthEngineTypes>, Seen) {
        let (engine_tx, mut engine_rx) = mpsc::unbounded_channel();
        let seen: Seen = Arc::default();
        let rec = seen.clone();
        tokio::spawn(async move {
            while let Some(msg) = engine_rx.recv().await {
                if let BeaconEngineMessage::ForkchoiceUpdated { state, tx, .. } = msg {
                    rec.lock().unwrap().push(state);
                    let _ = tx.send(Ok(if syncing {
                        reth_engine_primitives::OnForkChoiceUpdated::syncing()
                    } else {
                        reth_engine_primitives::OnForkChoiceUpdated::valid(
                            alloy_rpc_types_engine::PayloadStatus::from_status(
                                alloy_rpc_types_engine::PayloadStatusEnum::Valid,
                            ),
                        )
                    }));
                }
            }
        });
        (ConsensusEngineHandle::new(engine_tx), seen)
    }

    fn spawn_committer(
        engine: ConsensusEngineHandle<EthEngineTypes>,
        head: SealedHeader,
    ) -> BlockCommitterHandle<EthEngineTypes> {
        BlockCommitterHandle::<EthEngineTypes>::spawn(
            head.clone(),
            head,
            B256::ZERO,
            engine,
            PayloadBuilderHandle::new(mpsc::unbounded_channel().0),
            None,
        )
    }

    #[tokio::test]
    async fn sync_to_syncing_changes_nothing() {
        let (engine, seen) = mock_engine(true);
        let h0 = sealed(0);
        let handle = spawn_committer(engine, h0.clone());
        let target = B256::repeat_byte(7);

        assert_eq!(
            handle.sync_to(target).await.unwrap(),
            ForkchoiceOutcome::Syncing
        );
        assert_eq!(handle.last_header().hash(), h0.hash());
        let sent = seen.lock().unwrap()[0];
        assert_eq!(
            (
                sent.head_block_hash,
                sent.safe_block_hash,
                sent.finalized_block_hash
            ),
            (target, target, target)
        );

        assert!(matches!(
            handle.refresh_forkchoice().await,
            Err(DriverError::EngineNotReady(_))
        ));
        let next = seen.lock().unwrap()[1];
        assert_eq!(next.head_block_hash, h0.hash());
        assert_eq!(next.safe_block_hash, h0.hash());
    }

    #[tokio::test]
    async fn sync_to_valid_reports_valid() {
        let (engine, _seen) = mock_engine(false);
        let handle = spawn_committer(engine, sealed(0));
        assert_eq!(
            handle.sync_to(B256::repeat_byte(7)).await.unwrap(),
            ForkchoiceOutcome::Valid
        );
    }

    #[tokio::test]
    async fn adopt_synced_moves_every_mirror_without_an_fcu() {
        let (engine, seen) = mock_engine(false);
        let handle = spawn_committer(engine, sealed(0));
        let synced = sealed(5);

        handle.adopt_synced(synced.clone()).await.unwrap();
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(handle.last_header().hash(), synced.hash());

        handle.refresh_forkchoice().await.unwrap();
        let next = seen.lock().unwrap()[0];
        assert_eq!(
            (
                next.head_block_hash,
                next.safe_block_hash,
                next.finalized_block_hash
            ),
            (synced.hash(), synced.hash(), synced.hash())
        );
    }

    #[tokio::test]
    async fn advance_safe_finalized_syncing_keeps_anchors() {
        let (engine, seen) = mock_engine(true);
        let h0 = sealed(0);
        let handle = spawn_committer(engine, h0.clone());

        let res = handle
            .advance_safe_finalized(sealed(1), B256::repeat_byte(9))
            .await;
        assert!(matches!(res, Err(DriverError::EngineNotReady(_))));
        let _ = handle.refresh_forkchoice().await;
        let next = seen.lock().unwrap()[1];
        assert_eq!(next.safe_block_hash, h0.hash());
        assert_eq!(next.finalized_block_hash, B256::ZERO);
    }
}
