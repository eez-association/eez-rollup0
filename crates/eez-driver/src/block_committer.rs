//! Single-task actor that serializes every engine-API call to reth.
//!
//! Owns the `ConsensusEngineHandle`, the `PayloadBuilderHandle`,
//! `last_header`, and the unsafe / safe / finalized hash cursors.
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
//! Until the Deriver speaks, `safe` and `finalized` stay at the boot-time head.

use core::fmt;
use std::sync::{Arc, RwLock};

use alloy_primitives::B256;
use alloy_rpc_types_engine::{ForkchoiceState, ForkchoiceUpdateError, ForkchoiceUpdated};
use reth_engine_primitives::{BeaconForkChoiceUpdateError, ConsensusEngineHandle};
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{BuiltPayload, ExecutionPayload, PayloadKind, PayloadTypes};
use reth_primitives_traits::{SealedHeader, SealedHeaderFor};
use tokio::sync::{mpsc, oneshot};

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
        safe: B256,
        finalized: B256,
        response: oneshot::Sender<DriverResult<()>>,
    },
    AdvanceHead {
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        response: oneshot::Sender<DriverResult<ForkchoiceOutcome>>,
    },
    Derive {
        payload: <T as PayloadTypes>::ExecutionData,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        response: oneshot::Sender<DriverResult<DeriveOutcome>>,
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

/// Clone-cheap handle to a spawned [`BlockCommitter`] actor. Carries
/// an `Arc<RwLock<SealedHeader>>` head mirror the actor writes on every
/// Sequence/Derive — fixes the `invalid payload attributes` race
/// (Sequencer reads the post-Derive head without `CanonStateNotification`
/// lag).
pub struct BlockCommitterHandle<T: PayloadTypes> {
    sender: mpsc::Sender<CommitCommand<T>>,
    last_header: Arc<RwLock<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>>>,
    /// Held by Deriver across a whole batch reconcile; blocks
    /// `commit_sequenced` so sequencer ticks can't interleave
    reconcile_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<T: PayloadTypes> Clone for BlockCommitterHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            last_header: Arc::clone(&self.last_header),
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
    /// Spawn the actor on the current tokio runtime. `safe` and
    /// `finalized` are seeded to `initial_header.hash()` until the
    /// Deriver advances them via [`Self::advance_safe_finalized`].
    #[must_use]
    pub fn spawn(
        initial_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
        to_engine: ConsensusEngineHandle<T>,
        payload_builder: PayloadBuilderHandle<T>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(COMMAND_BUFFER);
        let initial_hash = initial_header.hash();
        let last_header = Arc::new(RwLock::new(initial_header));
        let actor = Actor {
            receiver,
            to_engine,
            payload_builder,
            last_header: Arc::clone(&last_header),
            unsafe_head_hash: initial_hash,
            safe_hash: initial_hash,
            finalized_hash: initial_hash,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            last_header,
            reconcile_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Hold across a multi-block reconcile to suppress concurrent `commit_sequenced`
    pub async fn begin_reconcile(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.reconcile_lock).lock_owned().await
    }

    /// Clone of the actor's current canonical head mirror. Reflects the
    /// most recent successful Sequence or Derive — no broadcast lag.
    ///
    /// # Panics
    ///
    /// If the mirror's `RwLock` is poisoned.
    #[must_use]
    pub fn last_header(&self) -> SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>
    where
        SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Clone,
    {
        self.last_header.read().unwrap().clone()
    }

    /// Builds a payload with the given attributes if the actor's head
    /// still matches `parent_hash`, commits it via `newPayload`, and
    /// advances the head cursor. `safe` / `finalized` are unaffected —
    /// only [`Self::advance_safe_finalized`] moves those.
    ///
    /// `parent_hash` is the head the caller computed `attrs.timestamp`
    /// against; returns [`DriverError::is_stale_parent`] upon mismatch
    /// prevents timestamp-drifted block.
    ///
    /// # Errors
    ///
    /// - [`DriverError::is_stale_parent`] on snapshot mismatch.
    /// - Engine-API failures or `committer_closed`.
    ///
    /// # Panics
    ///
    /// If the `last_header` lock is poisoned (a prior holder panicked
    /// while writing). The `BlockCommitter` actor is the only writer; a
    /// poisoned lock means the actor itself has died and the node
    /// should shut down anyway.
    pub async fn commit_sequenced(
        &self,
        parent_hash: B256,
        attrs: EthPayloadAttributes,
    ) -> DriverResult<CommitOutcome<T>> {
        // Prevents interleave with the Deriver's per-block reconcile.
        let _guard = self.reconcile_lock.lock().await;
        // Snapshot may have gone stale while we waited for the lock;
        // `attrs.timestamp` was computed against `parent_hash`, so if
        // the head moved we'd produce a drifted block.
        let current_head = self.last_header.read().unwrap().hash();
        if current_head != parent_hash {
            return Err(DriverError::stale_parent(parent_hash, current_head));
        }
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::Sequence {
                attrs,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::committer_closed())?;
        response_rx
            .await
            .map_err(|_| DriverError::committer_closed())?
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
            .map_err(|_| DriverError::committer_closed())?;
        response_rx
            .await
            .map_err(|_| DriverError::committer_closed())?
    }

    /// Updates the `safe` and `finalized` cursors and sends a fresh FCU
    /// with the same head reth currently has. Used by the Deriver
    /// (stage 3) to push L1-derived safe/finalized advances into reth.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::commit_sequenced`]. If reth rejects the
    /// updated triplet (e.g., the safe / finalized hashes aren't on its
    /// canonical chain), returns [`DriverError::is_invalid_forkchoice`].
    pub async fn advance_safe_finalized(&self, safe: B256, finalized: B256) -> DriverResult<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::AdvanceSafeFinalized {
                safe,
                finalized,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::committer_closed())?;
        response_rx
            .await
            .map_err(|_| DriverError::committer_closed())?
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
            .map_err(|_| DriverError::committer_closed())?;
        response_rx
            .await
            .map_err(|_| DriverError::committer_closed())?
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
    ) -> DriverResult<DeriveOutcome> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(CommitCommand::Derive {
                payload,
                header,
                response: response_tx,
            })
            .await
            .map_err(|_| DriverError::committer_closed())?;
        response_rx
            .await
            .map_err(|_| DriverError::committer_closed())?
    }
}

struct Actor<T: PayloadTypes> {
    receiver: mpsc::Receiver<CommitCommand<T>>,
    to_engine: ConsensusEngineHandle<T>,
    payload_builder: PayloadBuilderHandle<T>,
    /// Shared with the `BlockCommitterHandle`. The actor is the sole writer;
    /// Updated synchronously inside `process_sequence` and `process_derive`
    last_header: Arc<RwLock<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>>>,
    /// Last head accepted by forkchoice. May point at a syncing unsafe head
    /// before reth has imported the canonical header.
    unsafe_head_hash: B256,
    safe_hash: B256,
    finalized_hash: B256,
}

impl<T> Actor<T>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes> + Send + Sync + 'static,
    <T::BuiltPayload as BuiltPayload>::Primitives: Send + Sync + 'static,
    SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>: Send,
{
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
                CommitCommand::Derive {
                    payload,
                    header,
                    response,
                } => {
                    let result = self.process_derive(payload, header).await;
                    let _ = response.send(result);
                }
            }
        }
    }

    fn forkchoice_state(&self) -> ForkchoiceState {
        ForkchoiceState {
            head_block_hash: self.unsafe_head_hash,
            safe_block_hash: self.safe_hash,
            finalized_block_hash: self.finalized_hash,
        }
    }

    async fn process_refresh_forkchoice(&self) -> DriverResult<()> {
        let state = self.forkchoice_state();
        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !res.is_valid() && !res.is_syncing() {
            return Err(DriverError::invalid_forkchoice(format!("{res:?}")));
        }
        Ok(())
    }

    async fn process_advance_safe_finalized(
        &mut self,
        safe: B256,
        finalized: B256,
    ) -> DriverResult<()> {
        let mut state = self.forkchoice_state();
        state.safe_block_hash = safe;
        state.finalized_block_hash = finalized;
        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !res.is_valid() && !res.is_syncing() {
            return Err(DriverError::invalid_forkchoice(format!("{res:?}")));
        }
        self.safe_hash = safe;
        self.finalized_hash = finalized;
        Ok(())
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
            Err(err) => return Err(DriverError::engine_rpc(err)),
        };
        let outcome = classify_advance_head_response(&res)?;
        if matches!(
            outcome,
            ForkchoiceOutcome::Valid | ForkchoiceOutcome::Syncing
        ) {
            self.unsafe_head_hash = header.hash();
        }
        if outcome == ForkchoiceOutcome::Valid {
            *self.last_header.write().unwrap() = header;
        }
        Ok(outcome)
    }

    async fn process_derive(
        &mut self,
        payload: <T as PayloadTypes>::ExecutionData,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> DriverResult<DeriveOutcome> {
        let block_hash = payload.block_hash();
        let block_number = payload.block_number();

        let np = self
            .to_engine
            .new_payload(payload)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !np.is_valid() {
            return Err(DriverError::invalid_payload(format!("{np:?}")));
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
            .map_err(DriverError::engine_rpc)?;
        if !fcu.is_valid() {
            return Err(DriverError::invalid_forkchoice(format!("{fcu:?}")));
        }
        // Mirror the new head into the shared `RwLock`.
        self.unsafe_head_hash = block_hash;
        *self.last_header.write().unwrap() = header;

        Ok(DeriveOutcome {
            block_hash,
            block_number,
        })
    }

    async fn process_sequence(
        &mut self,
        attrs: EthPayloadAttributes,
    ) -> DriverResult<CommitOutcome<T>> {
        let head_block_hash = self.last_header.read().unwrap().hash();
        let state = ForkchoiceState {
            head_block_hash,
            safe_block_hash: self.safe_hash,
            finalized_block_hash: self.finalized_hash,
        };
        let fcu = self
            .to_engine
            .fork_choice_updated(state, Some(attrs))
            .await
            .map_err(DriverError::engine_rpc)?;
        if !fcu.is_valid() {
            return Err(DriverError::invalid_forkchoice(format!("{fcu:?}")));
        }
        let payload_id = fcu.payload_id.ok_or_else(DriverError::payload_missing)?;

        let payload = self
            .payload_builder
            .resolve_kind(payload_id, PayloadKind::WaitForPending)
            .await
            .ok_or_else(DriverError::payload_missing)?
            .map_err(DriverError::engine_rpc)?;

        let header: SealedHeader<_> = payload.block().sealed_header().clone();
        let exec_payload = T::block_to_payload(payload.block().clone(), None);

        let np = self
            .to_engine
            .new_payload(exec_payload)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !np.is_valid() {
            return Err(DriverError::invalid_payload(format!("{np:?}")));
        }

        self.unsafe_head_hash = header.hash();
        *self.last_header.write().unwrap() = header.clone();
        Ok(CommitOutcome { header })
    }
}

fn classify_advance_head_response(res: &ForkchoiceUpdated) -> DriverResult<ForkchoiceOutcome> {
    if res.is_valid() {
        Ok(ForkchoiceOutcome::Valid)
    } else if res.is_syncing() {
        Ok(ForkchoiceOutcome::Syncing)
    } else {
        Err(DriverError::invalid_forkchoice(format!("{res:?}")))
    }
}
