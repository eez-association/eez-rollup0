//! Incremental `prove.v2` session runtime.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use alloy_primitives::B256;
use eez_control_rpc::v2::prover_server::Prover;
use eez_control_rpc::v2::{
    Cancelled, ClientFrame, Proof, Ready, Rejected, ServerFrame, Validated, client_frame,
    server_frame,
};
use prost::Message as _;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::timeout;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Level, event, warn};

use super::settlement_job::{PipelineError, SettlementInput, run_settlement};
use super::{ProveSvc, ServiceLimits, ServiceState};
use crate::cancel::CancellationToken;
use crate::validate::{
    AdmittedBlock, IncrementalAnchor, ValidatedBlockArtifact, ValidatedWindow, ValidationError,
    into_execution_witness, wire_witness_item_count,
};

const MAX_RETAINED_SESSIONS: usize = 64;

type SessionId = B256;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send>>;
type FrameResult = Result<(ServerFrame, bool), FrameError>;

#[derive(Debug)]
struct Session {
    anchor: IncrementalAnchor,
    blocks: Vec<SessionBlock>,
    epoch: u64,
    connected: bool,
    cancellation: CancellationToken,
}

#[derive(Debug)]
struct SessionBlock {
    validated: Arc<ValidatedBlockArtifact>,
    // Charge each session for its own submission, even on a shared cache hit.
    payload_bytes: usize,
    witness_items: usize,
}

impl Session {
    fn tip(&self) -> IncrementalAnchor {
        self.blocks
            .last()
            .map_or(self.anchor, |block| block.validated.as_anchor())
    }
}

#[derive(Debug, Default)]
struct Registry {
    // When both locks are needed, acquire the registry before a session.
    sessions: HashMap<SessionId, Arc<Mutex<Session>>>,
    insertion_order: VecDeque<SessionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundSession {
    id: SessionId,
    epoch: u64,
}

type FrameError = (Option<BoundSession>, Status);

/// Shared by every v2 service clone. Session state is private; only immutable,
/// completely checked block artifacts cross session bounds.
#[derive(Debug)]
pub(super) struct IncrementalRuntime {
    state: Arc<ServiceState>,
    limits: ServiceLimits,
    registry: Mutex<Registry>,
    active_streams: AtomicUsize,
    idle: Notify,
}

impl IncrementalRuntime {
    pub(super) fn new(state: Arc<ServiceState>, limits: ServiceLimits) -> Arc<Self> {
        Arc::new(Self {
            state,
            limits,
            registry: Mutex::new(Registry::default()),
            active_streams: AtomicUsize::new(0),
            idle: Notify::new(),
        })
    }

    pub(super) async fn wait_until_idle(&self) {
        loop {
            let notified = self.idle.notified();
            if self.active_streams.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Serve one RPC stream, sequencing block work while accepting control frames.
    async fn serve_stream(
        self: Arc<Self>,
        mut input: Streaming<ClientFrame>,
        output: mpsc::Sender<Result<ServerFrame, Status>>,
    ) {
        enum Event {
            Input(Result<Option<ClientFrame>, Status>),
            Completed(Result<(u64, FrameResult), tokio::task::JoinError>),
        }

        let mut bound_session = None;
        let mut pending: VecDeque<ClientFrame> = VecDeque::new();
        let mut operation: Option<tokio::task::JoinHandle<(u64, FrameResult)>> = None;
        let mut input_closed = false;
        loop {
            // Start one queued operation at a time to preserve block and Finalize ordering.
            if operation.is_none()
                && let Some(frame) = pending.pop_front()
            {
                let runtime = Arc::clone(&self);
                let request_id = frame.request_id;
                let mut local_bound = bound_session;
                operation = Some(tokio::spawn(async move {
                    let result = runtime.handle_frame(&mut local_bound, frame).await;
                    (request_id, result)
                }));
                continue;
            }
            // Once input ends, exit only after all accepted work has drained.
            if operation.is_none() && input_closed {
                break;
            }

            let event = if let Some(active) = operation.as_mut() {
                // Keep reading control frames while active work uses its own request deadline.
                Ok(if input_closed {
                    Event::Completed(active.await)
                } else {
                    tokio::select! {
                        incoming = input.message() => Event::Input(incoming),
                        completed = active => Event::Completed(completed),
                    }
                })
            } else {
                // Apply the idle timeout only while waiting for input without active work.
                timeout(self.limits.stream_idle_timeout(), async {
                    Event::Input(input.message().await)
                })
                .await
            };
            // Close a stream that stays idle beyond the configured timeout.
            let Ok(event) = event else {
                let _ = output
                    .send(Err(Status::deadline_exceeded("ProveStream idle timeout")))
                    .await;
                break;
            };

            match event {
                // Queue dependent work so it cannot overtake earlier blocks or Finalize.
                Event::Input(Ok(Some(message)))
                    if matches!(
                        message.kind,
                        Some(client_frame::Kind::Block(_) | client_frame::Kind::Finalize(_))
                    ) =>
                {
                    pending.push_back(message);
                }
                // Handle control frames directly so Rewind and Cancel can fence active work.
                Event::Input(Ok(Some(message))) => {
                    let request_id = message.request_id;
                    let is_rewind = matches!(message.kind, Some(client_frame::Kind::Rewind(_)));
                    let response = self.handle_frame(&mut bound_session, message).await;
                    let should_close = matches!(response, Ok((_, true)));
                    let frame = match response {
                        Ok((frame, _)) => frame,
                        Err((authorized, status)) => {
                            self.rejected_frame(authorized, request_id, status).await
                        }
                    };
                    if output.send(Ok(frame)).await.is_err() {
                        break;
                    }
                    // Discard queued requests targeting the pre-rewind prefix.
                    if is_rewind {
                        pending.clear();
                    }
                    if should_close {
                        break;
                    }
                }
                // Stop reading after client half-close, but still finish accepted requests.
                Event::Input(Ok(None)) => {
                    input_closed = true;
                }
                // Input decoding or transport failures terminate the RPC, not just one request.
                Event::Input(Err(error)) => {
                    let status = if error.code() == tonic::Code::OutOfRange {
                        Status::resource_exhausted("ProveStream message exceeds decoding limit")
                    } else {
                        error
                    };
                    let _ = output.send(Err(status)).await;
                    break;
                }
                // Release the work slot and deliver the operation's success or rejection.
                Event::Completed(Ok((request_id, response))) => {
                    operation = None;
                    let should_close = matches!(response, Ok((_, true)));
                    // Suppress replies that do not belong to this stream's current binding.
                    let frame = match response {
                        Ok((frame, _))
                            if bound_session.is_some_and(|bound| {
                                frame.session_id == bound.id.as_slice()
                                    && frame.epoch == bound.epoch
                            }) =>
                        {
                            Some(frame)
                        }
                        Err((Some(authorized), status)) if bound_session == Some(authorized) => {
                            Some(
                                self.rejected_frame(Some(authorized), request_id, status)
                                    .await,
                            )
                        }
                        Ok(_) | Err(_) => None,
                    };
                    if let Some(frame) = frame
                        && output.send(Ok(frame)).await.is_err()
                    {
                        break;
                    }
                    if should_close {
                        break;
                    }
                }
                // A task panic or unexpected cancellation makes the stream unusable.
                Event::Completed(Err(error)) => {
                    operation = None;
                    warn!(?error, "incremental stream operation task failed");
                    let _ = output
                        .send(Err(Status::internal("incremental stream operation failed")))
                        .await;
                    break;
                }
            }
        }
        // End the response stream before waiting for any outstanding work to finish.
        drop(output);
        if let Some(operation) = operation {
            let _ = operation.await;
        }
        // An old stream must not mark a newer resumed attachment disconnected.
        if let Some(bound) = bound_session
            && let Ok(session) = self.session(bound.id).await
        {
            let mut session = session.lock().await;
            if session.epoch == bound.epoch {
                session.connected = false;
            }
        }
        // Wake shutdown waiters when the last stream has finished cleaning up.
        if self.active_streams.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }

    /// Handle one request, updating its stream binding and returning a response plus a close flag.
    async fn handle_frame(
        &self,
        bound_session: &mut Option<BoundSession>,
        frame: ClientFrame,
    ) -> Result<(ServerFrame, bool), FrameError> {
        let ClientFrame {
            session_id,
            request_id,
            epoch,
            kind,
        } = frame;
        let Some(kind) = kind else {
            return Err((
                authorized_bound(bound_session.as_ref(), &session_id, epoch),
                Status::invalid_argument("frame carries no operation"),
            ));
        };

        match kind {
            // Create and bind a new session at the supplied anchor, then report its ready cursor.
            client_frame::Kind::Begin(begin) => {
                if bound_session.is_some() || !session_id.is_empty() || epoch != 0 {
                    return Err((
                        None,
                        Status::invalid_argument(
                            "Begin must be the first frame with no session ID and epoch zero",
                        ),
                    ));
                }
                let (bound, ready) = self.begin(begin).await.map_err(|status| (None, status))?;
                *bound_session = Some(bound);
                Ok((
                    server_frame(bound, request_id, server_frame::Kind::Ready(ready)),
                    false,
                ))
            }
            // Reattach to retained progress with a new epoch that fences the previous attachment.
            client_frame::Kind::Resume(_) => {
                if bound_session.is_some() || epoch != 0 {
                    return Err((
                        None,
                        Status::invalid_argument("Resume must be the first frame with epoch zero"),
                    ));
                }
                let id = parse_session_id(&session_id).map_err(|status| (None, status))?;
                let (bound, ready) = self.resume(id).await.map_err(|status| (None, status))?;
                *bound_session = Some(bound);
                Ok((
                    server_frame(bound, request_id, server_frame::Kind::Ready(ready)),
                    false,
                ))
            }
            // Truncate to a retained ancestor and advance the epoch to fence old-prefix work.
            client_frame::Kind::Rewind(rewind) => {
                let bound = require_bound(bound_session.as_ref(), &session_id, epoch)?;
                let ancestor = parse_b256("rewind ancestor hash", &rewind.ancestor_hash)
                    .map_err(|status| (Some(bound), status))?;
                let (rewound, ready) = self
                    .rewind(bound, ancestor)
                    .await
                    .map_err(|status| (Some(bound), status))?;
                *bound_session = Some(rewound);
                Ok((
                    server_frame(rewound, request_id, server_frame::Kind::Ready(ready)),
                    false,
                ))
            }
            // Validate or reuse a block and acknowledge checked progress without issuing a proof.
            client_frame::Kind::Block(block) => {
                let bound = require_bound(bound_session.as_ref(), &session_id, epoch)?;
                let started = Instant::now();
                let validated = self
                    .validate_block(bound, block)
                    .await
                    .map_err(|status| (Some(bound), status))?;
                event!(
                    name: "eez.proof_signer.block_validated",
                    Level::INFO,
                    event_name = "eez.proof_signer.block_validated",
                    session_id = %bound.id,
                    number = validated.number,
                    reused = validated.reused,
                    elapsed_us = started.elapsed().as_micros(),
                    "incremental block validated",
                );
                Ok((
                    server_frame(bound, request_id, server_frame::Kind::Validated(validated)),
                    false,
                ))
            }
            // Check and sign the requested validated range, then close the stream with its proof.
            client_frame::Kind::Finalize(finalize) => {
                let bound = require_bound(bound_session.as_ref(), &session_id, epoch)?;
                let proof = self
                    .finalize(bound, finalize)
                    .await
                    .map_err(|status| (Some(bound), status))?;
                Ok((
                    server_frame(bound, request_id, server_frame::Kind::Proof(proof)),
                    true,
                ))
            }
            // Retire this session so it cannot resume, then acknowledge cancellation and close.
            client_frame::Kind::Cancel(_) => {
                let bound = require_bound(bound_session.as_ref(), &session_id, epoch)?;
                let mut registry = self.registry.lock().await;
                let session =
                    registry.sessions.get(&bound.id).cloned().ok_or_else(|| {
                        (None, Status::not_found("prover session is not retained"))
                    })?;
                let session = session.lock().await;
                if session.epoch != bound.epoch {
                    return Err((None, Status::aborted("prover session epoch is stale")));
                }
                session.cancellation.cancel();
                drop(session);
                registry.sessions.remove(&bound.id);
                registry
                    .insertion_order
                    .retain(|retained| *retained != bound.id);
                *bound_session = None;
                Ok((
                    server_frame(
                        bound,
                        request_id,
                        server_frame::Kind::Cancelled(Cancelled {}),
                    ),
                    true,
                ))
            }
        }
    }

    /// Create a retained session at the supplied anchor and return its initial binding and cursor.
    async fn begin(
        &self,
        begin: eez_control_rpc::v2::Begin,
    ) -> Result<(BoundSession, Ready), Status> {
        if begin.rollup_id != self.state.expected_rollup_id.get() {
            return Err(Status::failed_precondition(
                "session rollup identity rejected",
            ));
        }
        begin
            .anchor_number
            .checked_add(1)
            .ok_or_else(|| Status::invalid_argument("anchor block number cannot be incremented"))?;
        let anchor = IncrementalAnchor {
            number: begin.anchor_number,
            hash: parse_b256("anchor hash", &begin.anchor_hash)?,
            state_root: parse_b256("anchor state root", &begin.anchor_state_root)?,
        };
        self.state
            .validator
            .begin_incremental(anchor)
            .map_err(validation_status)?;

        let mut registry = self.registry.lock().await;
        // Make room by evicting the oldest-created disconnected sessions, keeping active ones.
        while registry.sessions.len() >= MAX_RETAINED_SESSIONS {
            let mut eviction = None;
            for id in &registry.insertion_order {
                if let Some(session) = registry.sessions.get(id)
                    && !session.lock().await.connected
                {
                    eviction = Some(*id);
                    break;
                }
            }
            let Some(eviction) = eviction else {
                break;
            };
            registry.sessions.remove(&eviction);
            registry
                .insertion_order
                .retain(|retained| *retained != eviction);
        }
        if registry.sessions.len() >= MAX_RETAINED_SESSIONS {
            return Err(Status::resource_exhausted(
                "too many retained prover sessions",
            ));
        }
        let id = loop {
            let mut bytes = [0_u8; 32];
            getrandom::fill(&mut bytes)
                .map_err(|_| Status::internal("unable to allocate a prover session ID"))?;
            let id = B256::from(bytes);
            if id != B256::ZERO && !registry.sessions.contains_key(&id) {
                break id;
            }
        };
        registry.sessions.insert(
            id,
            Arc::new(Mutex::new(Session {
                anchor,
                blocks: Vec::new(),
                epoch: 1,
                connected: true,
                cancellation: CancellationToken::default(),
            })),
        );
        registry.insertion_order.push_back(id);
        Ok((
            BoundSession { id, epoch: 1 },
            Ready {
                validated_through: anchor.number,
                validated_hash: anchor.hash.to_vec(),
            },
        ))
    }

    /// Reattach to retained progress with a new epoch, cancelling work from the previous attachment.
    async fn resume(&self, id: SessionId) -> Result<(BoundSession, Ready), Status> {
        // Keep the registry locked until attachment so eviction cannot remove
        // a disconnected session while it is being resumed.
        let registry = self.registry.lock().await;
        let session = registry
            .sessions
            .get(&id)
            .ok_or_else(|| Status::not_found("prover session is not retained"))?;
        let mut session = session.lock().await;
        session.cancellation.cancel();
        session.epoch = next_epoch(session.epoch);
        session.connected = true;
        session.cancellation = CancellationToken::default();
        Ok((
            BoundSession {
                id,
                epoch: session.epoch,
            },
            Ready {
                validated_through: session.tip().number,
                validated_hash: session.tip().hash.to_vec(),
            },
        ))
    }

    /// Truncate to a retained ancestor and advance the epoch to invalidate old-prefix work.
    async fn rewind(
        &self,
        bound: BoundSession,
        ancestor_hash: B256,
    ) -> Result<(BoundSession, Ready), Status> {
        let session = self.session(bound.id).await?;
        let mut session = session.lock().await;
        require_epoch(&session, bound.epoch)?;

        let retained = if ancestor_hash == session.anchor.hash {
            0
        } else {
            session
                .blocks
                .iter()
                .position(|block| block.validated.hash == ancestor_hash)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "Rewind ancestor is not in the session's validated active prefix",
                    )
                })?
        };
        session.blocks.truncate(retained);
        session.cancellation.cancel();
        session.epoch = next_epoch(session.epoch);
        session.cancellation = CancellationToken::default();
        let rewound = BoundSession {
            id: bound.id,
            epoch: session.epoch,
        };
        let ready = Ready {
            validated_through: session.tip().number,
            validated_hash: session.tip().hash.to_vec(),
        };
        Ok((rewound, ready))
    }

    async fn session(&self, id: SessionId) -> Result<Arc<Mutex<Session>>, Status> {
        self.registry
            .lock()
            .await
            .sessions
            .get(&id)
            .cloned()
            .ok_or_else(|| Status::not_found("prover session is not retained"))
    }

    /// Validate or reuse a block and retain its checked artifact in the session.
    async fn validate_block(
        &self,
        bound: BoundSession,
        block: eez_control_rpc::v2::Block,
    ) -> Result<Validated, Status> {
        let submitted = block
            .data
            .ok_or_else(|| Status::invalid_argument("Block carries no block data"))?;
        let session = self.session(bound.id).await?;
        let session_guard = session.lock().await;
        require_epoch(&session_guard, bound.epoch)?;

        if let Some(existing) = session_guard
            .blocks
            .iter()
            .map(|block| &block.validated)
            .find(|artifact| {
                artifact.number == submitted.number && artifact.hash.as_slice() == submitted.hash
            })
        {
            if existing.parent_hash.as_slice() != submitted.parent_hash
                || existing.rlp != submitted.rlp
            {
                return Err(Status::invalid_argument(
                    "duplicate block number does not match its validated artifact",
                ));
            }
            return Ok(validated_response(existing, false));
        }

        let anchor = session_guard.anchor;
        let parent = session_guard.tip();
        // Parse the wire fields; the shared validator checks the block's parent and number.
        let payload_bytes = submitted.encoded_len();
        let claimed_hash = parse_b256("block hash", &submitted.hash)?;
        let claimed_parent_hash = parse_b256("block parent hash", &submitted.parent_hash)?;
        let wire_witness = submitted
            .witness
            .ok_or_else(|| Status::invalid_argument("Block carries no execution witness"))?;
        let witness_items = wire_witness_item_count(&wire_witness);

        // Charge this session's submission before allocating the backend witness collections.
        let attempted_blocks = session_guard.blocks.len().saturating_add(1);
        if attempted_blocks > self.limits.max_blocks {
            return Err(Status::resource_exhausted("session block quota exceeded"));
        }
        let (attempted_bytes, attempted_items) = session_guard.blocks.iter().try_fold(
            (payload_bytes, witness_items),
            |(bytes, items), block| {
                let bytes = bytes
                    .checked_add(block.payload_bytes)
                    .ok_or_else(|| Status::resource_exhausted("session payload quota overflow"))?;
                let items = items
                    .checked_add(block.witness_items)
                    .ok_or_else(|| Status::resource_exhausted("session witness quota overflow"))?;
                Ok::<_, Status>((bytes, items))
            },
        )?;
        if attempted_bytes > self.limits.max_payload_bytes {
            return Err(Status::resource_exhausted("session payload quota exceeded"));
        }
        if attempted_items > self.limits.max_witness_items {
            return Err(Status::resource_exhausted("session witness quota exceeded"));
        }

        drop(session_guard);
        let admitted = AdmittedBlock {
            declared_number: submitted.number,
            claimed_hash,
            claimed_parent_hash,
            rlp: submitted.rlp,
            witness: into_execution_witness(wire_witness),
        };
        // Delegate block validation to the backend through the shared validator.
        let (cached, reused) = self
            .state
            .validator
            .validate_next(anchor, parent, admitted, self.limits.request_timeout())
            .await
            .map_err(validation_status)?;

        let mut session_guard = session.lock().await;
        require_epoch(&session_guard, bound.epoch)?;
        let tip = session_guard.tip();
        if tip.number.checked_add(1) != Some(cached.number)
            || tip.hash != cached.parent_hash
            || tip.state_root != cached.pre_state_root
        {
            return Err(Status::aborted(
                "session cursor changed while block validation was in flight",
            ));
        }
        session_guard.blocks.push(SessionBlock {
            validated: Arc::clone(&cached),
            payload_bytes,
            witness_items,
        });
        Ok(validated_response(&cached, reused))
    }

    /// Check the submitted batch against the selected validated range and sign its public-inputs hash.
    async fn finalize(
        &self,
        bound: BoundSession,
        finalize: eez_control_rpc::v2::Finalize,
    ) -> Result<Proof, Status> {
        let started = Instant::now();
        let terminal_hash = parse_b256("terminal hash", &finalize.terminal_hash)?;
        let post_batch = finalize
            .post_batch
            .ok_or_else(|| Status::invalid_argument("Finalize carries no PostBatch"))?;
        if !post_batch.l1_block_hash.is_empty() {
            return Err(Status::invalid_argument(
                "Finalize PostBatch l1_block_hash must be empty",
            ));
        }
        let session = self.session(bound.id).await?;
        // Check the requested range and terminal identity, then snapshot its artifacts and anchor.
        let (anchor, artifacts, start_index, cancellation) = {
            let session = session.lock().await;
            require_epoch(&session, bound.epoch)?;
            let first_validated = session.anchor.number.checked_add(1).ok_or_else(|| {
                Status::invalid_argument("session anchor block number cannot be incremented")
            })?;
            if finalize.from_block < first_validated || finalize.to_block < finalize.from_block {
                return Err(Status::invalid_argument(
                    "Finalize range is outside the session's validated active prefix",
                ));
            }
            let start_index = usize::try_from(finalize.from_block - first_validated)
                .map_err(|_| Status::resource_exhausted("Finalize range is too large"))?;
            let end_index = usize::try_from(finalize.to_block - first_validated)
                .map_err(|_| Status::resource_exhausted("Finalize range is too large"))?;
            if start_index > end_index || end_index >= session.blocks.len() {
                return Err(Status::failed_precondition(
                    "Finalize terminal block has not been fully validated",
                ));
            }
            let artifacts = session.blocks[start_index..=end_index]
                .iter()
                .map(|block| Arc::clone(&block.validated))
                .collect::<Vec<_>>();
            let terminal = artifacts.last().expect("non-empty checked range");
            if terminal.number != finalize.to_block || terminal.hash != terminal_hash {
                return Err(Status::failed_precondition(
                    "Finalize terminal identity does not match the validated prefix",
                ));
            }
            let anchor = if start_index == 0 {
                session.anchor
            } else {
                session.blocks[start_index - 1].validated.as_anchor()
            };
            (anchor, artifacts, start_index, session.cancellation.clone())
        };

        let state = Arc::clone(&self.state);
        match timeout(
            self.limits.request_timeout(),
            state.validator.recheck_incremental(anchor, &artifacts),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(validation_status(error)),
            Err(_) => {
                cancellation.cancel();
                return Err(Status::deadline_exceeded(
                    "ProveStream Finalize backend recheck deadline exceeded",
                ));
            }
        }
        let remaining = self
            .limits
            .request_timeout()
            .saturating_sub(started.elapsed());
        if remaining.is_zero() {
            cancellation.cancel();
            return Err(Status::deadline_exceeded(
                "ProveStream Finalize deadline exceeded",
            ));
        }
        let worker_cancellation = cancellation.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let validated_window = ValidatedWindow::from_incremental(anchor, &artifacts)
                .map_err(PipelineError::Validation)?;
            let hash = run_settlement(SettlementInput {
                submitted_post_batch_calldata: post_batch.abi_calldata,
                validated_window: &validated_window,
                expected_rollup_id: state.expected_rollup_id,
                expected_l2_system_address: state.expected_l2_system_address,
                proof_system_vkey: state.attester.proof_system_vkey(),
                expected_proof_system: state.attester.expected_proof_system(),
                system_transaction_reconstructor: &state.system_transaction_reconstructor,
                cancellation: &worker_cancellation,
            })
            .map_err(PipelineError::Settlement)?;
            Ok::<_, PipelineError>(hash)
        });

        let hash = match timeout(remaining, worker).await {
            Ok(Ok(Ok(hash))) => hash,
            Ok(Ok(Err(error))) => return Err(error.status()),
            Ok(Err(error)) => {
                warn!(?error, "incremental finalization worker failed");
                return Err(Status::internal("incremental finalization worker failed"));
            }
            Err(_) => {
                cancellation.cancel();
                return Err(Status::deadline_exceeded(
                    "ProveStream Finalize deadline exceeded",
                ));
            }
        };
        let signature = {
            let mut session = session.lock().await;
            require_epoch(&session, bound.epoch)?;
            if session.cancellation.is_cancelled() {
                return Err(Status::cancelled("ProveStream Finalize was cancelled"));
            }
            let signature = self
                .state
                .attester
                .sign(hash)
                .map_err(|_| Status::internal("attestation signing failed"))?;
            session.anchor = anchor;
            session.blocks.drain(..start_index);
            signature
        };
        event!(
            name: "eez.proof_signer.window_signed",
            Level::INFO,
            event_name = "eez.proof_signer.window_signed",
            session_id = %bound.id,
            from = finalize.from_block,
            to = finalize.to_block,
            elapsed_us = started.elapsed().as_micros(),
            "incremental window signed",
        );
        Ok(Proof {
            public_inputs_hash: hash.into_inner().to_vec(),
            proof: signature.to_vec(),
        })
    }
}

#[tonic::async_trait]
impl Prover for ProveSvc {
    type ProveStreamStream = ResponseStream;

    /// Start the incremental RPC handler and return its server-frame response stream.
    async fn prove_stream(
        &self,
        request: Request<Streaming<ClientFrame>>,
    ) -> Result<Response<Self::ProveStreamStream>, Status> {
        let (sender, receiver) = mpsc::channel(8);
        self.incremental
            .active_streams
            .fetch_add(1, Ordering::AcqRel);
        let runtime = Arc::clone(&self.incremental);
        tokio::spawn(runtime.serve_stream(request.into_inner(), sender));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

fn require_bound(
    bound: Option<&BoundSession>,
    submitted: &[u8],
    epoch: u64,
) -> Result<BoundSession, FrameError> {
    let id = parse_session_id(submitted).map_err(|status| (None, status))?;
    let Some(bound) = bound.copied() else {
        return Err((
            None,
            Status::permission_denied("stream is not bound to a session"),
        ));
    };
    if bound.id != id || bound.epoch != epoch {
        return Err((
            None,
            Status::permission_denied("frame does not belong to this stream's session"),
        ));
    }
    Ok(bound)
}

fn authorized_bound(
    bound: Option<&BoundSession>,
    submitted: &[u8],
    epoch: u64,
) -> Option<BoundSession> {
    let bound = bound.copied()?;
    (bound.epoch == epoch && submitted == bound.id.as_slice()).then_some(bound)
}

fn require_epoch(session: &Session, epoch: u64) -> Result<(), Status> {
    if session.epoch == epoch {
        Ok(())
    } else {
        Err(Status::aborted("prover session epoch is stale"))
    }
}

fn next_epoch(epoch: u64) -> u64 {
    epoch.wrapping_add(1).max(1)
}

fn parse_session_id(bytes: &[u8]) -> Result<SessionId, Status> {
    B256::try_from(bytes)
        .map_err(|_| Status::invalid_argument("session ID must contain exactly 32 bytes"))
}

fn parse_b256(name: &'static str, bytes: &[u8]) -> Result<B256, Status> {
    B256::try_from(bytes)
        .map_err(|_| Status::invalid_argument(format!("{name} must contain exactly 32 bytes")))
}

fn validated_response(artifact: &ValidatedBlockArtifact, reused: bool) -> Validated {
    Validated {
        number: artifact.number,
        hash: artifact.hash.to_vec(),
        post_state_root: artifact.post_state_root.to_vec(),
        reused,
    }
}

fn validation_status(error: ValidationError) -> Status {
    PipelineError::Validation(error).status()
}

fn server_frame(bound: BoundSession, request_id: u64, kind: server_frame::Kind) -> ServerFrame {
    ServerFrame {
        session_id: bound.id.to_vec(),
        request_id,
        epoch: bound.epoch,
        kind: Some(kind),
    }
}

impl IncrementalRuntime {
    async fn rejected_frame(
        &self,
        authorized: Option<BoundSession>,
        request_id: u64,
        status: Status,
    ) -> ServerFrame {
        let (session_id, epoch, validated_through, validated_hash) = if let Some(bound) = authorized
        {
            match self.session(bound.id).await {
                Ok(session) => {
                    let session = session.lock().await;
                    if session.epoch == bound.epoch {
                        (
                            bound.id.to_vec(),
                            bound.epoch,
                            session.tip().number,
                            session.tip().hash.to_vec(),
                        )
                    } else {
                        (Vec::new(), 0, 0, Vec::new())
                    }
                }
                Err(_) => (Vec::new(), 0, 0, Vec::new()),
            }
        } else {
            (Vec::new(), 0, 0, Vec::new())
        };
        ServerFrame {
            session_id,
            request_id,
            epoch,
            kind: Some(server_frame::Kind::Rejected(Rejected {
                code: i32::from(status.code()),
                message: status.message().to_owned(),
                details: status.details().to_vec(),
                validated_through,
                validated_hash,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    use alloy_primitives::{Address, Bytes, I256, U256, b256};
    use alloy_rpc_types_debug::ExecutionWitness;
    use alloy_sol_types::SolCall as _;
    use eez_control_rpc::v1::{BlockWitness, ExecutionWitness as WireWitness};
    use eez_control_rpc::v2::prover_client::ProverClient as StreamingClient;
    use eez_protocol::abi::{ExecutionEntrySol, RollupIdWithProofSystemsSol, StateUpdateSol};
    use eez_prover::{Prover as _, ProvingAnchor, ProvingContext};
    use eez_prover_client::RemoteProver;

    use super::*;
    use crate::attest::Attester;
    use crate::service::tests::TestServer;
    use crate::testkit::{TEST_SYSTEM_ADDRESS, test_proof_system_vkey};
    use crate::validate::{
        BackendBlockOutput, BackendWindowOutput, IncrementalBlockOutput, SettlementBlockEvidence,
        ValidationBackend,
    };
    use tokio_stream::wrappers::ReceiverStream as TokioReceiverStream;

    #[derive(Debug, Clone)]
    struct CountingBackend {
        validations: Arc<AtomicUsize>,
        delay: Duration,
        blocks: Arc<Mutex<HashMap<B256, TestCachedBlock>>>,
    }

    #[derive(Debug, Clone)]
    struct TestCachedBlock {
        rlp: Vec<u8>,
        output: IncrementalBlockOutput,
    }

    #[async_trait::async_trait]
    impl ValidationBackend for CountingBackend {
        fn label(&self) -> &'static str {
            "counting"
        }

        fn chain_id(&self) -> u64 {
            1
        }

        fn expected_l2_system_address(&self) -> Address {
            TEST_SYSTEM_ADDRESS
        }

        fn validate_blocks(
            &self,
            _blocks: &[AdmittedBlock],
            _witnesses: &mut [ExecutionWitness],
            _cancellation: &CancellationToken,
        ) -> Result<BackendWindowOutput, ValidationError> {
            unreachable!("v1 is not used by this test")
        }

        fn begin_incremental(&self, _anchor: IncrementalAnchor) -> Result<(), ValidationError> {
            Ok(())
        }

        async fn validate_next(
            &self,
            _anchor: IncrementalAnchor,
            parent: IncrementalAnchor,
            block: &AdmittedBlock,
            _witness: ExecutionWitness,
            request_timeout: Duration,
        ) -> Result<(IncrementalBlockOutput, bool), ValidationError> {
            if let Some(cached) = self.blocks.lock().await.get(&block.claimed_hash()).cloned() {
                if cached.rlp != block.rlp() || cached.output.pre_state_root != parent.state_root {
                    return Err(ValidationError::Rejected(
                        "cached block or parent mismatch".to_owned(),
                    ));
                }
                return Ok((cached.output, true));
            }
            self.validations.fetch_add(1, Ordering::SeqCst);
            timeout(request_timeout, tokio::time::sleep(self.delay))
                .await
                .map_err(|_| ValidationError::DeadlineExceeded)?;
            let decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(block.rlp()).unwrap();
            let post_root = decoded.header.state_root;
            let output = IncrementalBlockOutput {
                pre_state_root: parent.state_root,
                block: BackendBlockOutput {
                    decoded_number: block.declared_number(),
                    decoded_parent_hash: block.claimed_parent_hash(),
                    computed_hash: block.claimed_hash(),
                    decoded_transaction_count: 0,
                    receipt_successes: Vec::new(),
                    transaction_state_checkpoints: Vec::new(),
                    post_state_root: post_root,
                    settlement_evidence: SettlementBlockEvidence {
                        system_sender_flags: Vec::new(),
                        observed_outbound_events: Vec::new(),
                    },
                },
            };
            let mut blocks = self.blocks.lock().await;
            let cached = blocks
                .entry(block.claimed_hash())
                .or_insert_with(|| TestCachedBlock {
                    rlp: block.rlp().to_vec(),
                    output,
                });
            Ok((cached.output.clone(), false))
        }
    }

    fn begin() -> eez_control_rpc::v2::Begin {
        eez_control_rpc::v2::Begin {
            rollup_id: 1,
            anchor_number: 10,
            anchor_hash: vec![0x11; 32],
            anchor_state_root: vec![0x55; 32],
        }
    }

    async fn open_stream(
        client: &mut StreamingClient<tonic::transport::Channel>,
        initial: ClientFrame,
    ) -> (mpsc::Sender<ClientFrame>, Streaming<ServerFrame>) {
        let (requests, stream) = mpsc::channel(8);
        requests.send(initial).await.unwrap();
        let responses = client
            .prove_stream(TokioReceiverStream::new(stream))
            .await
            .unwrap()
            .into_inner();
        (requests, responses)
    }

    fn limits() -> ServiceLimits {
        limits_with_timeouts(Duration::from_secs(1), Duration::from_secs(1))
    }

    fn limits_with_timeouts(
        stream_idle_timeout: Duration,
        request_timeout: Duration,
    ) -> ServiceLimits {
        ServiceLimits::new(super::super::ServiceLimitsParams {
            max_window_blocks: NonZeroUsize::new(8).unwrap(),
            max_window_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            max_window_witness_items: NonZeroUsize::new(64).unwrap(),
            stream_idle_timeout,
            request_timeout,
        })
        .unwrap()
    }

    fn state(validations: Arc<AtomicUsize>) -> Arc<ServiceState> {
        state_with_delay(validations, Duration::from_millis(25))
    }

    fn state_with_delay(validations: Arc<AtomicUsize>, delay: Duration) -> Arc<ServiceState> {
        let attester = Attester::new(
            b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"),
            test_proof_system_vkey(),
            Address::repeat_byte(0xaa),
            TEST_SYSTEM_ADDRESS,
        )
        .unwrap();
        Arc::new(
            ServiceState::new(
                crate::validate::Validator::from_backend(CountingBackend {
                    validations,
                    delay,
                    blocks: Arc::default(),
                }),
                NonZeroU64::new(1).unwrap(),
                attester,
            )
            .unwrap(),
        )
    }

    fn block() -> eez_control_rpc::v2::Block {
        let block = eez_primitives::Block::new(
            alloy_consensus::Header {
                state_root: B256::repeat_byte(0x33),
                ..Default::default()
            },
            eez_primitives::BlockBody::default(),
        );
        eez_control_rpc::v2::Block {
            data: Some(BlockWitness {
                number: 11,
                hash: vec![0x22; 32],
                parent_hash: vec![0x11; 32],
                rlp: alloy_rlp::encode(block),
                witness: Some(WireWitness {
                    state: vec![vec![0x44]],
                    ..WireWitness::default()
                }),
            }),
        }
    }

    fn block_after(number: u64, parent_hash: B256, marker: u8) -> eez_control_rpc::v2::Block {
        let header = alloy_consensus::Header {
            number,
            parent_hash,
            state_root: B256::repeat_byte(marker),
            timestamp: u64::from(marker),
            ..Default::default()
        };
        let hash = header.hash_slow();
        let block = eez_primitives::Block::new(header, eez_primitives::BlockBody::default());
        eez_control_rpc::v2::Block {
            data: Some(BlockWitness {
                number,
                hash: hash.to_vec(),
                parent_hash: parent_hash.to_vec(),
                rlp: alloy_rlp::encode(block),
                witness: Some(WireWitness {
                    state: vec![vec![0x44]],
                    ..WireWitness::default()
                }),
            }),
        }
    }

    fn post_batch(
        anchor: B256,
        terminal: B256,
        block_count: usize,
    ) -> eez_control_rpc::v1::PostBatch {
        let mut batch = eez_protocol::EvmBatch::default();
        batch.entries.push(ExecutionEntrySol {
            stateUpdates: vec![StateUpdateSol {
                rollupId: 1,
                currentState: anchor,
                newState: terminal,
                etherDelta: I256::ZERO,
            }],
            proxyEntryHash: B256::ZERO,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: 1,
            success: true,
            returnData: Bytes::new(),
        });
        batch.immediateEntryCount = U256::from(1);
        batch.proofSystems = vec![Address::repeat_byte(0xaa)];
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: 1,
            proofSystemIndexes: vec![0],
        }];
        batch.callData =
            crate::settlement::encode_da_payload_for(1, &vec![Vec::new(); block_count], &[]).into();
        eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch).unwrap();
        eez_control_rpc::v1::PostBatch {
            abi_calldata: eez_protocol::entries::encode_postbatch(&batch),
            ..Default::default()
        }
    }

    fn remote_block(number: u64, parent: B256, marker: u8) -> eez_prover::BlockWitness {
        let block = block_after(number, parent, marker).data.unwrap();
        eez_prover::BlockWitness {
            number,
            hash: B256::from_slice(&block.hash),
            parent_hash: parent,
            rlp: block.rlp.into(),
            witness: into_execution_witness(block.witness.unwrap()),
        }
    }

    fn remote_context(
        anchor: ProvingAnchor,
        blocks: Vec<eez_prover::BlockWitness>,
    ) -> ProvingContext {
        let terminal = blocks.last().unwrap();
        let calldata = post_batch(anchor.hash, terminal.hash, blocks.len()).abi_calldata;
        ProvingContext {
            rollup_id: 1,
            from_block: blocks[0].number,
            to_block: terminal.number,
            anchor: Some(anchor),
            batch: eez_protocol::abi::postAndVerifyBatchCall::abi_decode(&calldata)
                .unwrap()
                .batch,
            blocks,
            l1_block_hash: None,
        }
    }

    #[tokio::test]
    async fn remote_client_recovers_from_server_progress_rewind_and_session_loss() {
        let validations = Arc::new(AtomicUsize::new(0));
        let state = state(Arc::clone(&validations));
        let attester = state.attester.address();
        let service = ProveSvc::new(state, limits());
        let runtime = Arc::clone(&service.incremental);
        let server = TestServer::with_service(service).await;
        let prover = RemoteProver::new(&server.endpoint, attester);
        let anchor = ProvingAnchor {
            number: 10,
            hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x55),
        };
        let first = remote_block(11, anchor.hash, 0x11);
        let second = remote_block(12, first.hash, 0x12);
        let third = remote_block(13, second.hash, 0x13);
        prover.prevalidate(1, anchor, first.clone()).await.unwrap();
        let id = *runtime
            .registry
            .lock()
            .await
            .sessions
            .keys()
            .next()
            .unwrap();
        let session = runtime.session(id).await.unwrap();
        let bound = BoundSession {
            id,
            epoch: session.lock().await.epoch,
        };

        // Simulate a completed Block whose response RemoteProver never observed.
        runtime
            .validate_block(bound, block_after(12, first.hash, 0x12))
            .await
            .unwrap();
        prover.prevalidate(1, anchor, second.clone()).await.unwrap();
        assert_eq!(session.lock().await.tip().hash, second.hash);
        assert_eq!(validations.load(Ordering::SeqCst), 2);

        // The next Resume must also accept a server cursor that moved backwards.
        let bound = BoundSession {
            id,
            epoch: session.lock().await.epoch,
        };
        runtime.rewind(bound, first.hash).await.unwrap();
        let context = remote_context(anchor, vec![first, second, third.clone()]);
        assert_eq!(prover.prove(context.clone()).await.unwrap().len(), 65);
        assert_eq!(session.lock().await.tip().hash, third.hash);
        assert_eq!(validations.load(Ordering::SeqCst), 3);

        // Lost retained session state produces NotFound without a session binding.
        *runtime.registry.lock().await = Registry::default();
        assert_eq!(prover.prove(context).await.unwrap().len(), 65);
        let registry = runtime.registry.lock().await;
        assert_eq!(registry.sessions.len(), 1);
        assert!(!registry.sessions.contains_key(&id));
        assert_eq!(
            validations.load(Ordering::SeqCst),
            3,
            "backfill must reuse the backend cache"
        );
        drop(registry);
        prover.cancel_session().await.unwrap();
        prover.cancel_session().await.unwrap();
        assert!(runtime.registry.lock().await.sessions.is_empty());
    }

    #[tokio::test]
    async fn remote_client_leaves_rewind_and_anchor_promotion_to_the_server() {
        let validations = Arc::new(AtomicUsize::new(0));
        let state = state(Arc::clone(&validations));
        let attester = state.attester.address();
        let service = ProveSvc::new(state, limits());
        let runtime = Arc::clone(&service.incremental);
        let server = TestServer::with_service(service).await;
        let prover = RemoteProver::new(&server.endpoint, attester);
        let anchor = ProvingAnchor {
            number: 10,
            hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x55),
        };
        let first = remote_block(11, anchor.hash, 0x11);
        let second = remote_block(12, first.hash, 0x12);
        let third = remote_block(13, second.hash, 0x13);
        for block in [&first, &second, &third] {
            prover.prevalidate(1, anchor, block.clone()).await.unwrap();
        }
        let id = *runtime
            .registry
            .lock()
            .await
            .sessions
            .keys()
            .next()
            .unwrap();
        let session = runtime.session(id).await.unwrap();
        let promoted = ProvingAnchor {
            number: 11,
            hash: first.hash,
            state_root: B256::repeat_byte(0x11),
        };
        prover
            .prove(remote_context(
                promoted,
                vec![second.clone(), third.clone()],
            ))
            .await
            .unwrap();
        assert_eq!(session.lock().await.anchor.hash, promoted.hash);

        // A delayed prevalidation can resend an older block without local history.
        prover
            .prevalidate(1, promoted, second.clone())
            .await
            .unwrap();
        assert_eq!(session.lock().await.tip().hash, second.hash);
        prover.prevalidate(1, promoted, third).await.unwrap();
        assert_eq!(validations.load(Ordering::SeqCst), 3);

        // Replace a suffix whose hash disagrees with the server's current tip.
        let replacement = remote_block(13, second.hash, 0x99);
        prover
            .prevalidate(1, promoted, replacement.clone())
            .await
            .unwrap();
        assert_eq!(session.lock().await.tip().hash, replacement.hash);
        assert_eq!(session.lock().await.blocks.len(), 2);
        prover
            .prove(remote_context(promoted, vec![second, replacement]))
            .await
            .unwrap();
        assert_eq!(validations.load(Ordering::SeqCst), 4);

        // The old anchor was pruned by Finalize. A request needing it starts anew.
        prover
            .prove(remote_context(anchor, vec![first]))
            .await
            .unwrap();
        assert_eq!(runtime.registry.lock().await.sessions.len(), 2);
        assert_eq!(validations.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn invalid_blocks_leave_the_session_unchanged_without_backend_execution() {
        for defect in [
            "missing data",
            "short hash",
            "long hash",
            "short parent hash",
            "long parent hash",
            "missing witness",
            "wrong number",
            "wrong parent",
        ] {
            let validations = Arc::new(AtomicUsize::new(0));
            let runtime = IncrementalRuntime::new(state(Arc::clone(&validations)), limits());
            let (bound, _) = runtime.begin(begin()).await.unwrap();
            let mut submitted = block();
            let data = submitted.data.as_mut().unwrap();
            match defect {
                "missing data" => submitted.data = None,
                "short hash" => data.hash.truncate(31),
                "long hash" => data.hash.push(0),
                "short parent hash" => data.parent_hash.truncate(31),
                "long parent hash" => data.parent_hash.push(0),
                "missing witness" => data.witness = None,
                "wrong number" => data.number += 1,
                "wrong parent" => data.parent_hash = vec![0xff; 32],
                _ => unreachable!(),
            }
            let error = runtime.validate_block(bound, submitted).await.unwrap_err();
            let expected_code = match defect {
                "wrong number" | "wrong parent" => tonic::Code::FailedPrecondition,
                _ => tonic::Code::InvalidArgument,
            };
            assert_eq!(error.code(), expected_code, "{defect}");
            assert_eq!(validations.load(Ordering::SeqCst), 0, "{defect}");
            {
                let session = runtime.session(bound.id).await.unwrap();
                let session = session.lock().await;
                assert!(session.blocks.is_empty(), "{defect}");
                assert_eq!(session.tip(), session.anchor, "{defect}");
            }

            // A rejected request must not consume quota or poison the next valid submission.
            let validated = runtime.validate_block(bound, block()).await.unwrap();
            assert_eq!(validated.number, 11, "{defect}");
            assert_eq!(validations.load(Ordering::SeqCst), 1, "{defect}");
        }
    }

    #[tokio::test]
    async fn cached_blocks_must_extend_the_receiving_sessions_exact_parent() {
        let validations = Arc::new(AtomicUsize::new(0));
        let runtime = IncrementalRuntime::new(state(Arc::clone(&validations)), limits());
        let begin = begin();
        let (source, _) = runtime.begin(begin.clone()).await.unwrap();
        runtime.validate_block(source, block()).await.unwrap();

        for defect in ["number", "hash", "state root"] {
            let mut other_begin = begin.clone();
            match defect {
                "number" => other_begin.anchor_number -= 1,
                "hash" => other_begin.anchor_hash = vec![0xff; 32],
                "state root" => other_begin.anchor_state_root = vec![0xff; 32],
                _ => unreachable!(),
            }
            let (receiver, _) = runtime.begin(other_begin).await.unwrap();
            let error = runtime.validate_block(receiver, block()).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{defect}");
            let session = runtime.session(receiver.id).await.unwrap();
            let session = session.lock().await;
            assert!(session.blocks.is_empty(), "{defect}");
            assert_eq!(session.tip(), session.anchor, "{defect}");
        }
        assert_eq!(validations.load(Ordering::SeqCst), 1);

        let (receiver, _) = runtime.begin(begin).await.unwrap();
        assert!(
            runtime
                .validate_block(receiver, block())
                .await
                .unwrap()
                .reused
        );
        assert_eq!(validations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn parallel_sessions_allow_duplicate_misses_without_cross_wiring() {
        let validations = Arc::new(AtomicUsize::new(0));
        let runtime = IncrementalRuntime::new(state(Arc::clone(&validations)), limits());
        let begin = begin();
        let (session_a, _) = runtime.begin(begin.clone()).await.unwrap();
        let (session_b, _) = runtime.begin(begin.clone()).await.unwrap();
        assert_ne!(session_a, session_b);

        let (a, b) = tokio::join!(
            runtime.validate_block(session_a, block()),
            runtime.validate_block(session_b, block()),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_eq!(a.number, 11);
        assert_eq!(b.number, 11);
        assert!(!a.reused);
        assert!(!b.reused);
        assert_eq!(validations.load(Ordering::SeqCst), 2);

        let (later, _) = runtime.begin(begin).await.unwrap();
        assert!(runtime.validate_block(later, block()).await.unwrap().reused);
        assert_eq!(validations.load(Ordering::SeqCst), 2);

        let a = runtime.session(session_a.id).await.unwrap();
        let b = runtime.session(session_b.id).await.unwrap();
        let a = a.lock().await;
        let b = b.lock().await;
        assert_eq!(a.blocks[0].validated, b.blocks[0].validated);
        assert_eq!(a.tip().hash, B256::repeat_byte(0x22));
        assert_eq!(b.tip().hash, B256::repeat_byte(0x22));
    }

    #[tokio::test]
    async fn finalize_selects_a_validated_suffix_and_promotes_its_predecessor() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let anchor_hash = B256::repeat_byte(0x11);
        let (bound, _) = runtime.begin(begin()).await.unwrap();
        let block_11 = block_after(11, anchor_hash, 0x11);
        let hash_11 = B256::from_slice(&block_11.data.as_ref().unwrap().hash);
        runtime.validate_block(bound, block_11).await.unwrap();
        let block_12 = block_after(12, hash_11, 0x12);
        let hash_12 = B256::from_slice(&block_12.data.as_ref().unwrap().hash);
        runtime.validate_block(bound, block_12).await.unwrap();

        let proof = runtime
            .finalize(
                bound,
                eez_control_rpc::v2::Finalize {
                    from_block: 12,
                    to_block: 12,
                    terminal_hash: hash_12.to_vec(),
                    post_batch: Some(post_batch(hash_11, hash_12, 1)),
                },
            )
            .await
            .unwrap();

        assert_eq!(proof.proof.len(), 65);
        let session = runtime.session(bound.id).await.unwrap();
        let session = session.lock().await;
        assert_eq!(session.anchor.number, 11);
        assert_eq!(session.anchor.hash, hash_11);
        assert_eq!(session.blocks.len(), 1);
        assert_eq!(session.blocks[0].validated.number, 12);
    }

    #[tokio::test]
    async fn rewind_and_finalize_release_only_the_removed_blocks_quota() {
        let anchor_hash = B256::repeat_byte(0x11);
        let first = block_after(11, anchor_hash, 0x11);
        let hash_11 = B256::from_slice(&first.data.as_ref().unwrap().hash);
        let second = block_after(12, hash_11, 0x12);
        let hash_12 = B256::from_slice(&second.data.as_ref().unwrap().hash);
        let third = block_after(13, hash_12, 0x13);
        // Each dimension independently permits exactly two of these blocks.
        for quota in ["blocks", "payload", "witness"] {
            let mut limits = limits();
            match quota {
                "blocks" => limits.max_blocks = 2,
                "payload" => {
                    limits.max_payload_bytes = first.data.as_ref().unwrap().encoded_len()
                        + second.data.as_ref().unwrap().encoded_len();
                }
                "witness" => limits.max_witness_items = 2,
                _ => unreachable!(),
            }
            let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits);
            let (bound, _) = runtime.begin(begin()).await.unwrap();
            runtime.validate_block(bound, first.clone()).await.unwrap();
            runtime.validate_block(bound, second.clone()).await.unwrap();
            let error = runtime
                .validate_block(bound, third.clone())
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted, "{quota}");

            let (rewound, _) = runtime.rewind(bound, hash_11).await.unwrap();
            assert!(
                runtime
                    .validate_block(rewound, second.clone())
                    .await
                    .unwrap()
                    .reused
            );
            assert_eq!(
                runtime
                    .validate_block(rewound, third.clone())
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::ResourceExhausted,
                "{quota}: rewind must retain block 11's charge"
            );

            runtime
                .finalize(
                    rewound,
                    eez_control_rpc::v2::Finalize {
                        from_block: 12,
                        to_block: 12,
                        terminal_hash: hash_12.to_vec(),
                        post_batch: Some(post_batch(hash_11, hash_12, 1)),
                    },
                )
                .await
                .unwrap();
            let (resumed, ready) = runtime.resume(bound.id).await.unwrap();
            assert_eq!(ready.validated_hash, hash_12.as_slice());
            runtime
                .validate_block(resumed, third.clone())
                .await
                .unwrap();

            // Rewind all the way to the promoted anchor. Neither a separate
            // anchor cursor nor the old block-10 root may be required here.
            let (rewound, ready) = runtime.rewind(resumed, hash_11).await.unwrap();
            assert_eq!(ready.validated_through, 11);
            let replacement = block_after(12, hash_11, 0x99);
            let replacement_hash = B256::from_slice(&replacement.data.as_ref().unwrap().hash);
            runtime.validate_block(rewound, replacement).await.unwrap();
            runtime
                .finalize(
                    rewound,
                    eez_control_rpc::v2::Finalize {
                        from_block: 12,
                        to_block: 12,
                        terminal_hash: replacement_hash.to_vec(),
                        post_batch: Some(post_batch(hash_11, replacement_hash, 1)),
                    },
                )
                .await
                .unwrap();
            let session = runtime.session(bound.id).await.unwrap();
            let session = session.lock().await;
            assert_eq!(session.anchor.state_root, B256::repeat_byte(0x11));
            assert_eq!(
                session.blocks[0].validated.pre_state_root,
                session.anchor.state_root
            );
            assert_eq!(session.tip().state_root, B256::repeat_byte(0x99));
        }
    }

    #[tokio::test]
    async fn backend_cache_hits_keep_each_sessions_own_submission_charges() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let begin = begin();
        let (a, _) = runtime.begin(begin.clone()).await.unwrap();
        let (b, _) = runtime.begin(begin).await.unwrap();
        let small = block();
        let small_bytes = small.data.as_ref().unwrap().encoded_len();
        let mut large = small.clone();
        large
            .data
            .as_mut()
            .unwrap()
            .witness
            .as_mut()
            .unwrap()
            .codes
            .push(vec![0x77; 100]);
        let large_bytes = large.data.as_ref().unwrap().encoded_len();
        runtime.validate_block(a, small.clone()).await.unwrap();
        assert!(runtime.validate_block(b, large).await.unwrap().reused);
        runtime.validate_block(a, small).await.unwrap(); // Duplicate adds no charge.
        let a = runtime.session(a.id).await.unwrap();
        let b = runtime.session(b.id).await.unwrap();
        let a = a.lock().await;
        let b = b.lock().await;
        assert_eq!(a.blocks.len(), 1);
        assert_eq!(a.blocks[0].validated, b.blocks[0].validated);
        assert_eq!(
            (a.blocks[0].payload_bytes, a.blocks[0].witness_items),
            (small_bytes, 1)
        );
        assert_eq!(
            (b.blocks[0].payload_bytes, b.blocks[0].witness_items),
            (large_bytes, 2)
        );
        assert!(large_bytes > small_bytes);
    }

    #[tokio::test]
    async fn rejected_carries_the_authoritative_validated_cursor() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let (bound, _) = runtime.begin(begin()).await.unwrap();
        runtime.validate_block(bound, block()).await.unwrap();

        let response = runtime
            .rejected_frame(
                Some(bound),
                44,
                Status::invalid_argument("synthetic rejected block"),
            )
            .await;
        let Some(server_frame::Kind::Rejected(rejected)) = response.kind else {
            panic!("expected Rejected");
        };
        assert_eq!(response.session_id, bound.id.as_slice());
        assert_eq!(response.epoch, bound.epoch);
        assert_eq!(rejected.validated_through, 11);
        assert_eq!(rejected.validated_hash, vec![0x22; 32]);
    }

    #[tokio::test]
    async fn rewind_fences_the_old_epoch_and_accepts_a_replacement_suffix() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let anchor_hash = B256::repeat_byte(0x11);
        let (bound, _) = runtime.begin(begin()).await.unwrap();
        let block_11 = block_after(11, anchor_hash, 0x11);
        let hash_11 = B256::from_slice(&block_11.data.as_ref().unwrap().hash);
        runtime.validate_block(bound, block_11).await.unwrap();
        let old_12 = block_after(12, hash_11, 0x12);
        runtime.validate_block(bound, old_12).await.unwrap();

        let (rewound, ready) = runtime.rewind(bound, hash_11).await.unwrap();
        assert_ne!(rewound.epoch, bound.epoch);
        assert_eq!(ready.validated_through, 11);
        assert!(
            runtime
                .validate_block(bound, block_after(12, hash_11, 0x13))
                .await
                .is_err()
        );
        let replacement = block_after(12, hash_11, 0x99);
        let replacement_hash = replacement.data.as_ref().unwrap().hash.clone();
        let validated = runtime.validate_block(rewound, replacement).await.unwrap();
        assert_eq!(validated.hash, replacement_hash);
    }

    #[tokio::test]
    async fn eviction_skips_connected_sessions_and_preserves_creation_order() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let begin = begin();
        let mut sessions = Vec::new();
        for _ in 0..MAX_RETAINED_SESSIONS {
            sessions.push(runtime.begin(begin.clone()).await.unwrap().0);
        }
        assert_eq!(
            runtime.begin(begin.clone()).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );

        // Set up two disconnected candidates in reverse creation order.
        for index in [8, 5] {
            runtime
                .session(sessions[index].id)
                .await
                .unwrap()
                .lock()
                .await
                .connected = false;
        }
        let (replacement, _) = runtime.begin(begin.clone()).await.unwrap();
        assert!(runtime.session(sessions[5].id).await.is_err());
        assert!(runtime.session(sessions[8].id).await.is_ok());
        assert!(runtime.session(sessions[0].id).await.is_ok());
        assert!(
            runtime
                .session(replacement.id)
                .await
                .unwrap()
                .lock()
                .await
                .connected
        );
        assert_eq!(
            runtime.registry.lock().await.sessions.len(),
            MAX_RETAINED_SESSIONS
        );

        // Resuming the remaining candidate must protect it from eviction.
        let (resumed, _) = runtime.resume(sessions[8].id).await.unwrap();
        assert_ne!(resumed.epoch, sessions[8].epoch);
        assert!(
            runtime
                .session(resumed.id)
                .await
                .unwrap()
                .lock()
                .await
                .connected
        );
        assert_eq!(
            runtime.begin(begin).await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[tokio::test]
    async fn stream_teardown_disconnects_only_the_current_session_epoch() {
        let service = ProveSvc::new(
            state(Arc::new(AtomicUsize::new(0))),
            limits_with_timeouts(Duration::from_secs(5), Duration::from_secs(1)),
        );
        let runtime = Arc::clone(&service.incremental);
        let server = TestServer::with_service(service).await;
        let mut client = server.streaming_client().await;
        let (old_requests, mut old_responses) = open_stream(
            &mut client,
            ClientFrame {
                session_id: Vec::new(),
                request_id: 1,
                epoch: 0,
                kind: Some(client_frame::Kind::Begin(begin())),
            },
        )
        .await;
        let original = old_responses.message().await.unwrap().unwrap();
        assert!(matches!(original.kind, Some(server_frame::Kind::Ready(_))));
        let session = runtime
            .session(B256::from_slice(&original.session_id))
            .await
            .unwrap();
        assert!(session.lock().await.connected);

        let (requests, mut responses) = open_stream(
            &mut client,
            ClientFrame {
                session_id: original.session_id.clone(),
                request_id: 2,
                epoch: 0,
                kind: Some(client_frame::Kind::Resume(eez_control_rpc::v2::Resume {})),
            },
        )
        .await;
        let resumed = responses.message().await.unwrap().unwrap();
        assert!(matches!(resumed.kind, Some(server_frame::Kind::Ready(_))));
        assert_ne!(resumed.epoch, original.epoch);
        assert_eq!(runtime.active_streams.load(Ordering::Acquire), 2);

        // End the old stream only after the new attachment has succeeded.
        drop(old_requests);
        assert!(old_responses.message().await.unwrap().is_none());
        timeout(Duration::from_secs(1), async {
            while runtime.active_streams.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        {
            let session = session.lock().await;
            assert_eq!(session.epoch, resumed.epoch);
            assert!(
                session.connected,
                "stale teardown must not detach the resumed stream"
            );
        }

        requests
            .send(ClientFrame {
                session_id: resumed.session_id,
                request_id: 3,
                epoch: resumed.epoch,
                kind: Some(client_frame::Kind::Rewind(eez_control_rpc::v2::Rewind {
                    ancestor_hash: vec![0x11; 32],
                })),
            })
            .await
            .unwrap();
        let rewound = responses.message().await.unwrap().unwrap();
        assert!(matches!(rewound.kind, Some(server_frame::Kind::Ready(_))));
        assert_ne!(rewound.epoch, resumed.epoch);
        assert!(session.lock().await.connected);

        drop(requests);
        assert!(responses.message().await.unwrap().is_none());
        timeout(Duration::from_secs(1), runtime.wait_until_idle())
            .await
            .unwrap();
        {
            let session = session.lock().await;
            assert_eq!(session.epoch, rewound.epoch);
            assert!(
                !session.connected,
                "current teardown must detach the session"
            );
        }
        drop(server);
    }

    #[tokio::test]
    async fn cancel_is_acknowledged_and_retires_only_its_session() {
        let runtime = IncrementalRuntime::new(state(Arc::new(AtomicUsize::new(0))), limits());
        let begin = begin();
        let (cancelled, _) = runtime.begin(begin.clone()).await.unwrap();
        let (survivor, _) = runtime.begin(begin).await.unwrap();
        let mut bound = Some(cancelled);
        let (response, close) = runtime
            .handle_frame(
                &mut bound,
                ClientFrame {
                    session_id: cancelled.id.to_vec(),
                    request_id: 9,
                    epoch: cancelled.epoch,
                    kind: Some(client_frame::Kind::Cancel(eez_control_rpc::v2::Cancel {})),
                },
            )
            .await
            .unwrap();

        assert!(close);
        assert!(matches!(
            response.kind,
            Some(server_frame::Kind::Cancelled(_))
        ));
        assert!(runtime.session(cancelled.id).await.is_err());
        assert!(runtime.session(survivor.id).await.is_ok());
    }

    #[tokio::test]
    async fn cancelling_one_session_does_not_prevent_backend_cache_reuse() {
        let validations = Arc::new(AtomicUsize::new(0));
        let runtime = IncrementalRuntime::new(state(Arc::clone(&validations)), limits());
        let begin = begin();
        let (a, _) = runtime.begin(begin.clone()).await.unwrap();
        let (b, _) = runtime.begin(begin).await.unwrap();
        let first = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move { runtime.validate_block(a, block()).await })
        };
        // Cancel after execution starts so the test exercises cache publication after cancellation.
        timeout(Duration::from_secs(1), async {
            while validations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut bound = Some(a);
        let (_, closed) = runtime
            .handle_frame(
                &mut bound,
                ClientFrame {
                    session_id: a.id.to_vec(),
                    request_id: 3,
                    epoch: a.epoch,
                    kind: Some(client_frame::Kind::Cancel(eez_control_rpc::v2::Cancel {})),
                },
            )
            .await
            .unwrap();
        assert!(closed);
        let _ = first.await.unwrap(); // A cancelled session must not stop backend cache publication.
        let survivor = runtime.validate_block(b, block()).await.unwrap();
        assert!(survivor.reused);
        assert_eq!(validations.load(Ordering::SeqCst), 1);
        assert!(runtime.session(a.id).await.is_err());
        assert_eq!(
            runtime.session(b.id).await.unwrap().lock().await.tip().hash,
            B256::repeat_byte(0x22)
        );
    }

    #[tokio::test]
    async fn cancel_overtakes_in_flight_validation_and_no_old_response_follows() {
        let service = ProveSvc::new(
            state_with_delay(Arc::new(AtomicUsize::new(0)), Duration::from_millis(250)),
            limits(),
        );
        let server = TestServer::with_service(service).await;
        let mut client = server.streaming_client().await;
        let (requests, mut responses) = open_stream(
            &mut client,
            ClientFrame {
                session_id: Vec::new(),
                request_id: 1,
                epoch: 0,
                kind: Some(client_frame::Kind::Begin(begin())),
            },
        )
        .await;
        let ready = responses.message().await.unwrap().unwrap();
        assert!(matches!(ready.kind, Some(server_frame::Kind::Ready(_))));
        requests
            .send(ClientFrame {
                session_id: ready.session_id.clone(),
                request_id: 2,
                epoch: ready.epoch,
                kind: Some(client_frame::Kind::Block(block())),
            })
            .await
            .unwrap();
        requests
            .send(ClientFrame {
                session_id: ready.session_id,
                request_id: 3,
                epoch: ready.epoch,
                kind: Some(client_frame::Kind::Cancel(eez_control_rpc::v2::Cancel {})),
            })
            .await
            .unwrap();

        let cancelled = tokio::time::timeout(Duration::from_millis(100), responses.message())
            .await
            .expect("Cancel must not wait for block execution")
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.request_id, 3);
        assert!(matches!(
            cancelled.kind,
            Some(server_frame::Kind::Cancelled(_))
        ));
        assert!(responses.message().await.unwrap().is_none());
        drop(server);
    }

    #[tokio::test]
    async fn active_block_work_uses_the_request_deadline_not_the_stream_idle_timeout() {
        let service = ProveSvc::new(
            state_with_delay(Arc::new(AtomicUsize::new(0)), Duration::from_millis(100)),
            limits_with_timeouts(Duration::from_millis(20), Duration::from_secs(1)),
        );
        let server = TestServer::with_service(service).await;
        let mut client = server.streaming_client().await;
        let (requests, mut responses) = open_stream(
            &mut client,
            ClientFrame {
                session_id: Vec::new(),
                request_id: 1,
                epoch: 0,
                kind: Some(client_frame::Kind::Begin(begin())),
            },
        )
        .await;
        let ready = responses.message().await.unwrap().unwrap();
        requests
            .send(ClientFrame {
                session_id: ready.session_id,
                request_id: 2,
                epoch: ready.epoch,
                kind: Some(client_frame::Kind::Block(block())),
            })
            .await
            .unwrap();

        let validated = tokio::time::timeout(Duration::from_millis(500), responses.message())
            .await
            .expect("block must use the longer request deadline")
            .unwrap()
            .unwrap();
        assert_eq!(validated.request_id, 2);
        assert!(matches!(
            validated.kind,
            Some(server_frame::Kind::Validated(_))
        ));
        drop(requests);
        drop(server);
    }

    #[test]
    fn a_frame_cannot_name_another_streams_session() {
        let a = BoundSession {
            id: B256::repeat_byte(0x01),
            epoch: 4,
        };
        let b = B256::repeat_byte(0x02);
        let error = require_bound(Some(&a), b.as_slice(), a.epoch)
            .unwrap_err()
            .1;
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }
}
