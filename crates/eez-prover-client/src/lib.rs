//! `RemoteProver` — the composer-side streaming prover client.
//!
//! Committed blocks are prevalidated through resumable `prove.v2` sessions.
//! Finalization backfills any missing suffix, binds the exact settlement
//! calldata, verifies the returned attestation, and returns its 65-byte
//! signature. `prove.v1` remains the compatibility fallback.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use eez_control_rpc::v1::{
    BlockWitness as WireBlockWitness, ExecutionWitness as WireWitness, PostBatch, ProveChunk,
    ProveHeader, prove_chunk, prove_failure, prover_client::ProverClient,
};
use eez_control_rpc::v2::{
    Begin as StreamBegin, Block as StreamBlock, Cancel as StreamCancel, ClientFrame,
    Finalize as StreamFinalize, Resume as StreamResume, Rewind as StreamRewind, ServerFrame,
    client_frame, prover_client::ProverClient as StreamingProverClient, server_frame,
};
use eez_prover::{
    ActionableProverFailure, BlockWitness, Prover, ProverError, ProverResult, ProvingAnchor,
    ProvingContext, RetryableProverError,
};
use tokio::sync::Mutex;
use tonic::{Code, Status};
use tracing::{Level, event};

/// A [`Prover`] backed by the v2 streaming service, with v1 fallback.
/// Cheap to clone (`Arc<Inner>`).
#[derive(Debug, Clone)]
pub struct RemoteProver {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The `eez-proof-signer` endpoint, e.g. `http://127.0.0.1:50061`.
    url: String,
    /// The proof system's registered attester. The returned signature MUST
    /// recover to this over the returned `publicInputsHash`, or the proof is
    /// rejected (a wrong/malicious prover cannot forge an attestation).
    attester: Address,
    streaming: Mutex<StreamingState>,
}

#[derive(Debug, Default)]
struct StreamingState {
    supported: Option<bool>,
    session: Option<StreamingSession>,
    next_request_id: u64,
}

#[derive(Debug)]
struct StreamingSession {
    // Transport binding only; the server owns the anchor and validated history.
    rollup_id: u64,
    id: Vec<u8>,
    epoch: u64,
}

impl RemoteProver {
    /// Build a remote prover pointing at `url`, verifying attestations against
    /// the registered `attester`.
    #[must_use]
    pub fn new(url: impl Into<String>, attester: Address) -> Self {
        Self {
            inner: Arc::new(Inner {
                url: url.into(),
                attester,
                streaming: Mutex::new(StreamingState::default()),
            }),
        }
    }

    /// The registered attester this prover verifies against.
    #[must_use]
    pub fn attester(&self) -> Address {
        self.inner.attester
    }

    /// Ask the prover to retire the retained proving session and wait for
    /// its correlated `Cancelled` acknowledgement. A missing session is an
    /// idempotent no-op.
    pub async fn cancel_session(&self) -> ProverResult<()> {
        let mut state = self.inner.streaming.lock().await;
        let Some(session_id) = state.session.as_ref().map(|session| session.id.clone()) else {
            return Ok(());
        };
        let (requests, mut responses, ready) = self.open_stream(&mut state, None, 0, 0).await?;
        if let Some(error) = rejection_error(&ready, 0, 0) {
            if matches!(
                error,
                ProverError::Retryable {
                    kind: RetryableProverError::Aborted,
                    ..
                }
            ) {
                state.session = None;
                return Ok(());
            }
            return Err(error);
        }
        if !matches!(ready.kind, Some(server_frame::Kind::Ready(_))) || ready.epoch == 0 {
            return Err(ProverError::Backend(
                "ProveStream did not acknowledge Resume before Cancel".into(),
            ));
        }
        let cancel_id = state.next_id();
        requests
            .send(ClientFrame {
                session_id: session_id.clone(),
                request_id: cancel_id,
                epoch: ready.epoch,
                kind: Some(client_frame::Kind::Cancel(StreamCancel {})),
            })
            .await
            .map_err(|_| ProverError::Backend("ProveStream closed before Cancel".into()))?;
        let cancelled = next_stream_response(
            &mut responses,
            cancel_id,
            Some(session_id.as_slice()),
            Some(ready.epoch),
            0,
            0,
        )
        .await?;
        if let Some(error) = rejection_error(&cancelled, 0, 0) {
            return Err(error);
        }
        if !matches!(cancelled.kind, Some(server_frame::Kind::Cancelled(_))) {
            return Err(ProverError::Backend(
                "ProveStream did not acknowledge Cancel".into(),
            ));
        }
        state.session = None;
        Ok(())
    }
}

/// Map a [`ProvingContext`] to the ordered `Prove` chunk stream: header first
/// (with the authoritative postBatch calldata), then one chunk per block.
fn chunks_for(ctx: &ProvingContext) -> Vec<ProveChunk> {
    // The authoritative on-chain payload (proofs[] empty — the proof isn't part
    // of the publicInputsHash). The prover decodes THIS to recompute the hash.
    let abi_calldata = eez_protocol::abi::postAndVerifyBatchCall {
        batch: ctx.batch.clone(),
    }
    .abi_encode();

    let mut chunks = Vec::with_capacity(1 + ctx.blocks.len());
    chunks.push(ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id: ctx.rollup_id,
            from_block: ctx.from_block,
            to_block: ctx.to_block,
            post_batch: Some(PostBatch {
                abi_calldata,
                // Empty: the prover recomputes the hash and returns it; we don't
                // pre-claim it (the batch determines it deterministically).
                public_inputs_hash: Vec::new(),
                l1_block_hash: ctx.l1_block_hash.map(|h| h.to_vec()).unwrap_or_default(),
            }),
        })),
    });
    for bw in &ctx.blocks {
        chunks.push(ProveChunk {
            kind: Some(prove_chunk::Kind::Block(wire_block(bw))),
        });
    }
    chunks
}

fn wire_block(block: &BlockWitness) -> WireBlockWitness {
    WireBlockWitness {
        number: block.number,
        hash: block.hash.to_vec(),
        parent_hash: block.parent_hash.to_vec(),
        rlp: block.rlp.to_vec(),
        witness: Some(WireWitness {
            state: block
                .witness
                .state
                .iter()
                .map(|bytes| bytes.to_vec())
                .collect(),
            codes: block
                .witness
                .codes
                .iter()
                .map(|bytes| bytes.to_vec())
                .collect(),
            keys: block
                .witness
                .keys
                .iter()
                .map(|bytes| bytes.to_vec())
                .collect(),
            headers: block
                .witness
                .headers
                .iter()
                .map(|bytes| bytes.to_vec())
                .collect(),
        }),
    }
}

#[derive(Debug)]
enum StreamingError {
    Unsupported,
    Prover(ProverError),
}

impl From<ProverError> for StreamingError {
    fn from(error: ProverError) -> Self {
        Self::Prover(error)
    }
}

impl From<StreamingError> for ProverError {
    fn from(error: StreamingError) -> Self {
        match error {
            StreamingError::Unsupported => {
                Self::Backend("ProveStream unexpectedly unavailable".into())
            }
            StreamingError::Prover(error) => error,
        }
    }
}

impl StreamingState {
    async fn send(
        &mut self,
        requests: &tokio::sync::mpsc::Sender<ClientFrame>,
        kind: client_frame::Kind,
    ) -> ProverResult<u64> {
        let message = match &kind {
            client_frame::Kind::Rewind(_) => "ProveStream closed while sending Rewind",
            client_frame::Kind::Block(_) => "ProveStream closed while sending a block",
            client_frame::Kind::Finalize(_) => "ProveStream closed while sending Finalize",
            _ => unreachable!("only bound block, rewind and finalize requests use this path"),
        };
        let request_id = self.next_id();
        let session = self.session.as_ref().expect("ready session exists");
        requests
            .send(ClientFrame {
                session_id: session.id.clone(),
                request_id,
                epoch: session.epoch,
                kind: Some(kind),
            })
            .await
            .map_err(|_| ProverError::Retryable {
                kind: RetryableProverError::Unavailable,
                message: message.into(),
            })?;
        Ok(request_id)
    }
    async fn response(
        &mut self,
        responses: &mut tonic::Streaming<ServerFrame>,
        request_id: u64,
        range: (u64, u64),
    ) -> Result<ServerFrame, StreamingError> {
        let session = self.session.as_ref().expect("ready session exists");
        let response = next_stream_response(
            responses,
            request_id,
            Some(&session.id),
            Some(session.epoch),
            range.0,
            range.1,
        )
        .await?;
        if let Some(error) = rejection_error(&response, range.0, range.1) {
            if response.session_id.is_empty()
                || error.retryable_kind() == Some(RetryableProverError::Aborted)
            {
                self.session = None;
            }
            return Err(error.into());
        }
        Ok(response)
    }
    fn next_id(&mut self) -> u64 {
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        self.next_request_id
    }
}

async fn next_stream_response(
    responses: &mut tonic::Streaming<ServerFrame>,
    request_id: u64,
    expected_session_id: Option<&[u8]>,
    expected_epoch: Option<u64>,
    from_block: u64,
    to_block: u64,
) -> Result<ServerFrame, StreamingError> {
    let response = responses
        .message()
        .await
        .map_err(|status| map_rpc_status(from_block, to_block, status))?
        .ok_or_else(|| ProverError::Backend("ProveStream ended before its response".into()))?;
    if response.request_id != request_id {
        return Err(ProverError::Backend(format!(
            "ProveStream response {} crossed request {}",
            response.request_id, request_id,
        ))
        .into());
    }
    // An unknown or stale session has no authorized binding to echo.
    let unbound_rejection = matches!(response.kind, Some(server_frame::Kind::Rejected(_)))
        && response.session_id.is_empty()
        && response.epoch == 0;
    if !unbound_rejection
        && expected_session_id.is_some_and(|expected| response.session_id != expected)
    {
        return Err(
            ProverError::Backend("ProveStream response crossed Composer sessions".into()).into(),
        );
    }
    if !unbound_rejection && expected_epoch.is_some_and(|expected| response.epoch != expected) {
        return Err(ProverError::Backend(format!(
            "ProveStream response epoch {} crossed epoch {}",
            response.epoch,
            expected_epoch.expect("checked above"),
        ))
        .into());
    }
    Ok(response)
}

fn rejection_error(response: &ServerFrame, from_block: u64, to_block: u64) -> Option<ProverError> {
    let server_frame::Kind::Rejected(rejected) = response.kind.as_ref()? else {
        return None;
    };
    let status = Status::with_details(
        Code::from_i32(rejected.code),
        rejected.message.clone(),
        rejected.details.clone().into(),
    );
    if status.code() == Code::NotFound {
        Some(ProverError::Retryable {
            kind: RetryableProverError::Aborted,
            message: format!(
                "ProveStream session {from_block}-{to_block} is no longer retained: {status}"
            ),
        })
    } else {
        Some(map_rpc_status(from_block, to_block, status))
    }
}

impl RemoteProver {
    async fn prove_v1(&self, ctx: ProvingContext) -> ProverResult<Bytes> {
        let chunks = chunks_for(&ctx);
        let mut client = ProverClient::connect(self.inner.url.clone())
            .await
            .map_err(|error| ProverError::Retryable {
                kind: RetryableProverError::Unavailable,
                message: format!("connect {}: {error}", self.inner.url),
            })?
            .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
            .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
        let response = client
            .prove(tokio_stream::iter(chunks))
            .await
            .map_err(|status| map_rpc_status(ctx.from_block, ctx.to_block, status))?
            .into_inner();
        self.verify_proof(
            ctx.from_block,
            ctx.to_block,
            response.public_inputs_hash,
            response.signature,
        )
    }

    fn verify_proof(
        &self,
        from_block: u64,
        to_block: u64,
        public_inputs_hash: Vec<u8>,
        signature: Vec<u8>,
    ) -> ProverResult<Bytes> {
        let hash = verify_attestation(&signature, &public_inputs_hash, self.inner.attester)?;
        event!(
            name: "eez.prover_client.attested",
            Level::INFO,
            event_name = "eez.prover_client.attested",
            from = from_block,
            to = to_block,
            %hash,
            "remote prover attested the window",
        );
        Ok(Bytes::from(signature))
    }

    /// Open a stream with Begin/Resume and correlate its first response.
    async fn open_stream(
        &self,
        state: &mut StreamingState,
        begin: Option<StreamBegin>,
        from_block: u64,
        to_block: u64,
    ) -> Result<
        (
            tokio::sync::mpsc::Sender<ClientFrame>,
            tonic::Streaming<ServerFrame>,
            ServerFrame,
        ),
        StreamingError,
    > {
        let mut client = StreamingProverClient::connect(self.inner.url.clone())
            .await
            .map_err(|error| ProverError::Retryable {
                kind: RetryableProverError::Unavailable,
                message: format!("connect {}: {error}", self.inner.url),
            })?
            .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
            .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
        let request_id = state.next_id();
        let expected_session = state.session.as_ref().map(|session| session.id.clone());
        let initial = ClientFrame {
            session_id: expected_session.clone().unwrap_or_default(),
            request_id,
            epoch: 0,
            kind: Some(begin.map_or(
                client_frame::Kind::Resume(StreamResume {}),
                client_frame::Kind::Begin,
            )),
        };
        let (requests, request_stream) = tokio::sync::mpsc::channel(1);
        requests
            .send(initial)
            .await
            .map_err(|_| ProverError::Backend("unable to open ProveStream".into()))?;
        let mut responses = client
            .prove_stream(tokio_stream::wrappers::ReceiverStream::new(request_stream))
            .await
            .map_err(|status| {
                if status.code() == Code::Unimplemented {
                    StreamingError::Unsupported
                } else {
                    map_rpc_status(from_block, to_block, status).into()
                }
            })?
            .into_inner();
        let ready = next_stream_response(
            &mut responses,
            request_id,
            expected_session.as_deref(),
            None,
            from_block,
            to_block,
        )
        .await?;
        Ok((requests, responses, ready))
    }

    async fn stream_exchange(
        &self,
        rollup_id: u64,
        anchor: ProvingAnchor,
        blocks: &[BlockWitness],
        finalize: Option<(u64, u64, PostBatch)>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, StreamingError> {
        let mut state = self.inner.streaming.lock().await;
        if state.supported == Some(false) {
            return Err(StreamingError::Unsupported);
        }
        if state
            .session
            .as_ref()
            .is_some_and(|session| session.rollup_id != rollup_id)
        {
            state.session = None;
        }
        let range = finalize.as_ref().map_or_else(
            || {
                (
                    blocks.first().map_or(anchor.number, |block| block.number),
                    blocks.last().map_or(anchor.number, |block| block.number),
                )
            },
            |(from, to, _)| (*from, *to),
        );
        let finalize = finalize
            .map(|(from_block, to_block, post_batch)| {
                let terminal = blocks
                    .iter()
                    .find(|block| block.number == to_block)
                    .ok_or_else(|| {
                        ProverError::Backend("Finalize terminal block was not supplied".into())
                    })?;
                Ok::<_, ProverError>(StreamFinalize {
                    from_block,
                    to_block,
                    terminal_hash: terminal.hash.to_vec(),
                    post_batch: Some(post_batch),
                })
            })
            .transpose()?;

        // Only the server retains the validated prefix. A fresh Ready determines
        // what to send, including work completed after a previous response was lost.
        let (requests, mut responses, blocks) = loop {
            let creating = state.session.is_none();
            let begin = creating.then(|| StreamBegin {
                rollup_id,
                anchor_number: anchor.number,
                anchor_hash: anchor.hash.to_vec(),
                anchor_state_root: anchor.state_root.to_vec(),
            });
            let (requests, mut responses, response) =
                match self.open_stream(&mut state, begin, range.0, range.1).await {
                    Ok(opened) => opened,
                    Err(StreamingError::Unsupported) => {
                        state.supported = Some(false);
                        state.session = None;
                        return Err(StreamingError::Unsupported);
                    }
                    Err(error) => return Err(error),
                };
            state.supported = Some(true);
            if !creating
                && matches!(&response.kind,
                    Some(server_frame::Kind::Rejected(rejected)) if Code::from_i32(rejected.code) == Code::NotFound
                )
            {
                state.session = None;
                continue; // The server evicted the session or restarted; begin at the supplied anchor.
            }
            if let Some(error) = rejection_error(&response, range.0, range.1) {
                return Err(error.into());
            }
            let Some(server_frame::Kind::Ready(ready)) = response.kind else {
                return Err(ProverError::Backend("ProveStream expected Ready".into()).into());
            };
            if response.session_id.len() != 32
                || response.epoch == 0
                || ready.validated_hash.len() != 32
            {
                return Err(ProverError::Backend(
                    "ProveStream returned an invalid Ready binding".into(),
                )
                .into());
            }
            if creating
                && (ready.validated_through != anchor.number
                    || ready.validated_hash != anchor.hash.as_slice())
            {
                return Err(ProverError::Backend(
                    "ProveStream Begin acknowledged another anchor".into(),
                )
                .into());
            }
            state.session = Some(StreamingSession {
                rollup_id,
                id: response.session_id,
                epoch: response.epoch,
            });

            let skip = blocks
                .iter()
                .position(|block| {
                    block.number == ready.validated_through
                        && block.hash.as_slice() == ready.validated_hash
                })
                .map_or(0, |index| index + 1);
            let remaining = &blocks[skip..];
            if let Some(first) = remaining.first()
                && (ready.validated_through.checked_add(1) != Some(first.number)
                    || ready.validated_hash != first.parent_hash.as_slice())
            {
                // Without a matching cursor, let the server resolve the requested
                // parent. Any resent blocks reuse the backend's validation cache.
                let request_id = state
                    .send(
                        &requests,
                        client_frame::Kind::Rewind(StreamRewind {
                            ancestor_hash: first.parent_hash.to_vec(),
                        }),
                    )
                    .await?;
                let session = state.session.as_ref().expect("ready session exists");
                let response = next_stream_response(
                    &mut responses,
                    request_id,
                    Some(&session.id),
                    None,
                    range.0,
                    range.1,
                )
                .await?;
                if !creating
                    && matches!(&response.kind,
                        Some(server_frame::Kind::Rejected(rejected)) if Code::from_i32(rejected.code) == Code::FailedPrecondition
                    )
                {
                    state.session = None;
                    continue; // The requested parent is no longer retained; backfill a new session.
                }
                if let Some(error) = rejection_error(&response, range.0, range.1) {
                    return Err(error.into());
                }
                let Some(server_frame::Kind::Ready(ready)) = response.kind else {
                    return Err(ProverError::Backend(
                        "ProveStream expected Ready after Rewind".into(),
                    )
                    .into());
                };
                if response.epoch == 0
                    || ready.validated_through.checked_add(1) != Some(first.number)
                    || ready.validated_hash != first.parent_hash.as_slice()
                {
                    state.session = None;
                    return Err(ProverError::Backend(
                        "ProveStream Rewind acknowledged another parent".into(),
                    )
                    .into());
                }
                state.session.as_mut().expect("ready session exists").epoch = response.epoch;
            }
            break (requests, responses, remaining);
        };

        // Pipeline the offered suffix and Finalize; correlation is transport state,
        // not a second copy of the server's validated block history.
        let mut pending_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            let request_id = state
                .send(
                    &requests,
                    client_frame::Kind::Block(StreamBlock {
                        data: Some(wire_block(block)),
                    }),
                )
                .await?;
            pending_blocks.push((request_id, block.number, block.hash));
        }
        let pending_finalize = if let Some(finalize) = finalize {
            Some(
                state
                    .send(&requests, client_frame::Kind::Finalize(finalize))
                    .await?,
            )
        } else {
            None
        };
        for (request_id, number, hash) in pending_blocks {
            let response = state.response(&mut responses, request_id, range).await?;
            if !matches!(response.kind, Some(server_frame::Kind::Validated(validated))
                if validated.number == number && validated.hash == hash.as_slice()
                    && validated.post_state_root.len() == 32)
            {
                return Err(ProverError::Backend(
                    "ProveStream Validated response identity mismatch".into(),
                )
                .into());
            }
        }
        let proof = if let Some(request_id) = pending_finalize {
            let response = state.response(&mut responses, request_id, range).await?;
            let Some(server_frame::Kind::Proof(proof)) = response.kind else {
                return Err(ProverError::Backend(
                    "ProveStream did not answer Finalize with Proof".into(),
                )
                .into());
            };
            Some((proof.public_inputs_hash, proof.proof))
        } else {
            None
        };
        drop(requests);
        match responses.message().await {
            Ok(None) => Ok(proof),
            Ok(Some(_)) => Err(ProverError::Backend(
                "ProveStream returned an unsolicited response".into(),
            )
            .into()),
            Err(status) => Err(map_rpc_status(range.0, range.1, status).into()),
        }
    }
}

#[async_trait]
impl Prover for RemoteProver {
    async fn prevalidate(
        &self,
        rollup_id: u64,
        anchor: ProvingAnchor,
        block: BlockWitness,
    ) -> ProverResult<()> {
        match self
            .stream_exchange(rollup_id, anchor, &[block], None)
            .await
        {
            Ok(None) | Err(StreamingError::Unsupported) => Ok(()),
            Ok(Some(_)) => Err(ProverError::Backend(
                "ProveStream returned a proof without Finalize".into(),
            )),
            Err(StreamingError::Prover(error)) => Err(error),
        }
    }

    async fn prove(&self, ctx: ProvingContext) -> ProverResult<Bytes> {
        let Some(anchor) = ctx.anchor else {
            return self.prove_v1(ctx).await;
        };
        let abi_calldata = eez_protocol::abi::postAndVerifyBatchCall {
            batch: ctx.batch.clone(),
        }
        .abi_encode();
        let post_batch = PostBatch {
            abi_calldata,
            public_inputs_hash: Vec::new(),
            l1_block_hash: ctx
                .l1_block_hash
                .map(|hash| hash.to_vec())
                .unwrap_or_default(),
        };
        match self
            .stream_exchange(
                ctx.rollup_id,
                anchor,
                &ctx.blocks,
                Some((ctx.from_block, ctx.to_block, post_batch)),
            )
            .await
        {
            Ok(Some((hash, proof))) => self.verify_proof(ctx.from_block, ctx.to_block, hash, proof),
            Ok(None) => Err(ProverError::Backend(
                "ProveStream Finalize returned no proof".into(),
            )),
            Err(StreamingError::Unsupported) => self.prove_v1(ctx).await,
            Err(StreamingError::Prover(error)) => Err(error),
        }
    }

    fn vkey(&self) -> B256 {
        // Left-zero-pad the 20-byte attester into a B256 (the registry vkey).
        self.inner.attester.into_word()
    }
}

/// Preserve the Composer profile's closed retryable-status allowlist across
/// the transport-neutral [`Prover`] boundary. Every other non-OK status is a
/// non-retryable backend rejection for the unchanged request.
fn map_rpc_status(from_block: u64, to_block: u64, status: Status) -> ProverError {
    let kind = match status.code() {
        Code::Unavailable => Some(RetryableProverError::Unavailable),
        Code::DeadlineExceeded => Some(RetryableProverError::DeadlineExceeded),
        Code::Aborted => Some(RetryableProverError::Aborted),
        _ => None,
    };
    let message = format!("Prove {from_block}-{to_block}: {status}");
    match kind {
        Some(kind) => ProverError::Retryable { kind, message },
        None if status.code() == Code::FailedPrecondition => {
            match decode_actionable_failure(status.details()) {
                Some(failure) => ProverError::Actionable { failure, message },
                None => ProverError::Backend(message),
            }
        }
        None => ProverError::Backend(message),
    }
}

/// Decode only the exact candidate identities the Composer can act on.
/// Empty, malformed, or wrong-length details remain non-actionable.
fn decode_actionable_failure(details: &[u8]) -> Option<ActionableProverFailure> {
    if details.is_empty() {
        return None;
    }
    let failure = eez_control_rpc::decode_prove_failure(details)
        .ok()?
        .actionable_failure?;
    match failure {
        prove_failure::ActionableFailure::Outbound(outbound) => {
            Some(ActionableProverFailure::Outbound {
                transaction_index: outbound.transaction_index.try_into().ok()?,
                transaction_hash: exact_hash(&outbound.transaction_hash)?,
            })
        }
        prove_failure::ActionableFailure::Inbound(inbound) => {
            Some(ActionableProverFailure::Inbound {
                entry_index: inbound.entry_index.try_into().ok()?,
                entry_hash: exact_hash(&inbound.entry_hash)?,
            })
        }
    }
}

fn exact_hash(bytes: &[u8]) -> Option<B256> {
    (bytes.len() == B256::len_bytes()).then(|| B256::from_slice(bytes))
}

/// Verify a prover attestation: the 65-byte signature must recover to `attester`
/// over the 32-byte `public_inputs_hash`. Returns the bound hash on success.
///
/// # Errors
///
/// [`ProverError::Backend`] on a wrong-length signature or hash, a malformed
/// signature, or a signer that isn't the registered attester.
fn verify_attestation(
    signature: &[u8],
    public_inputs_hash: &[u8],
    attester: Address,
) -> ProverResult<B256> {
    if signature.len() != 65 {
        return Err(ProverError::Backend(format!(
            "attestation is {} bytes, expected 65 (r||s||v)",
            signature.len()
        )));
    }
    if public_inputs_hash.len() != 32 {
        return Err(ProverError::Backend(format!(
            "publicInputsHash is {} bytes, expected 32",
            public_inputs_hash.len()
        )));
    }
    let hash = B256::from_slice(public_inputs_hash);
    let sig = Signature::try_from(signature)
        .map_err(|e| ProverError::Backend(format!("malformed attestation signature: {e}")))?;
    let recovered = sig
        .recover_address_from_prehash(&hash)
        .map_err(|e| ProverError::Backend(format!("attestation recover failed: {e}")))?;
    if recovered != attester {
        return Err(ProverError::Backend(format!(
            "attestation signer {recovered} != registered attester {attester}"
        )));
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use eez_control_rpc::v1::{
        InboundFailure, OutboundFailure, ProveFailure, ProveResponse,
        prover_server::{Prover as ProverService, ProverServer},
    };
    use eez_control_rpc::v2::{
        Proof as StreamProof, Ready, Validated as StreamValidated,
        prover_server::{Prover as StreamingProverService, ProverServer as StreamingProverServer},
    };
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_stream::Stream;
    use tonic::{Request, Response, Streaming, transport::Server};

    /// Stub `Prove` server: drains the window stream and returns a fixed
    /// `publicInputsHash` signed by `signer` (the r||s||v packing L1 expects).
    #[derive(Clone)]
    struct StubProver {
        signer: PrivateKeySigner,
        hash: B256,
    }

    #[tonic::async_trait]
    impl ProverService for StubProver {
        async fn prove(
            &self,
            request: Request<Streaming<ProveChunk>>,
        ) -> Result<Response<ProveResponse>, Status> {
            let mut stream = request.into_inner();
            while stream.message().await?.is_some() {}
            let sig = self.signer.sign_hash_sync(&self.hash).unwrap();
            let mut out = [0u8; 65];
            out[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
            out[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
            out[64] = u8::from(sig.v()) + 27;
            Ok(Response::new(ProveResponse {
                public_inputs_hash: self.hash.to_vec(),
                signature: out.to_vec(),
            }))
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct UnavailableProver;

    #[derive(Clone)]
    struct StreamingStub {
        signer: PrivateKeySigner,
        hash: B256,
        blocks: Arc<AtomicUsize>,
        cursor: Arc<Mutex<(u64, B256)>>,
        cross_wire_block_response: bool,
    }

    #[tonic::async_trait]
    impl StreamingProverService for StreamingStub {
        type ProveStreamStream =
            Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send + 'static>>;

        async fn prove_stream(
            &self,
            request: Request<Streaming<ClientFrame>>,
        ) -> Result<Response<Self::ProveStreamStream>, Status> {
            let mut input = request.into_inner();
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            let this = self.clone();
            tokio::spawn(async move {
                while let Ok(Some(frame)) = input.message().await {
                    let request_id = frame.request_id;
                    let mut epoch = frame.epoch;
                    let mut session_id = frame.session_id;
                    let kind = match frame.kind {
                        Some(client_frame::Kind::Begin(begin)) => {
                            session_id = vec![0x77; 32];
                            epoch = 1;
                            *this.cursor.lock().await =
                                (begin.anchor_number, B256::from_slice(&begin.anchor_hash));
                            let cursor = *this.cursor.lock().await;
                            server_frame::Kind::Ready(Ready {
                                validated_through: cursor.0,
                                validated_hash: cursor.1.to_vec(),
                            })
                        }
                        Some(client_frame::Kind::Resume(_)) => {
                            epoch = epoch.saturating_add(1).max(1);
                            let cursor = *this.cursor.lock().await;
                            server_frame::Kind::Ready(Ready {
                                validated_through: cursor.0,
                                validated_hash: cursor.1.to_vec(),
                            })
                        }
                        Some(client_frame::Kind::Block(block)) => {
                            let block = block.data.unwrap();
                            this.blocks.fetch_add(1, Ordering::SeqCst);
                            let hash = B256::from_slice(&block.hash);
                            *this.cursor.lock().await = (block.number, hash);
                            server_frame::Kind::Validated(StreamValidated {
                                number: block.number,
                                hash: block.hash,
                                post_state_root: vec![0x99; 32],
                                reused: false,
                            })
                        }
                        Some(client_frame::Kind::Finalize(_)) => {
                            server_frame::Kind::Proof(StreamProof {
                                public_inputs_hash: this.hash.to_vec(),
                                proof: sign_65(&this.signer, this.hash),
                            })
                        }
                        _ => break,
                    };
                    if sender
                        .send(Ok(ServerFrame {
                            session_id: if this.cross_wire_block_response
                                && matches!(&kind, server_frame::Kind::Validated(_))
                            {
                                vec![0x88; 32]
                            } else {
                                session_id
                            },
                            request_id,
                            epoch,
                            kind: Some(kind),
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(receiver),
            )))
        }
    }

    #[derive(Clone)]
    struct PipeliningStub {
        signer: PrivateKeySigner,
        hash: B256,
    }

    #[tonic::async_trait]
    impl StreamingProverService for PipeliningStub {
        type ProveStreamStream =
            Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send + 'static>>;

        async fn prove_stream(
            &self,
            request: Request<Streaming<ClientFrame>>,
        ) -> Result<Response<Self::ProveStreamStream>, Status> {
            let mut input = request.into_inner();
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            let this = self.clone();
            tokio::spawn(async move {
                let Some(begin) = input.message().await.ok().flatten() else {
                    return;
                };
                let Some(client_frame::Kind::Begin(begin_data)) = begin.kind else {
                    let _ = sender
                        .send(Err(Status::invalid_argument("expected Begin")))
                        .await;
                    return;
                };
                let session_id = vec![0x71; 32];
                let epoch = 1;
                if sender
                    .send(Ok(ServerFrame {
                        session_id: session_id.clone(),
                        request_id: begin.request_id,
                        epoch,
                        kind: Some(server_frame::Kind::Ready(Ready {
                            validated_through: begin_data.anchor_number,
                            validated_hash: begin_data.anchor_hash,
                        })),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }

                let Some(first) = input.message().await.ok().flatten() else {
                    return;
                };
                let Ok(Ok(Some(second))) =
                    tokio::time::timeout(std::time::Duration::from_millis(250), input.message())
                        .await
                else {
                    let _ = sender
                        .send(Err(Status::deadline_exceeded(
                            "client waited for the first Validated response",
                        )))
                        .await;
                    return;
                };
                let Some(finalize) = input.message().await.ok().flatten() else {
                    return;
                };
                let blocks = [first, second];
                for frame in blocks {
                    let Some(client_frame::Kind::Block(block)) = frame.kind else {
                        let _ = sender
                            .send(Err(Status::invalid_argument("expected Block")))
                            .await;
                        return;
                    };
                    let block = block.data.expect("test block carries data");
                    if sender
                        .send(Ok(ServerFrame {
                            session_id: session_id.clone(),
                            request_id: frame.request_id,
                            epoch,
                            kind: Some(server_frame::Kind::Validated(StreamValidated {
                                number: block.number,
                                hash: block.hash,
                                post_state_root: vec![0x99; 32],
                                reused: false,
                            })),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if !matches!(finalize.kind, Some(client_frame::Kind::Finalize(_))) {
                    let _ = sender
                        .send(Err(Status::invalid_argument("expected Finalize")))
                        .await;
                    return;
                }
                let _ = sender
                    .send(Ok(ServerFrame {
                        session_id,
                        request_id: finalize.request_id,
                        epoch,
                        kind: Some(server_frame::Kind::Proof(StreamProof {
                            public_inputs_hash: this.hash.to_vec(),
                            proof: sign_65(&this.signer, this.hash),
                        })),
                    }))
                    .await;
            });
            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(receiver),
            )))
        }
    }

    #[tonic::async_trait]
    impl ProverService for UnavailableProver {
        async fn prove(
            &self,
            _request: Request<Streaming<ProveChunk>>,
        ) -> Result<Response<ProveResponse>, Status> {
            Err(Status::unavailable("test prover is busy"))
        }
    }

    fn serve_stub(
        listener: tokio::net::TcpListener,
        signer: PrivateKeySigner,
        hash: B256,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            Server::builder()
                .add_service(ProverServer::new(StubProver { signer, hash }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        })
    }

    async fn spawn_stub(signer: PrivateKeySigner, hash: B256) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        serve_stub(listener, signer, hash);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        format!("http://{addr}")
    }

    async fn spawn_unavailable_stub() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(ProverServer::new(UnavailableProver))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_streaming_stub(stub: StreamingStub) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(StreamingProverServer::new(stub))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    fn test_anchor() -> ProvingAnchor {
        ProvingAnchor {
            number: 10,
            hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x55),
        }
    }

    fn test_block(number: u64, parent_hash: B256, hash: u8) -> BlockWitness {
        BlockWitness {
            number,
            parent_hash,
            hash: B256::repeat_byte(hash),
            rlp: Bytes::from_static(&[0xc0]),
            witness: Default::default(),
        }
    }

    fn test_key() -> PrivateKeySigner {
        PrivateKeySigner::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap()
    }

    /// An attestation that ecrecovers to the REGISTERED attester is accepted and
    /// returned verbatim (65 bytes).
    #[tokio::test]
    async fn attestation_recovering_to_registered_attester_is_accepted() {
        let key = test_key();
        let attester = key.address();
        let hash = B256::repeat_byte(0x7c);
        let url = spawn_stub(key, hash).await;

        let prover = RemoteProver::new(url, attester);
        let proof = prover
            .prove(ProvingContext::default())
            .await
            .expect("attestation recovering to the registered attester must be accepted");
        assert_eq!(proof.len(), 65, "returned proof is the 65-byte r||s||v");
    }

    /// An attestation signed by a DIFFERENT key does not recover to the registered
    /// attester → fail-closed (a wrong/malicious prover cannot forge one).
    #[tokio::test]
    async fn attestation_from_wrong_signer_fails_closed() {
        let key = test_key();
        let hash = B256::repeat_byte(0x7c);
        let url = spawn_stub(key, hash).await;

        let wrong_attester = address!("0x00000000000000000000000000000000000000ff");
        let prover = RemoteProver::new(url, wrong_attester);
        let err = prover
            .prove(ProvingContext::default())
            .await
            .expect_err("attestation not recovering to the registered attester must be refused");
        assert!(
            matches!(err, ProverError::Backend(_)),
            "wrong-signer attestation must fail closed, got {err:?}"
        );
    }

    #[tokio::test]
    async fn connection_failure_is_retryable_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let prover = RemoteProver::new(format!("http://{addr}"), Address::ZERO);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            prover.prove(ProvingContext::default()),
        )
        .await
        .expect("connection failure should be prompt")
        .unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::Unavailable)
        );
    }

    #[tokio::test]
    async fn unavailable_rpc_status_surfaces_as_retryable() {
        let prover = RemoteProver::new(spawn_unavailable_stub().await, Address::ZERO);

        let error = prover.prove(ProvingContext::default()).await.unwrap_err();

        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::Unavailable)
        );
    }

    /// `RemoteProver` must not retain a dead transport. Each proof call
    /// reconnects to the configured endpoint, so the same client recovers when
    /// the signer process is replaced on the same address.
    #[tokio::test]
    async fn remote_prover_reconnects_after_server_restart() {
        let key = test_key();
        let attester = key.address();
        let hash = B256::repeat_byte(0x7c);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first_server = serve_stub(listener, key.clone(), hash);
        let prover = RemoteProver::new(format!("http://{addr}"), attester);

        prover
            .prove(ProvingContext::default())
            .await
            .expect("first signer instance must attest");

        first_server.abort();
        let _ = first_server.await;
        let replacement_listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let replacement_server = serve_stub(replacement_listener, key, hash);

        prover
            .prove(ProvingContext::default())
            .await
            .expect("client must reconnect to the replacement signer");
        replacement_server.abort();
    }

    #[tokio::test]
    async fn finalized_session_is_resumed_and_validated_blocks_are_not_resent() {
        let key = test_key();
        let attester = key.address();
        let signed_hash = B256::repeat_byte(0x7c);
        let block_count = Arc::new(AtomicUsize::new(0));
        let anchor = test_anchor();
        let block_11 = test_block(11, anchor.hash, 0x22);
        let block_12 = test_block(12, block_11.hash, 0x23);
        let url = spawn_streaming_stub(StreamingStub {
            signer: key,
            hash: signed_hash,
            blocks: Arc::clone(&block_count),
            cursor: Arc::new(Mutex::new((0, B256::ZERO))),
            cross_wire_block_response: false,
        })
        .await;
        let prover = RemoteProver::new(url, attester);

        prover
            .prevalidate(1, anchor, block_11.clone())
            .await
            .unwrap();
        let proof = prover
            .prove(ProvingContext {
                rollup_id: 1,
                from_block: 11,
                to_block: 11,
                anchor: Some(anchor),
                batch: Default::default(),
                blocks: vec![block_11.clone()],
                l1_block_hash: None,
            })
            .await
            .unwrap();

        assert_eq!(proof.len(), 65);
        assert_eq!(block_count.load(Ordering::SeqCst), 1);

        // Witness capture can race Finalize. Re-observing the exact block that
        // Finalize already backfilled is idempotent rather than a false gap.
        prover
            .prevalidate(1, anchor, block_11.clone())
            .await
            .unwrap();
        prover
            .prevalidate(1, anchor, block_12.clone())
            .await
            .unwrap();
        let proof = prover
            .prove(ProvingContext {
                rollup_id: 1,
                from_block: 11,
                to_block: 12,
                anchor: Some(anchor),
                batch: Default::default(),
                blocks: vec![block_11, block_12],
                l1_block_hash: None,
            })
            .await
            .unwrap();

        assert_eq!(proof.len(), 65);
        assert_eq!(block_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn contiguous_blocks_and_finalize_are_pipelined_before_the_first_ack() {
        let key = test_key();
        let attester = key.address();
        let signed_hash = B256::repeat_byte(0x7c);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(StreamingProverServer::new(PipeliningStub {
                    signer: key,
                    hash: signed_hash,
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let anchor = test_anchor();
        let block_11 = test_block(11, anchor.hash, 0x22);
        let block_12 = test_block(12, block_11.hash, 0x23);
        let prover = RemoteProver::new(format!("http://{addr}"), attester);

        let proof = prover
            .prove(ProvingContext {
                rollup_id: 1,
                from_block: 11,
                to_block: 12,
                anchor: Some(anchor),
                batch: Default::default(),
                blocks: vec![block_11, block_12],
                l1_block_hash: None,
            })
            .await
            .unwrap();

        assert_eq!(proof.len(), 65);
    }

    #[tokio::test]
    async fn a_block_response_from_another_session_is_rejected() {
        let key = test_key();
        let anchor = test_anchor();
        let block = test_block(11, anchor.hash, 0x22);
        let url = spawn_streaming_stub(StreamingStub {
            signer: key.clone(),
            hash: B256::repeat_byte(0x7c),
            blocks: Arc::new(AtomicUsize::new(0)),
            cursor: Arc::new(Mutex::new((0, B256::ZERO))),
            cross_wire_block_response: true,
        })
        .await;
        let prover = RemoteProver::new(url, key.address());

        let error = prover.prevalidate(1, anchor, block).await.unwrap_err();

        assert!(
            matches!(error, ProverError::Backend(message) if message.contains("crossed Composer sessions"))
        );
    }

    /// Pack a signature the way the prover does (r||s||v, v+27).
    fn sign_65(signer: &PrivateKeySigner, hash: B256) -> Vec<u8> {
        let sig = signer.sign_hash_sync(&hash).unwrap();
        let mut out = [0u8; 65];
        out[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
        out[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
        out[64] = u8::from(sig.v()) + 27;
        out.to_vec()
    }

    #[test]
    fn verify_attestation_accepts_valid() {
        let key = test_key();
        let hash = B256::repeat_byte(0x7c);
        let sig = sign_65(&key, hash);
        assert_eq!(
            verify_attestation(&sig, hash.as_slice(), key.address()).unwrap(),
            hash
        );
    }

    #[test]
    fn verify_attestation_rejects_wrong_signer() {
        let key = test_key();
        let hash = B256::repeat_byte(0x7c);
        let sig = sign_65(&key, hash);
        let wrong = address!("0x00000000000000000000000000000000000000ff");
        assert!(verify_attestation(&sig, hash.as_slice(), wrong).is_err());
    }

    /// An otherwise valid signature is bound to one public-input hash;
    /// replaying it for a different window hash must fail closed.
    #[test]
    fn signature_replayed_for_a_different_window_hash_is_rejected() {
        let key = test_key();
        let first_window_hash = B256::repeat_byte(0x7c);
        let different_window_hash = B256::repeat_byte(0x7d);
        let signature = sign_65(&key, first_window_hash);

        let error = verify_attestation(&signature, different_window_hash.as_slice(), key.address())
            .expect_err("a signature must not authenticate a different window");
        assert!(matches!(error, ProverError::Backend(_)), "{error:?}");
    }

    #[test]
    fn verify_attestation_rejects_bad_signature_length() {
        let hash = B256::repeat_byte(0x7c);
        assert!(verify_attestation(&[0u8; 64], hash.as_slice(), test_key().address()).is_err());
    }

    #[test]
    fn verify_attestation_rejects_bad_hash_length() {
        let key = test_key();
        let hash = B256::repeat_byte(0x7c);
        let sig = sign_65(&key, hash);
        assert!(verify_attestation(&sig, &[0u8; 31], key.address()).is_err());
    }

    #[test]
    fn verify_attestation_rejects_malformed_signature() {
        let hash = B256::repeat_byte(0x7c);
        // 65 bytes but not a valid signature over `hash` → recover fails or the
        // recovered address won't be the attester.
        assert!(verify_attestation(&[0u8; 65], hash.as_slice(), test_key().address()).is_err());
    }

    #[test]
    fn rpc_status_retryability_is_a_closed_allowlist() {
        let retryable = [
            (Code::Unavailable, RetryableProverError::Unavailable),
            (
                Code::DeadlineExceeded,
                RetryableProverError::DeadlineExceeded,
            ),
            (Code::Aborted, RetryableProverError::Aborted),
        ];
        for (code, expected) in retryable {
            let error = map_rpc_status(5, 9, Status::new(code, "test"));
            assert_eq!(error.retryable_kind(), Some(expected), "{code:?}");
        }

        for code in [
            Code::Cancelled,
            Code::Unknown,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::DataLoss,
            Code::Unauthenticated,
        ] {
            let error = map_rpc_status(5, 9, Status::new(code, "test"));
            assert_eq!(error.retryable_kind(), None, "{code:?}");
            assert!(matches!(error, ProverError::Backend(_)), "{code:?}");
        }
    }

    #[test]
    fn failed_precondition_decodes_exact_actionable_candidate_identities() {
        let outbound = ProveFailure {
            actionable_failure: Some(prove_failure::ActionableFailure::Outbound(
                OutboundFailure {
                    transaction_index: 3,
                    transaction_hash: vec![0x44; 32],
                },
            )),
        };
        let error = map_rpc_status(
            5,
            9,
            Status::with_details(
                Code::FailedPrecondition,
                "rejected",
                eez_control_rpc::encode_prove_failure(&outbound).into(),
            ),
        );
        assert_eq!(
            error.actionable_failure(),
            Some(ActionableProverFailure::Outbound {
                transaction_index: 3,
                transaction_hash: B256::repeat_byte(0x44),
            })
        );

        let inbound = ProveFailure {
            actionable_failure: Some(prove_failure::ActionableFailure::Inbound(InboundFailure {
                entry_index: 4,
                entry_hash: vec![0x55; 32],
            })),
        };
        let error = map_rpc_status(
            5,
            9,
            Status::with_details(
                Code::FailedPrecondition,
                "rejected",
                eez_control_rpc::encode_prove_failure(&inbound).into(),
            ),
        );
        assert_eq!(
            error.actionable_failure(),
            Some(ActionableProverFailure::Inbound {
                entry_index: 4,
                entry_hash: B256::repeat_byte(0x55),
            })
        );
    }

    #[test]
    fn malformed_or_misclassified_details_never_become_actionable() {
        let wrong_hash_length = ProveFailure {
            actionable_failure: Some(prove_failure::ActionableFailure::Outbound(
                OutboundFailure {
                    transaction_index: 3,
                    transaction_hash: vec![0x44; 31],
                },
            )),
        };
        let statuses = [
            Status::with_details(
                Code::FailedPrecondition,
                "rejected",
                eez_control_rpc::encode_prove_failure(&wrong_hash_length).into(),
            ),
            Status::with_details(Code::FailedPrecondition, "rejected", vec![0xff].into()),
            Status::with_details(
                Code::Internal,
                "internal",
                eez_control_rpc::encode_prove_failure(&ProveFailure {
                    actionable_failure: Some(prove_failure::ActionableFailure::Inbound(
                        InboundFailure {
                            entry_index: 4,
                            entry_hash: vec![0x55; 32],
                        },
                    )),
                })
                .into(),
            ),
        ];

        for status in statuses {
            let error = map_rpc_status(5, 9, status);
            assert!(matches!(error, ProverError::Backend(_)));
            assert_eq!(error.actionable_failure(), None);
        }
    }
}
