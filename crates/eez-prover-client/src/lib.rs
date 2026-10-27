//! `RemoteProver` — the composer-side streaming prover client.
//!
//! Committed blocks are prevalidated through resumable `prove.v2` sessions.
//! Finalization backfills any missing suffix, binds the exact settlement
//! calldata, verifies the returned attestation, and returns its 65-byte
//! signature. `prove.v1` remains the compatibility fallback for calldata settlement.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use eez_control_rpc::v1::{
    BlockWitness as WireBlockWitness, ExecutionWitness as WireWitness, PostBatch, ProveChunk,
    ProveHeader, prove_chunk, prove_failure, prover_client::ProverClient,
};
use eez_control_rpc::v2::{
    Begin as StreamBegin, Block as StreamBlock, ClientFrame, Finalize as StreamFinalize,
    Resume as StreamResume, Rewind as StreamRewind, ServerFrame,
    SettlementInput as StreamSettlement, client_frame,
    prover_client::ProverClient as StreamingProverClient, server_frame,
};
use eez_prover::{
    ActionableProverFailure, BlockWitness, Prover, ProverError, ProverResult, ProvingAnchor,
    ProvingContext, RetryableProverError,
};
use tokio::sync::{Mutex, watch};
use tonic::{Code, Status};
use tracing::{Level, event};

// Rollup0 clients require bounded pipelining support from the streaming server.
const MAX_IN_FLIGHT: usize = 8;

/// A [`Prover`] backed by the v2 streaming service, with v1 fallback for calldata.
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
    // Counts queued/active proofs so best-effort prevalidation yields to all of them.
    proving: watch::Sender<usize>,
    exchange_timeout: Duration,
}

struct ProofPriority<'a>(&'a watch::Sender<usize>);

impl Drop for ProofPriority<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count -= 1);
    }
}

#[derive(Debug, Default)]
struct StreamingState {
    supported: Option<bool>,
    session: Option<StreamingSession>,
    // Retain the transport and its last acknowledged cursor until Finalize.
    // Taking it before an exchange makes cancellation drop the entire transport.
    connection: Option<(
        tokio::sync::mpsc::Sender<ClientFrame>,
        tonic::Streaming<ServerFrame>,
        ServerFrame,
    )>,
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
                proving: watch::channel(0).0,
                // Bound a stalled transport even outside Composer's shorter proof budget.
                exchange_timeout: Duration::from_secs(5),
            }),
        }
    }

    /// The registered attester this prover verifies against.
    #[must_use]
    pub fn attester(&self) -> Address {
        self.inner.attester
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
        .ok_or_else(|| ProverError::Retryable {
            kind: RetryableProverError::Unavailable,
            message: "ProveStream ended before its response".into(),
        })?;
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
        self.verify_proof(&ctx, response.public_inputs_hash, response.signature)
    }

    fn verify_proof(
        &self,
        ctx: &ProvingContext,
        public_inputs_hash: Vec<u8>,
        signature: Vec<u8>,
    ) -> ProverResult<Bytes> {
        let hash = verify_attestation(&signature, &public_inputs_hash, self.inner.attester)?;
        let expected = if let Some(blob) = &ctx.blob {
            let hashes = eez_blob::versioned_hashes(&blob.blobs)
                .map_err(|error| ProverError::Backend(format!("request blobs: {error}")))?;
            eez_protocol::public_inputs::public_inputs_hashes_with_context(
                &ctx.batch,
                self.vkey(),
                &hashes,
                blob.expected_custom_data.clone(),
            )
        } else {
            eez_protocol::public_inputs::public_inputs_hashes(&ctx.batch, self.vkey())
        }
        .map_err(|error| {
            ProverError::Backend(format!("cannot verify requested public inputs: {error}"))
        })?;
        if expected.len() != 1 || hash != expected[0] {
            return Err(ProverError::Backend(
                "prover proof does not bind the requested batch".into(),
            ));
        }
        event!(
            name: "eez.prover_client.attested",
            Level::INFO,
            event_name = "eez.prover_client.attested",
            from = ctx.from_block,
            to = ctx.to_block,
            %hash,
            "remote prover attested the window",
        );
        Ok(Bytes::from(signature))
    }

    /// Reuse the live stream, or open Begin/Resume and correlate its first response.
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
        if begin.is_none()
            && let Some(connection) = state.connection.take()
        {
            return Ok(connection);
        }
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
        finalize: Option<(u64, u64, StreamSettlement)>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, StreamingError> {
        // One deadline covers lock acquisition, connect, both stream directions and EOF.
        // Dropping the exchange releases its lock; a later Resume recovers accepted work.
        tokio::time::timeout(self.inner.exchange_timeout, async {
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
            state.connection = None;
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
            .map(|(from_block, to_block, settlement)| {
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
                    settlement: Some(settlement),
                })
            })
            .transpose()?;

        // An abandoned unproved prefix can fill the quota, or a rollback can need
        // pruned history; baseline servers can also reject subrange selection.
        // Rebuild once at the exact predecessor, preserving inputs and deadline.
        let mut retry_at_anchor = state.session.is_some()
            && finalize.as_ref().is_some_and(|f| anchor.number.checked_add(1) == Some(f.from_block));
        'attempt: loop {
        // Only the server retains the validated prefix. A fresh Ready determines
        // what to send, including work completed after a previous response was lost.
        let (requests, mut responses, blocks, mut cursor) = loop {
            let creating = state.session.is_none();
            if creating {
                retry_at_anchor = false; // Already starting at the requested anchor.
            }
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
                        event!(
                            name: "eez.prover_client.streaming_unsupported",
                            Level::WARN,
                            "remote prover does not support streaming; using v1 until client restart",
                        );
                        state.supported = Some(false);
                        state.session = None;
                        return Err(StreamingError::Unsupported);
                    }
                    Err(error) => return Err(error),
                };
            state.supported = Some(true);
            if !creating
                && matches!(&response.kind,
                    Some(server_frame::Kind::Rejected(rejected)) if matches!(Code::from_i32(rejected.code), Code::NotFound | Code::Unimplemented)
                )
            {
                state.session = None;
                continue; // The server evicted the session or restarted; begin at the supplied anchor.
            }
            if let Some(error) = rejection_error(&response, range.0, range.1) {
                return Err(error.into());
            }
            let mut cursor = response.clone();
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
                if finalize.is_none() && first.number <= ready.validated_through {
                    // Late captures must not rewind newer progress. Only prove()
                    // reconciles a replacement fork at an already checked height.
                    state.connection = Some((requests, responses, cursor));
                    return Ok(None);
                }
                if finalize.is_none()
                    && first.number > ready.validated_through.saturating_add(1)
                {
                    // Best-effort prevalidation can skip blocks while capture is busy.
                    // Keep the checked prefix; a complete prove() will backfill the gap.
                    event!(
                        name: "eez.prover_client.prevalidation_gap",
                        Level::DEBUG,
                        number = first.number,
                        validated_through = ready.validated_through,
                        "prevalidation skipped a gap; proof submission will backfill it",
                    );
                    state.connection = Some((requests, responses, cursor));
                    return Ok(None);
                }
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
                        Some(server_frame::Kind::Rejected(rejected)) if matches!(Code::from_i32(rejected.code), Code::FailedPrecondition | Code::Unimplemented)
                    )
                {
                    state.session = None;
                    continue; // The requested parent is no longer retained; backfill a new session.
                }
                if let Some(error) = rejection_error(&response, range.0, range.1) {
                    return Err(error.into());
                }
                cursor = response.clone();
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
            break (requests, responses, remaining, cursor);
        };

        // Poll both directions: a slow reader must not deadlock our request sender.
        // Keep outstanding frames bounded while draining acknowledgements.
        let total = blocks.len() + usize::from(finalize.is_some());
        let mut pending_finalize = finalize.clone();
        let mut next = 0;
        let mut pending = std::collections::VecDeque::new();
        let mut proof = None;
        while next < total || !pending.is_empty() {
            let request_id = pending.front().map_or(0, |&(id, _)| id);
            tokio::select! {
                permit = requests.reserve(), if next < total && pending.len() < MAX_IN_FLIGHT => {
                    let permit = permit.map_err(|_| ProverError::Retryable {
                        kind: RetryableProverError::Unavailable,
                        message: "ProveStream request stream closed".into(),
                    })?;
                    let kind = if next < blocks.len() {
                        client_frame::Kind::Block(StreamBlock { data: Some(wire_block(&blocks[next])) })
                    } else {
                        client_frame::Kind::Finalize(pending_finalize.take().expect("last request is Finalize"))
                    };
                    let id = state.next_id();
                    let session = state.session.as_ref().expect("Ready established a session");
                    permit.send(ClientFrame { session_id: session.id.clone(), epoch: session.epoch, request_id: id, kind: Some(kind) });
                    pending.push_back((id, next));
                    next += 1;
                }
                response = state.response(&mut responses, request_id, range), if !pending.is_empty() => {
                    let response = response?;
                    let (_, index) = pending.pop_front().expect("response has an outstanding request");
                    if retry_at_anchor && !response.session_id.is_empty()
                        && matches!(&response.kind, Some(server_frame::Kind::Rejected(rejected))
                            if Code::from_i32(rejected.code) == Code::ResourceExhausted
                                || (index == blocks.len() && matches!(Code::from_i32(rejected.code),
                                    Code::OutOfRange | Code::Unimplemented))) {
                        state.session = None;
                        continue 'attempt;
                    }
                    if let Some(error) = rejection_error(&response, range.0, range.1) {
                        if response.session_id.is_empty()
                            || error.retryable_kind() == Some(RetryableProverError::Aborted) {
                            state.session = None;
                        }
                        return Err(error.into());
                    }
                    if let Some(block) = blocks.get(index) {
                        if !matches!(&response.kind, Some(server_frame::Kind::Validated(validated))
                            if validated.number == block.number && validated.hash == block.hash.as_slice()
                                && validated.post_state_root.len() == 32) {
                            return Err(ProverError::Backend("ProveStream Validated response identity mismatch".into()).into());
                        }
                        cursor = ServerFrame { kind: Some(server_frame::Kind::Ready(eez_control_rpc::v2::Ready {
                            validated_through: block.number, validated_hash: block.hash.to_vec(),
                        })), ..response };
                    } else {
                        let Some(server_frame::Kind::Proof(result)) = response.kind else {
                            return Err(ProverError::Backend("ProveStream did not answer Finalize with Proof".into()).into());
                        };
                        proof = Some((result.public_inputs_hash, result.proof));
                    }
                }
            }
        }
        if proof.is_none() {
            state.connection = Some((requests, responses, cursor));
            return Ok(None);
        }
        drop(requests);
        return match responses.message().await {
            Ok(None) => Ok(proof),
            Ok(Some(_)) => Err(ProverError::Backend(
                "ProveStream returned an unsolicited response".into(),
            )
            .into()),
            Err(status) => Err(map_rpc_status(range.0, range.1, status).into()),
        };
        }
        }).await.map_err(|_| ProverError::Retryable {
            kind: RetryableProverError::DeadlineExceeded,
            message: "ProveStream exchange deadline exceeded".into(),
        })?
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
        let mut proving = self.inner.proving.subscribe();
        let blocks = [block];
        // Yield the shared stream to proofs. Backend work may still finish;
        // prove() resumes accepted progress or backfills missing blocks.
        let result = tokio::select! {
            // Prefer proof priority when both branches are ready.
            biased;
            _ = proving.wait_for(|count| *count != 0) => {
                event!(
                    name: "eez.prover_client.prevalidation_yielded",
                    Level::DEBUG,
                    number = blocks[0].number,
                    "prevalidation yielded to a proof request; proof submission will recover or backfill progress",
                );
                // Skipping this optimization is success, not a validation acknowledgement.
                return Ok(());
            }
            result = self.stream_exchange(rollup_id, anchor, &blocks, None) => result,
        };
        match result {
            Ok(None) | Err(StreamingError::Unsupported) => Ok(()),
            Ok(Some(_)) => Err(ProverError::Backend(
                "ProveStream returned a proof without Finalize".into(),
            )),
            Err(StreamingError::Prover(error)) => Err(error),
        }
    }

    async fn prove(&self, ctx: ProvingContext) -> ProverResult<Bytes> {
        self.inner.proving.send_modify(|count| *count += 1);
        let _priority = ProofPriority(&self.inner.proving);
        let Some(anchor) = ctx.anchor else {
            if ctx.blob.is_some() {
                return Err(ProverError::Backend(
                    "blob settlement requires a streaming anchor".into(),
                ));
            }
            return self.prove_v1(ctx).await;
        };
        let abi_calldata = eez_protocol::abi::postAndVerifyBatchCall {
            batch: ctx.batch.clone(),
        }
        .abi_encode();
        if ctx.l1_block_hash.is_some() {
            return Err(ProverError::Backend(
                "Rollup0 streaming does not support L1-bound settlement".into(),
            ));
        }
        let settlement = StreamSettlement {
            abi_calldata,
            blobs: ctx
                .blob
                .as_ref()
                .map(|blob| blob.blobs.iter().map(|bytes| bytes.to_vec()).collect())
                .unwrap_or_default(),
            posting_context: ctx.blob.as_ref().map(|blob| {
                let context = blob.posting_context;
                eez_control_rpc::v2::L1PostingContext {
                    chain_id: context.chain_id,
                    target_timestamp: context.target_timestamp,
                    parent_number: context.parent_number,
                    parent_hash: context.parent_hash.to_vec(),
                }
            }),
        };
        match self
            .stream_exchange(
                ctx.rollup_id,
                anchor,
                &ctx.blocks,
                Some((ctx.from_block, ctx.to_block, settlement)),
            )
            .await
        {
            Ok(Some((hash, proof))) => self.verify_proof(&ctx, hash, proof),
            Ok(None) => Err(ProverError::Backend(
                "ProveStream Finalize returned no proof".into(),
            )),
            Err(StreamingError::Unsupported) if ctx.blob.is_none() => self.prove_v1(ctx).await,
            Err(StreamingError::Unsupported) => Err(ProverError::Backend(
                "ProveStream does not support the required blob settlement profile".into(),
            )),
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
    if !matches!(signature[64], 27 | 28) {
        return Err(ProverError::Backend(
            "attestation v must be 27 or 28".into(),
        ));
    }
    let sig = Signature::try_from(signature)
        .map_err(|e| ProverError::Backend(format!("malformed attestation signature: {e}")))?;
    if sig.normalize_s().is_some() {
        return Err(ProverError::Backend(
            "attestation s must be canonical".into(),
        ));
    }
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
    struct StalledStream {
        phase: usize,
        reached: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl StreamingProverService for StalledStream {
        type ProveStreamStream = Pin<Box<dyn Stream<Item = Result<ServerFrame, Status>> + Send>>;

        async fn prove_stream(
            &self,
            request: Request<Streaming<ClientFrame>>,
        ) -> Result<Response<Self::ProveStreamStream>, Status> {
            let phase = self.phase;
            let reached = Arc::clone(&self.reached);
            let mut input = request.into_inner();
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                while let Ok(Some(frame)) = input.message().await {
                    let kind = match frame.kind {
                        Some(client_frame::Kind::Begin(begin)) if phase != 1 => {
                            server_frame::Kind::Ready(Ready {
                                validated_through: begin.anchor_number,
                                validated_hash: begin.anchor_hash,
                            })
                        }
                        Some(client_frame::Kind::Block(block)) if phase != 2 => {
                            let block = block.data.unwrap();
                            server_frame::Kind::Validated(StreamValidated {
                                number: block.number,
                                hash: block.hash,
                                post_state_root: vec![0; 32],
                                reused: false,
                            })
                        }
                        Some(client_frame::Kind::Finalize(_)) if phase == 3 => {
                            server_frame::Kind::Proof(StreamProof::default())
                        }
                        _ => {
                            reached.store(phase, Ordering::SeqCst);
                            sender.closed().await;
                            return;
                        }
                    };
                    if sender
                        .send(Ok(ServerFrame {
                            session_id: vec![0x77; 32],
                            epoch: 1,
                            request_id: frame.request_id,
                            kind: Some(kind),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                // Also exercise a peer that acknowledges everything but never sends EOF.
                reached.store(3, Ordering::SeqCst);
                sender.closed().await;
            });
            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(receiver),
            )))
        }
    }

    #[tokio::test]
    async fn exchange_deadline_covers_ready_block_eof_and_lock_waits() {
        for phase in 1..=3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let reached = Arc::new(AtomicUsize::new(0));
            let service = StalledStream {
                phase,
                reached: Arc::clone(&reached),
            };
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(StreamingProverServer::new(service))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            let mut prover = RemoteProver::new(format!("http://{addr}"), Address::ZERO);
            Arc::get_mut(&mut prover.inner).unwrap().exchange_timeout = Duration::from_millis(150);
            let input = test_block(11, test_anchor().hash, 0x22);
            let error = prover
                .stream_exchange(
                    1,
                    test_anchor(),
                    std::slice::from_ref(&input),
                    Some((11, 11, StreamSettlement::default())),
                )
                .await
                .map_err(ProverError::from)
                .unwrap_err();
            assert_eq!(
                error.retryable_kind(),
                Some(RetryableProverError::DeadlineExceeded),
                "phase {phase}"
            );
            assert_eq!(
                reached.load(Ordering::SeqCst),
                phase,
                "peer reached the intended stall"
            );
            let guard = prover
                .inner
                .streaming
                .try_lock()
                .expect("timeout releases the session lock");
            let error = prover
                .prevalidate(1, test_anchor(), input)
                .await
                .unwrap_err();
            assert_eq!(
                error.retryable_kind(),
                Some(RetryableProverError::DeadlineExceeded)
            );
            drop(guard);
            server.abort();
            let _ = server.await;
        }
    }

    #[derive(Clone)]
    struct StreamingStub {
        signer: PrivateKeySigner,
        hash: B256,
        blocks: Arc<AtomicUsize>,
        cursor: Arc<Mutex<(u64, B256)>>,
        cross_wire_block_response: bool,
        baseline_only: bool,
        begins: Arc<AtomicUsize>,
        stall_first_block: Option<Arc<tokio::sync::Notify>>,
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
                let mut anchor_number = None;
                while let Ok(Some(frame)) = input.message().await {
                    let request_id = frame.request_id;
                    let mut epoch = frame.epoch;
                    let mut session_id = frame.session_id;
                    let kind = match frame.kind {
                        Some(client_frame::Kind::Begin(begin)) => {
                            anchor_number = Some(begin.anchor_number);
                            this.begins.fetch_add(1, Ordering::SeqCst);
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
                        Some(client_frame::Kind::Resume(_)) if !this.baseline_only => {
                            epoch = epoch.saturating_add(1).max(1);
                            let cursor = *this.cursor.lock().await;
                            server_frame::Kind::Ready(Ready {
                                validated_through: cursor.0,
                                validated_hash: cursor.1.to_vec(),
                            })
                        }
                        Some(client_frame::Kind::Block(block)) => {
                            let block = block.data.unwrap();
                            let first = this.blocks.fetch_add(1, Ordering::SeqCst) == 0;
                            let hash = B256::from_slice(&block.hash);
                            *this.cursor.lock().await = (block.number, hash);
                            if first && let Some(reached) = &this.stall_first_block {
                                reached.notify_one();
                                sender.closed().await; // Accepted, but its ACK never reaches the client.
                                return;
                            }
                            server_frame::Kind::Validated(StreamValidated {
                                number: block.number,
                                hash: block.hash,
                                post_state_root: vec![0x99; 32],
                                reused: false,
                            })
                        }
                        Some(client_frame::Kind::Finalize(finalize))
                            if !this.baseline_only
                                || anchor_number.and_then(|n| n.checked_add(1))
                                    == Some(finalize.from_block) =>
                        {
                            server_frame::Kind::Proof(StreamProof {
                                public_inputs_hash: this.hash.to_vec(),
                                proof: sign_65(&this.signer, this.hash),
                            })
                        }
                        _ => server_frame::Kind::Rejected(eez_control_rpc::v2::Rejected {
                            code: i32::from(Code::Unimplemented),
                            message: "baseline only".into(),
                            ..Default::default()
                        }),
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

    fn test_batch() -> eez_protocol::EvmBatch {
        eez_protocol::EvmBatch {
            proofSystems: vec![Address::repeat_byte(0xaa)],
            rollupIdsWithProofSystems: vec![eez_protocol::abi::RollupIdWithProofSystemsSol {
                rollupId: 1,
                proofSystemIndexes: vec![0],
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn blob_settlement_requires_a_streaming_anchor() {
        let prover = RemoteProver::new("http://127.0.0.1:1", Address::ZERO);
        let error = prover
            .prove(ProvingContext {
                blob: Some(eez_prover::BlobProvingContext {
                    blobs: Vec::new(),
                    posting_context: eez_prover::L1PostingContext {
                        chain_id: 1,
                        target_timestamp: 1,
                        parent_number: 0,
                        parent_hash: B256::repeat_byte(1),
                    },
                    expected_custom_data: Bytes::new(),
                }),
                ..Default::default()
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("requires a streaming anchor"));
    }

    /// Both direct v1 and fallback require the registered signer and requested batch.
    #[tokio::test]
    async fn v1_and_fallback_bind_the_attestation_to_the_requested_batch() {
        for fallback in [false, true] {
            for wrong_batch in [false, true] {
                let key = test_key();
                let attester = key.address();
                let ctx = ProvingContext {
                    rollup_id: 1,
                    from_block: 11,
                    to_block: 11,
                    anchor: fallback.then(test_anchor),
                    blocks: vec![test_block(11, test_anchor().hash, 0x22)],
                    batch: test_batch(),
                    l1_block_hash: None,
                    blob: None,
                };
                let hash = if wrong_batch {
                    B256::repeat_byte(0x7c)
                } else {
                    eez_protocol::public_inputs::public_inputs_hashes(
                        &ctx.batch,
                        attester.into_word(),
                    )
                    .unwrap()[0]
                };
                let prover = RemoteProver::new(spawn_stub(key, hash).await, attester);
                let result = prover.prove(ctx).await;
                assert_eq!(
                    prover.inner.streaming.lock().await.supported,
                    fallback.then_some(false)
                );
                if wrong_batch {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("does not bind the requested batch")
                    );
                } else {
                    assert_eq!(result.unwrap().len(), 65);
                }
            }
        }
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
        let ctx = ProvingContext {
            batch: test_batch(),
            ..Default::default()
        };
        let hash =
            eez_protocol::public_inputs::public_inputs_hashes(&ctx.batch, attester.into_word())
                .unwrap()[0];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first_server = serve_stub(listener, key.clone(), hash);
        let prover = RemoteProver::new(format!("http://{addr}"), attester);

        prover
            .prove(ctx.clone())
            .await
            .expect("first signer instance must attest");

        first_server.abort();
        let _ = first_server.await;
        let replacement_listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let replacement_server = serve_stub(replacement_listener, key, hash);

        prover
            .prove(ctx)
            .await
            .expect("client must reconnect to the replacement signer");
        replacement_server.abort();
    }

    #[tokio::test]
    async fn finalized_session_is_resumed_and_validated_blocks_are_not_resent() {
        let key = test_key();
        let attester = key.address();
        let signed_hash =
            eez_protocol::public_inputs::public_inputs_hashes(&test_batch(), attester.into_word())
                .unwrap()[0];
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
            baseline_only: false,
            begins: Arc::default(),
            stall_first_block: None,
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
                batch: test_batch(),
                blocks: vec![block_11.clone()],
                l1_block_hash: None,
                blob: None,
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
                batch: test_batch(),
                blocks: vec![block_11, block_12],
                l1_block_hash: None,
                blob: None,
            })
            .await
            .unwrap();

        assert_eq!(proof.len(), 65);
        assert_eq!(block_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn proof_interrupts_stalled_prevalidation_and_recovers_accepted_progress() {
        for baseline_only in [false, true] {
            let key = test_key();
            let attester = key.address();
            let hash = eez_protocol::public_inputs::public_inputs_hashes(
                &test_batch(),
                attester.into_word(),
            )
            .unwrap()[0];
            let reached = Arc::new(tokio::sync::Notify::new());
            let begins = Arc::new(AtomicUsize::new(0));
            let blocks = Arc::new(AtomicUsize::new(0));
            let endpoint = spawn_streaming_stub(StreamingStub {
                signer: key,
                hash,
                blocks: Arc::clone(&blocks),
                cursor: Arc::new(Mutex::new((0, B256::ZERO))),
                cross_wire_block_response: false,
                baseline_only,
                begins: Arc::clone(&begins),
                stall_first_block: Some(Arc::clone(&reached)),
            })
            .await;
            let prover = RemoteProver::new(endpoint, attester);
            let anchor = test_anchor();
            let block = test_block(11, anchor.hash, 0x22);
            let prevalidation = tokio::spawn({
                let prover = prover.clone();
                let block = block.clone();
                async move { prover.prevalidate(1, anchor, block).await }
            });
            tokio::time::timeout(Duration::from_secs(1), reached.notified())
                .await
                .unwrap();
            let proof = tokio::time::timeout(
                Duration::from_secs(1),
                prover.prove(ProvingContext {
                    rollup_id: 1,
                    from_block: 11,
                    to_block: 11,
                    anchor: Some(anchor),
                    blocks: vec![block],
                    batch: test_batch(),
                    l1_block_hash: None,
                    blob: None,
                }),
            )
            .await
            .expect("proof must not wait for the stalled prevalidation's five-second timeout")
            .unwrap();
            assert_eq!(proof.len(), 65);
            prevalidation.await.unwrap().unwrap();
            assert_eq!(*prover.inner.proving.borrow(), 0);
            // Resume recovers the lost ACK; baseline-only servers receive the complete window.
            assert_eq!(
                begins.load(Ordering::SeqCst),
                if baseline_only { 2 } else { 1 }
            );
            assert_eq!(
                blocks.load(Ordering::SeqCst),
                if baseline_only { 2 } else { 1 }
            );
        }
    }

    #[tokio::test]
    async fn queued_proof_suppresses_prevalidation_and_cancellation_releases_priority() {
        let prover = RemoteProver::new("http://127.0.0.1:1", Address::ZERO);
        let mut priority = prover.inner.proving.subscribe();
        let lock = prover.inner.streaming.lock().await;
        let request = tokio::spawn({
            let prover = prover.clone();
            async move {
                prover
                    .prove(ProvingContext {
                        anchor: Some(test_anchor()),
                        ..Default::default()
                    })
                    .await
            }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            priority.wait_for(|count| *count == 1),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            prover.prevalidate(1, test_anchor(), test_block(11, test_anchor().hash, 0x22)),
        )
        .await
        .unwrap()
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        drop(lock);
        // Once the proof is cancelled, prevalidation must try the endpoint again,
        // not silently return success because priority was left permanently set.
        let error = prover
            .prevalidate(1, test_anchor(), test_block(11, test_anchor().hash, 0x22))
            .await
            .unwrap_err();
        assert_eq!(
            error.retryable_kind(),
            Some(RetryableProverError::Unavailable)
        );
    }

    #[tokio::test]
    async fn skipped_prevalidation_preserves_progress_for_proof_backfill() {
        let key = test_key();
        let attester = key.address();
        let hash =
            eez_protocol::public_inputs::public_inputs_hashes(&test_batch(), attester.into_word())
                .unwrap()[0];
        let begins = Arc::new(AtomicUsize::new(0));
        let blocks = Arc::new(AtomicUsize::new(0));
        let cursor = Arc::new(Mutex::new((0, B256::ZERO)));
        let endpoint = spawn_streaming_stub(StreamingStub {
            signer: key,
            hash,
            blocks: Arc::clone(&blocks),
            cursor: Arc::clone(&cursor),
            begins: Arc::clone(&begins),
            stall_first_block: None,
            baseline_only: false,
            cross_wire_block_response: false,
        })
        .await;
        let prover = RemoteProver::new(endpoint, attester);
        let anchor = test_anchor();
        let first = test_block(11, anchor.hash, 0x22);
        let second = test_block(12, first.hash, 0x23);
        let third = test_block(13, second.hash, 0x24);
        prover.prevalidate(1, anchor, first.clone()).await.unwrap();

        // Busy witness capture skipped block 12; block 13 must not discard 11.
        prover.prevalidate(1, anchor, third.clone()).await.unwrap();
        assert_eq!(begins.load(Ordering::SeqCst), 1);
        assert_eq!(blocks.load(Ordering::SeqCst), 1);
        assert_eq!(*cursor.lock().await, (11, first.hash));

        let proof = prover
            .prove(ProvingContext {
                rollup_id: 1,
                from_block: 11,
                to_block: 13,
                anchor: Some(anchor),
                blocks: vec![first.clone(), second, third.clone()],
                batch: test_batch(),
                l1_block_hash: None,
                blob: None,
            })
            .await
            .unwrap();
        assert_eq!(proof.len(), 65);
        assert_eq!(begins.load(Ordering::SeqCst), 1);
        assert_eq!(
            blocks.load(Ordering::SeqCst),
            3,
            "only 12 and 13 need backfill"
        );
        assert_eq!(*cursor.lock().await, (13, third.hash));
        // Capture can finish an older block after prove() has advanced the session.
        prover.prevalidate(1, anchor, first).await.unwrap();
        assert_eq!(*cursor.lock().await, (13, third.hash));
    }

    #[tokio::test]
    async fn baseline_keeps_one_stream_and_rejects_a_valid_signature_for_another_batch() {
        for wrong_batch in [false, true] {
            let key = test_key();
            let attester = key.address();
            let hash = if wrong_batch {
                B256::repeat_byte(0x7c)
            } else {
                eez_protocol::public_inputs::public_inputs_hashes(
                    &test_batch(),
                    attester.into_word(),
                )
                .unwrap()[0]
            };
            let begins = Arc::new(AtomicUsize::new(0));
            let blocks = Arc::new(AtomicUsize::new(0));
            let endpoint = spawn_streaming_stub(StreamingStub {
                signer: key,
                hash,
                cursor: Arc::new(Mutex::new((0, B256::ZERO))),
                blocks: Arc::clone(&blocks),
                begins: Arc::clone(&begins),
                baseline_only: true,
                cross_wire_block_response: false,
                stall_first_block: None,
            })
            .await;
            let prover = RemoteProver::new(endpoint, attester);
            let anchor = test_anchor();
            let first = test_block(11, anchor.hash, 0x22);
            let second = test_block(12, first.hash, 0x23);
            prover.prevalidate(1, anchor, first.clone()).await.unwrap();
            prover.prevalidate(1, anchor, second.clone()).await.unwrap();
            let proof = prover
                .prove(ProvingContext {
                    rollup_id: 1,
                    from_block: 11,
                    to_block: 12,
                    anchor: Some(anchor),
                    blocks: vec![first, second],
                    batch: test_batch(),
                    l1_block_hash: None,
                    blob: None,
                })
                .await;
            assert_eq!(begins.load(Ordering::SeqCst), 1);
            assert_eq!(blocks.load(Ordering::SeqCst), 2);
            if wrong_batch {
                assert!(
                    proof
                        .unwrap_err()
                        .to_string()
                        .contains("does not bind the requested batch")
                );
            } else {
                assert_eq!(proof.unwrap().len(), 65);
            }
        }
    }

    #[tokio::test]
    async fn optional_operations_fall_back_to_the_same_baseline_work() {
        for operation in ["resume", "rewind", "subrange"] {
            let key = test_key();
            let attester = key.address();
            let hash = eez_protocol::public_inputs::public_inputs_hashes(
                &test_batch(),
                attester.into_word(),
            )
            .unwrap()[0];
            let begins = Arc::new(AtomicUsize::new(0));
            let count = Arc::new(AtomicUsize::new(0));
            let endpoint = spawn_streaming_stub(StreamingStub {
                signer: key,
                hash,
                cursor: Arc::new(Mutex::new((0, B256::ZERO))),
                blocks: Arc::clone(&count),
                begins: Arc::clone(&begins),
                baseline_only: true,
                cross_wire_block_response: false,
                stall_first_block: None,
            })
            .await;
            let prover = RemoteProver::new(endpoint, attester);
            let anchor = test_anchor();
            let first = test_block(11, anchor.hash, 0x22);
            let second = test_block(12, first.hash, 0x23);
            prover.prevalidate(1, anchor, first.clone()).await.unwrap();
            prover.prevalidate(1, anchor, second.clone()).await.unwrap();
            match operation {
                "rewind" => {
                    prover
                        .prove(ProvingContext {
                            rollup_id: 1,
                            from_block: 11,
                            to_block: 11,
                            anchor: Some(anchor),
                            blocks: vec![test_block(11, anchor.hash, 0x99)],
                            batch: test_batch(),
                            l1_block_hash: None,
                            blob: None,
                        })
                        .await
                        .unwrap();
                }
                _ => {
                    let subrange = operation == "subrange";
                    if !subrange {
                        prover.inner.streaming.lock().await.connection = None;
                    }
                    let proof = prover
                        .prove(ProvingContext {
                            rollup_id: 1,
                            from_block: if subrange { 12 } else { 11 },
                            to_block: 12,
                            anchor: Some(if subrange {
                                ProvingAnchor {
                                    number: 11,
                                    hash: first.hash,
                                    state_root: B256::repeat_byte(0x99),
                                }
                            } else {
                                anchor
                            }),
                            blocks: if subrange {
                                vec![second]
                            } else {
                                vec![first, second]
                            },
                            batch: test_batch(),
                            l1_block_hash: None,
                            blob: None,
                        })
                        .await
                        .unwrap();
                    assert_eq!(proof.len(), 65, "{operation}");
                }
            }
            assert_eq!(begins.load(Ordering::SeqCst), 2, "{operation}");
            assert_eq!(
                count.load(Ordering::SeqCst),
                if operation == "resume" { 4 } else { 3 },
                "{operation}"
            );
        }
    }

    #[tokio::test]
    async fn contiguous_blocks_and_finalize_are_pipelined_before_the_first_ack() {
        let key = test_key();
        let attester = key.address();
        let signed_hash =
            eez_protocol::public_inputs::public_inputs_hashes(&test_batch(), attester.into_word())
                .unwrap()[0];
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
                batch: test_batch(),
                blocks: vec![block_11, block_12],
                l1_block_hash: None,
                blob: None,
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
            baseline_only: false,
            begins: Arc::default(),
            stall_first_block: None,
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
