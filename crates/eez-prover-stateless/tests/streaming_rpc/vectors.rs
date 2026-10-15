//! Executed RPC transcripts, with only transport identities/diagnostics normalized.
use std::collections::HashMap;
use std::time::Duration;

use alloy_primitives::{Address, B256, Signature};
use alloy_sol_types::SolCall as _;
use eez_control_rpc::{v1, v2};
use prost::Message;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

struct Stream {
    name: &'static str,
    requests: Option<mpsc::Sender<v2::ClientFrame>>,
    responses: tonic::Streaming<v2::ServerFrame>,
    binding: v2::ServerFrame,
    authorized_binding: v2::ServerFrame,
}

#[derive(Default)]
struct Transcript {
    steps: Vec<Value>,
    sessions: HashMap<Vec<u8>, Vec<u8>>,
    epochs: HashMap<(Vec<u8>, u64), u64>,
    next_request: u64,
    sent: HashMap<u64, v2::ClientFrame>,
    // Expected progress comes from successful requests, never from Ready/Rejected.
    prefixes: HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>,
    current_epochs: HashMap<Vec<u8>, u64>,
    check: String,
}

impl Transcript {
    fn binding(&mut self, id: &mut Vec<u8>, epoch: &mut u64) {
        if id.is_empty() {
            return;
        }
        // Only server-issued identities are placeholders. Malformed/unknown IDs
        // and deliberately invalid epochs must survive unchanged on the wire.
        let Some(normalized) = self.sessions.get(id).cloned() else {
            return;
        };
        if *epoch != 0 {
            *epoch = *self.epochs.get(&(id.clone(), *epoch)).unwrap_or(epoch);
        }
        *id = normalized;
    }

    async fn open(&mut self, endpoint: &str, name: &'static str) -> Stream {
        let mut client = v2::prover_client::ProverClient::connect(endpoint.to_owned())
            .await
            .unwrap()
            .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
            .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
        let (requests, stream) = mpsc::channel(16);
        let responses = client
            .prove_stream(ReceiverStream::new(stream))
            .await
            .unwrap()
            .into_inner();
        self.steps.push(json!({"stream": name, "action": "open"}));
        Stream {
            name,
            requests: Some(requests),
            responses,
            binding: v2::ServerFrame::default(),
            authorized_binding: v2::ServerFrame::default(),
        }
    }

    async fn send(&mut self, stream: &Stream, kind: Option<v2::client_frame::Kind>) -> u64 {
        self.next_request = self.next_request.wrapping_add(1);
        let frame = v2::ClientFrame {
            session_id: stream.binding.session_id.clone(),
            epoch: stream.binding.epoch,
            request_id: self.next_request,
            kind,
        };
        let mut normalized = frame.clone();
        self.binding(&mut normalized.session_id, &mut normalized.epoch);
        let operation = match &frame.kind {
            Some(v2::client_frame::Kind::Begin(_)) => "Begin",
            Some(v2::client_frame::Kind::Block(_)) => "Block",
            Some(v2::client_frame::Kind::Finalize(_)) => "Finalize",
            Some(v2::client_frame::Kind::Resume(_)) => "Resume",
            Some(v2::client_frame::Kind::Rewind(_)) => "Rewind",
            Some(v2::client_frame::Kind::Cancel(_)) => "Cancel",
            None => "unknown",
        };
        self.steps.push(json!({"stream": stream.name, "action": "send", "operation": operation, "check": self.check, "protobuf_hex": hex::encode(normalized.encode_to_vec())}));
        assert!(self.sent.insert(frame.request_id, frame.clone()).is_none());
        stream.requests.as_ref().unwrap().send(frame).await.unwrap();
        self.next_request
    }

    async fn receive(
        &mut self,
        stream: &mut Stream,
        request_id: u64,
        expected: &str,
        code: Option<Code>,
    ) -> v2::ServerFrame {
        let response = tokio::time::timeout(Duration::from_secs(10), stream.responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.request_id, request_id);
        let request = self.sent.remove(&request_id).unwrap();
        let expected_binding = (request.session_id.as_slice(), request.epoch);
        let kind = match response.kind.as_ref().unwrap() {
            v2::server_frame::Kind::Ready(ready) => {
                assert_eq!(response.session_id.len(), 32);
                assert_ne!(response.epoch, 0);
                use v2::client_frame::Kind as C;
                let expected_cursor = match request.kind.as_ref().unwrap() {
                    C::Begin(begin) => {
                        assert!(!self.prefixes.contains_key(&response.session_id));
                        let cursor = (begin.anchor_number, begin.anchor_hash.clone());
                        self.prefixes
                            .insert(response.session_id.clone(), vec![cursor.clone()]);
                        cursor
                    }
                    C::Resume(_) => {
                        assert_eq!(response.session_id, request.session_id);
                        self.prefixes[&request.session_id].last().unwrap().clone()
                    }
                    C::Rewind(rewind) => {
                        assert_eq!(response.session_id, request.session_id);
                        let prefix = self.prefixes.get_mut(&request.session_id).unwrap();
                        let index = prefix
                            .iter()
                            .position(|(_, hash)| hash == &rewind.ancestor_hash)
                            .unwrap();
                        prefix.truncate(index + 1);
                        prefix.last().unwrap().clone()
                    }
                    _ => panic!("unexpected Ready for {:?}", request.kind),
                };
                assert_eq!(
                    (ready.validated_through, ready.validated_hash.clone()),
                    expected_cursor,
                    "{}",
                    self.check
                );
                assert_ne!(
                    self.current_epochs
                        .insert(response.session_id.clone(), response.epoch),
                    Some(response.epoch)
                );
                let next_id = u8::try_from(self.sessions.len() + 1).unwrap();
                self.sessions
                    .entry(response.session_id.clone())
                    .or_insert_with(|| vec![next_id; 32]);
                let next_epoch = self
                    .epochs
                    .keys()
                    .filter(|(id, _)| id == &response.session_id)
                    .count() as u64
                    + 1;
                assert!(
                    self.epochs
                        .insert((response.session_id.clone(), response.epoch), next_epoch)
                        .is_none()
                );
                stream.binding = response.clone();
                stream.authorized_binding = response.clone();
                "ready"
            }
            v2::server_frame::Kind::Validated(validated) => {
                assert_eq!(
                    (response.session_id.as_slice(), response.epoch),
                    expected_binding
                );
                let Some(v2::client_frame::Kind::Block(block)) = request.kind.as_ref() else {
                    panic!("Validated without Block")
                };
                let data = block.data.as_ref().unwrap();
                let decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(&data.rlp).unwrap();
                assert_eq!(validated.number, data.number);
                assert_eq!(validated.hash, data.hash);
                assert_eq!(
                    validated.post_state_root,
                    decoded.header.state_root.as_slice()
                );
                let prefix = self.prefixes.get_mut(&request.session_id).unwrap();
                let cursor = (data.number, data.hash.clone());
                if !prefix.contains(&cursor) {
                    let parent = prefix.last().unwrap();
                    assert_eq!(parent.0.checked_add(1), Some(data.number));
                    assert_eq!(parent.1, data.parent_hash);
                    prefix.push(cursor);
                }
                "validated"
            }
            v2::server_frame::Kind::Proof(proof) => {
                assert_eq!(
                    (response.session_id.as_slice(), response.epoch),
                    expected_binding
                );
                let Some(v2::client_frame::Kind::Finalize(finalize)) = request.kind.as_ref() else {
                    panic!("Proof without Finalize")
                };
                let calldata = &finalize.settlement.as_ref().unwrap().abi_calldata;
                let batch = eez_protocol::abi::postAndVerifyBatchCall::abi_decode(calldata)
                    .unwrap()
                    .batch;
                let oracle: Value = serde_json::from_str(&super::fixture("oracle.json")).unwrap();
                let hashes = eez_protocol::public_inputs::public_inputs_hashes(
                    &batch,
                    oracle["proof_system_vkey"]
                        .as_str()
                        .unwrap()
                        .parse::<B256>()
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(proof.public_inputs_hash, hashes[0].as_slice());
                assert_eq!(
                    Signature::from_raw(&proof.proof)
                        .unwrap()
                        .recover_address_from_prehash(&hashes[0])
                        .unwrap(),
                    oracle["attester"]
                        .as_str()
                        .unwrap()
                        .parse::<Address>()
                        .unwrap()
                );
                if *calldata == super::fixture_hex(&super::fixture("postbatch.hex")) {
                    assert_eq!(
                        proof.public_inputs_hash,
                        super::fixture_hex(oracle["public_inputs_hash"].as_str().unwrap())
                    );
                    assert_eq!(
                        proof.proof,
                        super::fixture_hex(oracle["expected_test_signature"].as_str().unwrap())
                    );
                }
                let prefix = self.prefixes.get_mut(&request.session_id).unwrap();
                let parent = prefix
                    .iter()
                    .position(|(number, _)| number.checked_add(1) == Some(finalize.from_block))
                    .unwrap();
                prefix.drain(..parent);
                "proof"
            }
            v2::server_frame::Kind::Cancelled(_) => {
                assert_eq!(
                    (response.session_id.as_slice(), response.epoch),
                    expected_binding
                );
                self.prefixes.remove(&request.session_id).unwrap();
                self.current_epochs.remove(&request.session_id).unwrap();
                "cancelled"
            }
            v2::server_frame::Kind::Rejected(rejected) => {
                assert_eq!(
                    Code::from_i32(rejected.code),
                    code.unwrap(),
                    "{}: {}",
                    self.check,
                    rejected.message
                );
                let authorized = !request.session_id.is_empty()
                    && request.session_id == stream.authorized_binding.session_id
                    && request.epoch == stream.authorized_binding.epoch
                    && self.current_epochs.get(&request.session_id) == Some(&request.epoch)
                    && !matches!(
                        request.kind,
                        Some(v2::client_frame::Kind::Begin(_) | v2::client_frame::Kind::Resume(_))
                    );
                if authorized {
                    assert_eq!(
                        (response.session_id.as_slice(), response.epoch),
                        expected_binding
                    );
                    assert_eq!(
                        &(rejected.validated_through, rejected.validated_hash.clone()),
                        self.prefixes[&request.session_id].last().unwrap(),
                        "{}",
                        self.check
                    );
                } else {
                    assert!(response.session_id.is_empty());
                    assert_eq!(response.epoch, 0);
                    assert_eq!(rejected.validated_through, 0);
                    assert!(rejected.validated_hash.is_empty());
                }
                "rejected"
            }
        };
        assert_eq!(kind, expected, "{}", self.check);
        let mut normalized = response.clone();
        self.binding(&mut normalized.session_id, &mut normalized.epoch);
        match normalized.kind.as_mut().unwrap() {
            v2::server_frame::Kind::Validated(value) => value.reused = false,
            v2::server_frame::Kind::Rejected(value) => {
                value.message.clear();
            }
            _ => {}
        }
        self.steps.push(json!({"stream": stream.name, "action": "receive", "kind": kind, "protobuf_hex": hex::encode(normalized.encode_to_vec())}));
        response
    }

    async fn exchange(
        &mut self,
        stream: &mut Stream,
        kind: Option<v2::client_frame::Kind>,
        expected: &str,
        code: Option<Code>,
    ) -> v2::ServerFrame {
        let id = self.send(stream, kind).await;
        self.receive(stream, id, expected, code).await
    }

    async fn close(&mut self, mut stream: Stream, half_close: bool) {
        if half_close {
            self.half_close(&mut stream);
        }
        // Proof/Cancelled must close the server side even while input remains open.
        assert!(
            tokio::time::timeout(Duration::from_secs(10), stream.responses.message())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        self.steps
            .push(json!({"stream": stream.name, "action": "eof"}));
    }

    fn half_close(&mut self, stream: &mut Stream) {
        stream.requests = None;
        self.steps
            .push(json!({"stream": stream.name, "action": "half_close"}));
    }

    async fn rpc_error(&mut self, stream: &mut Stream, code: Code) {
        let error = tokio::time::timeout(Duration::from_secs(10), stream.responses.message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), code);
        self.steps
            .push(json!({"stream": stream.name, "action": "rpc_error", "code": i32::from(code)}));
    }
}

#[path = "boundaries.rs"]
mod boundaries;

pub(super) async fn verify(
    endpoint: &str,
    anchor: eez_prover::ProvingAnchor,
    window: &[v1::ProveChunk],
    oracle: &Value,
) {
    use v2::client_frame::Kind as C;
    let Some(v1::prove_chunk::Kind::Header(header)) = &window[0].kind else {
        panic!("fixture header")
    };
    let blocks = window[1..]
        .iter()
        .map(|chunk| {
            let Some(v1::prove_chunk::Kind::Block(block)) = &chunk.kind else {
                panic!("fixture block")
            };
            block.clone()
        })
        .collect::<Vec<_>>();
    let begin = C::Begin(v2::Begin {
        rollup_id: header.rollup_id,
        anchor_number: anchor.number,
        anchor_hash: anchor.hash.to_vec(),
        anchor_state_root: anchor.state_root.to_vec(),
    });
    let finalize = v2::Finalize {
        from_block: header.from_block,
        to_block: header.to_block,
        terminal_hash: blocks.last().unwrap().hash.clone(),
        settlement: Some(v2::SettlementInput {
            abi_calldata: header.post_batch.as_ref().unwrap().abi_calldata.clone(),
            ..Default::default()
        }),
    };
    let block = |data| Some(C::Block(v2::Block { data: Some(data) }));
    let mut cases = Vec::new();

    // Baseline plus rejected inputs: each error leaves the stream usable and its
    // authoritative cursor unchanged; proof bytes match the independent oracle.
    let mut t = Transcript::default();
    let mut s = t.open(endpoint, "baseline").await;
    t.exchange(
        &mut s,
        block(blocks[0].clone()),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    t.exchange(&mut s, Some(begin.clone()), "ready", None).await;
    t.exchange(&mut s, None, "rejected", Some(Code::Unimplemented))
        .await;
    let mut malformed = blocks[0].clone();
    malformed.hash.pop();
    t.exchange(
        &mut s,
        block(malformed),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    t.exchange(
        &mut s,
        block(blocks[1].clone()),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    t.exchange(&mut s, block(blocks[0].clone()), "validated", None)
        .await;
    t.exchange(&mut s, block(blocks[0].clone()), "validated", None)
        .await;
    let mut missing = blocks[0].clone();
    missing.witness = None;
    t.exchange(
        &mut s,
        block(missing),
        "rejected",
        Some(Code::InvalidArgument),
    )
    .await;
    for context in [false, true] {
        let mut unsupported = finalize.clone();
        if context {
            unsupported.settlement.as_mut().unwrap().posting_context =
                Some(v2::L1PostingContext::default());
        } else {
            unsupported
                .settlement
                .as_mut()
                .unwrap()
                .blobs
                .push(vec![0; 131072]);
        }
        t.exchange(
            &mut s,
            Some(C::Finalize(unsupported)),
            "rejected",
            Some(Code::Unimplemented),
        )
        .await;
    }
    t.exchange(
        &mut s,
        Some(C::Finalize(finalize.clone())),
        "rejected",
        Some(Code::FailedPrecondition),
    )
    .await;
    for data in &blocks[1..] {
        t.exchange(&mut s, block(data.clone()), "validated", None)
            .await;
    }
    let proof = t
        .exchange(&mut s, Some(C::Finalize(finalize.clone())), "proof", None)
        .await;
    let Some(v2::server_frame::Kind::Proof(proof)) = proof.kind else {
        unreachable!()
    };
    assert_eq!(
        proof.public_inputs_hash,
        super::fixture_hex(oracle["public_inputs_hash"].as_str().unwrap())
    );
    assert_eq!(
        proof.proof,
        super::fixture_hex(oracle["expected_test_signature"].as_str().unwrap())
    );
    t.close(s, false).await;
    cases.push(json!({"name": "baseline-and-rejections", "profile": "baseline", "steps": t.steps}));

    // Rewind replaces an actually executed alternate block, not just a synthetic cursor.
    let mut alternate = blocks[0].clone();
    let mut decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(&alternate.rlp).unwrap();
    decoded.header.extra_data = vec![0x52, 0x50, 0x43].into();
    alternate.hash = decoded.header.hash_slow().to_vec();
    alternate.rlp = alloy_rlp::encode(decoded);
    let mut t = Transcript::default();
    let mut old = t.open(endpoint, "old").await;
    t.exchange(&mut old, Some(begin.clone()), "ready", None)
        .await;
    t.exchange(&mut old, block(alternate), "validated", None)
        .await;
    let mut resumed = t.open(endpoint, "resumed").await;
    resumed.binding.session_id = old.binding.session_id.clone();
    t.exchange(&mut resumed, Some(C::Resume(v2::Resume {})), "ready", None)
        .await;
    t.exchange(
        &mut old,
        block(blocks[0].clone()),
        "rejected",
        Some(Code::Aborted),
    )
    .await;
    t.close(old, true).await;
    t.exchange(
        &mut resumed,
        Some(C::Rewind(v2::Rewind {
            ancestor_hash: anchor.hash.to_vec(),
        })),
        "ready",
        None,
    )
    .await;
    for data in &blocks {
        t.exchange(&mut resumed, block(data.clone()), "validated", None)
            .await;
    }
    t.exchange(
        &mut resumed,
        Some(C::Finalize(finalize.clone())),
        "proof",
        None,
    )
    .await;
    let session = resumed.binding.session_id.clone();
    t.close(resumed, false).await;
    let mut cancelled = t.open(endpoint, "cancelled").await;
    cancelled.binding.session_id = session.clone();
    t.exchange(
        &mut cancelled,
        Some(C::Resume(v2::Resume {})),
        "ready",
        None,
    )
    .await;
    t.exchange(
        &mut cancelled,
        Some(C::Cancel(v2::Cancel {})),
        "cancelled",
        None,
    )
    .await;
    t.close(cancelled, false).await;
    let mut lost = t.open(endpoint, "retired").await;
    lost.binding.session_id = session;
    t.exchange(
        &mut lost,
        Some(C::Resume(v2::Resume {})),
        "rejected",
        Some(Code::NotFound),
    )
    .await;
    t.close(lost, true).await;
    cases.push(json!({"name": "executed-fork-resume-rewind-cancel", "profile": "session-controls", "steps": t.steps}));

    // Independent transports interleave requests without cross-wiring responses.
    let mut t = Transcript::default();
    let mut a = t.open(endpoint, "composer-a").await;
    let mut b = t.open(endpoint, "composer-b").await;
    t.exchange(&mut a, Some(begin.clone()), "ready", None).await;
    t.exchange(&mut b, Some(begin.clone()), "ready", None).await;
    assert_ne!(a.binding.session_id, b.binding.session_id);
    // Knowing another session ID does not authorize requests on this transport.
    let binding_b = b.binding.clone();
    b.binding = a.binding.clone();
    t.exchange(
        &mut b,
        block(blocks[0].clone()),
        "rejected",
        Some(Code::PermissionDenied),
    )
    .await;
    b.binding = binding_b;
    let mut pending = Vec::new();
    for data in &blocks {
        pending.push((
            t.send(&a, block(data.clone())).await,
            t.send(&b, block(data.clone())).await,
        ));
    }
    let proof_ids = (
        t.send(&a, Some(C::Finalize(finalize.clone()))).await,
        t.send(&b, Some(C::Finalize(finalize.clone()))).await,
    );
    for (left, right) in pending {
        t.receive(&mut a, left, "validated", None).await;
        t.receive(&mut b, right, "validated", None).await;
    }
    t.receive(&mut a, proof_ids.0, "proof", None).await;
    t.receive(&mut b, proof_ids.1, "proof", None).await;
    t.close(a, false).await;
    t.close(b, false).await;
    cases.push(json!({"name": "two-pipelined-composers", "profile": "pipelining-and-multiple-sessions", "steps": t.steps}));

    Box::pin(boundaries::verify(
        endpoint, &begin, &blocks, &finalize, &mut cases,
    ))
    .await;

    check_recording("rollup0-transcripts.json", cases, oracle);
    let cases = boundaries::verify_limits(&begin, &blocks, &finalize, oracle).await;
    check_recording("rollup0-limits-transcripts.json", cases, oracle);
}

fn check_recording(filename: &str, cases: Vec<Value>, oracle: &Value) {
    let vectors = json!({
        "format": "eez-prove-v2-transcript-1",
        "fixture": "crates/eez-prover-stateless/tests/fixtures/captured-devnet-window-84",
        "oracle": oracle,
        "normalization": "Map only server-issued session IDs and epochs to recorded placeholders; unmapped invalid IDs/epochs are literal. Ignore Rejected.message and Validated.reused. The replay tool verifies each Proof signature over the recorded digest against oracle.attester; valid ECDSA nonce differences are allowed. Other fields, response ordering, cursor and EOF are checked. Frames are protobuf payloads, without the gRPC compression/length prefix. Use a fresh signer for the stateless-cold-cache case. Limit cases require their listed server_args.",
        "cases": cases,
    });
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../eez-control-rpc/conformance")
        .join(filename);
    if std::env::var_os("UPDATE_STREAMING_VECTORS").as_deref() == Some(std::ffi::OsStr::new("1")) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(&vectors).unwrap()),
        )
        .unwrap();
    } else {
        let recorded: Value = serde_json::from_str(&std::fs::read_to_string(&path).expect(
            "committed streaming vectors; regenerate explicitly with UPDATE_STREAMING_VECTORS=1",
        ))
        .unwrap();
        assert_eq!(vectors, recorded, "executed RPC transcript changed");
    }
}
