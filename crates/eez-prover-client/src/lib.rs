//! `RemoteProver` — the composer-side [`Prover`] backed by one `Prove` RPC.
//!
//! The composer holds `Arc<dyn Prover>` and calls `prove(ctx)`; when that
//! `Arc` is a [`RemoteProver`], the call maps [`ProvingContext`] to a
//! `prove.v1` client-stream (a header then one chunk per window block), dials
//! the configured `eez-proof-signer`, awaits the attestation, verifies it recovers
//! to the registered attester, and returns the 65-byte signature. Stateless:
//! one round-trip, no feed/cursor/sink.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, Signature};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use eez_control_rpc::v1::{
    BlockWitness as WireBlockWitness, ExecutionWitness as WireWitness, PostBatch, ProveChunk,
    ProveHeader, prove_chunk, prover_client::ProverClient,
};
use eez_prover::{Prover, ProverError, ProverResult, ProvingContext, RetryableProverError};
use tonic::{Code, Status};
use tracing::{Level, event};

/// A [`Prover`] that proves a window on a remote `eez-proof-signer` over the
/// `prove.v1.Prover` gRPC service. Cheap to clone (`Arc<Inner>`).
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
            kind: Some(prove_chunk::Kind::Block(WireBlockWitness {
                number: bw.number,
                hash: bw.hash.to_vec(),
                parent_hash: bw.parent_hash.to_vec(),
                rlp: bw.rlp.to_vec(),
                witness: Some(WireWitness {
                    state: bw.witness.state.iter().map(|b| b.to_vec()).collect(),
                    codes: bw.witness.codes.iter().map(|b| b.to_vec()).collect(),
                    keys: bw.witness.keys.iter().map(|b| b.to_vec()).collect(),
                    headers: bw.witness.headers.iter().map(|b| b.to_vec()).collect(),
                }),
            })),
        });
    }
    chunks
}

#[async_trait]
impl Prover for RemoteProver {
    async fn prove(&self, ctx: ProvingContext) -> ProverResult<Bytes> {
        let chunks = chunks_for(&ctx);
        let n_blocks = chunks.len().saturating_sub(1);

        // Raise the message-size cap on both directions: a single block's witness
        // can exceed tonic's 4 MiB default → `ResourceExhausted`. Server matches.
        let mut client = ProverClient::connect(self.inner.url.clone())
            .await
            .map_err(|error| ProverError::Retryable {
                kind: RetryableProverError::Unavailable,
                message: format!("connect {}: {error}", self.inner.url),
            })?
            .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
            .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
        let resp = client
            .prove(tokio_stream::iter(chunks))
            .await
            .map_err(|status| map_rpc_status(ctx.from_block, ctx.to_block, status))?
            .into_inner();

        // Fail-closed: the attestation must recover to the REGISTERED attester
        // over the hash the prover signed. A wrong prover cannot forge it.
        let hash = verify_attestation(
            &resp.signature,
            &resp.public_inputs_hash,
            self.inner.attester,
        )?;
        event!(
            name: "eez.prover_client.attested",
            Level::INFO,
            event_name = "eez.prover_client.attested",
            from = ctx.from_block,
            to = ctx.to_block,
            blocks = n_blocks,
            %hash,
            "remote prover attested the window",
        );
        Ok(Bytes::copy_from_slice(&resp.signature))
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
        None => ProverError::Backend(message),
    }
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
        ProveResponse,
        prover_server::{Prover as ProverService, ProverServer},
    };
    use std::str::FromStr;
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

    async fn spawn_stub(signer: PrivateKeySigner, hash: B256) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(ProverServer::new(StubProver { signer, hash }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        format!("http://{addr}")
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
}
