//! `RemoteProver` — the composer-side [`Prover`] backed by one `Prove` RPC.
//!
//! The composer holds `Arc<dyn Prover>` and calls `prove(ctx)`; when that
//! `Arc` is a [`RemoteProver`], the call maps [`ProvingContext`] to a
//! `prove.v1` client-stream (a header then one chunk per window block), dials
//! the configured `eez-proverd`, awaits the attestation, verifies it recovers
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
use eez_prover::{Prover, ProverError, ProverResult, ProvingContext};
use tracing::{Level, event};

/// A [`Prover`] that proves a window on a remote `eez-proverd` over the
/// `prove.v1.Prover` gRPC service. Cheap to clone (`Arc<Inner>`).
#[derive(Debug, Clone)]
pub struct RemoteProver {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// The `eez-proverd` endpoint, e.g. `http://127.0.0.1:50061`.
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
    let abi_calldata = eez_evm::types::postAndVerifyBatchCall {
        batch: ctx.batch.inner.clone(),
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
            .map_err(|e| ProverError::Backend(format!("connect {}: {e}", self.inner.url)))?
            .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
            .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES);
        let resp = client
            .prove(tokio_stream::iter(chunks))
            .await
            .map_err(|e| {
                ProverError::Backend(format!("Prove {}-{}: {e}", ctx.from_block, ctx.to_block))
            })?
            .into_inner();

        if resp.signature.len() != 65 {
            return Err(ProverError::Backend(format!(
                "attestation is {} bytes, expected 65 (r||s||v)",
                resp.signature.len()
            )));
        }
        if resp.public_inputs_hash.len() != 32 {
            return Err(ProverError::Backend(format!(
                "publicInputsHash is {} bytes, expected 32",
                resp.public_inputs_hash.len()
            )));
        }
        // Fail-closed: the attestation must recover to the REGISTERED attester
        // over the hash the prover signed. A wrong prover cannot forge it.
        let hash = B256::from_slice(&resp.public_inputs_hash);
        let sig = Signature::try_from(resp.signature.as_slice())
            .map_err(|e| ProverError::Backend(format!("malformed attestation signature: {e}")))?;
        let recovered = sig
            .recover_address_from_prehash(&hash)
            .map_err(|e| ProverError::Backend(format!("attestation recover failed: {e}")))?;
        if recovered != self.inner.attester {
            return Err(ProverError::Backend(format!(
                "attestation signer {recovered} != registered attester {}",
                self.inner.attester
            )));
        }
        event!(
            name: "eez.prover_client.attested",
            Level::INFO,
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
    use tonic::{Request, Response, Status, Streaming, transport::Server};

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
}
