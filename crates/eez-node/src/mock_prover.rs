//! In-process mock prover — devnet-only stand-in for `eez-proverd`.
//!
//! Mock mode (`EEZ_PROOF_SIGNER_KEY` set, no `EEZ_PROVER_URL`) spawns this
//! `prove.v1.Prover` stub on an ephemeral localhost port and points the
//! composer's `RemoteProver` at it, so the composer always talks to a prover
//! over the gRPC API. The stub drains (and ignores) the window stream, signs
//! the fixed [`MOCK_PROVER_DIGEST`] agreed with `MockECDSAProofSystem` — the
//! proof does **not** bind to the batch — and returns the digest as the
//! `publicInputsHash` with the 65-byte `r || s || v` signature.

use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use eez_control_rpc::v1::{
    ProveChunk, ProveResponse,
    prover_server::{Prover as ProverService, ProverServer},
};
use eez_protocol::MOCK_PROVER_DIGEST;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use tracing::{Level, event};

/// The `prove.v1.Prover` stub: ignores the window, signs the fixed digest.
#[derive(Debug)]
struct MockProverService {
    signer: PrivateKeySigner,
}

#[tonic::async_trait]
impl ProverService for MockProverService {
    async fn prove(
        &self,
        request: Request<Streaming<ProveChunk>>,
    ) -> Result<Response<ProveResponse>, Status> {
        // Mock semantics: the proof commits to nothing — drain and ignore.
        let mut stream = request.into_inner();
        while stream.message().await?.is_some() {}
        let signature = sign_mock_digest(&self.signer)
            .map_err(|e| Status::internal(format!("mock signer: {e}")))?;
        Ok(Response::new(ProveResponse {
            public_inputs_hash: MOCK_PROVER_DIGEST.to_vec(),
            signature: signature.to_vec(),
        }))
    }
}

/// Sign [`MOCK_PROVER_DIGEST`] and return `r || s || v` (65 bytes).
fn sign_mock_digest(signer: &PrivateKeySigner) -> Result<[u8; 65], alloy_signer::Error> {
    let sig = signer.sign_hash_sync(&MOCK_PROVER_DIGEST)?;

    // MockECDSAProofSystem expects `abi.encodePacked(r, s, v)`:
    //   r: bytes32, s: bytes32, v: uint8 (27 | 28).
    let mut out = [0u8; 65];
    out[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
    out[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
    // alloy's `Signature::v()` returns a `bool` (parity bit). EIP-2
    // canonical recovery id is 0/1; on-chain ECDSA verify wants the
    // legacy 27/28 form.
    out[64] = u8::from(sig.v()) + 27;
    Ok(out)
}

/// Bind an ephemeral localhost port, serve the stub on it in a spawned task,
/// and return the `http://…` URL to hand `RemoteProver::new`.
pub async fn spawn(signer: PrivateKeySigner) -> eyre::Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(
                ProverServer::new(MockProverService { signer })
                    .max_decoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(eez_control_rpc::MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
        {
            event!(
                name: "eez.node.mock_prover.exited",
                Level::ERROR,
                error = %e,
                "in-process mock prover server exited",
            );
        }
    });
    Ok(format!("http://{addr}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Signature, U256};
    use std::str::FromStr;

    /// The stub signs `MOCK_PROVER_DIGEST`; ecrecover against the same
    /// digest returns the signer address. This locks in the agreed-upon
    /// contract between the mock prover and `MockECDSAProofSystem.verify`.
    #[test]
    fn mock_proof_roundtrip_against_ecrecover() {
        let key = PrivateKeySigner::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let signer_addr = key.address();

        let proof = sign_mock_digest(&key).unwrap();
        let v = proof[64];
        assert!(v == 27 || v == 28, "v must be 27 or 28, got {v}");

        let r = U256::from_be_slice(&proof[..32]);
        let s = U256::from_be_slice(&proof[32..64]);
        let sig = Signature::new(r, s, v == 28);
        let recovered = sig
            .recover_address_from_prehash(&MOCK_PROVER_DIGEST)
            .unwrap();
        assert_eq!(
            recovered, signer_addr,
            "ECDSA sig over MOCK_PROVER_DIGEST must recover the signer",
        );
    }
}
