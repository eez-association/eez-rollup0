//! eez-proverd — the composer-controlled prover server.
//!
//! One RPC: `prove.v1.Prover/Prove`. The composer client-streams a posted
//! settlement window (a header then one chunk per block). The daemon re-executes
//! it statelessly (`native-validate`), runs the fail-closed settlement gates
//! ([`gates`]), and — only if EVERY gate passes — ECDSA-signs the recomputed
//! `publicInputsHash` and returns it. No feed, no dispatch, no sink, no cursor.
//!
//! The validator (`--validator-bin` + `--chain-config`) is MANDATORY: the daemon
//! never signs state it did not re-execute (an un-validated signer would attest
//! composer-supplied roots).

mod gates;

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use clap::Parser;
use eez_control_rpc::v1::{
    ProveChunk, ProveResponse, prove_chunk, prover_server::Prover, prover_server::ProverServer,
};
use eez_evm::EcdsaProofSigner;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use tracing::{Level, event};

#[derive(Parser, Debug)]
#[command(
    name = "eez-proverd",
    about = "EEZ composer-controlled prover (Prove RPC)"
)]
struct Args {
    /// The `Prove` gRPC bind address the composer dials.
    #[arg(long, env = "EEZ_PROVERD_ADDR", default_value = "0.0.0.0:50061")]
    listen_addr: SocketAddr,
    /// ZisK's `native-validate` — the stateless re-executor. MANDATORY.
    #[arg(long, env = "EEZ_VALIDATOR_BIN")]
    validator_bin: String,
    /// L2 chain-config JSON for `native-validate`. MANDATORY.
    #[arg(long, env = "EEZ_CHAIN_CONFIG")]
    chain_config: String,
    /// The proof system's registered attester key (32-byte hex). Signs the
    /// recomputed publicInputsHash.
    #[arg(long, env = "EEZ_PROOF_SIGNER_KEY")]
    signer_key: String,
    /// Scratch dir for per-window validator inputs.
    #[arg(
        long,
        env = "EEZ_VALIDATOR_WORKDIR",
        default_value = "/tmp/eez-proverd"
    )]
    work_dir: String,
    /// Proof-system vkey (32-byte hex). Defaults to `bytes32(uint160(signer))`.
    #[arg(long, env = "EEZ_VKEY")]
    vkey: Option<String>,
    /// The deployment's on-chain EEZL2 predeploy (CCM-L2) address. Threaded into
    /// the settlement gate's system-tx classification; must equal the composer's
    /// `EEZ_CCM_L2_ADDRESS`.
    #[arg(long, env = "EEZ_CCM_L2_ADDRESS")]
    ccm_l2_address: String,
}

/// `bytes32(uint160(address))` — the per-rollup registry membership convention.
fn vkey_from_address(addr: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(addr.as_slice());
    B256::from(bytes)
}

fn parse_b256_hex(field: &str, value: &str) -> eyre::Result<B256> {
    let raw = value.trim_start_matches("0x");
    let bytes = hex::decode(raw).map_err(|e| eyre::eyre!("{field}: invalid hex: {e}"))?;
    if bytes.len() != 32 {
        eyre::bail!("{field}: expected 32 bytes, got {}", bytes.len());
    }
    Ok(B256::from_slice(&bytes))
}

/// The `Prove` service. Cheap to clone (`Arc<Inner>`); tonic clones per connection.
#[derive(Clone)]
struct ProveSvc {
    inner: Arc<Inner>,
}

struct Inner {
    validator_bin: String,
    chain_config: String,
    work_dir: String,
    signer: EcdsaProofSigner,
    vkey: B256,
    ccm_l2_address: Address,
}

#[tonic::async_trait]
impl Prover for ProveSvc {
    async fn prove(
        &self,
        request: Request<Streaming<ProveChunk>>,
    ) -> Result<Response<ProveResponse>, Status> {
        let mut stream = request.into_inner();
        let mut header = None;
        let mut staged: Vec<gates::StagedBlock> = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.kind {
                Some(prove_chunk::Kind::Header(h)) => {
                    if header.is_some() {
                        return Err(Status::invalid_argument("duplicate header chunk"));
                    }
                    header = Some(h);
                }
                Some(prove_chunk::Kind::Block(b)) => staged.push(gates::StagedBlock {
                    number: b.number,
                    hash: b.hash,
                    rlp: b.rlp,
                    witness: b.witness,
                }),
                None => {}
            }
        }
        let header =
            header.ok_or_else(|| Status::invalid_argument("stream had no header chunk"))?;
        let pb = header
            .post_batch
            .ok_or_else(|| Status::invalid_argument("header carries no post_batch"))?;
        if staged.is_empty() {
            return Err(Status::invalid_argument("window carries no blocks"));
        }
        let inner = Arc::clone(&self.inner);

        // 1. Stateless re-execution (the independent check — never skipped).
        let vw = gates::validate_window(
            &staged,
            &inner.validator_bin,
            &inner.chain_config,
            &inner.work_dir,
        )
        .await
        .map_err(|e| {
            Status::failed_precondition(format!("native-validate rejected window: {e}"))
        })?;

        // 2. Recompute the publicInputsHash from the authoritative calldata.
        let public_inputs_hash = gates::verify_settlement_public_inputs(&pb, inner.vkey)
            .map_err(|e| Status::failed_precondition(format!("publicInputsHash: {e}")))?;

        // 3. Settlement-chain gate (telescope + effect-prefix roots).
        let batch = eez_evm::entries::decode_postbatch(&pb.abi_calldata)
            .map_err(|e| Status::invalid_argument(format!("decode postBatch: {e}")))?;
        // Single-call window: the batch anchor is the re-executed parent root.
        let sync_rlp = staged.last().map_or(&[][..], |b| b.rlp.as_slice());
        gates::verify_settlement_chain(
            &pb,
            &vw,
            vw.parent_state_root,
            sync_rlp,
            inner.ccm_l2_address,
        )
        .map_err(|e| Status::failed_precondition(format!("settlement chain: {e}")))?;

        // 4. Inbound N:N bijection (sealed deliveries vs deferred carriers).
        let sealed: Vec<_> = staged
            .iter()
            .flat_map(|b| gates::extract_inbounds(&b.rlp))
            .collect();
        gates::multi_inbound_outcome_gate(&batch, &sealed)
            .map_err(|e| Status::failed_precondition(format!("inbound-outcome gate: {e}")))?;

        // 5. Outbound authorization (settlement entries vs re-executed events).
        let observed = vw.sync_outbound_call_hashes.clone().unwrap_or_default();
        gates::verify_outbound_authorized(&batch, &observed, header.rollup_id)
            .map_err(|e| Status::failed_precondition(format!("outbound gate: {e}")))?;

        // 6. All gates passed → attest.
        let signature = inner
            .signer
            .sign_prehash(public_inputs_hash)
            .map_err(|e| Status::internal(format!("sign publicInputsHash: {e}")))?;
        event!(
            name: "eez.proverd.attested",
            Level::INFO,
            from = header.from_block,
            to = header.to_block,
            blocks = staged.len(),
            %public_inputs_hash,
            "window re-executed + gated + signed",
        );
        Ok(Response::new(ProveResponse {
            public_inputs_hash: public_inputs_hash.to_vec(),
            signature: signature.to_vec(),
        }))
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,eez_proverd=info".into()),
        )
        .init();
    let args = Args::parse();

    let key = parse_b256_hex("EEZ_PROOF_SIGNER_KEY", &args.signer_key)?;
    let signer = EcdsaProofSigner::from_private_key(key)
        .map_err(|e| eyre::eyre!("build proof signer: {e}"))?;
    let vkey = match &args.vkey {
        Some(v) => parse_b256_hex("EEZ_VKEY", v)?,
        None => vkey_from_address(signer.address()),
    };
    let ccm_l2_address: Address = args
        .ccm_l2_address
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_CCM_L2_ADDRESS: {e}"))?;

    event!(
        name: "eez.proverd.starting",
        Level::INFO,
        listen_addr = %args.listen_addr,
        attester = %signer.address(),
        %vkey,
        validator_bin = %args.validator_bin,
        "eez-proverd starting (validator mandatory)",
    );

    let svc = ProveSvc {
        inner: Arc::new(Inner {
            validator_bin: args.validator_bin,
            chain_config: args.chain_config,
            work_dir: args.work_dir,
            signer,
            vkey,
            ccm_l2_address,
        }),
    };
    Server::builder()
        .add_service(ProverServer::new(svc))
        .serve(args.listen_addr)
        .await?;
    Ok(())
}
