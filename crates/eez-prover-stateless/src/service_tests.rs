//! Captured Stateless request through the shared gRPC signer pipeline.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use eez_control_rpc::v1::prover_client::ProverClient;
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, prove_chunk,
};
use eez_proof_signer::{
    Attester, NonZeroProofSystemVkey, ProveSvc, ServiceLimits, ServiceLimitsParams, ServiceState,
    Validator,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

use crate::Backend;

const FIXTURE: &str = "captured-devnet-window-84";
const SYSTEM_ADDRESS: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076";
const ATTESTER_KEY: &str = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

fn fixture(name: &str) -> String {
    let path = format!(
        "{}/tests/fixtures/{FIXTURE}/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read fixture {path}: {error}"))
}

fn fixture_hex(value: &str) -> Vec<u8> {
    let value = value.trim();
    hex::decode(value.strip_prefix("0x").unwrap_or(value)).unwrap()
}

fn fixture_str<'a>(value: &'a serde_json::Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn fixture_u64(value: &serde_json::Value, field: &str) -> u64 {
    value[field].as_u64().unwrap()
}

fn recorded_wire_witness(encoded: &str) -> ExecutionWitness {
    let witness: serde_json::Value = serde_json::from_str(encoded).unwrap();
    let decode_items = |field: &str| {
        witness[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| fixture_hex(item.as_str().unwrap()))
            .collect()
    };
    ExecutionWitness {
        state: decode_items("state"),
        codes: decode_items("codes"),
        keys: decode_items("keys"),
        headers: decode_items("headers"),
    }
}

/// The end-to-end regression anchor: a complete window recorded from a live rig,
/// replayed through the real service.
///
/// `postbatch.hex` is the calldata actually mined on L1, correlated to this
/// window through the composer's settlement record; the digest is the one the
/// signer attested; the signature is generated independently, so the assertion
/// compares against a value this service did not produce. The window endpoints
/// are block hashes — under state-root commitments those fields held a
/// different kind of value, which is what this pins.
///
/// Regenerate with `scripts/capture-signer-window.sh`.
#[tokio::test]
async fn captured_window_is_validated_and_signed_by_the_shared_service() {
    let oracle: serde_json::Value = serde_json::from_str(&fixture("oracle.json")).unwrap();
    let expected_hash = fixture_str(&oracle, "public_inputs_hash")
        .parse::<B256>()
        .unwrap();
    let expected_signature = fixture_hex(fixture_str(&oracle, "expected_test_signature"));
    let proof_system = fixture_str(&oracle, "proof_system")
        .parse::<Address>()
        .unwrap();
    let proof_system_vkey = NonZeroProofSystemVkey::new(
        fixture_str(&oracle, "proof_system_vkey")
            .parse::<B256>()
            .unwrap(),
    )
    .unwrap();
    let from = fixture_u64(&oracle, "from_block");
    let to = fixture_u64(&oracle, "to_block");
    let rollup_id = NonZeroU64::new(fixture_u64(&oracle, "rollup_id")).unwrap();

    let mut window = vec![ProveChunk {
        kind: Some(prove_chunk::Kind::Header(ProveHeader {
            rollup_id: rollup_id.get(),
            from_block: from,
            to_block: to,
            post_batch: Some(PostBatch {
                abi_calldata: fixture_hex(&fixture("postbatch.hex")),
                public_inputs_hash: expected_hash.to_vec(),
                l1_block_hash: Vec::new(),
            }),
        })),
    }];
    let recorded_blocks: serde_json::Value = serde_json::from_str(&fixture("blocks.json")).unwrap();
    for recorded in recorded_blocks.as_array().unwrap() {
        let number = fixture_u64(recorded, "number");
        let rlp = fixture_hex(&fixture(&format!("block-{number}.rlp.hex")));
        let block = alloy_rlp::decode_exact::<eez_primitives::Block>(&rlp).unwrap();
        window.push(ProveChunk {
            kind: Some(prove_chunk::Kind::Block(BlockWitness {
                number,
                hash: block.header.hash_slow().to_vec(),
                parent_hash: block.header.parent_hash.to_vec(),
                rlp,
                witness: Some(recorded_wire_witness(&fixture(&format!(
                    "witness-{number}.json"
                )))),
            })),
        });
    }

    let chain_path = format!(
        "{}/tests/fixtures/{FIXTURE}/chain-config.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let system_address = SYSTEM_ADDRESS.parse::<Address>().unwrap();
    let backend = Backend::from_chain_document_file(chain_path.as_ref(), system_address).unwrap();
    let chain_id = backend.chain_id();
    assert_eq!(chain_id, fixture_u64(&oracle, "l2_chain_id"));
    let validator = Validator::from_backend(backend);
    let attester = Attester::new(
        ATTESTER_KEY.parse().unwrap(),
        proof_system_vkey,
        proof_system,
        system_address,
    )
    .unwrap();
    let limits = ServiceLimits::new(ServiceLimitsParams {
        max_window_blocks: NonZeroUsize::new(512).unwrap(),
        max_window_bytes: NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        max_window_witness_items: NonZeroUsize::new(1_000_000).unwrap(),
        stream_idle_timeout: Duration::from_secs(30),
        request_timeout: Duration::from_secs(30),
    })
    .unwrap();
    let service = ProveSvc::new(
        Arc::new(ServiceState::new(validator, rollup_id, attester).unwrap()),
        limits,
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let mut client = ProverClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let response = client
        .prove(tokio_stream::iter(window))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.public_inputs_hash, expected_hash.as_slice());
    assert_eq!(response.signature, expected_signature);
    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
}
