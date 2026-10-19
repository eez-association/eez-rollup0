//! Captured Stateless request through the shared gRPC signer pipeline.

use std::num::NonZeroU64;
use std::process::{Child, Command};
use std::time::Duration;

use alloy_primitives::B256;
use alloy_sol_types::SolCall as _;
use eez_control_rpc::v1::prover_client::ProverClient;
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness, PostBatch, ProveChunk, ProveHeader, prove_chunk,
};
use eez_prover::{Prover as _, ProvingAnchor, ProvingContext};
use eez_prover_client::RemoteProver;
use prost::Message as _;
use tokio::net::TcpListener;

const FIXTURE: &str = "captured-devnet-window-84";
const SYSTEM_ADDRESS: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076";
const ATTESTER_KEY: &str = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

#[path = "../../eez-control-rpc/conformance/replay.rs"]
mod replay;

struct SignerProcess(Child);

impl Drop for SignerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_signer(
    oracle: &serde_json::Value,
    extra_args: &[String],
) -> (SignerProcess, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let chain_path = format!(
        "{}/tests/fixtures/{FIXTURE}/chain-config.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut server = SignerProcess(
        Command::new(env!("CARGO_BIN_EXE_eez-proof-signer"))
            .args([
                "--listen-addr",
                &addr.to_string(),
                "--chain-config",
                &chain_path,
                "--rollup-id",
                &fixture_u64(oracle, "rollup_id").to_string(),
                "--vkey",
                fixture_str(oracle, "proof_system_vkey"),
                "--signer-key",
                ATTESTER_KEY,
                "--attester-address",
                fixture_str(oracle, "attester"),
                "--l2-system-address",
                SYSTEM_ADDRESS,
                "--proof-system",
                fixture_str(oracle, "proof_system"),
                "--request-timeout-secs",
                "30",
                "--stream-idle-timeout-secs",
                "30",
            ])
            .args(extra_args)
            .env("RUST_LOG", "info")
            .spawn()
            .unwrap(),
    );
    let endpoint = format!("http://{addr}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "signer exited before readiness"
            );
            if ProverClient::connect(endpoint.clone()).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("signer readiness deadline");
    (server, endpoint)
}

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

    let blocks = window
        .iter()
        .filter_map(|chunk| {
            let Some(prove_chunk::Kind::Block(block)) = &chunk.kind else {
                return None;
            };
            Some(eez_prover::BlockWitness {
                number: block.number,
                hash: B256::from_slice(&block.hash),
                parent_hash: B256::from_slice(&block.parent_hash),
                rlp: block.rlp.clone().into(),
                witness: serde_json::from_str(&fixture(&format!("witness-{}.json", block.number)))
                    .unwrap(),
            })
        })
        .collect::<Vec<_>>();
    let parent = blocks[0]
        .witness
        .headers
        .iter()
        .filter_map(|bytes| alloy_rlp::decode_exact::<alloy_consensus::Header>(bytes).ok())
        .find(|header| header.hash_slow() == blocks[0].parent_hash)
        .expect("recorded witness contains the execution parent");
    let anchor = ProvingAnchor {
        number: parent.number,
        hash: parent.hash_slow(),
        state_root: parent.state_root,
    };
    let context = ProvingContext {
        rollup_id: rollup_id.get(),
        from_block: from,
        to_block: to,
        anchor: Some(anchor),
        batch: eez_protocol::abi::postAndVerifyBatchCall::abi_decode(&fixture_hex(&fixture(
            "postbatch.hex",
        )))
        .unwrap()
        .batch,
        blocks,
        l1_block_hash: None,
    };

    let (_server, endpoint) = start_signer(&oracle, &[]).await;
    let mut client = ProverClient::connect(endpoint.clone()).await.unwrap();
    let response = client
        .prove(tokio_stream::iter(window.clone()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.public_inputs_hash, expected_hash.as_slice());
    assert_eq!(response.signature, expected_signature);

    // The actual Composer client streams checked execution before it supplies
    // settlement calldata. Both transports must attest the independent oracle.
    let signer = alloy_primitives::Signature::try_from(expected_signature.as_slice())
        .unwrap()
        .recover_address_from_prehash(&expected_hash)
        .unwrap();
    let prover = RemoteProver::new(endpoint.clone(), signer);
    for block in &context.blocks {
        prover
            .prevalidate(rollup_id.get(), anchor, block.clone())
            .await
            .unwrap();
    }
    let proof = prover.prove(context.clone()).await.unwrap();
    assert_eq!(proof.as_ref(), expected_signature.as_slice());

    // Independent Composer clients overlap on one real signer process.
    let a = RemoteProver::new(endpoint.clone(), signer);
    let b = RemoteProver::new(endpoint.clone(), signer);
    let (a, b) = tokio::join!(a.prove(context.clone()), b.prove(context.clone()));
    assert_eq!(a.unwrap().as_ref(), expected_signature.as_slice());
    assert_eq!(b.unwrap().as_ref(), expected_signature.as_slice());

    // Exercise duplex backpressure beyond both channel capacities. These are
    // legitimate duplicate submissions, not a claim of a longer executed chain.
    let mut repeated = context.clone();
    repeated.blocks = context.blocks.iter().cycle().take(192).cloned().collect();
    let pipelined = RemoteProver::new(endpoint.clone(), signer);
    assert_eq!(
        pipelined.prove(repeated).await.unwrap().as_ref(),
        expected_signature.as_slice()
    );

    // Split the captured empty-block window into two valid adjacent batches.
    // This exercises the real client's recovery, not just raw wire rejections.
    let subrange = |range: std::ops::Range<usize>| {
        let mut selected = context.clone();
        if range.start > 0 {
            let parent = alloy_rlp::decode_exact::<eez_primitives::Block>(
                &context.blocks[range.start - 1].rlp,
            )
            .unwrap();
            selected.anchor = Some(ProvingAnchor {
                number: parent.header.number,
                hash: parent.header.hash_slow(),
                state_root: parent.header.state_root,
            });
        }
        selected.blocks = context.blocks[range].to_vec();
        selected.from_block = selected.blocks.first().unwrap().number;
        selected.to_block = selected.blocks.last().unwrap().number;
        let update = &mut selected.batch.entries[0].rollupUpdates[0];
        update.currentRoot = selected.anchor.unwrap().hash;
        update.newRoot = selected.blocks.last().unwrap().hash;
        let span = selected
            .blocks
            .iter()
            .map(|block| {
                let decoded = alloy_rlp::decode_exact::<eez_primitives::Block>(&block.rlp).unwrap();
                assert!(decoded.body.transactions.is_empty());
                eez_payload_codec::SpanBlock {
                    beneficiary: decoded.header.beneficiary.into_array(),
                    extra_data: decoded.header.extra_data.to_vec(),
                    transactions: Vec::new(),
                }
            })
            .collect::<Vec<_>>();
        selected.batch.callData =
            eez_payload_codec::encode_container(selected.rollup_id, &span, &[])
                .unwrap()
                .into();
        eez_protocol::entries::finalize_l1_rolling_hashes(&mut selected.batch).unwrap();
        selected
    };
    let first = subrange(0..3);
    let second = subrange(3..6);
    let charges = window[1..]
        .iter()
        .map(|chunk| {
            let Some(prove_chunk::Kind::Block(block)) = &chunk.kind else {
                unreachable!()
            };
            let witness = block.witness.as_ref().unwrap();
            (
                block.encoded_len(),
                witness.state.len()
                    + witness.codes.len()
                    + witness.keys.len()
                    + witness.headers.len(),
            )
        })
        .collect::<Vec<_>>();
    let payload_limit = charges
        .chunks(3)
        .map(|group| group.iter().map(|c| c.0).sum::<usize>())
        .max()
        .unwrap();
    let witness_limit = charges
        .chunks(3)
        .map(|group| group.iter().map(|c| c.1).sum::<usize>())
        .max()
        .unwrap();
    let mut failures = Vec::new();
    for (flag, limit) in [
        ("--max-request-blocks", 3),
        ("--max-request-bytes", payload_limit),
        ("--max-request-witness-items", witness_limit),
        ("rollback", 0),
    ] {
        let args = if flag == "rollback" {
            Vec::new()
        } else {
            vec![flag.to_owned(), limit.to_string()]
        };
        let (_server, endpoint) = start_signer(&oracle, &args).await;
        let prover = RemoteProver::new(endpoint, signer);
        let result = async {
            prover.prove(first.clone()).await?;
            // Each batch fits separately, but together they exceed the quota.
            prover.prove(second.clone()).await?;
            if flag == "rollback" {
                // L1 rolled back both batches; L2 regenerates identical bytes.
                // Ready still matches the tip, but the session has pruned the
                // first batch. Recover without restarting either process.
                assert_eq!(
                    prover.prove(context.clone()).await?.as_ref(),
                    expected_signature.as_slice()
                );
            } else {
                // A fresh session cannot rescue an intrinsically oversized
                // window: surface its rejection rather than looping forever.
                let error = prover.prove(context.clone()).await.unwrap_err();
                assert!(
                    error.to_string().contains("quota exceeded"),
                    "{flag}: {error}"
                );
                assert!(error.retryable_kind().is_none(), "{flag}: {error}");
                // The rejected large window must not strand the next valid one.
                prover.prove(second.clone()).await?;
            }
            Ok::<_, eez_prover::ProverError>(())
        }
        .await;
        if let Err(error) = result {
            failures.push(format!("{flag}: {error}"));
        }
    }
    assert!(
        failures.is_empty(),
        "retained session recovery failed: {failures:#?}"
    );
}

#[tokio::test]
async fn recorded_streaming_vectors_match_the_real_signer() {
    let oracle: serde_json::Value = serde_json::from_str(&fixture("oracle.json")).unwrap();
    let attester = fixture_str(&oracle, "attester").parse().unwrap();
    for filename in [
        "rollup0-transcripts.json",
        "rollup0-limits-transcripts.json",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../eez-control-rpc/conformance")
            .join(filename);
        let document: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(document["oracle"], oracle);
        let cases = document["cases"].as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let args: Vec<String> = case["server_args"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|arg| arg.as_str().unwrap().to_owned())
                .collect();
            // Isolate caches and session state, including the cold-witness case.
            let (_server, endpoint) = start_signer(&oracle, &args).await;
            tokio::time::timeout(
                Duration::from_mins(2),
                replay::replay(&endpoint, case, attester),
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: replay deadline: {error}"))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            eprintln!("PASS {name}");
        }
    }
}
