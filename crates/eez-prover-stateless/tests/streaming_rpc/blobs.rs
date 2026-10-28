use std::{num::NonZeroUsize, sync::Arc};

use alloy_primitives::{Address, Bytes, Signature, U256};
use eez_proof_signer::{Attester, ProveSvc, ServiceLimits, ServiceLimitsParams, ServiceState};
use eez_prover_stateless::Backend;
use tokio_stream::wrappers::TcpListenerStream;

use super::*;

fn settlement_context(context: &ProvingContext, range: std::ops::Range<usize>) -> ProvingContext {
    let mut selected = context.clone();
    if range.start > 0 {
        let parent =
            alloy_rlp::decode_exact::<eez_primitives::Block>(&context.blocks[range.start - 1].rlp)
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
            let block = alloy_rlp::decode_exact::<eez_primitives::Block>(&block.rlp).unwrap();
            assert!(block.body.transactions.is_empty());
            eez_payload_codec::SpanBlock {
                beneficiary: block.header.beneficiary.into_array(),
                extra_data: block.header.extra_data.to_vec(),
                transactions: Vec::new(),
            }
        })
        .collect::<Vec<_>>();
    selected.batch.callData = Bytes::new();
    let blobs = eez_payload_codec::encode_blob_container(selected.rollup_id, &span, &[]).unwrap();
    selected.batch.blockNumber = 0;
    selected.batch.blobIndices = (0..blobs.len()).map(U256::from).collect();
    eez_protocol::entries::finalize_l1_rolling_hashes(&mut selected.batch).unwrap();
    selected.blobs = blobs.into_iter().map(Bytes::from).collect();
    selected
}

async fn check_proof(prover: &RemoteProver, context: ProvingContext, vkey: B256, signer: Address) {
    let hashes = eez_blob::versioned_hashes(&context.blobs).unwrap();
    let expected =
        eez_protocol::public_inputs::public_inputs_hashes_with_blobs(&context.batch, vkey, &hashes)
            .unwrap()[0];
    let proof = prover.prove(context).await.unwrap();
    assert_eq!(
        Signature::try_from(proof.as_ref())
            .unwrap()
            .recover_address_from_prehash(&expected)
            .unwrap(),
        signer
    );
}

#[tokio::test]
async fn captured_blob_windows_recover_after_quota_exhaustion_and_invalid_da() {
    let oracle: serde_json::Value = serde_json::from_str(&fixture("oracle.json")).unwrap();
    let (_, context) = recorded_window(&oracle);
    let first = settlement_context(&context, 0..3);
    let second = settlement_context(&context, 3..6);
    let whole = settlement_context(&context, 0..6);
    let vkey: B256 = fixture_str(&oracle, "proof_system_vkey").parse().unwrap();
    let system = SYSTEM_ADDRESS.parse().unwrap();
    let attester = || {
        Attester::new(
            ATTESTER_KEY.parse().unwrap(),
            eez_proof_signer::NonZeroProofSystemVkey::new(vkey).unwrap(),
            fixture_str(&oracle, "proof_system").parse().unwrap(),
            system,
        )
        .unwrap()
    };
    let signer = attester().address();

    for maximum in [3, 6] {
        let chain_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/{FIXTURE}/chain-config.json"));
        let backend = Backend::from_chain_document_file(&chain_path, system).unwrap();
        let state = ServiceState::new(
            backend,
            NonZeroU64::new(context.rollup_id).unwrap(),
            attester(),
        )
        .unwrap();
        let limits = ServiceLimits::new(ServiceLimitsParams {
            max_window_blocks: NonZeroUsize::new(maximum).unwrap(),
            max_window_bytes: NonZeroUsize::new(16 * 1024 * 1024).unwrap(),
            max_window_witness_items: NonZeroUsize::new(1_000_000).unwrap(),
            stream_idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        })
        .unwrap();
        let service = ProveSvc::new(Arc::new(state), limits);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.into_streaming_server())
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = done.await;
                })
                .await
                .unwrap();
        });
        let prover = RemoteProver::new(endpoint, signer);
        check_proof(&prover, first.clone(), vkey, signer).await;
        check_proof(&prover, second.clone(), vkey, signer).await;
        if maximum == 6 {
            check_proof(&prover, whole.clone(), vkey, signer).await;
        } else {
            let error = prover.prove(whole.clone()).await.unwrap_err();
            assert!(error.to_string().contains("quota exceeded"), "{error}");
            check_proof(&prover, second.clone(), vkey, signer).await;
        }
        let mut invalid = second.clone();
        invalid.blobs[0] = Bytes::from(vec![0xff; eez_blob::BYTES_PER_BLOB]);
        assert!(prover.prove(invalid).await.is_err());
        check_proof(&prover, second.clone(), vkey, signer).await;
        drop(prover);
        let _ = shutdown.send(());
        server.await.unwrap();
    }
}
