use super::*;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::B256;
use eez_primitives::{EEZL2_ADDRESS, EezTxEnvelope, SystemTransaction};

fn system() -> EezTxEnvelope {
    SystemTransaction {
        chain_id: 1,
        nonce: 0,
        to: EEZL2_ADDRESS,
        value: U256::ZERO,
        input: Bytes::from_static(&[1, 2, 3, 4]),
    }
    .into()
}

#[test]
fn engine_payloads_keep_native_transactions_and_empty_blob_bundles() {
    use alloy_rpc_types_engine::{
        BlobsBundleV1, BlobsBundleV2, ExecutionPayloadEnvelopeV3, ExecutionPayloadEnvelopeV4,
        ExecutionPayloadEnvelopeV5, ExecutionPayloadEnvelopeV6,
    };
    use reth_primitives_traits::SealedBlock;
    use std::sync::Arc;

    let block = Block::new(
        Default::default(),
        alloy_consensus::BlockBody {
            transactions: vec![system()],
            ..Default::default()
        },
    );
    let built = EezBuiltPayload::new(
        Arc::new(SealedBlock::seal_slow(block).try_recover().unwrap()),
        U256::ZERO,
        None,
        Some(Bytes::from_static(&[0xc0])),
    );
    let v3 = ExecutionPayloadEnvelopeV3::try_from(built.clone()).unwrap();
    let v4 = ExecutionPayloadEnvelopeV4::try_from(built.clone()).unwrap();
    let v5 = ExecutionPayloadEnvelopeV5::try_from(built.clone()).unwrap();
    let v6 = ExecutionPayloadEnvelopeV6::try_from(built).unwrap();
    assert_eq!(v3.blobs_bundle, BlobsBundleV1::empty());
    assert_eq!(v4.envelope_inner.blobs_bundle, BlobsBundleV1::empty());
    assert_eq!(v5.blobs_bundle, BlobsBundleV2::empty());
    assert_eq!(v6.blobs_bundle, BlobsBundleV2::empty());
    assert_eq!(
        v3.execution_payload
            .payload_inner
            .payload_inner
            .transactions,
        vec![Bytes::from(system().encoded_2718())]
    );
    assert_eq!(v4.envelope_inner.execution_payload, v3.execution_payload);
    assert_eq!(v5.execution_payload, v3.execution_payload);
    assert_eq!(v6.execution_payload.payload_inner, v3.execution_payload);
}

#[test]
fn engine_block_conversion_preserves_prague_requests_hash() {
    use alloy_eips::eip7685::Requests;
    use reth_payload_primitives::PayloadTypes;
    use reth_primitives_traits::SealedBlock;

    for requests in [
        Requests::default(),
        Requests::new(vec![Bytes::from_static(&[0, 1])]),
    ] {
        let requests_hash = requests.requests_hash();
        let block = SealedBlock::seal_slow(Block::new(
            alloy_consensus::Header {
                base_fee_per_gas: Some(1),
                withdrawals_root: Some(alloy_consensus::proofs::calculate_withdrawals_root(&[])),
                blob_gas_used: Some(0),
                excess_blob_gas: Some(0),
                parent_beacon_block_root: Some(B256::ZERO),
                requests_hash: Some(requests_hash),
                ..Default::default()
            },
            alloy_consensus::BlockBody {
                withdrawals: Some(Default::default()),
                ..Default::default()
            },
        ));
        let expected_hash = block.hash();
        let data = EezEngineTypes::block_to_payload(block, None);
        assert_eq!(data.sidecar.requests_hash(), Some(requests_hash));
        let restored = data
            .payload
            .try_into_block_with_sidecar::<EezTxEnvelope>(&data.sidecar)
            .unwrap();
        assert_eq!(restored.header.requests_hash, Some(requests_hash));
        assert_eq!(SealedBlock::seal_slow(restored).hash(), expected_hash);
    }
}
