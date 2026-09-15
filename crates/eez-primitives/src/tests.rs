use super::*;
use alloy_consensus::{SignableTransaction, TxLegacy, TxReceipt};
use alloy_primitives::Signature;

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
    let built = engine::EezBuiltPayload::new(
        Arc::new(SealedBlock::seal_slow(block)),
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
        let data = engine::EezEngineTypes::block_to_payload(block, None);
        assert_eq!(data.sidecar.requests_hash(), Some(requests_hash));
        let restored = data
            .payload
            .try_into_block_with_sidecar::<EezTxEnvelope>(&data.sidecar)
            .unwrap();
        assert_eq!(restored.header.requests_hash, Some(requests_hash));
        assert_eq!(SealedBlock::seal_slow(restored).hash(), expected_hash);
    }
}

#[test]
fn native_wire_sender_hash_and_storage_round_trip() {
    let tx = system();
    let bytes = tx.encoded_2718();
    let expected =
        alloy_primitives::hex!("76dd0180944200000000000000000000000000000000000007808401020304");
    assert_eq!(bytes, expected);
    assert_eq!(tx.tx_hash(), &alloy_primitives::keccak256(expected));
    assert_eq!(tx.recover_signer().unwrap(), SYSTEM_ADDRESS);
    assert_eq!(tx.recover_signer_unchecked().unwrap(), SYSTEM_ADDRESS);
    assert_eq!(EezTxEnvelope::decode_2718_exact(&bytes).unwrap(), tx);
    assert_eq!(
        alloy_rlp::decode_exact::<EezTxEnvelope>(&alloy_rlp::encode(&tx)).unwrap(),
        tx
    );
    assert_eq!(
        <EezTxEnvelope as reth_codecs::Decompress>::decompress(
            &<EezTxEnvelope as reth_codecs::Compress>::compress(tx.clone())
        )
        .unwrap(),
        tx
    );
    assert!(reth_ethereum_primitives::TransactionSigned::decode_2718_exact(&bytes).is_err());
}

#[test]
fn rejects_malformed_and_trailing_native_bytes() {
    let bytes = system().encoded_2718();
    for len in 0..bytes.len() {
        assert!(EezTxEnvelope::decode_2718_exact(&bytes[..len]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(EezTxEnvelope::decode_2718_exact(&trailing).is_err());
    let mut wrong_target = bytes;
    wrong_target[10] ^= 1;
    assert!(EezTxEnvelope::decode_2718_exact(&wrong_target).is_err());
    assert!(<EezTxEnvelope as reth_codecs::Decompress>::decompress(&wrong_target).is_err());
}

#[test]
fn ethereum_bytes_and_sender_are_unchanged() {
    let eth: reth_ethereum_primitives::TransactionSigned = TxLegacy {
        chain_id: Some(1),
        nonce: 42,
        gas_price: 7,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ONE,
        input: Bytes::new(),
    }
    .into_signed(Signature::new(U256::ONE, U256::ONE, false))
    .into();
    let tx = EezTxEnvelope::Ethereum(eth.clone());
    assert_eq!(tx.encoded_2718(), eth.encoded_2718());
    assert_eq!(alloy_rlp::encode(&tx), alloy_rlp::encode(&eth));
    assert_eq!(tx.recover_signer().unwrap(), eth.recover_signer().unwrap());
    assert_eq!(tx.tx_hash(), eth.tx_hash());
    assert_eq!(
        EezTxEnvelope::decode_2718_exact(&eth.encoded_2718()).unwrap(),
        tx
    );
}

#[test]
fn native_is_not_a_public_pooled_transaction() {
    type Pooled = alloy_consensus::EthereumTxEnvelope<alloy_consensus::TxEip4844WithSidecar>;
    assert!(Pooled::try_from(system()).is_err());
}

#[test]
fn rpc_serialization_has_native_execution_fields_and_zero_signature_placeholders() {
    let tx = system();
    let json = serde_json::to_value(&tx).unwrap();
    assert_eq!(json["type"], "0x76");
    assert_eq!(json["hash"], serde_json::to_value(tx.tx_hash()).unwrap());
    assert_eq!(json["chainId"], "0x1");
    assert_eq!(json["nonce"], "0x0");
    assert_eq!(json["gas"], "0x1e8480");
    assert_eq!(json["gasPrice"], "0x0");
    assert_eq!(tx.gas_limit(), SYSTEM_TX_GAS_LIMIT);
    assert_eq!(tx.effective_gas_price(Some(100)), 0);
    let rpc = alloy_rpc_types_eth::Transaction::from_transaction(
        alloy_consensus::transaction::Recovered::new_unchecked(tx.clone(), SYSTEM_ADDRESS),
        alloy_consensus::transaction::TransactionInfo {
            base_fee: Some(100),
            ..Default::default()
        },
    );
    let rpc_json = serde_json::to_value(&rpc).unwrap();
    assert_eq!(rpc_json["gas"], "0x1e8480");
    assert_eq!(rpc_json["gasPrice"], "0x0");
    assert_eq!(
        rpc_json["from"],
        serde_json::to_value(SYSTEM_ADDRESS).unwrap()
    );
    for field in ["v", "r", "s"] {
        assert_eq!(json[field], "0x0");
        assert_eq!(rpc_json[field], "0x0");
    }
    assert!(json.get("yParity").is_none());
    assert_eq!(serde_json::from_value::<EezTxEnvelope>(json).unwrap(), tx);
}

#[test]
fn receipt_type_survives_consensus_and_database_encoding() {
    use reth_codecs::Compact;
    for ty in [
        EezTxType::System,
        EezTxType::Ethereum(alloy_consensus::TxType::Legacy),
        EezTxType::Ethereum(alloy_consensus::TxType::Eip1559),
    ] {
        for (success, logs) in [
            (false, vec![]),
            (
                true,
                vec![alloy_primitives::Log::new_unchecked(
                    EEZL2_ADDRESS,
                    vec![B256::repeat_byte(7)],
                    Bytes::from(vec![1; 256]),
                )],
            ),
        ] {
            let receipt = Receipt {
                tx_type: ty,
                success,
                cumulative_gas_used: 42_000,
                logs,
            };
            let mut stored = Vec::new();
            let len = receipt.to_compact(&mut stored);
            assert_eq!(Receipt::from_compact(&stored, len).0, receipt);
            let receipt = alloy_consensus::ReceiptWithBloom {
                logs_bloom: receipt.bloom(),
                receipt,
            };
            let encoded = receipt.encoded_2718();
            assert_eq!(
                alloy_consensus::ReceiptWithBloom::<Receipt>::decode_2718_exact(&encoded).unwrap(),
                receipt
            );
            if ty == EezTxType::System {
                assert_eq!(encoded[0], SYSTEM_TX_TYPE);
            }
        }
    }
}
