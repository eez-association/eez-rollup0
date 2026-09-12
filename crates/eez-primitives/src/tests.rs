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
fn rpc_serialization_has_type_and_quantities_without_signature() {
    let tx = system();
    let json = serde_json::to_value(&tx).unwrap();
    assert_eq!(json["type"], "0x76");
    assert_eq!(json["hash"], serde_json::to_value(tx.tx_hash()).unwrap());
    assert_eq!(json["chainId"], "0x1");
    assert_eq!(json["nonce"], "0x0");
    assert!(json.get("gas").is_none());
    assert!(json.get("gasPrice").is_none());
    assert_eq!(tx.gas_limit(), SYSTEM_TX_GAS_LIMIT);
    assert_eq!(tx.effective_gas_price(Some(100)), 0);
    for field in ["v", "r", "s", "yParity"] {
        assert!(json.get(field).is_none());
    }
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
