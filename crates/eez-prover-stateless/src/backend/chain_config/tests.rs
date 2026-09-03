use super::*;

#[test]
fn accepts_full_genesis_and_bare_chain_config_documents() {
    let full_genesis = br#"{
        "config": {
            "chainId": 1,
            "homesteadBlock": 0,
            "londonBlock": 0,
            "shanghaiTime": 0,
            "cancunTime": 0,
            "pragueTime": 0,
            "osakaTime": 0
        },
        "nonce": "0x0",
        "timestamp": "0x6490fdd2",
        "extraData": "0x",
        "gasLimit": "0x1c9c380",
        "difficulty": "0x0",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "coinbase": "0x0000000000000000000000000000000000000000",
        "alloc": {
            "0x0000000000000000000000000000000000000001": {
                "balance": "0x1",
                "nonce": "0x1",
                "code": "0x00",
                "storage": {"0x00": "0x01"}
            }
        },
        "number": "0x0",
        "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "baseFeePerGas": "0x7",
        "blobGasUsed": "0x0",
        "excessBlobGas": "0x0"
    }"#;
    let (genesis, kind) = parse_chain_document(full_genesis).unwrap();
    assert_eq!(kind, ChainDocumentKind::Genesis);
    assert_ne!(genesis.timestamp, 0);
    assert_eq!(genesis.config.chain_id, 1);
    assert_eq!(genesis.config.homestead_block, Some(0));
    assert_eq!(genesis.config.london_block, Some(0));
    assert_eq!(genesis.config.shanghai_time, Some(0));
    assert_eq!(genesis.config.cancun_time, Some(0));
    assert_eq!(genesis.config.prague_time, Some(0));
    assert_eq!(genesis.config.osaka_time, Some(0));
    assert_eq!(genesis.alloc.len(), 1);

    let expected_config = genesis.config.clone();
    let encoded = serde_json::to_vec(&expected_config).unwrap();
    let (bare, kind) = parse_chain_document(&encoded).unwrap();
    assert_eq!(kind, ChainDocumentKind::BareChainConfig);
    assert_eq!(bare.config, expected_config);
    assert_eq!(bare.timestamp, 0);

    let nested = br#"{
        "chainId": 1,
        "clique": {"period": 5, "epoch": 30000},
        "blobSchedule": {
            "cancun": {"target": 3, "max": 6, "baseFeeUpdateFraction": 3338477}
        }
    }"#;
    let (nested, kind) = parse_chain_document(nested).unwrap();
    assert_eq!(kind, ChainDocumentKind::BareChainConfig);
    assert_eq!(nested.config.clique.unwrap().period, Some(5));
    assert_eq!(nested.config.blob_schedule.len(), 1);
}

#[test]
fn rejects_ambiguous_or_default_only_chain_documents() {
    let invalid: &[&[u8]] = &[
        b"{}",
        b"[]",
        br#"{"config": {}}"#,
        br#"{"timestamp": "0x0", "alloc": {}}"#,
        br#"{"confg": {"chainId": 1}, "timestamp": "0x0"}"#,
    ];
    for encoded in invalid {
        assert!(parse_chain_document(encoded).is_err());
    }
}

#[test]
fn rejects_unknown_chain_document_fields_and_reports_them_deterministically() {
    let cases: &[(&[u8], &str)] = &[
        (
            br#"{"chainId": 1, "shangaiTime": 0}"#,
            "bare ChainConfig contains unsupported fields: `shangaiTime`",
        ),
        (
            br#"{"config": {"chainId": 1, "cancuunTime": 0}}"#,
            "Genesis `config` contains unsupported fields: `cancuunTime`",
        ),
        (
            br#"{"config": {"chainId": 1}, "mysteryGenesisField": true}"#,
            "Genesis contains unsupported fields: `mysteryGenesisField`",
        ),
        (
            br#"{"chainId": 1, "zUnknown": 0, "aUnknown": 0}"#,
            "bare ChainConfig contains unsupported fields: `aUnknown`, `zUnknown`",
        ),
        (
            br#"{"chainId": 1, "clique": {"perod": 5}}"#,
            "bare ChainConfig.clique contains unsupported fields: `perod`",
        ),
        (
            br#"{"config": {"chainId": 1, "parlia": {"epoc": 10}}}"#,
            "Genesis `config`.parlia contains unsupported fields: `epoc`",
        ),
        (
            br#"{"chainId": 1, "ethash": {"unexpected": true}}"#,
            "bare ChainConfig.ethash contains unsupported fields: `unexpected`",
        ),
        (
            br#"{"chainId": 1, "blobSchedule": {"cancun": {"targt": 3}}}"#,
            "bare ChainConfig.blobSchedule.cancun contains unsupported fields: `targt`",
        ),
        (
            br#"{"chainId": 1, "blobSchedule": {"prauge": {"target": 6}}}"#,
            "bare ChainConfig.blobSchedule contains unsupported fields: `prauge`",
        ),
    ];

    for (encoded, expected) in cases {
        let error = parse_chain_document(encoded).unwrap_err();
        assert_eq!(error.to_string(), *expected);
    }
}
