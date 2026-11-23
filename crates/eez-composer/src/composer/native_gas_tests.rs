use super::*;
use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::Decodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Sealable, TxKind, bytes};
use alloy_signer_local::PrivateKeySigner;
use eez_protocol::system_tx::{PlannedTransaction, SystemTxContext};
use reth_chainspec::ChainSpecBuilder;
use reth_primitives_traits::SealedHeader;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};

fn fixture(
    code: Bytes,
) -> (
    MockEthProvider,
    eez_evm::EezEvmConfig,
    SealedHeader<alloy_consensus::Header>,
    PrivateKeySigner,
    Address,
) {
    let signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(7)).unwrap();
    let provider = MockEthProvider::new();
    provider.add_account(
        signer.address(),
        ExtendedAccount::new(0, U256::from(10u64.pow(18))),
    );
    let burner = Address::repeat_byte(0x12);
    // Burn until less than 500k gas remains. This changes no contract storage.
    provider.add_account(
        burner,
        ExtendedAccount::new(0, U256::ZERO).with_bytecode(bytes!("5b6207a1205a1160005700")),
    );
    provider.add_account(
        eez_primitives::EEZL2_ADDRESS,
        ExtendedAccount::new(0, U256::ZERO).with_bytecode(code),
    );
    let config = eez_evm::EezEvmConfig::new(Arc::new(
        ChainSpecBuilder::mainnet().cancun_activated().build(),
    ))
    .unwrap();
    let parent = SealedHeader::seal_slow(alloy_consensus::Header {
        number: 1,
        timestamp: 1000,
        gas_limit: BUILDER_GAS_LIMIT,
        base_fee_per_gas: Some(0),
        excess_blob_gas: Some(0),
        blob_gas_used: Some(0),
        ..Default::default()
    });
    (provider, config, parent, signer, burner)
}

fn signed(signer: &PrivateKeySigner, nonce: u64, to: Address, gas_limit: u64) -> Bytes {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1,
        gas_limit,
        to: TxKind::Call(to),
        ..Default::default()
    };
    let signature = signer.sign_transaction_sync(&mut tx).unwrap();
    let tx: eez_primitives::EezTxEnvelope = tx.into_signed(signature).into();
    tx.encoded_2718().into()
}

fn delivery() -> PlannedTransaction {
    use eez_protocol::entries::{InboundSidecar, IncomingEntry};
    let entry = InboundSidecar::new(IncomingEntry {
        target: Address::repeat_byte(0x23),
        source: Address::repeat_byte(0x34),
        value: U256::ZERO,
        data: Bytes::new(),
        source_rollup_id: eez_protocol::RollupId::MAINNET,
        l2_rollup_id: eez_protocol::RollupId(1),
        return_data: Bytes::new(),
        success: true,
    })
    .unwrap();
    let cfg = SystemTxContext {
        eezl2_address: eez_primitives::EEZL2_ADDRESS,
        l2_chain_id: 1,
        this_rollup_id: 1,
    };
    PlannedTransaction::Native(
        eez_protocol::system_tx::build_inbound_system_txs(
            &[entry],
            &cfg,
            eez_protocol::native_source::SourceEntry::new(1, B256::repeat_byte(1), 0).unwrap(),
        )
        .unwrap()
        .remove(0),
    )
}

#[test]
fn permanent_native_revert_is_not_capacity_under_load() {
    let (provider, config, parent, signer, burner) = fixture(bytes!("63deadbeef6000526004601cfd"));
    let open = |prefix: &[Bytes]| {
        SyncBlockState::open(
            Arc::new(provider.clone()),
            &config,
            &parent,
            1012,
            Address::ZERO,
            prefix,
        )
        .unwrap()
    };
    let plan = delivery();
    let full = append_and_execute(&mut open(&[]), std::slice::from_ref(&plan), &mut Vec::new())
        .unwrap_err();
    assert!(matches!(full.1, BuildError::ExecuteTx { .. }));
    let mut prefix = open(&[signed(&signer, 0, burner, 14_500_000)]);
    assert!(prefix.gas_used() > 13_222_784);
    let reduced = append_and_execute(&mut prefix, &[plan], &mut Vec::new()).unwrap_err();
    assert!(matches!(reduced.1, BuildError::ExecuteTx { .. }));
}

#[test]
fn native_packing_dependent_success_is_refused_without_commit() {
    // One program returns GAS; the other stores it while returning identical
    // output. Both succeed at either budget, so status alone misses the change.
    for code in [
        bytes!("5a5f5260205ff3"),
        bytes!("36156009575a5f55005b5f545f5260205ff3"),
    ] {
        let writes_storage = code.len() > 8;
        let (provider, config, parent, signer, burner) = fixture(code);
        let open = |prefix: &[Bytes]| {
            SyncBlockState::open(
                Arc::new(provider.clone()),
                &config,
                &parent,
                1012,
                Address::ZERO,
                prefix,
            )
            .unwrap()
        };
        let plan = delivery();
        let mut empty = open(&[]);
        let full = empty
            .execute_tx(&plan.at_position(BUILDER_GAS_LIMIT, 0).unwrap())
            .unwrap();
        let mut busy = open(&[signed(&signer, 0, burner, 14_500_000)]);
        let gas = busy.gas_used();
        assert!(gas > 13_222_784);
        let reduced = busy
            .execute_tx(&plan.at_position(BUILDER_GAS_LIMIT, gas).unwrap())
            .unwrap_err();
        assert!(full.success);
        assert!(matches!(reduced, BuildError::Capacity));
        assert_eq!(
            busy.gas_used(),
            gas,
            "capacity refusal cannot advance the prefix"
        );
        if writes_storage {
            let read = busy
                .execute_tx(&signed(&signer, 1, eez_primitives::EEZL2_ADDRESS, 100_000))
                .unwrap();
            assert!(read.success);
            assert_eq!(
                read.output.as_ref(),
                &[0; 32],
                "neither speculative execution may commit storage"
            );
        }
    }
}
#[test]
fn user_fits_before_load_but_not_after() {
    let (provider, config, parent, signer, burner) = fixture(bytes!("00"));
    let mut prefix = SyncBlockState::open(
        Arc::new(provider),
        &config,
        &parent,
        1012,
        Address::ZERO,
        &[signed(&signer, 0, burner, 14_500_000)],
    )
    .unwrap();
    let remaining = BUILDER_GAS_LIMIT - prefix.gas_used();
    assert_eq!(
        block_gas_fit(prefix.gas_used(), remaining, BUILDER_GAS_LIMIT),
        BlockGasFit::Accept
    );
    let cfg = SystemTxContext {
        eezl2_address: eez_primitives::EEZL2_ADDRESS,
        l2_chain_id: 1,
        this_rollup_id: 1,
    };
    let load = eez_protocol::system_tx::build_outbound_load_table_txs(
        &[eez_protocol::abi::L2ExecutionEntrySol::default()],
        &cfg,
        eez_protocol::native_source::SourceEntry::new(1, B256::repeat_byte(1), 0).unwrap(),
    )
    .unwrap()
    .remove(0);
    let plans = [
        PlannedTransaction::Native(load),
        PlannedTransaction::User(signed(&signer, 1, Address::repeat_byte(0x99), remaining)),
    ];
    let failure = append_and_execute(&mut prefix, &plans, &mut Vec::new()).unwrap_err();
    assert!(matches!(failure, (1, BuildError::Capacity)));
}

#[test]
fn empty_block_capacity_is_poison_but_provider_failure_is_retryable() {
    use eez_protocol::{ComposerError, ExecutorError};
    assert!(sim_error_is_poison(&ComposerError::Executor(
        ExecutorError::Capacity
    )));
    assert!(!sim_error_is_poison(&ComposerError::Executor(
        ExecutorError::Provider("offline".into())
    )));
}

#[test]
fn canonical_materialization_uses_recorded_execution_gas() {
    let (provider, config, parent, signer, burner) = fixture(bytes!("00"));
    let mut draft = SyncBlockState::open(
        Arc::new(provider),
        &config,
        &parent,
        1012,
        Address::ZERO,
        &[],
    )
    .unwrap();
    let plans = vec![
        PlannedTransaction::User(signed(&signer, 0, burner, 14_500_000)),
        delivery(),
        delivery(),
    ];
    let mut encoded = Vec::new();
    append_and_execute(&mut draft, &plans, &mut encoded).unwrap();
    assert_eq!(draft.materialize_prefix(&plans).unwrap(), encoded);
    let mut changed = plans.clone();
    changed.swap(0, 1);
    assert_ne!(draft.materialize_prefix(&changed).unwrap(), encoded);
}

#[test]
fn inbound_probe_under_clamp_commits_only_the_canonical_delivery() {
    use eez_protocol::{
        CallMode, CompositionBuilder, ExecutionRequest, RollupId, TargetExecutionSession,
    };

    let genesis: serde_json::Value =
        serde_json::from_str(include_str!("../../../../genesis.json")).unwrap();
    let code = genesis["alloc"][eez_primitives::EEZL2_ADDRESS.to_string()]["code"]
        .as_str()
        .unwrap()
        .parse::<Bytes>()
        .unwrap();
    let (provider, config, parent, signer, burner) = fixture(code);
    let target = Address::repeat_byte(0x23);
    // Increment storage[0] and return it. A committed probe would double count.
    provider.add_account(
        target,
        ExtendedAccount::new(0, U256::ZERO).with_bytecode(bytes!("5f54600101805f555f5260205ff3")),
    );
    let mut draft = SyncBlockState::open(
        Arc::new(provider),
        &config,
        &parent,
        1012,
        Address::ZERO,
        &[signed(&signer, 0, burner, 14_500_000)],
    )
    .unwrap();
    assert!(BUILDER_GAS_LIMIT - draft.gas_used() < eez_primitives::SYSTEM_TX_GAS_CAP);
    let mut session = crate::local::InboundL2TargetSession::new(
        draft.fork().unwrap(),
        SystemTxContext {
            eezl2_address: eez_primitives::EEZL2_ADDRESS,
            l2_chain_id: 1,
            this_rollup_id: 1,
        },
        eez_protocol::native_source::SourceEntry::new(1, B256::repeat_byte(1), 0).unwrap(),
    );
    let request = ExecutionRequest {
        call_mode: CallMode::Mutable,
        target_address: target,
        data: Bytes::new(),
        value: U256::ZERO,
        source_address: Address::repeat_byte(0x34),
        source_rollup_id: RollupId::MAINNET,
    };
    let mut dispatcher = CompositionBuilder::new(RollupId::MAINNET, Default::default());
    for expected in [1u64, 2] {
        let outcome = session.execute(request.clone(), &mut dispatcher).unwrap();
        assert!(outcome.is_success());
        assert_eq!(
            outcome.return_data().unwrap(),
            U256::from(expected).to_be_bytes::<32>()
        );
    }
}

#[test]
fn native_intrinsic_gas_exceeding_clamped_budget_is_capacity_without_commit() {
    use eez_evm::EezEvmTx;
    use reth_primitives_traits::Recovered;

    let (provider, config, parent, signer, burner) = fixture(bytes!("00"));
    let mut draft = SyncBlockState::open(
        Arc::new(provider),
        &config,
        &parent,
        1012,
        Address::ZERO,
        &[signed(&signer, 0, burner, BUILDER_GAS_LIMIT)],
    )
    .unwrap();
    let gas_before = draft.gas_used();
    let raw = delivery()
        .at_position(BUILDER_GAS_LIMIT, gas_before)
        .unwrap();
    let eez_primitives::EezTxEnvelope::System(native) =
        eez_primitives::EezTxEnvelope::decode_2718(&mut raw.as_ref()).unwrap()
    else {
        panic!("expected native envelope")
    };
    let mut native = native.into_inner();
    // The STOP fixture accepts any input at the full cap. Only intrinsic gas
    // prevents this same transaction from running at its clamped position.
    let mut input = native.input.to_vec();
    input.extend(vec![0xff; 40_000]);
    native.input = input.into();
    assert!(native.gas_limit < 21_000 + 16 * 40_000);
    let mut fork = draft.fork().unwrap();
    let (state, env) = fork.state_and_env();
    let tx = Recovered::new_unchecked(
        EezEvmTx::System(native.clone()),
        eez_primitives::SYSTEM_ADDRESS,
    );
    assert!(matches!(
        state.transact(&config, env, &tx, None),
        Err(evm2::registry::HandlerError::IntrinsicGasTooLow { .. })
    ));
    let mut full = native.clone();
    full.gas_limit = eez_primitives::SYSTEM_TX_GAS_CAP;
    assert!(
        state
            .transact(
                &config,
                env,
                &Recovered::new_unchecked(EezEvmTx::System(full), eez_primitives::SYSTEM_ADDRESS),
                None
            )
            .unwrap()
            .result
            .is_success()
    );
    let raw: Bytes = eez_primitives::EezTxEnvelope::System(native.seal_slow())
        .encoded_2718()
        .into();
    assert!(matches!(draft.execute_tx(&raw), Err(BuildError::Capacity)));
    assert_eq!(draft.gas_used(), gas_before);
    assert!(
        draft
            .execute_tx(&signed(&signer, 1, Address::repeat_byte(0x99), 21_000))
            .unwrap()
            .success
    );
}
