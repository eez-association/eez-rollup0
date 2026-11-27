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
fn native_execution_follows_full_cap_without_commit() {
    use evm2::{evm::DynDatabase as _, interpreter::Word};

    for (name, code, full_success) in [
        ("revert", bytes!("63deadbeef6000526004601cfd"), false),
        ("invalid", bytes!("fe"), false),
        // Write storage before looping forever; rejection must discard the write.
        ("out of gas", bytes!("60015f555b600456"), false),
        // Branch on whether GAS exceeds 16M: only the full-cap run stops.
        (
            "full success, reduced out of gas",
            bytes!("62f424005a11600d575b6009565b00"),
            true,
        ),
        // The full-cap run hits INVALID; the reduced run succeeds.
        (
            "full invalid, reduced success",
            bytes!("62f424005a11600a57005bfe"),
            false,
        ),
        // Both halt and consume their budgets, but with different stop reasons.
        (
            "full invalid, reduced stack underflow",
            bytes!("62f424005a11600b5750005bfe"),
            false,
        ),
        // Both revert, but their output contains the starting gas.
        ("different revert output", bytes!("5a5f5260205ffd"), false),
    ] {
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
        let full = open(&[])
            .execute_tx(&plan.at_position(BUILDER_GAS_LIMIT, 0).unwrap())
            .unwrap();
        assert_eq!(full.success, full_success, "{name}");
        let mut busy = open(&[signed(&signer, 0, burner, 14_500_000)]);
        let gas_before = busy.gas_used();
        assert!(BUILDER_GAS_LIMIT - gas_before < 16_000_000);
        let reduced = busy.execute_tx(&plan.at_position(BUILDER_GAS_LIMIT, gas_before).unwrap());
        assert!(
            matches!(
                &reduced,
                Err(BuildError::Capacity) if full_success
            ) || matches!(&reduced, Err(BuildError::ExecuteTx { .. }) if !full_success),
            "{name}: {reduced:?}"
        );
        assert_eq!(busy.gas_used(), gas_before, "{name}");
        let mut fork = busy.fork().unwrap();
        assert_eq!(
            fork.state_and_env()
                .0
                .database_mut()
                .get_storage(&eez_primitives::EEZL2_ADDRESS, &Word::ZERO)
                .unwrap(),
            Word::ZERO,
            "{name}: rejected execution cannot leak storage",
        );
    }
}

#[test]
fn inbound_drain_rejects_full_cap_failures_but_defers_capacity_under_load() {
    use crate::composer::CrossChainExecCtx;
    use crate::composer::budget::DEFAULT_MAX_POSTBATCH_GAS;
    use alloy_provider::{ProviderBuilder, mock::Asserter};
    use eez_l1::{L1ReaderConfig, Submitter, SubmitterConfig};
    use eez_protocol::{ChainDialect, ProxyLookupConfig, RollupId, TargetConfig};
    use evm2::{evm::DynDatabase as _, interpreter::Word};

    let genesis: serde_json::Value =
        serde_json::from_str(include_str!("../../../../genesis.json")).unwrap();
    let manager_code = genesis["alloc"][eez_primitives::EEZL2_ADDRESS.to_string()]["code"]
        .as_str()
        .unwrap()
        .parse::<Bytes>()
        .unwrap();
    for (name, bad_code, full_success, reduced_success) in [
        ("revert", bytes!("60015f555f5ffd"), false, false),
        ("invalid", bytes!("60015f55fe"), false, false),
        ("out of gas", bytes!("60015f555b600456"), false, false),
        // At full cap the target hits INVALID; at the reduced budget it writes
        // storage and succeeds. The real EEZL2 must not admit that reduced run.
        (
            "full invalid, reduced success",
            bytes!("62dd40a05a11600e5760015f55005bfe"),
            false,
            true,
        ),
        (
            "full success, reduced out of gas",
            bytes!("62dd40a05a11600d575b6009565b00"),
            true,
            false,
        ),
    ] {
        let (provider, config, parent, bad_signer, burner) = fixture(manager_code.clone());
        let good_signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(8)).unwrap();
        provider.add_account(
            good_signer.address(),
            ExtendedAccount::new(0, U256::from(10u64.pow(18))),
        );
        let targets = [Address::repeat_byte(0x23), Address::repeat_byte(0x24)];
        provider.add_account(
            targets[0],
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(bad_code),
        );
        // Nonempty return data makes the placeholder probe revert even for
        // this valid call. Only the finalized delivery may commit the increment.
        provider.add_account(
            targets[1],
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode(bytes!("5f54600101805f555f5260205ff3")),
        );
        let registry = Address::repeat_byte(0x40);
        let proxies = [Address::repeat_byte(0x41), Address::repeat_byte(0x42)];
        let wrappers = [Address::repeat_byte(0x51), Address::repeat_byte(0x52)];
        let mut registrations = Vec::new();
        for ((proxy, target), wrapper) in proxies.into_iter().zip(targets).zip(wrappers) {
            let mut word = [0u8; 32];
            word[3..11].copy_from_slice(&1u64.to_be_bytes());
            word[11..31].copy_from_slice(target.as_ref());
            word[31] = 1;
            registrations.push((
                eez_protocol::proxy_mapping_key(
                    proxy,
                    ChainDialect::EvmL1Style.proxy_lookup_slot(),
                ),
                U256::from_be_bytes(word),
            ));
            // Increment L1 storage before calling the registered proxy, so a
            // rejected source simulation has observable writes to discard too.
            let mut code = bytes!("5f546001015f555f5f5f5f5f73").to_vec();
            code.extend_from_slice(proxy.as_ref());
            code.extend_from_slice(&bytes!("5af15000"));
            provider.add_account(
                wrapper,
                ExtendedAccount::new(0, U256::ZERO).with_bytecode(code.into()),
            );
        }
        provider.add_account(
            registry,
            ExtendedAccount::new(0, U256::ZERO).extend_storage(registrations),
        );
        let l1_entry = crate::LocalChainClient::new_entry(
            provider.clone(),
            config.clone(),
            RollupId::MAINNET,
            registry,
            ChainDialect::EvmL1Style,
        );
        let l2_entry = crate::LocalChainClient::new_entry(
            provider.clone(),
            config.clone(),
            RollupId(1),
            eez_primitives::EEZL2_ADDRESS,
            ChainDialect::EvmL2Style,
        );
        let reader = L1ReaderConfig {
            rpc_url: "http://127.0.0.1:1".parse().unwrap(),
            trace_rpc_url: "http://127.0.0.1:1".parse().unwrap(),
            eez: registry,
            rollup_id: 1,
            deploy_block: 0,
            beacon_url: None,
        };
        let cc = CrossChainWiring {
            entry_rollup_id: RollupId::MAINNET,
            rollups: HashMap::from([(
                RollupId(1),
                (
                    l2_entry.clone() as Arc<dyn eez_protocol::ChainClient + Send + Sync>,
                    TargetConfig {
                        proxy_lookup: ProxyLookupConfig {
                            contract_address: eez_primitives::EEZL2_ADDRESS,
                            authorized_proxies_slot: ChainDialect::EvmL2Style.proxy_lookup_slot(),
                        },
                        dialect: ChainDialect::EvmL2Style,
                    },
                ),
            )]),
            exec_ctx: Arc::new(CrossChainExecCtx {
                eezl2_address: eez_primitives::EEZL2_ADDRESS,
                l2_chain_id: 1,
                l1_provider: ProviderBuilder::default().connect_mocked_client(Asserter::new()),
                submitter: Submitter::new(SubmitterConfig {
                    builder_rpc_url: reader.rpc_url.clone(),
                    reader,
                    target_rpc_url: None,
                    poster: bad_signer.clone(),
                }),
                l1_poster_signer: bad_signer.clone(),
                l1_chain_id: 1,
                l1_post_batch_priority_fee: 0,
                eez_registry: registry,
                blob_posting: None,
            }),
            local: crate::local::LocalComposeClients { l1_entry, l2_entry },
        };
        let reopen = |prefix: &[Bytes]| {
            SyncBlockState::open(
                Arc::new(provider.clone()),
                &config,
                &parent,
                1012,
                Address::ZERO,
                prefix,
            )
        };
        // Seed the accepted block prefix with load before entering the actual
        // inbound drain. No source simulation or target execution is mocked.
        let prefix = vec![signed(&bad_signer, 0, burner, 16_500_000)];
        let mut draft = reopen(&prefix).unwrap();
        let remaining = BUILDER_GAS_LIMIT - draft.gas_used();
        assert!(remaining < 14_500_000);
        // Bypass the comparison only for this fixture check: prove the actual
        // EEZL2 delivery reaches the stated outcomes at both budgets.
        for (gas_limit, success) in [
            (eez_primitives::SYSTEM_TX_GAS_CAP, full_success),
            (remaining, reduced_success),
        ] {
            let raw = delivery()
                .at_position(BUILDER_GAS_LIMIT, BUILDER_GAS_LIMIT - gas_limit)
                .unwrap();
            let tx = reth_primitives_traits::Recovered::new_unchecked(
                eez_evm::EezEvmTx::from(
                    eez_primitives::EezTxEnvelope::decode_2718(&mut raw.as_ref()).unwrap(),
                ),
                eez_primitives::SYSTEM_ADDRESS,
            );
            let mut fork = draft.fork().unwrap();
            let (state, env) = fork.state_and_env();
            assert_eq!(
                state
                    .transact(&config, env, &tx, None)
                    .unwrap()
                    .result
                    .is_success(),
                success,
                "{name}: gas limit {gas_limit}"
            );
        }
        let mut drain = CrossChainDrain {
            rollup_id: 1,
            live_source: eez_protocol::native_source::SourceEntry::new(1, B256::repeat_byte(1), 0)
                .unwrap(),
            l1_state: L1SlotState {
                anchor: parent.clone(),
                cache: Default::default(),
            },
            draft,
            sync_txs: prefix.clone(),
            system_txs_appended: 0,
            survivors: Vec::new(),
            survivor_comps: Vec::new(),
            pending_out: Vec::new(),
            pending_in: Vec::new(),
            outbound_entries: Vec::new(),
            outbound_target_gas: 0,
            escrow_remaining: None,
            poison: Vec::new(),
            poison_gaps: Vec::new(),
            funding_deferred: Vec::new(),
            funding_gaps: Vec::new(),
            budget: PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS, 1),
            end: DrainEnd::Complete,
        };
        let held: Vec<_> = [&bad_signer, &good_signer]
            .into_iter()
            .zip(wrappers)
            .map(|(signer, wrapper)| {
                let raw_tx = signed(signer, 0, wrapper, 1_000_000);
                HeldTx {
                    hash: alloy_primitives::keccak256(&raw_tx),
                    raw_tx,
                    sender: signer.address(),
                    nonce: 0,
                    direction: Direction::Inbound,
                    attempts: 0,
                    max_fee_per_gas: 1,
                    priority_fee_per_gas: 1,
                }
            })
            .collect();
        drain.compose_inbound(
            &cc,
            &SystemTxContext {
                eezl2_address: eez_primitives::EEZL2_ADDRESS,
                l2_chain_id: 1,
                this_rollup_id: 1,
            },
            None,
            reopen,
            held.iter().cloned().enumerate().collect(),
        );
        let expected_writes = if full_success {
            let DrainEnd::Cut {
                cut: BudgetCut::BlockGas(_),
                rest,
            } = &drain.end
            else {
                panic!("{name}: a successful full-cap delivery must remain retryable");
            };
            assert_eq!(
                rest.iter().map(|(_, tx)| tx.hash).collect::<Vec<_>>(),
                held.iter().map(|tx| tx.hash).collect::<Vec<_>>()
            );
            assert!(rest.iter().all(|(_, tx)| tx.attempts == 0));
            assert!(drain.poison.is_empty());
            assert!(drain.survivors.is_empty());
            assert!(drain.pending_in.is_empty());
            assert_eq!(drain.sync_txs, prefix);
            [0u64, 0]
        } else {
            assert!(
                matches!(drain.end, DrainEnd::Complete),
                "{name}: drain must not defer the suffix"
            );
            assert_eq!(
                drain.poison.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
                [held[0].hash],
                "{name}"
            );
            assert_eq!(
                drain
                    .survivors
                    .iter()
                    .map(|(_, tx)| tx.hash)
                    .collect::<Vec<_>>(),
                [held[1].hash],
                "{name}"
            );
            assert_eq!(drain.pending_in.len(), 1, "{name}");
            assert_eq!(drain.system_txs_appended, 1, "{name}");
            assert_eq!(drain.sync_txs.len(), prefix.len() + 1, "{name}");
            [0, 1]
        };
        let mut canonical = reopen(&drain.sync_txs).unwrap();
        assert_eq!(
            drain.draft.gas_used(),
            canonical.gas_used(),
            "{name}: rejected gas cannot leak"
        );
        let (mut source, _) = drain.l1_state.fork_state(&cc.local.l1_entry).unwrap();
        for (index, expected) in expected_writes.into_iter().enumerate() {
            let expected = Word::from(expected);
            assert_eq!(
                source
                    .database_mut()
                    .get_storage(&wrappers[index], &Word::ZERO)
                    .unwrap(),
                expected,
                "{name}: L1 source storage"
            );
            for state in [&mut drain.draft, &mut canonical] {
                let mut fork = state.fork().unwrap();
                assert_eq!(
                    fork.state_and_env()
                        .0
                        .database_mut()
                        .get_storage(&targets[index], &Word::ZERO)
                        .unwrap(),
                    expected,
                    "{name}: L2 target storage"
                );
            }
        }
    }
}

#[test]
fn native_packing_dependent_success_is_refused_without_commit() {
    // GAS can change output, storage, or only gas spent. All three programs
    // succeed at either budget, so status alone misses the change.
    for (code, writes_storage) in [
        (bytes!("5a5f5260205ff3"), false),
        (bytes!("36156009575a5f55005b5f545f5260205ff3"), true),
        (bytes!("5b6207a1205a1160005700"), false),
    ] {
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
