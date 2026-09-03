//! Launcher for the production eez Composer node.
//!
//! Wraps reth with our composer stack: reth provides the EVM, storage,
//! networking, RPC, and engine; we provide block production
//! (Sequencer + Scheduler), L1 anchoring (`L1Watcher` + Deriver), and
//! batch submission (Composer umbrella).
//!
mod bundle_rpc;
pub mod config;
mod ingress;
mod l1_embedded;
mod witness_source;

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_primitives::B256;
use eez_composer::composer::CrossChainWiring;
use eez_composer::{Composer, HeldPool, RollupConfig, RollupState};
use eez_deriver::Deriver;
use eez_driver::{
    BlockCommitterHandle, EthAttributesBuilder, Sequencer, SyncSlotComposerHandle,
    spawn_l1_anchored,
};
use eez_l1::{L1CanonicalHead, L1HeadStream, L1Watcher, Submitter, SubmitterConfig};
use eez_node_common::{
    EezPayloadBuilder, EezPoolBuilder, L2NodeBuilder, checkpoint_dir,
    config::{ConfigArgs, load},
    node_cli, wait_for_l1_ready,
};
use reth_node_builder::components::BasicPayloadServiceBuilder;
use reth_node_ethereum::EthereumNode;
use tokio::sync::mpsc;
use tracing::{Level, event};

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// ~15 min at the capped backoff: outlasts a restarting L1, but a permanently
/// refused RPC call still surfaces as an exit.
const BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES: u32 = 32;
const L2_SYSTEM_TX_GAS_PRICE: u128 = 1_000_000_000;
const L2_SYSTEM_TX_GAS_LIMIT: u64 = 2_000_000;

/// Embedded L1 reth handle — owned by `main` for the node lifetime so
/// the L1 stays alive (drop tears it down). Generic over both variants'
/// `NodeHandle` params because the AddOns type differs between
/// EthereumNode (Devnet/Testing) and GnosisNode (Chiado).
///
/// Downstream matches `.as_ref()` for the chain_spec / provider /
/// evm_config to build the cross-chain composer; the Chiado provider
/// goes through `eez_composer::GnosisL1Adapter` to translate
/// `GnosisHeader → alloy_consensus::Header` on each read.
enum EmbeddedL1<Ethereum, Chiado> {
    Ethereum(Ethereum),
    Chiado(Chiado),
}

/// Launch a production composer node.
pub fn run_composer() -> eyre::Result<()> {
    node_cli::<ConfigArgs>()?.run(launch_composer)
}

#[allow(clippy::too_many_lines)]
async fn launch_composer(builder: L2NodeBuilder, ext: ConfigArgs) -> eyre::Result<()> {
    let config: config::Config = load(&ext.eez_config_path)?;
    config.validate()?;
    let timing = config.timing.build()?;
    let system_signer = config.l2_system_key.signer()?;
    let system_address = system_signer.address();
    let poster_signer = config.submission.poster_key.signer()?;
    let embedded_l1_config = config.embedded_l1.build()?;
    let l2_datadir = builder.config().datadir().data_dir().to_path_buf();
    eyre::ensure!(builder.config().rpc.http, "Composer requires reth HTTP RPC");
    let l2_rpc_url = format!("http://127.0.0.1:{}", builder.config().rpc.http_port);

    let witness_db_path = l2_datadir.join("witnesses");
    let (witness_sender, witness_receiver) = mpsc::unbounded_channel::<B256>();
    let witness_store = witness_source::new_store(&witness_db_path)?;
    let prover: Arc<dyn eez_prover::Prover> = Arc::new(eez_prover_client::RemoteProver::new(
        config.prover.url.to_string(),
        config.prover.attester_address,
    ));
    event!(
        name: "eez.node.witness_store.opened",
        Level::INFO,
        path = %witness_db_path.display(),
        "persistent witness store opened",
    );
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "composer",
        config = %ext.eez_config_path.display(),
        "launching eez composer",
    );
    // Launch the embedded L1 reth first in composer mode — its
    // `StateProviderFactory` backs `LocalChainClient::new_entry` for
    // L1 source-tx simulation. Inline (not in `l1_embedded.rs`)
    // because the `NodeHandle` AddOns type resists a typed return.
    // Shared L1-reth tokio runtime — built once, used by whichever
    // L1 path runs.
    let build_l1_runtime = || {
        reth_tasks::RuntimeBuilder::new(reth_tasks::RuntimeConfig::default().with_tokio(
            reth_tasks::TokioConfig::existing_handle(tokio::runtime::Handle::current()),
        ))
        .build()
        .map_err(|e| eyre::eyre!("L1 embedded RuntimeBuilder: {e}"))
    };
    // Testing = vanilla EthereumNode (5s auto-mine); Devnet = vanilla
    // EthereumNode + external CL; Chiado = reth_gnosis::GnosisNode +
    // external CL, its provider wrapped by `GnosisL1Adapter` for the
    // alloy-Header bound. Returns an `EmbeddedL1` owning the NodeHandle so
    // the L1 outlives the node.
    let embedded_l1: EmbeddedL1<_, _> = {
        let l1_cfg = embedded_l1_config;
        match l1_cfg.kind {
            l1_embedded::L1ChainKind::Devnet => {
                let node_cfg = l1_embedded::build_devnet_node_config(&l1_cfg)?;
                let db = reth_db::init_db(
                    node_cfg.datadir().db(),
                    reth_db::mdbx::DatabaseArguments::default(),
                )
                .map_err(|e| eyre::eyre!("L1 embedded init_db: {e}"))?;
                event!(
                    name: "eez.node.l1_embedded.launching",
                    Level::INFO,
                    kind = "devnet",
                    http_port = l1_cfg.http_port,
                    auth_port = l1_cfg.auth_port,
                    "launching embedded L1 reth (devnet); external consensus client must connect to authrpc",
                );
                let l1_handle = reth_node_builder::NodeBuilder::new(node_cfg)
                    .with_database(db)
                    .with_launch_context(build_l1_runtime()?)
                    .node(EthereumNode::default())
                    .launch_with_debug_capabilities()
                    .await?;
                event!(
                    name: "eez.node.l1_embedded.ready",
                    Level::INFO,
                    kind = "devnet",
                    l1_chain_id = %l1_handle.node.chain_spec().chain(),
                    "embedded L1 reth (devnet) ready",
                );
                EmbeddedL1::Ethereum(l1_handle)
            }
            l1_embedded::L1ChainKind::Testing => {
                let node_cfg = l1_embedded::build_testing_node_config(&l1_cfg)?;
                let db = reth_db::init_db(
                    node_cfg.datadir().db(),
                    reth_db::mdbx::DatabaseArguments::default(),
                )
                .map_err(|e| eyre::eyre!("L1 embedded init_db: {e}"))?;
                event!(
                    name: "eez.node.l1_embedded.launching",
                    Level::INFO,
                    kind = "testing",
                    http_port = l1_cfg.http_port,
                    "launching embedded L1 reth (testing)",
                );
                let l1_handle = reth_node_builder::NodeBuilder::new(node_cfg)
                    .with_database(db)
                    .with_launch_context(build_l1_runtime()?)
                    .node(EthereumNode::default())
                    .extend_rpc_modules(bundle_rpc::install_dev_bundle_rpc)
                    .launch_with_debug_capabilities()
                    .await?;
                event!(
                    name: "eez.node.l1_embedded.ready",
                    Level::INFO,
                    kind = "testing",
                    l1_chain_id = %l1_handle.node.chain_spec().chain(),
                    "embedded L1 reth (testing) ready",
                );
                EmbeddedL1::Ethereum(l1_handle)
            }
            l1_embedded::L1ChainKind::Chiado => {
                let node_cfg = l1_embedded::build_chiado_node_config(&l1_cfg)?;
                let db = reth_db::init_db(
                    node_cfg.datadir().db(),
                    reth_db::mdbx::DatabaseArguments::default(),
                )
                .map_err(|e| eyre::eyre!("L1 chiado init_db: {e}"))?;
                event!(
                    name: "eez.node.l1_embedded.launching",
                    Level::INFO,
                    kind = "chiado",
                    http_port = l1_cfg.http_port,
                    auth_port = l1_cfg.auth_port,
                    datadir = ?l1_cfg.datadir,
                    "launching embedded L1 reth (chiado via reth_gnosis::GnosisNode); \
                     external lighthouse CL must connect to authrpc",
                );
                let chiado_handle = reth_node_builder::NodeBuilder::new(node_cfg)
                    .with_database(db)
                    .with_launch_context(build_l1_runtime()?)
                    .node(reth_gnosis::GnosisNode::default())
                    .launch_with_debug_capabilities()
                    .await?;
                event!(
                    name: "eez.node.l1_embedded.ready",
                    Level::INFO,
                    kind = "chiado",
                    l1_chain_id = %chiado_handle.node.chain_spec().inner.chain(),
                    "embedded L1 reth (chiado) ready — cross-chain composer \
                     wraps the provider via eez_composer::GnosisL1Adapter",
                );
                EmbeddedL1::Chiado(chiado_handle)
            }
        }
    };

    // L2 reth. `EezPayloadBuilder` writes `gas_limit`/`extra_data` from
    // shared `eez-driver` constants so deriver replay and sequencer builds
    // yield identical headers.
    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                // Reorged-out system transactions must not leak from reth's
                // reinjection path into an ordinary Live block.
                .pool(EezPoolBuilder::new(system_address))
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder::default())),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let l2_genesis_timestamp = chain_spec.genesis().timestamp;
    let provider = handle.node.provider.clone();
    let payload_builder_handle = handle.node.payload_builder_handle.clone();
    let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
    let task_executor = handle.node.task_executor.clone();

    // Shared L1-confirmed L2 head. Created unconditionally so the
    // Sequencer's speculative-depth limit can be wired even when
    // the L1 stack isn't activated. Deriver is the sole writer
    // (only in follower/composer modes).
    let l1_head = Arc::new(L1CanonicalHead::default());

    let attributes = EthAttributesBuilder::new(chain_spec.clone());

    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        beacon_engine_handle,
        payload_builder_handle,
        Some(witness_sender),
    )?;

    let submitter_config = SubmitterConfig {
        reader: config.l1.reader(),
        builder_rpc_url: config.submission.builder_rpc_url.clone(),
        target_rpc_url: config.submission.target_rpc_url.clone(),
        poster: poster_signer.clone(),
    };
    let deploy_block = submitter_config.reader.deploy_block;
    let rollup_config = RollupConfig {
        rollup_id: config.l1.rollup_id,
        expect_external_batches: config.expect_external_batches,
    };

    let submitter = Submitter::new(submitter_config);
    // Handle only — polling starts after boot catch-up fixes the
    // seed and every consumer has subscribed.
    let l1_watcher = L1Watcher::new(config.l1.watcher());

    // Build the Composer and Sequencer around the same binary-owned
    // BlockCommitter handle. The Deriver receives another clone below, so all
    // engine traffic shares one actor, reconcile lock, and head mirror.
    let (sequencer, composer, held_pool, system_tx_cfg, l1_source_chain_id) = {
        let rollup_id = rollup_config.rollup_id;
        let l1_variant = &embedded_l1;
        let l1_source_chain_id = match l1_variant {
            EmbeddedL1::Ethereum(l1_handle) => l1_handle.node.chain_spec().chain().id(),
            EmbeddedL1::Chiado(chiado_handle) => chiado_handle.node.chain_spec().inner.chain().id(),
        };
        // Share the SAME HeldPool the ingress middleware pushes into.
        let held_pool = Arc::new(HeldPool::new());
        let rollup_state = RollupState {
            config: rollup_config.clone(),
            l2_provider: Arc::new(provider.clone()),
            l1_head: Arc::clone(&l1_head),
            held_pool: Arc::clone(&held_pool),
            optimistic: Arc::new(eez_composer::OptimisticallyIncluded::new()),
        };
        let mut rollups = HashMap::with_capacity(1);
        rollups.insert(rollup_id, rollup_state);
        // EVM config feeds the manual Sync-block construction path
        // (build_sync_block → reth-evm BlockBuilder). Same chain spec
        // the engine uses, so blocks produced here pass validation
        // when reth ingests them via newPayload.
        let evm_config = reth_evm_ethereum::EthEvmConfig::new(chain_spec.clone());

        // Build the cross-chain composer over the mandatory embedded L1.
        // Inlined because the `FullNode` AddOns type resists a typed helper
        // return.
        use eez_composer::{GnosisL1Adapter, LocalChainClient};
        use eez_protocol::rollup_id::RollupId;
        use eez_protocol::{ProxyLookupConfig, TargetConfig};

        let eez_registry = config.l1.registry_address;
        let eezl2_address = eez_protocol::EEZL2_ADDRESS;
        let l1_rollup_id = RollupId(0);
        let l2_rollup_id_typed = RollupId(rollup_id);

        // L1 entry differs per kind: Devnet/Testing use the native
        // provider + EvmConfig; Chiado wraps it in
        // `GnosisL1Adapter` and builds a fresh `EthEvmConfig`
        // over the chiado ChainSpec (source-sim needs only
        // revm, not GnosisNode's AuRa paths). Both yield the
        // same concrete client type, so composition is identical.
        let l1_entry_client: Arc<LocalChainClient> = match l1_variant {
            EmbeddedL1::Ethereum(l1_handle) => {
                let l1_provider = l1_handle.node.provider.clone();
                let l1_evm_config = l1_handle.node.evm_config.clone();
                LocalChainClient::new_entry(
                    l1_provider,
                    l1_evm_config,
                    l1_rollup_id,
                    eez_registry,
                    eez_protocol::ChainDialect::EvmL1Style,
                )
            }
            EmbeddedL1::Chiado(chiado_handle) => {
                // `GnosisChainSpec.inner` is the standard
                // reth `ChainSpec` (via `#[deref]`); wrap it
                // fresh as `Arc<ChainSpec>` for the
                // standard `EthEvmConfig` simulation envs.
                let gnosis_chain_spec = chiado_handle.node.chain_spec();
                let l1_chain_spec: Arc<reth_chainspec::ChainSpec> =
                    Arc::new(gnosis_chain_spec.inner.clone());
                let l1_provider = GnosisL1Adapter::new(chiado_handle.node.provider.clone());
                let l1_evm_config =
                    reth_evm_ethereum::EthEvmConfig::new(Arc::clone(&l1_chain_spec));
                LocalChainClient::new_entry(
                    l1_provider,
                    l1_evm_config,
                    l1_rollup_id,
                    eez_registry,
                    eez_protocol::ChainDialect::EvmL1Style,
                )
            }
        };
        let entry_client_view: Arc<dyn eez_protocol::executor::ChainClient + Send + Sync> =
            l1_entry_client.clone();

        // L2 follower — EvmL2Style. Its dispatch contract is the
        // `EEZL2` predeploy, whose inherited `authorizedProxies`
        // mapping occupies slot 0.
        let l2_follower = LocalChainClient::new_follower(
            provider.clone(),
            evm_config.clone(),
            l2_rollup_id_typed,
            eezl2_address,
            eez_protocol::ChainDialect::EvmL2Style,
        );
        let l2_follower_view: std::sync::Arc<
            dyn eez_protocol::executor::ChainClient + Send + Sync,
        > = l2_follower;

        // L2 ENTRY client (follower's provider/dialect, but
        // Role::Entry) — the follower client errors `Unavailable` for
        // the outbound source-sim `simulate_and_resolve_recorded_for`.
        let l2_entry_client = LocalChainClient::new_entry(
            provider.clone(),
            evm_config.clone(),
            l2_rollup_id_typed,
            eezl2_address,
            eez_protocol::ChainDialect::EvmL2Style,
        );

        let entry_cfg = TargetConfig {
            proxy_lookup: ProxyLookupConfig {
                contract_address: eez_registry,
                authorized_proxies_slot: eez_protocol::ChainDialect::EvmL1Style.proxy_lookup_slot(),
            },
            dialect: eez_protocol::ChainDialect::EvmL1Style,
        };
        let l2_follower_cfg = TargetConfig {
            proxy_lookup: ProxyLookupConfig {
                contract_address: eezl2_address,
                authorized_proxies_slot: eez_protocol::ChainDialect::EvmL2Style.proxy_lookup_slot(),
            },
            dialect: eez_protocol::ChainDialect::EvmL2Style,
        };

        let mut wired_rollups = std::collections::HashMap::new();
        wired_rollups.insert(l1_rollup_id, (entry_client_view, entry_cfg));
        // A colliding id would silently overwrite the fixed L1 source entry.
        if wired_rollups
            .insert(l2_rollup_id_typed, (l2_follower_view, l2_follower_cfg))
            .is_some()
        {
            return Err(eyre::eyre!(
                "duplicate rollup id {l2_rollup_id_typed}: L2 rollup id collides \
                     with the fixed L1 source rollup id; the L2 follower \
                     registration would overwrite the L1 entry"
            ));
        }
        // CrossChainExecCtx: signer + L2 addresses needed to wrap
        // EvmComposer's `(load_table, execute)` calldata pairs
        // into signed legacy L2 system txs at Sync-slot time.
        // Constructed only when EvmComposer is constructed —
        // both are tied to embedded L1 mode.
        // Submission RPC for postBatch and inbound source-chain reads.
        // This can differ from the embedded L1 used for local source
        // simulation (the E2E harness deliberately uses Anvil here), so use
        // the chain ID paired with this provider in the L1 config.
        let l1_rpc_url = config.l1.rpc_url.clone();
        let l1_provider = alloy_provider::RootProvider::new_http(l1_rpc_url.clone());
        // One submission path everywhere: the composer always
        // routes `[postBatch, user_tx_1, …]` through the shared
        // Submitter. The Submitter owns the transport decision —
        // `eth_sendBundle` on relays that support it (rbuilder),
        // ordered mempool submission on plain execution RPCs
        // (dev reth, anvil) detected via JSON-RPC -32601.
        let exec_ctx = Arc::new(eez_composer::CrossChainExecCtx {
            system_signer,
            eezl2_address,
            eez_registry_address: eez_registry,
            l2_chain_id: chain_spec.chain().id(),
            l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
            l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
            l1_provider,
            submitter: submitter.clone(),
            l1_poster_signer: poster_signer,
            l1_chain_id: config.l1.chain_id,
            l1_post_batch_priority_fee: config.submission.priority_fee,
            ecdsa_proof_system_address: config.submission.proof_system_address,
        });
        event!(
            name: "eez.node.evm_composer.ready",
            Level::INFO,
            l1_rollup_id = l1_rollup_id.0,
            l2_rollup_id = rollup_id,
            eez_registry = %eez_registry,
            %eezl2_address,
            "cross-chain composer constructed (L1 entry + L2 follower)",
        );
        let cross_chain = CrossChainWiring {
            entry_rollup_id: l1_rollup_id,
            rollups: wired_rollups,
            exec_ctx,
            local: eez_composer::LocalComposeClients {
                l1_entry: l1_entry_client,
                l2_entry: l2_entry_client,
            },
        };

        // Project the Arc<CrossChainExecCtx> into a SystemTxContext
        // BEFORE moving it into the Composer. The Deriver picks
        // this up further down to STF-reconstruct the same L2
        // system txs the composer produced.
        let deriver_system_tx_cfg = {
            let ctx = &cross_chain.exec_ctx;
            eez_protocol::system_tx::SystemTxContext {
                system_signer: ctx.system_signer.clone(),
                eezl2_address: ctx.eezl2_address,
                l2_chain_id: ctx.l2_chain_id,
                l2_gas_price: ctx.l2_gas_price,
                l2_gas_limit: ctx.l2_gas_limit,
                this_rollup_id: rollup_id,
            }
        };
        // Remote-prover mode: spawn the commit-time witness capture and back the
        // composer's witness source with its store. Capturing at commit (parent
        // state still fresh) is why this works on a non-archival node. Spawned
        // before `evm_config` is moved into `Composer::new` below.
        let cap_provider = provider.clone();
        let cap_evm = evm_config.clone();
        let ws_provider = provider.clone();
        let ws_evm = evm_config.clone();
        let cap_store = Arc::clone(&witness_store);
        // Purge floor = L1-FINALIZED height (a reorg could un-settle a posted batch).
        let cap_l1_head = Arc::clone(&l1_head);
        task_executor.spawn_critical_task("eez-witness-capture", async move {
            witness_source::run_capture(
                witness_receiver,
                cap_store,
                cap_provider,
                cap_evm,
                move || cap_l1_head.finalized_l2(),
            )
            .await;
        });
        // Hybrid: read the store, else re-exec on demand the newest block
        // the async capture hasn't drained yet (state still retained; fast
        // for near-empty blocks).
        let witness_source = Some(Arc::new(witness_source::NodeWitnessSource::new(
            witness_store,
            ws_provider,
            ws_evm,
        )) as Arc<dyn eez_prover::ProvingWitnessSource>);
        let composer = Composer::new(
            rollups,
            prover,
            evm_config,
            cross_chain,
            block_committer.clone(),
            witness_source,
            timing,
            config.limits.into(),
        )?;
        let sync_slot_handle: SyncSlotComposerHandle = Arc::new(composer.clone());

        let schedule_rx = spawn_l1_anchored(
            L1HeadStream::from_watcher(&l1_watcher),
            timing,
            l2_genesis_timestamp,
        );
        let sequencer = Sequencer::composer(
            attributes,
            block_committer.clone(),
            schedule_rx,
            timing,
            rollup_id,
            sync_slot_handle,
            config.max_speculative_depth,
            Arc::clone(&l1_head),
        );
        (
            sequencer,
            composer,
            held_pool,
            deriver_system_tx_cfg,
            l1_source_chain_id,
        )
    };

    let l2_source_chain_id = chain_spec.chain().id();
    // Resolve both required fronts now. Their upstream chain-id checks run
    // after the L1 readiness gate so the ports can bind while L1 catches up.
    let mut xchain_fronts = Vec::new();
    let front_configs = [
        (
            XchainFrontSpec {
                direction: eez_composer::Direction::Inbound,
                task: "eez-l1-xchain-front",
                expected_source_chain_id: l1_source_chain_id,
            },
            config.cross_chain.l1_port,
            config.l1.rpc_url.to_string(),
        ),
        (
            XchainFrontSpec {
                direction: eez_composer::Direction::Outbound,
                task: "eez-l2-xchain-front",
                expected_source_chain_id: l2_source_chain_id,
            },
            config.cross_chain.l2_port,
            l2_rpc_url,
        ),
    ];
    for (spec, port, url) in front_configs {
        let validation_provider = alloy_provider::RootProvider::new_http(url.parse()?);
        xchain_fronts.push((spec, port, url, validation_provider));
    }

    // Deriver: drives BlockCommitter from L1Events (follower +
    // composer). A wired `SystemTxContext` makes it reconstruct the
    // same L2 system txs the composer produced (single-source STF).
    let l2_block_time_secs = timing.l2_block_time().as_secs();
    let deriver = Deriver::new(
        block_committer.clone(),
        Arc::new(provider.clone()),
        submitter.reader(),
        chain_spec,
        l2_block_time_secs,
        deploy_block,
        Arc::clone(&l1_head),
        Some(system_tx_cfg),
        checkpoint_dir(&l2_datadir),
    );

    // Bind before the L1 wait so port checks see a live front. Submissions are
    // refused until catch-up completes and the watcher owns subsequent blocks.
    let xchain_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let xchain_checks: Vec<_> = xchain_fronts
        .iter()
        .map(|(spec, _, _, provider)| (spec.expected_source_chain_id, provider.clone()))
        .collect();
    for (spec, port, url, validation_provider) in xchain_fronts {
        let pool = Arc::clone(&held_pool);
        let ready = Arc::clone(&xchain_ready);
        task_executor.spawn_critical_task(spec.task, async move {
            ingress::run_cross_chain_front(
                port,
                url,
                spec.direction,
                pool,
                validation_provider,
                spec.expected_source_chain_id,
                ready,
            )
            .await
            .unwrap_or_else(|e| panic!("configured cross-chain front exited: {e:#}"));
        });
    }

    let l1_reader = submitter.reader();
    wait_for_l1_ready(&l1_reader, deploy_block, l1_source_chain_id).await?;
    for (expected_chain_id, provider) in &xchain_checks {
        ingress::validate_cross_chain_front(provider, *expected_chain_id).await?;
    }

    let mut catch_up_retry_delay = BOOT_CATCH_UP_INITIAL_RETRY_DELAY;
    let mut catch_up_attempts = 0_u64;
    let mut transport_failures = 0_u32;
    let (l1_seed_number, l1_seed_hash) = loop {
        match deriver.catch_up_with_seed().await {
            Ok(seed) => break seed,
            Err(err)
                if err.is_l1_transport() && {
                    transport_failures += 1;
                    transport_failures >= BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES
                } =>
            {
                event!(
                    name: "eez.node.deriver.boot_catch_up.transport_exhausted",
                    Level::ERROR,
                    mode = "composer",
                    transport_failures,
                    error = %err,
                    "L1 transport kept failing during boot catch-up; the endpoint is likely refusing a call we need, not merely unreachable",
                );
                return Err(eyre::eyre!(
                    "boot-time deriver catch_up gave up after {transport_failures} L1 transport failures: {err}"
                ));
            }
            Err(err) if err.is_source_incomplete() || err.is_l1_transport() => {
                catch_up_attempts += 1;
                event!(
                    name: "eez.node.deriver.boot_catch_up.source_incomplete",
                    Level::WARN,
                    mode = "composer",
                    attempts = catch_up_attempts,
                    retry_delay_secs = catch_up_retry_delay.as_secs(),
                    error = %err,
                    "boot-time catch_up could not read all L1 source data yet; retrying before starting L1-active tasks",
                );
                tokio::time::sleep(catch_up_retry_delay).await;
                catch_up_retry_delay = Duration::from_secs(
                    catch_up_retry_delay
                        .as_secs()
                        .saturating_mul(2)
                        .min(BOOT_CATCH_UP_MAX_RETRY_DELAY.as_secs()),
                );
            }
            Err(err) => {
                event!(
                    name: "eez.node.deriver.boot_catch_up.failed",
                    Level::ERROR,
                    mode = "composer",
                    error = %err,
                    "boot-time catch_up failed; refusing to start L1-active tasks before reconciliation",
                );
                return Err(eyre::eyre!("boot-time deriver catch_up failed: {err}"));
            }
        }
    };
    event!(
        name: "eez.node.deriver.spawned",
        Level::INFO,
        mode = "composer",
        initial_posted_through = deriver.cursor(),
        "spawning eez deriver",
    );
    let deriver_run = deriver.clone();
    let deriver_events = l1_watcher.subscribe();
    task_executor.spawn_critical_task("eez-deriver", async move {
        deriver_run.run(deriver_events).await;
    });

    event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "composer", "spawning eez sequencer (L1-anchored)");
    task_executor.spawn_critical_task("eez-sequencer", async move {
        sequencer.run().await;
    });

    event!(name: "eez.node.composer.spawned", Level::INFO, "spawning eez composer umbrella");
    let composer_events = l1_watcher.subscribe();
    task_executor.spawn_critical_task("eez-composer", async move {
        composer.run(composer_events).await;
    });

    // Start polling last: every consumer is subscribed and catch-up
    // covered [deploy_block, seed], so the watcher owns all blocks
    // strictly after its seed — no startup scan gap.
    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );
    xchain_ready.store(true, std::sync::atomic::Ordering::Relaxed);

    handle.wait_for_node_exit().await
}

#[derive(Debug, Clone, Copy)]
struct XchainFrontSpec {
    direction: eez_composer::Direction,
    task: &'static str,
    expected_source_chain_id: u64,
}
