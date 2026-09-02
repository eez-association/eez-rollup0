//! Shared launcher for the explicit eez Rollup-0 node-role binaries.
//!
//! Wraps reth with our composer stack: reth provides the EVM, storage,
//! networking, RPC, and engine; we provide block production
//! (Sequencer + Scheduler), L1 anchoring (`L1Watcher` + Deriver), and
//! batch submission (Composer umbrella).
//!
//! # Roles
//!
//! Role selection belongs to the executable, not to incidental environment
//! variable presence:
//!
//! - `eez-composer`: L1-anchored sequencing, proving, posting, and cross-chain ingress.
//! - `eez-follower`: L1-derived replay plus verified signed unsafe blocks over P2P.
//! - `eez-dev-node`: unanchored interval sequencing for local development.

mod bundle_rpc;
mod follower;
mod ingress;
mod l1_embedded;
mod witness_source;

use std::{collections::HashMap, env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_provider::Provider as _;
use alloy_signer_local::PrivateKeySigner;
use clap::Parser as _;
use eez_composer::composer::CrossChainWiring;
use eez_composer::{Composer, HeldPool, RollupConfig, RollupState};
use eez_deriver::Deriver;
use eez_driver::{
    BlockCommitterHandle, DEFAULT_MAX_SPECULATIVE_DEPTH, EthAttributesBuilder, RollupTiming,
    Sequencer, SyncSlotComposerHandle, spawn_interval, spawn_l1_anchored,
};
use eez_l1::{
    L1CanonicalHead, L1HeadStream, L1Reader, L1ReaderConfig, L1Watcher, L1WatcherConfig, Submitter,
    SubmitterConfig,
};
use eez_p2p::{BACKFILL_CACHE_SIZE, NetworkConfig, NetworkService, sign_payload};
use eez_prover::MockEcdsaProver;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_node_builder::{NodeBuilder, WithLaunchContext, components::BasicPayloadServiceBuilder};
use reth_node_ethereum::EthereumNode;
use reth_payload_primitives::PayloadTypes;
use reth_storage_api::{BlockIdReader, BlockReader, TransactionVariant};
use tokio::sync::mpsc;
use tracing::{Level, event};

use follower::UnsafeHeadFollower;

mod payload;
mod pool;
use payload::EezPayloadBuilder;
use pool::EezPoolBuilder;

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// ~15 min at the capped backoff: outlasts a restarting L1, but a permanently
/// refused RPC call still surfaces as an exit.
const BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES: u32 = 32;
const L1_CHAIN_ID_READ_TIMEOUT: Duration = Duration::from_secs(30);
const L2_SYSTEM_TX_GAS_PRICE: u128 = 1_000_000_000;
const L2_SYSTEM_TX_GAS_LIMIT: u64 = 2_000_000;

/// Witness-capture resources selected by the mandatory composer prover.
enum WitnessCapture {
    NotRequired,
    Remote {
        sender: mpsc::UnboundedSender<B256>,
        receiver: mpsc::UnboundedReceiver<B256>,
        store: witness_source::WitnessStore,
    },
}

struct ComposerProving {
    prover: Arc<dyn eez_prover::Prover>,
    witness_capture: WitnessCapture,
}

impl WitnessCapture {
    fn sender(&self) -> Option<mpsc::UnboundedSender<B256>> {
        match self {
            Self::NotRequired => None,
            Self::Remote { sender, .. } => Some(sender.clone()),
        }
    }
}

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

/// Composer and development nodes have no role-specific CLI arguments.
#[derive(clap::Args, Debug, Clone)]
struct NoRoleArgs {}

/// Chain-scoped signed-block P2P transport settings.
#[derive(clap::Args, Debug, Clone)]
struct P2pArgs {
    /// libp2p TCP listen multiaddr.
    #[arg(
        long,
        env = "EEZ_P2P_LISTEN_ADDR",
        default_value = "/ip4/0.0.0.0/tcp/9300"
    )]
    p2p_listen_addr: String,

    /// Comma-separated static libp2p peer multiaddrs.
    #[arg(long, env = "EEZ_P2P_PEERS", value_delimiter = ',')]
    p2p_peers: Vec<String>,
}

impl P2pArgs {
    fn network_config(&self, chain_id: u64) -> eyre::Result<NetworkConfig> {
        NetworkConfig::parse(
            chain_id,
            &self.p2p_listen_addr,
            self.p2p_peers.iter().map(String::as_str),
        )
        .map_err(Into::into)
    }
}

/// Composer-specific CLI arguments.
#[derive(clap::Args, Debug, Clone)]
struct ComposerArgs {
    #[command(flatten)]
    p2p: P2pArgs,
}

/// Follower-specific CLI arguments layered on top of reth's CLI.
#[derive(clap::Args, Debug, Clone)]
struct FollowerArgs {
    #[command(flatten)]
    p2p: P2pArgs,

    /// Address authorized to sign this rollup's unsafe blocks.
    #[arg(long, env = "EEZ_UNSAFE_BLOCK_SIGNER_ADDRESS")]
    unsafe_block_signer_address: Address,
}

/// Launch a production composer node.
pub fn run_composer() -> eyre::Result<()> {
    node_cli::<ComposerArgs>()?.run(launch_composer)
}

/// Launch an L1-derived follower node.
pub fn run_follower() -> eyre::Result<()> {
    node_cli::<FollowerArgs>()?.run(launch_follower)
}

/// Launch an unanchored local-development sequencer.
pub fn run_dev_node() -> eyre::Result<()> {
    node_cli::<NoRoleArgs>()?.run(launch_dev_node)
}

type L2NodeBuilder =
    WithLaunchContext<NodeBuilder<reth_db::DatabaseEnv, reth_chainspec::ChainSpec>>;

fn node_cli<Ext>() -> eyre::Result<Cli<EthereumChainSpecParser, Ext>>
where
    Ext: clap::Args + std::fmt::Debug,
{
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("deployments.env");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    // Engine API FCUs may intentionally unwind the L2 head during recovery.
    let mut argv: Vec<String> = std::env::args().collect();
    for flag in [
        "--engine.always-process-payload-attributes-on-canonical-head",
        "--engine.allow-unwind-canonical-header",
    ] {
        if !argv.iter().any(|a| a == flag) {
            argv.push(flag.to_owned());
        }
    }

    Ok(Cli::<EthereumChainSpecParser, Ext>::try_parse_from(argv)?)
}

fn composer_proving_from_env() -> eyre::Result<ComposerProving> {
    match env::var("EEZ_PROVER_URL") {
        Ok(url) => {
            if url.trim().is_empty() {
                return Err(eyre::eyre!("EEZ_PROVER_URL must not be empty"));
            }
            let attester = env::var("EEZ_ATTESTER_ADDRESS")
                .map_err(|_| eyre::eyre!("EEZ_ATTESTER_ADDRESS required in remote-prover mode"))?;
            let attester = Address::from_str(attester.trim())
                .map_err(|e| eyre::eyre!("EEZ_ATTESTER_ADDRESS: {e}"))?;
            let (sender, receiver) = mpsc::unbounded_channel::<B256>();
            let witness_db_path =
                env::var("EEZ_WITNESS_DB_PATH").unwrap_or_else(|_| "eez-witnesses".to_owned());
            let store = witness_source::new_store(std::path::Path::new(&witness_db_path))?;
            event!(
                name: "eez.node.witness_store.opened",
                Level::INFO,
                path = %witness_db_path,
                "persistent witness store opened",
            );
            Ok(ComposerProving {
                prover: Arc::new(eez_prover_client::RemoteProver::new(url, attester)),
                witness_capture: WitnessCapture::Remote {
                    sender,
                    receiver,
                    store,
                },
            })
        }
        Err(env::VarError::NotPresent) => {
            let key = env::var("EEZ_PROOF_SIGNER_KEY")
                .map_err(|_| eyre::eyre!("EEZ_PROOF_SIGNER_KEY required in local-prover mode"))?;
            let signer =
                PrivateKeySigner::from_bytes(&B256::from_str(key.trim_start_matches("0x"))?)?;
            Ok(ComposerProving {
                prover: Arc::new(MockEcdsaProver::new(signer)),
                witness_capture: WitnessCapture::NotRequired,
            })
        }
        Err(env::VarError::NotUnicode(_)) => {
            Err(eyre::eyre!("EEZ_PROVER_URL contains non-UTF-8 bytes"))
        }
    }
}

async fn launch_dev_node(builder: L2NodeBuilder, _ext: NoRoleArgs) -> eyre::Result<()> {
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "standalone",
        "launching eez development node",
    );
    warn_on_deprecated_env();
    // Standalone produces no system txs, so the gate is inert without a
    // configured address — no warning, nothing to leak.
    let l2_system_address = read_l2_system_address()?;

    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                // Keeps SYSTEM_ADDRESS txs out of the pool: a reorged-out
                // Sync block must not leak system txs into a Live block.
                .pool(EezPoolBuilder::new(l2_system_address))
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder)),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let timing = if env::var_os("EEZ_L2_BLOCK_TIME_MS").is_some() {
        let timing = RollupTiming::from_env()?;
        event!(
            name: "eez.node.timing.standalone_configured",
            Level::INFO,
            l2_block_time_ms = timing.l2_block_time().as_millis(),
            "standalone mode — using configured RollupTiming",
        );
        timing
    } else {
        event!(
            name: "eez.node.timing.standalone_default",
            Level::INFO,
            "standalone mode — using default RollupTiming (L2=2s); set EEZ_*_TIME_MS to override",
        );
        RollupTiming::standalone_default()
    };
    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &handle.node.provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;
    let sequencer = Sequencer::standalone(
        EthAttributesBuilder::new(chain_spec),
        block_committer,
        spawn_interval(timing.l2_block_time()),
        timing,
    );
    event!(
        name: "eez.node.sequencer.spawned",
        Level::INFO,
        mode = "standalone",
        "spawning eez sequencer",
    );
    handle
        .node
        .task_executor
        .spawn_critical_task("eez-sequencer", async move {
            sequencer.run().await;
        });

    handle.wait_for_node_exit().await
}

async fn launch_follower(builder: L2NodeBuilder, ext: FollowerArgs) -> eyre::Result<()> {
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "follower",
        "launching eez follower",
    );
    warn_on_deprecated_env();
    let l2_system_address = read_l2_system_address()?;
    if l2_system_address.is_none() {
        event!(
            name: "eez.node.pool.system_gate_disabled",
            Level::WARN,
            "no EEZ_L2_SYSTEM_ADDRESS / EEZ_L2_SYSTEM_KEY — the L2 pool accepts any \
             sender; only safe for a pure-user-tx follower",
        );
    }

    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                // Keeps SYSTEM_ADDRESS txs out of the pool: a reorged-out
                // Sync block must not leak system txs into a Live block.
                .pool(EezPoolBuilder::new(l2_system_address))
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder)),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let l2_chain_id = chain_spec.chain().id();
    let provider = handle.node.provider.clone();
    let task_executor = handle.node.task_executor.clone();
    let timing = RollupTiming::from_env()?;
    let l1_head = Arc::new(L1CanonicalHead::default());

    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;

    let l1_reader = L1Reader::new(L1ReaderConfig::from_env()?);
    let rollup_config = RollupConfig::from_env()?;
    let l1_watcher = L1Watcher::new(L1WatcherConfig::from_env()?);
    let system_tx_cfg = build_follower_system_tx_cfg(&chain_spec)?;
    event!(
        name: "eez.node.follower.system_tx_cfg",
        Level::INFO,
        enabled = system_tx_cfg.is_some(),
        "cross-chain system tx reconstruction config loaded",
    );

    let deriver = Deriver::new(
        block_committer.clone(),
        Arc::new(provider.clone()),
        l1_reader.clone(),
        chain_spec,
        timing.l2_block_time().as_secs(),
        rollup_config.deploy_block,
        Arc::clone(&l1_head),
        system_tx_cfg,
        read_checkpoint_dir(),
    );

    wait_for_l1_ready(&l1_reader, rollup_config.deploy_block, read_l1_chain_id()?).await?;

    let mut retry_delay = BOOT_CATCH_UP_INITIAL_RETRY_DELAY;
    let mut attempts = 0_u64;
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
                    mode = "follower",
                    transport_failures,
                    error = %err,
                    "L1 transport kept failing during boot catch-up; the endpoint is likely refusing a call we need, not merely unreachable",
                );
                return Err(eyre::eyre!(
                    "boot-time deriver catch_up gave up after {transport_failures} L1 transport failures: {err}"
                ));
            }
            Err(err) if err.is_source_incomplete() || err.is_l1_transport() => {
                attempts += 1;
                event!(
                    name: "eez.node.deriver.boot_catch_up.source_incomplete",
                    Level::WARN,
                    mode = "follower",
                    attempts,
                    retry_delay_secs = retry_delay.as_secs(),
                    error = %err,
                    "boot-time catch_up could not read all L1 source data yet; retrying before starting L1-active tasks",
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = Duration::from_secs(
                    retry_delay
                        .as_secs()
                        .saturating_mul(2)
                        .min(BOOT_CATCH_UP_MAX_RETRY_DELAY.as_secs()),
                );
            }
            Err(err) => {
                event!(
                    name: "eez.node.deriver.boot_catch_up.failed",
                    Level::ERROR,
                    mode = "follower",
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
        mode = "follower",
        initial_posted_through = deriver.cursor(),
        "spawning eez deriver",
    );
    let deriver_events = l1_watcher.subscribe();
    task_executor.spawn_critical_task("eez-deriver", async move {
        deriver.run(deriver_events).await;
    });

    let (p2p_service, p2p_handle, p2p_events) =
        NetworkService::new(ext.p2p.network_config(l2_chain_id)?)?;
    task_executor.spawn_critical_task("eez-unsafe-block-p2p", p2p_service.run());
    let follower = UnsafeHeadFollower::new(
        block_committer,
        provider,
        l2_chain_id,
        ext.unsafe_block_signer_address,
        p2p_events,
        p2p_handle,
    );
    event!(
        name: "eez.node.follower.p2p.spawned",
        Level::INFO,
        signer = %ext.unsafe_block_signer_address,
        "spawning signed P2P unsafe-block follower",
    );
    task_executor.spawn_critical_task("eez-node-follower-unsafe-head", async move {
        follower.run().await;
    });

    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );

    handle.wait_for_node_exit().await
}

#[allow(clippy::too_many_lines)]
async fn launch_composer(builder: L2NodeBuilder, ext: ComposerArgs) -> eyre::Result<()> {
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "composer",
        "launching eez composer",
    );

    warn_on_deprecated_env();
    // Fail before launching either reth if no usable prover is configured.
    let ComposerProving {
        prover,
        witness_capture,
    } = composer_proving_from_env()?;
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
        let l1_cfg = build_embedded_l1_config()?;
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

    // A composer signs system txs, so the pool MUST know which sender to
    // refuse — without it a reorged-out Sync block's system txs could be
    // re-mined into a Live block.
    let l2_system_address = Some(read_l2_system_address()?.ok_or_else(|| {
        eyre::eyre!(
            "EEZ_L2_SYSTEM_ADDRESS (or EEZ_L2_SYSTEM_KEY) is required in composer mode: the \
             L2 pool must refuse SYSTEM_ADDRESS transactions"
        )
    })?);

    // L2 reth. `EezPayloadBuilder` writes `gas_limit`/`extra_data` from
    // shared `eez-driver` constants so deriver replay and sequencer builds
    // yield identical headers.
    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                // Keeps SYSTEM_ADDRESS txs out of the pool: a reorged-out
                // Sync block must not leak system txs into a Live block.
                .pool(EezPoolBuilder::new(l2_system_address))
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder)),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let l2_genesis_timestamp = chain_spec.genesis().timestamp;
    let l2_chain_id = chain_spec.chain().id();
    let provider = handle.node.provider.clone();
    let payload_builder_handle = handle.node.payload_builder_handle.clone();
    let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
    let task_executor = handle.node.task_executor.clone();

    // Shared L1-confirmed L2 head. Created unconditionally so the
    // Sequencer's speculative-depth limit can be wired even when
    // the L1 stack isn't activated. Deriver is the sole writer
    // (only in follower/composer modes).
    let l1_head = Arc::new(L1CanonicalHead::default());

    let timing = RollupTiming::from_env()?;

    let attributes = EthAttributesBuilder::new(chain_spec.clone());

    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        beacon_engine_handle,
        payload_builder_handle,
        witness_capture.sender(),
    )?;
    let mut produced_payloads = block_committer.subscribe_produced_payloads();
    let unsafe_block_signer = unsafe_block_signer_from_env()?;
    let unsafe_block_signer_address = unsafe_block_signer.address();
    let (p2p_service, p2p_handle, _p2p_events) =
        NetworkService::new(ext.p2p.network_config(l2_chain_id)?)?;
    let p2p_seed_handle = p2p_handle.clone();
    let p2p_seed_signer = unsafe_block_signer.clone();
    task_executor.spawn_critical_task("eez-unsafe-block-p2p", p2p_service.run());
    task_executor.spawn_critical_task("eez-unsafe-block-publisher", async move {
        loop {
            let payload = match produced_payloads.recv().await {
                Ok(payload) => payload,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    panic!("unsafe-block publisher lagged and skipped {skipped} committed payloads")
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            };
            let block_number = payload.block_number();
            let block_hash = payload.block_hash();
            let message =
                sign_payload(&payload, l2_chain_id, &unsafe_block_signer).unwrap_or_else(|error| {
                    panic!(
                        "could not encode/sign produced block {block_number} {block_hash}: {error}"
                    )
                });
            p2p_handle
                .publish(block_number, message)
                .await
                .unwrap_or_else(|error| panic!("unsafe-block P2P service stopped: {error}"));
        }
    });
    event!(
        name: "eez.node.composer.p2p.spawned",
        Level::INFO,
        signer = %unsafe_block_signer_address,
        "spawning signed unsafe-block P2P publisher",
    );
    let depth = env::var("EEZ_MAX_SPECULATIVE_DEPTH")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_SPECULATIVE_DEPTH);

    let submitter_config = SubmitterConfig::from_env()?;
    let rollup_config = RollupConfig::from_env()?;
    let l1_watcher_config = L1WatcherConfig::from_env()?;

    let submitter = Submitter::new(submitter_config);
    // Handle only — polling starts after boot catch-up fixes the
    // seed and every consumer has subscribed.
    let l1_watcher = L1Watcher::new(l1_watcher_config);

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

        let eez_registry: Address =
            Address::from_str(&env::var("EEZ_REGISTRY_ADDRESS").map_err(|_| {
                eyre::eyre!(
                    "EEZ_REGISTRY_ADDRESS required for the cross-chain composer (set by deploy.sh)"
                )
            })?)?;

        let eezl2_address: Address =
            Address::from_str(&env::var("EEZL2_ADDRESS").map_err(|_| {
                eyre::eyre!(
                    "EEZL2_ADDRESS required for the cross-chain composer (set by deploy.sh)"
                )
            })?)?;
        let l1_rollup_id_u64 = read_l1_rollup_id()?;
        let l1_rollup_id = RollupId(l1_rollup_id_u64);
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
        // A colliding id would silently overwrite the L1 entry
        // registration (EEZ_L1_ROLLUP_ID defaults to 0).
        if wired_rollups
            .insert(l2_rollup_id_typed, (l2_follower_view, l2_follower_cfg))
            .is_some()
        {
            return Err(eyre::eyre!(
                "duplicate rollup id {l2_rollup_id_typed}: EEZ_ROLLUP_ID collides \
                     with EEZ_L1_ROLLUP_ID; the L2 follower registration would \
                     overwrite the L1 entry"
            ));
        }
        // CrossChainExecCtx: signer + L2 addresses needed to wrap
        // EvmComposer's `(load_table, execute)` calldata pairs
        // into signed legacy L2 system txs at Sync-slot time.
        // Constructed only when EvmComposer is constructed —
        // both are tied to embedded L1 mode.
        let system_key = env::var("EEZ_L2_SYSTEM_KEY").map_err(|_| {
            eyre::eyre!("EEZ_L2_SYSTEM_KEY required when the cross-chain composer is wired")
        })?;
        let system_signer =
            PrivateKeySigner::from_bytes(&B256::from_str(system_key.trim_start_matches("0x"))?)?;
        // Submission RPC for postBatch and inbound source-chain reads.
        // This can differ from the embedded L1 used for local source
        // simulation (the E2E harness deliberately uses Anvil here), so
        // derive the signing chain ID from this provider rather than the
        // embedded chain spec.
        let l1_rpc_url: reqwest::Url = env::var("EEZ_L1_RPC_URL")
            .map_err(|_| eyre::eyre!("EEZ_L1_RPC_URL required for L1 forwarding"))?
            .parse()
            .map_err(|e| eyre::eyre!("EEZ_L1_RPC_URL malformed: {e}"))?;
        let l1_provider = alloy_provider::RootProvider::new_http(l1_rpc_url.clone());
        let l1_submission_chain_id =
            tokio::time::timeout(L1_CHAIN_ID_READ_TIMEOUT, l1_provider.get_chain_id())
                .await
                .map_err(|_| eyre::eyre!("timed out reading chain id from EEZ_L1_RPC_URL"))?
                .map_err(|e| eyre::eyre!("read chain id from EEZ_L1_RPC_URL: {e}"))?;
        let l1_poster_key = env::var("EEZ_L1_POSTER_KEY")
            .map_err(|_| eyre::eyre!("EEZ_L1_POSTER_KEY required for L1 postBatch signing"))?;
        let l1_poster_signer =
            PrivateKeySigner::from_bytes(&B256::from_str(l1_poster_key.trim_start_matches("0x"))?)?;
        let ecdsa_proof_system_address: Address =
            Address::from_str(&env::var("EEZ_ECDSA_PROOF_SYSTEM_ADDRESS").map_err(|_| {
                eyre::eyre!(
                    "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS required for L1 postBatch \
                         proofSystems[0]"
                )
            })?)?;
        // 10 gwei comfortably exceeds the smoke user_tx's
        // 2-gwei priority fee, so dev-reth's payload builder
        // orders postBatch ahead of the user_tx within the
        // L1 block.
        let l1_post_batch_priority_fee: u128 = env::var("EEZ_L1_POSTBATCH_PRIORITY_FEE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000_000_000);
        // One submission path everywhere: the composer always
        // routes `[postBatch, user_tx_1, …]` through the shared
        // Submitter. The Submitter owns the transport decision —
        // `eth_sendBundle` on relays that support it (rbuilder),
        // ordered mempool submission on plain execution RPCs
        // (dev reth, anvil) detected via JSON-RPC -32601.
        let exec_ctx = Arc::new(eez_composer::CrossChainExecCtx {
            system_signer,
            eezl2_address,
            l2_chain_id: chain_spec.chain().id(),
            l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
            l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
            l1_provider,
            submitter: submitter.clone(),
            l1_poster_signer,
            l1_chain_id: l1_submission_chain_id,
            l1_post_batch_priority_fee,
            ecdsa_proof_system_address,
        });
        event!(
            name: "eez.node.evm_composer.ready",
            Level::INFO,
            l1_rollup_id = l1_rollup_id_u64,
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
        let witness_source = match witness_capture {
            WitnessCapture::Remote {
                receiver: rx,
                store,
                ..
            } => {
                let cap_provider = provider.clone();
                let cap_evm = evm_config.clone();
                let ws_provider = provider.clone();
                let ws_evm = evm_config.clone();
                let cap_store = Arc::clone(&store);
                // Purge floor = L1-FINALIZED height (a reorg could un-settle a posted batch).
                let cap_l1_head = Arc::clone(&l1_head);
                task_executor.spawn_critical_task("eez-witness-capture", async move {
                    witness_source::run_capture(rx, cap_store, cap_provider, cap_evm, move || {
                        cap_l1_head.finalized_l2()
                    })
                    .await;
                });
                // Hybrid: read the store, else re-exec on demand the newest block
                // the async capture hasn't drained yet (state still retained; fast
                // for near-empty blocks).
                Some(Arc::new(witness_source::NodeWitnessSource::new(
                    store,
                    ws_provider,
                    ws_evm,
                ))
                    as Arc<dyn eez_prover::ProvingWitnessSource>)
            }
            WitnessCapture::NotRequired => None,
        };
        let composer = Composer::new(
            rollups,
            prover,
            evm_config,
            cross_chain,
            block_committer.clone(),
            witness_source,
            timing,
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
            depth,
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
    for spec in xchain_front_specs(l1_source_chain_id, l2_source_chain_id) {
        let (port, url, parsed) = read_xchain_front_config(spec.port_env, spec.url_env)?;
        let validation_provider = alloy_provider::RootProvider::new_http(parsed);
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
        rollup_config.deploy_block,
        Arc::clone(&l1_head),
        Some(system_tx_cfg),
        read_checkpoint_dir(),
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
    wait_for_l1_ready(&l1_reader, rollup_config.deploy_block, l1_source_chain_id).await?;
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

    // Rebuild the bounded recovery window after reconciliation. The live
    // publisher only observes newly produced blocks, while a restarting
    // composer may already have locally persisted unsafe descendants.
    let unsafe_head = block_committer.last_header().number;
    let safe_number = provider
        .safe_block_num_hash()
        .map_err(|error| eyre::eyre!("read L2 safe head for P2P cache: {error}"))?
        .map_or(0, |safe| safe.number);
    let first_cached = safe_number
        .saturating_add(1)
        .max(unsafe_head.saturating_sub(BACKFILL_CACHE_SIZE.saturating_sub(1) as u64));
    let mut seeded = 0_u64;
    for number in first_cached..=unsafe_head {
        let block = provider
            .recovered_block(number.into(), TransactionVariant::WithHash)
            .map_err(|error| eyre::eyre!("read L2 block {number} for P2P cache: {error}"))?
            .ok_or_else(|| eyre::eyre!("L2 block {number} missing while seeding P2P cache"))?;
        let payload =
            <EthEngineTypes as PayloadTypes>::block_to_payload(block.into_sealed_block(), None);
        let message = match sign_payload(&payload, l2_chain_id, &p2p_seed_signer) {
            Ok(message) => message,
            Err(error) => {
                event!(
                    name: "eez.node.composer.p2p.cache_seed_incomplete",
                    Level::WARN,
                    block.number = number,
                    %error,
                    "persisted block lacks complete fork-specific payload data; stopping P2P cache seeding",
                );
                break;
            }
        };
        p2p_seed_handle
            .publish(number, message)
            .await
            .map_err(|error| eyre::eyre!("seed signed-block P2P cache: {error}"))?;
        seeded += 1;
    }
    event!(
        name: "eez.node.composer.p2p.cache_seeded",
        Level::INFO,
        first_block = first_cached,
        last_block = unsafe_head,
        blocks = seeded,
        "seeded signed unsafe-block recovery cache",
    );
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

/// Blocks until L1 serves our history: head at or past the deploy block, with
/// that block's header, logs, and tx bodies readable. An L1 that stops making
/// progress fails here instead of hanging.
async fn wait_for_l1_ready(
    l1_reader: &L1Reader,
    deploy_block: u64,
    expected_l1_chain_id: u64,
) -> eyre::Result<()> {
    const POLL: Duration = Duration::from_secs(2);
    const STALLED_POLLS: u32 = 450; // 15 min with no progress
    const MAX_POLLS: u32 = 3_600; // 2 h overall, even while it claims to sync

    let mut best_remaining = u64::MAX;
    let mut stalled = 0_u32;
    let mut waited = 0_u32;
    let mut last_err: Option<String> = None;
    let mut chain_verified = false;

    loop {
        if !chain_verified {
            match l1_reader.chain_id().await {
                Ok(actual) if actual != expected_l1_chain_id => {
                    return Err(eyre::eyre!(
                        "EEZ_L1_RPC_URL serves chain {actual}, expected {expected_l1_chain_id}"
                    ));
                }
                Ok(_) => chain_verified = true,
                Err(err) => last_err = Some(format!("eth_chainId: {err}")),
            }
        }

        let (progressing, status) = match l1_reader.readiness().await {
            Err(err) => {
                last_err = Some(err.to_string());
                (false, format!("unreachable: {err}"))
            }
            Ok(state) if state.head_block_number < deploy_block => {
                let remaining = deploy_block - state.head_block_number;
                let closer = remaining < best_remaining;
                best_remaining = best_remaining.min(remaining);
                (closer, format!("{remaining} blocks below the deploy block"))
            }
            Ok(state) => match l1_reader.serves_history(deploy_block).await {
                Ok(true) if chain_verified => {
                    event!(
                        name: "eez.node.l1_ready",
                        Level::INFO,
                        head = state.head_block_number,
                        deploy_block,
                        "L1 source can serve our history",
                    );
                    return Ok(());
                }
                Ok(true) => (false, "chain id not read yet".to_string()),
                Ok(false) => (state.syncing, "does not serve the deploy block".to_string()),
                Err(err) => {
                    last_err = Some(err.to_string());
                    (false, format!("history probe failed: {err}"))
                }
            },
        };

        stalled = if progressing { 0 } else { stalled + 1 };
        waited += 1;
        if waited <= 1 || waited.is_multiple_of(30) {
            event!(
                name: "eez.node.l1_not_ready",
                Level::WARN,
                deploy_block,
                stalled_polls = stalled,
                status,
                "waiting for L1 to serve our history",
            );
        }
        if stalled >= STALLED_POLLS || waited >= MAX_POLLS {
            let cause = last_err.unwrap_or(status);
            return Err(eyre::eyre!(
                "L1 never became able to serve block {deploy_block}: {cause}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Build a `SystemTxContext` for follower mode from env (the follower
/// has no Composer to feed the composer-mode projection). Returns
/// `Ok(None)` when cross-chain env isn't present → pure-user-tx follower
/// mode. Reads `EEZ_L2_SYSTEM_KEY` / `EEZL2_ADDRESS` /
/// `EEZ_ROLLUP_ID`; the `l2_gas_price` (1 gwei) and `l2_gas_limit` (2M)
/// defaults mirror composer-mode so reconstructed system txs are
/// byte-identical.
///
/// # Errors
///
/// `eyre::Error` for malformed env values; missing required vars →
/// `Ok(None)`.
fn build_follower_system_tx_cfg<ChainSpec>(
    chain_spec: &ChainSpec,
) -> eyre::Result<Option<eez_protocol::system_tx::SystemTxContext>>
where
    ChainSpec: reth_chainspec::EthChainSpec,
{
    let system_key = match env::var("EEZ_L2_SYSTEM_KEY") {
        Ok(system_key) => system_key,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(eyre::eyre!("EEZ_L2_SYSTEM_KEY contains non-UTF-8 bytes"));
        }
    };
    let eezl2_address_str = env::var("EEZL2_ADDRESS")
        .map_err(|_| eyre::eyre!("EEZL2_ADDRESS required when EEZ_L2_SYSTEM_KEY is set"))?;
    let rollup_id_str = env::var("EEZ_ROLLUP_ID")
        .map_err(|_| eyre::eyre!("EEZ_ROLLUP_ID required when EEZ_L2_SYSTEM_KEY is set"))?;

    let system_signer =
        PrivateKeySigner::from_bytes(&B256::from_str(system_key.trim_start_matches("0x"))?)?;
    let eezl2_address: Address = Address::from_str(&eezl2_address_str)?;
    let this_rollup_id: u64 = rollup_id_str
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_ROLLUP_ID malformed: {e}"))?;

    Ok(Some(eez_protocol::system_tx::SystemTxContext {
        system_signer,
        eezl2_address,
        l2_chain_id: chain_spec.chain().id(),
        l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
        l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
        this_rollup_id,
    }))
}

/// The SYSTEM_ADDRESS the L2 pool refuses: `EEZ_L2_SYSTEM_ADDRESS`, else
/// derived from `EEZ_L2_SYSTEM_KEY`. `None` when neither is set — each role
/// decides whether that is fatal.
///
/// Both set and disagreeing is fatal: the key signs, so a differing address
/// would gate a sender that never appears and leave the real one open.
fn read_l2_system_address() -> eyre::Result<Option<Address>> {
    let from_key = l2_system_key_address()?;
    match env::var("EEZ_L2_SYSTEM_ADDRESS") {
        Ok(raw) => {
            let address = Address::from_str(raw.trim())
                .map_err(|e| eyre::eyre!("EEZ_L2_SYSTEM_ADDRESS malformed: {e}"))?;
            eyre::ensure!(
                from_key.is_none_or(|k| k == address),
                "EEZ_L2_SYSTEM_ADDRESS {address} does not match EEZ_L2_SYSTEM_KEY"
            );
            Ok(Some(address))
        }
        Err(env::VarError::NotPresent) => Ok(from_key),
        Err(env::VarError::NotUnicode(_)) => Err(eyre::eyre!(
            "EEZ_L2_SYSTEM_ADDRESS contains non-UTF-8 bytes"
        )),
    }
}

/// Address of `EEZ_L2_SYSTEM_KEY`, if set.
fn l2_system_key_address() -> eyre::Result<Option<Address>> {
    match env::var("EEZ_L2_SYSTEM_KEY") {
        Ok(raw) => Ok(Some(
            PrivateKeySigner::from_bytes(&B256::from_str(raw.trim().trim_start_matches("0x"))?)?
                .address(),
        )),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(eyre::eyre!("EEZ_L2_SYSTEM_KEY contains non-UTF-8 bytes"))
        }
    }
}

/// Read the L1 rollup id from env. Defaults to `0` to match the bridge
/// E2E fixture's `MAINNET_ROLLUP_ID` only when the variable is absent.
fn read_l1_rollup_id() -> eyre::Result<u64> {
    match env::var("EEZ_L1_ROLLUP_ID") {
        Ok(raw) => parse_l1_rollup_id(Some(&raw)),
        Err(env::VarError::NotPresent) => parse_l1_rollup_id(None),
        Err(env::VarError::NotUnicode(_)) => {
            Err(eyre::eyre!("EEZ_L1_ROLLUP_ID contains non-UTF-8 bytes"))
        }
    }
}

fn parse_l1_rollup_id(raw: Option<&str>) -> eyre::Result<u64> {
    let Some(raw) = raw else {
        return Ok(0);
    };
    raw.trim()
        .parse::<u64>()
        .map_err(|e| eyre::eyre!("EEZ_L1_ROLLUP_ID={raw:?} malformed: {e}"))
}

/// Where the deriver persists its boot checkpoint. Decided once: a missing
/// directory means this var is not our datadir, and retrying the write every
/// batch would only spam.
fn read_checkpoint_dir() -> Option<std::path::PathBuf> {
    let dir = env::var("EEZ_L2_DATADIR")
        .ok()
        .map(std::path::PathBuf::from)
        .filter(|dir| dir.is_dir());
    if dir.is_none() {
        event!(
            name: "eez.node.checkpoint.disabled",
            Level::INFO,
            configured = ?env::var("EEZ_L2_DATADIR").ok(),
            "no usable EEZ_L2_DATADIR; boot will rescan L1 from the deploy block",
        );
    }
    dir
}

/// Required for followers: guessing would either assert the wrong source
/// chain or silently skip the configured RPC's identity check.
fn read_l1_chain_id() -> eyre::Result<u64> {
    let value = env::var("EEZ_L1_CHAIN_ID").map_err(|err| {
        eyre::eyre!("EEZ_L1_CHAIN_ID is required (the L1 chain id this node derives from): {err}")
    })?;
    value
        .parse::<u64>()
        .map_err(|err| eyre::eyre!("EEZ_L1_CHAIN_ID={value:?} malformed: {err}"))
}

fn unsafe_block_signer_from_env() -> eyre::Result<PrivateKeySigner> {
    let key = env::var("EEZ_UNSAFE_BLOCK_SIGNER_KEY")
        .map_err(|_| eyre::eyre!("EEZ_UNSAFE_BLOCK_SIGNER_KEY is required in composer mode"))?;
    PrivateKeySigner::from_bytes(&B256::from_str(key.trim_start_matches("0x"))?).map_err(Into::into)
}

#[derive(Debug, Clone, Copy)]
struct XchainFrontSpec {
    port_env: &'static str,
    url_env: &'static str,
    direction: eez_composer::Direction,
    task: &'static str,
    expected_source_chain_id: u64,
}

fn xchain_front_specs(l1_chain_id: u64, l2_chain_id: u64) -> [XchainFrontSpec; 2] {
    [
        XchainFrontSpec {
            port_env: "EEZ_L1_XCHAIN_PORT",
            url_env: "EEZ_L1_RPC_URL",
            direction: eez_composer::Direction::Inbound,
            task: "eez-l1-xchain-front",
            expected_source_chain_id: l1_chain_id,
        },
        XchainFrontSpec {
            port_env: "EEZ_L2_XCHAIN_PORT",
            url_env: "EEZ_L2_RPC_URL",
            direction: eez_composer::Direction::Outbound,
            task: "eez-l2-xchain-front",
            expected_source_chain_id: l2_chain_id,
        },
    ]
}

fn read_xchain_front_config(
    port_env: &str,
    url_env: &str,
) -> eyre::Result<(u16, String, reqwest::Url)> {
    let port = match env::var(port_env) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(eyre::eyre!("{port_env} is not valid unicode: {err}")),
    };
    let url = match env::var(url_env) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(eyre::eyre!("{url_env} is not valid unicode: {err}")),
    };
    parse_xchain_front_config(port_env, url_env, port.as_deref(), url.as_deref())
}

fn parse_xchain_front_config(
    port_env: &str,
    url_env: &str,
    port: Option<&str>,
    url: Option<&str>,
) -> eyre::Result<(u16, String, reqwest::Url)> {
    let port_raw = port.ok_or_else(|| eyre::eyre!("{port_env} is required in composer mode"))?;
    let port = port_raw
        .parse::<u16>()
        .map_err(|err| eyre::eyre!("{port_env}={port_raw:?} malformed: {err}"))?;
    let Some(url_raw) = url else {
        return Err(eyre::eyre!("{port_env} is set but {url_env} is missing"));
    };
    let parsed = url_raw
        .parse::<reqwest::Url>()
        .map_err(|err| eyre::eyre!("{url_env}={url_raw:?} malformed: {err}"))?;
    Ok((port, url_raw.to_string(), parsed))
}

/// Build the `EmbeddedL1Config` from env; all vars optional, with testing
/// defaults so the smoke harness only overrides what it needs.
///
///   - `EEZ_L1_HTTP_PORT` — default `18545` (WS = http_port + 1)
///   - `EEZ_L1_AUTH_PORT` — default `http_port + 6`
///   - `EEZ_L1_P2P_PORT`  — default `30444` (P2P + discv4)
///   - `EEZ_L1_DISCV5_PORT` — default `p2p_port + 10` (discv5 UDP)
///   - `EEZ_L1_DATADIR`   — default `$TMPDIR/eez-l1-embedded` (ephemeral)
///   - `EEZ_L1_CHAIN_PATH` — L1 genesis JSON; unset → reth's `dev`
///     chainspec (all forks at genesis, no funded accounts)
fn build_embedded_l1_config() -> eyre::Result<l1_embedded::EmbeddedL1Config> {
    use reth_cli::chainspec::ChainSpecParser;

    let http_port = env::var("EEZ_L1_HTTP_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(18545);
    // Auth RPC port — kept clear of the WS port (which
    // build_network_rpc_args derives as http_port + 1) and configurable
    // so it can dodge a default-port collision with other nodes on the
    // host. Defaults to http_port + 6.
    let auth_port = env::var("EEZ_L1_AUTH_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(http_port.wrapping_add(6));
    let p2p_port = env::var("EEZ_L1_P2P_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(30444);
    // discv5 UDP port — kept separate from p2p_port (discv4) and
    // configurable so it can dodge a default-port collision with other
    // nodes on the host. Defaults to p2p_port + 10.
    let discv5_port = env::var("EEZ_L1_DISCV5_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(p2p_port.wrapping_add(10));
    let datadir = env::var("EEZ_L1_DATADIR").map_or_else(
        |_| std::env::temp_dir().join("eez-l1-embedded"),
        std::path::PathBuf::from,
    );

    // Chain spec: explicit path wins. Otherwise default to reth's
    // built-in `dev` chainspec — same shape `reth --chain dev` uses,
    // which has all forks (including Cancun/Prague where supported)
    // active at genesis with a couple of pre-funded dev EOAs.
    let chain_arg = env::var("EEZ_L1_CHAIN_PATH").unwrap_or_else(|_| "dev".to_string());
    let dev_chain_spec = EthereumChainSpecParser::parse(&chain_arg)
        .map_err(|e| eyre::eyre!("EEZ_L1_CHAIN_PATH={chain_arg}: {e}"))?;

    // L1 chain selector: `testing` (vanilla EthereumNode, auto-mine 5s),
    // `devnet` (EthereumNode + external CL), or `chiado`
    // (reth_gnosis::GnosisNode, real chiado state). The
    // `dev_chain_spec` is consumed by testing/devnet paths.
    let kind = l1_embedded::L1ChainKind::from_env();

    // JWT secret path — required for chiado/devnet (lighthouse engine API
    // auth); optional for testing mode (no external CL).
    let jwtsecret = env::var("EEZ_L1_JWT_SECRET")
        .ok()
        .map(std::path::PathBuf::from);

    let trusted_peers = env::var("EEZ_L1_TRUSTED_PEERS")
        .ok()
        .map(|peers| {
            peers
                .split([',', ' '])
                .map(str::trim)
                .filter(|peer| !peer.is_empty())
                .map(str::parse)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(l1_embedded::EmbeddedL1Config {
        dev_chain_spec,
        kind,
        datadir,
        http_port,
        auth_port,
        p2p_port,
        discv5_port,
        jwtsecret,
        trusted_peers,
    })
}

fn warn_on_deprecated_env() {
    for name in [
        "EEZ_COMPOSER_INTERVAL_SECS",
        "EEZ_SEQUENCER_DISABLED",
        "EEZ_COMPOSER_DISABLED",
    ] {
        if env::var_os(name).is_some() {
            event!(
                name: "eez.node.env.deprecated",
                Level::WARN,
                env = name,
                "env var is ignored; select the node role with the eez-composer, eez-follower, or eez-dev-node executable."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l1_rollup_id_defaults_only_when_absent() {
        assert_eq!(parse_l1_rollup_id(None).unwrap(), 0);
        assert_eq!(parse_l1_rollup_id(Some(" 7 ")).unwrap(), 7);
        let err = parse_l1_rollup_id(Some("1o")).unwrap_err().to_string();
        assert!(err.contains("EEZ_L1_ROLLUP_ID=\"1o\" malformed"));
    }

    #[test]
    fn xchain_front_missing_port_fails_fast() {
        let err = parse_xchain_front_config("PORT", "URL", None, Some("http://127.0.0.1:8545"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("PORT is required in composer mode"));
    }

    #[test]
    fn xchain_front_malformed_port_fails_fast() {
        let err =
            parse_xchain_front_config("PORT", "URL", Some("not-a-port"), Some("http://127.0.0.1"))
                .unwrap_err()
                .to_string();
        assert!(err.contains("PORT=\"not-a-port\" malformed"));
    }

    #[test]
    fn xchain_front_missing_upstream_fails_fast() {
        let err = parse_xchain_front_config("PORT", "URL", Some("8546"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("PORT is set but URL is missing"));
    }

    #[test]
    fn xchain_front_malformed_upstream_fails_fast() {
        let err = parse_xchain_front_config("PORT", "URL", Some("8546"), Some("not a url"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("URL=\"not a url\" malformed"));
    }

    #[test]
    fn xchain_front_valid_config_is_returned() {
        let (port, url, parsed) =
            parse_xchain_front_config("PORT", "URL", Some("8546"), Some("http://127.0.0.1:8545"))
                .expect("valid config");

        assert_eq!(port, 8546);
        assert_eq!(url, "http://127.0.0.1:8545");
        assert_eq!(parsed.as_str(), "http://127.0.0.1:8545/");
    }

    #[test]
    fn xchain_front_specs_assign_source_chain_ids() {
        let specs = xchain_front_specs(31_337, 90_210);

        assert_eq!(specs[0].port_env, "EEZ_L1_XCHAIN_PORT");
        assert_eq!(specs[0].direction, eez_composer::Direction::Inbound);
        assert_eq!(specs[0].expected_source_chain_id, 31_337);

        assert_eq!(specs[1].port_env, "EEZ_L2_XCHAIN_PORT");
        assert_eq!(specs[1].direction, eez_composer::Direction::Outbound);
        assert_eq!(specs[1].expected_source_chain_id, 90_210);
    }
}
