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
//! - `eez-follower`: L1-derived replay with an optional unsafe-head RPC overlay.
//! - `eez-dev-node`: unanchored interval sequencing for local development.

mod bundle_rpc;
mod follower;
mod ingress;
mod l1_embedded;
mod witness_source;

use std::{collections::HashMap, env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_provider::{Provider as _, RootProvider};
use alloy_signer_local::PrivateKeySigner;
use clap::Parser as _;
use eez_composer::composer::CrossChainWiring;
use eez_composer::{Composer, HeldPool, RollupConfig, RollupState};
use eez_deriver::Deriver;
use eez_driver::{
    EthAttributesBuilder, RollupTiming, Sequencer, SlotEvent, SyncSlotComposerHandle,
    spawn_interval, spawn_l1_anchored,
};
use eez_l1::{
    L1CanonicalHead, L1HeadStream, L1Watcher, L1WatcherConfig, Submitter, SubmitterConfig,
};
use eez_prover::MockEcdsaProver;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_builder::components::BasicPayloadServiceBuilder;
use reth_node_ethereum::EthereumNode;
use tokio::sync::mpsc;
use tracing::{Level, event};

use follower::UnsafeHeadFollower;

mod payload;
use payload::EezPayloadBuilder;

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Operational role selected by the invoking binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Standalone,
    Follower,
    Composer,
}

enum L1RoleRuntime<Sequencer, Composer> {
    Follower {
        system_tx_cfg: Option<eez_protocol::system_tx::SystemTxContext>,
    },
    Composer {
        sequencer: Sequencer,
        composer: Composer,
        held_pool: Arc<HeldPool>,
        system_tx_cfg: eez_protocol::system_tx::SystemTxContext,
        l1_source_chain_id: u64,
    },
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

impl NodeRole {
    const fn name(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Follower => "follower",
            Self::Composer => "composer",
        }
    }
}

/// eez-node-specific CLI arguments layered on top of reth's CLI.
#[derive(clap::Args, Debug, Clone)]
struct NodeExt {
    /// Sequencer JSON-RPC URL. In follower mode this enables the
    /// optional unsafe-head overlay; safe/finalized remain L1-derived.
    #[arg(long, env = "EEZ_SEQUENCER_RPC")]
    sequencer_rpc: Option<url::Url>,
}

// Bootstrap wiring is linear; splitting it across helpers fragments the
// dependency chain without making it easier to read (the helpers would
// need every reth generic threaded through). The clippy.toml threshold
// catches genuinely sprawling logic — the shared launcher is the exception.
#[allow(clippy::too_many_lines)]
pub fn run(role: NodeRole) -> eyre::Result<()> {
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("deployments.env");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    // The optimistic composer + Deriver roll the L2 head back to a
    // canonical ancestor; by Engine API spec an FCU to an ancestor is a
    // no-op, which would freeze payload builds. These flags opt into
    // reth's proposer-may-reorg-own-chain mode so the FCU unwinds the head.
    let mut argv: Vec<String> = std::env::args().collect();
    for flag in [
        "--engine.always-process-payload-attributes-on-canonical-head",
        "--engine.allow-unwind-canonical-header",
    ] {
        if !argv.iter().any(|a| a == flag) {
            argv.push(flag.to_owned());
        }
    }

    Cli::<EthereumChainSpecParser, NodeExt>::try_parse_from(argv)?.run(async move |builder, ext| {
        event!(
            name: "eez.node.launching",
            Level::INFO,
            mode = role.name(),
            "launching eez-node",
        );

        warn_on_deprecated_env();
        if role != NodeRole::Follower && ext.sequencer_rpc.is_some() {
            return Err(eyre::eyre!(
                "follower sequencer RPC can only be set in follower mode",
            ));
        }

        // Launch the embedded L1 reth first in composer mode — its
        // `StateProviderFactory` backs `LocalChainClient::new_entry` for
        // L1 source-tx simulation. Inline (not in `l1_embedded.rs`)
        // because the `NodeHandle` AddOns type resists a typed return.
        let embed_l1 = role == NodeRole::Composer;
        // Shared L1-reth tokio runtime — built once, used by whichever
        // L1 path runs.
        let build_l1_runtime = || {
            reth_tasks::RuntimeBuilder::new(
                reth_tasks::RuntimeConfig::default().with_tokio(
                    reth_tasks::TokioConfig::existing_handle(
                        tokio::runtime::Handle::current(),
                    ),
                ),
            )
            .build()
            .map_err(|e| eyre::eyre!("L1 embedded RuntimeBuilder: {e}"))
        };
        // Testing = vanilla EthereumNode (5s auto-mine); Devnet = vanilla
        // EthereumNode + external CL; Chiado = reth_gnosis::GnosisNode +
        // external CL, its provider wrapped by `GnosisL1Adapter` for the
        // alloy-Header bound. Returns an `EmbeddedL1` owning the NodeHandle so
        // the L1 outlives the node.
        let embedded_l1: Option<EmbeddedL1<_, _>> = if embed_l1 {
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
                    Some(EmbeddedL1::Ethereum(l1_handle))
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
                    Some(EmbeddedL1::Ethereum(l1_handle))
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
                    Some(EmbeddedL1::Chiado(chiado_handle))
                }
            }
        } else {
            None
        };

        // L2 reth. `EezPayloadBuilder` writes `gas_limit`/`extra_data` from
        // shared `eez-driver` constants so deriver replay and sequencer builds
        // yield identical headers.
        let handle = builder
            .with_types::<EthereumNode>()
            .with_components(
                EthereumNode::components()
                    .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder)),
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

        // RollupTiming: required when L1 is engaged; standalone-default
        // when not (only `l2_block_time()` is meaningful in that path).
        let timing = if role == NodeRole::Standalone {
            event!(
                name: "eez.node.timing.standalone_default",
                Level::INFO,
                "standalone mode — using default RollupTiming (L2=2s); set EEZ_*_TIME_MS to override",
            );
            RollupTiming::standalone_default()
        } else {
            RollupTiming::from_env()?
        };

        let attributes = EthAttributesBuilder::new(chain_spec.clone());

        // Build per-mode pieces: schedule receiver + (optional) batch
        // candidate channel + (optional) speculative limit.
        let schedule_rx = match role {
            NodeRole::Standalone => spawn_interval(timing.l2_block_time()),
            NodeRole::Follower => {
                // Dummy channel; Sequencer is constructed (to spawn
                // BlockCommitter) but never .run(). Holding the sender
                // here keeps the receiver from closing immediately.
                let (_tx, rx) = mpsc::channel::<SlotEvent>(1);
                rx
            }
            NodeRole::Composer => {
                let submitter_config = SubmitterConfig::from_env()?;
                let _ = submitter_config; // validated by Composer block below
                let l1_watcher_config_preview = L1WatcherConfig::from_env()?;
                let _ = l1_watcher_config_preview; // validated below
                // Placeholder for schedule_rx; replaced inside the composer
                // arm with the real L1-anchored schedule (spawn_l1_anchored).
                let (_drop_tx, drop_rx) = mpsc::channel::<SlotEvent>(1);
                drop_rx
            }
        };

        // Sequencer is constructed in all modes so its `BlockCommitter`
        // actor is available for the Deriver (follower / composer)
        // and so its receiver-side schedule channel is wired up. In
        // standalone mode it runs the produce loop; in follower it's
        // dropped (committer stays alive via the cloned handle); in
        // composer the L1-anchored schedule arrives via spawn_l1_anchored.
        // Remote-prover composer mode: the committer emits each committed block's
        // hash here; a capture task re-executes it while parent state is fresh and
        // stores the witness for the composer. `None` otherwise. Created here to
        // thread `witness_sender` into the committer at spawn.
        let (witness_sender, witness_rx, witness_store) =
            if role == NodeRole::Composer && env::var_os("EEZ_PROVER_URL").is_some() {
                let (tx, rx) = mpsc::unbounded_channel::<B256>();
                // Dedicated mdbx env (never reth's node DB); path env-configurable.
                let witness_db_path =
                    env::var("EEZ_WITNESS_DB_PATH").unwrap_or_else(|_| "eez-witnesses".to_owned());
                let store = witness_source::new_store(std::path::Path::new(&witness_db_path))?;
                event!(
                    name: "eez.node.witness_store.opened",
                    Level::INFO,
                    path = %witness_db_path,
                    "persistent witness store opened",
                );
                (Some(tx), Some(rx), Some(store))
            } else {
                (None, None, None)
            };

        let mut sequencer = Sequencer::new(
            &provider,
            attributes,
            beacon_engine_handle,
            schedule_rx,
            payload_builder_handle,
            timing,
            witness_sender,
        )?;
        if role != NodeRole::Standalone {
            let depth = env::var("EEZ_MAX_SPECULATIVE_DEPTH")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(64);
            if depth != 0 {
                sequencer = sequencer.with_speculative_limit(depth, Arc::clone(&l1_head));
            }
        }

        let block_committer = sequencer.committer();

        // ─── Standalone: spawn Sequencer + done ──────────────────────
        if role == NodeRole::Standalone {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "standalone", "spawning eez sequencer");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });
            return handle.wait_for_node_exit().await;
        }

        // ─── L1 stack (follower + composer) ──────────────────────────
        let submitter_config = if role == NodeRole::Follower {
            SubmitterConfig::from_env_read_only()?
        } else {
            SubmitterConfig::from_env()?
        };
        let rollup_config = RollupConfig::from_env()?;
        let l1_watcher_config = L1WatcherConfig::from_env()?;

        let submitter = Submitter::new(submitter_config);
        // Handle only — polling starts after boot catch-up fixes the
        // seed and every consumer has subscribed.
        let l1_watcher = L1Watcher::new(l1_watcher_config);

        // Composer-only: build the umbrella, then attach it to the
        // Sequencer built above (swapping in the L1-anchored schedule via
        // `with_schedule_rx`). Must reuse that instance — its
        // BlockCommitter actor is the one the Deriver shares; rebuilding
        // would spawn a second actor with its own reconcile lock + head
        // mirror, splitting the serialization domain.
        let composer_setup = if role == NodeRole::Composer {
            // Attestation source. Remote mode (`EEZ_PROVER_URL`) holds NO signing
            // key in the composer: it dials eez-proof-signer and only verifies that each
            // attestation recovers to the configured attester address (the on-chain
            // proof-system check is authoritative; this is a fail-fast).
            let prover: Arc<dyn eez_prover::Prover> = match env::var("EEZ_PROVER_URL") {
                Ok(url) => {
                    let attester = env::var("EEZ_ATTESTER_ADDRESS").map_err(|_| {
                        eyre::eyre!("EEZ_ATTESTER_ADDRESS required in remote-prover mode")
                    })?;
                    let attester = Address::from_str(attester.trim())
                        .map_err(|e| eyre::eyre!("EEZ_ATTESTER_ADDRESS: {e}"))?;
                    Arc::new(eez_prover_client::RemoteProver::new(url, attester))
                }
                Err(_) => {
                    let key = env::var("EEZ_PROOF_SIGNER_KEY").map_err(|_| {
                        eyre::eyre!("EEZ_PROOF_SIGNER_KEY required in mock-prover mode")
                    })?;
                    let signer = PrivateKeySigner::from_bytes(&B256::from_str(
                        key.trim_start_matches("0x"),
                    )?)?;
                    Arc::new(MockEcdsaProver::new(signer))
                }
            };
            let rollup_id = rollup_config.rollup_id;
            let l1_variant = embedded_l1
                .as_ref()
                .expect("eez-composer always launches its embedded L1");
            let l1_source_chain_id = match l1_variant {
                EmbeddedL1::Ethereum(l1_handle) => {
                    l1_handle.node.chain_spec().chain().id()
                }
                EmbeddedL1::Chiado(chiado_handle) => {
                    chiado_handle.node.chain_spec().inner.chain().id()
                }
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

            let eez_registry: Address = Address::from_str(&env::var("EEZ_REGISTRY_ADDRESS").map_err(
                |_| eyre::eyre!("EEZ_REGISTRY_ADDRESS required for the cross-chain composer (set by deploy.sh)"),
            )?)?;

            let eezl2_address: Address = Address::from_str(&env::var("EEZL2_ADDRESS").map_err(
                |_| eyre::eyre!("EEZL2_ADDRESS required for the cross-chain composer (set by deploy.sh)"),
            )?)?;
            let l1_rollup_id_u64 = read_l1_rollup_id();
            let l1_rollup_id = RollupId(l1_rollup_id_u64);
            let l2_rollup_id_typed = RollupId(rollup_id);

            // L1 entry differs per kind: Devnet/Testing use the native
            // provider + EvmConfig; Chiado wraps it in
            // `GnosisL1Adapter` and builds a fresh `EthEvmConfig`
            // over the chiado ChainSpec (source-sim needs only
            // revm, not GnosisNode's AuRa paths). Both yield the
            // same erased views, so composition is identical.
            let entry_client_view = match l1_variant {
                EmbeddedL1::Ethereum(l1_handle) => {
                    let l1_provider = l1_handle.node.provider.clone();
                    let l1_evm_config = l1_handle.node.evm_config.clone();
                    let entry_client = LocalChainClient::new_entry(
                        l1_provider,
                        l1_evm_config,
                        l1_rollup_id,
                        eez_registry,
                        eez_protocol::ChainDialect::EvmL1Style,
                    );
                    let entry_view: std::sync::Arc<
                        dyn eez_protocol::executor::ChainClient
                            + Send
                            + Sync,
                    > = entry_client.clone();
                    entry_view
                }
                EmbeddedL1::Chiado(chiado_handle) => {
                    // `GnosisChainSpec.inner` is the standard
                    // reth `ChainSpec` (via `#[deref]`); wrap it
                    // fresh as `Arc<ChainSpec>` for the
                    // standard `EthEvmConfig` simulation envs.
                    let gnosis_chain_spec = chiado_handle.node.chain_spec();
                    let l1_chain_spec: Arc<reth_chainspec::ChainSpec> =
                        Arc::new(gnosis_chain_spec.inner.clone());
                    let l1_provider = GnosisL1Adapter::new(
                        chiado_handle.node.provider.clone(),
                    );
                    let l1_evm_config =
                        reth_evm_ethereum::EthEvmConfig::new(Arc::clone(&l1_chain_spec));
                    let entry_client = LocalChainClient::new_entry(
                        l1_provider,
                        l1_evm_config,
                        l1_rollup_id,
                        eez_registry,
                        eez_protocol::ChainDialect::EvmL1Style,
                    );
                    let entry_view: std::sync::Arc<
                        dyn eez_protocol::executor::ChainClient
                            + Send
                            + Sync,
                    > = entry_client.clone();
                    entry_view
                }
            };

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
                dyn eez_protocol::executor::ChainClient
                    + Send
                    + Sync,
            > = l2_follower;

            // L2 ENTRY client (follower's provider/dialect, but
            // Role::Entry) — the follower client errors `Unavailable` for
            // the outbound source-sim `simulate_and_resolve_recorded_for`.
            let l2_entry = LocalChainClient::new_entry(
                provider.clone(),
                evm_config.clone(),
                l2_rollup_id_typed,
                eezl2_address,
                eez_protocol::ChainDialect::EvmL2Style,
            );
            let l2_entry_view: std::sync::Arc<
                dyn eez_protocol::executor::ChainClient
                    + Send
                    + Sync,
            > = l2_entry;

            let entry_cfg = TargetConfig {
                proxy_lookup: ProxyLookupConfig {
                    contract_address: eez_registry,
                    authorized_proxies_slot: eez_protocol::ChainDialect::EvmL1Style
                        .proxy_lookup_slot(),
                },
                dialect: eez_protocol::ChainDialect::EvmL1Style,
            };
            let l2_follower_cfg = TargetConfig {
                proxy_lookup: ProxyLookupConfig {
                    contract_address: eezl2_address,
                    authorized_proxies_slot: eez_protocol::ChainDialect::EvmL2Style
                        .proxy_lookup_slot(),
                },
                dialect: eez_protocol::ChainDialect::EvmL2Style,
            };

            let mut wired_rollups = std::collections::HashMap::new();
            wired_rollups
                .insert(l1_rollup_id, (Arc::clone(&entry_client_view), entry_cfg));
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
            let system_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                system_key.trim_start_matches("0x"),
            )?)?;
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
            let l1_submission_chain_id = l1_provider
                .get_chain_id()
                .await
                .map_err(|e| eyre::eyre!("read chain id from EEZ_L1_RPC_URL: {e}"))?;
            let l1_poster_key = env::var("EEZ_L1_POSTER_KEY").map_err(|_| {
                eyre::eyre!("EEZ_L1_POSTER_KEY required for L1 postBatch signing")
            })?;
            let l1_poster_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                l1_poster_key.trim_start_matches("0x"),
            )?)?;
            let ecdsa_proof_system_address: Address = Address::from_str(
                &env::var("EEZ_ECDSA_PROOF_SYSTEM_ADDRESS").map_err(|_| {
                    eyre::eyre!(
                        "EEZ_ECDSA_PROOF_SYSTEM_ADDRESS required for L1 postBatch \
                         proofSystems[0]"
                    )
                })?,
            )?;
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
                l2_gas_price: 1_000_000_000,
                l2_gas_limit: 2_000_000,
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
                entry_client: entry_client_view,
                rollups: wired_rollups,
                exec_ctx,
                l2_entry_client: l2_entry_view,
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
            let witness_source = match (witness_rx, witness_store) {
                (Some(rx), Some(store)) => {
                    let cap_provider = provider.clone();
                    let cap_evm = evm_config.clone();
                    let ws_provider = provider.clone();
                    let ws_evm = evm_config.clone();
                    let cap_store = Arc::clone(&store);
                    // Purge floor = L1-FINALIZED height (a reorg could un-settle a posted batch).
                    let cap_l1_head = Arc::clone(&l1_head);
                    task_executor.spawn_critical_task("eez-witness-capture", async move {
                        witness_source::run_capture(
                            rx,
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
                    Some(Arc::new(witness_source::NodeWitnessSource::new(
                        store, ws_provider, ws_evm,
                    )) as Arc<dyn eez_prover::ProvingWitnessSource>)
                }
                _ => None,
            };
            let composer = Composer::new(
                rollups,
                prover,
                submitter.clone(),
                evm_config,
                cross_chain,
                timing,
            );
            if let Some(ws) = witness_source {
                composer.set_witness_source(ws);
            }
            let sync_slot_handle: SyncSlotComposerHandle = Arc::new(composer.clone());

            // Reuse the same Sequencer (and its single BlockCommitter
            // actor, shared with the Deriver) — swap in the L1-anchored
            // schedule + composer hooks. Speculative depth already
            // applied above.
            let schedule_rx = spawn_l1_anchored(
                L1HeadStream::from_watcher(&l1_watcher),
                timing,
                l2_genesis_timestamp,
            );
            let sequencer = sequencer
                .with_schedule_rx(schedule_rx)
                .with_sync_slot_composer(rollup_id, sync_slot_handle);
            // Hand the same BlockCommitter handle to the composer so its
            // slot-context recovery can roll back failed optimistic Sync
            // blocks — the actor stays the sole engine-API owner.
            composer.set_committer(sequencer.committer());
            L1RoleRuntime::Composer {
                sequencer,
                composer,
                held_pool,
                system_tx_cfg: deriver_system_tx_cfg,
                l1_source_chain_id,
            }
        } else {
            // Follower: drop the placeholder Sequencer (BlockCommitter
            // survives via the cloned handle). Build a SystemTxContext
            // from env so the Deriver reconstructs system txs the same
            // way the composer does; None → pure-user-tx batches only.
            drop(sequencer);
            let follower_system_tx_cfg = build_follower_system_tx_cfg(&chain_spec)?;
            event!(
                name: "eez.node.follower.system_tx_cfg",
                Level::INFO,
                enabled = follower_system_tx_cfg.is_some(),
                "cross-chain system tx reconstruction config loaded",
            );
            L1RoleRuntime::Follower {
                system_tx_cfg: follower_system_tx_cfg,
            }
        };

        let l2_source_chain_id = chain_spec.chain().id();
        // Resolve and validate both required fronts before spawning the deriver,
        // sequencer, or composer. The fronts are required infrastructure, so a
        // missing configuration or bad/unavailable upstream must fail launch
        // rather than leave a healthy-looking node running without cross-chain
        // ingress.
        let mut xchain_fronts = Vec::new();
        if let L1RoleRuntime::Composer {
            l1_source_chain_id,
            ..
        } = &composer_setup
        {
            for spec in xchain_front_specs(*l1_source_chain_id, l2_source_chain_id) {
                let (port, url, parsed) =
                    read_xchain_front_config(spec.port_env, spec.url_env)?;
                let validation_provider = alloy_provider::RootProvider::new_http(parsed);
                ingress::validate_cross_chain_front(
                    &validation_provider,
                    spec.expected_source_chain_id,
                )
                .await?;
                xchain_fronts.push((spec, port, url, validation_provider));
            }
        }

        // Deriver: drives BlockCommitter from L1Events (follower +
        // composer). A wired `SystemTxContext` makes it reconstruct the
        // same L2 system txs the composer produced (single-source STF).
        let l2_block_time_secs = timing.l2_block_time().as_secs();
        let system_tx_cfg = match &composer_setup {
            L1RoleRuntime::Follower { system_tx_cfg } => system_tx_cfg.clone(),
            L1RoleRuntime::Composer { system_tx_cfg, .. } => Some(system_tx_cfg.clone()),
        };
        let deriver = Deriver::new(
            block_committer.clone(),
            Arc::new(provider.clone()),
            submitter.clone(),
            chain_spec,
            l2_block_time_secs,
            rollup_config.deploy_block,
            Arc::clone(&l1_head),
            system_tx_cfg,
        );

        let mut catch_up_retry_delay = BOOT_CATCH_UP_INITIAL_RETRY_DELAY;
        let mut catch_up_attempts = 0_u64;
        let (l1_seed_number, l1_seed_hash) = loop {
            match deriver.catch_up_with_seed().await {
                Ok(seed) => break seed,
                Err(err) if err.is_source_incomplete() => {
                    catch_up_attempts += 1;
                    event!(
                        name: "eez.node.deriver.boot_catch_up.source_incomplete",
                        Level::WARN,
                        mode = role.name(),
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
                        mode = role.name(),
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
            mode = role.name(),
            initial_posted_through = deriver.cursor(),
            "spawning eez deriver",
        );
        let deriver_run = deriver.clone();
        let deriver_events = l1_watcher.subscribe();
        task_executor.spawn_critical_task("eez-deriver", async move {
            deriver_run.run(deriver_events).await;
        });

        // Follower-only: optional sequencer-RPC unsafe-head overlay.
        // L1 replay always boots first, so safe/finalized anchors are
        // reconciled before the overlay can move unsafe head.
        if let L1RoleRuntime::Follower { .. } = &composer_setup {
            if let Some(sequencer_rpc) = ext.sequencer_rpc {
                let sequencer_rpc = RootProvider::new_http(sequencer_rpc);
                let follower = UnsafeHeadFollower::new(
                    block_committer,
                    sequencer_rpc,
                    provider,
                    timing.l2_block_time(),
                );
                event!(
                    name: "eez.node.follower.sequencer_rpc.spawned",
                    Level::INFO,
                    "spawning sequencer-RPC unsafe-head follower",
                );
                task_executor.spawn_critical_task("eez-node-follower-unsafe-head", async move {
                    follower.run().await;
                });
            } else {
                event!(
                    name: "eez.node.follower.l1_derived_only",
                    Level::INFO,
                    "EEZ_SEQUENCER_RPC not set; running L1-derived-only follower",
                );
            }
        }

        // ─── Composer-only: spawn Sequencer + umbrella ───────────────
        if let L1RoleRuntime::Composer {
            sequencer,
            composer,
            held_pool,
            ..
        } = composer_setup
        {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "composer", "spawning eez sequencer (L1-anchored)");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });

            event!(name: "eez.node.composer.spawned", Level::INFO, "spawning eez composer umbrella");
            let composer_events = l1_watcher.subscribe();
            task_executor.spawn_critical_task("eez-composer", async move {
                composer.run(composer_events).await;
            });

            // Cross-chain ingress fronts (see `run_cross_chain_front`) — one per
            // SOURCE chain, sharing `held_pool`. Both are required in composer mode:
            //   L1 front (EEZ_L1_XCHAIN_PORT → EEZ_L1_RPC_URL): L1→L2 Inbound.
            //   L2 front (EEZ_L2_XCHAIN_PORT → EEZ_L2_RPC_URL): L2→L1 Outbound.
            for (spec, port, url, validation_provider) in xchain_fronts {
                let pool = Arc::clone(&held_pool);
                task_executor.spawn_critical_task(spec.task, async move {
                    ingress::run_cross_chain_front(
                        port,
                        url,
                        spec.direction,
                        pool,
                        validation_provider,
                        spec.expected_source_chain_id,
                    )
                    .await
                    .unwrap_or_else(|e| panic!("configured cross-chain front exited: {e:#}"));
                });
            }
        }

        // Start polling last: every consumer is subscribed and catch-up
        // covered [deploy_block, seed], so the watcher owns all blocks
        // strictly after its seed — no startup scan gap.
        task_executor.spawn_critical_task(
            "eez-l1-watcher",
            l1_watcher.polling(l1_seed_number, l1_seed_hash),
        );

        handle.wait_for_node_exit().await
    })
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
        l2_gas_price: 1_000_000_000,
        l2_gas_limit: 2_000_000,
        this_rollup_id,
    }))
}

/// Read the L1 rollup id from env. Defaults to `0` to match the bridge
/// E2E fixture's `MAINNET_ROLLUP_ID`.
fn read_l1_rollup_id() -> u64 {
    env::var("EEZ_L1_ROLLUP_ID")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
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
