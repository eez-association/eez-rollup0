//! eez Rollup-0 node binary.
//!
//! Wraps reth with our composer stack: reth provides the EVM, storage,
//! networking, RPC, and engine; we provide block production
//! (Sequencer + Scheduler), L1 anchoring (`L1Watcher` + Deriver), and
//! batch submission (Composer umbrella).
//!
//! # Modes
//!
//! Mode is decided by env-var presence at startup. "Can attest" =
//! `EEZ_PROVER_URL` (remote prover; composer needs only `EEZ_ATTESTER_ADDRESS`,
//! never the key) OR `EEZ_PROOF_SIGNER_KEY` (local mock signer):
//!
//! | `EEZ_L1_RPC_URL` | can attest | Mode | Stack |
//! |---|---|---|---|
//! | unset | — | **standalone** | reth + Sequencer (interval Scheduler, Live blocks only) |
//! | set | no | **follower** | reth + `L1Watcher` + Deriver (no Sequencer) |
//! | set | yes | **composer** | reth + `L1Watcher` + Deriver + Sequencer (L1-anchored) + Composer umbrella |

mod bundle_rpc;
mod follower;
mod ingress;
mod l1_embedded;
mod witness_source;

use std::{collections::HashMap, env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_provider::RootProvider;
use alloy_signer_local::PrivateKeySigner;
use clap::Parser as _;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Standalone,
    Follower,
    Composer,
}

/// Embedded L1 reth handle — owned by `main` for the node lifetime so
/// the L1 stays alive (drop tears it down). Generic over both variants'
/// `NodeHandle` params because the AddOns type differs between
/// EthereumNode (Dev) and GnosisNode (Chiado).
///
/// Downstream matches `.as_ref()` for the chain_spec / provider /
/// evm_config to build the cross-chain composer; the Chiado provider
/// goes through `eez_composer::GnosisL1Adapter` to translate
/// `GnosisHeader → alloy_consensus::Header` on each read.
enum EmbeddedL1<Dev, Chiado> {
    Dev(Dev),
    Chiado(Chiado),
}

impl Mode {
    fn from_env() -> Self {
        let l1_enabled = env::var_os("EEZ_L1_RPC_URL").is_some();
        // A composer needs an attestation source: a remote prover
        // (`EEZ_PROVER_URL`, which holds the key remotely — the composer only
        // needs `EEZ_ATTESTER_ADDRESS`) or a local mock signer
        // (`EEZ_PROOF_SIGNER_KEY`). Either presence marks composer mode; a
        // follower has L1 but neither.
        let can_attest = env::var_os("EEZ_PROVER_URL").is_some()
            || env::var_os("EEZ_PROOF_SIGNER_KEY").is_some();
        match (l1_enabled, can_attest) {
            (false, _) => Self::Standalone,
            (true, false) => Self::Follower,
            (true, true) => Self::Composer,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
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
// catches genuinely sprawling logic — main() is the exception.
#[allow(clippy::too_many_lines)]
fn main() -> eyre::Result<()> {
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

    let mode = Mode::from_env();

    Cli::<EthereumChainSpecParser, NodeExt>::try_parse_from(argv)?.run(async move |builder, ext| {
        event!(
            name: "eez.node.launching",
            Level::INFO,
            mode = mode.name(),
            "launching eez-node",
        );

        warn_on_deprecated_env();
        if mode != Mode::Follower && ext.sequencer_rpc.is_some() {
            return Err(eyre::eyre!(
                "follower sequencer RPC can only be set in follower mode",
            ));
        }

        // The per-rollup HeldPool, shared with the umbrella composer so the
        // cross-chain fronts' pushes and the Sync-slot drain see one queue.
        let held_pool = Arc::new(HeldPool::new());

        // Launch the embedded L1 reth first in composer mode — its
        // `StateProviderFactory` backs `LocalChainClient::new_entry` for
        // L1 source-tx simulation. Inline (not in `l1_embedded.rs`)
        // because the `NodeHandle` AddOns type resists a typed return.
        let embed_l1 = mode == Mode::Composer
            && env::var("EEZ_L1_EMBEDDED").map_or(true, |v| {
                !matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "" | "0" | "false" | "no" | "off"
                )
            });
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
        // Dev = vanilla EthereumNode (5s auto-mine); Chiado =
        // reth_gnosis::GnosisNode + external CL, its provider wrapped by
        // `GnosisL1Adapter` for the alloy-Header bound. Returns an
        // `EmbeddedL1` owning the NodeHandle so the L1 outlives the node.
        let embedded_l1: Option<EmbeddedL1<_, _>> = if embed_l1 {
            let l1_cfg = build_embedded_l1_config()?;
            match l1_cfg.kind {
                l1_embedded::L1ChainKind::Dev => {
                    let node_cfg = l1_embedded::build_dev_node_config(&l1_cfg)?;
                    let db = reth_db::init_db(
                        node_cfg.datadir().db(),
                        reth_db::mdbx::DatabaseArguments::default(),
                    )
                    .map_err(|e| eyre::eyre!("L1 embedded init_db: {e}"))?;
                    event!(
                        name: "eez.node.l1_embedded.launching",
                        Level::INFO,
                        kind = "dev",
                        http_port = l1_cfg.http_port,
                        "launching embedded L1 reth (dev)",
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
                        kind = "dev",
                        l1_chain_id = %l1_handle.node.chain_spec().chain(),
                        "embedded L1 reth (dev) ready",
                    );
                    Some(EmbeddedL1::Dev(l1_handle))
                }
                l1_embedded::L1ChainKind::Testing => {
                    let node_cfg = l1_embedded::build_dev_node_config(&l1_cfg)?;
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
                    Some(EmbeddedL1::Dev(l1_handle))
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
        let timing = if mode == Mode::Standalone {
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
        let schedule_rx = match mode {
            Mode::Standalone => spawn_interval(timing.l2_block_time()),
            Mode::Follower => {
                // Dummy channel; Sequencer is constructed (to spawn
                // BlockCommitter) but never .run(). Holding the sender
                // here keeps the receiver from closing immediately.
                let (_tx, rx) = mpsc::channel::<SlotEvent>(1);
                rx
            }
            Mode::Composer => {
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
            if mode == Mode::Composer && env::var_os("EEZ_PROVER_URL").is_some() {
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
        if mode != Mode::Standalone {
            let depth = env::var("EEZ_MAX_SPECULATIVE_DEPTH")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(64);
            if depth != 0 {
                sequencer = sequencer.with_speculative_limit(depth, Arc::clone(&l1_head) as _);
            }
        }

        let block_committer = sequencer.committer();

        // ─── Standalone: spawn Sequencer + done ──────────────────────
        if mode == Mode::Standalone {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "standalone", "spawning eez sequencer");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });
            return handle.wait_for_node_exit().await;
        }

        // ─── L1 stack (follower + composer) ──────────────────────────
        let submitter_config = if mode == Mode::Follower {
            SubmitterConfig::from_env_read_only()?
        } else {
            SubmitterConfig::from_env()?
        };
        let rollup_config = RollupConfig::from_env()?;
        let l1_watcher_config = L1WatcherConfig::from_env()?;

        let submitter = Submitter::new(submitter_config);
        let l1_watcher = L1Watcher::spawn(l1_watcher_config);

        // Composer-only: build the umbrella, then attach it to the
        // Sequencer built above (swapping in the L1-anchored schedule via
        // `with_schedule_rx`). Must reuse that instance — its
        // BlockCommitter actor is the one the Deriver shares; rebuilding
        // would spawn a second actor with its own reconcile lock + head
        // mirror, splitting the serialization domain.
        let (sequencer, umbrella, system_tx_cfg) = if mode == Mode::Composer {
            // Attestation source. Remote mode (`EEZ_PROVER_URL`) holds NO signing
            // key in the composer: it dials eez-proverd and only VERIFIES that each
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
            // Share the SAME HeldPool the ingress middleware pushes into
            // (the `Option<Arc<HeldPool>>` leaves room for per-rollup
            // pools under one ingress layer later).
            let held_pool_for_rollup = Some(Arc::clone(&held_pool));
            let rollup_state = RollupState {
                config: rollup_config.clone(),
                timing,
                l2_provider: Arc::new(provider.clone()),
                l1_head: Arc::clone(&l1_head),
                held_pool: held_pool_for_rollup,
                optimistic: Arc::new(eez_composer::OptimisticallyIncluded::new()),
            };
            let mut rollups = HashMap::with_capacity(1);
            rollups.insert(rollup_id, rollup_state);
            // EVM config feeds the manual Sync-block construction path
            // (build_sync_block → reth-evm BlockBuilder). Same chain spec
            // the engine uses, so blocks produced here pass validation
            // when reth ingests them via newPayload.
            let evm_config = reth_evm_ethereum::EthEvmConfig::new(chain_spec.clone());

            // Build the cross-chain `EvmComposer` when the embedded L1 is
            // up — it owns `LocalChainClient`s over L1 (entry) and L2
            // (follower). `None` without an embedded L1. Inlined because
            // the `FullNode` AddOns type resists a typed helper return.
            // L2 ENTRY client for OUTBOUND (L2→L1) source-sim — built inside the
            // block below alongside the L2 follower, threaded into `Composer::new`.
            // `None` without an embedded L1 (outbound txs then evict at compose).
            let mut l2_entry_client: Option<
                Arc<
                    dyn eez_protocol::executor::EntryChainClient<Protocol = eez_evm::EvmProtocol>
                        + Send
                        + Sync,
                >,
            > = None;
            let evm_composer: Option<eez_evm_inspector::EvmComposer> =
                if let Some(l1_variant) = embedded_l1.as_ref() {
                    use eez_composer::{GnosisL1Adapter, LocalChainClient};
                    use eez_evm::EvmProtocol;
                    use eez_evm_inspector::{
                        DEFAULT_CCM_GAS_LIMIT, EvmTargetConfig, ProxyLookupConfig,
                    };
                    use eez_protocol::Composer as ProtocolComposer;
                    use eez_protocol::rollup_id::RollupId;

                    let eez_registry: Address = Address::from_str(&env::var("EEZ_REGISTRY_ADDRESS").map_err(
                        |_| eyre::eyre!("EEZ_REGISTRY_ADDRESS required for EvmComposer (set by deploy.sh)"),
                    )?)?;
                    let ccm_l2: Address = Address::from_str(&env::var("EEZ_CCM_L2_ADDRESS").map_err(
                        |_| eyre::eyre!("EEZ_CCM_L2_ADDRESS required for EvmComposer (set by deploy.sh)"),
                    )?)?;
                    let l2_system_address: Address = env::var("EEZ_L2_SYSTEM_ADDRESS")
                        .ok()
                        .and_then(|s| Address::from_str(&s).ok())
                        .unwrap_or(Address::ZERO);
                    let l1_rollup_id_u64 = read_l1_rollup_id();
                    let l1_rollup_id = RollupId(l1_rollup_id_u64);
                    let l2_rollup_id_typed = RollupId(rollup_id);

                    // L1 entry differs per kind: Dev uses the native
                    // provider + EvmConfig; Chiado wraps it in
                    // `GnosisL1Adapter` and builds a fresh `EthEvmConfig`
                    // over the chiado ChainSpec (source-sim needs only
                    // revm, not GnosisNode's AuRa paths). Both yield the
                    // same erased views, so composition is identical.
                    let (entry_client_view, root_reader_view) = match l1_variant {
                        EmbeddedL1::Dev(l1_handle) => {
                            let l1_chain_spec = l1_handle.node.chain_spec();
                            let l1_provider = l1_handle.node.provider.clone();
                            let l1_evm_config = l1_handle.node.evm_config.clone();
                            let entry_client = LocalChainClient::new_entry(
                                l1_provider,
                                l1_evm_config,
                                l1_chain_spec,
                                l1_rollup_id,
                                eez_registry,
                                eez_registry,
                                eez_evm::ChainDialect::EvmL1Style,
                            );
                            let entry_view: std::sync::Arc<
                                dyn eez_protocol::executor::EntryChainClient<Protocol = EvmProtocol>
                                    + Send
                                    + Sync,
                            > = entry_client.clone();
                            let root_view: std::sync::Arc<
                                dyn eez_protocol::executor::CommittedRootReader<Protocol = EvmProtocol>
                                    + Send
                                    + Sync,
                            > = entry_client.clone();
                            (entry_view, root_view)
                        }
                        EmbeddedL1::Chiado(chiado_handle) => {
                            // `GnosisChainSpec.inner` is the standard
                            // reth `ChainSpec` (via `#[deref]`); wrap it
                            // fresh as `Arc<ChainSpec>` for both the
                            // LocalChainClient chain_spec slot and the
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
                                l1_chain_spec,
                                l1_rollup_id,
                                eez_registry,
                                eez_registry,
                                eez_evm::ChainDialect::EvmL1Style,
                            );
                            let entry_view: std::sync::Arc<
                                dyn eez_protocol::executor::EntryChainClient<Protocol = EvmProtocol>
                                    + Send
                                    + Sync,
                            > = entry_client.clone();
                            let root_view: std::sync::Arc<
                                dyn eez_protocol::executor::CommittedRootReader<Protocol = EvmProtocol>
                                    + Send
                                    + Sync,
                            > = entry_client.clone();
                            (entry_view, root_view)
                        }
                    };

                    // L2 follower — EvmL2Style. `dispatch_address` =
                    // `ccm_address` = CCM-L2 predeploy (where
                    // authorizedProxies lives at slot 2).
                    let l2_follower = LocalChainClient::new_follower(
                        provider.clone(),
                        evm_config.clone(),
                        chain_spec.clone(),
                        l2_rollup_id_typed,
                        ccm_l2,
                        ccm_l2,
                        eez_evm::ChainDialect::EvmL2Style,
                    );
                    let l2_follower_view: std::sync::Arc<
                        dyn eez_protocol::executor::ChainClient<Protocol = EvmProtocol>
                            + Send
                            + Sync,
                    > = l2_follower;

                    // L2 ENTRY client (follower's provider/dialect, but
                    // Role::Entry) — the follower client errors `Unavailable` for
                    // the outbound source-sim `simulate_and_resolve_recorded_for`.
                    let l2_entry = LocalChainClient::new_entry(
                        provider.clone(),
                        evm_config.clone(),
                        chain_spec.clone(),
                        l2_rollup_id_typed,
                        ccm_l2,
                        ccm_l2,
                        eez_evm::ChainDialect::EvmL2Style,
                    );
                    let l2_entry_view: std::sync::Arc<
                        dyn eez_protocol::executor::EntryChainClient<Protocol = EvmProtocol>
                            + Send
                            + Sync,
                    > = l2_entry;
                    l2_entry_client = Some(l2_entry_view);

                    let entry_cfg = EvmTargetConfig {
                        ccm_address: eez_registry,
                        system_address: Address::ZERO, // entry has no system-tx CCM path
                        ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
                        proxy_lookup: ProxyLookupConfig {
                            contract_address: eez_registry,
                            authorized_proxies_slot: eez_evm::ChainDialect::EvmL1Style
                                .proxy_lookup_slot(),
                        },
                        dialect: eez_evm::ChainDialect::EvmL1Style,
                        settles_via_session_root: false,
                    };
                    let l2_follower_cfg = EvmTargetConfig {
                        ccm_address: ccm_l2,
                        system_address: l2_system_address,
                        ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
                        proxy_lookup: ProxyLookupConfig {
                            contract_address: ccm_l2,
                            authorized_proxies_slot: eez_evm::ChainDialect::EvmL2Style
                                .proxy_lookup_slot(),
                        },
                        dialect: eez_evm::ChainDialect::EvmL2Style,
                        settles_via_session_root: false,
                    };

                    let composed = ProtocolComposer::<EvmProtocol>::builder(
                        EvmProtocol,
                        l1_rollup_id,
                    )
                    .entry(entry_client_view, entry_cfg)
                    .root_reader(root_reader_view)
                    .rollup(l2_rollup_id_typed, l2_follower_view, l2_follower_cfg)
                    .build()
                    .map_err(|e| eyre::eyre!("EvmComposer build failed: {e}"))?;
                    event!(
                        name: "eez.node.evm_composer.ready",
                        Level::INFO,
                        l1_rollup_id = l1_rollup_id_u64,
                        l2_rollup_id = rollup_id,
                        eez_registry = %eez_registry,
                        ccm_l2 = %ccm_l2,
                        "cross-chain EvmComposer constructed (L1 entry + L2 follower)",
                    );
                    Some(composed)
                } else {
                    event!(
                        name: "eez.node.evm_composer.skipped",
                        Level::WARN,
                        "embedded L1 not active; cross-chain EvmComposer disabled",
                    );
                    None
                };

            // CrossChainExecCtx: signer + L2 addresses needed to wrap
            // EvmComposer's `(load_table, execute)` calldata pairs
            // into signed legacy L2 system txs at Sync-slot time.
            // Constructed only when EvmComposer is constructed —
            // both are tied to embedded L1 mode.
            let cc_exec_ctx: Option<Arc<eez_composer::CrossChainExecCtx>> = if evm_composer
                .is_some()
            {
                let system_key = env::var("EEZ_L2_SYSTEM_KEY").map_err(|_| {
                    eyre::eyre!("EEZ_L2_SYSTEM_KEY required when EvmComposer is wired")
                })?;
                let system_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                    system_key.trim_start_matches("0x"),
                )?)?;
                let ccm_l2_address: Address = Address::from_str(
                    &env::var("EEZ_CCM_L2_ADDRESS").map_err(|_| {
                        eyre::eyre!("EEZ_CCM_L2_ADDRESS required (set by deploy.sh)")
                    })?,
                )?;
                // L1 RPC URL: the embedded L1's HTTP port (so the
                // composer's L1-forwarding round-trips back into our
                // own L1 reth). Same value as `EEZ_L1_RPC_URL` for
                // embedded mode.
                let l1_rpc_url: reqwest::Url = env::var("EEZ_L1_RPC_URL")
                    .map_err(|_| eyre::eyre!("EEZ_L1_RPC_URL required for L1 forwarding"))?
                    .parse()
                    .map_err(|e| eyre::eyre!("EEZ_L1_RPC_URL malformed: {e}"))?;
                let l1_provider = alloy_provider::RootProvider::new_http(l1_rpc_url.clone());
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
                let l2_rollup_id_for_ctx: u64 = env::var("EEZ_ROLLUP_ID")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        eyre::eyre!("EEZ_ROLLUP_ID required for L1 postBatch rollupIdsWithProofSystems[]")
                    })?;
                // L1 chainId queried from the provider would be more
                // robust, but pulling it out of env keeps the ctx
                // construction sync. Embedded reth `--chain dev` is
                // chainId=1337; smoke harness sets this explicitly.
                let l1_chain_id: u64 = env::var("EEZ_L1_CHAIN_ID")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1337);
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
                Some(Arc::new(eez_composer::CrossChainExecCtx {
                    system_signer,
                    ccm_l2_address,
                    l2_chain_id: chain_spec.chain().id(),
                    l2_gas_price: 1_000_000_000,
                    l2_gas_limit: 2_000_000,
                    l1_provider,
                    submitter: submitter.clone(),
                    l1_poster_signer,
                    l1_chain_id,
                    l1_post_batch_priority_fee,
                    ecdsa_proof_system_address,
                    l2_rollup_id: l2_rollup_id_for_ctx,
                }))
            } else {
                None
            };
            // Project the Arc<CrossChainExecCtx> into a SystemTxContext
            // BEFORE moving it into the Composer. The Deriver picks
            // this up further down to STF-reconstruct the same L2
            // system txs the composer produced.
            let deriver_system_tx_cfg = cc_exec_ctx.as_ref().map(|ctx| {
                eez_evm::system_tx::SystemTxContext {
                    system_signer: ctx.system_signer.clone(),
                    ccm_l2_address: ctx.ccm_l2_address,
                    l2_chain_id: ctx.l2_chain_id,
                    l2_gas_price: ctx.l2_gas_price,
                    l2_gas_limit: ctx.l2_gas_limit,
                    this_rollup_id: ctx.l2_rollup_id,
                }
            });
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
                l1_watcher.clone(),
                evm_config,
                evm_composer,
                cc_exec_ctx,
                l2_entry_client,
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
            (Some(sequencer), Some(composer), deriver_system_tx_cfg)
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
            (None, None, follower_system_tx_cfg)
        };

        // Deriver: drives BlockCommitter from L1Events (follower +
        // composer). A wired `SystemTxContext` makes it reconstruct the
        // same L2 system txs the composer produced (single-source STF).
        let l2_block_time_secs = timing.l2_block_time().as_secs();
        let deriver = Deriver::new(
            l1_watcher.clone(),
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
        loop {
            match deriver.catch_up().await {
                Ok(()) => break,
                Err(err) if err.is_source_incomplete() => {
                    catch_up_attempts += 1;
                    event!(
                        name: "eez.node.deriver.boot_catch_up.source_incomplete",
                        Level::WARN,
                        mode = mode.name(),
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
                        mode = mode.name(),
                        error = %err,
                        "boot-time catch_up failed; refusing to start L1-active tasks before reconciliation",
                    );
                    return Err(eyre::eyre!("boot-time deriver catch_up failed: {err}"));
                }
            }
        }
        event!(
            name: "eez.node.deriver.spawned",
            Level::INFO,
            mode = mode.name(),
            initial_posted_through = deriver.cursor(),
            "spawning eez deriver",
        );
        let deriver_run = deriver.clone();
        task_executor.spawn_critical_task("eez-deriver", async move {
            deriver_run.run().await;
        });

        // Follower-only: optional sequencer-RPC unsafe-head overlay.
        // L1 replay always boots first, so safe/finalized anchors are
        // reconciled before the overlay can move unsafe head.
        if mode == Mode::Follower {
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
        if let (Some(sequencer), Some(composer)) = (sequencer, umbrella) {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "composer", "spawning eez sequencer (L1-anchored)");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });

            event!(name: "eez.node.composer.spawned", Level::INFO, "spawning eez composer umbrella");
            task_executor.spawn_critical_task("eez-composer", async move {
                composer.run().await;
            });

            // Cross-chain ingress fronts (see `run_cross_chain_front`) — one per
            // SOURCE chain, sharing `held_pool`, gated on its port env (absent →
            // no front for that chain):
            //   L1 front (EEZ_L1_XCHAIN_PORT → EEZ_L1_RPC_URL): L1→L2 Inbound.
            //   L2 front (EEZ_L2_XCHAIN_PORT → EEZ_L2_RPC_URL): L2→L1 Outbound.
            for (port_env, url_env, direction, task) in [
                (
                    "EEZ_L1_XCHAIN_PORT",
                    "EEZ_L1_RPC_URL",
                    eez_composer::Direction::Inbound,
                    "eez-l1-xchain-front",
                ),
                (
                    "EEZ_L2_XCHAIN_PORT",
                    "EEZ_L2_RPC_URL",
                    eez_composer::Direction::Outbound,
                    "eez-l2-xchain-front",
                ),
            ] {
                let Some(port) = env::var(port_env).ok().and_then(|p| p.parse::<u16>().ok()) else {
                    continue;
                };
                let Ok(url) = env::var(url_env) else {
                    event!(name: "eez.xchain_front.no_upstream", Level::WARN, port_env, url_env, "cross-chain front port set but no upstream RPC; skipping");
                    continue;
                };
                let Ok(parsed) = url.parse::<reqwest::Url>() else {
                    event!(name: "eez.xchain_front.bad_upstream", Level::WARN, %url, "cross-chain front upstream RPC malformed; skipping");
                    continue;
                };
                let pool = Arc::clone(&held_pool);
                let provider = alloy_provider::RootProvider::new_http(parsed);
                task_executor.spawn_critical_task(task, async move {
                    if let Err(e) =
                        ingress::run_cross_chain_front(port, url, direction, pool, provider).await
                    {
                        event!(name: "eez.xchain_front.exited", Level::ERROR, error = %e, "cross-chain front exited");
                    }
                });
            }
        }

        handle.wait_for_node_exit().await
    })
}

/// Build a `SystemTxContext` for follower mode from env (the follower
/// has no Composer to feed the composer-mode projection). Returns
/// `Ok(None)` when cross-chain env isn't present → pure-user-tx follower
/// mode. Reads `EEZ_L2_SYSTEM_KEY` / `EEZ_CCM_L2_ADDRESS` /
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
) -> eyre::Result<Option<eez_evm::system_tx::SystemTxContext>>
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
    let ccm_l2_str = env::var("EEZ_CCM_L2_ADDRESS")
        .map_err(|_| eyre::eyre!("EEZ_CCM_L2_ADDRESS required when EEZ_L2_SYSTEM_KEY is set"))?;
    let rollup_id_str = env::var("EEZ_ROLLUP_ID")
        .map_err(|_| eyre::eyre!("EEZ_ROLLUP_ID required when EEZ_L2_SYSTEM_KEY is set"))?;

    let system_signer =
        PrivateKeySigner::from_bytes(&B256::from_str(system_key.trim_start_matches("0x"))?)?;
    let ccm_l2_address: Address = Address::from_str(&ccm_l2_str)?;
    let this_rollup_id: u64 = rollup_id_str
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_ROLLUP_ID malformed: {e}"))?;

    Ok(Some(eez_evm::system_tx::SystemTxContext {
        system_signer,
        ccm_l2_address,
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

/// Build the [`EmbeddedL1Config`] from env; all vars optional, with dev
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

    // L1 chain selector: `dev` (vanilla EthereumNode, auto-mine 5s)
    // vs `chiado` (reth_gnosis::GnosisNode, real chiado state). The
    // `dev_chain_spec` is only consumed by the dev path.
    let kind = l1_embedded::L1ChainKind::from_env();

    // JWT secret path — required for chiado (lighthouse engine API
    // auth); optional for dev mode (no external CL).
    let jwtsecret = env::var("EEZ_L1_JWT_SECRET")
        .ok()
        .map(std::path::PathBuf::from);

    Ok(l1_embedded::EmbeddedL1Config {
        dev_chain_spec,
        kind,
        datadir,
        http_port,
        auth_port,
        p2p_port,
        discv5_port,
        jwtsecret,
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
                "env var is ignored; mode is derived from EEZ_L1_RPC_URL + (EEZ_PROVER_URL | EEZ_PROOF_SIGNER_KEY) presence (see crate docs)."
            );
        }
    }
}
