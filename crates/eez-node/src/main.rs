//! eez Rollup-0 node binary.
//!
//! Wraps reth with our composer stack: reth provides the EVM, storage,
//! networking, RPC, and engine; we provide block production
//! (Sequencer + Scheduler), L1 anchoring (`L1Watcher` + Deriver), and
//! batch submission (Composer umbrella).
//!
//! # Modes
//!
//! Mode is decided by env-var presence at startup:
//!
//! | `EEZ_L1_RPC_URL` | `EEZ_PROOF_SIGNER_KEY` | Mode | Stack |
//! |---|---|---|---|
//! | unset | — | **standalone** | reth + Sequencer (interval Scheduler, Live blocks only) |
//! | set | unset | **follower** | reth + `L1Watcher` + Deriver (no Sequencer) |
//! | set | set | **composer** | reth + `L1Watcher` + Deriver + Sequencer (L1-anchored) + Composer umbrella |
//!
//! Replaces the old `EEZ_SEQUENCER_DISABLED` / `EEZ_COMPOSER_DISABLED`
//! flags — mode is now implicit from which credentials are configured.

mod ingress;

use std::{collections::HashMap, env, str::FromStr, sync::Arc};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use eez_composer::{Composer, HeldPool, IngressClassifier, RollupConfig, RollupState};
use eez_deriver::Deriver;
use eez_driver::{
    BatchCandidate, BatchPolicy, EthAttributesBuilder, RollupTiming, Sequencer, SlotEvent,
    SyncSlotComposerHandle, spawn_interval, spawn_l1_anchored,
};
use eez_l1::{
    L1CanonicalHead, L1HeadStream, L1Watcher, L1WatcherConfig, Submitter, SubmitterConfig,
};
use eez_prover::MockEcdsaProver;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_ethereum::EthereumNode;
use tokio::sync::mpsc;
use tracing::{Level, event};

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Sequencer → Composer batch-candidate channel capacity. Small fixed
/// queue: the produce loop applies backpressure if the Composer falls
/// behind (slow down rather than drop).
const BATCH_CANDIDATE_CHAN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Standalone,
    Follower,
    Composer,
}

impl Mode {
    fn from_env() -> Self {
        let l1_enabled = env::var_os("EEZ_L1_RPC_URL").is_some();
        let proof_signer_set = env::var_os("EEZ_PROOF_SIGNER_KEY").is_some();
        match (l1_enabled, proof_signer_set) {
            (false, _) => Self::Standalone,
            (true, false) => Self::Follower,
            (true, true) => Self::Composer,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Follower => "follower",
            Self::Composer => "composer",
        }
    }
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

    let mode = Mode::from_env();

    Cli::<EthereumChainSpecParser>::parse_args().run(async move |builder, _ext| {
        event!(
            name: "eez.node.launching",
            Level::INFO,
            mode = mode.name(),
            "launching eez-node",
        );

        warn_on_deprecated_env();

        // Construct the per-rollup HeldPool + IngressClassifier
        // BEFORE reth launches: they're attached to reth's RPC layer
        // as an `eth_sendRawTransaction` middleware (`ingress::IngressLayer`).
        // The same `held_pool` Arc is shared with the umbrella's
        // RollupState in composer mode so the middleware's pushes and
        // the Sync-slot drain see the same queue.
        //
        // Live in all modes: in follower/standalone the classifier is
        // empty (no proxies registered), so the middleware passes
        // every tx through to reth's pool — zero behavior change.
        let held_pool = Arc::new(HeldPool::new());
        let classifier: Arc<IngressClassifier> = Arc::new(
            parse_cross_chain_proxy_env().into_iter().collect(),
        );
        if !classifier.is_empty() {
            event!(
                name: "eez.node.ingress.classifier",
                Level::INFO,
                proxy_count = classifier.len(),
                "ingress classifier configured with cross-chain proxy addresses",
            );
        }

        // Launch reth with the IngressLayer attached to its RPC stack.
        let handle = builder
            .with_types::<EthereumNode>()
            .with_components(EthereumNode::components())
            .with_add_ons(
                reth_node_ethereum::node::EthereumAddOns::default()
                    .with_rpc_middleware(ingress::IngressLayer::new(
                        Arc::clone(&held_pool),
                        Arc::clone(&classifier),
                    )),
            )
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
        let (schedule_rx, batch_rx, batch_tx) = match mode {
            Mode::Standalone => {
                let rx = spawn_interval(timing.l2_block_time());
                (rx, None, None)
            }
            Mode::Follower => {
                // Dummy channel; Sequencer is constructed (to spawn
                // BlockCommitter) but never .run(). Holding the sender
                // here keeps the receiver from closing immediately.
                let (_tx, rx) = mpsc::channel::<SlotEvent>(1);
                (rx, None, None)
            }
            Mode::Composer => {
                let submitter_config = SubmitterConfig::from_env()?;
                let _ = submitter_config; // validated by Composer block below
                let l1_watcher_config_preview = L1WatcherConfig::from_env()?;
                let _ = l1_watcher_config_preview; // validated below
                // L1-anchored Scheduler needs the L1Watcher handle —
                // built inside the composer arm below where we
                // construct submitter/l1_watcher together.
                let (bt, br) = mpsc::channel::<BatchCandidate>(BATCH_CANDIDATE_CHAN);
                // Placeholder for schedule_rx; replaced inside composer arm
                let (_drop_tx, drop_rx) = mpsc::channel::<SlotEvent>(1);
                (drop_rx, Some(br), Some(bt))
            }
        };

        // Sequencer is constructed in all modes so its `BlockCommitter`
        // actor is available for the Deriver (follower / composer)
        // and so its receiver-side schedule channel is wired up. In
        // standalone mode it runs the produce loop; in follower it's
        // dropped (committer stays alive via the cloned handle); in
        // composer the L1-anchored schedule arrives via spawn_l1_anchored.
        let mut sequencer = Sequencer::new(
            &provider,
            attributes,
            beacon_engine_handle,
            schedule_rx,
            payload_builder_handle,
            timing,
        )?;
        if mode != Mode::Standalone {
            sequencer = sequencer.with_speculative_limit(64, Arc::clone(&l1_head) as _);
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
        let submitter_config = SubmitterConfig::from_env()?;
        let rollup_config = RollupConfig::from_env()?;
        let l1_watcher_config = L1WatcherConfig::from_env()?;

        let submitter = Submitter::new(submitter_config);
        let l1_watcher = L1Watcher::spawn(l1_watcher_config);

        // ─── Composer-only: build the umbrella first, then rebuild ────
        // Sequencer with L1-anchored schedule + batch_emitter + the
        // umbrella attached as SyncSlotComposer. The placeholder
        // Sequencer above was wired with a dummy receiver to keep types
        // simple at the build site; for composer mode we drop and
        // rebuild now that the L1Watcher exists. Building the umbrella
        // first lets us pass it to the Sequencer for per-Sync-slot
        // cross-chain-content drain.
        let (sequencer, batch_rx, umbrella) = if mode == Mode::Composer {
            drop(sequencer);

            // Umbrella construction first. The rollup state owns its
            // HeldPool (S4.7 drain target).
            let proof_signer_key = env::var("EEZ_PROOF_SIGNER_KEY")
                .map_err(|_| eyre::eyre!("EEZ_PROOF_SIGNER_KEY required in composer mode"))?;
            let proof_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                proof_signer_key.trim_start_matches("0x"),
            )?)?;
            let prover = Arc::new(MockEcdsaProver::new(proof_signer));
            let rollup_id = rollup_config.rollup_id;
            // Share the SAME HeldPool the ingress middleware pushes
            // into. RollupState's `Option<Arc<HeldPool>>` wraps the
            // shared Arc so multi-rollup deployments later can keep
            // per-rollup pools while still sharing one ingress layer.
            let held_pool_for_rollup = Some(Arc::clone(&held_pool));
            let rollup_state = RollupState {
                config: rollup_config.clone(),
                timing,
                l2_provider: Arc::new(provider.clone()),
                l1_head: Arc::clone(&l1_head),
                held_pool: held_pool_for_rollup,
            };
            let mut rollups = HashMap::with_capacity(1);
            rollups.insert(rollup_id, rollup_state);
            let composer = Composer::new(rollups, prover, submitter.clone(), l1_watcher.clone());
            let sync_slot_handle: SyncSlotComposerHandle = Arc::new(composer.clone());

            // Sequencer with all hooks: speculative-depth, batch
            // emitter, sync-slot composer.
            let attributes = EthAttributesBuilder::new(chain_spec.clone());
            let schedule_rx = spawn_l1_anchored(
                L1HeadStream::from_watcher(&l1_watcher),
                timing,
                l2_genesis_timestamp,
            );
            let batch_tx = batch_tx.expect("composer mode: channel built above");
            let batch_rx = batch_rx.expect("composer mode: channel built above");
            let sequencer = Sequencer::new(
                &provider,
                attributes,
                handle.node.add_ons_handle.beacon_engine_handle.clone(),
                schedule_rx,
                handle.node.payload_builder_handle.clone(),
                timing,
            )?
            .with_speculative_limit(64, Arc::clone(&l1_head) as _)
            .with_batch_emitter(
                rollup_id,
                BatchPolicy::EveryKBlocks(u64::from(timing.k())),
                batch_tx,
            )
            .with_sync_slot_composer(sync_slot_handle);
            (Some(sequencer), Some(batch_rx), Some(composer))
        } else {
            // Follower: drop the placeholder Sequencer (BlockCommitter
            // stays alive via the cloned handle held below).
            drop(sequencer);
            (None, None, None)
        };

        // Deriver: drives BlockCommitter from L1Events. Active in both
        // follower and composer modes.
        let deriver = Deriver::new(
            l1_watcher.clone(),
            block_committer,
            Arc::new(provider.clone()),
            submitter.clone(),
            chain_spec,
            rollup_config.deploy_block,
            Arc::clone(&l1_head),
        );

        if let Err(err) = deriver.catch_up().await {
            event!(
                name: "eez.node.deriver.boot_catch_up.failed",
                Level::WARN,
                error = %err,
                "boot-time catch_up failed; deriver.run() will retry post-subscribe",
            );
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

        // ─── Composer-only: spawn Sequencer + umbrella ───────────────
        if let (Some(sequencer), Some(batch_rx), Some(composer)) =
            (sequencer, batch_rx, umbrella)
        {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, mode = "composer", "spawning eez sequencer (L1-anchored)");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });

            event!(name: "eez.node.composer.spawned", Level::INFO, "spawning eez composer umbrella");
            task_executor.spawn_critical_task("eez-composer", async move {
                composer.run(batch_rx).await;
            });
        }

        handle.wait_for_node_exit().await
    })
}

/// Parse `EEZ_CROSS_CHAIN_PROXY_ADDRESSES` (comma-separated hex
/// addresses) into a `Vec<Address>`. Empty / unset / malformed → empty
/// vec (the ingress classifier then becomes a passthrough).
fn parse_cross_chain_proxy_env() -> Vec<Address> {
    let Ok(raw) = env::var("EEZ_CROSS_CHAIN_PROXY_ADDRESSES") else {
        return Vec::new();
    };
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| Address::from_str(s).ok())
        .collect()
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
                "env var is ignored from S4.2 onward; mode is now derived from EEZ_L1_RPC_URL + EEZ_PROOF_SIGNER_KEY presence (see crate docs)."
            );
        }
    }
}
