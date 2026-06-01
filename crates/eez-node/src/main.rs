//! eez Rollup-0 node binary.
//!
//! Wraps reth with our [`Sequencer`](eez_driver::Sequencer): reth provides the
//! EVM, storage, networking, RPC, and engine; we provide the block-production
//! schedule and engine-API driver loop. The L1 stack
//! ([`L1Watcher`](eez_l1::L1Watcher), [`Deriver`](eez_deriver::Deriver),
//! [`Composer`](eez_l1::Composer)) spins up when `EEZ_L1_RPC_URL` is set.
//!
//! ## Env knobs that affect what runs
//!
//! - `EEZ_L1_RPC_URL` (and friends) — present → L1 stack spawns.
//! - `EEZ_COMPOSER_DISABLED` — when set, no postBatch tx leaves this
//!   node. Sequencer + Deriver still run (local block production + L1
//!   replay).
//! - `EEZ_SEQUENCER_DISABLED` — when set, no local block production.
//!   `BlockCommitter` still spawns (Deriver needs it for engine-API
//!   replay), but the per-tick FCU+attrs loop is suppressed. Pair with
//!   `EEZ_COMPOSER_DISABLED` for a clean follower.

use std::{env, str::FromStr, sync::Arc};

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use eez_deriver::Deriver;
use eez_driver::{
    BatchCandidate, BatchPolicy, EthAttributesBuilder, RollupTiming, Sequencer, scheduler,
};
use eez_l1::{
    Composer, ComposerConfig, L1CanonicalHead, L1Watcher, L1WatcherConfig, Submitter,
    SubmitterConfig,
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
/// behind, which is the desired behavior (slow down rather than drop).
const BATCH_CANDIDATE_CHAN: usize = 16;

/// L2 blocks per Sequencer-side batch window for stage 4.
///
/// 30 blocks × 2 s = 60 s — preserves the current `EEZ_COMPOSER_INTERVAL_SECS`
/// cadence so smoke tests stay stable. S4.2's `RollupTiming` makes this
/// env-driven and ties it to `L1_block_time / L2_block_time`; until
/// then it's a single named constant.
const BATCH_BLOCK_WINDOW: u64 = 30;

// Bootstrap wiring is linear; splitting it across helpers fragments the
// dependency chain without making it easier to read. The clippy.toml
// threshold catches genuinely sprawling logic — main() is the
// exception.
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

    Cli::<EthereumChainSpecParser>::parse_args().run(async move |builder, _ext| {
        event!(
            name: "eez.node.launching",
            Level::INFO,
            "launching eez-node (timing config logged after env reads)",
        );

        // Launch reth with all default Ethereum components.
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let chain_spec: Arc<_> = handle.node.chain_spec();
        let provider = handle.node.provider.clone();
        let payload_builder_handle = handle.node.payload_builder_handle.clone();
        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();

        // Stage-4 cadence-source change: warn loudly if a deployment
        // still sets the stage-2 polling knob so operators notice their
        // setting is now a no-op rather than silently lose
        // configurability (invariant 7).
        if env::var_os("EEZ_COMPOSER_INTERVAL_SECS").is_some() {
            event!(
                name: "eez.node.env.deprecated",
                Level::WARN,
                env = "EEZ_COMPOSER_INTERVAL_SECS",
                "EEZ_COMPOSER_INTERVAL_SECS is ignored from stage 4 onward; submission cadence is now Sequencer-driven (BatchPolicy::EveryKBlocks via the BatchCandidate channel — see docs/plans/IMPLEMENTATION.md §5.4.1). Remove from env to silence this warning."
            );
        }

        // Shared L1-confirmed L2 head. Created unconditionally so the
        // Sequencer's speculative-depth limit can be wired
        // even when the L1 stack isn't activated this run
        // When the L1 stack does spin up, the same Arc is
        // handed to the Deriver as the write side.
        let l1_head = Arc::new(L1CanonicalHead::default());
        let l1_enabled = env::var_os("EEZ_L1_RPC_URL").is_some();
        let sequencer_disabled = env::var_os("EEZ_SEQUENCER_DISABLED").is_some();
        // Composer needs a live Sequencer feeding it BatchCandidates;
        // auto-couple the disable flags to keep the channel from
        // closing under a half-running stack.
        let composer_disabled =
            env::var_os("EEZ_COMPOSER_DISABLED").is_some() || sequencer_disabled;
        if sequencer_disabled && env::var_os("EEZ_COMPOSER_DISABLED").is_none() {
            event!(
                name: "eez.node.composer.auto_disabled",
                Level::WARN,
                "EEZ_SEQUENCER_DISABLED set; auto-disabling Composer (no Sequencer to feed BatchCandidates)",
            );
        }

        // Sequencer-side ComposerConfig hoist: rollup_id is needed at
        // Sequencer-builder time to tag emitted BatchCandidates.
        let composer_config = if l1_enabled {
            Some(ComposerConfig::from_env()?)
        } else {
            None
        };

        // Build the BatchCandidate channel only when both ends will run.
        let (batch_tx, batch_rx) = if l1_enabled && !composer_disabled {
            let (tx, rx) = mpsc::channel::<BatchCandidate>(BATCH_CANDIDATE_CHAN);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // ─── Sequencer (always-on; produce loop suppressed by env) ───
        // Stage-4 step S4.2: Sequencer takes `RollupTiming` + a
        // `Receiver<ScheduleEvent>`. For now both standalone and
        // L1-enabled modes use the interval scheduler (functional
        // regression: L1-enabled mode produces Live-only blocks at
        // L2_block_time cadence, no sync slots — matches pre-S4.2
        // behavior). The L1-anchored Scheduler lands in `eez-composer`
        // as part of the umbrella extraction; eez-node will branch on
        // `l1_enabled` at that point.
        let timing = if l1_enabled {
            RollupTiming::from_env()?
        } else {
            event!(
                name: "eez.node.timing.standalone_default",
                Level::INFO,
                "no L1 stack; using standalone-dev RollupTiming defaults (L2=2s)",
            );
            RollupTiming::standalone_default()
        };
        let attributes = EthAttributesBuilder::new(chain_spec);
        let schedule_rx = scheduler::spawn_interval(timing.l2_block_time());
        let mut sequencer = Sequencer::new(
            &provider,
            attributes,
            beacon_engine_handle,
            schedule_rx,
            payload_builder_handle,
            timing,
        )?;
        if l1_enabled {
            sequencer = sequencer.with_speculative_limit(64, Arc::clone(&l1_head) as _);
        }
        if let (Some(tx), Some(cfg)) = (batch_tx, composer_config.as_ref()) {
            sequencer = sequencer.with_batch_emitter(
                cfg.rollup_id,
                BatchPolicy::EveryKBlocks(BATCH_BLOCK_WINDOW),
                tx,
            );
        }
        let block_committer = sequencer.committer();

        if sequencer_disabled {
            event!(
                name: "eez.node.sequencer.disabled",
                Level::INFO,
                "EEZ_SEQUENCER_DISABLED set; BlockCommitter spawned but no local block production",
            );
            drop(sequencer);
        } else {
            event!(name: "eez.node.sequencer.spawned", Level::INFO, "spawning eez sequencer");
            task_executor.spawn_critical_task("eez-sequencer", async move {
                sequencer.run().await;
            });
        }

        // ─── L1 stack (opt-in via EEZ_L1_RPC_URL) ────────────────────
        if l1_enabled {
            let submitter_config = SubmitterConfig::from_env()?;
            let composer_config = composer_config.expect("hoisted above when l1_enabled");
            let l1_watcher_config = L1WatcherConfig::from_env()?;

            let submitter = Submitter::new(submitter_config);
            let l1_watcher = L1Watcher::spawn(l1_watcher_config);

            // l1_head was created earlier (shared with the Sequencer
            // via with_speculative_limit). Deriver is the sole writer.
            let deriver = Deriver::new(
                l1_watcher.clone(),
                block_committer,
                Arc::new(provider.clone()),
                submitter.clone(),
                handle.node.chain_spec(),
                composer_config.deploy_block,
                Arc::clone(&l1_head),
            );

            // Boot-time catch-up is best-effort: the state-
            // retention race can surface mid-replay against a stale
            // datadir. `Deriver::run()` retries `catch_up()` after
            // subscribing (warn-and-continue on failure), so we
            // don't bail here — log and proceed.
            if let Err(err) = deriver.catch_up().await {
                event!(
                    name: "eez.node.deriver.boot_catch_up.failed",
                    Level::WARN,
                    error = %err,
                    "boot-time catch_up failed; deriver.run() will retry post-subscribe",
                );
            }
            let initial_posted_through = deriver.cursor();

            event!(
                name: "eez.node.deriver.spawned",
                Level::INFO,
                initial_posted_through,
                "spawning eez deriver",
            );
            let deriver_run = deriver.clone();
            task_executor.spawn_critical_task("eez-deriver", async move {
                deriver_run.run().await;
            });

            // ─── Composer (opt-out via EEZ_COMPOSER_DISABLED) ────────
            if composer_disabled {
                event!(
                    name: "eez.node.composer.disabled",
                    Level::INFO,
                    "Composer disabled; this node will not post any batches",
                );
            } else {
                let batch_rx = batch_rx.expect("channel built when composer is enabled");
                let proof_signer_key = env::var("EEZ_PROOF_SIGNER_KEY").map_err(|_| {
                    eyre::eyre!("EEZ_PROOF_SIGNER_KEY required when EEZ_L1_RPC_URL set")
                })?;
                let proof_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                    proof_signer_key.trim_start_matches("0x"),
                )?)?;
                let prover = Arc::new(MockEcdsaProver::new(proof_signer));
                let composer = Composer::new(
                    composer_config,
                    prover,
                    Arc::new(provider),
                    submitter,
                    l1_watcher,
                    Arc::clone(&l1_head),
                );
                event!(name: "eez.node.composer.spawned", Level::INFO, "spawning eez composer");
                task_executor.spawn_critical_task("eez-composer", async move {
                    composer.run(batch_rx).await;
                });
            }
        } else {
            event!(
                name: "eez.node.l1_stack.skipped",
                Level::INFO,
                "EEZ_L1_RPC_URL not set; running sequencer only",
            );
        }

        handle.wait_for_node_exit().await
    })
}
