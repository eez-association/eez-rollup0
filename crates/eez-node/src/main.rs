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

use std::{env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use eez_deriver::Deriver;
use eez_driver::{EthAttributesBuilder, Scheduler, Sequencer};
use eez_l1::{
    Composer, ComposerConfig, L1CanonicalHead, L1Watcher, L1WatcherConfig, Submitter,
    SubmitterConfig,
};
use eez_prover::MockEcdsaProver;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// L2 block cadence for Rollup-0. Pinned at 2s per Rollup-1 spec §1.3.
const BLOCK_TIME: Duration = Duration::from_secs(2);

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
            block_time.secs = BLOCK_TIME.as_secs(),
            "launching eez-node with {{block_time.secs}}s block time",
        );

        // Launch reth with all default Ethereum components.
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let chain_spec: Arc<_> = handle.node.chain_spec();
        let provider = handle.node.provider.clone();
        let payload_builder_handle = handle.node.payload_builder_handle.clone();
        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();

        // Shared L1-confirmed L2 head. Created unconditionally so the
        // Sequencer's speculative-depth limit can be wired
        // even when the L1 stack isn't activated this run
        // When the L1 stack does spin up, the same Arc is
        // handed to the Deriver as the write side.
        let l1_head = Arc::new(L1CanonicalHead::default());
        let l1_enabled = env::var_os("EEZ_L1_RPC_URL").is_some();

        // ─── Sequencer (always-on) ───────────────────────────────────
        let attributes = EthAttributesBuilder::new(chain_spec);
        let scheduler = Scheduler::interval(BLOCK_TIME);
        let mut sequencer = Sequencer::new(
            &provider,
            attributes,
            beacon_engine_handle,
            scheduler,
            payload_builder_handle,
        )?;
        if l1_enabled {
            sequencer = sequencer.with_speculative_limit(64, Arc::clone(&l1_head) as _);
        }
        let block_committer = sequencer.committer();

        if env::var_os("EEZ_SEQUENCER_DISABLED").is_some() {
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
            let composer_config = ComposerConfig::from_env()?;
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
            if env::var_os("EEZ_COMPOSER_DISABLED").is_some() {
                event!(
                    name: "eez.node.composer.disabled",
                    Level::INFO,
                    "EEZ_COMPOSER_DISABLED set; this node will not post any batches",
                );
            } else {
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
                    composer.run().await;
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
