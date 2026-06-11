//! eez Rollup-0 follower node binary.
//!
//! The follower uses PR #5's L1 derivation path for safe/finalized
//! correctness. Sequencer RPC, when configured, is only an unsafe-head
//! source; L1-latest/finalized checkpoints are hash-bearing blocks derived
//! from posted batches and applied through the shared `BlockCommitter`.

mod error;
mod follower;

use std::{sync::Arc, time::Duration};

use alloy_provider::RootProvider;
use clap::Parser as _;
use eez_deriver::Deriver;
use eez_driver::{BlockCommitterHandle, Scheduler};
use eez_l1::{
    L1CanonicalHead, L1Watcher, L1WatcherConfig, Submitter, SubmitterConfig,
    registry_deploy_block_from_env,
};
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

use crate::follower::Follower;

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// L2 block cadence for Rollup-0. Must match the sequencer's value.
const BLOCK_TIME: Duration = Duration::from_secs(2);

/// Follower-specific CLI arguments, layered on top of reth's `Cli` via the
/// `Ext` typestate.
#[derive(clap::Args, Debug, Clone)]
struct FollowerExt {
    /// Sequencer JSON-RPC URL. When omitted, the node runs as an
    /// L1-derived-only follower and head advances only when L1-latest
    /// batches are derived.
    #[arg(long, env = "EEZ_SEQUENCER_RPC")]
    sequencer_rpc: Option<url::Url>,
}

fn main() -> eyre::Result<()> {
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("deployments.env");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    Cli::<EthereumChainSpecParser, FollowerExt>::parse().run(async move |builder, ext| {
        let l1_watcher_config = L1WatcherConfig::from_env()?;
        let submitter_config = SubmitterConfig::from_env_read_only()?;
        let deploy_block = registry_deploy_block_from_env()?;

        event!(
            name: "eez.follower.launching",
            Level::INFO,
            block_time.secs = BLOCK_TIME.as_secs(),
            sequencer_rpc = ext.sequencer_rpc.as_ref().map_or("<disabled>", url::Url::as_str),
            l1_rpc = %l1_watcher_config.rpc_url,
            "launching eez-follower",
        );

        let handle = builder.launch_node(EthereumNode::default()).await?;

        let chain_spec = handle.node.chain_spec();
        let provider = handle.node.provider.clone();
        let payload_builder_handle = handle.node.payload_builder_handle.clone();
        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();

        // Boot forkchoice anchors come from reth's persisted
        // safe/finalized (genesis on a fresh chain) — never the
        // speculative best header, which L1-derived replays can
        // displace.
        let block_committer = BlockCommitterHandle::<EthEngineTypes>::spawn_from_provider(
            &provider,
            beacon_engine_handle,
            payload_builder_handle,
        )?;

        let l1_head = Arc::new(L1CanonicalHead::default());
        let submitter = Submitter::new(submitter_config);
        let l1_watcher = L1Watcher::spawn(l1_watcher_config);
        let deriver = Deriver::new(
            l1_watcher,
            block_committer.clone(),
            Arc::new(provider.clone()),
            submitter,
            chain_spec,
            deploy_block,
            Arc::clone(&l1_head),
        );

        if let Err(err) = deriver.catch_up().await {
            event!(
                name: "eez.follower.deriver.boot_catch_up.failed",
                Level::WARN,
                error = %err,
                "boot-time catch_up failed; deriver.run() will retry post-subscribe",
            );
        }

        let deriver_run = deriver.clone();
        task_executor.spawn_critical_task("eez-follower-deriver", async move {
            deriver_run.run().await;
        });

        if let Some(sequencer_rpc) = ext.sequencer_rpc {
            let sequencer_rpc = RootProvider::new_http(sequencer_rpc);
            let scheduler = Scheduler::interval(BLOCK_TIME);
            let follower = Follower::new(block_committer, sequencer_rpc, scheduler);
            event!(
                name: "eez.follower.sequencer_rpc.spawned",
                Level::INFO,
                "spawning sequencer-RPC unsafe-head follower",
            );
            task_executor.spawn_critical_task("eez-follower-unsafe-head", async move {
                follower.run().await;
            });
        } else {
            event!(
                name: "eez.follower.l1_derived_only",
                Level::INFO,
                "EEZ_SEQUENCER_RPC not set; running L1-derived-only follower",
            );
        }

        handle.wait_for_node_exit().await
    })
}
