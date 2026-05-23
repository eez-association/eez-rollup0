//! eez Rollup-0 follower node binary.
//!
//! Wraps reth with a [`Follower`] task plus an [`L1Watcher`] task. Reth
//! provides the EVM, storage, P2P networking, RPC, and engine; the
//! follower drives the engine API to track the sequencer's chain
//! (unsafe head from sequencer JSON-RPC, safe/finalized from L1
//! `BatchPosted` events tailed by the L1 watcher).
//!
//! L1 is mandatory — see `l1.rs`. All `EEZ_*` env vars must be set.
//! `EEZ_SEQUENCER_RPC` is required too (or `--sequencer-rpc`).
//! P2P peering with the sequencer's reth is configured via reth's
//! own `--trusted-peers` flag.

mod error;
mod follower;
mod l1;

use std::{sync::Arc, time::Duration};

use alloy_provider::RootProvider;
use clap::Parser as _;
use eez_driver::Scheduler;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

use crate::follower::Follower;
use crate::l1::{L1Config, L1View, L1Watcher};

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// L2 block cadence for Rollup-0. Must match the sequencer's value
/// (`eez-node/src/main.rs:31`); both will move to a shared config later.
const BLOCK_TIME: Duration = Duration::from_secs(2);

/// Follower-specific CLI arguments, layered on top of reth's `Cli` via the
/// `Ext` typestate.
#[derive(clap::Args, Debug, Clone)]
struct FollowerExt {
    /// Sequencer JSON-RPC URL. Used per tick to fetch the current head hash
    /// (`eth_getBlockByNumber`); block bodies and receipts flow over P2P.
    #[arg(long, env = "EEZ_SEQUENCER_RPC")]
    sequencer_rpc: url::Url,
}

fn main() -> eyre::Result<()> {
    // Auto-load `.env` from cwd and the machine-written `deployments.env`
    // that `scripts/deploy.sh` produces — same convention as `eez-node`.
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("deployments.env");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    Cli::<EthereumChainSpecParser, FollowerExt>::parse().run(async move |builder, ext| {
        // Validate L1 config BEFORE launching reth so missing env vars
        // fail in milliseconds rather than after reth boots RPC servers.
        let l1_config = L1Config::from_env()?;

        event!(
            name: "eez.follower.launching",
            Level::INFO,
            block_time.secs = BLOCK_TIME.as_secs(),
            sequencer_rpc = %ext.sequencer_rpc,
            l1_rpc = %l1_config.rpc_url,
            "launching eez-follower with {{block_time.secs}}s block time, sequencer={{sequencer_rpc}}, l1={{l1_rpc}}",
        );

        // Launch reth with all default Ethereum components: tx pool, network,
        // payload builder, RPC, engine. Same `launch_node` (not the debug
        // variant) as eez-node — avoids racing reth's own `LocalMiner` if
        // `--dev` is set on the follower.
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();
        let l2_provider = Arc::new(handle.node.provider.clone());

        let l1_view = Arc::new(L1View::default());
        let l1_watcher =
            L1Watcher::new(l1_config, l1_view.clone(), l2_provider.clone()).await?;
        task_executor.spawn_critical_task("eez-follower-l1", async move {
            l1_watcher.run().await;
        });

        let sequencer_rpc: RootProvider = RootProvider::new_http(ext.sequencer_rpc.clone());
        let scheduler = Scheduler::interval(BLOCK_TIME);
        let follower = Follower::new(
            beacon_engine_handle,
            sequencer_rpc,
            scheduler,
            l2_provider,
            l1_view,
        )?;

        event!(
            name: "eez.follower.spawned",
            Level::INFO,
            "spawning eez follower",
        );

        task_executor.spawn_critical_task("eez-follower", async move {
            follower.run().await;
        });

        handle.wait_for_node_exit().await
    })
}
