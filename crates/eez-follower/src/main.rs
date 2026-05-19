//! eez Rollup-0 follower node binary.
//!
//! Wraps reth with a [`Follower`] task: reth provides the EVM, storage, P2P
//! networking, and RPC; we drive its engine API to track whatever chain head
//! the sequencer publishes via its standard JSON-RPC. The actual block data
//! flows over reth's devp2p (eth/68) — see `follower::Follower` docs for
//! the data-plane vs. control-plane split.
//!
//! Stage 1 hard-codes the 2-second block time (same as `eez-node`). The
//! sequencer's RPC endpoint is configured via `--sequencer-rpc` (or the
//! `EEZ_SEQUENCER_RPC` env var). P2P peering with the sequencer's reth is
//! configured via reth's own `--trusted-peers` flag.

mod error;
mod follower;

use std::time::Duration;

use alloy_provider::RootProvider;
use clap::Parser as _;
use eez_driver::Scheduler;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

use crate::follower::Follower;

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// L2 block cadence for Rollup-0. Must match the sequencer's value
/// (`eez-node/src/main.rs:27`); both will move to a shared config later.
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
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    Cli::<EthereumChainSpecParser, FollowerExt>::parse().run(async move |builder, ext| {
        event!(
            name: "eez.follower.launching",
            Level::INFO,
            block_time.secs = BLOCK_TIME.as_secs(),
            sequencer_rpc = %ext.sequencer_rpc,
            "launching eez-follower with {{block_time.secs}}s block time, sequencer={{sequencer_rpc}}",
        );

        // Launch reth with all default Ethereum components: tx pool, network,
        // payload builder, RPC, engine. Same `launch_node` (not the debug
        // variant) as eez-node — avoids racing reth's own `LocalMiner` if
        // `--dev` is set on the follower.
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();

        let rpc: RootProvider = RootProvider::new_http(ext.sequencer_rpc.clone());
        let scheduler = Scheduler::interval(BLOCK_TIME);
        let follower = Follower::new(beacon_engine_handle, rpc, scheduler);

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
