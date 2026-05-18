//! eez Rollup-0 node binary.
//!
//! Wraps reth with our [`Sequencer`](eez_driver::Sequencer): reth provides the
//! EVM, storage, networking, RPC, and engine; we provide the block-production
//! schedule and engine-API driver loop.
//!
//! Stage 1 ships with a fixed 2-second block time and a placeholder fee
//! recipient. Future CLI flags will expose these; for now they're constants
//! at the top of `main`.

use std::{sync::Arc, time::Duration};

use eez_driver::{EthAttributesBuilder, Scheduler, Sequencer};
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// L2 block cadence for Rollup-0.
///
/// Configurable later (a CLI flag fits in `Ext` on [`Cli`]). Pinned at 2s for
/// stage 1 to match the Rollup-1 draft spec §1.3.
const BLOCK_TIME: Duration = Duration::from_secs(2);

fn main() -> eyre::Result<()> {
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

        // Launch reth with all default Ethereum components: tx pool, network,
        // payload builder, RPC, engine. We deliberately use `launch_node` —
        // **not** the debug variant — because that auto-spawns reth's
        // `LocalMiner` whenever `--dev` is set, which would race our
        // sequencer driving the same engine.
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let chain_spec: Arc<_> = handle.node.chain_spec();
        let provider = handle.node.provider.clone();
        let payload_builder_handle = handle.node.payload_builder_handle.clone();
        let beacon_engine_handle = handle.node.add_ons_handle.beacon_engine_handle.clone();
        let task_executor = handle.node.task_executor.clone();

        let attributes = EthAttributesBuilder::new(chain_spec);
        let scheduler = Scheduler::interval(BLOCK_TIME);
        let sequencer = Sequencer::new(
            &provider,
            attributes,
            beacon_engine_handle,
            scheduler,
            payload_builder_handle,
        )?;

        event!(
            name: "eez.node.sequencer.spawned",
            Level::INFO,
            "spawning eez sequencer",
        );

        task_executor.spawn_critical_task("eez-sequencer", async move {
            sequencer.run().await;
        });

        handle.wait_for_node_exit().await
    })
}
