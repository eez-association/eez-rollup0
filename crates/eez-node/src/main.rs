//! eez Rollup-0 node binary.
//!
//! Wraps reth with our [`Sequencer`](eez_driver::Sequencer): reth provides the
//! EVM, storage, networking, RPC, and engine; we provide the block-production
//! schedule and engine-API driver loop.
//!
//! Stage 1 ships with a fixed 2-second block time and a placeholder fee
//! recipient. Future CLI flags will expose these; for now they're constants
//! at the top of `main`.

use std::{env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use eez_driver::{EthAttributesBuilder, Scheduler, Sequencer};
use eez_l1::{Submitter, SubmitterConfig};
use eez_prover::EcdsaProver;
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
    // Auto-load `.env` from the current working directory (gitignored).
    // Plus, if it exists, the machine-written `deployments.env` that
    // `scripts/deploy.sh` produces — addresses + rollup id flow from
    // there into the Submitter's config without manual paste-in.
    //
    // Failure to find either is fine — operators may set env vars
    // out-of-band (CI / systemd unit / shell `source`).
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

        // Submitter — opt-in via env. If `EEZ_L1_RPC_URL` is set, we
        // parse the full SubmitterConfig and spawn the task. Missing
        // required vars produce a loud config error; without `EEZ_L1_RPC_URL`
        // the node runs sequencer-only (stage-1-style smoke test).
        if env::var_os("EEZ_L1_RPC_URL").is_some() {
            let config = SubmitterConfig::from_env()?;
            let proof_signer_key = env::var("EEZ_PROOF_SIGNER_KEY").map_err(|_| {
                eyre::eyre!("EEZ_PROOF_SIGNER_KEY required when EEZ_L1_RPC_URL set")
            })?;
            let proof_signer = PrivateKeySigner::from_bytes(&B256::from_str(
                proof_signer_key.trim_start_matches("0x"),
            )?)?;
            let prover = Arc::new(EcdsaProver::new(proof_signer));
            let submitter = Submitter::new(config, prover, Arc::new(provider)).await?;
            event!(
                name: "eez.node.submitter.spawned",
                Level::INFO,
                "spawning eez submitter",
            );
            handle
                .node
                .task_executor
                .spawn_critical_task("eez-submitter", async move {
                    submitter.run().await;
                });
        } else {
            event!(
                name: "eez.node.submitter.skipped",
                Level::INFO,
                "EEZ_L1_RPC_URL not set; running sequencer only",
            );
        }

        handle.wait_for_node_exit().await
    })
}
