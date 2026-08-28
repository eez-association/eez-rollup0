//! Launcher for the unanchored local-development eez sequencer.

use std::{env, sync::Arc};

use eez_driver::{
    BlockCommitterHandle, EthAttributesBuilder, RollupTiming, Sequencer, spawn_interval,
};
use eez_node_common::{
    EezPayloadBuilder, L2NodeBuilder, NoRoleArgs, node_cli, warn_on_deprecated_env,
};
use reth_node_builder::components::BasicPayloadServiceBuilder;
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

/// Launch an unanchored local-development sequencer.
///
/// # Errors
///
/// Returns an error when configuration is invalid or reth fails to launch.
pub fn run() -> eyre::Result<()> {
    node_cli::<NoRoleArgs>()?.run(launch)
}

async fn launch(builder: L2NodeBuilder, _ext: NoRoleArgs) -> eyre::Result<()> {
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "standalone",
        "launching eez development node",
    );
    warn_on_deprecated_env();

    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder::default())),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let timing = if env::var_os("EEZ_L2_BLOCK_TIME_MS").is_some() {
        let timing = RollupTiming::from_env()?;
        event!(
            name: "eez.node.timing.standalone_configured",
            Level::INFO,
            l2_block_time_ms = timing.l2_block_time().as_millis(),
            "standalone mode — using configured RollupTiming",
        );
        timing
    } else {
        event!(
            name: "eez.node.timing.standalone_default",
            Level::INFO,
            "standalone mode — using default RollupTiming (L2=2s); set EEZ_*_TIME_MS to override",
        );
        RollupTiming::standalone_default()
    };
    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &handle.node.provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;
    let sequencer = Sequencer::standalone(
        EthAttributesBuilder::new(chain_spec),
        block_committer,
        spawn_interval(timing.l2_block_time()),
        timing,
    );
    event!(
        name: "eez.node.sequencer.spawned",
        Level::INFO,
        mode = "standalone",
        "spawning eez sequencer",
    );
    handle
        .node
        .task_executor
        .spawn_critical_task("eez-sequencer", async move {
            sequencer.run().await;
        });

    handle.wait_for_node_exit().await
}
