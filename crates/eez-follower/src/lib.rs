//! Launcher for the L1-derived eez follower node.

pub mod config;
mod unsafe_head;

use std::{sync::Arc, time::Duration};

use alloy_provider::RootProvider;
use eez_deriver::Deriver;
use eez_driver::BlockCommitterHandle;
use eez_l1::{L1CanonicalHead, L1Reader, L1Watcher};
use eez_node_common::{
    EezPayloadBuilder, EezPoolBuilder, L2NodeBuilder, checkpoint_dir,
    config::{ConfigArgs, load},
    node_cli, wait_for_l1_ready,
};
use reth_chainspec::EthChainSpec as _;
use reth_node_builder::components::BasicPayloadServiceBuilder;
use reth_node_ethereum::EthereumNode;
use tracing::{Level, event};

use unsafe_head::UnsafeHeadFollower;

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// ~15 min at the capped backoff: outlasts a restarting L1, but a permanently
/// refused RPC call still surfaces as an exit.
const BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES: u32 = 32;
const L2_SYSTEM_TX_GAS_PRICE: u128 = 1_000_000_000;
const L2_SYSTEM_TX_GAS_LIMIT: u64 = 2_000_000;

/// Launch an L1-derived follower node.
///
/// # Errors
///
/// Returns an error when configuration is invalid or either reth or the L1
/// bootstrap fails.
pub fn run() -> eyre::Result<()> {
    node_cli::<ConfigArgs>()?.run(launch)
}

async fn launch(builder: L2NodeBuilder, ext: ConfigArgs) -> eyre::Result<()> {
    let config: config::Config = load(&ext.eez_config_path)?;
    config.validate()?;
    let timing = config.timing.build()?;
    let system_signer = config.l2_system_key.signer()?;
    let system_address = system_signer.address();
    let l2_datadir = builder.config().datadir().data_dir().to_path_buf();
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "follower",
        config = %ext.eez_config_path.display(),
        "launching eez follower",
    );

    let handle = builder
        .with_types::<EthereumNode>()
        .with_components(
            EthereumNode::components()
                // Reorged-out system transactions must not leak from reth's
                // reinjection path into an ordinary Live block.
                .pool(EezPoolBuilder::new(system_address))
                .payload(BasicPayloadServiceBuilder::new(EezPayloadBuilder::default())),
        )
        .with_add_ons(reth_node_ethereum::node::EthereumAddOns::default())
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let provider = handle.node.provider.clone();
    let task_executor = handle.node.task_executor.clone();
    let l1_head = Arc::new(L1CanonicalHead::default());

    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;

    let l1_reader_config = config.l1.reader();
    let deploy_block = l1_reader_config.deploy_block;
    let l1_reader = L1Reader::new(l1_reader_config);
    let l1_watcher = L1Watcher::new(config.l1.watcher());
    let system_tx_cfg = eez_protocol::system_tx::SystemTxContext {
        system_signer,
        eezl2_address: eez_protocol::EEZL2_ADDRESS,
        l2_chain_id: chain_spec.chain().id(),
        l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
        l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
        this_rollup_id: config.l1.rollup_id,
    };
    event!(
        name: "eez.node.follower.system_tx_cfg",
        Level::INFO,
        %system_address,
        "cross-chain system tx reconstruction config loaded",
    );

    let deriver = Deriver::new(
        block_committer.clone(),
        Arc::new(provider.clone()),
        l1_reader.clone(),
        chain_spec,
        timing.l2_block_time().as_secs(),
        deploy_block,
        Arc::clone(&l1_head),
        Some(system_tx_cfg),
        checkpoint_dir(&l2_datadir),
    );

    wait_for_l1_ready(&l1_reader, deploy_block, config.l1.chain_id).await?;

    let mut retry_delay = BOOT_CATCH_UP_INITIAL_RETRY_DELAY;
    let mut attempts = 0_u64;
    let mut transport_failures = 0_u32;
    let (l1_seed_number, l1_seed_hash) = loop {
        match deriver.catch_up_with_seed().await {
            Ok(seed) => break seed,
            Err(err)
                if err.is_l1_transport() && {
                    transport_failures += 1;
                    transport_failures >= BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES
                } =>
            {
                event!(
                    name: "eez.node.deriver.boot_catch_up.transport_exhausted",
                    Level::ERROR,
                    mode = "follower",
                    transport_failures,
                    error = %err,
                    "L1 transport kept failing during boot catch-up; the endpoint is likely refusing a call we need, not merely unreachable",
                );
                return Err(eyre::eyre!(
                    "boot-time deriver catch_up gave up after {transport_failures} L1 transport failures: {err}"
                ));
            }
            Err(err) if err.is_source_incomplete() || err.is_l1_transport() => {
                attempts += 1;
                event!(
                    name: "eez.node.deriver.boot_catch_up.source_incomplete",
                    Level::WARN,
                    mode = "follower",
                    attempts,
                    retry_delay_secs = retry_delay.as_secs(),
                    error = %err,
                    "boot-time catch_up could not read all L1 source data yet; retrying before starting L1-active tasks",
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = Duration::from_secs(
                    retry_delay
                        .as_secs()
                        .saturating_mul(2)
                        .min(BOOT_CATCH_UP_MAX_RETRY_DELAY.as_secs()),
                );
            }
            Err(err) => {
                event!(
                    name: "eez.node.deriver.boot_catch_up.failed",
                    Level::ERROR,
                    mode = "follower",
                    error = %err,
                    "boot-time catch_up failed; refusing to start L1-active tasks before reconciliation",
                );
                return Err(eyre::eyre!("boot-time deriver catch_up failed: {err}"));
            }
        }
    };

    event!(
        name: "eez.node.deriver.spawned",
        Level::INFO,
        mode = "follower",
        initial_posted_through = deriver.cursor(),
        "spawning eez deriver",
    );
    let deriver_events = l1_watcher.subscribe();
    task_executor.spawn_critical_task("eez-deriver", async move {
        deriver.run(deriver_events).await;
    });

    if let Some(sequencer_rpc) = config.sequencer_rpc {
        let follower = UnsafeHeadFollower::new(
            block_committer,
            RootProvider::new_http(sequencer_rpc),
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
            "no sequencer RPC configured; running L1-derived-only follower",
        );
    }

    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );

    handle.wait_for_node_exit().await
}
