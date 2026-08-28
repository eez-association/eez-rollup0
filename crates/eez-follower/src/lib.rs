//! Launcher for the L1-derived eez follower node.

mod unsafe_head;

use std::{env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_provider::RootProvider;
use alloy_signer_local::PrivateKeySigner;
use eez_deriver::Deriver;
use eez_driver::{BlockCommitterHandle, RollupTiming};
use eez_l1::{L1CanonicalHead, L1Reader, L1ReaderConfig, L1Watcher, L1WatcherConfig};
use eez_node_common::{
    EezPayloadBuilder, L2NodeBuilder, node_cli, wait_for_l1_ready, warn_on_deprecated_env,
};
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

/// Follower-specific CLI arguments layered on top of reth's CLI.
#[derive(clap::Args, Debug, Clone)]
struct FollowerArgs {
    /// Sequencer JSON-RPC URL. When set, this enables the optional unsafe-head
    /// overlay; safe and finalized remain L1-derived.
    #[arg(long, env = "EEZ_SEQUENCER_RPC")]
    sequencer_rpc: Option<url::Url>,
}

/// Launch an L1-derived follower node.
///
/// # Errors
///
/// Returns an error when configuration is invalid or either reth or the L1
/// bootstrap fails.
pub fn run() -> eyre::Result<()> {
    node_cli::<FollowerArgs>()?.run(launch)
}

async fn launch(builder: L2NodeBuilder, ext: FollowerArgs) -> eyre::Result<()> {
    event!(
        name: "eez.node.launching",
        Level::INFO,
        mode = "follower",
        "launching eez follower",
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
    let provider = handle.node.provider.clone();
    let task_executor = handle.node.task_executor.clone();
    let timing = RollupTiming::from_env()?;
    let l1_head = Arc::new(L1CanonicalHead::default());

    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;

    let l1_reader_config = L1ReaderConfig::from_env()?;
    let deploy_block = l1_reader_config.deploy_block;
    let l1_reader = L1Reader::new(l1_reader_config);
    let l1_watcher = L1Watcher::new(L1WatcherConfig::from_env()?);
    let system_tx_cfg = build_system_tx_config(&chain_spec)?;
    event!(
        name: "eez.node.follower.system_tx_cfg",
        Level::INFO,
        enabled = system_tx_cfg.is_some(),
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
        system_tx_cfg,
    );

    wait_for_l1_ready(&l1_reader, deploy_block, read_l1_chain_id()?).await?;

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

    if let Some(sequencer_rpc) = ext.sequencer_rpc {
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
            "EEZ_SEQUENCER_RPC not set; running L1-derived-only follower",
        );
    }

    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );

    handle.wait_for_node_exit().await
}

/// Build deterministic system-transaction reconstruction config from env.
fn build_system_tx_config<ChainSpec>(
    chain_spec: &ChainSpec,
) -> eyre::Result<Option<eez_protocol::system_tx::SystemTxContext>>
where
    ChainSpec: reth_chainspec::EthChainSpec,
{
    let system_key = match env::var("EEZ_L2_SYSTEM_KEY") {
        Ok(system_key) => system_key,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(eyre::eyre!("EEZ_L2_SYSTEM_KEY contains non-UTF-8 bytes"));
        }
    };
    let eezl2_address_str = env::var("EEZL2_ADDRESS")
        .map_err(|_| eyre::eyre!("EEZL2_ADDRESS required when EEZ_L2_SYSTEM_KEY is set"))?;
    let rollup_id_str = env::var("EEZ_ROLLUP_ID")
        .map_err(|_| eyre::eyre!("EEZ_ROLLUP_ID required when EEZ_L2_SYSTEM_KEY is set"))?;

    let system_signer =
        PrivateKeySigner::from_bytes(&B256::from_str(system_key.trim_start_matches("0x"))?)?;
    let eezl2_address: Address = Address::from_str(&eezl2_address_str)?;
    let this_rollup_id: u64 = rollup_id_str
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_ROLLUP_ID malformed: {e}"))?;

    Ok(Some(eez_protocol::system_tx::SystemTxContext {
        system_signer,
        eezl2_address,
        l2_chain_id: chain_spec.chain().id(),
        l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
        l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
        this_rollup_id,
    }))
}

/// Required for followers: guessing would either assert the wrong source
/// chain or silently skip the configured RPC's identity check.
fn read_l1_chain_id() -> eyre::Result<u64> {
    let value = env::var("EEZ_L1_CHAIN_ID").map_err(|err| {
        eyre::eyre!("EEZ_L1_CHAIN_ID is required (the L1 chain id this node derives from): {err}")
    })?;
    value
        .parse::<u64>()
        .map_err(|err| eyre::eyre!("EEZ_L1_CHAIN_ID={value:?} malformed: {err}"))
}
