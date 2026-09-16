//! Launcher for the L1-derived eez follower node.

mod unsafe_head;

use std::{env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use eez_deriver::Deriver;
use eez_driver::{BlockCommitterHandle, RollupTiming};
use eez_l1::{L1CanonicalHead, L1Reader, L1ReaderConfig, L1Watcher, L1WatcherConfig};
use eez_node_common::{
    EezPayloadBuilder, EezPoolBuilder, L2NodeBuilder, node_cli, read_checkpoint_dir,
    wait_for_l1_ready, warn_on_deprecated_env,
};
use eez_p2p::{NetworkConfig, NetworkService};
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

struct FollowerSystemConfig {
    system_signer: PrivateKeySigner,
    eezl2_address: Address,
    this_rollup_id: u64,
}

impl FollowerSystemConfig {
    fn into_context(self, l2_chain_id: u64) -> eez_protocol::system_tx::SystemTxContext {
        eez_protocol::system_tx::SystemTxContext {
            system_signer: self.system_signer,
            eezl2_address: self.eezl2_address,
            l2_chain_id,
            l2_gas_price: L2_SYSTEM_TX_GAS_PRICE,
            l2_gas_limit: L2_SYSTEM_TX_GAS_LIMIT,
            this_rollup_id: self.this_rollup_id,
        }
    }
}

/// Follower-specific CLI arguments layered on top of reth's CLI.
#[derive(clap::Args, Debug, Clone)]
struct FollowerArgs {
    /// libp2p TCP listen multiaddr.
    #[arg(
        long,
        env = "EEZ_P2P_LISTEN_ADDR",
        default_value = "/ip4/0.0.0.0/tcp/9300"
    )]
    p2p_listen_addr: String,

    /// Comma-separated static libp2p peer multiaddrs.
    #[arg(long, env = "EEZ_P2P_PEERS", value_delimiter = ',')]
    p2p_peers: Vec<String>,

    /// Address authorized to sign this rollup's unsafe blocks. When omitted,
    /// the follower remains L1-derived-only.
    #[arg(long, env = "EEZ_UNSAFE_BLOCK_SIGNER_ADDRESS")]
    unsafe_block_signer_address: Option<Address>,
}

impl FollowerArgs {
    fn network_config(&self, chain_id: u64) -> eyre::Result<NetworkConfig> {
        NetworkConfig::parse(
            chain_id,
            &self.p2p_listen_addr,
            self.p2p_peers.iter().map(String::as_str),
        )
        .map_err(Into::into)
    }
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

    let stateful_proof_signer = eez_prover_stateful::Config::from_env()?;
    if stateful_proof_signer.is_some() && ext.unsafe_block_signer_address.is_some() {
        return Err(eyre::eyre!(
            "stateful proof signing requires an L1-derived-only follower; remove EEZ_UNSAFE_BLOCK_SIGNER_ADDRESS",
        ));
    }
    // A production follower must reconstruct the Composer's Sync-block system
    // transactions byte-for-byte. Read every required value before launching
    // reth so a misconfigured follower never appears healthy.
    let system_config = read_system_config()?;
    let system_address = system_config.system_signer.address();

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
    let l2_chain_id = chain_spec.chain().id();
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
    let system_tx_cfg = system_config.into_context(chain_spec.chain().id());
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
        read_checkpoint_dir(),
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
                    event_name = "eez.node.deriver.boot_catch_up.failed",
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

    if let Some(config) = stateful_proof_signer {
        let signer_provider = provider.clone();
        let signer_chain_spec = handle.node.chain_spec();
        event!(
            name: "eez.node.stateful_proof_signer.spawned",
            Level::INFO,
            "spawning stateful proof signer over the L1-derived follower",
        );
        task_executor.spawn_critical_with_graceful_shutdown_signal(
            "eez-stateful-proof-signer",
            move |shutdown| async move {
                // Signal tonic while retaining Reth's shutdown guard during drain.
                let shutdown_signal = shutdown.clone().ignore_guard();
                let result = eez_prover_stateful::serve(
                    config,
                    signer_provider,
                    signer_chain_spec,
                    shutdown_signal,
                )
                .await;
                drop(shutdown);
                result.unwrap_or_else(|error| panic!("stateful proof signer exited: {error:#}"));
            },
        );
    }

    if let Some(unsafe_block_signer_address) = ext.unsafe_block_signer_address {
        let (p2p_service, p2p_handle, p2p_events) =
            NetworkService::new(ext.network_config(l2_chain_id)?)?;
        task_executor.spawn_critical_task("eez-unsafe-block-p2p", p2p_service.run());
        let follower = UnsafeHeadFollower::new(
            block_committer,
            provider,
            l2_chain_id,
            unsafe_block_signer_address,
            p2p_events,
            p2p_handle,
        );
        event!(
            name: "eez.node.follower.p2p.spawned",
            Level::INFO,
            signer = %unsafe_block_signer_address,
            "spawning signed P2P unsafe-block follower",
        );
        task_executor.spawn_critical_task("eez-node-follower-unsafe-head", async move {
            follower.run().await;
        });
    } else {
        event!(
            name: "eez.node.follower.l1_derived_only",
            Level::INFO,
            "EEZ_UNSAFE_BLOCK_SIGNER_ADDRESS not set; running L1-derived-only follower",
        );
    }

    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );

    handle.wait_for_node_exit().await
}

/// Read the mandatory system-transaction identity before follower startup.
fn read_system_config() -> eyre::Result<FollowerSystemConfig> {
    let system_key = required_env("EEZ_L2_SYSTEM_KEY")?;
    let eezl2_address = required_env("EEZL2_ADDRESS")?;
    let rollup_id = required_env("EEZ_ROLLUP_ID")?;

    let system_signer =
        PrivateKeySigner::from_bytes(&B256::from_str(system_key.trim_start_matches("0x"))?)?;
    let eezl2_address = Address::from_str(&eezl2_address)
        .map_err(|err| eyre::eyre!("EEZL2_ADDRESS malformed: {err}"))?;
    let this_rollup_id = rollup_id
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_ROLLUP_ID malformed: {e}"))?;

    Ok(FollowerSystemConfig {
        system_signer,
        eezl2_address,
        this_rollup_id,
    })
}

fn required_env(name: &str) -> eyre::Result<String> {
    match env::var(name) {
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Err(eyre::eyre!("{name} is required in follower mode")),
        Err(env::VarError::NotUnicode(_)) => Err(eyre::eyre!("{name} contains non-UTF-8 bytes")),
    }
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
