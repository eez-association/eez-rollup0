//! Launcher for the L1-derived eez follower node.

mod unsafe_head;

use std::{env, str::FromStr, sync::Arc, time::Duration};

use alloy_primitives::Address;
use eez_deriver::{Deriver, DeriverError};
use eez_driver::{BlockCommitterHandle, RollupTiming};
use eez_l1::{L1CanonicalHead, L1Error, L1Reader, L1ReaderConfig, L1Watcher, L1WatcherConfig};
use eez_node_common::EezNode;
use eez_node_common::{
    L2NodeBuilder, node_cli, read_checkpoint_dir, wait_for_l1_ready, warn_on_deprecated_env,
};
use eez_seq_sig::HeadDomain;
use reth_chainspec::EthChainSpec as _;
use reth_network::BlockDownloaderProvider as _;
use tokio::sync::mpsc;
use tracing::{Level, event};

use unsafe_head::{PeerBlocks, UnsafeHeadFollower};

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// ~15 min at the capped backoff: outlasts a restarting L1, but a permanently
/// refused RPC call still surfaces as an exit.
const BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES: u32 = 32;
/// Overlay heads queued while the follower imports; past it the overlay drops
/// and WARNs instead of waiting.
const GOSSIP_BUFFER: usize = 64;

struct FollowerSystemConfig {
    eezl2_address: Address,
    this_rollup_id: u64,
}

impl FollowerSystemConfig {
    fn into_context(self, l2_chain_id: u64) -> eez_protocol::system_tx::SystemTxContext {
        eez_protocol::system_tx::SystemTxContext {
            eezl2_address: self.eezl2_address,
            l2_chain_id,
            this_rollup_id: self.this_rollup_id,
        }
    }
}

/// Follower-specific CLI arguments layered on top of reth's CLI.
#[derive(clap::Args, Debug, Clone)]
struct FollowerArgs {
    /// The only address whose signed heads are followed, over the p2p overlay.
    /// When set, the unsafe head follows them; safe and finalized remain
    /// L1-derived.
    #[arg(long, env = "EEZ_SEQUENCER_ADDRESS")]
    sequencer_address: Option<Address>,
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
    if stateful_proof_signer.is_some() && ext.sequencer_address.is_some() {
        return Err(eyre::eyre!(
            "stateful proof signing requires an L1-derived-only follower; remove EEZ_SEQUENCER_ADDRESS",
        ));
    }
    // A production follower must reconstruct the Composer's Sync-block system
    // transactions byte-for-byte. Read every required value before launching
    // reth so a misconfigured follower never appears healthy.
    let system_config = read_system_config()?;
    let p2p_config = eez_p2p::P2pConfig::from_env(
        builder.config().chain.chain().id(),
        system_config.this_rollup_id,
    )?;
    let head_follower = head_follower(p2p_config, ext.sequencer_address)?;
    let system_address = eez_primitives::SYSTEM_ADDRESS;

    let handle = builder
        .node(EezNode)
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
    let registry = l1_reader_config.eez;
    let l1_reader = L1Reader::new(l1_reader_config);
    let l1_watcher = L1Watcher::new(L1WatcherConfig::from_env()?);
    let l2_chain_id = chain_spec.chain().id();
    let head_domain = HeadDomain {
        l2_chain_id,
        registry,
        rollup_id: system_config.this_rollup_id,
    };
    let system_tx_cfg = system_config.into_context(l2_chain_id);
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
                if matches!(&err, DeriverError::L1Scan(L1Error::Provider(_))) && {
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
            Err(err) if matches!(&err, DeriverError::L1Scan(l1) if l1.is_retryable()) => {
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
        let signer_committer = block_committer.clone();
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
                    signer_committer,
                    shutdown_signal,
                )
                .await;
                drop(shutdown);
                result.unwrap_or_else(|error| panic!("stateful proof signer exited: {error:#}"));
            },
        );
    }

    if let Some((sequencer_address, cfg)) = head_follower {
        event!(
            name: "eez.node.follower.unsafe_head.spawned",
            Level::INFO,
            %sequencer_address,
            "spawning signed-head unsafe-head follower",
        );
        let network = handle.node.network.clone();
        let (gossip_tx, gossip_rx) = mpsc::channel(GOSSIP_BUFFER);
        let overlay = eez_p2p::Overlay::new(
            cfg,
            eez_p2p::keypair(&network.secret_key().secret_bytes())?,
            eez_p2p::Validator::new(head_domain, sequencer_address),
            eez_node_common::discover(network.clone()),
        )?;
        task_executor.spawn_critical_task("eez-p2p", overlay.run(None, Some(gossip_tx)));
        let follower = UnsafeHeadFollower::new(
            block_committer,
            gossip_rx,
            PeerBlocks::new(network.fetch_client().await?),
            provider,
            l1_head,
            timing.l2_block_time(),
        );
        task_executor.spawn_critical_task("eez-node-follower-unsafe-head", follower.run());
    } else {
        event!(
            name: "eez.node.follower.l1_derived_only",
            Level::INFO,
            "EEZ_SEQUENCER_ADDRESS not set; running L1-derived-only follower",
        );
    }

    task_executor.spawn_critical_task(
        "eez-l1-watcher",
        l1_watcher.polling(l1_seed_number, l1_seed_hash),
    );

    handle.wait_for_node_exit().await
}

/// The follower-modes rule: the address and the overlay switch the head
/// follower on together; either alone is an error.
fn head_follower<C>(
    overlay: Option<C>,
    address: Option<Address>,
) -> eyre::Result<Option<(Address, C)>> {
    match (address, overlay) {
        (Some(address), Some(overlay)) => Ok(Some((address, overlay))),
        (Some(_), None) => Err(eyre::eyre!("EEZ_SEQUENCER_ADDRESS requires EEZ_P2P_LISTEN")),
        (None, Some(_)) => Err(eyre::eyre!("EEZ_P2P_LISTEN requires EEZ_SEQUENCER_ADDRESS")),
        (None, None) => Ok(None),
    }
}

/// Read the mandatory system-transaction identity before follower startup.
fn read_system_config() -> eyre::Result<FollowerSystemConfig> {
    let eezl2_address = required_env("EEZL2_ADDRESS")?;
    let rollup_id = required_env("EEZ_ROLLUP_ID")?;

    let eezl2_address = Address::from_str(&eezl2_address)
        .map_err(|err| eyre::eyre!("EEZL2_ADDRESS malformed: {err}"))?;
    let this_rollup_id = rollup_id
        .parse()
        .map_err(|e| eyre::eyre!("EEZ_ROLLUP_ID malformed: {e}"))?;

    Ok(FollowerSystemConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_follower_modes() {
        let address = Address::repeat_byte(1);
        assert_eq!(
            head_follower(Some(()), Some(address)).unwrap(),
            Some((address, ()))
        );
        assert_eq!(head_follower::<()>(None, None).unwrap(), None);
        let err = |overlay, address| head_follower(overlay, address).unwrap_err().to_string();
        assert_eq!(
            err(None, Some(address)),
            "EEZ_SEQUENCER_ADDRESS requires EEZ_P2P_LISTEN"
        );
        assert_eq!(
            err(Some(()), None),
            "EEZ_P2P_LISTEN requires EEZ_SEQUENCER_ADDRESS"
        );
    }
}
