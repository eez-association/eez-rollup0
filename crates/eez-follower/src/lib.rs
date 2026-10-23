//! Launcher for the L1-derived eez follower node.

mod unsafe_head;

use std::{env, str::FromStr, sync::Arc};

use alloy_primitives::Address;
use alloy_provider::RootProvider;
use eez_deriver::Deriver;
use eez_driver::{BlockCommitterHandle, RollupTiming};
use eez_l1::{L1CanonicalHead, L1Reader, L1ReaderConfig, L1Watcher, L1WatcherConfig};
use eez_node_common::EezNode;
use eez_node_common::{
    L2NodeBuilder, boot, node_cli, sync_mode_from_env, wait_for_idle_engine, wait_for_l1_ready,
    warn_on_deprecated_env,
};
use eez_seq_sig::HeadDomain;
use reth_chainspec::EthChainSpec as _;
use reth_network::{BlockDownloaderProvider as _, NetworkInfo as _, PeersInfo as _};
use tokio::sync::mpsc;
use tracing::{Level, event};

use unsafe_head::{PeerBlocks, UnsafeHeadFollower};

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
    /// Sequencer JSON-RPC URL, polled for signed heads; without the p2p
    /// overlay it also serves the missing blocks.
    #[arg(long, env = "EEZ_SEQUENCER_RPC")]
    sequencer_rpc: Option<url::Url>,

    /// The only address whose signed heads are followed. When set, the unsafe
    /// head follows them; safe and finalized remain L1-derived.
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
    let sync_mode = sync_mode_from_env()?;

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
    let head_address = head_follower(
        ext.sequencer_rpc.clone(),
        p2p_config.is_some(),
        ext.sequencer_address,
    )?;
    let system_address = eez_primitives::SYSTEM_ADDRESS;

    let handle = builder
        .node(EezNode)
        .launch_with_debug_capabilities()
        .await?;

    let chain_spec: Arc<_> = handle.node.chain_spec();
    let provider = handle.node.provider.clone();
    let task_executor = handle.node.task_executor.clone();
    let network = handle.node.network.clone();
    let timing = RollupTiming::from_env()?;
    let l1_head = Arc::new(L1CanonicalHead::default());

    // Env reads come first so a bad L1 env fails at startup, not after a backfill.
    let l1_reader_config = L1ReaderConfig::from_env()?;
    let l1_watcher_config = L1WatcherConfig::from_env()?;

    // The committer reads reth's head, which a resumed backfill is still moving.
    wait_for_idle_engine(|| network.is_syncing()).await;
    let block_committer = BlockCommitterHandle::spawn_from_provider(
        &provider,
        handle.node.add_ons_handle.beacon_engine_handle.clone(),
        handle.node.payload_builder_handle.clone(),
        None,
    )?;

    let deploy_block = l1_reader_config.deploy_block;
    let registry = l1_reader_config.eez;
    let l1_reader = L1Reader::new(l1_reader_config);
    let l1_watcher = L1Watcher::new(l1_watcher_config);
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
        sync_mode.deriver_boot(),
    );

    let probe = sync_mode.readiness_probe(deploy_block);
    wait_for_l1_ready(&l1_reader, probe, read_l1_chain_id()?).await?;
    let (l1_seed_number, l1_seed_hash) = boot(
        sync_mode,
        &provider,
        &block_committer,
        &deriver,
        &l1_reader,
        || network.num_connected_peers(),
        "follower",
    )
    .await?;

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

    if let Some(sequencer_address) = head_address {
        let sequencer_rpc = ext.sequencer_rpc.map(RootProvider::new_http);
        let poll_interval = timing.l2_block_time();
        event!(
            name: "eez.node.follower.unsafe_head.spawned",
            Level::INFO,
            %sequencer_address,
            rpc = sequencer_rpc.is_some(),
            p2p = p2p_config.is_some(),
            "spawning signed-head unsafe-head follower",
        );
        const TASK: &str = "eez-node-follower-unsafe-head";
        // With the overlay on, blocks come only from peers; the RPC, if any, only polls heads.
        if let Some(cfg) = p2p_config {
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
                sequencer_rpc,
                Some(gossip_rx),
                PeerBlocks::new(network.fetch_client().await?),
                provider,
                l1_head,
                head_domain,
                sequencer_address,
                poll_interval,
            );
            task_executor.spawn_critical_task(TASK, follower.run());
        } else {
            let rpc = sequencer_rpc.expect("head_follower requires a head source");
            let follower = UnsafeHeadFollower::new(
                block_committer,
                Some(rpc.clone()),
                None,
                rpc,
                provider,
                l1_head,
                head_domain,
                sequencer_address,
                poll_interval,
            );
            task_executor.spawn_critical_task(TASK, follower.run());
        }
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

/// The follower-modes rule: the address switches the head follower on and
/// needs at least one head source; a source without the address is an error.
fn head_follower(
    rpc: Option<url::Url>,
    p2p: bool,
    address: Option<Address>,
) -> eyre::Result<Option<Address>> {
    match (address, rpc.is_some(), p2p) {
        (Some(_), false, false) => Err(eyre::eyre!(
            "EEZ_SEQUENCER_ADDRESS requires EEZ_SEQUENCER_RPC or EEZ_P2P_LISTEN"
        )),
        (Some(address), _, _) => Ok(Some(address)),
        (None, true, _) => Err(eyre::eyre!(
            "EEZ_SEQUENCER_RPC requires EEZ_SEQUENCER_ADDRESS"
        )),
        (None, false, true) => Err(eyre::eyre!("EEZ_P2P_LISTEN requires EEZ_SEQUENCER_ADDRESS")),
        (None, false, false) => Ok(None),
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
        let rpc = || Some(url::Url::parse("http://127.0.0.1:8545").unwrap());
        let address = Some(Address::repeat_byte(1));
        assert_eq!(head_follower(rpc(), false, address).unwrap(), address);
        assert_eq!(head_follower(None, true, address).unwrap(), address);
        let err = |rpc, p2p, address| head_follower(rpc, p2p, address).unwrap_err().to_string();
        assert_eq!(
            err(None, false, address),
            "EEZ_SEQUENCER_ADDRESS requires EEZ_SEQUENCER_RPC or EEZ_P2P_LISTEN"
        );
        assert_eq!(
            err(rpc(), false, None),
            "EEZ_SEQUENCER_RPC requires EEZ_SEQUENCER_ADDRESS"
        );
        assert_eq!(
            err(None, true, None),
            "EEZ_P2P_LISTEN requires EEZ_SEQUENCER_ADDRESS"
        );
        assert_eq!(head_follower(None, false, None).unwrap(), None);
    }
}
