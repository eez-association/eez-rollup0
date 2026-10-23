//! Shared, role-neutral bootstrap substrate for eez node binaries.

use clap::Parser as _;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{
    chainspec::EthereumChainSpecParser,
    interface::{Cli, Commands},
};
use reth_node_builder::{NodeBuilder, WithLaunchContext};
use tracing::{Level, event};

#[cfg(feature = "l1-startup")]
mod boot;
#[cfg(feature = "l1-startup")]
pub use boot::{SyncMode, boot, sync_mode_from_env, wait_for_idle_engine};
mod discovery;
pub use discovery::{EezNetworkBuilder, discover};
mod node;
pub use node::EezNode;
mod rpc;
pub use rpc::eez_add_ons;
mod payload;
pub use payload::EezPayloadBuilder;
mod payload_builder;
mod pool;
mod pool_transaction;
pub use pool::{EezPoolBuilder, HeldTxs};

/// Per M-MIMALLOC-APPS — meaningful win on allocation-heavy workloads.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Role-neutral reth builder passed to each typed launcher.
pub type L2NodeBuilder =
    WithLaunchContext<NodeBuilder<reth_db::DatabaseEnv, reth_chainspec::ChainSpec>>;

/// Composer nodes have no role-specific CLI arguments.
#[derive(clap::Args, Debug, Clone)]
pub struct NoRoleArgs {}

/// Parse the shared reth CLI and layer role-specific arguments on top.
///
/// # Errors
///
/// Returns an error when command-line arguments are invalid, or when discovery
/// is on without `--bootnodes`.
pub fn node_cli<Ext>() -> eyre::Result<Cli<EthereumChainSpecParser, Ext>>
where
    Ext: clap::Args + std::fmt::Debug,
{
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("deployments.env");

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: set during single-threaded startup before any other thread is spawned.
        unsafe {
            std::env::set_var("RUST_BACKTRACE", "1");
        }
    }

    l2_cli(std::env::args().collect())
}

/// Parses the CLI with the flags every L2 node runs with and refuses reth's mainnet bootnodes.
fn l2_cli<Ext>(mut argv: Vec<String>) -> eyre::Result<Cli<EthereumChainSpecParser, Ext>>
where
    Ext: clap::Args + std::fmt::Debug,
{
    // Engine API FCUs may intentionally unwind the L2 head during recovery.
    // Transactions enter only through the sequencer's ingress, never devp2p gossip.
    // Sync state reads Syncing only while reth backfills, so boot can wait it out.
    for flag in [
        "--engine.always-process-payload-attributes-on-canonical-head",
        "--engine.allow-unwind-canonical-header",
        "--disable-tx-gossip",
        "--debug.startup-sync-state-idle",
    ] {
        if !argv.iter().any(|a| a == flag) {
            argv.push(flag.to_owned());
        }
    }
    let cli = Cli::<EthereumChainSpecParser, Ext>::try_parse_from(argv)?;
    // Parsed flags include reth's aliases and its `--dev` default. Without bootnodes,
    // reth dials mainnet's for a custom chain.
    if let Commands::Node(node) = &cli.command
        && !node.network.discovery.disable_discovery
        && node.network.bootnodes.is_none()
    {
        eyre::bail!(
            "discovery is on but --bootnodes is not set; reth would dial Ethereum mainnet bootnodes"
        );
    }
    Ok(cli)
}

/// Warn about obsolete runtime role selectors now replaced by explicit binaries.
pub fn warn_on_deprecated_env() {
    for name in [
        "EEZ_COMPOSER_INTERVAL_SECS",
        "EEZ_SEQUENCER_DISABLED",
        "EEZ_COMPOSER_DISABLED",
    ] {
        if std::env::var_os(name).is_some() {
            event!(
                name: "eez.node.env.deprecated",
                Level::WARN,
                env = name,
                "env var is ignored; select the node role with the eez-composer or eez-follower executable."
            );
        }
    }
}

/// Select the L2 datadir used for the deriver's boot checkpoint.
///
/// A missing or unusable directory disables checkpoint persistence for this
/// process, avoiding a failed write after every settled batch.
#[cfg(feature = "l1-startup")]
#[must_use]
pub(crate) fn read_checkpoint_dir() -> Option<std::path::PathBuf> {
    let dir = std::env::var("EEZ_L2_DATADIR")
        .ok()
        .map(std::path::PathBuf::from)
        .filter(|dir| dir.is_dir());
    if dir.is_none() {
        event!(
            name: "eez.node.checkpoint.disabled",
            Level::INFO,
            configured = ?std::env::var("EEZ_L2_DATADIR").ok(),
            "no usable EEZ_L2_DATADIR; boot will rescan L1 from the deploy block",
        );
    }
    dir
}

/// The L1 block that must be served before boot reads L1.
#[cfg(feature = "l1-startup")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessProbe {
    /// L1-only derivation scans from the deploy block.
    DeployBlock(u64),
    /// Peers mode anchors at L1's finalized block.
    Finalized,
}

#[cfg(feature = "l1-startup")]
impl std::fmt::Display for ReadinessProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeployBlock(block) => write!(f, "deploy block {block}"),
            Self::Finalized => f.write_str("the finalized block"),
        }
    }
}

/// Blocks until L1 serves the `probe` block and matches the expected chain.
///
/// # Errors
///
/// Returns an error for a mismatched chain or when the L1 source fails to make
/// progress within the bounded startup window.
#[cfg(feature = "l1-startup")]
pub async fn wait_for_l1_ready(
    l1_reader: &eez_l1::L1Reader,
    probe: ReadinessProbe,
    expected_l1_chain_id: u64,
) -> eyre::Result<()> {
    use std::time::Duration;

    const POLL: Duration = Duration::from_secs(2);
    const STALLED_POLLS: u32 = 450; // 15 min with no progress
    const MAX_POLLS: u32 = 3_600; // 2 h overall, even while it claims to sync

    let mut best_remaining = u64::MAX;
    let mut best_head = 0_u64;
    let mut stalled = 0_u32;
    let mut waited = 0_u32;
    let mut last_err: Option<String> = None;
    let mut chain_verified = false;

    loop {
        if !chain_verified {
            match l1_reader.chain_id().await {
                Ok(actual) if actual != expected_l1_chain_id => {
                    return Err(eyre::eyre!(
                        "EEZ_L1_RPC_URL serves chain {actual}, expected {expected_l1_chain_id}"
                    ));
                }
                Ok(_) => chain_verified = true,
                Err(err) => last_err = Some(format!("eth_chainId: {err}")),
            }
        }

        let (progressing, status) = match l1_reader.readiness().await {
            Err(err) => {
                last_err = Some(err.to_string());
                (false, format!("unreachable: {err}"))
            }
            Ok(state) => {
                // While L1 has no finalized block, a moving head counts as progress.
                let head_advanced = state.head_block_number > best_head;
                best_head = best_head.max(state.head_block_number);
                let block = match probe {
                    ReadinessProbe::DeployBlock(block) => Ok(Some(block)),
                    ReadinessProbe::Finalized => l1_reader
                        .finalized_block()
                        .await
                        .map(|finalized| finalized.map(|(number, _)| number)),
                };
                match block {
                    Err(err) => {
                        last_err = Some(err.to_string());
                        (false, format!("finalized block probe failed: {err}"))
                    }
                    Ok(None) => (head_advanced, "L1 has no finalized block yet".to_string()),
                    Ok(Some(block)) if state.head_block_number < block => {
                        let remaining = block - state.head_block_number;
                        let closer = remaining < best_remaining;
                        best_remaining = best_remaining.min(remaining);
                        (closer, format!("{remaining} blocks below {probe}"))
                    }
                    Ok(Some(block)) => match l1_reader.serves_history(block).await {
                        Ok(true) if chain_verified => {
                            event!(
                                name: "eez.node.l1_ready",
                                Level::INFO,
                                head = state.head_block_number,
                                block,
                                %probe,
                                "L1 source can serve our history",
                            );
                            return Ok(());
                        }
                        Ok(true) => (false, "chain id not read yet".to_string()),
                        Ok(false) => (state.syncing, format!("does not serve {probe}")),
                        Err(err) => {
                            last_err = Some(err.to_string());
                            (false, format!("history probe failed: {err}"))
                        }
                    },
                }
            }
        };

        stalled = if progressing { 0 } else { stalled + 1 };
        waited += 1;
        if waited <= 1 || waited.is_multiple_of(30) {
            event!(
                name: "eez.node.l1_not_ready",
                Level::WARN,
                %probe,
                stalled_polls = stalled,
                status,
                "waiting for L1 to serve our history",
            );
        }
        if stalled >= STALLED_POLLS || waited >= MAX_POLLS {
            let cause = last_err.unwrap_or(status);
            return Err(eyre::eyre!(
                "L1 never became able to serve {probe}: {cause}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> eyre::Result<Cli<EthereumChainSpecParser, NoRoleArgs>> {
        l2_cli(
            ["eez"]
                .iter()
                .chain(argv)
                .map(ToString::to_string)
                .collect(),
        )
    }

    #[test]
    fn cli_requires_bootnodes_with_discovery() {
        let err = parse(&["node"]).unwrap_err();
        assert_eq!(
            err.to_string(),
            "discovery is on but --bootnodes is not set; reth would dial Ethereum mainnet bootnodes"
        );
        let enode = format!("enode://{}@127.0.0.1:30303", "11".repeat(64));
        for ok in [
            &["--bootnodes", enode.as_str()][..],
            &["--disable-discovery"],
            &["-d"],
            &["--dev"],
            &["--auto-mine"],
        ] {
            assert!(parse(&[&["node"], ok].concat()).is_ok(), "{ok:?}");
        }
        // Only a running node is guarded; help and version still print.
        for argv in [&["node", "--help"][..], &["--version"]] {
            let err = parse(argv).unwrap_err();
            assert!(err.downcast_ref::<clap::Error>().is_some(), "{argv:?}");
        }
    }

    #[test]
    fn cli_adds_forced_flags() {
        for argv in [
            &["node", "-d"][..],
            &["node", "-d", "--disable-tx-gossip"],
            &["node", "-d", "--debug.startup-sync-state-idle"],
        ] {
            let Commands::Node(node) = parse(argv).unwrap().command else {
                panic!("not the node command");
            };
            assert!(node.network.disable_tx_gossip, "{argv:?}");
            assert!(node.debug.startup_sync_state_idle, "{argv:?}");
        }
    }
}
