//! Shared, role-neutral bootstrap substrate for eez node binaries.

use clap::Parser as _;
use mimalloc::MiMalloc;
use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
use reth_node_builder::{NodeBuilder, WithLaunchContext};
use tracing::{Level, event};

mod payload;
pub use payload::EezPayloadBuilder;

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
/// Returns an error when command-line arguments are invalid.
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

    // Engine API FCUs may intentionally unwind the L2 head during recovery.
    let mut argv: Vec<String> = std::env::args().collect();
    for flag in [
        "--engine.always-process-payload-attributes-on-canonical-head",
        "--engine.allow-unwind-canonical-header",
    ] {
        if !argv.iter().any(|a| a == flag) {
            argv.push(flag.to_owned());
        }
    }

    Ok(Cli::<EthereumChainSpecParser, Ext>::try_parse_from(argv)?)
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

/// Blocks until L1 serves the configured history and matches the expected chain.
///
/// # Errors
///
/// Returns an error for a mismatched chain or when the L1 source fails to make
/// progress within the bounded startup window.
#[cfg(feature = "l1-startup")]
pub async fn wait_for_l1_ready(
    l1_reader: &eez_l1::L1Reader,
    deploy_block: u64,
    expected_l1_chain_id: u64,
) -> eyre::Result<()> {
    use std::time::Duration;

    const POLL: Duration = Duration::from_secs(2);
    const STALLED_POLLS: u32 = 450; // 15 min with no progress
    const MAX_POLLS: u32 = 3_600; // 2 h overall, even while it claims to sync

    let mut best_remaining = u64::MAX;
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
            Ok(state) if state.head_block_number < deploy_block => {
                let remaining = deploy_block - state.head_block_number;
                let closer = remaining < best_remaining;
                best_remaining = best_remaining.min(remaining);
                (closer, format!("{remaining} blocks below the deploy block"))
            }
            Ok(state) => match l1_reader.serves_history(deploy_block).await {
                Ok(true) if chain_verified => {
                    event!(
                        name: "eez.node.l1_ready",
                        Level::INFO,
                        head = state.head_block_number,
                        deploy_block,
                        "L1 source can serve our history",
                    );
                    return Ok(());
                }
                Ok(true) => (false, "chain id not read yet".to_string()),
                Ok(false) => (state.syncing, "does not serve the deploy block".to_string()),
                Err(err) => {
                    last_err = Some(err.to_string());
                    (false, format!("history probe failed: {err}"))
                }
            },
        };

        stalled = if progressing { 0 } else { stalled + 1 };
        waited += 1;
        if waited <= 1 || waited.is_multiple_of(30) {
            event!(
                name: "eez.node.l1_not_ready",
                Level::WARN,
                deploy_block,
                stalled_polls = stalled,
                status,
                "waiting for L1 to serve our history",
            );
        }
        if stalled >= STALLED_POLLS || waited >= MAX_POLLS {
            let cause = last_err.unwrap_or(status);
            return Err(eyre::eyre!(
                "L1 never became able to serve block {deploy_block}: {cause}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}
