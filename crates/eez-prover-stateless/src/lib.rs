//! Witness-backed proof-signer backend and standalone daemon.

#![forbid(unsafe_code)]

use eez_proof_signer::ServerConfig;
use eyre::WrapErr as _;
use tracing::{error, info};

mod backend;
mod config;

#[cfg(test)]
mod testkit {
    use alloy_primitives::{Address, address};

    pub const TEST_SYSTEM_ADDRESS_ARG: &str = "f39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
    pub const TEST_SYSTEM_ADDRESS: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
    pub const SYSTEM_TX: &str = "f85f8001825208944200000000000000000000000000000000000007808026a0ed95c78ea14cbb6af669c61f27c5fb7fb0192101d4d706d055ab9ff9895c9f66a027c2e67303de8fa1cad36d0e59298a98df684e54295eb5f61ab99609c1738f73";
}

pub use backend::Backend;

#[cfg(test)]
mod service_tests;

/// Run the standalone stateless proof-signer daemon.
pub async fn run() -> eyre::Result<()> {
    init_tracing()?;
    let config = config::Config::load()?;

    let listen_addr = config.listen_addr;
    let limits = config.limits;
    let expected_l2_system_address = config.expected_l2_system_address;
    let backend = Backend::from_chain_document_file(&config.chain_document_path)?;
    let chain_id = backend.chain_id();
    eez_proof_signer::serve(
        ServerConfig {
            listen_addr,
            limits,
            chain_id,
            expected_rollup_id: config.expected_rollup_id,
            expected_l2_system_address,
            attester: config.attester,
            system_transaction_key: config.system_transaction_key,
        },
        backend,
        shutdown_signal(),
    )
    .await
}

/// Stop accepting connections on Ctrl-C or SIGTERM.
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = wait_for_ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        Err(signal_error) => {
            error!(error = %signal_error, "failed to install SIGTERM handler");
            wait_for_ctrl_c().await;
        }
    }
    info!("shutdown requested; draining the active Prove request");
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    wait_for_ctrl_c().await;
    info!("shutdown requested; draining the active Prove request");
}

async fn wait_for_ctrl_c() {
    if let Err(signal_error) = tokio::signal::ctrl_c().await {
        error!(error = %signal_error, "Ctrl-C handler failed");
        std::future::pending::<()>().await;
    }
}

fn init_tracing() -> eyre::Result<()> {
    use tracing_subscriber::EnvFilter;
    let filter = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(spec) => {
            EnvFilter::try_new(&spec).map_err(|e| eyre::eyre!("invalid RUST_LOG `{spec}`: {e}"))?
        }
        Err(std::env::VarError::NotPresent) => EnvFilter::new("info"),
        Err(e) => return Err(eyre::eyre!("RUST_LOG: {e}")),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|error| eyre::eyre!(error))
        .wrap_err("install tracing subscriber")
}
