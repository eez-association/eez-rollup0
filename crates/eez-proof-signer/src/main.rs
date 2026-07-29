//! `eez-proof-signer` — stateless validation and settlement attestation.
//!
//! The composer client-streams one posted window per `Prove` RPC: a header
//! followed by one block-and-witness chunk per declared block. The daemon
//! re-executes every block, validates the posted batch against the derived
//! execution facts, and signs the fully authorized, locally recomputed
//! public-input hash. Settlement entries consist of one anchor, then zero or
//! more success-expected single-call outbound effects, then zero or more fully
//! reconstructed successful inbound effects.
//!
//! This binary is wiring only: configuration, tracing, and serving
//! [`service::ProveSvc`]. The window discipline lives in [`window`], the
//! backend contract in [`validate`], settlement checks in [`settlement`],
//! and the RPC handling in [`service`].

#![forbid(unsafe_code)]

use std::sync::Arc;

use eyre::WrapErr as _;
use tracing::{error, info, warn};

mod attest;
mod cancel;
mod config;
mod service;
mod settlement;
#[cfg(test)]
mod testkit;
mod validate;
mod window;

/// Fixed L2 address used for cross-chain transaction and event checks.
pub(crate) const EEZL2_ADDRESS: alloy_primitives::Address =
    alloy_primitives::address!("4200000000000000000000000000000000000007");

#[tokio::main]
async fn main() -> eyre::Result<()> {
    init_tracing()?;
    let config = config::Config::load()?;

    let listen_addr = config.listen_addr;
    let limits = config.limits;
    let validator = validate::Validator::stateless(&config.chain_document_path)?;
    if !listen_addr.ip().is_loopback() {
        warn!(
            listen = %listen_addr,
            "Prove is binding beyond loopback; restrict this operator endpoint to authorized composers",
        );
    }
    let window_limits = limits.window_limits();
    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %listen_addr,
        max_request_blocks = window_limits.blocks,
        max_request_bytes = window_limits.payload_bytes,
        max_request_witness_items = window_limits.witness_items,
        max_transaction_state_checkpoints = limits.max_transaction_state_checkpoints(),
        max_decoding_message_bytes = limits.max_decoding_message_bytes(),
        stream_idle_timeout_secs = limits.stream_idle_timeout().as_secs(),
        request_timeout_secs = limits.request_timeout().as_secs(),
        validator = validator.label(),
        expected_rollup_id = config.expected_rollup_id.get(),
        attester = %config.attester.address(),
        expected_proof_system = %config.attester.expected_proof_system(),
        proof_system_vkey = %config.attester.proof_system_vkey(),
        l2_chain_id = validator.chain_id(),
        system_address = %eez_protocol::SYSTEM_ADDRESS,
        profile = "anchor_single_call_outbound_then_inbound",
        "serving Prove — waiting for composer windows",
    );
    let svc = service::ProveSvc::new(
        Arc::new(service::ServiceState::new(
            validator,
            config.expected_rollup_id,
            config.attester,
            config.system_transaction_key,
        )),
        limits,
    );
    let shutdown_service = svc.clone();
    let serve_result = tonic::transport::Server::builder()
        .add_service(svc.into_server())
        .serve_with_shutdown(listen_addr, shutdown_signal())
        .await;
    // A timed-out RPC may leave non-interruptible blocking work running after
    // tonic has drained its request future. Do not terminate that work midway.
    shutdown_service.wait_until_idle().await;
    serve_result.wrap_err_with(|| format!("serve {listen_addr}"))
}

/// Stop accepting connections on Ctrl-C or SIGTERM, degrading to Ctrl-C only
/// when the SIGTERM handler cannot be installed.
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

/// Stop accepting connections on Ctrl-C on platforms without Unix signals.
#[cfg(not(unix))]
async fn shutdown_signal() {
    wait_for_ctrl_c().await;
    info!("shutdown requested; draining the active Prove request");
}

/// Wait for Ctrl-C without turning listener failure into a false shutdown.
async fn wait_for_ctrl_c() {
    if let Err(signal_error) = tokio::signal::ctrl_c().await {
        error!(error = %signal_error, "Ctrl-C handler failed");
        std::future::pending::<()>().await;
    }
}

/// Configure tracing from `RUST_LOG`. An absent variable selects `info`;
/// invalid filter syntax or a non-Unicode value fails startup.
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
