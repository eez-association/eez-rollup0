//! Shared proof-signer request pipeline.
//!
//! Proof backends plug into the same stream admission, settlement validation,
//! and attestation code through [`validate::ValidationBackend`].

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;

use alloy_primitives::Address;
use eyre::WrapErr as _;
use tracing::{info, warn};

pub mod attest;
pub mod cancel;
pub mod service;
mod settlement;
pub mod validate;
pub mod window;

#[cfg(test)]
mod testkit;

/// Fixed L2 predeploy used for cross-chain transaction and event checks.
pub use eez_protocol::EEZL2_ADDRESS;

pub use attest::{Attester, NonZeroProofSystemVkey};
pub use service::{ProveSvc, ServiceLimits, ServiceLimitsParams, ServiceState};
pub use settlement::SystemTransactionKey;
pub use validate::{ValidationBackend, Validator};

/// Backend-neutral configuration for one proof-signer service.
#[derive(Debug)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub limits: ServiceLimits,
    pub chain_id: u64,
    pub expected_rollup_id: NonZeroU64,
    pub expected_l2_system_address: Address,
    pub attester: Attester,
    pub system_transaction_key: SystemTransactionKey,
}

/// Initialize one backend and serve the shared proof-signer pipeline.
pub async fn serve(
    config: ServerConfig,
    backend: impl ValidationBackend,
    shutdown: impl Future<Output = ()>,
) -> eyre::Result<()> {
    let ServerConfig {
        listen_addr,
        limits,
        chain_id,
        expected_rollup_id,
        expected_l2_system_address,
        attester,
        system_transaction_key,
    } = config;
    let validator = Validator::from_backend(backend, chain_id, expected_l2_system_address)?;
    log_server_config(
        listen_addr,
        limits,
        &validator,
        expected_rollup_id.get(),
        &attester,
    );
    let svc = ProveSvc::new(
        Arc::new(ServiceState::new(
            validator,
            expected_rollup_id,
            attester,
            system_transaction_key,
        )?),
        limits,
    );
    if !listen_addr.ip().is_loopback() {
        warn!(
            listen = %listen_addr,
            "Prove is binding beyond loopback; restrict this operator endpoint to authorized composers",
        );
    }
    let shutdown_service = svc.clone();
    let serve_result = tonic::transport::Server::builder()
        .add_service(svc.into_server())
        .serve_with_shutdown(listen_addr, shutdown)
        .await;
    shutdown_service.wait_until_idle().await;
    serve_result.wrap_err_with(|| format!("serve {listen_addr}"))
}

/// Emit the common startup description for the configured validation backend.
fn log_server_config(
    listen_addr: SocketAddr,
    limits: ServiceLimits,
    validator: &Validator,
    expected_rollup_id: u64,
    attester: &Attester,
) {
    let window_limits = limits.window_limits();
    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %listen_addr,
        max_request_blocks = window_limits.blocks,
        max_request_bytes = window_limits.payload_bytes,
        max_request_witness_items = window_limits.witness_items,
        max_decoding_message_bytes = limits.max_decoding_message_bytes(),
        stream_idle_timeout_secs = limits.stream_idle_timeout().as_secs(),
        request_timeout_secs = limits.request_timeout().as_secs(),
        validator = validator.label(),
        expected_rollup_id,
        attester = %attester.address(),
        expected_proof_system = %attester.expected_proof_system(),
        proof_system_vkey = %attester.proof_system_vkey(),
        l2_chain_id = validator.chain_id(),
        expected_l2_system_address = %validator.expected_l2_system_address(),
        profile = "anchor_single_call_outbound_then_inbound",
        "serving Prove — waiting for composer windows",
    );
}
