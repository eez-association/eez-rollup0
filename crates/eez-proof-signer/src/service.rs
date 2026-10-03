//! The `Prove` RPC service: bounded ingestion, validation, and attestation.
//!
//! Each RPC consumes one window. `WindowAssembler` enforces stream structure
//! and quotas, [`crate::validate`] re-executes its blocks, and the settlement
//! stage binds the posted batch to the execution facts. The
//! attester signs only after every stage succeeds.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use eez_control_rpc::v1::prover_server::ProverServer;
use eez_control_rpc::v2::prover_server::ProverServer as StreamingProverServer;
use tokio::sync::Semaphore;

use crate::{attest::Attester, settlement, validate, window};

mod incremental;
mod rpc;
mod settlement_job;
mod stream;

pub(crate) use settlement_job::AttestablePublicInputsHash;

/// Absolute ceiling for one decoded `ProveChunk`; the configured aggregate
/// request-byte limit may lower the effective per-message limit.
const MAX_DECODING_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// `ProveResponse` contains only a 32-byte digest and 65-byte signature.
const MAX_ENCODING_MESSAGE_BYTES: usize = 1024;

/// Named construction inputs for [`ServiceLimits`].
#[derive(Debug, Clone, Copy)]
pub struct ServiceLimitsParams {
    pub max_window_blocks: NonZeroUsize,
    pub max_window_bytes: NonZeroUsize,
    pub max_window_witness_items: NonZeroUsize,
    pub stream_idle_timeout: Duration,
    pub request_timeout: Duration,
}

/// Runtime limits shared by every service clone.
///
/// The request deadline covers ingestion through signing; the idle timeout
/// applies to each streamed-message wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLimits {
    max_blocks: usize,
    max_payload_bytes: usize,
    max_witness_items: usize,
    stream_idle_timeout: Duration,
    request_timeout: Duration,
}

impl ServiceLimits {
    pub fn new(params: ServiceLimitsParams) -> eyre::Result<Self> {
        let ServiceLimitsParams {
            max_window_blocks,
            max_window_bytes,
            max_window_witness_items,
            stream_idle_timeout,
            request_timeout,
        } = params;
        let now = Instant::now();
        for (name, timeout) in [
            ("stream idle timeout", stream_idle_timeout),
            ("request timeout", request_timeout),
        ] {
            eyre::ensure!(!timeout.is_zero(), "{name} must be non-zero");
            eyre::ensure!(now.checked_add(timeout).is_some(), "{name} is out of range");
        }
        Ok(Self {
            max_blocks: max_window_blocks.get(),
            max_payload_bytes: max_window_bytes.get(),
            max_witness_items: max_window_witness_items.get(),
            stream_idle_timeout,
            request_timeout,
        })
    }

    /// Adapt the shared service quotas for v1 window admission.
    pub fn window_limits(self) -> window::WindowLimits {
        window::WindowLimits {
            blocks: self.max_blocks,
            payload_bytes: self.max_payload_bytes,
            witness_items: self.max_witness_items,
        }
    }

    pub const fn stream_idle_timeout(self) -> Duration {
        self.stream_idle_timeout
    }

    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    pub fn max_decoding_message_bytes(self) -> usize {
        self.max_payload_bytes.min(MAX_DECODING_MESSAGE_BYTES)
    }
}

/// Immutable dependencies shared by all service clones.
#[derive(Debug)]
pub struct ServiceState {
    validator: validate::Validator,
    expected_rollup_id: NonZeroU64,
    expected_l2_system_address: Address,
    attester: Attester,
    system_transaction_reconstructor: settlement::SystemTransactionReconstructor,
}

impl ServiceState {
    /// Bind system-transaction reconstruction to the same configured L2 chain
    /// and expected rollup identities used by validation and settlement.
    pub fn new(
        validator: validate::Validator,
        expected_rollup_id: NonZeroU64,
        attester: Attester,
    ) -> eyre::Result<Self> {
        let expected_l2_system_address = eez_primitives::SYSTEM_ADDRESS;
        eyre::ensure!(
            validator.expected_l2_system_address() == expected_l2_system_address,
            "validator and native system transactions use different L2 system addresses"
        );
        eyre::ensure!(
            attester.expected_l2_system_address() == expected_l2_system_address,
            "attester and native system transactions use different L2 system addresses"
        );
        let system_transaction_reconstructor = settlement::SystemTransactionReconstructor::new(
            validator.chain_id(),
            expected_rollup_id,
        );
        Ok(Self {
            validator,
            expected_rollup_id,
            expected_l2_system_address,
            attester,
            system_transaction_reconstructor,
        })
    }
}

/// The `Prove` service. All cheap clones share state and one active-request slot.
#[derive(Debug, Clone)]
pub struct ProveSvc {
    state: Arc<ServiceState>,
    limits: ServiceLimits,
    active_request_slot: Arc<Semaphore>,
    incremental: Arc<incremental::IncrementalRuntime>,
}

impl ProveSvc {
    pub fn new(state: Arc<ServiceState>, limits: ServiceLimits) -> Self {
        let incremental = incremental::IncrementalRuntime::new(Arc::clone(&state), limits);
        Self {
            state,
            limits,
            active_request_slot: Arc::new(Semaphore::new(1)),
            incremental,
        }
    }

    /// Wait until no admitted request or detached request worker holds the slot.
    ///
    /// Observes idleness by acquiring the active-request slot, so it must run only
    /// after the listener stops accepting requests; called while serving it
    /// would briefly occupy the slot and reject an incoming `Prove`.
    pub async fn wait_until_idle(&self) {
        drop(
            self.active_request_slot
                .acquire()
                .await
                .expect("the active-request semaphore is never closed"),
        );
        self.incremental.wait_until_idle().await;
    }

    /// Build the gRPC server with the configured request and response size limits.
    pub fn into_server(self) -> ProverServer<Self> {
        let message_bytes = self.limits.max_decoding_message_bytes();
        ProverServer::new(self)
            .max_decoding_message_size(message_bytes)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_BYTES)
    }

    /// Build the incremental v2 gRPC server with the same message limits.
    pub fn into_streaming_server(self) -> StreamingProverServer<Self> {
        let message_bytes = self.limits.max_decoding_message_bytes();
        StreamingProverServer::new(self)
            .max_decoding_message_size(message_bytes)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_BYTES)
    }
}

#[cfg(test)]
mod tests;
