//! Bounded streaming sessions, execution validation, settlement, and attestation.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use eez_control_rpc::v1::prover_server::ProverServer;

use crate::{attest::Attester, settlement, validate};

mod sessions;
mod settlement_job;

pub(crate) use settlement_job::AttestablePublicInputsHash;

/// Absolute ceiling for one decoded `ClientFrame`; the configured aggregate
/// request-byte limit may lower the effective per-message limit.
const MAX_DECODING_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// Responses carry bounded cursors, rejection details, or one attestation.
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
/// The operation deadline covers block validation or finalization; the idle
/// timeout applies to each streamed-message wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLimits {
    pub(crate) max_blocks: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) max_witness_items: usize,
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
    backend: Box<dyn validate::ValidationBackend>,
    expected_rollup_id: NonZeroU64,
    expected_l2_system_address: Address,
    attester: Attester,
    system_transaction_reconstructor: settlement::SystemTransactionReconstructor,
}

impl ServiceState {
    /// Bind system-transaction reconstruction to the same configured L2 chain
    /// and expected rollup identities used by validation and settlement.
    pub fn new(
        backend: impl validate::ValidationBackend,
        expected_rollup_id: NonZeroU64,
        attester: Attester,
    ) -> eyre::Result<Self> {
        let expected_l2_system_address = eez_primitives::SYSTEM_ADDRESS;
        eyre::ensure!(
            backend.expected_l2_system_address() == expected_l2_system_address,
            "validator and native system transactions use different L2 system addresses"
        );
        eyre::ensure!(
            attester.expected_l2_system_address() == expected_l2_system_address,
            "attester and native system transactions use different L2 system addresses"
        );
        let system_transaction_reconstructor =
            settlement::SystemTransactionReconstructor::new(backend.chain_id(), expected_rollup_id);
        Ok(Self {
            backend: Box::new(backend),
            expected_rollup_id,
            expected_l2_system_address,
            attester,
            system_transaction_reconstructor,
        })
    }
}

/// The streaming prover service. Cheap clones share the session runtime.
#[derive(Debug, Clone)]
pub struct ProveSvc {
    sessions: Arc<sessions::SessionRuntime>,
}

impl ProveSvc {
    pub fn new(state: Arc<ServiceState>, limits: ServiceLimits) -> Self {
        let sessions = sessions::SessionRuntime::new(state, limits);
        Self { sessions }
    }

    /// Wait for active streams after the listener stops accepting requests.
    pub async fn wait_until_idle(&self) {
        self.sessions.wait_until_idle().await;
    }

    /// Build the gRPC server with the configured request and response size limits.
    pub fn into_server(self) -> ProverServer<Self> {
        let message_bytes = self.sessions.limits.max_decoding_message_bytes();
        ProverServer::new(self)
            .max_decoding_message_size(message_bytes)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_BYTES)
    }
}

#[cfg(test)]
mod tests;
