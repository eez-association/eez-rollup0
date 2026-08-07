//! Daemon configuration.
//!
//! Every option is a CLI flag with an environment-variable fallback: the
//! flag overrides the variable, and a variable set to the empty string
//! counts as set (clap's native semantics).

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use clap::Parser;

use crate::attest::{Attester, NonZeroProofSystemVkey};
use crate::service::{ServiceLimits, ServiceLimitsParams};
use crate::settlement::SystemTransactionKey;

/// An infallibly parsed CLI secret that retains only decoded bytes.
///
/// Invalid syntax is represented without preserving the input and is reported
/// after clap, so neither `Debug` nor clap diagnostics can echo key material.
// Clap's generated parser requires value types to be cloneable while merging
// command-line and environment sources.
#[derive(Clone)]
struct SecretKeyArg(Option<B256>);

impl FromStr for SecretKeyArg {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.parse().ok()))
    }
}

impl std::fmt::Debug for SecretKeyArg {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl SecretKeyArg {
    /// Return the decoded key or a redacted syntax error.
    fn into_key(self, name: &str) -> eyre::Result<B256> {
        self.0
            .ok_or_else(|| eyre::eyre!("{name} key must contain exactly 32 bytes of hexadecimal"))
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "eez-proof-signer",
    about = "EEZ window validation and attestation daemon"
)]
struct Args {
    /// The `Prove` gRPC bind address the composer dials.
    #[arg(long, env = "EEZ_PROOF_SIGNER_ADDR", default_value = "127.0.0.1:50061")]
    listen_addr: SocketAddr,

    /// Path to an operator-configured Alloy `ChainConfig` or complete `Genesis` document.
    #[arg(long = "chain-config", env = "EEZ_CHAIN_CONFIG")]
    chain_document_path: PathBuf,

    /// Expected nonzero L1 rollup-registry ID this signer serves.
    ///
    /// This is the `rollupId` used by the L1 settlement/registry contracts,
    /// not an EIP-155 chain ID.
    #[arg(long = "rollup-id", env = "EEZ_ROLLUP_ID")]
    expected_rollup_id: NonZeroU64,

    /// Operator-configured, non-zero proof-system verification key.
    #[arg(long = "vkey", env = "EEZ_VKEY")]
    proof_system_vkey: NonZeroProofSystemVkey,

    /// Private secp256k1 key used to sign the fully authorized, locally
    /// recomputed public-input hash.
    /// It must not derive the reserved L2 system address.
    #[arg(
        long = "signer-key",
        env = "EEZ_PROOF_SIGNER_KEY",
        value_name = "32-BYTE-HEX",
        hide_env_values = true
    )]
    attestation_key: SecretKeyArg,

    /// Expected attester address configured in the deployed proof system.
    #[arg(long = "attester-address", env = "EEZ_ATTESTER_ADDRESS")]
    expected_attester_address: Address,

    /// Private secp256k1 key for the deployment-configured L2 system address,
    /// used to reconstruct system transactions omitted from Sync-block DA.
    #[arg(
        long = "l2-system-key",
        env = "EEZ_L2_SYSTEM_KEY",
        value_name = "32-BYTE-HEX",
        hide_env_values = true
    )]
    system_transaction_key: SecretKeyArg,

    /// L2 system address embedded in the deployed EEZL2 contract.
    #[arg(long = "l2-system-address", env = "EEZ_L2_SYSTEM_ADDRESS")]
    expected_l2_system_address: Address,

    /// Expected non-zero proof-system contract selected by the operator.
    #[arg(long = "proof-system", env = "EEZ_PROOF_SYSTEM")]
    expected_proof_system: Address,

    /// Maximum blocks declared by one `Prove` request.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_MAX_REQUEST_BLOCKS",
        default_value = "512"
    )]
    max_request_blocks: NonZeroUsize,

    /// Aggregate canonical known-field protobuf bytes allowed per request.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_MAX_REQUEST_BYTES",
        default_value = "536870912"
    )]
    max_request_bytes: NonZeroUsize,

    /// Aggregate witness-array elements allowed per request.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_MAX_REQUEST_WITNESS_ITEMS",
        default_value = "1000000"
    )]
    max_request_witness_items: NonZeroUsize,

    /// Maximum transaction-state checkpoints derived from the settling block.
    /// A zero limit rejects every settling block with an effect-candidate boundary.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS",
        default_value = "8"
    )]
    max_transaction_state_checkpoints: usize,

    /// Maximum silence while awaiting the next streamed message.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_STREAM_IDLE_TIMEOUT_SECS",
        default_value = "120"
    )]
    stream_idle_timeout_secs: NonZeroU64,

    /// End-to-end deadline for ingestion, validation, settlement, and signing.
    #[arg(
        long,
        env = "EEZ_PROOF_SIGNER_REQUEST_TIMEOUT_SECS",
        default_value = "600"
    )]
    request_timeout_secs: NonZeroU64,
}

/// Runtime configuration assembled from CLI and environment input.
#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) chain_document_path: PathBuf,
    /// Expected L1 rollup-registry ID, distinct from the L2 EIP-155 chain ID.
    pub(crate) expected_rollup_id: NonZeroU64,
    /// Deployment-configured privileged L2 transaction signer.
    pub(crate) expected_l2_system_address: Address,
    pub(crate) attester: Attester,
    pub(crate) system_transaction_key: SystemTransactionKey,
    pub(crate) limits: ServiceLimits,
}

impl Config {
    /// Parse CLI and environment input, then validate keys and service limits.
    pub(crate) fn load() -> eyre::Result<Self> {
        Self::from_args(Args::parse())
    }

    fn from_args(args: Args) -> eyre::Result<Self> {
        let attestation_private_key = args.attestation_key.into_key("attestation")?;
        let attester = Attester::new(
            attestation_private_key,
            args.proof_system_vkey,
            args.expected_proof_system,
            args.expected_l2_system_address,
        )
        .map_err(eyre::Report::new)?;
        eyre::ensure!(
            attester.address() == args.expected_attester_address,
            "attestation key does not match the expected attester address"
        );
        let system_transaction_key = SystemTransactionKey::new(
            args.system_transaction_key.into_key("L2 system")?,
            args.expected_l2_system_address,
        )
        .map_err(eyre::Report::new)?;
        Ok(Self {
            listen_addr: args.listen_addr,
            chain_document_path: args.chain_document_path,
            expected_rollup_id: args.expected_rollup_id,
            expected_l2_system_address: args.expected_l2_system_address,
            attester,
            system_transaction_key,
            limits: ServiceLimits::new(ServiceLimitsParams {
                max_window_blocks: args.max_request_blocks,
                max_window_bytes: args.max_request_bytes,
                max_window_witness_items: args.max_request_witness_items,
                max_transaction_state_checkpoints: args.max_transaction_state_checkpoints,
                stream_idle_timeout: Duration::from_secs(args.stream_idle_timeout_secs.get()),
                request_timeout: Duration::from_secs(args.request_timeout_secs.get()),
            })?,
        })
    }
}

#[cfg(test)]
mod tests;
