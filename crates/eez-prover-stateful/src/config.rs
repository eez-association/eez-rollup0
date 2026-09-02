//! Environment configuration for the follower-hosted proof service.

use std::env;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use eez_proof_signer::{
    Attester, NonZeroProofSystemVkey, ServiceLimits, ServiceLimitsParams, SystemTransactionKey,
};
use tracing::warn;

/// Complete configuration for one stateful proof service.
#[derive(Debug)]
pub struct Config {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) expected_rollup_id: NonZeroU64,
    pub(crate) expected_l2_system_address: Address,
    pub(crate) attester: Attester,
    pub(crate) system_transaction_key: SystemTransactionKey,
    pub(crate) limits: ServiceLimits,
}

impl Config {
    /// Load the service when `EEZ_STATEFUL_PROOF_SIGNER_ADDR` is present.
    /// Its absence leaves follower behavior unchanged.
    pub fn from_env() -> eyre::Result<Option<Self>> {
        let Some(listen_addr) = optional("EEZ_STATEFUL_PROOF_SIGNER_ADDR")? else {
            if optional("EEZ_STATEFUL_PROOF_SIGNER_KEY")?.is_some() {
                warn!(
                    "EEZ_STATEFUL_PROOF_SIGNER_KEY is set but \
                     EEZ_STATEFUL_PROOF_SIGNER_ADDR is absent; stateful proof signer disabled",
                );
            }
            return Ok(None);
        };
        let listen_addr = parse("EEZ_STATEFUL_PROOF_SIGNER_ADDR", &listen_addr)?;
        let expected_rollup_id = parse_required("EEZ_ROLLUP_ID")?;
        let expected_l2_system_address = parse_required("EEZ_L2_SYSTEM_ADDRESS")?;
        let proof_system_vkey: NonZeroProofSystemVkey = parse_required("EEZ_VKEY")?;
        let expected_proof_system = parse_required("EEZ_PROOF_SYSTEM")?;
        let expected_attester_address: Address = parse_required("EEZ_ATTESTER_ADDRESS")?;
        let attestation_key = parse_secret("EEZ_STATEFUL_PROOF_SIGNER_KEY")?;
        let system_key = parse_secret("EEZ_L2_SYSTEM_KEY")?;

        let attester = Attester::new(
            attestation_key,
            proof_system_vkey,
            expected_proof_system,
            expected_l2_system_address,
        )?;
        eyre::ensure!(
            attester.address() == expected_attester_address,
            "EEZ_STATEFUL_PROOF_SIGNER_KEY does not match EEZ_ATTESTER_ADDRESS",
        );
        let system_transaction_key =
            SystemTransactionKey::new(system_key, expected_l2_system_address)?;
        let request_timeout_secs =
            parse_or::<NonZeroU64>("EEZ_PROOF_SIGNER_REQUEST_TIMEOUT_SECS", "240")?.get();
        if request_timeout_secs >= 300 {
            warn!(
                request_timeout_secs,
                "proof timeout reaches Reth's default 300s MDBX read-transaction limit; \
                 adjust the proof or database timeout",
            );
        }
        let limits = ServiceLimits::new(ServiceLimitsParams {
            max_window_blocks: parse_or("EEZ_PROOF_SIGNER_MAX_REQUEST_BLOCKS", "512")?,
            max_window_bytes: parse_or("EEZ_PROOF_SIGNER_MAX_REQUEST_BYTES", "536870912")?,
            max_window_witness_items: parse_or(
                "EEZ_PROOF_SIGNER_MAX_REQUEST_WITNESS_ITEMS",
                "1000000",
            )?,
            stream_idle_timeout: Duration::from_secs(
                parse_or::<NonZeroU64>("EEZ_PROOF_SIGNER_STREAM_IDLE_TIMEOUT_SECS", "120")?.get(),
            ),
            // Leave margin below Reth's default 300s MDBX read timeout.
            request_timeout: Duration::from_secs(request_timeout_secs),
        })?;

        Ok(Some(Self {
            listen_addr,
            expected_rollup_id,
            expected_l2_system_address,
            attester,
            system_transaction_key,
            limits,
        }))
    }
}

fn optional(name: &str) -> eyre::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(eyre::eyre!("{name} is not valid Unicode")),
    }
}

fn required(name: &str) -> eyre::Result<String> {
    optional(name)?.ok_or_else(|| eyre::eyre!("{name} is required for the stateful proof signer"))
}

fn parse_required<T>(name: &str) -> eyre::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    parse(name, &required(name)?)
}

fn parse_or<T>(name: &str, default: &str) -> eyre::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    parse(name, optional(name)?.as_deref().unwrap_or(default))
}

fn parse<T>(name: &str, value: &str) -> eyre::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| eyre::eyre!("{name} is invalid: {error}"))
}

fn parse_secret(name: &str) -> eyre::Result<B256> {
    required(name)?
        .parse()
        .map_err(|_| eyre::eyre!("{name} must contain exactly 32 bytes of hexadecimal"))
}
