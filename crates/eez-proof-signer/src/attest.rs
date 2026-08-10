//! Mandatory signing identity available only after the complete
//! validation-and-settlement pipeline succeeds.

use std::fmt;
use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes};
use eez_protocol::{EcdsaProofSigner, SignerError};
use thiserror::Error;

use crate::service::AttestablePublicInputsHash;

/// Operator-configured, non-zero proof-system verification key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NonZeroProofSystemVkey(B256);

impl NonZeroProofSystemVkey {
    pub(crate) fn new(value: B256) -> Option<Self> {
        (value != B256::ZERO).then_some(Self(value))
    }

    pub(crate) const fn get(self) -> B256 {
        self.0
    }
}

impl FromStr for NonZeroProofSystemVkey {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.parse::<B256>().map_err(|error| error.to_string())?;
        Self::new(value).ok_or_else(|| "vkey must be non-zero".to_owned())
    }
}

impl fmt::Display for NonZeroProofSystemVkey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A complete operator-configured attestation identity.
///
/// Keeping the signer, verification key, and proof-system address together
/// prevents the service from being constructed with only part of its
/// attestation configuration. The proof-system vkey is deliberately
/// independent from the signing address: deployments may register a
/// proof-system-specific vkey.
/// The signing address must also differ from the deployment-configured L2
/// system address, whose key has separate authority to create system
/// transactions.
pub(crate) struct Attester {
    signer: EcdsaProofSigner,
    proof_system_vkey: NonZeroProofSystemVkey,
    expected_proof_system: Address,
    expected_l2_system_address: Address,
}

impl fmt::Debug for Attester {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Attester")
            .field("address", &self.address())
            .field("proof_system_vkey", &self.proof_system_vkey)
            .field("expected_proof_system", &self.expected_proof_system)
            .field(
                "expected_l2_system_address",
                &self.expected_l2_system_address,
            )
            .finish()
    }
}

/// Startup errors that are safe to display without revealing key material.
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum AttesterConfigError {
    #[error("attestation key is not a valid secp256k1 private key")]
    InvalidPrivateKey,
    #[error("attestation key must not derive the reserved L2 system address")]
    ReservedSystemAddress,
    #[error("proof-system address must be non-zero")]
    ZeroProofSystem,
}

impl Attester {
    /// Construct one complete identity, rejecting invalid or reserved signing
    /// keys and a zero proof-system address before the server starts.
    pub(crate) fn new(
        attestation_private_key: B256,
        proof_system_vkey: NonZeroProofSystemVkey,
        expected_proof_system: Address,
        expected_l2_system_address: Address,
    ) -> Result<Self, AttesterConfigError> {
        if expected_proof_system == Address::ZERO {
            return Err(AttesterConfigError::ZeroProofSystem);
        }
        let signer = EcdsaProofSigner::from_private_key(attestation_private_key)
            .map_err(|_| AttesterConfigError::InvalidPrivateKey)?;
        // System-transaction signing and public-input attestation are separate
        // authorities; one key must never confer both capabilities.
        if signer.address() == expected_l2_system_address {
            return Err(AttesterConfigError::ReservedSystemAddress);
        }
        Ok(Self {
            signer,
            proof_system_vkey,
            expected_proof_system,
            expected_l2_system_address,
        })
    }

    /// Return the attestation address derived from the configured private key.
    pub(crate) fn address(&self) -> Address {
        self.signer.address()
    }

    /// Return the independently configured proof-system verification key.
    pub(crate) const fn proof_system_vkey(&self) -> NonZeroProofSystemVkey {
        self.proof_system_vkey
    }

    /// Return the expected proof-system contract address selected at startup.
    pub(crate) const fn expected_proof_system(&self) -> Address {
        self.expected_proof_system
    }

    /// Return the deployment identity excluded from attestation authority.
    pub(crate) const fn expected_l2_system_address(&self) -> Address {
        self.expected_l2_system_address
    }

    /// Sign a digest authorized by the complete request pipeline, without an
    /// EIP-191 prefix.
    pub(crate) fn sign(
        &self,
        public_inputs_hash: AttestablePublicInputsHash,
    ) -> Result<Bytes, SignerError> {
        self.signer.sign_prehash(public_inputs_hash.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, b256};

    use super::*;
    use crate::testkit::{SYSTEM_PRIVATE_KEY, TEST_SYSTEM_ADDRESS};

    fn test_proof_system_vkey() -> NonZeroProofSystemVkey {
        NonZeroProofSystemVkey::new(B256::repeat_byte(0x42)).unwrap()
    }

    fn test_proof_system() -> Address {
        address!("00000000000000000000000000000000000000aa")
    }

    #[test]
    fn accepts_an_attestation_key_distinct_from_the_system_identity() {
        // Anvil account #1. Test-only and intentionally public.
        let attestation_key =
            b256!("59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");

        let attester = Attester::new(
            attestation_key,
            test_proof_system_vkey(),
            test_proof_system(),
            TEST_SYSTEM_ADDRESS,
        )
        .unwrap();

        assert_eq!(
            attester.address(),
            address!("70997970c51812dc3a010c7d01b50e0d17dc79c8")
        );
    }

    #[test]
    fn system_identity_is_selected_by_deployment() {
        let attester = Attester::new(
            SYSTEM_PRIVATE_KEY,
            test_proof_system_vkey(),
            test_proof_system(),
            Address::repeat_byte(0xbb),
        )
        .unwrap();

        assert_eq!(attester.address(), TEST_SYSTEM_ADDRESS);
    }

    #[test]
    fn rejects_the_reserved_system_identity_without_exposing_its_key() {
        let error = Attester::new(
            SYSTEM_PRIVATE_KEY,
            test_proof_system_vkey(),
            test_proof_system(),
            TEST_SYSTEM_ADDRESS,
        )
        .unwrap_err();
        let displayed = error.to_string();

        assert_eq!(error, AttesterConfigError::ReservedSystemAddress);
        assert_eq!(
            displayed,
            "attestation key must not derive the reserved L2 system address"
        );
        assert!(!displayed.contains(&SYSTEM_PRIVATE_KEY.to_string()));
    }
}
