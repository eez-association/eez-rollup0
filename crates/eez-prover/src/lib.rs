//! Prover abstraction + stage-2 mock.
//!
//! A [`Prover`] turns proving context into the `proof` bytes that the
//! matching on-chain `IProofSystem.verify` accepts. Stage 2 ships
//! [`MockEcdsaProver`], which signs a fixed digest agreed with
//! [`MockECDSAProofSystem`]'s `MOCK_PROVER_DIGEST` — the proof does **not**
//! bind to the batch. A real prover (zk or stateless-execution ECDSA)
//! takes the calldata + chain context, runs the STF itself, derives the
//! per-rollup hashes, and produces a proof that commits to them; that
//! redesign reshapes [`ProvingContext`] but keeps the trait surface.
//!
//! The EEZ `sol!` ABI binding (structs, `postAndVerifyBatch`, and the
//! `BatchPosted` / `L2ExecutionPerformed` events) lives in `eez-evm` —
//! the single ABI source the whole workspace shares. This crate is
//! just the prover abstraction.
//!
//! [`MockECDSAProofSystem`]: ../../../contracts/src/MockECDSAProofSystem.sol

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use alloy_primitives::{Address, B256, Bytes, b256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use thiserror::Error;

/// Result alias.
pub type ProverResult<T> = Result<T, ProverError>;

/// Error returned by [`Prover::prove`].
#[derive(Debug, Error)]
pub enum ProverError {
    /// Underlying signer rejected the digest.
    #[error("signer error: {0}")]
    Signer(String),
}

/// Inputs the prover needs to produce a proof.
///
/// Unit-shaped in stage 2 — [`MockEcdsaProver`] signs a fixed digest and
/// needs nothing else. A real prover will reshape this to carry calldata
/// and chain context (prev state root, prev tip hash, …) once that
/// design lands.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProvingContext;

/// Turns proving context into `proof` bytes the matching on-chain
/// `IProofSystem.verify` accepts.
#[async_trait]
pub trait Prover: Send + Sync + std::fmt::Debug {
    /// Produce a proof.
    ///
    /// # Errors
    ///
    /// Implementation-defined. The stage-2 mock returns
    /// [`ProverError::Signer`] if the underlying ECDSA signer rejects the
    /// digest; a real prover will surface circuit / execution errors.
    async fn prove(&self, ctx: ProvingContext) -> ProverResult<Bytes>;

    /// Registry-membership key for this prover. The per-rollup
    /// `IRollupContract` records this in its vkey map; EEZ reads it
    /// when checking proof-system membership.
    fn vkey(&self) -> B256;
}

/// Devnet-only ECDSA mock. Signs [`MOCK_PROVER_DIGEST`] and returns
/// `r || s || v` (65 bytes) — the proof does **not** commit to any batch.
/// The matching `MockECDSAProofSystem` contract recovers against the same
/// digest. A real prover replaces both sides with a binding scheme.
#[derive(Debug, Clone)]
pub struct MockEcdsaProver {
    signer: PrivateKeySigner,
}

/// Fixed digest signed by [`MockEcdsaProver`] and recovered against by
/// `MockECDSAProofSystem.verify`. Equals `keccak256("eez-mock-prover")`.
/// Both sides MUST agree on this value bit-for-bit — if you change it,
/// change it in `contracts/src/MockECDSAProofSystem.sol` too.
pub const MOCK_PROVER_DIGEST: B256 =
    b256!("0x02753eb401fed50317a35a1cfa1c67c003b761ba4009cbe36632c724ef0a06df");

impl MockEcdsaProver {
    /// Build a mock prover from a private key signer.
    #[must_use]
    pub const fn new(signer: PrivateKeySigner) -> Self {
        Self { signer }
    }

    /// Address whose key signs [`MOCK_PROVER_DIGEST`]. Must match the
    /// `authorizedSigner` baked into the deployed `MockECDSAProofSystem`.
    #[must_use]
    pub fn address(&self) -> alloy_primitives::Address {
        self.signer.address()
    }
}

#[async_trait]
impl Prover for MockEcdsaProver {
    async fn prove(&self, _ctx: ProvingContext) -> ProverResult<Bytes> {
        let sig = self
            .signer
            .sign_hash_sync(&MOCK_PROVER_DIGEST)
            .map_err(|e| ProverError::Signer(e.to_string()))?;

        // MockECDSAProofSystem expects `abi.encodePacked(r, s, v)`:
        //   r: bytes32, s: bytes32, v: uint8 (27 | 28).
        let mut out = [0u8; 65];
        out[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
        out[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
        // alloy's `Signature::v()` returns a `bool` (parity bit). EIP-2
        // canonical recovery id is 0/1; on-chain ECDSA verify wants the
        // legacy 27/28 form.
        out[64] = u8::from(sig.v()) + 27;
        Ok(Bytes::copy_from_slice(&out))
    }

    /// `vkey = bytes32(uint256(uint160(authorizedSigner)))` — the
    /// per-rollup `IRollupContract`'s membership ticket convention.
    fn vkey(&self) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(self.signer.address().as_slice());
        B256::from(bytes)
    }
}

/// Address-only prover for split composer/prover topologies: the composer
/// machine holds just the registered attester ADDRESS while the matching
/// private key lives only on the remote `eez-proverd`. `vkey()` derives the
/// same membership ticket as [`MockEcdsaProver`]; `prove()` returns a
/// syntactically valid 65-byte placeholder signed by a fixed, public,
/// powerless key (`1`) — never the attester's key. Callers must refuse an
/// attester equal to [`Self::placeholder_address`] (the node wiring does),
/// so the placeholder cannot recover to the attester and would be rejected
/// by the proof system if it ever reached L1.
///
/// Sound ONLY in deferred-post mode (`EEZ_PROOF_SYSTEM_KIND=real`), where
/// the composer overwrites the placeholder with the remote prover's
/// verified attestation before L1 submission. With the synchronous mock
/// proof system the placeholder WOULD be the on-chain proof, so the node
/// refuses to start address-only there.
#[derive(Debug, Clone)]
pub struct AddressOnlyProver {
    attester: Address,
    placeholder: PrivateKeySigner,
}

impl AddressOnlyProver {
    /// Build from the registered attester address (the proof system's
    /// `authorizedSigner`, whose key signs on the remote prover).
    ///
    /// # Panics
    ///
    /// Never in practice: the placeholder key is the constant `1`, a
    /// valid secp256k1 scalar.
    #[must_use]
    pub fn new(attester: Address) -> Self {
        let placeholder = PrivateKeySigner::from_bytes(&B256::with_last_byte(1))
            .expect("1 is a valid secp256k1 scalar");
        Self {
            attester,
            placeholder,
        }
    }

    /// The registered attester address this composer verifies against.
    #[must_use]
    pub const fn address(&self) -> Address {
        self.attester
    }

    /// Address of the fixed placeholder key. The configured attester must
    /// differ from this, or the placeholder proof would recover to the
    /// attester — callers enforce the inequality at startup.
    #[must_use]
    pub fn placeholder_address(&self) -> Address {
        self.placeholder.address()
    }
}

#[async_trait]
impl Prover for AddressOnlyProver {
    async fn prove(&self, _ctx: ProvingContext) -> ProverResult<Bytes> {
        let sig = self
            .placeholder
            .sign_hash_sync(&MOCK_PROVER_DIGEST)
            .map_err(|e| ProverError::Signer(e.to_string()))?;
        let mut out = [0u8; 65];
        out[..32].copy_from_slice(&sig.r().to_be_bytes::<32>());
        out[32..64].copy_from_slice(&sig.s().to_be_bytes::<32>());
        out[64] = u8::from(sig.v()) + 27;
        Ok(Bytes::copy_from_slice(&out))
    }

    /// Same convention as [`MockEcdsaProver::vkey`]:
    /// `vkey = bytes32(uint256(uint160(attester)))`.
    fn vkey(&self) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(self.attester.as_slice());
        B256::from(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Signature, U256};
    use std::str::FromStr;

    /// [`MockEcdsaProver`] signs `MOCK_PROVER_DIGEST`; ecrecover against
    /// the same digest returns the signer address. This locks in the
    /// agreed-upon contract between the mock prover and
    /// `MockECDSAProofSystem.verify`.
    #[tokio::test]
    async fn mock_proof_roundtrip_against_ecrecover() {
        let key = PrivateKeySigner::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let prover = MockEcdsaProver::new(key.clone());
        let signer_addr = key.address();

        let proof = prover.prove(ProvingContext).await.unwrap();
        assert_eq!(proof.len(), 65, "proof must be r||s||v");
        let v = proof[64];
        assert!(v == 27 || v == 28, "v must be 27 or 28, got {v}");

        let r = U256::from_be_slice(&proof[..32]);
        let s = U256::from_be_slice(&proof[32..64]);
        let sig = Signature::new(r, s, v == 28);
        let recovered = sig
            .recover_address_from_prehash(&MOCK_PROVER_DIGEST)
            .unwrap();
        assert_eq!(
            recovered, signer_addr,
            "ECDSA sig over MOCK_PROVER_DIGEST must recover the signer",
        );
    }

    /// [`AddressOnlyProver`] derives its vkey from the configured attester,
    /// while its placeholder proof deliberately does NOT recover to that
    /// attester — so a placeholder leaking to L1 fails verification instead
    /// of forging an attestation.
    #[tokio::test]
    async fn address_only_placeholder_never_recovers_to_attester() {
        let attester =
            alloy_primitives::Address::from_str("0xfB05940Aaf4eA8AA6d4628B75Fcd5E1176B5F003")
                .unwrap();
        let prover = AddressOnlyProver::new(attester);

        let mut expected_vkey = [0u8; 32];
        expected_vkey[12..].copy_from_slice(attester.as_slice());
        assert_eq!(prover.vkey(), B256::from(expected_vkey));
        assert_eq!(prover.address(), attester);

        let proof = prover.prove(ProvingContext).await.unwrap();
        assert_eq!(proof.len(), 65, "placeholder must be r||s||v shaped");
        let v = proof[64];
        assert!(v == 27 || v == 28, "v must be 27 or 28, got {v}");

        let r = U256::from_be_slice(&proof[..32]);
        let s = U256::from_be_slice(&proof[32..64]);
        let sig = Signature::new(r, s, v == 28);
        let recovered = sig
            .recover_address_from_prehash(&MOCK_PROVER_DIGEST)
            .unwrap();
        assert_ne!(
            recovered, attester,
            "placeholder signature must not recover to the attester",
        );
        assert_eq!(
            recovered,
            prover.placeholder_address(),
            "placeholder signature recovers to the fixed placeholder key's address",
        );
        assert_ne!(
            prover.placeholder_address(),
            attester,
            "the startup guard's inequality must hold for the real attester",
        );
    }
}
