//! Narrow ECDSA signer for the postBatch poster's proof system.
//!
//! Produces the 65-byte `abi.encodePacked(r, s, v)` signature
//! shape `ECDSAProofSystem.verify` (Phase 08 E1) expects. The
//! digest is the raw `publicInputsHash` 32-byte value — no
//! EIP-191 prefix, no domain separator. `s` is normalized to
//! the canonical low half of `secp256k1_n` (the on-chain
//! verifier rejects high-`s` for malleability); `v` is
//! normalized to `{27, 28}`.
//!
//! # Why `k256` direct (not `alloy-signer-local`)
//!
//! Per Codex v1 plan review: the surface is narrow — raw 32-byte
//! digest in, canonical `(r, s, v)` bytes out, no EIP-191
//! prefix. `alloy-signer-local` would pull in `Wallet` /
//! transaction-signing abstractions the workspace doesn't
//! otherwise use, and would obscure the bytes at the one place
//! that MUST exactly match the Solidity verifier.
//!
//! The pure-Rust round-trip test uses `alloy_primitives::Signature::recover_address_from_prehash`
//! (which itself calls `k256::ecdsa::VerifyingKey`) as the
//! oracle, so the byte-equality is checked end-to-end without
//! anvil.
//!
//! # Spec anchors
//!
//! - `deployments/devnet/contracts/ECDSAProofSystem.sol` (Phase
//!   08 E1) — the on-chain verifier this signer is paired
//!   with.
//! - `docs/DERIVATION.md` §6e (post-`1061244`): "65-byte
//!   `abi.encodePacked(r, s, v)` signature, `s` in the
//!   canonical (low) half of `secp256k1_n`, `v` normalized to
//!   `{27, 28}`".
//! - `docs/plans/09-postbatch-poster.md` §D.

use alloy_primitives::{Address, B256, Bytes};
use k256::ecdsa::{RecoveryId, Signature as K256Signature, SigningKey};

/// Narrow ECDSA signer over raw `B256` digests. Produces a
/// 65-byte `abi.encodePacked(r, s, v)` signature in canonical
/// form (`s` low half, `v` ∈ `{27, 28}`).
///
/// Construct via [`EcdsaProofSigner::from_private_key`]. Cheap
/// to clone; holds only the 32-byte private key.
#[derive(Clone)]
pub struct EcdsaProofSigner {
    key: SigningKey,
    /// Cached signer address (the `signer()` value the
    /// `ECDSAProofSystem` contract expects to recover).
    address: Address,
}

impl std::fmt::Debug for EcdsaProofSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NEVER print the private key, even in Debug.
        f.debug_struct("EcdsaProofSigner")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// Construction-time and sign-time errors for
/// [`EcdsaProofSigner`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignerError {
    /// The 32-byte private key wasn't a valid secp256k1 scalar
    /// (zero, ≥ curve order, or rejected by k256).
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(String),
    /// k256's `RecoveryId::to_byte()` returned a value outside
    /// `{0, 1}`. The 2-bit packed form can carry parity (low
    /// bit) plus an EIP-155-style `is_x_reduced` flag (high
    /// bit); the on-chain `ECDSAProofSystem` only accepts
    /// `v ∈ {27, 28}` (parity-only), so a `recid > 1` would
    /// produce a verifier-rejected `v ∈ {29, 30}`. Refuse the
    /// signature instead of emitting silently invalid bytes.
    #[error(
        "k256 RecoveryId={recid} out of parity range; ECDSAProofSystem accepts only v ∈ {{27, 28}}"
    )]
    RecoveryIdOutOfRange {
        /// The offending recovery-id byte.
        recid: u8,
    },
}

impl EcdsaProofSigner {
    /// Build from a 32-byte private-key value. Returns
    /// [`SignerError::InvalidPrivateKey`] if the value is not a
    /// valid secp256k1 scalar (zero, ≥ curve order).
    ///
    /// # Errors
    ///
    /// See [`SignerError`].
    pub fn from_private_key(private_key: B256) -> Result<Self, SignerError> {
        let key = SigningKey::from_bytes((&private_key.0).into())
            .map_err(|e| SignerError::InvalidPrivateKey(e.to_string()))?;
        let address = address_from_verifying_key(key.verifying_key());
        Ok(Self { key, address })
    }

    /// The address downstream verifiers will recover from
    /// signatures this signer produces. For the dev devnet,
    /// this is the value passed as `authorizedSigner` to
    /// `DeployECDSAProofSystem` (Phase 08 E2); the on-chain
    /// `ECDSAProofSystem` stores it as `signer()` and rejects
    /// any signature whose ecrecover doesn't match.
    #[must_use]
    pub fn address(&self) -> Address {
        self.address
    }

    /// Sign a raw 32-byte digest. Produces a 65-byte
    /// `abi.encodePacked(r, s, v)` signature:
    ///
    /// - `r`: bytes 0-31 — R component of the signature
    /// - `s`: bytes 32-63 — S component, canonical low half of
    ///   `secp256k1_n` (k256's signer enforces this internally
    ///   on signing; the byte-level check is belt-and-suspenders)
    /// - `v`: byte 64 — recovery identifier, `27 + recid` where
    ///   `recid ∈ {0, 1}` ⇒ `v ∈ {27, 28}`. If k256 ever
    ///   produces a 2-bit `recid` > 1, the call returns
    ///   [`SignerError::RecoveryIdOutOfRange`] rather than
    ///   silently emitting `v ∈ {29, 30}` (which the on-chain
    ///   verifier would reject).
    ///
    /// No EIP-191 prefix is applied. The `ECDSAProofSystem`
    /// verifier signs the same raw digest.
    ///
    /// # Errors
    ///
    /// See [`SignerError::RecoveryIdOutOfRange`].
    pub fn sign_prehash(&self, digest: B256) -> Result<Bytes, SignerError> {
        // k256's `sign_prehash` returns canonical signatures
        // (low-s) by default since k256 v0.13 — the
        // `signature::hazmat::PrehashSigner` impl uses
        // `try_sign_prehashed_with_rng` internally with low-s
        // normalization on. (Lock against future-version drift
        // via `sign_prehash_low_s_canonical` test.)
        let (sig, recid): (K256Signature, RecoveryId) = self
            .key
            .sign_prehash_recoverable(digest.as_slice())
            .expect("k256 sign_prehash_recoverable: infallible for valid keys + 32-byte digest");
        let v = recid_to_v(recid)?;
        let r = sig.r();
        let s = sig.s();
        let mut out = [0u8; 65];
        out[..32].copy_from_slice(&r.to_bytes());
        out[32..64].copy_from_slice(&s.to_bytes());
        out[64] = v;
        Ok(Bytes::from(out.to_vec()))
    }
}

/// Convert a k256 `RecoveryId` to the on-chain `v` byte format
/// (`27 + parity_bit`). Rejects values outside `{0, 1}` — the
/// on-chain `ECDSAProofSystem` only accepts `v ∈ {27, 28}`.
///
/// `RecoveryId::to_byte()` returns a 2-bit packed value:
/// - bit 0: y-coordinate parity
/// - bit 1: x-reduced flag (EIP-155-style chain-id-aware
///   recovery; out-of-scope for the raw-digest verifier this
///   signer pairs with)
///
/// In practice, k256's `sign_prehash_recoverable` over plain
/// secp256k1 only emits parity-only recids (0 or 1); the
/// guard is defensive against k256 internals shifting under
/// us.
///
/// # Errors
///
/// Returns [`SignerError::RecoveryIdOutOfRange`] if
/// `recid.to_byte() > 1`.
fn recid_to_v(recid: RecoveryId) -> Result<u8, SignerError> {
    let r = recid.to_byte();
    if r > 1 {
        return Err(SignerError::RecoveryIdOutOfRange { recid: r });
    }
    Ok(27 + r)
}

/// Address derivation: `keccak256(pubkey_uncompressed[1..])[12..]`.
///
/// The k256 `VerifyingKey` carries the public key; we serialize
/// it uncompressed (65 bytes, leading `0x04` SEC1 prefix), skip
/// that prefix, and keccak the remaining 64 bytes (X || Y).
/// Bottom 20 bytes of the keccak output is the EVM address.
fn address_from_verifying_key(key: &k256::ecdsa::VerifyingKey) -> Address {
    // `to_encoded_point` is an inherent method on VerifyingKey
    // (re-exported from k256's PublicKey impl).
    let point = key.to_encoded_point(false);
    let bytes = point.as_bytes(); // 65 bytes: 0x04 || X (32) || Y (32)
    debug_assert_eq!(bytes[0], 0x04, "k256 SEC1 uncompressed prefix");
    let hash = alloy_primitives::keccak256(&bytes[1..]);
    Address::from_slice(&hash[12..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Signature;

    /// Dev key from `devnet-up.sh`'s `$PK_COMPOSER`.
    const SEQUENCER_KEY: [u8; 32] = [
        0x59, 0xc6, 0x99, 0x5e, 0x99, 0x8f, 0x97, 0xa5, 0xa0, 0x04, 0x49, 0x66, 0xf0, 0x94, 0x53,
        0x89, 0xdc, 0x9e, 0x86, 0xda, 0xe8, 0x8c, 0x7a, 0x84, 0x12, 0xf4, 0x60, 0x3b, 0x6b, 0x78,
        0x69, 0x0d,
    ];
    /// Expected address for that key (matches
    /// `cast wallet address --private-key 0x59...` and the
    /// devnet's live `$SEQUENCER_ADDR`).
    const SEQUENCER_ADDR: Address = Address::new([
        0x70, 0x99, 0x79, 0x70, 0xc5, 0x18, 0x12, 0xdc, 0x3a, 0x01, 0x0c, 0x7d, 0x01, 0xb5, 0x0e,
        0x0d, 0x17, 0xdc, 0x79, 0xc8,
    ]);

    #[test]
    fn rejects_zero_private_key() {
        let err = EcdsaProofSigner::from_private_key(B256::ZERO).unwrap_err();
        assert!(matches!(err, SignerError::InvalidPrivateKey(_)));
    }

    #[test]
    fn address_matches_devnet_sequencer_addr() {
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        // Locks the address-derivation byte chain against the
        // live devnet's $SEQUENCER_ADDR (the value
        // DeployECDSAProofSystem.s.sol passes as the authorized
        // signer for the on-chain verifier).
        assert_eq!(signer.address(), SEQUENCER_ADDR);
    }

    #[test]
    fn sign_prehash_produces_65_bytes() {
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        let digest = B256::repeat_byte(0x42);
        let sig = signer.sign_prehash(digest).expect("low-recid");
        assert_eq!(sig.len(), 65);
    }

    #[test]
    fn sign_prehash_v_in_27_28() {
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        // Cover many digests; any one v ∉ {27, 28} would fail.
        for i in 0u8..32 {
            let digest = B256::repeat_byte(i);
            let sig = signer.sign_prehash(digest).expect("low-recid");
            let v = sig[64];
            assert!(
                v == 27 || v == 28,
                "v={v} for digest 0x{i:02x}; must be in {{27, 28}}"
            );
        }
    }

    #[test]
    fn sign_prehash_low_s_canonical() {
        // k256's sign_prehash_recoverable produces canonical
        // signatures (low-s) by default. Spot-check that s ≤
        // secp256k1_n/2 for a batch of digests.
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        let half_n: [u8; 32] = [
            0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0x5D, 0x57, 0x6E, 0x73, 0x57, 0xA4, 0x50, 0x1D, 0xDF, 0xE9, 0x2F, 0x46,
            0x68, 0x1B, 0x20, 0xA0,
        ];
        for i in 0u8..16 {
            let digest = B256::repeat_byte(i);
            let sig = signer.sign_prehash(digest).expect("low-recid");
            let s: [u8; 32] = sig[32..64].try_into().unwrap();
            assert!(s <= half_n, "high-s signature emitted for digest 0x{i:02x}");
        }
    }

    #[test]
    fn signature_round_trips_via_alloy_ecrecover() {
        // The strongest available oracle without anvil. alloy's
        // Signature::recover_address_from_prehash internally uses
        // k256's ecrecover — same crate family as the on-chain
        // ECDSA.recover behavior the ECDSAProofSystem contract
        // emulates. If this asserts true for every digest, the
        // signature shape + canonical form + v normalization are
        // all correct.
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        for i in 0u8..16 {
            let digest = B256::repeat_byte(i);
            let sig_bytes = signer.sign_prehash(digest).expect("low-recid");
            let parsed =
                Signature::try_from(sig_bytes.as_ref()).expect("alloy Signature parse 65 bytes");
            let recovered = parsed
                .recover_address_from_prehash(&digest)
                .expect("alloy recover");
            assert_eq!(
                recovered,
                signer.address(),
                "recovered ≠ signer for digest 0x{i:02x}"
            );
        }
    }

    #[test]
    fn recid_to_v_accepts_parity_rejects_high_bits() {
        // Locks the explicit RecoveryId range guard. k256
        // exposes `RecoveryId::from_byte` so we can construct
        // each possible recid value directly.
        assert_eq!(recid_to_v(RecoveryId::from_byte(0).unwrap()), Ok(27));
        assert_eq!(recid_to_v(RecoveryId::from_byte(1).unwrap()), Ok(28));
        // recid 2 + 3 carry the EIP-155-style is_x_reduced bit
        // that the on-chain ECDSAProofSystem doesn't accept.
        assert_eq!(
            recid_to_v(RecoveryId::from_byte(2).unwrap()),
            Err(SignerError::RecoveryIdOutOfRange { recid: 2 })
        );
        assert_eq!(
            recid_to_v(RecoveryId::from_byte(3).unwrap()),
            Err(SignerError::RecoveryIdOutOfRange { recid: 3 })
        );
    }

    #[test]
    fn signature_determinism_within_same_signer() {
        // ECDSA over k256 with deterministic-k (RFC 6979) is
        // deterministic — same key + same digest ⇒ same
        // signature. Locks against accidental nondeterminism if
        // anyone swaps in a randomized signer.
        let signer =
            EcdsaProofSigner::from_private_key(B256::from(SEQUENCER_KEY)).expect("valid key");
        let digest = B256::repeat_byte(0xab);
        let sig1 = signer.sign_prehash(digest).expect("low-recid");
        let sig2 = signer.sign_prehash(digest).expect("low-recid");
        assert_eq!(sig1, sig2);
    }
}
