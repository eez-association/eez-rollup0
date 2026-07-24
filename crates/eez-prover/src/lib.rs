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

use alloy_primitives::{B256, Bytes, b256};
use alloy_rpc_types_debug::ExecutionWitness;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use eez_evm::EvmBatch;
use thiserror::Error;

/// Result alias.
pub type ProverResult<T> = Result<T, ProverError>;

/// Error returned by [`Prover::prove`].
#[derive(Debug, Error)]
pub enum ProverError {
    /// Underlying signer rejected the digest.
    #[error("signer error: {0}")]
    Signer(String),
    /// The proving backend (remote daemon, witness source, …) failed.
    #[error("prover backend: {0}")]
    Backend(String),
}

/// One settling-window block the prover re-executes: its consensus RLP plus
/// the exact (augmented) execution witness that re-execution needs.
#[derive(Debug, Clone)]
pub struct BlockWitness {
    /// L2 block number.
    pub number: u64,
    /// The block hash the composer sealed — the prover cross-checks its own
    /// re-derived hash against this.
    pub hash: B256,
    /// Parent hash — lets the prover chain contiguity across the window.
    pub parent_hash: B256,
    /// Consensus RLP (header + body).
    pub rlp: Bytes,
    /// Minimal execution witness (`state`/`codes`/`keys`/`headers`), augmented
    /// with the removal-closure nodes intermediate per-tx roots need.
    pub witness: ExecutionWitness,
}

/// Inputs the prover needs to prove one posted settlement window.
///
/// The composer fills this and calls [`Prover::prove`]; the whole window's
/// block data travels in-band ([`blocks`](Self::blocks)) so the prover is a
/// stateless function of its input — no feed, no cursor, no backfill.
/// [`MockEcdsaProver`] ignores every field (it signs a fixed digest), so a
/// mock-mode composer may leave [`blocks`](Self::blocks) empty.
#[derive(Debug, Clone, Default)]
pub struct ProvingContext {
    /// The L2 this window settles.
    pub rollup_id: u64,
    /// First block of the window: `posted + 1` (the OD-5 anchor block + 1).
    pub from_block: u64,
    /// Last (settling) block of the window: the Sync height.
    pub to_block: u64,
    /// The authoritative postBatch payload (proof carriers filled, `proofs[]`
    /// empty). The prover recomputes the `publicInputsHash` from this.
    pub batch: EvmBatch,
    /// Every window block's RLP + augmented witness, in block order.
    pub blocks: Vec<BlockWitness>,
    /// `blockhash(N)` for a block-bound batch's `blockNumber = N`; `None` for a
    /// timeless (0) batch.
    pub l1_block_hash: Option<B256>,
}

/// Produces the [`BlockWitness`] for a committed L2 block — the seam by which
/// the composer fills [`ProvingContext::blocks`] without owning the reth
/// provider itself. `eez-node` backs this with the node's provider +
/// `eez_driver::witness`; the composer only calls it.
pub trait ProvingWitnessSource: Send + Sync + std::fmt::Debug {
    /// Build the RLP + augmented witness for block `number`.
    ///
    /// # Errors
    ///
    /// Returns a message if the block is missing or witness generation fails.
    fn block_witness(&self, number: u64) -> Result<BlockWitness, String>;
}

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

        let proof = prover.prove(ProvingContext::default()).await.unwrap();
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
}
