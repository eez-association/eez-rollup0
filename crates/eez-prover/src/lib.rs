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
//! The EEZ `sol!` binding lives in this crate (re-exported below) so
//! `ProofSystemBatchPerVerificationEntries` is the same concrete type
//! both `eez-l1` and `eez-prover` see.
//!
//! [`MockECDSAProofSystem`]: ../../../contracts/src/MockECDSAProofSystem.sol

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use alloy_primitives::{B256, Bytes, b256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use thiserror::Error;

pub use eez_l1_batch::{
    EezRegistry, ExecutionEntry, ProofSystemBatchPerVerificationEntries, RollupIdWithProofSystems,
    StateDelta,
};

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

mod eez_l1_batch {
    alloy_sol_types::sol! {
        #![sol(rpc, all_derives)]

        /// Per-rollup state delta — mirrors `sync-rollups-protocol/src/interfaces/IEEZ.sol`.
        struct StateDelta {
            uint256 rollupId;
            bytes32 currentState;
            bytes32 newState;
            int256 etherDelta;
        }

        struct L2ToL1Call {
            address targetAddress;
            uint256 value;
            bytes data;
            address sourceAddress;
            uint256 sourceRollupId;
            uint256 revertSpan;
        }

        struct ExpectedL1ToL2Call {
            bytes32 crossChainCallHash;
            uint256 callCount;
            bytes returnData;
        }

        struct ExecutionEntry {
            StateDelta[] stateDeltas;
            bytes32 proxyEntryHash;
            uint256 destinationRollupId;
            L2ToL1Call[] L2ToL1Calls;
            ExpectedL1ToL2Call[] expectedL1ToL2Calls;
            uint256 callCount;
            bytes returnData;
            bytes32 rollingHash;
        }

        struct LookupCall {
            bytes32 crossChainCallHash;
            uint256 destinationRollupId;
            bytes returnData;
            bool failed;
            uint64 callNumber;
            uint64 lastNestedActionConsumed;
            L2ToL1Call[] calls;
            bytes32 rollingHash;
        }

        /// One rollup's participation in a posting batch.
        struct RollupIdWithProofSystems {
            uint256 rollupId;
            uint64[] proofSystemIndex;
        }

        /// Mirrors `sync-rollups-protocol/src/EEZ.sol`. Stage 2 uses
        /// `entries`/`l1ToL2lookupCalls` as empty dynamic arrays — they
        /// hold cross-chain content in stage 4. The full struct types are
        /// declared here (not shortened to `bytes[]`) because the function
        /// selector hashes the canonical types — if our declared types
        /// diverge from `EEZ.sol`'s, `postAndVerifyBatch`'s
        /// 4-byte selector won't match and the call hits no function.
        struct ProofSystemBatchPerVerificationEntries {
            ExecutionEntry[] entries;
            LookupCall[] l1ToL2lookupCalls;
            uint256 transientExecutionEntryCount;
            uint256 transientLookupCallCount;
            address[] proofSystems;
            RollupIdWithProofSystems[] rollupIdsWithProofSystems;
            bytes32 crossProofSystemInteractions;
            uint256[] blobIndices;
            bytes callData;
            bytes[] proofs;
        }

        /// The EEZ entry point + event. Just the surface the Submitter
        /// and Follower need from `sync-rollups-protocol/src/EEZ.sol`.
        ///
        /// `rollupCounter()` is the auto-generated getter on the
        /// `uint256 public rollupCounter` state variable — total rollups
        /// registered. Used by the Follower at startup to validate the
        /// configured `EEZ_ROLLUP_ID` is actually in the registry.
        #[sol(rpc)]
        contract EezRegistry {
            function postAndVerifyBatch(
                ProofSystemBatchPerVerificationEntries calldata batch
            ) external;

            function rollupCounter() external view returns (uint256);

            event BatchPosted(uint256 indexed rollupCount);

            /// Winner signal: emitted by `_applyStateDeltas`
            /// (EEZ.sol:979) when a batch's state delta actually
            /// applied. Absence ⇒ loser (`ImmediateEntrySkipped`).
            event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Signature, U256, hex};
    use std::str::FromStr;

    /// Permanent guard against the bytes[]-vs-ExecutionEntry[] shortcut bug.
    /// If anyone re-declares `entries` / `l1ToL2lookupCalls` as `bytes[]`
    /// in the `sol!` block, the function selector will diverge from
    /// `EEZ.sol`'s canonical signature and submissions will silently
    /// fall through dispatch. This test fails before that ships.
    #[test]
    fn selector_matches_contract_abi() {
        use alloy_sol_types::SolCall;
        let sel = EezRegistry::postAndVerifyBatchCall::SELECTOR;
        // From contracts/out/EEZ.sol/EEZ.json — methodIdentifiers entry for
        // the canonical postAndVerifyBatch signature.
        let expected = hex::decode("7dd4d7d7").unwrap();
        assert_eq!(
            sel.as_slice(),
            expected.as_slice(),
            "sol! selector must match EEZ.sol's canonical signature; mismatch means our struct fields diverge from the contract.",
        );
    }

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
}
