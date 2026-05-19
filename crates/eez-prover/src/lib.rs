//! Prover abstraction: take a batch + rollup id, return `proof` bytes that
//! the matching on-chain `IProofSystem.verify` will accept.
//!
//! The prover owns the `publicInputsHash` computation — mirrors what
//! `EEZ._verifyProofSystemBatch` does, with one PS + one rollup in the
//! stage-2 case. Pushing PIH inside the prover keeps the [`Submitter`] in
//! `eez-l1` independent of which proof system is plugged in, and gives
//! the future zk prover one place to do whatever circuit-specific public-
//! input packing it needs.
//!
//! Stage 2 ships [`EcdsaProver`]: sign the raw 32-byte hash directly (no
//! EIP-191 prefix) and return `abi.encodePacked(r, s, v)`. Stage 4 will
//! add a `ZiskProver` behind the same trait — it'll need additional state
//! context (prev/new state roots, fromBlock/toBlock), which slots into
//! [`ProvingContext`] without changing the trait's call signature.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::SolValue;
use async_trait::async_trait;
use thiserror::Error;

pub use eez_l1_batch::{
    EezRegistry, ProofSystemBatchPerVerificationEntries, RollupIdWithProofSystems,
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
/// Stage 2 only requires `batch` + `rollup_id` (everything the EEZ
/// publicInputsHash formula touches). Stage 4 will add `prev_state_root`,
/// `prev_tip_hash`, etc. — additive field additions; the trait method
/// stays the same.
#[derive(Debug, Clone)]
pub struct ProvingContext<'a> {
    /// The batch about to be submitted. The prover may read everything
    /// in here (entries, lookupCalls, callData, ...) to build the proof.
    pub batch: &'a ProofSystemBatchPerVerificationEntries,
    /// The rollup id this batch attests for.
    pub rollup_id: u64,
}

/// A prover turns batch context into the `proof` bytes the matching
/// on-chain `IProofSystem.verify` accepts.
#[async_trait]
pub trait Prover: Send + Sync + std::fmt::Debug {
    /// Produce a proof for the given context.
    async fn prove<'a>(&'a self, ctx: ProvingContext<'a>) -> ProverResult<Bytes>;

    /// `vkey` bytes registered with the per-rollup manager for this
    /// prover. The Submitter doesn't need this directly (the on-chain
    /// EEZ reads it from the Rollup manager), but exposing it here lets
    /// deploy scripts / smoke tests round-trip against the same source
    /// of truth.
    fn vkey(&self) -> B256;
}

/// Stage-2 ECDSA prover. Computes `publicInputsHash` exactly as
/// `EEZ._verifyProofSystemBatch` does for the one-PS / one-rollup case
/// `Rollup.sol`'s reference impl uses (timestamps + blockHashes both
/// zero), signs the raw 32-byte hash, returns `r || s || v`.
#[derive(Debug, Clone)]
pub struct EcdsaProver {
    signer: PrivateKeySigner,
}

impl EcdsaProver {
    /// Build a prover from a private key signer.
    #[must_use]
    pub const fn new(signer: PrivateKeySigner) -> Self {
        Self { signer }
    }

    /// Address whose key signs `publicInputsHash`. Must match the
    /// `authorizedSigner` baked into the deployed `ECDSAProofSystem`.
    #[must_use]
    pub fn address(&self) -> alloy_primitives::Address {
        self.signer.address()
    }
}

#[async_trait]
impl Prover for EcdsaProver {
    async fn prove<'a>(&'a self, ctx: ProvingContext<'a>) -> ProverResult<Bytes> {
        let public_inputs_hash = compute_public_inputs_hash(ctx.batch, ctx.rollup_id, self.vkey());

        let sig = self
            .signer
            .sign_hash_sync(&public_inputs_hash)
            .map_err(|e| ProverError::Signer(e.to_string()))?;

        // ECDSAProofSystem expects `abi.encodePacked(r, s, v)`:
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

    /// `vkey = bytes32(uint256(uint160(authorizedSigner)))` per
    /// `DeployRollup.s.sol`'s convention.
    fn vkey(&self) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[12..].copy_from_slice(self.signer.address().as_slice());
        B256::from(bytes)
    }
}

/// Mirror of `EEZ._verifyProofSystemBatch`'s `publicInputsHash` for the
/// stage-2 case: one proof system, one rollup, empty entries / lookups /
/// blobs. `Rollup.sol`'s reference impl returns `(0, 0)` for
/// `getTimestampAndBlockHash`, so those two fields are zero.
///
/// Formula for `k = 0`:
///
/// ```text
/// sharedPublicInput = keccak256(
///     abi.encode(entryHashes)        // [] in stage 2
///  || abi.encode(lookupCallHashes)   // [] in stage 2
///  || abi.encode(blobHashes)         // [] in stage 2
///  || keccak256(batch.callData)
///  || batch.crossProofSystemInteractions
/// )
///
/// acc = keccak256(abi.encode(
///     bytes32(0),                    // initial accumulator
///     rollupId, vkey, bytes32(0), uint256(0)
/// ))
///
/// publicInputsHash = keccak256(sharedPublicInput || acc)
/// ```
fn compute_public_inputs_hash(
    batch: &ProofSystemBatchPerVerificationEntries,
    rollup_id: u64,
    vkey: B256,
) -> B256 {
    let empty: Vec<B256> = Vec::new();

    let mut packed_shared: Vec<u8> = Vec::new();
    packed_shared.extend_from_slice(&empty.abi_encode()); // entryHashes
    packed_shared.extend_from_slice(&empty.abi_encode()); // lookupCallHashes
    packed_shared.extend_from_slice(&empty.abi_encode()); // blobHashes
    packed_shared.extend_from_slice(keccak256(&batch.callData).as_slice());
    packed_shared.extend_from_slice(batch.crossProofSystemInteractions.as_slice());
    let shared = keccak256(&packed_shared);

    let acc_input: (B256, U256, B256, B256, U256) = (
        B256::ZERO,
        U256::from(rollup_id),
        vkey,
        B256::ZERO,
        U256::ZERO,
    );
    let acc = keccak256(acc_input.abi_encode());

    let mut packed = Vec::with_capacity(64);
    packed.extend_from_slice(shared.as_slice());
    packed.extend_from_slice(acc.as_slice());
    keccak256(&packed)
}

// We need the `ProofSystemBatchPerVerificationEntries` type to be the
// SAME concrete type both eez-l1 and eez-prover use. Defining it once
// here (rather than in eez-l1's `sol!` block) makes that explicit and
// breaks the temptation to drift the shape between crates.
/// The EEZ contract binding lives here (rather than in `eez-l1`) so the
/// `sol!`-generated `ProofSystemBatchPerVerificationEntries` is exactly
/// the type the prover takes — no cross-crate ABI duplication.
mod eez_l1_batch {
    alloy_sol_types::sol! {
        #![sol(rpc, all_derives)]

        /// Per-rollup state delta — mirrors `sync-rollups-protocol/src/EEZ.sol`.
        struct StateDelta {
            uint256 rollupId;
            bytes32 currentState;
            bytes32 newState;
            int256 etherDelta;
        }

        struct CrossChainCall {
            address targetAddress;
            uint256 value;
            bytes data;
            address sourceAddress;
            uint256 sourceRollupId;
            uint256 revertSpan;
        }

        struct NestedAction {
            bytes32 crossChainCallHash;
            uint256 callCount;
            bytes returnData;
        }

        struct ExecutionEntry {
            StateDelta[] stateDeltas;
            bytes32 crossChainCallHash;
            uint256 destinationRollupId;
            CrossChainCall[] calls;
            NestedAction[] nestedActions;
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
            CrossChainCall[] calls;
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
        /// diverge from `EEZ.sol`'s, `postVerifyAndExecuteOrSaveExecutionsFromBatch`'s
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
        /// needs from `sync-rollups-protocol/src/EEZ.sol`.
        #[sol(rpc)]
        contract EezRegistry {
            function postVerifyAndExecuteOrSaveExecutionsFromBatch(
                ProofSystemBatchPerVerificationEntries calldata batch
            ) external;

            event BatchPosted(uint256 indexed rollupCount);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Signature, hex};
    use std::str::FromStr;

    /// Permanent guard against the bytes[]-vs-ExecutionEntry[] shortcut bug.
    /// If anyone re-declares `entries` / `l1ToL2lookupCalls` as `bytes[]`
    /// in the `sol!` block, the function selector will diverge from
    /// `EEZ.sol`'s canonical signature and submissions will silently
    /// fall through dispatch. This test fails before that ships.
    #[test]
    fn selector_matches_contract_abi() {
        use alloy_sol_types::SolCall;
        let sel = EezRegistry::postVerifyAndExecuteOrSaveExecutionsFromBatchCall::SELECTOR;
        // From contracts/out/EEZ.sol/EEZ.json — methodIdentifiers entry for
        // the canonical postVerifyAndExecuteOrSaveExecutionsFromBatch signature.
        let expected = hex::decode("618f5a78").unwrap();
        assert_eq!(
            sel.as_slice(),
            expected.as_slice(),
            "sol! selector must match EEZ.sol's canonical signature; mismatch means our struct fields diverge from the contract.",
        );
    }

    /// End-to-end: prover signs a PIH derived from a known batch + vkey,
    /// then we `ecrecover` the resulting signature against the same PIH
    /// and expect the prover's signer address back. Subsumes the
    /// individual encoding checks — if `abi.encode`, `keccak256`, or the
    /// sig packing breaks, recovery fails.
    #[tokio::test]
    async fn ecdsa_proof_roundtrip_against_ecrecover() {
        let key = PrivateKeySigner::from_str(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let prover = EcdsaProver::new(key.clone());
        let signer_addr = key.address();

        let batch = ProofSystemBatchPerVerificationEntries {
            entries: Vec::new(),
            l1ToL2lookupCalls: Vec::new(),
            transientExecutionEntryCount: U256::ZERO,
            transientLookupCallCount: U256::ZERO,
            proofSystems: vec![alloy_primitives::Address::ZERO],
            rollupIdsWithProofSystems: vec![RollupIdWithProofSystems {
                rollupId: U256::ZERO,
                proofSystemIndex: vec![0u64],
            }],
            crossProofSystemInteractions: B256::ZERO,
            blobIndices: Vec::new(),
            callData: alloy_primitives::Bytes::new(),
            proofs: Vec::new(),
        };

        let proof = prover
            .prove(ProvingContext { batch: &batch, rollup_id: 0 })
            .await
            .unwrap();
        assert_eq!(proof.len(), 65, "proof must be r||s||v");
        let v = proof[64];
        assert!(v == 27 || v == 28, "v must be 27 or 28, got {v}");

        let pih = compute_public_inputs_hash(&batch, 0, prover.vkey());
        let r = U256::from_be_slice(&proof[..32]);
        let s = U256::from_be_slice(&proof[32..64]);
        let sig = Signature::new(r, s, v == 28);
        let recovered = sig.recover_address_from_prehash(&pih).unwrap();
        assert_eq!(
            recovered, signer_addr,
            "ECDSA sig over our PIH must recover the signer",
        );
    }
}