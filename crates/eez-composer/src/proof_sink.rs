//! The composer-side `control.v1.ProofSink` server — the RETURN path of the
//! out-of-process prover loop. Having re-executed + gated a settling window, the
//! prover ECDSA-signs its publicInputsHash and submits it here; the composer
//! verifies the 65-byte signature recovers to the REGISTERED attester over the
//! claimed hash and accepts (or refuses) accordingly.
//!
//! # Deferred-post data-flow (P4-b)
//!
//! This module also provides the leak-free PRIMITIVES of the deferred post — the
//! seam by which the composer fills a batch's `proofs[]` with the REAL
//! attestation in place of the dev mock:
//!
//! - [`ProofStore`] — verified attestations, keyed by publicInputsHash.
//! - [`ProofSinkSvc::with_store`] — drops each verified signature into the store.
//! - [`apply_proof`] — drains the store entry for a batch's publicInputsHash into
//!   its `batch.proofs[]`.
//!
//! The integration test exercises the whole flow: a prover signs a batch's
//! publicInputsHash, the ProofSink verifies + stores it, and `apply_proof` fills
//! the batch's `proofs[]` with that exact signature.
//!
//! NOT wired into production yet (it stays additive — eez-node constructs the
//! ProofSink WITHOUT a store, so the dev/mock post path is untouched and nothing
//! leaks): the actual DEFERRED post (move the L1 submission out of
//! `prepare_post_batch_raw` to fire when the proof arrives) + the switch to the
//! real on-chain `ECDSAProofSystem` (the dev `MockECDSAProofSystem` recovers
//! against a fixed digest and would reject the real proof) is a focused
//! deployment-integration step.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256, Bytes, Signature};
use eez_control_rpc::v1::{SlotProof, SubmitAck, proof_sink_server::ProofSink};
use eez_evm::EvmBatch;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::posted_windows::PostedWindows;

/// Verified prover attestations, keyed by the publicInputsHash the prover signed.
/// Shared between the [`ProofSinkSvc`] (writer) and the composer's deferred post
/// (drainer, via [`apply_proof`]).
pub type ProofStore = Arc<Mutex<HashMap<B256, Bytes>>>;

/// Verify a submitted `SlotProof`: its 65-byte `post_batch_proof` must recover to
/// `attester` over the 32-byte `public_inputs_hash`. Pure → unit-tested.
#[must_use]
pub fn verify_attestation(attester: Address, proof: &SlotProof) -> bool {
    if proof.public_inputs_hash.len() != 32 {
        warn!("ProofSink: publicInputsHash is not 32 bytes");
        return false;
    }
    let hash = B256::from_slice(&proof.public_inputs_hash);
    let Ok(sig) = Signature::try_from(proof.post_batch_proof.as_slice()) else {
        warn!(
            l1_slot_anchor = proof.l1_slot_anchor,
            "ProofSink: malformed 65-byte signature"
        );
        return false;
    };
    match sig.recover_address_from_prehash(&hash) {
        Ok(recovered) if recovered == attester => {
            info!(
                l1_slot_anchor = proof.l1_slot_anchor,
                %hash,
                attester = %attester,
                "✓ ProofSink: verified attestation from the registered prover",
            );
            true
        }
        Ok(recovered) => {
            warn!(
                %recovered,
                expected = %attester,
                "ProofSink: REJECTED — attestation signer is not the registered prover",
            );
            false
        }
        Err(e) => {
            warn!(error = %e, "ProofSink: signature recover failed");
            false
        }
    }
}

/// Drain the verified attestation for `public_inputs_hash` from `store` into
/// `batch.proofs[]` — the deferred-post FILL, the composer using the prover's
/// real signature in place of the dev mock. Returns `true` if a proof was
/// applied (and removed from the store).
#[must_use]
pub fn apply_proof(batch: &mut EvmBatch, public_inputs_hash: B256, store: &ProofStore) -> bool {
    let Ok(mut map) = store.lock() else {
        return false;
    };
    match map.remove(&public_inputs_hash) {
        Some(sig) => {
            batch.inner.proofs = vec![sig];
            true
        }
        None => false,
    }
}

/// The composer's `ProofSink` tonic service. Accepts a `SlotProof` iff
/// [`verify_attestation`] passes against the registered attester address, and —
/// when constructed [`with_store`](Self::with_store) — records the verified
/// signature for the deferred post.
#[derive(Debug)]
pub struct ProofSinkSvc {
    attester: Address,
    store: Option<ProofStore>,
    /// Composer-driven ledger (Phase 1). When wired, a verified attestation
    /// flips the matching window's `attested` + advances the verified frontier
    /// — the ONLY place the frontier moves, keyed by content (publicInputsHash).
    posted_windows: Option<PostedWindows>,
}

impl ProofSinkSvc {
    /// Verify-only (no store) — the dev/mock posture: attestations are verified +
    /// logged but not consumed (the mock self-signs the actual post).
    #[must_use]
    pub fn new(attester: Address) -> Self {
        Self {
            attester,
            store: None,
            posted_windows: None,
        }
    }

    /// Verify + RECORD into `store` for the deferred post (drained by
    /// [`apply_proof`]).
    #[must_use]
    pub fn with_store(attester: Address, store: ProofStore) -> Self {
        Self {
            attester,
            store: Some(store),
            posted_windows: None,
        }
    }

    /// Verify + record into `store` (deferred post) AND advance the
    /// composer-driven [`PostedWindows`] frontier on each verified attestation.
    #[must_use]
    pub fn with_store_and_windows(
        attester: Address,
        store: ProofStore,
        posted_windows: PostedWindows,
    ) -> Self {
        Self {
            attester,
            store: Some(store),
            posted_windows: Some(posted_windows),
        }
    }

    /// Verify the attestation and, if a store is wired, record it. Returns
    /// whether it was accepted.
    fn verify_and_store(&self, proof: &SlotProof) -> bool {
        if !verify_attestation(self.attester, proof) {
            return false;
        }
        // `verify_attestation` guaranteed a 32-byte hash + a parseable sig.
        let hash = B256::from_slice(&proof.public_inputs_hash);
        if let Some(store) = &self.store {
            if let Ok(mut map) = store.lock() {
                map.insert(hash, Bytes::copy_from_slice(&proof.post_batch_proof));
            }
        }
        // Composer-driven ledger (Phase 1): advance the verified frontier — ONLY
        // here, DOWNSTREAM of the cryptographic verify, keyed by the content the
        // prover signed. Never via a composer-asserted height.
        if let Some(windows) = &self.posted_windows {
            let frontier = windows.mark_attested(hash);
            info!(%hash, verified_frontier = frontier, "ProofSink: attestation advanced verified frontier");
        }
        true
    }
}

#[tonic::async_trait]
impl ProofSink for ProofSinkSvc {
    async fn submit_slot_proof(
        &self,
        req: Request<SlotProof>,
    ) -> Result<Response<SubmitAck>, Status> {
        let accepted = self.verify_and_store(&req.into_inner());
        Ok(Response::new(SubmitAck { accepted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{U256, address};
    use eez_evm::public_inputs::public_inputs_hashes;
    use eez_evm::signer::EcdsaProofSigner;
    use eez_evm::types::RollupIdWithProofSystemsSol;

    // Anvil/hardhat #1 key (any valid key works for the round-trip).
    const KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    fn vkey_of(addr: Address) -> B256 {
        let mut b = [0u8; 32];
        b[12..].copy_from_slice(addr.as_slice());
        B256::from(b)
    }

    /// A minimal finalized batch — one PS, one rollup, timeless.
    fn carrier_batch() -> EvmBatch {
        let mut batch = EvmBatch::default();
        batch.inner.blockNumber = 0;
        batch.inner.proofSystems = vec![address!("00000000000000000000000000000000000000aa")];
        batch.inner.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: U256::from(1),
            proofSystemIndex: vec![0],
        }];
        batch
    }

    fn signed(hash: B256) -> (Address, SlotProof) {
        let signer = EcdsaProofSigner::from_private_key(KEY.parse().unwrap()).unwrap();
        let sig = signer.sign_prehash(hash).unwrap();
        (
            signer.address(),
            SlotProof {
                l1_slot_anchor: 1,
                public_inputs_hash: hash.to_vec(),
                post_batch_proof: sig.to_vec(),
            },
        )
    }

    #[test]
    fn verifies_a_real_signature() {
        let (attester, proof) = signed(B256::repeat_byte(0xe5));
        assert!(verify_attestation(attester, &proof));
    }

    #[test]
    fn rejects_a_wrong_signer() {
        let (_attester, proof) = signed(B256::repeat_byte(0xe5));
        assert!(!verify_attestation(Address::repeat_byte(0xaa), &proof));
    }

    #[test]
    fn rejects_malformed() {
        let proof = SlotProof {
            l1_slot_anchor: 1,
            public_inputs_hash: B256::ZERO.to_vec(),
            post_batch_proof: vec![0u8; 10], // not 65 bytes
        };
        assert!(!verify_attestation(Address::repeat_byte(0xaa), &proof));
    }

    #[test]
    fn proof_flows_from_attestation_into_batch_proofs() {
        // The composer's batch + the publicInputsHash the prover will sign.
        let signer = EcdsaProofSigner::from_private_key(KEY.parse().unwrap()).unwrap();
        let attester = signer.address();
        let mut batch = carrier_batch();
        let hash = public_inputs_hashes(&batch, vkey_of(attester), None).unwrap()[0];
        let sig = signer.sign_prehash(hash).unwrap();
        let proof = SlotProof {
            l1_slot_anchor: 1,
            public_inputs_hash: hash.to_vec(),
            post_batch_proof: sig.to_vec(),
        };

        // The ProofSink verifies + records the attestation.
        let store: ProofStore = Arc::new(Mutex::new(HashMap::new()));
        let svc = ProofSinkSvc::with_store(attester, Arc::clone(&store));
        assert!(svc.verify_and_store(&proof));
        assert_eq!(store.lock().unwrap().len(), 1);

        // The composer drains it into the batch's proofs[] (the deferred-post fill).
        assert!(apply_proof(&mut batch, hash, &store));
        assert_eq!(batch.inner.proofs, vec![sig]);
        assert!(store.lock().unwrap().is_empty(), "store drained on apply");

        // A second apply finds nothing.
        assert!(!apply_proof(&mut batch, hash, &store));
    }

    #[test]
    fn verify_only_svc_records_nothing() {
        let (attester, proof) = signed(B256::repeat_byte(0xe5));
        // `new` (no store) still verifies, but records nothing to drain.
        assert!(ProofSinkSvc::new(attester).verify_and_store(&proof));
    }
}
