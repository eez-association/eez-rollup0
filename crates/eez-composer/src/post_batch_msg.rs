//! Build the `control.v1.PostBatch` the composer ships to the prover on the
//! settling block's `ControlEvent.composition` (prover-chain P4-a).
//!
//! The authoritative field is `abi_calldata` — the ABI-encoded
//! `EEZ.postAndVerifyBatch` calldata with the proof-system carriers filled but
//! `proofs[]` EMPTY (the out-of-process prover decodes THIS to recompute the
//! `publicInputsHash`, ECDSA-signs it, and the composer fills `proofs[]` with
//! the attestation before broadcasting). The decoded summary fields are
//! observability only — except `l1_block_hash`, which the prover's
//! `getTimestampAndBlockHash(blockNumber)` fold REQUIRES (it has no L1 view).

use alloy_primitives::B256;
use alloy_sol_types::SolCall;

use eez_evm::types::postAndVerifyBatchCall;
use eez_evm::EvmBatch;

/// Lift an assembled [`EvmBatch`] + its L1 binding into the wire
/// [`eez_control_rpc::v1::PostBatch`]. `l1_block_hash` is `Some(blockhash(N))`
/// for a batch bound to `blockNumber = N`, or `None` for the TIMELESS batch
/// (`blockNumber = 0`, eez0's current settlement) — it must agree with
/// `batch.inner.blockNumber` (mismatch → the hash stays unset, the prover
/// recomputes from `abi_calldata`). `vkey` is the proof system's verification key.
#[must_use]
pub fn build_post_batch_msg(
    batch: &EvmBatch,
    vkey: B256,
    l1_block_hash: Option<B256>,
) -> eez_control_rpc::v1::PostBatch {
    // The wire calldata carries `proofs[]` EMPTY — the prover fills them after
    // attesting, and recomputes the publicInputsHash from this exact encoding.
    let mut wire = batch.clone();
    wire.inner.proofs.clear();
    let abi_calldata = postAndVerifyBatchCall { batch: wire.inner.clone() }.abi_encode();

    // Summary: first entry's StateDelta = (rollup, currentState R0); last =
    // newState R_N. The prover re-derives these from abi_calldata, so these are
    // observability only.
    let first = batch.inner.entries.first().and_then(|e| e.stateDeltas.first());
    let last = batch.inner.entries.last().and_then(|e| e.stateDeltas.last());

    // publicInputsHash the composer computed — the prover cross-checks against
    // its own recomputation (the prover's is canonical). On a malformed
    // block-context binding (e.g. a BOUND batch reaching here while
    // `l1_block_hash` is None) `public_inputs_hashes` errors; do NOT silently
    // swallow it into a benign-looking zero — log loudly. The prover recomputes
    // from abi_calldata and fail-closes regardless, but a silent zero is a
    // debugging trap. (Real settling batches always carry >=1 proof system, so
    // this Err never fires in normal single-rollup/timeless operation.)
    let public_inputs_hash =
        match eez_evm::public_inputs::public_inputs_hashes(batch, vkey, l1_block_hash) {
            Ok(hs) => hs.first().copied().unwrap_or_default(),
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    "build_post_batch_msg: publicInputsHash recompute FAILED; shipping zero (the prover will reject)"
                );
                B256::ZERO
            }
        };

    eez_control_rpc::v1::PostBatch {
        abi_calldata,
        rollup_id: first.map_or(0, |d| d.rollupId.to::<u64>()),
        current_state: first.map_or_else(Vec::new, |d| d.currentState.to_vec()),
        new_state: last.map_or_else(Vec::new, |d| d.newState.to_vec()),
        entry_count: u32::try_from(batch.inner.entries.len()).unwrap_or(u32::MAX),
        public_inputs_hash: public_inputs_hash.to_vec(),
        l1_block_hash: l1_block_hash.map_or_else(Vec::new, |h| h.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_timeless_batch_yields_wellformed_post_batch() {
        // eez0's current settlement is TIMELESS (blockNumber=0) → l1_block_hash None.
        let msg = build_post_batch_msg(&EvmBatch::default(), B256::ZERO, None);
        // An empty batch still encodes a valid postAndVerifyBatch call.
        assert!(!msg.abi_calldata.is_empty(), "abi_calldata must encode the (empty) batch");
        assert_eq!(msg.entry_count, 0);
        assert_eq!(msg.rollup_id, 0);
        assert!(msg.current_state.is_empty());
        assert!(msg.new_state.is_empty());
        // Timeless → no l1_block_hash binding.
        assert!(msg.l1_block_hash.is_empty());
        // Empty batch carries no proof systems → no per-PS hash.
        assert_eq!(msg.public_inputs_hash, B256::ZERO.to_vec());
    }
}
