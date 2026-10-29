//! Blob-index selection and settlement commitment verification.

use alloy_primitives::{Address, B256};
use eez_protocol::abi::EvmBatch;
use eez_protocol::public_inputs::batch_shared_public_input_with_blobs;

use crate::error::{L1Error, L1Result};

pub(super) fn commitment(
    batch: &EvmBatch,
    hashes: &[B256],
    sender: Address,
    here: &str,
) -> L1Result<(B256, Vec<B256>)> {
    if batch.blockNumber != 0 || batch.blobIndices.is_empty() || !batch.callData.is_empty() {
        return Err(L1Error::Decode(format!("{here}: invalid blob DA carrier")));
    }
    let selected = selected_hashes(batch, hashes, here)?;
    let commitment = batch_shared_public_input_with_blobs(batch, sender, &selected)
        .map_err(|error| L1Error::Decode(format!("{here}: {error}")))?;
    Ok((commitment, selected))
}

/// Read the named outer-transaction blobs in batch order, ignoring the rest.
pub(super) fn selected_hashes(
    batch: &EvmBatch,
    hashes: &[B256],
    here: &str,
) -> L1Result<Vec<B256>> {
    batch
        .blobIndices
        .iter()
        .map(|index| {
            usize::try_from(*index)
                .ok()
                .and_then(|i| hashes.get(i).copied())
                .ok_or_else(|| {
                    L1Error::Decode(format!("{here}: blob index {index} is out of range"))
                })
        })
        .collect()
}
