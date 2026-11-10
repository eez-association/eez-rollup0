//! KZG sidecars, versioned hashes and pooled type-3 transaction encoding.

use alloy_consensus::TxEnvelope;
pub use alloy_eips::eip4844::BYTES_PER_BLOB;
use alloy_eips::{
    Decodable2718 as _,
    eip2718::Eip2718Error,
    eip4844::{
        AsCkzg as _, Blob, BlobTransactionSidecar, env_settings::EnvKzgSettings,
        kzg_to_versioned_hash,
    },
    eip7594::{BlobTransactionSidecarEip7594, BlobTransactionSidecarVariant},
};
use alloy_primitives::{B256, TxHash};

/// Propagation wrapper the target L1 fork's transaction pool accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobSidecarEncoding {
    /// Pre-Fulu blob proofs.
    Eip4844,
    /// Fulu cell proofs.
    Eip7594,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("a blob transaction requires at least one blob")]
    EmptyBlobs,
    #[error("blob {index} has {actual} bytes; expected {BYTES_PER_BLOB}")]
    InvalidBlobLength { index: usize, actual: usize },
    #[error("KZG commitment for blob {index} failed: {reason}")]
    Kzg { index: usize, reason: String },
    #[error("KZG sidecar construction failed: {0}")]
    Sidecar(String),
}

/// Derive each blob's EIP-4844 versioned hash from its bytes. The signer and
/// client derive these locally rather than trusting request-supplied hashes.
///
/// # Errors
///
/// [`TransportError::InvalidBlobLength`] or [`TransportError::Kzg`] for a blob
/// that is not a full canonical EIP-4844 blob.
pub fn versioned_hashes<B: AsRef<[u8]>>(blobs: &[B]) -> Result<Vec<B256>, TransportError> {
    blobs
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let blob = to_blob(index, bytes.as_ref())?;
            // Loaded per blob so calldata settlement never loads the trusted setup.
            let commitment = EnvKzgSettings::Default
                .get()
                .blob_to_kzg_commitment(blob.as_ckzg())
                .map_err(|error| TransportError::Kzg {
                    index,
                    reason: error.to_string(),
                })?;
            Ok(kzg_to_versioned_hash(commitment.to_bytes().as_ref()))
        })
        .collect()
}

/// A signed postBatch's L1 hash, calldata or blob. Decoding leaves a pooled
/// blob transaction's sidecar out, which keccak of the raw bytes would not.
///
/// # Errors
///
/// When `raw` is not exactly one EIP-2718 transaction.
pub fn post_batch_tx_hash(raw: &[u8]) -> Result<TxHash, Eip2718Error> {
    Ok(*TxEnvelope::decode_2718_exact(raw)?.tx_hash())
}

/// Build the propagation sidecar in the wrapper the target fork expects.
///
/// # Errors
///
/// [`TransportError`] for an empty set, a malformed blob, or KZG failure.
pub fn prepare_blob_sidecar<B: AsRef<[u8]>>(
    blobs: &[B],
    encoding: BlobSidecarEncoding,
) -> Result<BlobTransactionSidecarVariant, TransportError> {
    if blobs.is_empty() {
        return Err(TransportError::EmptyBlobs);
    }
    let blobs = blobs
        .iter()
        .enumerate()
        .map(|(index, bytes)| to_blob(index, bytes.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let settings = EnvKzgSettings::Default.get();
    Ok(match encoding {
        BlobSidecarEncoding::Eip4844 => {
            BlobTransactionSidecar::try_from_blobs_with_settings(blobs, settings)
                .map_err(|error| TransportError::Sidecar(error.to_string()))?
                .into()
        }
        BlobSidecarEncoding::Eip7594 => {
            BlobTransactionSidecarEip7594::try_from_blobs_with_settings(blobs, settings)
                .map_err(|error| TransportError::Sidecar(error.to_string()))?
                .into()
        }
    })
}

fn to_blob(index: usize, bytes: &[u8]) -> Result<Blob, TransportError> {
    if bytes.len() != BYTES_PER_BLOB {
        return Err(TransportError::InvalidBlobLength {
            index,
            actual: bytes.len(),
        });
    }
    Ok(Blob::from_slice(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecar_hashes_match_independent_derivation_in_both_wrappers() {
        let blobs = [vec![0_u8; BYTES_PER_BLOB], vec![0_u8; BYTES_PER_BLOB]];
        let expected = versioned_hashes(&blobs).unwrap();
        for encoding in [BlobSidecarEncoding::Eip4844, BlobSidecarEncoding::Eip7594] {
            let sidecar = prepare_blob_sidecar(&blobs, encoding).unwrap();
            assert_eq!(sidecar.versioned_hashes().collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn malformed_and_empty_blob_sets_are_rejected() {
        assert!(matches!(
            prepare_blob_sidecar::<Vec<u8>>(&[], BlobSidecarEncoding::Eip4844),
            Err(TransportError::EmptyBlobs),
        ));
        assert!(matches!(
            versioned_hashes(&[vec![0_u8; 31]]),
            Err(TransportError::InvalidBlobLength {
                index: 0,
                actual: 31
            }),
        ));
    }
}
