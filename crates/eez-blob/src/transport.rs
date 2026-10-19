//! KZG sidecars, versioned hashes and pooled type-3 transaction encoding.

use alloy_consensus::{
    SignableTransaction as _, Signed, TxEip4844, TxEip4844WithSidecar, transaction::TxHashRef as _,
};
pub use alloy_eips::eip4844::BYTES_PER_BLOB;
use alloy_eips::{
    Decodable2718 as _, Encodable2718 as _,
    eip2930::AccessList,
    eip4844::{
        AsCkzg as _, Blob, BlobTransactionSidecar,
        env_settings::{EnvKzgSettings, KzgSettings},
        kzg_to_versioned_hash,
    },
    eip7594::{BlobTransactionSidecarEip7594, BlobTransactionSidecarVariant},
};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, Signature, TxHash, U256};

/// Propagation wrapper the target L1 fork's transaction pool accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobSidecarEncoding {
    /// Pre-Fulu blob proofs.
    Eip4844,
    /// Fulu cell proofs.
    Eip7594,
}

/// Fee-resolved fields covered by the type-3 transaction signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobTransactionParams {
    pub chain_id: u64,
    pub nonce: u64,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub max_fee_per_blob_gas: u128,
    pub to: Address,
    pub input: Bytes,
}

/// KZG material prepared once per candidate and reused for signing.
#[derive(Clone, Debug)]
pub struct PreparedBlobSidecar {
    sidecar: BlobTransactionSidecarVariant,
    versioned_hashes: Vec<B256>,
}

impl PreparedBlobSidecar {
    #[must_use]
    pub fn versioned_hashes(&self) -> &[B256] {
        &self.versioned_hashes
    }
}

/// Signed transaction-pool encoding, including its complete sidecar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedBlobTransaction {
    pub transaction_hash: TxHash,
    pub pooled_transaction: Bytes,
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
    #[error("blob transaction signing failed: {0}")]
    Signing(String),
    #[error("invalid pooled blob transaction: {0}")]
    InvalidEncoding(String),
}

/// Derive each blob's EIP-4844 versioned hash from its bytes. This is the only
/// source of blob hashes any component may fold into a public input.
///
/// # Errors
///
/// [`TransportError::InvalidBlobLength`] or [`TransportError::Kzg`] for a blob
/// that is not a full canonical EIP-4844 blob.
pub fn versioned_hashes<B: AsRef<[u8]>>(
    blobs: &[B],
    settings: &KzgSettings,
) -> Result<Vec<B256>, TransportError> {
    blobs
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let blob = to_blob(index, bytes.as_ref())?;
            let commitment = settings
                .blob_to_kzg_commitment(blob.as_ckzg())
                .map_err(|error| TransportError::Kzg {
                    index,
                    reason: error.to_string(),
                })?;
            Ok(kzg_to_versioned_hash(commitment.to_bytes().as_ref()))
        })
        .collect()
}

/// Recover the sidecar-independent hash from a pooled type-3 wrapper.
///
/// # Errors
///
/// [`TransportError::InvalidEncoding`] unless `encoded` is exactly one pooled
/// EIP-4844 transaction.
pub fn pooled_blob_transaction_hash(encoded: &[u8]) -> Result<TxHash, TransportError> {
    let mut input = encoded;
    let signed =
        Signed::<TxEip4844WithSidecar<BlobTransactionSidecarVariant>>::decode_2718(&mut input)
            .map_err(|error| TransportError::InvalidEncoding(error.to_string()))?;
    if !input.is_empty() {
        return Err(TransportError::InvalidEncoding(format!(
            "{} trailing bytes",
            input.len()
        )));
    }
    Ok(*signed.tx_hash())
}

/// Build the propagation sidecar in the wrapper the target fork expects.
///
/// # Errors
///
/// [`TransportError`] for an empty set, a malformed blob, or KZG failure.
pub fn prepare_blob_sidecar<B: AsRef<[u8]>>(
    blobs: &[B],
    encoding: BlobSidecarEncoding,
) -> Result<PreparedBlobSidecar, TransportError> {
    if blobs.is_empty() {
        return Err(TransportError::EmptyBlobs);
    }
    let blobs = blobs
        .iter()
        .enumerate()
        .map(|(index, bytes)| to_blob(index, bytes.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let settings = EnvKzgSettings::Default.get();
    let sidecar: BlobTransactionSidecarVariant = match encoding {
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
    };
    let versioned_hashes = sidecar.versioned_hashes().collect();
    Ok(PreparedBlobSidecar {
        sidecar,
        versioned_hashes,
    })
}

/// Bind, validate, sign and pool-encode a type-3 transaction.
///
/// # Errors
///
/// [`TransportError`] when the sidecar does not match the transaction or
/// signing fails.
pub fn sign_blob_transaction<S: TxSignerSync<Signature>>(
    params: BlobTransactionParams,
    prepared: PreparedBlobSidecar,
    signer: &S,
) -> Result<SignedBlobTransaction, TransportError> {
    let PreparedBlobSidecar {
        sidecar,
        versioned_hashes,
    } = prepared;
    let tx = TxEip4844 {
        chain_id: params.chain_id,
        nonce: params.nonce,
        gas_limit: params.gas_limit,
        max_fee_per_gas: params.max_fee_per_gas,
        max_priority_fee_per_gas: params.max_priority_fee_per_gas,
        to: params.to,
        value: U256::ZERO,
        access_list: AccessList::default(),
        blob_versioned_hashes: versioned_hashes,
        max_fee_per_blob_gas: params.max_fee_per_blob_gas,
        input: params.input,
    };
    tx.validate_blob(&sidecar, EnvKzgSettings::Default.get())
        .map_err(|error| TransportError::Sidecar(error.to_string()))?;
    let mut pooled = TxEip4844WithSidecar { tx, sidecar };
    let signature = signer
        .sign_transaction_sync(&mut pooled)
        .map_err(|error| TransportError::Signing(error.to_string()))?;
    let signed = pooled.into_signed(signature);
    let transaction_hash = *signed.tx_hash();
    let mut encoded = Vec::with_capacity(signed.encode_2718_len());
    signed.encode_2718(&mut encoded);
    Ok(SignedBlobTransaction {
        transaction_hash,
        pooled_transaction: encoded.into(),
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
    use alloy_signer_local::PrivateKeySigner;

    use super::*;

    fn params() -> BlobTransactionParams {
        BlobTransactionParams {
            chain_id: 1,
            nonce: 7,
            gas_limit: 100_000,
            max_fee_per_gas: 2,
            max_priority_fee_per_gas: 1,
            max_fee_per_blob_gas: 3,
            to: Address::repeat_byte(0x11),
            input: Bytes::from_static(&[0xaa]),
        }
    }

    #[test]
    fn sidecar_hashes_match_independent_derivation_in_both_wrappers() {
        let blobs = [vec![0_u8; BYTES_PER_BLOB], vec![0_u8; BYTES_PER_BLOB]];
        let expected = versioned_hashes(&blobs, EnvKzgSettings::Default.get()).unwrap();
        assert!(
            expected.iter().all(|hash| hash[0] == 1),
            "EIP-4844 version byte"
        );
        for encoding in [BlobSidecarEncoding::Eip4844, BlobSidecarEncoding::Eip7594] {
            let prepared = prepare_blob_sidecar(&blobs, encoding).unwrap();
            assert_eq!(prepared.versioned_hashes(), expected);
        }
    }

    #[test]
    fn signed_pooled_transaction_hash_round_trips() {
        let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x01)).unwrap();
        let prepared =
            prepare_blob_sidecar(&[vec![0_u8; BYTES_PER_BLOB]], BlobSidecarEncoding::Eip4844)
                .unwrap();
        let signed = sign_blob_transaction(params(), prepared, &signer).unwrap();
        assert_eq!(signed.pooled_transaction[0], 3, "type-3 envelope");
        assert_eq!(
            pooled_blob_transaction_hash(&signed.pooled_transaction).unwrap(),
            signed.transaction_hash,
        );

        let mut trailing = signed.pooled_transaction.to_vec();
        trailing.push(0);
        assert!(pooled_blob_transaction_hash(&trailing).is_err());
    }

    #[test]
    fn malformed_and_empty_blob_sets_are_rejected() {
        assert!(matches!(
            prepare_blob_sidecar::<Vec<u8>>(&[], BlobSidecarEncoding::Eip4844),
            Err(TransportError::EmptyBlobs),
        ));
        assert!(matches!(
            versioned_hashes(&[vec![0_u8; 31]], EnvKzgSettings::Default.get()),
            Err(TransportError::InvalidBlobLength {
                index: 0,
                actual: 31
            }),
        ));
        // A field element at or above the BLS modulus is not a valid blob.
        let mut invalid = vec![0_u8; BYTES_PER_BLOB];
        invalid[0] = 0xff;
        assert!(matches!(
            versioned_hashes(&[invalid], EnvKzgSettings::Default.get()),
            Err(TransportError::Kzg { index: 0, .. }),
        ));
    }
}
