//! EIP-4844 carrier for Rollup0 blob settlement.
//!
//! `eez-payload-codec` owns the logical stream and its 31-byte field-element
//! packing; this crate owns everything Ethereum adds around those bytes: KZG
//! commitments, versioned hashes, the pooled type-3 transaction, and which
//! blob limit and sidecar wrapper apply at a given L1 timestamp.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod schedule;
mod transport;

pub use schedule::{ActiveBlobLimit, blob_limit_at};
pub use transport::{
    BYTES_PER_BLOB, BlobSidecarEncoding, BlobTransactionParams, PreparedBlobSidecar,
    TransportError, pooled_blob_transaction_hash, prepare_blob_sidecar, sign_blob_transaction,
    versioned_hashes,
};
