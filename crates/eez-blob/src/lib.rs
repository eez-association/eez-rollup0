//! EIP-4844 carrier for Rollup0 blob settlement.
//!
//! `eez-payload-codec` owns the logical stream and its 31-byte field-element
//! packing; this crate owns everything Ethereum adds around those bytes: KZG
//! commitments, versioned hashes and the pooled type-3 transaction's hash.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod transport;

pub use transport::{
    BYTES_PER_BLOB, BlobSidecarEncoding, TransportError, post_batch_tx_hash, prepare_blob_sidecar,
    versioned_hashes,
};
