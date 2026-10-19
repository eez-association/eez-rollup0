//! EIP-4844 carrier for Rollup0 candidate-V1 settlement.
//!
//! `eez-payload-codec` owns the logical stream and its 31-byte field-element
//! packing; this crate owns everything Ethereum adds around those bytes: KZG
//! commitments, versioned hashes, the pooled type-3 transaction, and which
//! blob limit and sidecar wrapper apply at a given L1 timestamp.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod schedule;
mod transport;

pub use alloy_eips::eip4844::env_settings::{EnvKzgSettings, KzgSettings};
pub use schedule::{BlobFork, BlobSchedule, L1SlotClock, ScheduleError, l1_schedule_from_env};
pub use transport::{
    BYTES_PER_BLOB, BlobSidecarEncoding, BlobTransactionParams, PreparedBlobSidecar,
    SignedBlobTransaction, TransportError, pooled_blob_transaction_hash, prepare_blob_sidecar,
    sign_blob_transaction, versioned_hashes,
};
