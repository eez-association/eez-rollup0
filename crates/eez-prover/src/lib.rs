//! Shared proving seams.
//!
//! The composer always talks to the prover over the `prove.v1` gRPC API
//! (`eez-prover-client`'s `RemoteProver` → `eez-proverd`), which turns a
//! [`ProvingContext`](eez_protocol::ProvingContext) into the `proof` bytes
//! that the matching on-chain `IProofSystem.verify` accepts. The context
//! types ([`ProvingContext`](eez_protocol::ProvingContext),
//! [`BlockWitness`]) live in `eez-protocol`; this crate carries the
//! [`ProvingWitnessSource`] seam the composer fills them through, and the
//! error surface.
//!
//! The EEZ `sol!` ABI binding (structs, `postAndVerifyBatch`, and the
//! `BatchPosted` / `L2ExecutionPerformed` events) lives in `eez-protocol` —
//! the single ABI source the whole workspace shares.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use eez_protocol::BlockWitness;
use thiserror::Error;

/// Result alias.
pub type ProverResult<T> = Result<T, ProverError>;

/// Error returned by `RemoteProver::prove`.
#[derive(Debug, Error)]
pub enum ProverError {
    /// The proving backend (remote daemon, witness source, …) failed.
    #[error("prover backend: {0}")]
    Backend(String),
}

/// Produces the [`BlockWitness`] for a committed L2 block — the seam by which
/// the composer fills
/// [`ProvingContext::blocks`](eez_protocol::ProvingContext::blocks) without
/// owning the reth provider itself. `eez-node` backs this with the node's
/// provider + `eez_driver::witness`; the composer only calls it.
pub trait ProvingWitnessSource: Send + Sync + std::fmt::Debug {
    /// Build the RLP + augmented witness for block `number`.
    ///
    /// # Errors
    ///
    /// Returns a message if the block is missing or witness generation fails.
    fn block_witness(&self, number: u64) -> Result<BlockWitness, String>;
}
