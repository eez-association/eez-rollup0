//! Prover abstraction shared by all composers.
//!
//! A [`Prover`] turns proving context into the `proof` bytes that the
//! matching on-chain `IProofSystem.verify` accepts. The production proof signer
//! takes the calldata + chain context, runs the STF itself, derives the
//! per-rollup hashes, and produces an attestation that commits to them.
//!
//! The EEZ `sol!` ABI binding (structs, `postAndVerifyBatch`, and the
//! `BatchPosted` / `L2ExecutionPerformed` events) lives in `eez-protocol` —
//! the single ABI source the whole workspace shares. This crate is
//! just the prover abstraction.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use alloy_primitives::{B256, Bytes};
use alloy_rpc_types_debug::ExecutionWitness;
use async_trait::async_trait;
use eez_protocol::EvmBatch;
use thiserror::Error;

/// Result alias.
pub type ProverResult<T> = Result<T, ProverError>;

/// A transient proving failure for which the complete operation may be retried.
///
/// These variants are the transport-independent form of the Composer profile's
/// retryable gRPC status allowlist. Prover implementations in any process or
/// language map their wire status into this enum before returning through the
/// [`Prover`] trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RetryableProverError {
    /// The prover or its transport is temporarily unavailable.
    #[error("UNAVAILABLE")]
    Unavailable,
    /// The proving attempt exceeded its deadline.
    #[error("DEADLINE_EXCEEDED")]
    DeadlineExceeded,
    /// The attempt was aborted because its state or snapshot changed.
    #[error("ABORTED")]
    Aborted,
}

/// A proof rejection that identifies one held cross-chain candidate.
///
/// The Composer may remove the identified candidate and rebuild. Other proof
/// failures deliberately remain ordinary backend errors and must not trigger
/// eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ActionableProverFailure {
    /// The terminal Sync block contains the poisoned outbound user transaction.
    #[error("outbound transaction {transaction_index} ({transaction_hash})")]
    Outbound {
        /// Zero-based position of the original user transaction in the Sync block.
        transaction_index: usize,
        /// Canonical signed transaction hash, equal to `HeldTx.hash`.
        transaction_hash: B256,
    },
    /// The posted batch contains the poisoned inbound effect entry.
    #[error("inbound entry {entry_index} ({entry_hash})")]
    Inbound {
        /// Zero-based position in `PostBatch.entries`.
        entry_index: usize,
        /// Keccak256 of the canonical `ExecutionEntrySol` ABI encoding.
        entry_hash: B256,
    },
}

/// Error returned by [`Prover::prove`].
#[derive(Debug, Error)]
pub enum ProverError {
    /// The proving backend (remote daemon, witness source, …) failed.
    #[error("prover backend: {0}")]
    Backend(String),
    /// The prover rejected one attributable cross-chain candidate.
    #[error("actionable prover rejection ({failure}): {message}")]
    Actionable {
        /// Candidate identity the Composer may safely resolve and remove.
        failure: ActionableProverFailure,
        /// Diagnostic detail; callers MUST NOT parse it for classification.
        message: String,
    },
    /// A transient failure that permits retrying the complete proving operation.
    #[error("retryable prover error ({kind}): {message}")]
    Retryable {
        /// Canonical retry classification.
        kind: RetryableProverError,
        /// Diagnostic detail; callers MUST NOT parse it for classification.
        message: String,
    },
}

impl ProverError {
    /// Return the canonical retry classification, if this error is retryable.
    #[must_use]
    pub const fn retryable_kind(&self) -> Option<RetryableProverError> {
        match self {
            Self::Retryable { kind, .. } => Some(*kind),
            Self::Signer(_) | Self::Backend(_) | Self::Actionable { .. } => None,
        }
    }

    /// Return an attributable candidate rejection, if present.
    #[must_use]
    pub const fn actionable_failure(&self) -> Option<ActionableProverFailure> {
        match self {
            Self::Actionable { failure, .. } => Some(*failure),
            Self::Signer(_) | Self::Backend(_) | Self::Retryable { .. } => None,
        }
    }
}

/// One settling-window block the prover re-executes: its consensus RLP plus
/// the exact (augmented) execution witness that re-execution needs.
#[derive(Debug, Clone)]
pub struct BlockWitness {
    /// L2 block number.
    pub number: u64,
    /// The block hash the composer sealed — the prover cross-checks its own
    /// re-derived hash against this.
    pub hash: B256,
    /// Parent hash — lets the prover chain contiguity across the window.
    pub parent_hash: B256,
    /// Consensus RLP (header + body).
    pub rlp: Bytes,
    /// Minimal execution witness (`state`/`codes`/`keys`/`headers`), augmented
    /// with the removal-closure nodes intermediate per-tx roots need.
    pub witness: ExecutionWitness,
}

/// Inputs the prover needs to prove one posted settlement window.
///
/// The composer fills this and calls [`Prover::prove`]; the whole window's
/// block data travels in-band ([`blocks`](Self::blocks)) so the prover is a
/// stateless function of its input — no feed, no cursor, no backfill.
#[derive(Debug, Clone, Default)]
pub struct ProvingContext {
    /// The L2 this window settles.
    pub rollup_id: u64,
    /// First block of the window: `posted + 1` (the OD-5 anchor block + 1).
    pub from_block: u64,
    /// Last (settling) block of the window: the Sync height.
    pub to_block: u64,
    /// The authoritative postBatch payload (proof carriers filled, `proofs[]`
    /// empty). The prover recomputes the `publicInputsHash` from this.
    pub batch: EvmBatch,
    /// Every window block's RLP + augmented witness, in block order.
    pub blocks: Vec<BlockWitness>,
    /// `blockhash(N)` for a block-bound batch's `blockNumber = N`; `None` for a
    /// timeless (0) batch.
    pub l1_block_hash: Option<B256>,
}

/// Produces the [`BlockWitness`] for a committed L2 block — the seam by which
/// the composer fills [`ProvingContext::blocks`] without owning the reth
/// provider itself. `eez-composer` backs this with the node's provider +
/// `eez_driver::witness`; the composer only calls it.
pub trait ProvingWitnessSource: Send + Sync + std::fmt::Debug {
    /// Build the RLP + augmented witness for block `number`.
    ///
    /// # Errors
    ///
    /// Returns a message if the block is missing or witness generation fails.
    fn block_witness(&self, number: u64) -> Result<BlockWitness, String>;
}

/// Turns proving context into `proof` bytes the matching on-chain
/// `IProofSystem.verify` accepts.
#[async_trait]
pub trait Prover: Send + Sync + std::fmt::Debug {
    /// Produce a proof.
    ///
    /// # Errors
    ///
    /// Implementation-defined. A prover may surface transport, execution, or
    /// proof-generation errors.
    async fn prove(&self, ctx: ProvingContext) -> ProverResult<Bytes>;

    /// Registry-membership key for this prover. The per-rollup
    /// `IRollupContract` records this in its vkey map; EEZ reads it
    /// when checking proof-system membership.
    fn vkey(&self) -> B256;
}
