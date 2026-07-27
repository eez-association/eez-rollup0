//! Proving context types ([`ProvingContext`], [`BlockWitness`]) the
//! composer and the prover wire must agree on.

use alloy_primitives::{B256, Bytes, b256};
use alloy_rpc_types_debug::ExecutionWitness;

use crate::batch::EvmBatch;

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
/// The composer fills this and calls `RemoteProver::prove`; the whole window's
/// block data travels in-band ([`blocks`](Self::blocks)) so the prover is a
/// stateless function of its input — no feed, no cursor, no backfill.
/// The mock prover ignores every field (it signs a fixed digest), so a
/// mock-mode composer may leave [`blocks`](Self::blocks) empty.
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

/// Fixed digest signed by the mock prover and recovered against by
/// `MockECDSAProofSystem.verify`. Equals `keccak256("eez-mock-prover")`.
/// Both sides MUST agree on this value bit-for-bit — if you change it,
/// change it in `contracts/src/MockECDSAProofSystem.sol` too.
pub const MOCK_PROVER_DIGEST: B256 =
    b256!("0x02753eb401fed50317a35a1cfa1c67c003b761ba4009cbe36632c724ef0a06df");
