//! Incremental structural admission of one `Prove` stream.
//!
//! One header must be followed by exactly its declared `from_block..=to_block`
//! span, in ascending order and with adjacent claimed hashes linked. The
//! assembler checks that shape and the aggregate quotas as chunks arrive.
//!
//! This is admission only: hashes, RLP, and witnesses remain untrusted until
//! backend re-execution. The byte quota uses the canonical encoded size of
//! decoded known fields and is independent of tonic's per-message limit.

use std::num::NonZeroU64;

use alloy_primitives::B256;
use alloy_rpc_types_debug::ExecutionWitness;
use eez_control_rpc::v1::{
    BlockWitness, ExecutionWitness as WireExecutionWitness, PostBatch, ProveChunk, ProveHeader,
    prove_chunk,
};
use prost::Message;
use thiserror::Error;

/// Resource quotas for one structurally admitted window.
///
/// The byte quota bounds canonical known fields accounted across accepted
/// chunks, including fields discarded after checking. The item quota is
/// checked before witness conversion and bounds accounted collection
/// cardinality and downstream traversal. Neither quota can undo allocations
/// Prost made while decoding the current message; tonic's per-message limit
/// bounds that stage. These are service defenses, not protocol bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WindowLimits {
    pub(crate) blocks: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) witness_items: usize,
}

/// One block retained after stream-level structural checks.
///
/// Admission checks its declared number, hash widths, witness presence, and
/// adjacency between streamed hash claims. Backend validation binds and checks
/// the RLP, hashes, and witness.
#[cfg_attr(test, derive(Clone))]
pub(crate) struct AdmittedBlock {
    /// Composer-declared number, checked against the admitted window range.
    pub(crate) declared_number: u64,
    /// Composer-claimed hash; backend validation must re-derive it from RLP.
    pub(crate) claimed_hash: B256,
    /// Composer-claimed parent; admission compares it with the preceding
    /// streamed hash when one exists.
    pub(crate) claimed_parent_hash: B256,
    pub(crate) rlp: Vec<u8>,
    pub(crate) witness: ExecutionWitness,
}

impl AdmittedBlock {
    /// Construct the minimal admitted block used by downstream unit tests.
    #[cfg(test)]
    pub(crate) fn test(
        declared_number: u64,
        claimed_parent_hash_byte: u8,
        claimed_hash_byte: u8,
    ) -> Self {
        Self {
            declared_number,
            claimed_hash: B256::repeat_byte(claimed_hash_byte),
            claimed_parent_hash: B256::repeat_byte(claimed_parent_hash_byte),
            rlp: Vec::new(),
            witness: ExecutionWitness::default(),
        }
    }
}

/// Structurally admitted Composer header and its submitted settlement calldata.
///
/// Every decoded header field is charged against the payload quota. Admission
/// then discards the non-authoritative Composer `public_inputs_hash` and the
/// checked-empty L1 block hash. The retained calldata remains untrusted until
/// settlement validation succeeds. The rollup claim is held separately only
/// long enough for the service to compare it with operator configuration.
pub(crate) struct AdmittedHeader {
    pub(crate) declared_from_block: u64,
    pub(crate) declared_to_block: u64,
    /// Composer-submitted calldata retained for canonical decoding and settlement checks.
    pub(crate) submitted_post_batch_calldata: Vec<u8>,
}

/// A complete, structurally checked window ready for backend validation.
///
/// Construction guarantees stream completeness and equality between the
/// Composer rollup claim and operator configuration, not consensus correctness.
pub(crate) struct AdmittedWindow {
    header: AdmittedHeader,
    blocks: AdmittedBlocks,
}

impl AdmittedWindow {
    /// Consume the admitted boundary and return the data needed by the
    /// complete validation-and-settlement pipeline.
    pub(crate) fn into_parts(self) -> (AdmittedHeader, AdmittedBlocks) {
        (self.header, self.blocks)
    }
}

/// Nonempty block sequence produced only by an identity-checked assembler.
///
/// Keeping the vector behind this private constructor prevents production
/// callers from bypassing [`AdmittedWindow`]'s stream and identity checks when
/// invoking validation.
pub(crate) struct AdmittedBlocks(Vec<AdmittedBlock>);

impl AdmittedBlocks {
    /// Borrow the structurally admitted blocks in stream order.
    #[cfg(test)]
    pub(crate) fn as_slice(&self) -> &[AdmittedBlock] {
        &self.0
    }

    /// Consume the admission capability for the validation backend.
    pub(crate) fn into_vec(self) -> Vec<AdmittedBlock> {
        self.0
    }

    /// Build a capability for unit tests that exercise validation in isolation.
    #[cfg(test)]
    pub(crate) fn for_test(blocks: Vec<AdmittedBlock>) -> Self {
        Self(blocks)
    }
}

/// Header values separated by whether they remain relevant after admission.
struct ParsedHeader {
    /// Composer claim checked by the service against operator configuration.
    claimed_rollup_id: u64,
    admitted: AdmittedHeader,
    declared_block_count: usize,
}

/// A Composer rollup claim that differs from operator configuration.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("Composer rollup id {claimed} does not match expected rollup id {expected}")]
pub(crate) struct RollupIdentityMismatch {
    claimed: u64,
    expected: u64,
}

#[derive(Debug, Error)]
pub(crate) enum WindowError {
    #[error("empty Prove stream")]
    EmptyStream,
    #[error("first chunk must be the window header, got block {number}")]
    BlockBeforeHeader { number: u64 },
    #[error("chunk at index {chunk_index} carries no kind")]
    MissingKind { chunk_index: usize },
    #[error("duplicate header chunk")]
    DuplicateHeader,
    #[error("header carries no post_batch")]
    MissingPostBatch,
    #[error("header post_batch carries a {actual}-byte l1_block_hash; it must be empty")]
    NonemptyL1BlockHash { actual: usize },
    #[error("invalid window bounds {from}..={to}")]
    InvalidBounds { from: u64, to: u64 },
    #[error("window {from}..={to} spans {span} blocks, limit is {max}")]
    BlockLimit {
        from: u64,
        to: u64,
        span: u64,
        max: usize,
    },
    #[error("window payload reached {attempted} bytes, limit is {max}")]
    PayloadLimit { attempted: usize, max: usize },
    #[error("window witness reached {attempted} items, limit is {max}")]
    WitnessItemLimit { attempted: usize, max: usize },
    #[error("unable to reserve storage for {attempted_blocks} streamed blocks")]
    StorageLimit { attempted_blocks: usize },
    #[error("window already carries its declared {expected} blocks")]
    ExtraBlock { expected: usize },
    #[error("expected block {expected} at block index {block_index}, got {actual}")]
    WrongBlockNumber {
        block_index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("block {number} has a {actual}-byte block hash")]
    BlockHashLength { number: u64, actual: usize },
    #[error("block {number} has a {actual}-byte parent hash")]
    ParentHashLength { number: u64, actual: usize },
    #[error("block {number} carries no execution witness")]
    MissingWitness { number: u64 },
    #[error(
        "hash-chain break at block {number}: expected parent {expected} from block {previous}, got {actual}"
    )]
    ChainBreak {
        number: u64,
        previous: u64,
        expected: B256,
        actual: B256,
    },
    #[error("window {from}..={to} expects {expected} blocks, stream ended after {actual}")]
    Incomplete {
        from: u64,
        to: u64,
        expected: usize,
        actual: usize,
    },
}

impl WindowError {
    /// Whether this refusal represents exhausted capacity rather than malformed input.
    pub(crate) fn is_resource_exhausted(&self) -> bool {
        matches!(
            self,
            Self::BlockLimit { .. }
                | Self::PayloadLimit { .. }
                | Self::WitnessItemLimit { .. }
                | Self::StorageLimit { .. }
        )
    }
}

/// Transport-independent structural admission state for one chunk stream.
///
/// The `false` state contains only a checked header. Verifying its Composer
/// rollup claim against operator configuration consumes it and produces the
/// `true` state; only that state can accept blocks or finalize a window.
pub(crate) struct WindowAssembler<const IDENTITY_CHECKED: bool> {
    limits: WindowLimits,
    accounted_payload_bytes: usize,
    accounted_witness_items: usize,
    claimed_rollup_id: u64,
    header: AdmittedHeader,
    declared_block_count: usize,
    blocks: Vec<AdmittedBlock>,
}

impl WindowAssembler<false> {
    /// Start admission from the stream's required header chunk.
    pub(crate) fn start(limits: WindowLimits, first: ProveChunk) -> Result<Self, WindowError> {
        let mut accounted_payload_bytes = 0;
        charge(
            &mut accounted_payload_bytes,
            first.encoded_len(),
            limits.payload_bytes,
            |attempted, max| WindowError::PayloadLimit { attempted, max },
        )?;
        let header = match first.kind {
            Some(prove_chunk::Kind::Header(header)) => header,
            Some(prove_chunk::Kind::Block(block)) => {
                return Err(WindowError::BlockBeforeHeader {
                    number: block.number,
                });
            }
            None => return Err(WindowError::MissingKind { chunk_index: 0 }),
        };
        let ParsedHeader {
            claimed_rollup_id,
            admitted: header,
            declared_block_count,
        } = Self::check_header(header, limits)?;

        Ok(Self {
            limits,
            accounted_payload_bytes,
            accounted_witness_items: 0,
            claimed_rollup_id,
            header,
            declared_block_count,
            blocks: Vec::new(),
        })
    }

    /// Return the Composer rollup claim for tracing before the identity check.
    pub(crate) fn claimed_rollup_id(&self) -> u64 {
        self.claimed_rollup_id
    }

    /// Borrow the admitted header so its range can be recorded before identity
    /// verification. Calldata is deliberately not traced.
    pub(crate) fn header(&self) -> &AdmittedHeader {
        &self.header
    }

    /// Consume the header-only state after matching its rollup claim against
    /// operator configuration, enabling block admission and finalization.
    pub(crate) fn verify_rollup_identity(
        self,
        expected_rollup_id: NonZeroU64,
    ) -> Result<WindowAssembler<true>, RollupIdentityMismatch> {
        let expected = expected_rollup_id.get();
        if self.claimed_rollup_id != expected {
            return Err(RollupIdentityMismatch {
                claimed: self.claimed_rollup_id,
                expected,
            });
        }

        Ok(WindowAssembler {
            limits: self.limits,
            accounted_payload_bytes: self.accounted_payload_bytes,
            accounted_witness_items: self.accounted_witness_items,
            claimed_rollup_id: expected,
            header: self.header,
            declared_block_count: self.declared_block_count,
            blocks: self.blocks,
        })
    }

    /// Parse the header, retain its rollup claim, and validate its range.
    fn check_header(
        header: ProveHeader,
        limits: WindowLimits,
    ) -> Result<ParsedHeader, WindowError> {
        let ProveHeader {
            rollup_id: claimed_rollup_id,
            from_block: declared_from_block,
            to_block: declared_to_block,
            post_batch: submitted_post_batch,
        } = header;
        let submitted_post_batch = submitted_post_batch.ok_or(WindowError::MissingPostBatch)?;
        // List every wire field explicitly so schema additions require a
        // deliberate retention, validation, or discard decision here.
        let PostBatch {
            // This non-authoritative Composer claim is deliberately ignored.
            // Only a locally recomputed hash authorized by every pipeline gate
            // may be signed.
            public_inputs_hash: _composer_claimed_public_inputs_hash,
            l1_block_hash: submitted_l1_block_hash,
            abi_calldata: submitted_post_batch_calldata,
        } = submitted_post_batch;
        let l1_block_hash_len = submitted_l1_block_hash.len();
        if l1_block_hash_len != 0 {
            return Err(WindowError::NonemptyL1BlockHash {
                actual: l1_block_hash_len,
            });
        }
        if declared_from_block == 0 || declared_from_block > declared_to_block {
            return Err(WindowError::InvalidBounds {
                from: declared_from_block,
                to: declared_to_block,
            });
        }
        // The checked nonzero, ordered bounds make this inclusive span overflow-safe.
        let span = declared_to_block - declared_from_block + 1;
        let max_blocks = u64::try_from(limits.blocks).unwrap_or(u64::MAX);
        if span > max_blocks {
            return Err(WindowError::BlockLimit {
                from: declared_from_block,
                to: declared_to_block,
                span,
                max: limits.blocks,
            });
        }
        let declared_block_count =
            usize::try_from(span).expect("span was bounded by the usize block limit");

        Ok(ParsedHeader {
            claimed_rollup_id,
            admitted: AdmittedHeader {
                declared_from_block,
                declared_to_block,
                submitted_post_batch_calldata,
            },
            declared_block_count,
        })
    }
}

impl WindowAssembler<true> {
    /// Account, structurally check, and retain the next block chunk.
    ///
    /// Accounting precedes some shape checks, so every error is terminal:
    /// callers must discard the assembler rather than retry the chunk.
    pub(crate) fn push(&mut self, chunk: ProveChunk) -> Result<(), WindowError> {
        charge(
            &mut self.accounted_payload_bytes,
            chunk.encoded_len(),
            self.limits.payload_bytes,
            |attempted, max| WindowError::PayloadLimit { attempted, max },
        )?;

        match chunk.kind {
            Some(prove_chunk::Kind::Header(_)) => Err(WindowError::DuplicateHeader),
            Some(prove_chunk::Kind::Block(block)) => self.push_block(block),
            None => Err(WindowError::MissingKind {
                chunk_index: 1 + self.blocks.len(),
            }),
        }
    }

    /// Finalize after the caller observes EOF, requiring exactly the declared
    /// span.
    pub(crate) fn finish(self) -> Result<AdmittedWindow, WindowError> {
        let header = self.header;
        if self.blocks.len() != self.declared_block_count {
            return Err(WindowError::Incomplete {
                from: header.declared_from_block,
                to: header.declared_to_block,
                expected: self.declared_block_count,
                actual: self.blocks.len(),
            });
        }
        Ok(AdmittedWindow {
            header,
            blocks: AdmittedBlocks(self.blocks),
        })
    }

    /// Structurally check and retain one block after the admitted header.
    fn push_block(&mut self, submitted_block: BlockWitness) -> Result<(), WindowError> {
        let block_index = self.blocks.len();
        let expected = (self.header.declared_from_block..=self.header.declared_to_block)
            .nth(block_index)
            .ok_or(WindowError::ExtraBlock {
                expected: self.declared_block_count,
            })?;
        if submitted_block.number != expected {
            return Err(WindowError::WrongBlockNumber {
                block_index,
                expected,
                actual: submitted_block.number,
            });
        }
        let BlockWitness {
            number: declared_number,
            hash: claimed_hash_bytes,
            parent_hash: claimed_parent_hash_bytes,
            rlp,
            witness,
        } = submitted_block;
        let claimed_hash = B256::try_from(claimed_hash_bytes.as_slice()).map_err(|_| {
            WindowError::BlockHashLength {
                number: declared_number,
                actual: claimed_hash_bytes.len(),
            }
        })?;
        let claimed_parent_hash =
            B256::try_from(claimed_parent_hash_bytes.as_slice()).map_err(|_| {
                WindowError::ParentHashLength {
                    number: declared_number,
                    actual: claimed_parent_hash_bytes.len(),
                }
            })?;
        let wire_witness = witness.ok_or(WindowError::MissingWitness {
            number: declared_number,
        })?;
        // Prost has already allocated the decoded vectors. Enforce the item
        // quota before conversion allocates the retained Alloy collections.
        charge(
            &mut self.accounted_witness_items,
            wire_witness_item_count(&wire_witness),
            self.limits.witness_items,
            |attempted, max| WindowError::WitnessItemLimit { attempted, max },
        )?;
        let witness = into_execution_witness(wire_witness);
        // This gate checks adjacency between stream claims; backend validation
        // binds each claim to the decoded block RLP.
        if let Some(previous) = self.blocks.last()
            && claimed_parent_hash != previous.claimed_hash
        {
            return Err(WindowError::ChainBreak {
                number: declared_number,
                previous: previous.declared_number,
                expected: previous.claimed_hash,
                actual: claimed_parent_hash,
            });
        }

        // Grow lazily so an allowed span does not reserve memory before its
        // block chunks actually arrive.
        self.blocks
            .try_reserve(1)
            .map_err(|_| WindowError::StorageLimit {
                attempted_blocks: block_index.saturating_add(1),
            })?;
        self.blocks.push(AdmittedBlock {
            declared_number,
            claimed_hash,
            claimed_parent_hash,
            rlp,
            witness,
        });
        Ok(())
    }
}

/// Charge a quota counter without overflowing or exceeding its limit.
fn charge(
    current: &mut usize,
    add: usize,
    max: usize,
    over: impl Fn(usize, usize) -> WindowError,
) -> Result<(), WindowError> {
    let attempted = current
        .checked_add(add)
        .ok_or_else(|| over(usize::MAX, max))?;
    if attempted > max {
        return Err(over(attempted, max));
    }
    *current = attempted;
    Ok(())
}

/// Convert the protobuf witness into the Alloy type consumed by Stateless.
/// Converting each payload into `Bytes` reuses its source allocation.
fn into_execution_witness(witness: WireExecutionWitness) -> ExecutionWitness {
    let WireExecutionWitness {
        state,
        codes,
        keys,
        headers,
    } = witness;
    ExecutionWitness {
        state: state.into_iter().map(Into::into).collect(),
        codes: codes.into_iter().map(Into::into).collect(),
        keys: keys.into_iter().map(Into::into).collect(),
        headers: headers.into_iter().map(Into::into).collect(),
    }
}

/// Count protobuf witness items before allocating the retained representation.
fn wire_witness_item_count(witness: &WireExecutionWitness) -> usize {
    witness
        .state
        .len()
        .saturating_add(witness.codes.len())
        .saturating_add(witness.keys.len())
        .saturating_add(witness.headers.len())
}

#[cfg(test)]
mod tests;
