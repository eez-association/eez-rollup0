//! Adapter for in-process Stateless validation.
//!
//! This module exact-decodes and binds streamed block claims, loads the
//! operator-configured chain trust input, derives checkpoint plans, invokes
//! Stateless/Reth, checks window continuity, and maps validated execution facts
//! into associated per-block output.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
use alloy_genesis::ChainConfig;
use alloy_genesis::Genesis;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_evm::EezEvmConfig;
use eez_proof_signer::validate::support::{
    CheckpointPlan, decode_match_and_recover_signers, observe_outbound_events,
};
use eez_proof_signer::validate::{
    AdmittedBlock, BackendBlockOutput, BlockAnchor, SettlementBlockEvidence, StateCheckpoint,
    ValidatedBlock, ValidationBackend, ValidationError,
};
use lru::LruCache;
use reth_chainspec::ChainSpec;
use stateless_reth::validation::StatelessValidationError;
use stateless_reth::{
    StatelessValidationOutput, stateless_validation_recovered,
    stateless_validation_recovered_with_state_checkpoints,
};
use tokio::sync::Semaphore;
use tracing::{debug, info, trace};

mod chain_config;

use chain_config::{ChainDocumentKind, load_chain_document};

const MAX_CACHED_BLOCKS: usize = 32_768;
const MAX_CACHED_BYTES: usize = 512 * 1024 * 1024;
const MAX_CONCURRENT_VALIDATIONS: usize = 4;

#[derive(Debug)]
struct BlockCache {
    entries: LruCache<B256, (Arc<ValidatedBlock>, usize)>,
    bytes: usize,
    max_bytes: usize,
}

impl BlockCache {
    fn insert(&mut self, block: Arc<ValidatedBlock>) -> Arc<ValidatedBlock> {
        if let Some((cached, _)) = self.entries.get(&block.hash()) {
            return Arc::clone(cached);
        }
        let bytes = block.size();
        // Oversized results remain usable by their session without displacing the cache.
        if bytes <= self.max_bytes {
            while self.bytes > self.max_bytes - bytes
                || self.entries.len() == self.entries.cap().get()
            {
                let (_, (_, removed_bytes)) = self.entries.pop_lru().expect("nonempty cache");
                self.bytes -= removed_bytes;
            }
            self.entries.put(block.hash(), (Arc::clone(&block), bytes));
            self.bytes += bytes;
        }
        block
    }
}

/// In-process Stateless/Reth backend configured from operator-selected rules.
#[derive(Debug, Clone)]
pub struct Backend {
    chain_spec: Arc<ChainSpec>,
    evm_config: EezEvmConfig,
    expected_l2_system_address: Address,
    blocks: Arc<Mutex<BlockCache>>,
    validation_slots: Arc<Semaphore>,
}

impl Backend {
    /// EIP-155 identity pinned by the loaded execution chain specification.
    pub fn chain_id(&self) -> u64 {
        self.chain_spec.chain().id()
    }

    /// Load execution rules and bind the deployment's privileged L2 identity.
    pub fn from_chain_document_file(
        path: &Path,
        expected_l2_system_address: Address,
    ) -> eyre::Result<Self> {
        let (genesis, document_kind) = load_chain_document(path)?;
        let chain_config = &genesis.config;
        let genesis_timestamp =
            (document_kind == ChainDocumentKind::Genesis).then_some(genesis.timestamp);
        info!(
            path = %path.display(),
            source = document_kind.label(),
            chain_id = chain_config.chain_id,
            "loaded stateless chain configuration",
        );
        debug!(
            ?chain_config,
            ?genesis_timestamp,
            "stateless chain configuration details",
        );
        Self::from_genesis(genesis, expected_l2_system_address)
    }

    #[cfg(test)]
    pub(super) fn new(chain_config: ChainConfig, expected_l2_system_address: Address) -> Self {
        Self::from_genesis(
            Genesis {
                config: chain_config,
                ..Default::default()
            },
            expected_l2_system_address,
        )
        .unwrap()
    }

    fn from_genesis(genesis: Genesis, expected_l2_system_address: Address) -> eyre::Result<Self> {
        let chain_spec = Arc::new(ChainSpec::from_genesis(genesis));
        let evm_config = EezEvmConfig::new(Arc::clone(&chain_spec))?;
        Ok(Self {
            chain_spec,
            evm_config,
            expected_l2_system_address,
            blocks: Arc::new(Mutex::new(BlockCache {
                entries: LruCache::new(NonZeroUsize::new(MAX_CACHED_BLOCKS).unwrap()),
                bytes: 0,
                max_bytes: MAX_CACHED_BYTES,
            })),
            validation_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_VALIDATIONS)),
        })
    }

    /// Execute one block against its exact parent and publish checked evidence.
    fn validate_block(
        &self,
        parent: BlockAnchor,
        admitted: &AdmittedBlock,
        witness: ExecutionWitness,
    ) -> Result<Arc<ValidatedBlock>, ValidationError> {
        admitted.check_parent(parent)?;
        let started = Instant::now();
        let block = decode_match_and_recover_signers(admitted, &self.chain_spec)?;
        let number = block.header().number;
        let (plan, system_sender_flags) =
            CheckpointPlan::for_block(&block, self.expected_l2_system_address);
        // Checkpoints are determined by block contents, independently of which
        // range a later Finalize selects. Ordinary blocks use normal execution.
        let (output, checkpoints) = if plan.positions().is_empty() {
            let output = stateless_validation_recovered(
                block.execution_input(),
                witness,
                Arc::clone(&self.chain_spec),
                self.evm_config.clone(),
            )
            .map_err(|error| map_stateless_error(number, error))?;
            (output, Vec::new())
        } else {
            let output = stateless_validation_recovered_with_state_checkpoints(
                block.execution_input(),
                witness,
                Arc::clone(&self.chain_spec),
                self.evm_config.clone(),
                plan.positions(),
            )
            .map_err(|error| map_stateless_error(number, error))?;
            let checkpoints = output
                .checkpoints
                .checkpoints
                .into_iter()
                .map(|checkpoint| StateCheckpoint {
                    at: checkpoint.at,
                    state_root: checkpoint.state_root,
                    block_hash: checkpoint.block_hash,
                })
                .collect::<Vec<_>>();
            plan.verify_returned(&checkpoints)?;
            (output.validation, checkpoints)
        };
        let StatelessValidationOutput {
            block_hash: computed_hash,
            pre_state_root,
            post_state_root,
            execution_output,
            block_access_list: _validated_block_access_list,
        } = output;
        if pre_state_root != parent.state_root {
            return Err(ValidationError::Rejected(format!(
                "stateless state roots do not telescope at block {number}: expected pre-state root {}, got {pre_state_root}",
                parent.state_root,
            )));
        }
        let receipts = execution_output.result.receipts;
        let output = BackendBlockOutput {
            computed_hash,
            receipt_successes: receipts.iter().map(|receipt| receipt.success).collect(),
            transaction_state_checkpoints: checkpoints,
            post_state_root,
            settlement_evidence: SettlementBlockEvidence {
                system_sender_flags,
                observed_outbound_events: observe_outbound_events(&receipts),
            },
        };
        let checked = block.finish(output, pre_state_root)?;
        trace!(block_number = number, %computed_hash, %post_state_root,
            elapsed_ms = elapsed_millis(started), "stateless block validated");
        Ok(checked)
    }
}

#[async_trait::async_trait]
impl ValidationBackend for Backend {
    fn label(&self) -> &'static str {
        "stateless"
    }

    fn chain_id(&self) -> u64 {
        self.chain_id()
    }

    fn expected_l2_system_address(&self) -> Address {
        self.expected_l2_system_address
    }

    fn validate_anchor(&self, _anchor: BlockAnchor) -> Result<(), ValidationError> {
        Ok(())
    }

    async fn validate_next(
        &self,
        parent: BlockAnchor,
        block: &AdmittedBlock,
        witness: ExecutionWitness,
        request_timeout: Duration,
    ) -> Result<(Arc<ValidatedBlock>, bool), ValidationError> {
        tokio::time::timeout(request_timeout, async {
            {
                let mut blocks = self.blocks.lock().unwrap();
                if let Some((cached, _)) = blocks.entries.get(&block.claimed_hash()) {
                    cached.matches_submission(block, parent)?;
                    return Ok((Arc::clone(cached), true));
                }
            }
            block.check_parent(parent)?;
            let permit = Arc::clone(&self.validation_slots)
                .acquire_owned()
                .await
                .map_err(|_| {
                    ValidationError::Unavailable(
                        "stateless validation scheduler stopped".to_owned(),
                    )
                })?;
            let backend = self.clone();
            let admitted = block.clone();
            tokio::task::spawn_blocking(move || {
                // The worker owns its slot until execution ends, even if its caller times out.
                // A completed job may populate the cache for later callers; partial results never do.
                let _permit = permit;
                let output = backend.validate_block(parent, &admitted, witness)?;
                let mut blocks = backend.blocks.lock().unwrap();
                // Concurrent misses may execute twice; only fully checked evidence is published.
                Ok((blocks.insert(output), false))
            })
            .await
            .map_err(|error| {
                ValidationError::InternalInvariant(format!(
                    "stateless validation worker failed: {error}"
                ))
            })?
        })
        .await
        .map_err(|_| ValidationError::DeadlineExceeded)?
    }
}

/// Classify a rejected local checkpoint plan as an internal invariant failure;
/// all other Stateless errors reject the input.
fn map_stateless_error(block_number: u64, error: StatelessValidationError) -> ValidationError {
    match error {
        error @ (StatelessValidationError::UnorderedCheckpoints { .. }
        | StatelessValidationError::TransactionCheckpointOutOfBounds { .. }) => {
            ValidationError::InternalInvariant(format!(
                "stateless rejected the locally derived checkpoint plan for block {block_number}: \
                 {error}",
            ))
        }
        error => ValidationError::Rejected(format!(
            "stateless validation rejected block {block_number}: {error}",
        )),
    }
}

fn elapsed_millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
