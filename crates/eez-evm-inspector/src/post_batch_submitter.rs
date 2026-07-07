//! Phase 09 §E1 — post-batch submitter.
//!
//! Threads the workspace pieces built across §B–§D into a single
//! L1-submission pipeline:
//!
//! 1. **Touched-rollup extraction** — scan the [`EvmBatch`] for every
//!    rollup id referenced by an entry's `destinationRollupId`, an
//!    entry's `stateDeltas[].rollupId`, or a lookup call's
//!    `destinationRollupId`. Dedup; this is the input set for the
//!    proof-plan resolver.
//! 2. **Proof-plan resolution** — call [`ProofPlanResolver::resolve`]
//!    (§B) to read `EEZ.rollups(rid).rollupContract`,
//!    `Rollup.checkProofSystemsAndGetVkeys`, and
//!    `IRollupContract.getTimestampAndBlockHash` for each touched
//!    rollup. Returns a validated [`ProofPlan`].
//! 3. **Proof-carrier population** — mutate `batch.inner` to carry
//!    `proofSystems`, `rollupIdsWithProofSystems`, and
//!    `crossProofSystemInteractions` from the plan. For Phase 09
//!    blobs/callData stay empty (`blobIndices = []`, `callData = b""`,
//!    `crossProofSystemInteractions = bytes32(0)`).
//! 4. **publicInputsHash[k] construction** — call
//!    [`all_per_ps_hashes`] (§C) to compute one digest per PS.
//! 5. **Per-PS signature** — for the single ECDSA proof system in this
//!    phase, sign every per-PS digest with the configured
//!    [`EcdsaProofSigner`] (§D). Append each 65-byte signature to
//!    `batch.inner.proofs[]` parallel to `proofSystems[]`. (Phase 10+
//!    swaps this loop for a `BatchProofProducer` trait so different
//!    PSes can emit different proof shapes.)
//! 6. **L1 submission** — encode calldata via
//!    [`EvmProtocol::encode_postbatch`] (proofs ride inside the
//!    batch struct under the multi-prover ABI), fetch the
//!    poster's `pending` nonce explicitly via
//!    `eth_getTransactionCount(addr, "pending")`, build a single
//!    `TransactionRequest` with that nonce + `chain_id`, send it via
//!    the wallet-filled provider, and wait for the receipt.
//! 7. **Receipt parsing** — wait for the receipt with an explicit
//!    timeout (`PostBatchError::ReceiptTimeout` on expiry — loud
//!    failure, no replacement policy). Require `status == 1`. Decode
//!    the single `BatchPosted(rollupCount)` event and every
//!    `L2ExecutionPerformed(rollupId, newState)` event in the receipt
//!    logs, filtering to those emitted by the configured EEZ address
//!    so an unrelated contract emitting a colliding selector can't
//!    spoof the outcome. Returns a [`PostBatchOutcome`] carrying both.
//!
//! No automatic nonce management, no replacement-tx policy, no retry
//! loop — `invariant 7` says failures are loud.
//!
//! # Spec anchors
//!
//! - `docs/plans/09-postbatch-poster.md` §E1.
//! - `docs/DERIVATION.md` §6e.
//! - `sync-rollups-protocol@0864392`:
//!   - `src/EEZ.sol:202` (`L2ExecutionPerformed` event).
//!   - `src/EEZ.sol:216` (`BatchPosted` event).
//!   - `src/EEZ.sol:486-555` (`_validateStructure`).
//!   - `src/EEZ.sol:606-668` (`_verifyProofSystemBatch`).
//!
//! [`EvmBatch`]: eez_evm::EvmBatch
//! [`ProofPlanResolver::resolve`]: eez_protocol::ProofPlanResolver::resolve
//! [`ProofPlan`]: eez_protocol::ProofPlan
//! [`all_per_ps_hashes`]: eez_evm::public_inputs::all_per_ps_hashes
//! [`EvmProtocol::encode_postbatch`]: eez_evm::EvmProtocol::encode_postbatch
//! [`EcdsaProofSigner`]: eez_evm::signer::EcdsaProofSigner

use std::collections::BTreeSet;
use std::time::Duration;

use alloy_network::{Ethereum, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Log, TransactionReceipt, TransactionRequest};
use alloy_sol_types::{SolEvent, sol};
use eez_evm::public_inputs::{all_per_ps_hashes, entry_hash, lookup_call_hash};
use eez_evm::signer::{EcdsaProofSigner, SignerError};
use eez_evm::{EvmBatch, EvmProtocol};
use eez_protocol::{
    ChainProtocol, ExecutorError, ProofPlan, ProofPlanInvariantError, ProofPlanResolver, RollupId,
};

use eez_evm::types::RollupIdWithProofSystemsSol;

// ── Event bindings ─────────────────────────────────────────────────

sol! {
    /// `event BatchPosted(uint256 indexed rollupCount);`
    /// `EEZ.sol:216`. Single indexed topic. The event name MUST
    /// match the on-chain Solidity identifier exactly — the
    /// `sol!`-derived `SIGNATURE_HASH` is `keccak256("Name(types..)")`
    /// over this Rust-side name, so any mismatch silently breaks
    /// receipt parsing (and was the §F1 first-run bug).
    #[allow(missing_docs, reason = "sol! generates the doc-less variant struct")]
    event BatchPosted(uint256 indexed rollupCount);

    /// `event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);`
    /// `EEZ.sol:202`. Indexed rollupId, data = newState. See
    /// [`BatchPosted`] for the name-must-match rationale.
    #[allow(missing_docs, reason = "sol! generates the doc-less variant struct")]
    event L2ExecutionPerformed(uint256 indexed rollupId, bytes32 newState);
}

// ── Outcome ───────────────────────────────────────────────────────

/// A single `L2ExecutionPerformed(rollupId, newState)` event observed
/// in the submitted batch's receipt. Named without the `Performed`
/// suffix to avoid colliding with the `sol!`-generated event type
/// (which MUST keep the canonical name so its derived signature
/// hash matches the on-chain topic0).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2ExecutionLog {
    /// Rollup id the execution applied to.
    pub rollup_id: U256,
    /// Resulting state root after the apply.
    pub new_state: B256,
}

/// Successful submission outcome.
#[derive(Debug, Clone)]
pub struct PostBatchOutcome {
    /// Hash of the submitted L1 transaction.
    pub tx_hash: B256,
    /// `rollupCount` decoded from the receipt's single `BatchPosted`
    /// log. Equals the number of distinct rollups whose state roots
    /// advanced in this batch.
    pub rollup_count: U256,
    /// Every `L2ExecutionPerformed` event in receipt order.
    pub l2_executions: Vec<L2ExecutionLog>,
}

// ── Errors ────────────────────────────────────────────────────────

/// Errors surfaced by [`PostBatchSubmitter::submit`].
///
/// Each variant carries enough context to attribute the failure to a
/// specific pipeline stage; no variant silently swallows information
/// (per `invariant 7`).
#[derive(Debug, thiserror::Error)]
pub enum PostBatchError {
    /// The batch carries no rollup references — empty `entries[]` AND
    /// empty `l1ToL2lookupCalls[]`. Caller passed an empty batch by
    /// mistake; the on-chain `_validateStructure` would reject this
    /// regardless.
    #[error("post_batch: batch is empty — no entries, no lookup calls")]
    EmptyBatch,
    /// A referenced `rollupId` overflowed `u64`. The on-chain ABI is
    /// `uint256` but `eez-protocol`'s `RollupId(u64)` is the
    /// workspace-wide carrier. Surfacing this explicitly (rather than
    /// silently saturating) means the caller sees a typed boundary
    /// violation instead of a confusing later registry-miss.
    #[error("post_batch: rollupId 0x{value:x} doesn't fit in u64")]
    RollupIdOverflow {
        /// The offending uint256 value.
        value: U256,
    },
    /// `batch.inner.blobIndices` was non-empty. Phase 09 doesn't wire
    /// the blob carrier path — `submit` always passes an empty
    /// `blob_hashes` slice to `all_per_ps_hashes`, so a non-empty
    /// `blobIndices` would silently mismatch the on-chain
    /// `blobhash(blobIndices[i])` fold. Refuse the submission instead.
    /// Phase 10+ wires the blob carrier path end-to-end.
    #[error(
        "post_batch: batch.inner.blobIndices has {len} entries; \
         Phase 09 submitter requires blobIndices == [] (blob carrier \
         path is deferred to Phase 10+)"
    )]
    UnsupportedBlobIndices {
        /// Number of blob indices on the rejected batch.
        len: usize,
    },
    /// The L1 transaction did not produce a receipt within the
    /// configured timeout. Loud failure per `invariant 7` — the
    /// submitter doesn't replace / re-send.
    #[error(
        "post_batch: tx 0x{tx_hash:x} produced no receipt within {timeout_secs}s; \
         no replacement-tx policy in this phase"
    )]
    ReceiptTimeout {
        /// Submitted transaction hash.
        tx_hash: B256,
        /// Configured timeout, in seconds (rendered).
        timeout_secs: u64,
    },
    /// [`ProofPlanResolver::resolve`] failed.
    #[error("post_batch: proof-plan resolution failed: {0}")]
    Resolver(#[source] ExecutorError),
    /// The resolved plan's `proof_systems.len()` didn't match the
    /// configured signer's reach (Phase 09 single-PS expects exactly
    /// one PS in the plan). Surfaced explicitly so callers don't
    /// silently sign a multi-PS batch with a single key — Phase 10+
    /// swaps the signer for a multi-key producer.
    #[error(
        "post_batch: plan returned {got} proof systems; this Phase-09 \
         submitter is single-PS (expected exactly 1)"
    )]
    UnsupportedProofSystemCount {
        /// Number of proof systems in the resolved plan.
        got: usize,
    },
    /// One per-PS digest could not be signed. Almost always a sentinel
    /// for k256 `RecoveryId` drifting outside the parity range — see
    /// `SignerError::RecoveryIdOutOfRange`.
    #[error("post_batch: per-PS signature failed (ps_index={ps_index}): {source}")]
    Sign {
        /// Index into `plan.proof_systems`.
        ps_index: usize,
        /// Underlying signer error.
        #[source]
        source: SignerError,
    },
    /// `plan.check_invariants()` failed inside [`all_per_ps_hashes`].
    /// The resolver is supposed to enforce these — this is a
    /// belt-and-suspenders check.
    #[error("post_batch: proof-plan invariants violated: {0}")]
    PlanInvariants(#[source] ProofPlanInvariantError),
    /// An eth_call / send_transaction / receipt-fetch RPC failed. The
    /// `stage` field attributes the failure to the specific RPC.
    #[error("post_batch: provider call failed at {stage}: {source}")]
    Provider {
        /// Pipeline stage where the failure occurred.
        stage: &'static str,
        /// Underlying transport / contract error (string-typed
        /// because alloy transport errors don't `Clone`/`Eq`).
        #[source]
        source: alloy_provider::PendingTransactionError,
    },
    /// `eth_getTransactionCount` or `eth_sendTransaction` failed at
    /// the transport layer (alloy `RpcError`, distinct from the
    /// `PendingTransactionError` variant above which wraps watch /
    /// receipt-fetch failures).
    #[error("post_batch: transport call failed at {stage}: {source}")]
    Transport {
        /// Pipeline stage where the failure occurred.
        stage: &'static str,
        /// Underlying alloy transport error.
        #[source]
        source: alloy_provider::transport::TransportError,
    },
    /// The receipt arrived but reported a reverted transaction.
    #[error("post_batch: receipt reported failure (tx_hash={tx_hash}, status=0)")]
    ReceiptStatusFailure {
        /// Submitted transaction hash.
        tx_hash: B256,
    },
    /// Receipt parsed cleanly but no `BatchPosted` log was present.
    /// The on-chain `postVerifyAndExecuteOrSaveExecutionsFromBatch`
    /// always emits exactly one `BatchPosted`; absence means the EEZ
    /// address was wrong or the contract was upgraded out from under
    /// us.
    #[error("post_batch: receipt has no BatchPosted log (tx_hash={tx_hash})")]
    MissingBatchPostedEvent {
        /// Submitted transaction hash.
        tx_hash: B256,
    },
    /// Receipt carried multiple `BatchPosted` logs (the contract
    /// emits exactly one — multiple means something is wrong).
    #[error("post_batch: receipt has {count} BatchPosted logs, expected 1 (tx_hash={tx_hash})")]
    DuplicateBatchPostedEvent {
        /// Submitted transaction hash.
        tx_hash: B256,
        /// Observed log count.
        count: usize,
    },
    /// An event-log decode failed (mismatched ABI shape).
    #[error("post_batch: event log decode failed ({event}): {source}")]
    EventDecode {
        /// Event name that failed to decode.
        event: &'static str,
        /// Underlying alloy sol-types decode error.
        #[source]
        source: alloy_sol_types::Error,
    },
}

// ── Submitter ─────────────────────────────────────────────────────

/// Phase 09 single-PS submitter wiring §B + §C + §D into a live
/// `postVerifyAndExecuteOrSaveExecutionsFromBatch` submission.
///
/// `provider` MUST be wallet-filled — i.e. constructed via
/// `ProviderBuilder::new().wallet(EthereumWallet::from(<poster
/// signer>))` — so [`Provider::send_transaction`] signs the L1 tx
/// with the poster EOA. The submitter only takes the trait surface;
/// it never reads the wallet directly.
///
/// `proof_signer` signs each per-PS `publicInputsHash` digest. On
/// devnet it's the same key as the poster (`$PK_COMPOSER ==
/// $SEQUENCER_KEY`); in production they MUST be distinct.
#[derive(Debug, Clone)]
pub struct PostBatchSubmitter<R, P>
where
    R: ProofPlanResolver<EvmProtocol>,
    P: Provider<Ethereum>,
{
    provider: P,
    resolver: R,
    proof_signer: EcdsaProofSigner,
    eez_address: Address,
    poster_address: Address,
    chain_id: u64,
    /// Maximum time to wait for the L1 tx receipt. Exceeding this
    /// returns [`PostBatchError::ReceiptTimeout`] — loud failure per
    /// `invariant 7`, no replacement-tx policy.
    receipt_timeout: Duration,
}

/// Default receipt-wait timeout. Devnet block time is ~1s; the
/// kurtosis L1 should produce a receipt well within 30s. Tunable via
/// [`PostBatchSubmitter::with_receipt_timeout`].
pub const DEFAULT_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);

impl<R, P> PostBatchSubmitter<R, P>
where
    R: ProofPlanResolver<EvmProtocol>,
    P: Provider<Ethereum>,
{
    /// Build a submitter. `poster_address` must match the wallet the
    /// `provider` was constructed with — the submitter sets it as the
    /// `from` field on the `TransactionRequest` so alloy's
    /// `WalletFiller` finds the matching signer.
    pub fn new(
        provider: P,
        resolver: R,
        proof_signer: EcdsaProofSigner,
        eez_address: Address,
        poster_address: Address,
        chain_id: u64,
    ) -> Self {
        Self {
            provider,
            resolver,
            proof_signer,
            eez_address,
            poster_address,
            chain_id,
            receipt_timeout: DEFAULT_RECEIPT_TIMEOUT,
        }
    }

    /// Override the default receipt-wait timeout. Returns the
    /// submitter for fluent configuration.
    #[must_use]
    pub fn with_receipt_timeout(mut self, timeout: Duration) -> Self {
        self.receipt_timeout = timeout;
        self
    }

    /// Submit `batch` to the configured EEZ. See module docs for the
    /// pipeline. Returns the submitted tx hash + receipt-decoded
    /// events.
    ///
    /// # Errors
    ///
    /// See [`PostBatchError`].
    pub async fn submit(&self, mut batch: EvmBatch) -> Result<PostBatchOutcome, PostBatchError> {
        // 0. Reject non-empty blobIndices up front. Phase 09 doesn't
        //    wire the blob carrier path; a non-empty blobIndices on the
        //    input batch would silently mismatch the on-chain
        //    `blobhash(blobIndices[i])` fold against our empty
        //    `blob_hashes` slice.
        if !batch.inner.blobIndices.is_empty() {
            return Err(PostBatchError::UnsupportedBlobIndices {
                len: batch.inner.blobIndices.len(),
            });
        }

        // 1. Touched-rollup extraction.
        let touched = extract_touched_rollups(&batch)?;
        if touched.is_empty() {
            return Err(PostBatchError::EmptyBatch);
        }
        let touched_vec: Vec<RollupId> = touched.into_iter().collect();

        // 2. Proof-plan resolution.
        let plan = self
            .resolver
            .resolve(&touched_vec)
            .await
            .map_err(PostBatchError::Resolver)?;

        // Phase 09 scope guard: single PS only. Phase 10+ relaxes
        // this when the signer becomes a producer trait.
        if plan.proof_systems.len() != 1 {
            return Err(PostBatchError::UnsupportedProofSystemCount {
                got: plan.proof_systems.len(),
            });
        }

        // 3. Populate proof carriers on the batch from the plan.
        populate_proof_carriers(&mut batch, &plan);

        // 4. Compute per-PS publicInputsHash[k].
        let entry_hashes: Vec<B256> = batch.entries().iter().map(entry_hash).collect();
        let lookup_call_hashes: Vec<B256> =
            batch.lookup_calls().iter().map(lookup_call_hash).collect();
        // Phase 09: no blob carriers in the smoke payload. The
        // shared-hash construction folds an empty blob-hashes array
        // here; matches the on-chain `blobhash(blobIndices[i])` walk
        // over the empty `blobIndices`.
        let blob_hashes: Vec<B256> = Vec::new();

        let per_ps_hashes = all_per_ps_hashes(
            &plan,
            &entry_hashes,
            &lookup_call_hashes,
            &blob_hashes,
            batch.call_data(),
        )
        .map_err(PostBatchError::PlanInvariants)?;

        // 5. Sign each per-PS digest. Phase 09 single-PS — the loop
        // runs exactly once.
        let mut proofs: Vec<Bytes> = Vec::with_capacity(per_ps_hashes.len());
        for (ps_index, hash) in per_ps_hashes.iter().enumerate() {
            let sig = self
                .proof_signer
                .sign_prehash(*hash)
                .map_err(|source| PostBatchError::Sign { ps_index, source })?;
            proofs.push(sig);
        }
        batch.inner.proofs = proofs;

        // 6. Encode calldata + submit. Proofs ride inside the batch
        // struct under the multi-prover ABI; trait takes the batch
        // alone.
        let calldata = EvmProtocol.encode_postbatch(&batch);

        // Explicit pending-nonce fetch. Disables alloy's NonceFiller
        // for this submission (it skips when `nonce` is already set
        // on the request).
        let nonce = self
            .provider
            .get_transaction_count(self.poster_address)
            .pending()
            .await
            .map_err(|source| PostBatchError::Transport {
                stage: "eth_getTransactionCount(pending)",
                source,
            })?;

        let tx = TransactionRequest::default()
            .with_from(self.poster_address)
            .with_to(self.eez_address)
            .with_input(calldata)
            .with_nonce(nonce)
            .with_chain_id(self.chain_id);

        // 7. Send ONCE. No replacement / retry policy in Phase 09.
        let pending = self.provider.send_transaction(tx).await.map_err(|source| {
            PostBatchError::Transport {
                stage: "eth_sendTransaction",
                source,
            }
        })?;
        let tx_hash = *pending.tx_hash();

        // Wait for the receipt with an explicit timeout. Alloy's
        // `PendingTransactionBuilder::with_timeout` configures the
        // heartbeat's reaping loop to abort the watch after the
        // configured duration — the receipt future then resolves to
        // `PendingTransactionError::TxWatcher(...)`. We translate
        // that to a typed `ReceiptTimeout` so stuck-pending txs fail
        // loud per the §E1 spec (no replacement-tx policy in this
        // phase).
        let receipt = pending
            .with_timeout(Some(self.receipt_timeout))
            .get_receipt()
            .await
            .map_err(|source| match source {
                alloy_provider::PendingTransactionError::TxWatcher(_) => {
                    PostBatchError::ReceiptTimeout {
                        tx_hash,
                        timeout_secs: self.receipt_timeout.as_secs(),
                    }
                }
                other => PostBatchError::Provider {
                    stage: "get_receipt",
                    source: other,
                },
            })?;

        if !receipt.status() {
            return Err(PostBatchError::ReceiptStatusFailure { tx_hash });
        }

        decode_outcome(tx_hash, self.eez_address, &receipt)
    }
}

// ── Helpers ───────────────────────────────────────────────────────

/// Collect every rollup id referenced by `batch`. Walks entry
/// destinations, entry-level stateDeltas, and lookup-call
/// destinations.
///
/// Returns [`PostBatchError::RollupIdOverflow`] if any referenced
/// `uint256` rollupId doesn't fit in `u64`. The on-chain ABI is
/// `uint256` but `eez-protocol`'s `RollupId(u64)` is the
/// workspace-wide carrier; surfacing the boundary explicitly is
/// loud per `invariant 7` (silent saturation would let a malformed
/// batch sail past the resolver into a confusing registry-miss).
fn extract_touched_rollups(batch: &EvmBatch) -> Result<BTreeSet<RollupId>, PostBatchError> {
    let mut touched = BTreeSet::new();
    for entry in batch.entries() {
        // `destinationRollupId` is the routing target; always present.
        touched.insert(RollupId(u256_to_u64_checked(entry.destinationRollupId)?));
        for delta in &entry.stateDeltas {
            touched.insert(RollupId(u256_to_u64_checked(delta.rollupId)?));
        }
    }
    for lookup in batch.lookup_calls() {
        touched.insert(RollupId(u256_to_u64_checked(lookup.destinationRollupId)?));
    }
    Ok(touched)
}

/// Convert a `uint256` rollupId to `u64`, returning
/// [`PostBatchError::RollupIdOverflow`] on overflow.
fn u256_to_u64_checked(v: U256) -> Result<u64, PostBatchError> {
    v.try_into()
        .map_err(|_err: alloy_primitives::ruint::FromUintError<u64>| {
            // The error carries the same `value: U256` we already
            // record in the variant; drop it to keep PostBatchError
            // free of an alloy ruint dep in its public surface.
            PostBatchError::RollupIdOverflow { value: v }
        })
}

/// Mutate `batch.inner` to carry the resolved plan's proof-system
/// fields. Leaves `entries`, `l1ToL2lookupCalls`,
/// `transientExecutionEntryCount`, `transientLookupCallCount`,
/// `blobIndices`, and `callData` untouched — those were populated by
/// `build_batch` (and `blobIndices`/`callData` stay empty in Phase
/// 09).
fn populate_proof_carriers(batch: &mut EvmBatch, plan: &ProofPlan<EvmProtocol>) {
    batch.inner.proofSystems = plan.proof_systems.clone();
    batch.inner.rollupIdsWithProofSystems = plan
        .rollup_assignments
        .iter()
        .map(|a| RollupIdWithProofSystemsSol {
            rollupId: U256::from(a.rollup_id.0),
            proofSystemIndex: a.proof_system_index.clone(),
        })
        .collect();
    batch.inner.crossProofSystemInteractions = B256::from(plan.cross_proof_system_interactions);
}

/// Parse the receipt logs into a [`PostBatchOutcome`]. Filters logs
/// to those emitted by `eez_address` — an unrelated contract called
/// during the tx could otherwise emit a colliding `BatchPosted` /
/// `L2ExecutionPerformed` selector and spoof the outcome. Requires
/// exactly one `BatchPosted` log; collects every
/// `L2ExecutionPerformed` in receipt order.
fn decode_outcome(
    tx_hash: B256,
    eez_address: Address,
    receipt: &TransactionReceipt,
) -> Result<PostBatchOutcome, PostBatchError> {
    decode_outcome_from_logs(tx_hash, eez_address, receipt.inner.logs())
}

/// Inner helper — exposes the log-slice surface so unit tests can
/// drive the receipt decoder without constructing a full
/// [`TransactionReceipt`].
fn decode_outcome_from_logs(
    tx_hash: B256,
    eez_address: Address,
    logs: &[Log],
) -> Result<PostBatchOutcome, PostBatchError> {
    let batch_posted_sig = BatchPosted::SIGNATURE_HASH;
    let l2_exec_sig = L2ExecutionPerformed::SIGNATURE_HASH;

    let mut batch_posted: Option<U256> = None;
    let mut batch_posted_count = 0usize;
    let mut l2_executions: Vec<L2ExecutionLog> = Vec::new();

    for log in logs {
        // EEZ is the sole authority for these event topics; reject
        // logs from any other contract that happens to emit the
        // colliding selector.
        if log.address() != eez_address {
            continue;
        }
        let Some(topic0) = log.topic0() else {
            continue;
        };
        if *topic0 == batch_posted_sig {
            batch_posted_count += 1;
            let decoded = BatchPosted::decode_log_data(&log.inner.data).map_err(|source| {
                PostBatchError::EventDecode {
                    event: "BatchPosted",
                    source,
                }
            })?;
            batch_posted = Some(decoded.rollupCount);
        } else if *topic0 == l2_exec_sig {
            let decoded =
                L2ExecutionPerformed::decode_log_data(&log.inner.data).map_err(|source| {
                    PostBatchError::EventDecode {
                        event: "L2ExecutionPerformed",
                        source,
                    }
                })?;
            l2_executions.push(L2ExecutionLog {
                rollup_id: decoded.rollupId,
                new_state: decoded.newState,
            });
        }
    }

    if batch_posted_count > 1 {
        return Err(PostBatchError::DuplicateBatchPostedEvent {
            tx_hash,
            count: batch_posted_count,
        });
    }
    let rollup_count = batch_posted.ok_or(PostBatchError::MissingBatchPostedEvent { tx_hash })?;

    Ok(PostBatchOutcome {
        tx_hash,
        rollup_count,
        l2_executions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::I256;
    use eez_evm::types::{
        ExecutionEntrySol, LookupCallSol, ProofSystemBatchPerVerificationEntriesSol, StateDeltaSol,
    };
    use eez_protocol::{RollupProofAssignment, TimestampAndBlockHash};

    fn entry_with(dest: u64, delta_rids: &[u64]) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: delta_rids
                .iter()
                .map(|rid| StateDeltaSol {
                    rollupId: U256::from(*rid),
                    currentState: B256::ZERO,
                    newState: B256::ZERO,
                    etherDelta: I256::ZERO,
                })
                .collect(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(dest),
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        }
    }

    fn lookup_with(dest: u64) -> LookupCallSol {
        LookupCallSol {
            crossChainCallHash: B256::ZERO,
            destinationRollupId: U256::from(dest),
            returnData: Bytes::new(),
            failed: false,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            rollingHash: B256::ZERO,
            expectedStateRoots: Vec::new(),
        }
    }

    fn make_batch(entries: Vec<ExecutionEntrySol>, lookups: Vec<LookupCallSol>) -> EvmBatch {
        EvmBatch {
            inner: ProofSystemBatchPerVerificationEntriesSol {
                entries,
                l1ToL2lookupCalls: lookups,
                transientExecutionEntryCount: U256::ZERO,
                transientLookupCallCount: U256::ZERO,
                proofSystems: Vec::new(),
                rollupIdsWithProofSystems: Vec::new(),
                crossProofSystemInteractions: B256::ZERO,
                blobIndices: Vec::new(),
                callData: Bytes::new(),
                proofs: Vec::new(),
                blockNumber: 0,
            },
        }
    }

    #[test]
    fn extract_dedups_across_entries_deltas_and_lookups() {
        let batch = make_batch(
            vec![entry_with(1, &[1, 2]), entry_with(3, &[2, 4])],
            vec![lookup_with(1), lookup_with(5)],
        );
        let touched = extract_touched_rollups(&batch).expect("u64 range");
        let ids: Vec<u64> = touched.iter().map(|r| r.0).collect();
        // BTreeSet ⇒ ascending order, dedup'd.
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn extract_empty_batch_returns_empty_set() {
        let batch = make_batch(Vec::new(), Vec::new());
        assert!(
            extract_touched_rollups(&batch)
                .expect("empty is ok")
                .is_empty()
        );
    }

    #[test]
    fn u256_overflow_is_loud_typed_error() {
        let huge = U256::MAX;
        let err = u256_to_u64_checked(huge).unwrap_err();
        match err {
            PostBatchError::RollupIdOverflow { value } => assert_eq!(value, huge),
            other => panic!("expected RollupIdOverflow, got {other:?}"),
        }
        // Normal range round-trips cleanly.
        assert_eq!(u256_to_u64_checked(U256::from(42u64)).unwrap(), 42);
    }

    #[test]
    fn extract_propagates_overflow_for_oversized_rollup_id() {
        // U256 value just outside u64 range — must surface as
        // RollupIdOverflow rather than silently saturate or panic.
        let oversized = U256::from(u64::MAX) + U256::from(1u64);
        let mut entry = entry_with(1, &[]);
        entry.destinationRollupId = oversized;
        let batch = make_batch(vec![entry], Vec::new());
        let err = extract_touched_rollups(&batch).unwrap_err();
        assert!(matches!(
            err,
            PostBatchError::RollupIdOverflow { value } if value == oversized
        ));
    }

    #[test]
    fn populate_carriers_copies_plan_into_batch() {
        let mut batch = make_batch(vec![entry_with(1, &[1])], Vec::new());
        let ps_addr = Address::from_slice(&[0u8; 20]);
        // populate_proof_carriers does NOT validate — that's the
        // resolver's job. We just check the field-by-field copy.
        let plan = ProofPlan::<EvmProtocol> {
            proof_systems: vec![ps_addr],
            rollup_assignments: vec![RollupProofAssignment {
                rollup_id: RollupId(1),
                proof_system_index: vec![0],
            }],
            per_rollup_context: vec![TimestampAndBlockHash {
                timestamp: [0u8; 32],
                block_hash: [0u8; 32],
            }],
            vk_matrix: vec![vec![[0u8; 32]]],
            cross_proof_system_interactions: [0u8; 32],
        };
        populate_proof_carriers(&mut batch, &plan);
        assert_eq!(batch.inner.proofSystems, vec![ps_addr]);
        assert_eq!(batch.inner.rollupIdsWithProofSystems.len(), 1);
        assert_eq!(
            batch.inner.rollupIdsWithProofSystems[0].rollupId,
            U256::from(1u64)
        );
        assert_eq!(
            batch.inner.rollupIdsWithProofSystems[0].proofSystemIndex,
            vec![0u64]
        );
        assert_eq!(batch.inner.crossProofSystemInteractions, B256::ZERO);
    }

    // ── Selector lock (regression test for §F1 first-run bug) ───────

    /// Locks the `sol!`-derived `SIGNATURE_HASH` constants against
    /// the on-chain topic0 values observed in a real EEZ receipt
    /// (and verified via `cast keccak`). The §F1 first-run bug was
    /// renaming the events to `*Event` to avoid a Rust-side
    /// collision, which silently shifted the derived hash to
    /// `keccak256("BatchPostedEvent(uint256)")` etc. — so receipt
    /// parsing produced `MissingBatchPostedEvent` against perfectly
    /// valid on-chain logs. Lock byte-equality here so a future
    /// rename can't reintroduce the same drift without failing
    /// loudly in `cargo test`.
    #[test]
    fn event_signature_hashes_match_on_chain_topic0() {
        // From `cast keccak "BatchPosted(uint256)"`.
        const BATCH_POSTED_TOPIC0: B256 = B256::new([
            0xd6, 0xf8, 0xd7, 0x1c, 0xe4, 0x2a, 0x79, 0x9b, 0x91, 0xf3, 0x99, 0x27, 0x1f, 0x4b,
            0x0e, 0x91, 0xf8, 0x5e, 0xb8, 0x7f, 0xac, 0x7b, 0xb2, 0xce, 0xdd, 0x4b, 0x3a, 0x52,
            0xfa, 0xd3, 0x61, 0x82,
        ]);
        // From `cast keccak "L2ExecutionPerformed(uint256,bytes32)"`.
        const L2_EXEC_TOPIC0: B256 = B256::new([
            0x01, 0x33, 0xf6, 0x62, 0xc2, 0x9e, 0x67, 0xee, 0xdf, 0xc9, 0xb5, 0x3c, 0x0c, 0x1f,
            0x65, 0x7b, 0x30, 0xeb, 0xaf, 0x97, 0x48, 0x09, 0x4d, 0x09, 0xfa, 0x46, 0x59, 0xd7,
            0x69, 0xdd, 0x4f, 0x78,
        ]);
        assert_eq!(BatchPosted::SIGNATURE_HASH, BATCH_POSTED_TOPIC0);
        assert_eq!(L2ExecutionPerformed::SIGNATURE_HASH, L2_EXEC_TOPIC0);
    }

    // ── Receipt-decode tests ─────────────────────────────────────────

    /// Address constants for the receipt-decode tests. `EEZ_ADDR` is
    /// the authoritative emitter; `ROGUE_ADDR` represents an
    /// unrelated contract called during the same tx that happens to
    /// emit the colliding selector.
    const EEZ_ADDR: Address = Address::new([0x11u8; 20]);
    const ROGUE_ADDR: Address = Address::new([0x99u8; 20]);

    fn batch_posted_log(emitter: Address, rollup_count: U256) -> Log {
        // `BatchPosted(uint256 indexed rollupCount)` — rollupCount in
        // topics[1], no data. Topic encoding: 32-byte big-endian uint.
        let topics = vec![BatchPosted::SIGNATURE_HASH, B256::from(rollup_count)];
        let inner = alloy_primitives::Log::new_unchecked(emitter, topics, Bytes::new());
        Log {
            inner,
            ..Default::default()
        }
    }

    fn l2_exec_log(emitter: Address, rollup_id: U256, new_state: B256) -> Log {
        // `L2ExecutionPerformed(uint256 indexed rollupId, bytes32
        // newState)` — rollupId in topics[1], newState as 32-byte
        // data.
        let topics = vec![L2ExecutionPerformed::SIGNATURE_HASH, B256::from(rollup_id)];
        let inner = alloy_primitives::Log::new_unchecked(
            emitter,
            topics,
            Bytes::from(new_state.0.to_vec()),
        );
        Log {
            inner,
            ..Default::default()
        }
    }

    #[test]
    fn decode_outcome_happy_path_single_eez_batch_posted_and_two_executions() {
        let logs = vec![
            batch_posted_log(EEZ_ADDR, U256::from(2u64)),
            l2_exec_log(EEZ_ADDR, U256::from(1u64), B256::repeat_byte(0xAA)),
            l2_exec_log(EEZ_ADDR, U256::from(2u64), B256::repeat_byte(0xBB)),
        ];
        let tx_hash = B256::repeat_byte(0x42);
        let outcome = decode_outcome_from_logs(tx_hash, EEZ_ADDR, &logs).expect("happy path");
        assert_eq!(outcome.tx_hash, tx_hash);
        assert_eq!(outcome.rollup_count, U256::from(2u64));
        assert_eq!(outcome.l2_executions.len(), 2);
        assert_eq!(outcome.l2_executions[0].rollup_id, U256::from(1u64));
        assert_eq!(outcome.l2_executions[0].new_state, B256::repeat_byte(0xAA));
        assert_eq!(outcome.l2_executions[1].rollup_id, U256::from(2u64));
        assert_eq!(outcome.l2_executions[1].new_state, B256::repeat_byte(0xBB));
    }

    #[test]
    fn decode_outcome_ignores_rogue_batch_posted_collision() {
        // A rogue contract emits a `BatchPosted(uint256)` log AND an
        // `L2ExecutionPerformed(uint256, bytes32)` log inside the
        // same tx. Both must be filtered out — otherwise the rogue
        // log would trigger DuplicateBatchPostedEvent (false alarm)
        // and inject a spoofed L2ExecutionPerformed into the
        // outcome.
        let logs = vec![
            batch_posted_log(ROGUE_ADDR, U256::from(99u64)), // spoof
            l2_exec_log(ROGUE_ADDR, U256::from(99u64), B256::repeat_byte(0xCC)), // spoof
            batch_posted_log(EEZ_ADDR, U256::from(1u64)),    // genuine
            l2_exec_log(EEZ_ADDR, U256::from(1u64), B256::repeat_byte(0xAA)), // genuine
        ];
        let tx_hash = B256::repeat_byte(0x42);
        let outcome =
            decode_outcome_from_logs(tx_hash, EEZ_ADDR, &logs).expect("rogue logs filtered out");
        assert_eq!(outcome.rollup_count, U256::from(1u64));
        assert_eq!(outcome.l2_executions.len(), 1);
        assert_eq!(outcome.l2_executions[0].new_state, B256::repeat_byte(0xAA));
    }

    #[test]
    fn decode_outcome_missing_batch_posted_is_loud() {
        // Only an L2ExecutionPerformed event — no BatchPosted.
        // Surfaces as MissingBatchPostedEvent rather than a silent
        // pass.
        let logs = vec![l2_exec_log(EEZ_ADDR, U256::from(1u64), B256::ZERO)];
        let tx_hash = B256::repeat_byte(0x42);
        let err = decode_outcome_from_logs(tx_hash, EEZ_ADDR, &logs).unwrap_err();
        assert!(matches!(
            err,
            PostBatchError::MissingBatchPostedEvent { .. }
        ));
    }

    #[test]
    fn decode_outcome_duplicate_batch_posted_from_eez_is_loud() {
        // Two BatchPosted from the genuine EEZ — should surface
        // DuplicateBatchPostedEvent. (In practice the on-chain code
        // emits exactly one; this guards against future contract
        // changes silently slipping past.)
        let logs = vec![
            batch_posted_log(EEZ_ADDR, U256::from(1u64)),
            batch_posted_log(EEZ_ADDR, U256::from(2u64)),
        ];
        let tx_hash = B256::repeat_byte(0x42);
        let err = decode_outcome_from_logs(tx_hash, EEZ_ADDR, &logs).unwrap_err();
        assert!(matches!(
            err,
            PostBatchError::DuplicateBatchPostedEvent { count: 2, .. }
        ));
    }
}
