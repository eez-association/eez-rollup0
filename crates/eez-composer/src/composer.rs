//! [`Composer`]: the umbrella that owns each rollup's cross-chain
//! produce → prove → submit path.
//!
//! Two entry points:
//!
//! - `compose_sync_slot` — called by the Sequencer on each Sync slot.
//!   Processes a bounded batch from the rollup's [`HeldPool`](crate::HeldPool),
//!   simulates eligible transactions, and returns a Sync block for optimistic
//!   L2 commit. Inbound survivors accompany `postBatch` in the L1 bundle;
//!   outbound survivors execute in the L2 Sync block and are carried in DA. A
//!   background observer records the settlement verdict for later slot-context
//!   recovery (see [`crate::optimistic`]).
//! - [`Composer::run`] — follows the shared `L1Watcher` event stream,
//!   logging confirmed vs external `BatchPosted` for this rollup. The
//!   L1-confirmed cursor + batch index are advanced by the Deriver (sole
//!   writer of [`L1CanonicalHead`](eez_l1::L1CanonicalHead)).
//!
//! Each batch is proved by the shared [`Prover`] and sent via the shared
//! [`Submitter`] bundle relay.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider as _;
use async_trait::async_trait;
use eez_driver::{
    BUILDER_GAS_LIMIT, BlockCommitterHandle, MAX_BLOCKS_PER_BATCH, ParentContext, RollupTiming,
    SyncSlotBlock, SyncSlotComposer, SyncSlotMode,
    witness::{ExecutionWitnessMode, block_witness},
};
use eez_l1::{BundleTarget, L1Event, SendOutcome, Submitter};
use eez_prover::{ActionableProverFailure, BlockWitness, Prover, ProverError, ProvingContext};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{
    BlockReader, BlockSource, StateProvider, StateProviderFactory, TransactionsProvider,
};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::held_pool::{HeldPool, HeldTx};
use crate::ingress::Direction;
use crate::local::{
    BuildError, InboundL2TargetSession, L1SlotState, L1TargetSession, build::SyncBlockState,
    build_sync_block, sync_block_pair_roots,
};
use crate::optimistic::OptimisticallyIncluded;
use crate::prover_retry::{
    actionable_held_tx, partition_retryable, prove_with_retry, validate_actionable_prover_failure,
};
use crate::rollup::RollupState;

/// Runtime config for the cross-chain execution path on Sync slots.
/// Carried inside [`CrossChainWiring`] next to the wired
/// cross-chain simulation: the keys and addresses needed to construct and sign
/// canonical L2 system transactions from composition batch entries.
///
/// Owned by `eez-node` at startup and shared via `Arc` because the
/// `PrivateKeySigner` is bigger than two-line clone-cheap.
#[derive(Clone)]
pub struct CrossChainExecCtx {
    /// Signing key for SYSTEM_ADDRESS — must match `EEZL2`'s
    /// `SYSTEM_ADDRESS` immutable. Used for `loadExecutionTable` and
    /// `executeIncomingCrossChainCall` system transactions.
    pub system_signer: alloy_signer_local::PrivateKeySigner,
    /// `EEZL2` address, where SYSTEM_ADDRESS calls both
    /// `loadExecutionTable` and `executeIncomingCrossChainCall`.
    pub eezl2_address: Address,
    /// L2 chain id for EIP-155 signing.
    pub l2_chain_id: u64,
    /// L2 system tx gas_price (legacy). 1 gwei is plenty above
    /// devnet basefee.
    pub l2_gas_price: u128,
    /// Per-tx gas limit for the load + execute system txs. Matches
    /// the reference `EXECUTE_INCOMING_GAS_LIMIT` (~2M).
    pub l2_gas_limit: u64,
    /// Alloy provider for the embedded L1 RPC. Used to sign the
    /// `postAndVerifyBatch` transaction (nonce + fee reads). Submission goes
    /// through `submitter`.
    pub l1_provider: alloy_provider::RootProvider,
    /// Shared `Submitter` handle — the single L1 submission path.
    /// `Submitter` is internally `Arc<Inner>`, so `Clone` is cheap.
    /// `compose_sync_slot` hands it `[postBatch, inbound_user_tx_1, …]` via
    /// `Submitter::send_bundle`; outbound user transactions execute in the L2
    /// Sync block instead. The Submitter owns the transport decision (atomic
    /// `eth_sendBundle` on supporting relays, ordered mempool submission on
    /// plain execution RPCs).
    pub submitter: eez_l1::Submitter,
    /// L1 EOA whose key signs the `postAndVerifyBatch` transaction. Different from
    /// `system_signer` (which is the L2 SYSTEM_ADDRESS). For dev
    /// smoke this is typically the hardhat #0 deployer key; in
    /// production this is the based-rollup composer's L1 wallet.
    pub l1_poster_signer: alloy_signer_local::PrivateKeySigner,
    /// L1 chain id for EIP-155 signing of the `postAndVerifyBatch` transaction.
    pub l1_chain_id: u64,
    /// L1 priority fee for the `postAndVerifyBatch` transaction, in wei per gas.
    /// Must exceed any held user `raw_tx`'s priority fee so that
    /// dev-reth's payload builder orders `postBatch` first in
    /// the L1 block. Default: 10 gwei (well above the smoke's
    /// `cast mktx --gas-price 2 gwei` user_tx).
    pub l1_post_batch_priority_fee: u128,
    /// Address of the rollup's on-chain proof-system contract, embedded
    /// in `batch.proofSystems[0]`; `EEZ.postAndVerifyBatch` iterates
    /// `proofSystems[]` and calls `verify` on each. Deployment registers
    /// `ECDSAProofSystem`, which requires
    /// `ECDSA.recover(publicInputsHash, proof) == signer`; the remote proof
    /// signer signs that exact hash after validating the batch.
    pub ecdsa_proof_system_address: Address,
}

impl std::fmt::Debug for CrossChainExecCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainExecCtx")
            .field("system_address", &self.system_signer.address())
            .field("eezl2_address", &self.eezl2_address)
            .field("l2_chain_id", &self.l2_chain_id)
            .field("l2_gas_price", &self.l2_gas_price)
            .field("l2_gas_limit", &self.l2_gas_limit)
            .finish_non_exhaustive()
    }
}

/// The cross-chain compose dependencies, wired together (all-or-
/// nothing) by `eez-node` startup when the embedded L1 is up.
pub struct CrossChainWiring {
    /// Rollup id of the entry chain.
    pub entry_rollup_id: eez_protocol::RollupId,
    /// All registered rollups (entry + followers). The entry is also
    /// in this map — composition orchestration uses it uniformly.
    pub rollups: HashMap<
        eez_protocol::RollupId,
        (
            Arc<dyn eez_protocol::executor::ChainClient + Send + Sync>,
            eez_protocol::TargetConfig,
        ),
    >,
    /// Runtime context for deriving and signing L2 system transactions from
    /// composition batches.
    pub exec_ctx: Arc<CrossChainExecCtx>,
    /// Concrete local clients for slot-scoped chained composition, un-erased so
    /// the drain can reach `L1SlotState` and `simulate_source_tx_on`. The same
    /// instances registered in `rollups`; they share one overlay channel.
    pub local: crate::local::LocalComposeClients,
}

/// Target sessions for one composition, keyed by the rollup they execute on.
type SlotSessions =
    HashMap<eez_protocol::RollupId, Box<dyn eez_protocol::TargetExecutionSession + Send>>;

/// A one-entry [`SlotSessions`] — every drain composition seeds exactly one
/// target chain.
fn seed_session(
    rollup_id: eez_protocol::RollupId,
    session: impl eez_protocol::TargetExecutionSession + 'static,
) -> SlotSessions {
    HashMap::from([(
        rollup_id,
        Box::new(session) as Box<dyn eez_protocol::TargetExecutionSession + Send>,
    )])
}

/// Compose one source tx over caller-owned, slot-scoped execution contexts.
///
/// The sessions come back with the composition: the caller commits their
/// effects on accept and drops them on eviction.
///
/// Third element is the probed target gas: the call runs again inside
/// `postAndVerifyBatch`, so the budget charges for it.
/// # Errors
///
/// Source-side executor failures and composition/finalize failures, classified
/// by [`sim_error_is_poison`]. A source simulation that merely reverts is not
/// an error here — it finalizes to an empty composition, classified downstream.
// Preserve the protocol crate's structured public error type.
#[allow(clippy::result_large_err)]
#[tracing::instrument(skip_all, fields(tx_len = raw_tx.len(), %entry_rollup_id))]
fn compose_crosschain(
    cc: &CrossChainWiring,
    entry_rollup_id: eez_protocol::RollupId,
    entry_client: &crate::local::LocalChainClient,
    raw_tx: &[u8],
    sessions: SlotSessions,
    source_state: &mut crate::local::build::DraftDb,
    source_env: reth_evm::EvmEnvFor<EthEvmConfig>,
) -> eez_protocol::ComposerResult<(eez_protocol::Composition, SlotSessions, u64)> {
    use eez_protocol::ChainClient as _;
    use eez_protocol::composition::Rollup;

    // Overlay snapshots are transaction-local. Clear every participating
    // client so an unbalanced stack cannot affect this composition.
    entry_client.reset_composition_state();
    for (client, _) in cc.rollups.values() {
        client.reset_composition_state();
    }

    tracing::info!(
        name: "composer.simulate.start",
        %entry_rollup_id,
        tx_len = raw_tx.len(),
        rollup_count = cc.rollups.len(),
        "starting composition pipeline"
    );

    let seeded: Vec<eez_protocol::RollupId> = sessions.keys().copied().collect();

    // Assemble registered clients and configs; the seeded sessions replace the
    // lazy ones for the rollups this direction dispatches to.
    let mut rollups: HashMap<eez_protocol::RollupId, Rollup> =
        HashMap::with_capacity(cc.rollups.len());
    for (rollup_id, (client, config)) in &cc.rollups {
        rollups.insert(
            *rollup_id,
            Rollup {
                client: Arc::clone(client),
                session: None,
                config: config.clone(),
            },
        );
    }

    let mut builder =
        eez_protocol::CompositionBuilder::new(entry_rollup_id, rollups).with_sessions(sessions);
    entry_client
        .simulate_source_tx_on(raw_tx.to_vec(), &mut builder, source_state, source_env)
        .map_err(eez_protocol::CompositionError::from)?;
    let recorded_count = builder.recorded_count();
    let target_gas = builder.recorded_gas_used();
    // Reclaimed before `finalize` consumes the builder — the accepted effects
    // live in the sessions, not in the composition.
    let sessions = builder.take_sessions();

    // A session on a rollup nobody seeded means the dispatch opened a lazy one
    // and ran UNCHAINED — off this slot's states (invariant 6/8). Entry-chain
    // sessions are legitimate: overlay re-entry opens them there.
    if let Some(unseeded) = sessions
        .keys()
        .find(|id| **id != entry_rollup_id && !seeded.contains(id))
    {
        // Unavailable ⇒ classified TRANSIENT: a wiring gap is not the tx's
        // fault, so the slot aborts and re-queues rather than evicting.
        return Err(eez_protocol::ExecutorError::from(
            eez_protocol::ExecutorErrorKind::Unavailable(format!(
                "composition dispatched to rollup {unseeded}, which has no slot session; \
                 it would execute unchained against chain state instead of this slot's state"
            )),
        )
        .into());
    }

    let composition = builder.finalize()?;

    tracing::info!(
        name: "composer.simulate.complete",
        target_count = composition.targets.len(),
        recorded = recorded_count,
        target_gas,
        "composition complete"
    );

    Ok((composition, sessions, target_gas))
}

impl std::fmt::Debug for CrossChainWiring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainWiring")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("rollups", &self.rollups.len())
            .finish()
    }
}

/// Failed settlement attempts before a held user_tx is evicted as probable
/// poison. This covers both relay drops and proof requests that still fail
/// after their retry episode. After this many failures, the transaction and
/// its nonce-dependent suffix are evicted so they cannot block the FIFO queue
/// indefinitely.
///
/// Drain-time simulations are chained per chain in canonical order
/// (`compose_crosschain` over the slot's L1 state and the Sync block under
/// construction — `docs/CHAINED-INTERSTATE-DESIGN.md`), so a co-bundled
/// prerequisite is already visible when its dependant composes. What this bound
/// backstops is the residual: L1 state that moves between compose time and the
/// bundle's inclusion block.
pub const MAX_BUNDLE_ATTEMPTS: u32 = 3;

/// Stand-in for `proofs[0]` when sizing before the prover runs (ECDSA is 65 B).
const MAX_PROOF_BYTES: usize = 128;

/// Ceiling on a postBatch's gas limit (`EEZ_MAX_POSTBATCH_GAS` overrides): the
/// EIP-7825 per-tx cap, above which no tx is valid at any block gas limit.
const DEFAULT_MAX_POSTBATCH_GAS: u64 = 16_777_216;

/// The belt prices bytes the drain cannot see (ABI framing, proof, DA shape), so
/// the drain must stop earlier or the same set requeues forever. Gap seen: 230k.
const POSTBATCH_DRAIN_MARGIN: u64 = 600_000;

/// Execution for a batch with just the leading immediate entry, calldata aside.
/// Measured 139k, 10% slack; pinned by `contracts/test/PostBatchGasPins.t.sol`.
const POSTBATCH_BASE_GAS_PIN: u64 = 160_000;

/// One more entry costs EEZ.sol hashing, a state SSTORE, and a CREATE2 proxy
/// deploy for a new sender. Same pin test; measured 334k worst case, 10% slack.
const POSTBATCH_ENTRY_GAS_PIN: u64 = 370_000;

/// Below this the drain admits nothing and evicts the first held tx as poison.
const MIN_VIABLE_POSTBATCH_GAS: u64 =
    projected_postbatch_gas(2, 0, 0).saturating_add(POSTBATCH_DRAIN_MARGIN);

/// EIP-7623 calldata floor — below it the tx is invalid and dies at simulation.
fn calldata_floor_gas(calldata: &[u8]) -> u64 {
    let nonzero = calldata.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = calldata.len() as u64 - nonzero;
    21_000 + 10 * (zero + 4 * nonzero)
}

/// Exact standard-rate calldata gas: 4 per zero byte, 16 per non-zero byte.
fn calldata_gas(bytes: &[u8]) -> u64 {
    let nonzero = bytes.iter().filter(|byte| **byte != 0).count() as u64;
    let zero = bytes.len() as u64 - nonzero;
    4 * zero + 16 * nonzero
}

/// Whole-postBatch gas on EIP-7623's standard branch: tx base, calldata, pins,
/// probed target calls. The floor branch is checked in `sign_post_batch_tx`.
const fn projected_postbatch_gas(entry_count: u64, calldata_gas: u64, target_gas: u64) -> u64 {
    21_000_u64
        .saturating_add(POSTBATCH_BASE_GAS_PIN)
        .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(entry_count))
        .saturating_add(calldata_gas)
        .saturating_add(target_gas)
}

/// What one accepted held tx adds to the postBatch projection, per term.
#[derive(Debug, Clone, Copy)]
struct TxL1Gas {
    /// `batch.entries` rows it contributes, each costing [`POSTBATCH_ENTRY_GAS_PIN`].
    entries: u64,
    /// Standard-rate gas for every byte it puts in the postBatch calldata.
    calldata_gas: u64,
    /// Probed cost of the target call EEZ.sol runs inline (outbound only).
    target_gas: u64,
}

/// L1 gas one accepted tx adds. `da_entries`/`da_tx` are its DA copies; only
/// outbound pays `target_gas` inline.
fn projected_tx_l1_gas(
    batch_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_entries: &[eez_protocol::abi::ExecutionEntrySol],
    da_tx: &[u8],
    target_gas: u64,
) -> TxL1Gas {
    use alloy_sol_types::SolValue as _;
    let encoded = |entries: &[eez_protocol::abi::ExecutionEntrySol]| -> u64 {
        entries
            .iter()
            .map(|entry| calldata_gas(&entry.abi_encode()))
            .sum()
    };
    TxL1Gas {
        entries: batch_entries.len() as u64,
        calldata_gas: encoded(batch_entries)
            .saturating_add(encoded(da_entries))
            .saturating_add(calldata_gas(da_tx)),
        target_gas,
    }
}

/// Running gas projection, so the drain stops before it builds a batch no L1
/// block can run (EIP-7825 caps one tx at ~16.7M).
#[derive(Debug, Clone, Copy)]
struct PostBatchGasBudget {
    cap: u64,
    /// Running total, seeded with the leading immediate entry every postBatch
    /// carries. The projection is linear, so costs just add.
    projected: u64,
}

impl PostBatchGasBudget {
    fn new(max_gas: u64) -> Self {
        Self {
            cap: max_gas.saturating_sub(POSTBATCH_DRAIN_MARGIN),
            projected: projected_postbatch_gas(1, 0, 0),
        }
    }

    /// Charge `cost` if it fits. `false` leaves the projection untouched, so the
    /// caller can defer this tx and stop draining.
    fn try_accept(&mut self, cost: TxL1Gas) -> bool {
        let next = self
            .projected
            .saturating_add(POSTBATCH_ENTRY_GAS_PIN.saturating_mul(cost.entries))
            .saturating_add(cost.calldata_gas)
            .saturating_add(cost.target_gas);
        if next > self.cap {
            return false;
        }
        self.projected = next;
        true
    }
}

/// What the drain can do with a pair wanting `declared` gas on a Sync block
/// that already holds `gas_used`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockGasFit {
    /// Room in this block — accept.
    Accept,
    /// No room here, but an emptier block would take it: hold for a later slot.
    Defer,
    /// Over the whole block limit, so no slot can ever take it.
    Unfittable,
}

/// Whether a pair fits the L2 Sync block. Sums DECLARED limits, which is what
/// the builder refuses on; over-estimating only defers early, never overflows.
const fn block_gas_fit(gas_used: u64, declared: u64, block_gas_limit: u64) -> BlockGasFit {
    if gas_used.saturating_add(declared) <= block_gas_limit {
        BlockGasFit::Accept
    } else if declared > block_gas_limit {
        BlockGasFit::Unfittable
    } else {
        BlockGasFit::Defer
    }
}

/// Declared gas limit of a raw tx. An undecodable tx counts as the whole block
/// so it is refused, never read as free space (`invariant 7`).
fn declared_gas_limit(raw: &Bytes) -> u64 {
    use alloy_consensus::Transaction as _;
    use alloy_eips::eip2718::Decodable2718 as _;
    reth_ethereum_primitives::TransactionSigned::decode_2718(&mut raw.as_ref())
        .map_or(u64::MAX, |tx| tx.gas_limit())
}

/// The refused pair's numbers, for the block-gas cut event.
#[derive(Debug, Clone, Copy)]
struct BlockGasCut {
    gas_used: u64,
    declared: u64,
}

/// Emission bounds, resolved once at construction. `timing` is here for its
/// `k()` — the grid the historical chunk boundary snaps to.
#[derive(Debug, Clone, Copy)]
struct EmissionLimits {
    timing: RollupTiming,
    /// Cap on a batch's block span, `cursor+1 ..= terminal`.
    max_blocks: u64,
    /// Gas limit every postBatch is signed with, and the cap the drain uses.
    max_gas: u64,
}

impl EmissionLimits {
    fn from_env(timing: RollupTiming) -> Self {
        // Absent, malformed, and zero all mean "unset" — fall back to the default.
        let read_var = |var: &str| {
            std::env::var(var)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&n| n > 0)
        };
        let k = u64::from(timing.k());
        let requested_blocks = read_var("EEZ_MAX_BLOCKS_PER_BATCH");
        let requested_gas = read_var("EEZ_MAX_POSTBATCH_GAS");
        let max_blocks = grid_aligned_cap(requested_blocks.unwrap_or(MAX_BLOCKS_PER_BATCH), k);
        let max_gas = clamp_max_postbatch_gas(requested_gas.unwrap_or(DEFAULT_MAX_POSTBATCH_GAS));
        // The one place the effective bounds are visible, adjusted or not.
        event!(
            name: "eez.composer.emission.limits",
            Level::INFO,
            max_blocks,
            max_gas,
            k,
            requested_blocks = ?requested_blocks,
            requested_gas = ?requested_gas,
            "emission bounds: at most {max_blocks} blocks and {max_gas} gas per postBatch",
        );
        Self {
            timing,
            max_blocks,
            max_gas,
        }
    }
}

/// Largest multiple of `k` not above `requested`, never below one `k`.
const fn grid_aligned_cap(requested: u64, k: u64) -> u64 {
    if k == 0 {
        return requested; // RollupTiming::validate rejects K < 2 upstream
    }
    let aligned = requested - requested % k;
    if aligned == 0 { k } else { aligned }
}

/// Whether [`Composer::recover_failed_batch`] moved the head, so the caller knows
/// whether its slot still owes a block.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryOutcome {
    /// A rollback and/or sibling commit landed — the caller's slot parent is stale.
    HeadMoved,
    /// Nothing was committed (bail, stale verdict, or nothing to roll back).
    HeadUnchanged,
}

#[derive(Debug, thiserror::Error)]
enum PreparePostBatchError {
    #[error("{0}")]
    Build(String),
    #[error("prover.prove: {0}")]
    Prover(#[source] ProverError),
    #[error("prover rejected {0}")]
    Actionable(ActionableProverFailure),
}

#[derive(Debug, Clone, Copy)]
enum SettlementFailureSource {
    Prover,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SettlementFailureOutcome {
    requeued: usize,
    evicted: usize,
}

/// Count one failed settlement episode for every candidate, evict transactions
/// that reach the bound, and cascade each eviction through its sender/direction
/// nonce suffix. Survivors retain their attempt count and FIFO order.
fn recover_settlement_failure(
    pool: &HeldPool,
    rollup_id: u64,
    txs: Vec<HeldTx>,
    source: SettlementFailureSource,
) -> SettlementFailureOutcome {
    let mut retry = Vec::with_capacity(txs.len());
    let mut evicted = Vec::new();
    let mut evicted_chains: Vec<(Address, Direction, u64)> = Vec::new();

    for mut tx in txs {
        tx.attempts = tx.attempts.saturating_add(1);
        if tx.attempts >= MAX_BUNDLE_ATTEMPTS {
            evicted_chains.push((tx.sender, tx.direction, tx.nonce));
            event!(
                name: "eez.composer.recovery.poison_evicted",
                Level::ERROR,
                rollup_id,
                source = ?source,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                direction = ?tx.direction,
                nonce = tx.nonce,
                attempts = tx.attempts,
                "potentially valid user_tx evicted after MAX_BUNDLE_ATTEMPTS failed settlement episodes to preserve chain liveness; repeated component disagreement requires investigation; resubmit required",
            );
            evicted.push(tx);
        } else {
            retry.push(tx);
        }
    }

    // Without the missing nonce, later transactions cannot execute and would
    // keep invalidating batches that include them.
    for (sender, direction, nonce) in evicted_chains {
        retry.retain(|tx| {
            let cascade =
                tx.sender == sender && tx.direction == direction && tx.nonce > nonce;
            if cascade {
                event!(
                    name: "eez.composer.recovery.nonce_chain_evicted",
                    Level::ERROR,
                    rollup_id,
                    source = ?source,
                    tx_hash = %tx.hash,
                    sender = %tx.sender,
                    direction = ?tx.direction,
                    nonce = tx.nonce,
                    gap_at = nonce,
                    "potentially valid same-sender tx depends on an evicted nonce; evicting nonce suffix for liveness (resubmit in order)",
                );
                evicted.push(tx.clone());
            }
            !cascade
        });
        for tx in pool.evict_chain_at_or_above(sender, direction, nonce) {
            event!(
                name: "eez.composer.recovery.nonce_chain_evicted",
                Level::ERROR,
                rollup_id,
                source = ?source,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                direction = ?tx.direction,
                nonce = tx.nonce,
                gap_at = nonce,
                "potentially valid pooled tx depends on an evicted nonce; evicting nonce suffix for liveness (resubmit in order)",
            );
            evicted.push(tx);
        }
    }

    let outcome = SettlementFailureOutcome {
        requeued: retry.len(),
        evicted: evicted.len(),
    };
    pool.release_in_flight_batch(&evicted);
    pool.push_front_batch(retry);
    outcome
}

impl From<String> for PreparePostBatchError {
    fn from(error: String) -> Self {
        Self::Build(error)
    }
}

impl From<&str> for PreparePostBatchError {
    fn from(error: &str) -> Self {
        Self::Build(error.to_owned())
    }
}

/// Both ends are unusable and clamp to the default: above it no tx is valid
/// (EIP-7825), and below [`MIN_VIABLE_POSTBATCH_GAS`] every emission path
/// refuses forever.
fn clamp_max_postbatch_gas(requested: u64) -> u64 {
    if (MIN_VIABLE_POSTBATCH_GAS..=DEFAULT_MAX_POSTBATCH_GAS).contains(&requested) {
        return requested;
    }
    event!(
        name: "eez.composer.emission.gas_budget_clamped",
        Level::ERROR,
        requested,
        clamped_to = DEFAULT_MAX_POSTBATCH_GAS,
        min_viable = MIN_VIABLE_POSTBATCH_GAS,
        "EEZ_MAX_POSTBATCH_GAS is out of range (must leave room for the drain margin plus one held tx, and stay within the EIP-7825 tx gas cap) — clamping to the default",
    );
    DEFAULT_MAX_POSTBATCH_GAS
}

/// Classify a [`compose_crosschain`] failure. `true` = DETERMINISTIC:
/// the composition is structurally invalid for this tx (no cross-chain
/// call / revert / bad encoding), so the tx is poison and must be
/// evicted before it can enter — and perpetually drop — a bundle.
/// `false` = TRANSIENT (chain unreachable / provider / transport /
/// missing data), which a retry may clear → re-queue, don't evict.
fn sim_error_is_poison(err: &eez_protocol::ComposerError) -> bool {
    use eez_protocol::{ComposerErrorKind, ExecutorErrorKind};
    match err.kind() {
        // Protocol failures = the composition itself is invalid
        // (EmptyCalls, broken chaining, unknown target, bad encoding) —
        // deterministic for this tx.
        ComposerErrorKind::Protocol(_) => true,
        // Executor failures: poison EXCEPT the clearly-transient ones.
        ComposerErrorKind::Executor(ee) => !matches!(
            ee.kind(),
            ExecutorErrorKind::Unavailable(_)
                | ExecutorErrorKind::Provider(_)
                | ExecutorErrorKind::Missing(_)
        ),
        // Lifecycle / internal (misconfigured, lock poisoned, double
        // register) — not the tx's fault → retry, don't evict.
        _ => false,
    }
}

alloy_sol_types::sol! {
    /// Storage getter for `EEZ.rollups[rollupId]` (auto-generated
    /// from `mapping(uint64 => RollupConfig) public rollups`).
    /// Returns the three public-getter fields in declaration order;
    /// this reader consumes `etherBalance`.
    #[sol(rpc)]
    interface IEEZReader {
        function rollups(uint64 rollupId)
            external
            view
            returns (address rollupContract, bytes32 stateRoot, uint256 etherBalance);
    }
}

/// L1-confirmed escrow (`rollups(rid).etherBalance`) an outbound withdrawal draws
/// down. `None` on any read failure, so the caller skips this early rejection;
/// the on-chain escrow check remains authoritative.
async fn read_rollup_escrow(provider: &alloy_provider::RootProvider, rid: u64) -> Option<U256> {
    let eez = std::env::var("EEZ_REGISTRY_ADDRESS")
        .ok()?
        .parse::<Address>()
        .ok()?;
    IEEZReader::new(eez, provider)
        .rollups(rid)
        .call()
        .await
        .ok()
        .map(|r| r.etherBalance)
}

/// Read canonical L1 nonces for inbound senders.
async fn inbound_source_nonces_for_drain(
    ctx: &CrossChainExecCtx,
    rollup_id: u64,
    drained: &[HeldTx],
) -> HashMap<(Address, Direction), u64> {
    let mut source_nonces = HashMap::new();
    for tx in drained {
        let key = (tx.sender, tx.direction);
        if tx.direction != Direction::Inbound || source_nonces.contains_key(&key) {
            continue;
        }
        match ctx.l1_provider.get_transaction_count(tx.sender).await {
            Ok(nonce) => {
                source_nonces.insert(key, nonce);
            }
            Err(err) => event!(
                name: "eez.composer.cc_compose.source_nonce_check_failed",
                Level::WARN,
                rollup_id,
                sender = %tx.sender,
                direction = ?tx.direction,
                error = %err,
                "canonical source-chain nonce preflight failed; proceeding with simulation",
            ),
        }
    }
    source_nonces
}

/// Read outbound nonces from the Sync block's parent state.
fn outbound_source_nonces_for_drain(
    parent_state: &dyn StateProvider,
    rollup_id: u64,
    drained: &[HeldTx],
) -> HashMap<(Address, Direction), u64> {
    let mut source_nonces = HashMap::new();
    for tx in drained {
        let key = (tx.sender, tx.direction);
        if tx.direction != Direction::Outbound || source_nonces.contains_key(&key) {
            continue;
        }
        match parent_state.account_nonce(&tx.sender) {
            Ok(nonce) => {
                source_nonces.insert(key, nonce.unwrap_or(0));
            }
            Err(err) => event!(
                name: "eez.composer.cc_compose.source_nonce_check_failed",
                Level::WARN,
                rollup_id,
                sender = %tx.sender,
                direction = ?tx.direction,
                error = %err,
                "parent-state source nonce preflight failed; proceeding with simulation",
            ),
        }
    }
    source_nonces
}

fn partition_stale(
    drained: Vec<HeldTx>,
    source_nonces: &HashMap<(Address, Direction), u64>,
) -> (Vec<HeldTx>, Vec<HeldTx>) {
    drained.into_iter().partition(|tx| {
        source_nonces
            .get(&(tx.sender, tx.direction))
            .is_none_or(|source_nonce| *source_nonce <= tx.nonce)
    })
}

fn poison_gap_for(gaps: &[(Address, Direction, u64)], tx: &HeldTx) -> Option<u64> {
    gaps.iter()
        .filter(|(sender, direction, nonce)| {
            tx.sender == *sender && tx.direction == *direction && tx.nonce > *nonce
        })
        .map(|(_, _, nonce)| *nonce)
        .min()
}

fn push_poison_root(
    poison: &mut Vec<HeldTx>,
    poison_gaps: &mut Vec<(Address, Direction, u64)>,
    tx: HeldTx,
) {
    let gap = (tx.sender, tx.direction, tx.nonce);
    if !poison_gaps.contains(&gap) {
        poison_gaps.push(gap);
    }
    poison.push(tx);
}

/// Drop the drain indices, restoring the pool's FIFO order. The drain composes
/// in two direction phases, so a re-queue must be re-sorted or it would hand
/// the pool back a permutation of what it dealt out.
fn restore_pool_order(mut txs: Vec<(usize, HeldTx)>) -> Vec<HeldTx> {
    txs.sort_by_key(|(idx, _)| *idx);
    txs.into_iter().map(|(_, tx)| tx).collect()
}

/// Everything a transient abort still owes the pool: the failing transaction
/// (absent when it was evicted as poison first), the rest of the current phase,
/// and the whole untouched other phase.
fn abort_rest(
    failing: Option<(usize, HeldTx)>,
    rest_of_phase: &mut impl Iterator<Item = (usize, HeldTx)>,
    other_phase: Vec<(usize, HeldTx)>,
) -> Vec<(usize, HeldTx)> {
    let mut rest: Vec<(usize, HeldTx)> = failing.into_iter().collect();
    rest.extend(rest_of_phase);
    rest.extend(other_phase);
    rest
}

/// Back to the pool front in FIFO order, no attempt counted: nothing failed.
/// One above a nonce evicted this drain is dropped; it can never land.
fn requeue_unprocessed(
    pool: &crate::HeldPool,
    rollup_id: u64,
    poison_gaps: &[(Address, Direction, u64)],
    txs: Vec<(usize, HeldTx)>,
) {
    let mut requeue = restore_pool_order(txs);
    let mut cascade_evicted = Vec::new();
    requeue.retain(|tx| {
        let Some(gap_at) = poison_gap_for(poison_gaps, tx) else {
            return true;
        };
        event!(
            name: "eez.composer.cc_compose.poison_chain_evicted",
            Level::WARN,
            rollup_id,
            tx_hash = %tx.hash,
            sender = %tx.sender,
            nonce = tx.nonce,
            gap_at,
            "same-sender unprocessed tx above an evicted poison nonce; evicted instead of re-queued (resubmit in order)",
        );
        cascade_evicted.push(tx.clone());
        false
    });
    pool.release_in_flight_batch(&cascade_evicted);
    pool.push_front_batch(requeue);
}

/// Append `txs` to the Sync block under construction, stopping at the first one
/// that will not land. `Some((position, why))` means the block is half-extended
/// and must be reopened on the accepted list. `why` carries its class, so the
/// caller can tell a rejected tx from an unreachable backing store.
/// Append `txs` to the block and execute them. `Ok` carries their logs, so the
/// caller can check what a tx actually did against what it composed.
fn append_and_execute(
    prefix: &mut SyncBlockState,
    txs: &[Bytes],
) -> Result<Vec<alloy_primitives::Log>, (usize, BuildError)> {
    let mut logs = Vec::new();
    for (at, tx) in txs.iter().enumerate() {
        match prefix.execute_tx(tx) {
            Ok(outcome) if outcome.success => logs.extend(outcome.logs),
            Ok(outcome) => {
                return Err((
                    at,
                    BuildError::ExecuteTx {
                        idx: at,
                        msg: format!("reverted: {}", truncated_hex(&outcome.output)),
                    },
                ));
            }
            Err(e) => return Err((at, e)),
        }
    }
    Ok(logs)
}

/// Take the L1 session's accumulated effects for commit into the slot's state.
/// The payload shape is pinned by `L1TargetSession::checkpoint`: a boxed
/// `CacheState` and nothing else.
fn take_l1_cache(
    sessions: &mut SlotSessions,
    l1_rollup_id: eez_protocol::RollupId,
) -> Result<revm::database::CacheState, String> {
    let mut session = sessions
        .remove(&l1_rollup_id)
        .ok_or_else(|| format!("composition returned no session for L1 rollup {l1_rollup_id}"))?;
    let snapshot = session
        .checkpoint()
        .map_err(|e| format!("L1 session checkpoint: {e}"))?;
    snapshot
        .downcast::<revm::database::CacheState>()
        .map(|cache| *cache)
        .map_err(|_e| "L1 session checkpoint payload is not a CacheState".to_owned())
}

/// Hex of the first 32 bytes of a revert/return payload, for event messages.
fn truncated_hex(data: &Bytes) -> String {
    const MAX: usize = 32;
    if data.len() <= MAX {
        return alloy_primitives::hex::encode_prefixed(data);
    }
    format!(
        "{}… ({} bytes)",
        alloy_primitives::hex::encode_prefixed(&data[..MAX]),
        data.len()
    )
}

/// Composer umbrella. Cheaply [`Clone`]able (`Arc<Inner>`).
#[derive(Clone)]
pub struct Composer<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

/// Invalid composer dependency wiring rejected at construction.
#[derive(Debug, thiserror::Error)]
pub enum ComposerConfigError {
    /// The submitter observes a different L1 account than the one signing
    /// `postAndVerifyBatch`, so the Composer could misattribute its own batch.
    #[error(
        "L1 submission identity mismatch: Submitter poster {submitter_poster} does not match postBatch signer {post_batch_signer}"
    )]
    SubmissionIdentityMismatch {
        /// Account whose receipts and `BatchPosted` events the Submitter tracks.
        submitter_poster: Address,
        /// Account signing the `postAndVerifyBatch` transaction.
        post_batch_signer: Address,
    },
}

fn ensure_submission_identity(
    submitter_poster: Address,
    post_batch_signer: Address,
) -> Result<(), ComposerConfigError> {
    if submitter_poster != post_batch_signer {
        return Err(ComposerConfigError::SubmissionIdentityMismatch {
            submitter_poster,
            post_batch_signer,
        });
    }
    Ok(())
}

struct Inner<L2: BlockReader> {
    /// Per-rollup state keyed by rollup ID.
    rollups: HashMap<u64, RollupState<L2>>,
    /// Shared across rollups: one prover, one submitter.
    prover: Arc<dyn Prover>,
    submitter: Submitter,
    /// EVM config — used by [`build_sync_block`] to construct the
    /// per-Sync-slot block via reth-evm `BlockBuilder`.
    evm_config: EthEvmConfig,
    /// Cross-chain clients and execution context, guaranteed by the
    /// `eez-composer` entrypoint.
    cross_chain: CrossChainWiring,
    /// Handle to the binary-owned `BlockCommitter` actor (the sole engine-API
    /// owner), shared with the Sequencer and Deriver. Slot-context recovery
    /// uses it to reorg an optimistically committed Sync block after L1 failure.
    committer: BlockCommitterHandle<EthEngineTypes>,
    /// Per-block witnesses for [`eez_prover::ProvingContext::blocks`]. `None`
    /// means the configured in-process prover does not require block witnesses;
    /// it does not mean that the Composer has no prover.
    witness_source: Option<Arc<dyn eez_prover::ProvingWitnessSource>>,
    /// Bounds on what one postBatch may settle. Read from env once here so
    /// the emission decision, the boundary math, and the span guard in
    /// `prepare_post_batch_raw` can never disagree about the cap mid-run.
    emission: EmissionLimits,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_ids", &self.inner.rollups.keys().collect::<Vec<_>>())
            .field("prover", &self.inner.prover)
            .field("submitter", &self.inner.submitter)
            .finish()
    }
}

impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Constructs the umbrella. Synchronous — does no I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ComposerConfigError::SubmissionIdentityMismatch`] when the
    /// cross-chain postBatch signer does not match the Submitter's poster.
    pub fn new(
        rollups: HashMap<u64, RollupState<L2>>,
        prover: Arc<dyn Prover>,
        evm_config: EthEvmConfig,
        cross_chain: CrossChainWiring,
        committer: BlockCommitterHandle<EthEngineTypes>,
        witness_source: Option<Arc<dyn eez_prover::ProvingWitnessSource>>,
        timing: RollupTiming,
    ) -> Result<Self, ComposerConfigError> {
        let submitter = cross_chain.exec_ctx.submitter.clone();
        let submitter_poster = submitter.poster_address();
        let post_batch_signer = cross_chain.exec_ctx.l1_poster_signer.address();
        ensure_submission_identity(submitter_poster, post_batch_signer)?;

        Ok(Self {
            inner: Arc::new(Inner {
                rollups,
                prover,
                submitter,
                evm_config,
                cross_chain,
                committer,
                witness_source,
                emission: EmissionLimits::from_env(timing),
            }),
        })
    }

    /// Run loop. Drains `l1_events` (subscribed by the caller before
    /// the `L1Watcher` starts): logs own/external
    /// `BatchPosted` attribution and drives optimistic recovery on
    /// `Reorg`/`Finalized` (the cursor and its retreats are owned by
    /// the Deriver via the shared `L1CanonicalHead`).
    ///
    /// Cross-chain Sync-slot composition is driven separately through
    /// the [`SyncSlotComposer`] trait (the Sequencer calls
    /// `compose_sync_slot` on its schedule), so this loop takes no
    /// batch-candidate input. Exits when the L1 event stream closes —
    /// the upstream `L1Watcher` task has exited.
    pub async fn run(self, mut l1_events: broadcast::Receiver<L1Event>) {
        let our_address = self.inner.submitter.poster_address();

        event!(
            name: "eez.composer.started",
            Level::INFO,
            rollup_count = self.inner.rollups.len(),
            our_address = %our_address,
            "composer umbrella loop started",
        );

        loop {
            let event = l1_events.recv().await;
            let closed = matches!(event, Err(broadcast::error::RecvError::Closed));
            self.inner.on_l1_event(&event, our_address);
            if closed {
                break;
            }
        }
    }
}

impl<L2> Inner<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Diagnostic-only handler for L1 events. State (cursor + reorg
    /// retreats) lives in the shared `L1CanonicalHead` written by the
    /// Deriver; here we just log own-vs-external batch attribution +
    /// flag the `expect_external_batches=false` violation when in
    /// sequenced mode.
    fn on_l1_event(
        &self,
        event: &Result<L1Event, broadcast::error::RecvError>,
        our_address: alloy_primitives::Address,
    ) {
        match event {
            Ok(L1Event::BatchPosted {
                l1_block_number,
                tx_hash,
                submitter,
                ..
            }) => {
                let is_ours = *submitter == our_address;
                if is_ours {
                    event!(
                        name: "eez.composer.batch.confirmed",
                        Level::INFO,
                        l1_block_number,
                        tx_hash = %tx_hash,
                        "our batch landed on L1",
                    );
                } else {
                    // Log level by whether any rollup expects external
                    // batches. Per-rollup attribution can refine once the
                    // BatchPosted event carries rollup_id.
                    let any_expects_external = self
                        .rollups
                        .values()
                        .any(|r| r.config.expect_external_batches);
                    if any_expects_external {
                        event!(
                            name: "eez.composer.batch.external",
                            Level::INFO,
                            l1_block_number,
                            tx_hash = %tx_hash,
                            submitter = %submitter,
                            "external batch landed (based mode)",
                        );
                    } else {
                        event!(
                            name: "eez.composer.batch.external.unexpected",
                            Level::ERROR,
                            l1_block_number,
                            tx_hash = %tx_hash,
                            submitter = %submitter,
                            "external batch landed in sequenced-mode rollup — someone else is sequencing our chain",
                        );
                    }
                }
            }
            Ok(L1Event::Reorg {
                common_ancestor_number,
                ..
            }) => {
                // L1 rolled out blocks. Any optimistic batch above the
                // retreated cursor lost its L1 backing — recover its
                // txs into the held pool for re-composition. The
                // Deriver independently retreats the cursor and the L2
                // head; `highest_l2_at_or_below_l1` reads the same
                // batch index it prunes, so this is order-independent.
                for (rollup_id, rollup) in &self.rollups {
                    let new_cursor = rollup
                        .l1_head
                        .highest_l2_at_or_below_l1(*common_ancestor_number)
                        .unwrap_or(0);
                    let txs = rollup.optimistic.take_rolled_out(new_cursor);
                    if txs.is_empty() {
                        continue;
                    }
                    event!(
                        name: "eez.composer.optimistic.l1_reorg_recovery",
                        Level::WARN,
                        rollup_id,
                        common_ancestor = common_ancestor_number,
                        new_cursor,
                        tx_count = txs.len(),
                        "L1 reorg rolled out optimistic batches; re-queueing their user_txs",
                    );
                    rollup.held_pool.push_front_batch(txs);
                }
            }
            Ok(L1Event::Finalized { block_number, .. }) => {
                // Settled batches at or below L1 finality can never be
                // rolled out — but "Settled" is the observer's/cursor's
                // belief. Finality audit backstop: before discarding
                // each ledger entry, verify its postBatch receipt still
                // exists on L1. A missing receipt means a reorg rolled
                // the batch out and the L1Watcher missed it. The receipt checks
                // are async while `on_l1_event` is synchronous, so the audit
                // runs in a spawned task per rollup.
                for (rollup_id, rollup) in &self.rollups {
                    let Some(fin_l2) = rollup.l1_head.highest_l2_at_or_below_l1(*block_number)
                    else {
                        continue;
                    };
                    let finalized = rollup.optimistic.take_finalized(fin_l2);
                    if finalized.is_empty() {
                        continue;
                    }
                    let rollup_id = *rollup_id;
                    let submitter = self.submitter.clone();
                    let held_pool = rollup.held_pool.clone();
                    tokio::spawn(async move {
                        for (sync_height, post_batch_hash, txs) in finalized {
                            match submitter.receipt_exists(post_batch_hash).await {
                                Ok(true) => {
                                    held_pool.release_in_flight_batch(&txs);
                                    event!(
                                        name: "eez.composer.finality_audit.ok",
                                        Level::DEBUG,
                                        rollup_id,
                                        sync_height,
                                        post_batch_hash = %post_batch_hash,
                                        "finalized batch audited: postBatch receipt present; ledger entry dropped",
                                    );
                                }
                                Ok(false) => {
                                    // Detection + tx recovery only —
                                    // repairing the L2 history is the
                                    // Deriver's job.
                                    event!(
                                        name: "eez.composer.finality_audit.rolled_out",
                                        Level::ERROR,
                                        rollup_id,
                                        sync_height,
                                        post_batch_hash = %post_batch_hash,
                                        tx_count = txs.len(),
                                        "finalized batch has NO postBatch receipt on L1 — reorg rolled it out unobserved; re-queueing its user_txs (front)",
                                    );
                                    held_pool.push_front_batch(txs);
                                }
                                Err(err) => {
                                    // Inconclusive: re-queueing txs that
                                    // actually landed would burn nonces
                                    // and poison the next bundle's simulation,
                                    // so an inconclusive audit does not recover
                                    // transactions speculatively.
                                    event!(
                                        name: "eez.composer.finality_audit.check_failed",
                                        Level::ERROR,
                                        rollup_id,
                                        sync_height,
                                        post_batch_hash = %post_batch_hash,
                                        error = %err,
                                        "finality-audit receipt lookup failed; ledger entry dropped UNAUDITED",
                                    );
                                    held_pool.release_in_flight_batch(&txs);
                                }
                            }
                        }
                    });
                }
            }
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                event!(
                    name: "eez.composer.l1_events.lagged",
                    Level::WARN,
                    skipped,
                    "L1 event stream lagged",
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                event!(
                    name: "eez.composer.l1_events.closed",
                    Level::ERROR,
                    "L1 event stream closed",
                );
            }
        }
    }
}

#[async_trait]
impl<L2> SyncSlotComposer for Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Send
        + Sync
        + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Drain `rollup_id`'s cross-chain `HeldPool`, compose its transactions,
    /// and hand the resulting Sync block back for the Sequencer to commit.
    /// Returns `None` (→ vanilla pool-driven Sync commit) when cross-chain
    /// composition is unavailable or cannot produce a block this slot.
    ///
    /// Each eligible transaction runs through cross-chain simulation. Inbound
    /// survivors execute through the L1 bundle; outbound survivors execute in
    /// the constructed L2 Sync block and are carried in DA.
    async fn compose_sync_slot(
        &self,
        rollup_id: u64,
        parent: ParentContext,
        timestamp: u64,
        target_l1_block: Option<u64>,
        mode: SyncSlotMode,
    ) -> Option<SyncSlotBlock> {
        // Some(n) → aim L1 block n exactly, pinned to this Sync block's
        // timestamp (a skipped L1 slot then drops the bundle instead of
        // settling with a drifted L2 timestamp); None → next available
        // (catch-up, unpinned).
        let bundle_target = target_l1_block.map_or(BundleTarget::NextBlock, |block| {
            BundleTarget::Exact { block, timestamp }
        });
        event!(
            name: "eez.composer.sync_slot.invoked",
            Level::INFO,
            rollup_id,
            timestamp,
            mode = ?mode,
            "compose_sync_slot invoked",
        );
        let rollup = self.inner.rollups.get(&rollup_id).or_else(|| {
            event!(
                name: "eez.composer.sync_slot.unknown_rollup",
                Level::ERROR,
                rollup_id,
                known_rollups = ?self.inner.rollups.keys().collect::<Vec<_>>(),
                "compose_sync_slot called for unknown rollup_id",
            );
            None
        })?;
        let pool = rollup.held_pool.as_ref();
        let cc = &self.inner.cross_chain;
        // Use the Sequencer-supplied parent header directly (it reflects
        // the just-committed block via `last_header`'s mirror); a
        // best-block re-lookup can race reth's provider-index and build
        // on a stale parent (see [`eez_driver::ParentContext`]).
        let parent_header = parent.header;
        let parent_number = parent_header.number();
        let suggested_fee_recipient: Address = Address::ZERO;

        // ── One-in-flight gate ───────────────────────────────────────
        // Emit a postBatch only once the previous resolves — FAILED
        // (observer verdict) or CURSOR-CONFIRMED (Deriver advanced
        // `l1_head.cursor()` past its Sync height). Two overlapping
        // bundles share the same `from` cursor; the second hits
        // `StateRootMismatch` and burns its user_txs' nonces. Gating on
        // the DERIVER's cursor (not the observer's faster verdict) also
        // keeps the next bundle's `posted` read fresh.
        //
        // While blocked the slot still gets its (empty) Sync block — L2
        // cadence is unconditional; the next postBatch covers it.
        let cursor = rollup.l1_head.cursor();
        let newly_cursor_confirmed = rollup.optimistic.resolve_below_cursor(cursor);
        if !newly_cursor_confirmed.is_empty() {
            pool.release_in_flight_batch(&newly_cursor_confirmed);
        }

        // ── Slot-context failure recovery ────────────────────────────
        // The observer task only RECORDS verdicts; the destructive
        // recovery happens here, serialized with the Sequencer's own
        // commits (the slot loop is sequential — no commit can race
        // this) and with the Deriver (reconcile lock). By now the
        // failed Sync block has either committed (head ≥ height →
        // reorg it out) or permanently didn't (stale-parent bail —
        // nothing to roll back).
        if let Some(failed) = rollup.optimistic.take_failed_for_recovery(cursor) {
            // A moved head makes this slot's parent stale — the Sequencer's bail
            // covers it. An UNCHANGED head (reinsert / stale-verdict) still owes
            // the slot a block, and returning None there would let the
            // mempool-fed commit_one fallback mint a tx-bearing grid block.
            return match self.recover_failed_batch(rollup_id, rollup, failed).await {
                RecoveryOutcome::HeadUnchanged => {
                    self.build_empty_slot_block(rollup, &parent_header, timestamp)
                }
                RecoveryOutcome::HeadMoved => None,
            };
        }

        let blocked = rollup.optimistic.blocking_height(cursor).is_some();
        let sync_height = parent_number + 1;
        self.log_settlement_backlog(rollup_id, cursor, sync_height, blocked);
        if blocked {
            event!(
                name: "eez.composer.sync_slot.bundle_in_flight",
                Level::INFO,
                rollup_id,
                cursor,
                parent_number,
                "previous postBatch unresolved; committing Sync block without emission this slot",
            );
            return self.build_empty_slot_block(rollup, &parent_header, timestamp);
        }

        // ── Bounded emission ─────────────────────────────────────────
        // A stall holds `cursor` still while the head keeps cadence, growing the
        // range by K a slot until the signer's window wedges it.
        if sync_height.saturating_sub(cursor) > self.inner.emission.max_blocks {
            if let Err(err) = self
                .emit_historical_chunk(&cc.exec_ctx, rollup_id, rollup, cursor, sync_height)
                .await
            {
                event!(
                    name: "eez.composer.emission.historical_failed",
                    Level::ERROR,
                    rollup_id,
                    cursor,
                    sync_height,
                    error = %err,
                    "historical chunk emission failed; committing Sync block without emission — same bounded range retries next slot",
                );
            }
            return self.build_empty_slot_block(rollup, &parent_header, timestamp);
        }

        // Deferred-late: the bundle missed its L1 slot, so suppress PINNED
        // emission only. Ordered after bounded emission on purpose — a
        // historical chunk targets NextBlock and carries no pin, so lateness
        // can't invalidate it; checking first would starve a chronically late
        // node (slow prover, relay latency) of settlement entirely.
        if matches!(mode, SyncSlotMode::Empty) {
            return self.build_empty_slot_block(rollup, &parent_header, timestamp);
        }

        // Catchup: structural-only — skip the drain, emit a minimal postBatch
        // (cross-chain stays pooled for the next Steady slot).
        if matches!(mode, SyncSlotMode::Catchup) {
            return self
                .dispatch_minimal_postbatch(
                    &cc.exec_ctx,
                    rollup_id,
                    rollup,
                    &parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target, // catch-up → NextBlock (unpinned)
                )
                .await
                .unwrap_or_else(|err| {
                    event!(
                        name: "eez.composer.catchup.minimal_failed",
                        Level::ERROR,
                        rollup_id,
                        error = %err,
                        "catchup minimal postBatch failed; Sequencer commits empty Sync",
                    );
                    None
                });
        }

        let pool_len_before = pool.len();
        // Caps how many held transactions one bundle attempts and therefore how
        // many an atomic-relay drop re-queues. L1 block gas remains the hard
        // protocol constraint, so deployments may need a lower cap.
        let max_user_txs = std::env::var("EEZ_MAX_USER_TXS_PER_BUNDLE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(50);
        let drained = pool.pop_n(max_user_txs);
        // When the pool is empty, do not return early. This slot attempts a
        // minimal postBatch whose leading immediate entry advances L1's stored
        // root to the empty Sync block's final state root. Without it, L1's view
        // would stop advancing while L2 continues producing blocks. The L1
        // bundle contains only `postBatch` in this path.
        let drained_count = drained.len();
        // Per-slot drain visibility (pool depth vs how many txs this bundle
        // took) — DEBUG so it doesn't spam the steady-state INFO stream.
        event!(
            name: "eez.composer.sync_slot.drain",
            Level::DEBUG,
            rollup_id,
            cursor,
            parent_number,
            pool_len_before,
            drained_count,
            "drained held pool at Sync slot",
        );

        // Process held transactions through direction-specific nonce preflight
        // and simulation. The cross-chain path computes per-effect roots and
        // stitches state updates so their final endpoint equals the locally
        // built Sync-block root.
        //
        // This path owns the composed Sync block, registers its survivors in
        // the optimistic ledger, and observes L1 settlement in the background.
        // Inbound source transactions execute through the L1 bundle; outbound
        // source transactions execute as user halves of L2 Sync pairs.
        // Boxed: the drain holds two live execution contexts, so its future is
        // past the inline-size budget.
        match Box::pin(self.compose_cross_chain_batch(
            cc,
            rollup_id,
            drained.clone(),
            &parent_header,
            timestamp,
            suggested_fee_recipient,
            bundle_target,
        ))
        .await
        {
            Ok(Some(built)) => {
                event!(
                    name: "eez.composer.sync_slot.built",
                    Level::INFO,
                    rollup_id,
                    tx_count = drained_count,
                    parent_number,
                    timestamp,
                    "built Sync block carrying {{tx_count}} held tx(s)",
                );
                Some(built)
            }
            Ok(None) => None,
            Err(err) => {
                // Errors occur before drain classification, so requeueing the
                // complete original batch is safe.
                event!(
                    name: "eez.composer.cc_compose.failed",
                    Level::ERROR,
                    rollup_id,
                    error = %err,
                    "cross-chain compose failed; retaining held transactions while Sequencer commits a fallback Sync block",
                );
                pool.push_front_batch(drained);
                None
            }
        }
    }

    /// Run recovery on every tick, not just Sync slots, so a mid-slot verdict
    /// isn't held for K blocks. Idempotent: no Failed entry means no work.
    async fn recover_failed(&self, rollup_id: u64) {
        let Some(rollup) = self.inner.rollups.get(&rollup_id) else {
            return;
        };
        let cursor = rollup.l1_head.cursor();
        if let Some(failed) = rollup.optimistic.take_failed_for_recovery(cursor) {
            // Deliberately ignored: a tick owes no block, and the Sequencer
            // re-reads the head right after this hook returns.
            let _ = self.recover_failed_batch(rollup_id, rollup, failed).await;
        }
    }
}

// Helper impl block for the cross-chain compose path. Lives outside
// the trait impl so we can use generic bounds independently.
impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header>
        + StateProviderFactory
        + Send
        + Sync
        + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// How many L2 blocks L1 has yet to record. Flat is healthy; monotonic
    /// growth is what every settlement stall looks like.
    fn log_settlement_backlog(
        &self,
        rollup_id: u64,
        cursor: u64,
        sync_height: u64,
        gate_blocked: bool,
    ) {
        let backlog = sync_height.saturating_sub(cursor);
        let max_blocks = self.inner.emission.max_blocks;
        if backlog > max_blocks.saturating_mul(2) {
            event!(
                name: "eez.composer.settlement.backlog",
                Level::WARN,
                rollup_id,
                cursor,
                sync_height,
                backlog,
                gate_blocked,
                max_blocks,
                "settlement backlog past 2x the batch cap; L1 is falling behind L2",
            );
        } else {
            event!(
                name: "eez.composer.settlement.backlog",
                Level::INFO,
                rollup_id,
                cursor,
                sync_height,
                backlog,
                gate_blocked,
                "settlement backlog",
            );
        }
    }

    /// Reorg a landed failed batch out, substitute an empty sibling (the shape
    /// a deriver rebuilds from L1), re-push its txs minus burned nonces.
    async fn recover_failed_batch(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        failed: crate::optimistic::FailedBatch,
    ) -> RecoveryOutcome {
        let sync_height = failed.sync_height;
        let pool = rollup.held_pool.as_ref();
        // Set by the two operations that move the head: the rollback and the
        // sibling commit.
        let mut outcome = RecoveryOutcome::HeadUnchanged;
        let committer = &self.inner.committer;
        let mut landed = Vec::new();
        let mut receipt_error = None;
        for tx in &failed.txs {
            match self.inner.submitter.receipt_exists(tx.hash).await {
                Ok(true) => landed.push(tx.hash),
                Ok(false) => {}
                Err(err) => {
                    receipt_error = Some((tx.hash, err));
                    break;
                }
            }
        }
        if let Some((tx_hash, err)) = receipt_error {
            event!(
                name: "eez.composer.recovery.receipt_check_failed",
                Level::WARN,
                rollup_id,
                sync_height,
                tx_hash = %tx_hash,
                error = %err,
                "user receipt lookup failed; retaining failed batch for retry",
            );
            rollup.optimistic.reinsert_failed(failed);
            return RecoveryOutcome::HeadUnchanged;
        }
        // Reorg only if the failed block (or descendants) actually
        // became canonical. Rich Sync blocks carry unsettled
        // cross-chain effects; empty (minimal-path) blocks are
        // L1-independent and need no rollback.
        if !failed.txs.is_empty() {
            let _guard = committer.begin_reconcile().await;
            // Cursor re-check UNDER the lock: the Deriver may have
            // canonicalized this very batch from L1 between the
            // observer's verdict and this slot (a false-failure
            // verdict, or the bundle landing later than observed).
            // Rolling back an L1-confirmed block would start a
            // recovery-vs-deriver war — the deriver restores it, we
            // roll it back again, and every postBatch in between
            // anchors against the wrong root.
            if rollup.l1_head.cursor() >= sync_height {
                pool.release_in_flight_batch(&failed.txs);
                event!(
                    name: "eez.composer.recovery.stale_verdict",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    cursor = rollup.l1_head.cursor(),
                    "failure verdict is stale — Deriver confirmed the batch from L1; dropping recovery",
                );
                return RecoveryOutcome::HeadUnchanged;
            }
            let head = committer.last_header();
            if head.number() >= sync_height {
                if let Err(err) = committer.reorg_to(failed.parent.clone()).await {
                    event!(
                        name: "eez.composer.recovery.reorg_failed",
                        Level::ERROR,
                        rollup_id,
                        sync_height,
                        error = %err,
                        "reorg_to failed; keeping entry Failed — next slot retries",
                    );
                    rollup.optimistic.reinsert_failed(failed);
                    return RecoveryOutcome::HeadUnchanged;
                }
                outcome = RecoveryOutcome::HeadMoved;
                event!(
                    name: "eez.composer.recovery.rolled_back",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    head = head.number(),
                    "bundle failed on L1; optimistic Sync block rolled back to parent",
                );
            } else {
                event!(
                    name: "eez.composer.recovery.not_committed",
                    Level::INFO,
                    rollup_id,
                    sync_height,
                    head = head.number(),
                    "failed Sync block never became canonical; nothing to roll back",
                );
            }
            // Still under the reconcile guard, so nothing slips between the
            // reorg and the replacement. Head elsewhere → nothing to substitute.
            if committer.last_header().hash() == failed.parent.hash() {
                let timestamp = failed
                    .parent
                    .timestamp()
                    .saturating_add(self.inner.emission.timing.l2_block_time().as_secs());
                match build_sync_block(
                    rollup.l2_provider.as_ref(),
                    &self.inner.evm_config,
                    &failed.parent,
                    timestamp,
                    Address::ZERO,
                    &[],
                ) {
                    // feed_witness=true: a PRODUCED block — later historical
                    // chunks need its witness in the store.
                    Ok(built) => match committer
                        .commit_derived(built.payload, built.header, true)
                        .await
                    {
                        Ok(_) => {
                            outcome = RecoveryOutcome::HeadMoved;
                            event!(
                                name: "eez.composer.recovery.substituted",
                                Level::WARN,
                                rollup_id,
                                sync_height,
                                timestamp,
                                "failed Sync block substituted with its empty sibling",
                            );
                        }
                        Err(err) => event!(
                            name: "eez.composer.recovery.substitute_failed",
                            Level::ERROR,
                            rollup_id,
                            sync_height,
                            error = %err,
                            "empty-sibling commit failed; head stays at the parent and Catchup rebuilds",
                        ),
                    },
                    Err(err) => event!(
                        name: "eez.composer.recovery.substitute_failed",
                        Level::ERROR,
                        rollup_id,
                        sync_height,
                        error = %err,
                        "empty-sibling build failed; head stays at the parent and Catchup rebuilds",
                    ),
                }
            }
        }
        // Re-push user_txs whose nonce survived — to the FRONT of the
        // pool, ahead of anything submitted since, so user ordering is
        // preserved across retries. An included-but-reverted tx has a
        // burned nonce — re-bundling it would poison the next bundle's
        // simulation.
        {
            let mut keep: Vec<crate::HeldTx> = Vec::with_capacity(failed.txs.len());
            let mut release: Vec<crate::HeldTx> = Vec::new();
            let mut dropped = 0usize;
            // slot_skipped: the drop wasn't the txs' fault (skipped L1 slot or
            // relay transport failure) — re-queue without counting an attempt.
            let slot_skipped = failed.slot_skipped;
            for tx in failed.txs {
                if landed.contains(&tx.hash) {
                    dropped += 1;
                    release.push(tx.clone());
                    event!(
                        name: "eez.composer.recovery.nonce_burned",
                        Level::WARN,
                        rollup_id,
                        tx_hash = %tx.hash,
                        "user_tx already has an L1 receipt; not re-queueing (user must resubmit)",
                    );
                } else {
                    keep.push(tx);
                }
            }
            pool.release_in_flight_batch(&release);
            let (re_pushed, settlement_evicted) = if slot_skipped {
                let re_pushed = keep.len();
                pool.push_front_batch(keep);
                (re_pushed, 0)
            } else {
                let recovery = recover_settlement_failure(
                    pool,
                    rollup_id,
                    keep,
                    SettlementFailureSource::Relay,
                );
                (recovery.requeued, recovery.evicted)
            };
            dropped += settlement_evicted;
            if re_pushed > 0 || dropped > 0 {
                event!(
                    name: "eez.composer.recovery.re_pushed",
                    Level::INFO,
                    rollup_id,
                    sync_height,
                    re_pushed,
                    dropped,
                    "failed batch recovered; user_txs re-queued (front) for next Sync slot",
                );
            }
        }
        outcome
    }

    /// Compose the drained transactions in canonical order over the slot's
    /// chained execution contexts, build the Sync block, register optimistic
    /// state, spawn L1 submission observation, and return the block for
    /// immediate L2 commit.
    ///
    /// Cadence: `CompositionBuilder::finalize` runs once per drained tx (via
    /// the phase arms below, each seeded with the slot's live sessions);
    /// [`Self::prepare_post_batch_raw`] runs once per slot, merging every
    /// survivor's `Composition`.
    ///
    /// # Errors
    /// Returns errors only before drain classification begins.
    async fn compose_cross_chain_batch(
        &self,
        cc: &CrossChainWiring,
        rollup_id: u64,
        drained: Vec<HeldTx>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
        suggested_fee_recipient: Address,
        bundle_target: BundleTarget,
    ) -> Result<Option<SyncSlotBlock>, String> {
        let ctx = cc.exec_ctx.as_ref();
        // Read the base SYSTEM_ADDRESS nonce from parent state. The canonical
        // builder allocates outbound-load nonces before inbound-delivery nonces.
        let parent_hash = parent_header.hash();
        let rollup =
            self.inner.rollups.get(&rollup_id).ok_or_else(|| {
                format!("unknown rollup_id {rollup_id} in compose_cross_chain_batch")
            })?;
        let pool = rollup.held_pool.as_ref();
        let state = rollup
            .l2_provider
            .state_by_block_hash(parent_hash)
            .map_err(|e| format!("state_by_block_hash({parent_hash}): {e}"))?;
        let system_address = ctx.system_signer.address();
        // Base SYSTEM_ADDRESS nonce; the canonical builder advances it
        // internally (outbound loads, then inbound deliveries) post-drain.
        let nonce = state
            .account_nonce(&system_address)
            .map_err(|e| format!("account_nonce({system_address}): {e}"))?
            .unwrap_or(0);

        let stf_cfg = eez_protocol::system_tx::SystemTxContext {
            system_signer: ctx.system_signer.clone(),
            eezl2_address: ctx.eezl2_address,
            l2_chain_id: ctx.l2_chain_id,
            l2_gas_price: ctx.l2_gas_price,
            l2_gas_limit: ctx.l2_gas_limit,
            this_rollup_id: rollup_id,
        };

        // ── Optimistic chained compose (`docs/CHAINED-INTERSTATE-DESIGN.md` §4)
        // The Sync block commits to L2 immediately and a background task
        // observes the L1 bundle (see [`crate::optimistic`]). RICH when at
        // least one tx survives every build step; MINIMAL (postBatch with only
        // the leading immediate, empty Sync block) when the drain is empty, no
        // tx survives, or a build step fails.
        // The drain runs in canonical block order — all outbound, then all
        // inbound — over two slot-scoped states: the L1 state pinned at the
        // anchor and the Sync block under construction. Each tx simulates on a
        // FORK of both and, on accept, appends its canonical txs to the block
        // and commits its L1 effects to the state, so the next composition sees
        // its predecessors exactly as sequential execution will. Per tx: a
        // deterministic failure (sim, entry shape, reverting append) is POISON —
        // evict it plus its nonce cascade, rebuild the prefix, keep composing;
        // a transient failure aborts the slot — re-queue everything and degrade
        // to minimal.
        if drained.is_empty() {
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                )
                .await;
        }

        let outbound_source_nonces =
            outbound_source_nonces_for_drain(state.as_ref(), rollup_id, &drained);
        drop(state);
        let mut source_nonces = inbound_source_nonces_for_drain(ctx, rollup_id, &drained).await;
        source_nonces.extend(outbound_source_nonces);
        let (drained, stale) = partition_stale(drained, &source_nonces);
        for tx in &stale {
            event!(
                name: "eez.composer.cc_compose.stale_nonce_evicted",
                Level::WARN,
                rollup_id,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                nonce = tx.nonce,
                direction = ?tx.direction,
                "held tx nonce is below its canonical source-chain nonce; evicting stale tx",
            );
        }
        pool.release_in_flight_batch(&stale);

        // ── Slot execution contexts (design §2) ──────────────────────
        // One L1 state pinned at the anchor and one live Sync-block prefix,
        // both advanced only by accepted transactions. Failing to open either
        // is transient: nothing has been consumed, so the whole drain goes
        // back to the pool untouched.
        let local = &cc.local;
        let l2_dyn: Arc<dyn StateProviderFactory> = rollup.l2_provider.clone();
        // Rebuilding from the accepted list — not restoring a cache — is what
        // keeps the prefix provably equal to the block the canonical rebuild
        // produces.
        let reopen = |txs: &[Bytes]| {
            SyncBlockState::open(
                l2_dyn.clone(),
                &self.inner.evm_config,
                parent_header,
                timestamp,
                suggested_fee_recipient,
                txs,
            )
        };
        let slot_ctx = L1SlotState::open(&local.l1_entry)
            .map_err(|e| format!("L1SlotState::open: {e}"))
            .and_then(|state| {
                reopen(&[])
                    .map(|draft| (state, draft))
                    .map_err(|e| format!("SyncBlockState::open: {e}"))
            });
        let (mut l1_state, mut draft) = match slot_ctx {
            Ok(contexts) => contexts,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.slot_setup_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    drained = drained.len(),
                    "slot execution contexts unavailable; re-queueing the whole drain, degrading to minimal postBatch",
                );
                pool.push_front_batch(drained);
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
        };
        event!(
            name: "eez.composer.phase2.slot_anchored",
            Level::INFO,
            rollup_id,
            l1_anchor = l1_state.anchor.number(),
            l1_anchor_hash = %l1_state.anchor.hash(),
            drained = drained.len(),
            "L1 state pinned and Sync block prefix opened for this slot",
        );

        // The Sync block's txs in canonical order, exactly as accepted: the
        // list the prefix is rebuilt from and the keystone assert compares the
        // canonical rebuild against.
        let mut sync_txs: Vec<Bytes> = Vec::new();
        // SYSTEM_ADDRESS cursor: one nonce per appended system tx. Phase 1 runs
        // to completion first, so this counts outbound loads N..N+K-1 and then
        // inbound deliveries from N+K — `build_cross_chain_sync_pairs`' split,
        // by construction.
        let mut system_txs_appended: u64 = 0;

        // Drain indices ride along so a re-queue can restore pool order across
        // the two direction phases.
        let mut survivors: Vec<(usize, HeldTx)> = Vec::with_capacity(drained.len());
        // Inbound survivors' compositions (their `source.batch` = the L1
        // deferred entries) feed `prepare_post_batch_raw`'s merge.
        // Keep each inbound composition attached to its HeldTx. The same
        // compositions feed batch assembly and actionable entry resolution.
        let mut survivor_comps: Vec<(eez_protocol::Composition, B256)> =
            Vec::with_capacity(drained.len());
        // Staged for the post-drain canonical rebuild, which must reproduce
        // `sync_txs` byte-for-byte. `pending_out` pairs an outbound settlement
        // entry with its user tx; `pending_in` holds inbound target-side
        // derivation entries; and `outbound_entries` holds settlement entries
        // spliced into `postBatch`.
        let mut pending_out: Vec<(eez_protocol::abi::ExecutionEntrySol, Bytes)> = Vec::new();
        let mut pending_in: Vec<eez_protocol::abi::ExecutionEntrySol> = Vec::new();
        let mut outbound_entries: Vec<eez_protocol::abi::ExecutionEntrySol> = Vec::new();
        // Probed gas of the accepted outbound target calls; they re-execute
        // inside postAndVerifyBatch, so its gas limit must cover them.
        let mut outbound_target_gas: u64 = 0;
        // Escrow drawn down per outbound withdrawal (read once, lazily) so several
        // in one slot can't collectively over-drain. `None` = not yet read.
        let mut escrow_remaining: Option<U256> = None;
        let mut poison: Vec<HeldTx> = Vec::new();
        let mut poison_gaps: Vec<(Address, Direction, u64)> = Vec::new();
        // On a transient failure we abort the slot; this holds the error
        // string + the txs still needing re-queue (the failing tx + the
        // unprocessed remainder of both phases; survivors are added below).
        let mut transient: Option<(String, Vec<(usize, HeldTx)>)> = None;
        // Gas, not the tx cap, is what really bounds this bundle; overflow stays
        // held for later slots.
        let mut budget = PostBatchGasBudget::new(self.inner.emission.max_gas);
        // Set by a budget cut: the tx that did not fit plus everything after it,
        // owed back to the pool unpenalized.
        let mut deferred: Vec<(usize, HeldTx)> = Vec::new();
        // Cost of the tx that tripped the budget, for the cut event.
        let mut rejected_cost: Option<u64> = None;
        // Set instead when it was the L2 block's gas, not the postBatch's, that
        // ran out. The two are mutually exclusive: the first refusal ends the drain.
        let mut block_gas_cut: Option<BlockGasCut> = None;

        // Canonical block order is all outbound pairs then all inbound
        // deliveries, and L1 splits the same way physically (postBatch
        // immediates run before the bundle's user txs), so the drain composes
        // in two phases. A sender's nonce chain is never reordered: the two
        // directions live on different chains, and poison-gap bookkeeping is
        // keyed per (sender, direction).
        let (outbounds, mut inbounds): (Vec<(usize, HeldTx)>, Vec<(usize, HeldTx)>) = drained
            .into_iter()
            .enumerate()
            .partition(|(_, held)| held.direction == Direction::Outbound);

        // ── PHASE 1 — OUTBOUND (L2→L1) ───────────────────────────────
        // Source-sim runs on a fork of the Sync block against the L2 ENTRY
        // client (the L2 follower errors `Unavailable`); the L1 side executes
        // real `_processNCalls` frames on a fork of the state. On accept the
        // canonical `[load, user]` pair extends the block and the L1 frames
        // commit to the state.
        let mut out_iter = outbounds.into_iter();
        while let Some((idx, held)) = out_iter.next() {
            if let Some(gap_at) = poison_gap_for(&poison_gaps, &held) {
                event!(
                    name: "eez.composer.cc_compose.poison_chain_evicted",
                    Level::WARN,
                    rollup_id,
                    tx_idx = idx,
                    tx_hash = %held.hash,
                    sender = %held.sender,
                    nonce = held.nonce,
                    gap_at,
                    "same-sender tx above an evicted poison nonce in the same drain; evicted (resubmit in order)",
                );
                poison.push(held);
                continue;
            }

            let contexts = L1TargetSession::new(&l1_state, local.l1_entry.clone())
                .map_err(|e| format!("L1TargetSession::new: {e}"))
                .and_then(|exec| {
                    draft
                        .fork()
                        .map(|fork| (exec, fork))
                        .map_err(|e| format!("SyncBlockState::fork: {e}"))
                });
            let (l1_exec, mut l2_fork) = match contexts {
                Ok(contexts) => contexts,
                Err(e) => {
                    transient = Some((
                        format!("outbound execution contexts tx#{idx}: {e}"),
                        abort_rest(
                            Some((idx, held)),
                            &mut out_iter,
                            std::mem::take(&mut inbounds),
                        ),
                    ));
                    break;
                }
            };
            let sessions = seed_session(cc.entry_rollup_id, l1_exec);
            let (state, env) = l2_fork.state_and_env();
            let env = env.clone();
            let sim = compose_crosschain(
                cc,
                eez_protocol::RollupId(rollup_id),
                &local.l2_entry,
                held.raw_tx.as_ref(),
                sessions,
                state,
                env,
            );
            match sim {
                Ok((composition, mut sessions, target_gas)) => {
                    let l1_entries: Vec<eez_protocol::abi::ExecutionEntrySol> = composition
                        .targets
                        .iter()
                        .flat_map(|t| t.batch.entries.iter().cloned())
                        .collect();
                    if l1_entries.is_empty() {
                        event!(
                            name: "eez.composer.cc_compose.outbound_no_entries",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            "outbound tx produced no L1 settlement entry; evicting (resubmit required)",
                        );
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        continue;
                    }
                    // `check_entry_shape` covers multiple calls within one entry;
                    // this guard covers one transaction producing multiple
                    // entries. Each entry would pair with the same nonce-bearing
                    // raw transaction, causing the Sync block to include it more
                    // than once and fail replay.
                    if l1_entries.len() > 1 {
                        event!(
                            name: "eez.composer.cc_compose.outbound_multicall_unsupported",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            entries = l1_entries.len(),
                            "outbound tx made multiple cross-chain calls; evicting because one source transaction cannot back multiple entries",
                        );
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        continue;
                    }
                    // Evict a withdrawal that would exceed the rollup's L1 escrow —
                    // it would revert on-chain and drop the whole bundle.
                    // "ether out" is the amount of Ether being withdrawn in this outbound settlement entry.
                    // If missing, the entry is malformed and must be evicted.
                    // Review once reentrancy is available
                    let Some(need) = eez_protocol::entries::outbound_ether_out(&l1_entries[0])
                    else {
                        event!(name: "eez.composer.cc_compose.outbound_ether_out_missing", Level::WARN, rollup_id, tx_idx = idx, tx_hash = %held.hash, "outbound tx is missing ether out entry, likely malformed; evicting");
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        continue;
                    };
                    // Check here (cheap early eviction, before any append work);
                    // the debit happens at accept below.
                    if need > U256::ZERO {
                        if escrow_remaining.is_none() {
                            escrow_remaining =
                                read_rollup_escrow(&ctx.l1_provider, rollup_id).await;
                        }
                        if let Some(avail) = escrow_remaining
                            && need > avail
                        {
                            event!(
                                name: "eez.composer.cc_compose.outbound_over_escrow",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                need = %need,
                                escrow = %avail,
                                "outbound withdrawal exceeds L1 rollup escrow; evicting at compose time (would revert InsufficientRollupBalance on L1 — resubmit required)",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                    }
                    // `[load, user]` must fit the Sync block or `build_sync_block`
                    // hard-errors. Before the L1 budget, so a refusal costs nothing.
                    let declared = stf_cfg
                        .l2_gas_limit
                        .saturating_add(declared_gas_limit(&held.raw_tx));
                    match block_gas_fit(draft.gas_used(), declared, BUILDER_GAS_LIMIT) {
                        BlockGasFit::Accept => {}
                        BlockGasFit::Defer => {
                            block_gas_cut = Some(BlockGasCut {
                                gas_used: draft.gas_used(),
                                declared,
                            });
                            deferred = abort_rest(
                                Some((idx, held)),
                                &mut out_iter,
                                std::mem::take(&mut inbounds),
                            );
                            break;
                        }
                        BlockGasFit::Unfittable => {
                            event!(
                                name: "eez.composer.cc_compose.block_gas_unfittable",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                declared,
                                block_gas_limit = BUILDER_GAS_LIMIT,
                                "outbound [load, user] pair declares more gas than a whole L2 block; no slot can ever take it — evicting (resubmit with a lower gas limit)",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                    }
                    // Gate before accept: this entry settles inline, so over
                    // the cap the whole bundle dies silently (invariant 7).
                    let cost =
                        projected_tx_l1_gas(&l1_entries, &l1_entries, &held.raw_tx, target_gas);
                    if !budget.try_accept(cost) {
                        rejected_cost = Some(projected_postbatch_gas(
                            cost.entries,
                            cost.calldata_gas,
                            cost.target_gas,
                        ));
                        deferred = abort_rest(
                            Some((idx, held)),
                            &mut out_iter,
                            std::mem::take(&mut inbounds),
                        );
                        break;
                    }
                    // ── ACCEPT ───────────────────────────────────────
                    // Block first, state second: a pair evicted at append must
                    // leave the L1 state untouched. `build_outbound_pair` runs
                    // the shape gate itself, so an entry the Sync-block lowering
                    // cannot represent comes back as its `Err`.
                    let pairs_k = match eez_protocol::system_tx::build_outbound_pair(
                        &l1_entries[0],
                        &held.raw_tx,
                        &stf_cfg,
                        nonce + system_txs_appended,
                    ) {
                        Ok(pairs) => pairs,
                        Err(e) => {
                            event!(
                                name: "eez.composer.cc_compose.shape_evicted",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                error = %e,
                                "outbound entry shape is not representable in a Sync block; evicting (resubmit required)",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                    };
                    let pair_txs = eez_protocol::system_tx::interleave_sync_block_txs(&pairs_k);
                    let pair_logs = match append_and_execute(&mut draft, &pair_txs) {
                        Ok(logs) => logs,
                        Err((at, why)) => {
                            // A backing-store failure says nothing about the tx —
                            // abort the slot instead of evicting a valid pair.
                            if why.is_provider() {
                                transient = Some((
                                    format!("outbound append tx#{idx} at {at}: {why}"),
                                    abort_rest(
                                        Some((idx, held)),
                                        &mut out_iter,
                                        std::mem::take(&mut inbounds),
                                    ),
                                ));
                                break;
                            }
                            event!(
                                name: "eez.composer.cc_compose.append_reverted",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                appended_idx = at,
                                error = %why,
                                "outbound [load, user] pair does not execute on the Sync block prefix; evicting the tx and rebuilding the prefix",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            // A failed append may be half-applied; the accepted
                            // list is the only truth to rebuild from.
                            draft = match reopen(&sync_txs) {
                                Ok(rebuilt) => rebuilt,
                                Err(e) => {
                                    transient = Some((
                                        format!("Sync block prefix rebuild after tx#{idx}: {e}"),
                                        abort_rest(
                                            None,
                                            &mut out_iter,
                                            std::mem::take(&mut inbounds),
                                        ),
                                    ));
                                    break;
                                }
                            };
                            continue;
                        }
                    };
                    // The entry was composed from a sim that ran WITHOUT the
                    // preceding load tx, so re-check it against what the user tx
                    // actually did here (`docs/issues/outbound-sim-divergence.md`).
                    let observed = eez_protocol::outbound_gate::observations_from_logs(
                        &pair_logs,
                        stf_cfg.eezl2_address,
                    );
                    if let Err(e) = eez_protocol::outbound_gate::verify_outbound_authorized(
                        &l1_entries,
                        &observed,
                        rollup_id,
                    ) {
                        event!(
                            name: "eez.composer.cc_compose.outbound_unobserved",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            error = %e,
                            "composed outbound entry does not match the call the tx made on the Sync block; evicting",
                        );
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        draft = match reopen(&sync_txs) {
                            Ok(rebuilt) => rebuilt,
                            Err(e) => {
                                transient = Some((
                                    format!("Sync block prefix rebuild after tx#{idx}: {e}"),
                                    abort_rest(None, &mut out_iter, std::mem::take(&mut inbounds)),
                                ));
                                break;
                            }
                        };
                        continue;
                    }
                    sync_txs.extend(pair_txs);
                    system_txs_appended += pairs_k.len() as u64;
                    // Debit at accept, not at the check above: a tx evicted in
                    // between never draws the escrow down on L1, and a budget
                    // reduced for it would evict later legitimate withdrawals.
                    if let Some(avail) = escrow_remaining {
                        escrow_remaining = Some(avail.saturating_sub(need));
                    }

                    // The state advances only now, behind the accepted block.
                    match take_l1_cache(&mut sessions, cc.entry_rollup_id) {
                        Ok(cache) => l1_state.cache = cache,
                        Err(e) => {
                            event!(
                                name: "eez.composer.cc_compose.l1_session_lost",
                                Level::ERROR,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                error = %e,
                                "the L1 execution session did not come back from the composition; the slot's L1 state can no longer chain — degrading",
                            );
                            transient = Some((
                                format!("L1 session hand-off tx#{idx}: {e}"),
                                abort_rest(
                                    Some((idx, held)),
                                    &mut out_iter,
                                    std::mem::take(&mut inbounds),
                                ),
                            ));
                            break;
                        }
                    }

                    pending_out.push((l1_entries[0].clone(), held.raw_tx.clone()));
                    outbound_entries.extend(l1_entries);
                    outbound_target_gas = outbound_target_gas.saturating_add(target_gas);
                    // NOT pushed to `survivor_comps`: its `source.batch` is OUR
                    // L2's entries (a dest=MAINNET call that must not settle on
                    // L1). The L1 settlement is `outbound_entries`, spliced
                    // separately with dest=rid.
                    survivors.push((idx, held));
                }
                Err(e) if sim_error_is_poison(&e) => {
                    event!(
                        name: "eez.composer.cc_compose.outbound_poison",
                        Level::WARN,
                        rollup_id,
                        tx_idx = idx,
                        tx_hash = %held.hash,
                        error = %e,
                        "outbound tx fails simulation deterministically; evicting",
                    );
                    push_poison_root(&mut poison, &mut poison_gaps, held);
                }
                Err(e) => {
                    transient = Some((
                        format!("compose_crosschain outbound tx#{idx}: {e}"),
                        abort_rest(
                            Some((idx, held)),
                            &mut out_iter,
                            std::mem::take(&mut inbounds),
                        ),
                    ));
                    break;
                }
            }
        }

        // ── PHASE 2 — INBOUND (L1→L2) ────────────────────────────────
        // Source-sim runs the L1 user tx on a fork of the state; the L2 side
        // probes the canonical delivery on a fork of the block and reads the
        // claim off the real `EEZL2 → proxy` frame. On accept the delivery
        // extends the block and the source fork's writes become the state.
        // A phase-1 abort already collected these txs into the re-queue set.
        let inbounds = if transient.is_some() {
            Vec::new()
        } else {
            inbounds
        };
        let mut in_iter = inbounds.into_iter();
        while let Some((idx, held)) = in_iter.next() {
            if let Some(gap_at) = poison_gap_for(&poison_gaps, &held) {
                event!(
                    name: "eez.composer.cc_compose.poison_chain_evicted",
                    Level::WARN,
                    rollup_id,
                    tx_idx = idx,
                    tx_hash = %held.hash,
                    sender = %held.sender,
                    nonce = held.nonce,
                    gap_at,
                    "same-sender tx above an evicted poison nonce in the same drain; evicted (resubmit in order)",
                );
                poison.push(held);
                continue;
            }

            let contexts = draft
                .fork()
                .map_err(|e| format!("SyncBlockState::fork: {e}"))
                .map(|fork| {
                    InboundL2TargetSession::new(fork, stf_cfg.clone(), nonce + system_txs_appended)
                })
                .and_then(|probe| {
                    l1_state
                        .fork_state(&local.l1_entry)
                        .map(|source| (probe, source))
                        .map_err(|e| format!("L1SlotState::fork_state: {e}"))
                });
            let (probe, (mut l1_fork, l1_env)) = match contexts {
                Ok(contexts) => contexts,
                Err(e) => {
                    transient = Some((
                        format!("inbound execution contexts tx#{idx}: {e}"),
                        abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                    ));
                    break;
                }
            };
            let sessions = seed_session(eez_protocol::RollupId(rollup_id), probe);
            let sim = compose_crosschain(
                cc,
                cc.entry_rollup_id,
                &local.l1_entry,
                held.raw_tx.as_ref(),
                sessions,
                &mut l1_fork,
                l1_env,
            );
            match sim {
                Ok((composition, _sessions, _probe_gas)) => {
                    let target_entries: Vec<eez_protocol::abi::ExecutionEntrySol> = composition
                        .targets
                        .iter()
                        .flat_map(|t| t.batch.entries.iter().cloned())
                        .collect();
                    // Shape gate at accept, both halves: an entry the delivery
                    // lowering cannot represent, or a nested recording on the
                    // source side, is this tx's problem, not the slot's.
                    let shape = target_entries
                        .iter()
                        .try_for_each(|entry| {
                            eez_protocol::system_tx::check_entry_shape(entry, "inbound")
                        })
                        .and_then(|()| {
                            match composition
                                .source
                                .batch
                                .entries
                                .iter()
                                .find(|entry| !entry.expectedL1ToL2Calls.is_empty())
                            {
                                Some(nested) => Err(format!(
                                    "inbound source entry records {} nested L1→L2 call(s); nested composition is parked",
                                    nested.expectedL1ToL2Calls.len(),
                                )),
                                None => Ok(()),
                            }
                        });
                    if let Err(e) = shape {
                        event!(
                            name: "eez.composer.cc_compose.shape_evicted",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            error = %e,
                            "inbound composition uses an unsupported entry shape; evicting (resubmit required)",
                        );
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        continue;
                    }

                    // Same gate: one delivery tx per entry, each at `l2_gas_limit`.
                    // Foreign entries never ship, so this over-counts at worst.
                    let declared = stf_cfg
                        .l2_gas_limit
                        .saturating_mul(target_entries.len() as u64);
                    match block_gas_fit(draft.gas_used(), declared, BUILDER_GAS_LIMIT) {
                        BlockGasFit::Accept => {}
                        BlockGasFit::Defer => {
                            block_gas_cut = Some(BlockGasCut {
                                gas_used: draft.gas_used(),
                                declared,
                            });
                            deferred = abort_rest(Some((idx, held)), &mut in_iter, Vec::new());
                            break;
                        }
                        BlockGasFit::Unfittable => {
                            event!(
                                name: "eez.composer.cc_compose.block_gas_unfittable",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                declared,
                                deliveries = target_entries.len(),
                                block_gas_limit = BUILDER_GAS_LIMIT,
                                "inbound deliveries declare more gas than a whole L2 block; no slot can ever take them — evicting (resubmit as separate calls)",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                    }
                    // Same gate, no target gas: a deferred entry only queues
                    // here, and its L1 half is a separate bundled tx.
                    let cost = projected_tx_l1_gas(
                        &composition.source.batch.entries,
                        &target_entries,
                        &[],
                        0,
                    );
                    if !budget.try_accept(cost) {
                        rejected_cost = Some(projected_postbatch_gas(
                            cost.entries,
                            cost.calldata_gas,
                            cost.target_gas,
                        ));
                        deferred = abort_rest(Some((idx, held)), &mut in_iter, Vec::new());
                        break;
                    }
                    // ── ACCEPT ───────────────────────────────────────
                    let deliveries = match eez_protocol::system_tx::build_inbound_system_txs(
                        &target_entries,
                        &stf_cfg,
                        nonce + system_txs_appended,
                    ) {
                        Ok(deliveries) if deliveries.is_empty() && !target_entries.is_empty() => {
                            event!(
                                name: "eez.composer.cc_compose.shape_evicted",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                entries = target_entries.len(),
                                "every inbound target entry was skipped as foreign, which cannot happen for own-rollup targets; evicting",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                        Ok(deliveries) => deliveries,
                        Err(e) => {
                            event!(
                                name: "eez.composer.cc_compose.shape_evicted",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                error = %e,
                                "build_inbound_system_txs rejected the entries; evicting (resubmit required)",
                            );
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        }
                    };
                    // The delivery re-runs the on-chain claim compare, so this
                    // append IS the verifier: a revert means the claims and the
                    // block disagree and the tx must go.
                    if let Err((at, why)) = append_and_execute(&mut draft, &deliveries) {
                        // A backing-store failure says nothing about the tx —
                        // abort the slot instead of evicting a valid delivery.
                        if why.is_provider() {
                            transient = Some((
                                format!("inbound append tx#{idx} at {at}: {why}"),
                                abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                            ));
                            break;
                        }
                        event!(
                            name: "eez.composer.cc_compose.append_reverted",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            appended_idx = at,
                            error = %why,
                            "inbound delivery does not execute on the Sync block prefix; evicting the tx and rebuilding the prefix",
                        );
                        push_poison_root(&mut poison, &mut poison_gaps, held);
                        draft = match reopen(&sync_txs) {
                            Ok(rebuilt) => rebuilt,
                            Err(e) => {
                                transient = Some((
                                    format!("Sync block prefix rebuild after tx#{idx}: {e}"),
                                    abort_rest(None, &mut in_iter, Vec::new()),
                                ));
                                break;
                            }
                        };
                        continue;
                    }
                    system_txs_appended += deliveries.len() as u64;
                    sync_txs.extend(deliveries);
                    // The source fork carries the L1 user tx's committed writes;
                    // later inbound sims must observe them (design §4 step 6).
                    l1_state.cache = l1_fork.cache;

                    let target_count = target_entries.len();
                    pending_in.extend(target_entries);
                    event!(
                        name: "eez.composer.cc_compose.tx",
                        Level::INFO,
                        rollup_id,
                        tx_idx = idx,
                        target_count,
                        "composition produced {{target_count}} target(s) for held tx #{{tx_idx}}",
                    );
                    survivor_comps.push((composition, held.hash));
                    survivors.push((idx, held));
                }
                Err(e) if sim_error_is_poison(&e) => {
                    event!(
                        name: "eez.composer.cc_compose.poison_evicted",
                        Level::WARN,
                        rollup_id,
                        tx_idx = idx,
                        tx_hash = %held.hash,
                        sender = %held.sender,
                        nonce = held.nonce,
                        error = %e,
                        "held tx fails simulation deterministically (e.g. wrong proxy → EmptyCalls, or revert); evicting — it can never compose, resubmit required",
                    );
                    push_poison_root(&mut poison, &mut poison_gaps, held);
                }
                Err(e) => {
                    // Transient (provider / transport / unavailable) —
                    // abort the slot, re-queue this tx + the remainder.
                    transient = Some((
                        format!("compose_crosschain inbound tx#{idx}: {e}"),
                        abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                    ));
                    break;
                }
            }
        }

        // ── Budget cut: settle what fits, keep the rest held. ─────────
        // Normal under load. Before the poison sweep, so an eviction here rides
        // the same nonce cascade.
        let deferred_was_empty = deferred.is_empty();
        if !deferred.is_empty() {
            let deferred_count = deferred.len();
            let accepted_count = survivors.len();
            // A block-gas cut needs gas on the block, which needs an accepted tx,
            // so the eviction branch below can only ever be an L1-budget cut.
            debug_assert!(
                accepted_count > 0 || block_gas_cut.is_none(),
                "a block-gas cut cannot happen with nothing accepted"
            );
            if accepted_count == 0 {
                // It lost against the whole budget, so no slot is roomier and
                // re-queueing would block the FIFO forever. It sits first.
                let (_, head) = deferred.remove(0);
                event!(
                    name: "eez.composer.sync_slot.gas_budget_cut",
                    Level::WARN,
                    rollup_id,
                    accepted_count,
                    deferred_count,
                    projected_gas = budget.projected,
                    rejected_cost = rejected_cost.unwrap_or(0),
                    budget = budget.cap,
                    tx_hash = %head.hash,
                    "the first held tx alone exceeds the postBatch gas budget; it can never settle — evicting it (resubmit a cheaper call)",
                );
                push_poison_root(&mut poison, &mut poison_gaps, head);
            } else if let Some(BlockGasCut { gas_used, declared }) = block_gas_cut {
                event!(
                    name: "eez.composer.sync_slot.block_gas_cut",
                    Level::INFO,
                    rollup_id,
                    accepted_count,
                    deferred_count,
                    gas_used,
                    declared,
                    block_gas_limit = BUILDER_GAS_LIMIT,
                    "Sync block gas limit reached; {{deferred_count}} held tx(s) stay queued for a later Sync slot",
                );
            } else {
                event!(
                    name: "eez.composer.sync_slot.gas_budget_cut",
                    Level::INFO,
                    rollup_id,
                    accepted_count,
                    deferred_count,
                    projected_gas = budget.projected,
                    rejected_cost = rejected_cost.unwrap_or(0),
                    budget = budget.cap,
                    "postBatch gas budget reached; {{deferred_count}} held tx(s) stay queued for a later Sync slot",
                );
            }
        }

        // Evict the poison txs' gapped higher nonces from the pool — once
        // a sender's nonce N is evicted, N+1.. can never land.
        for tx in &poison {
            // Inclusive eviction releases the poison root's reservation too.
            for t in pool.evict_chain_at_or_above(tx.sender, tx.direction, tx.nonce) {
                event!(
                    name: "eez.composer.cc_compose.poison_chain_evicted",
                    Level::WARN,
                    rollup_id,
                    tx_hash = %t.hash,
                    sender = %t.sender,
                    nonce = t.nonce,
                    gap_at = tx.nonce,
                    "same-sender tx above an evicted poison nonce; gapped chain can't land — evicted (resubmit in order)",
                );
            }
        }

        if !deferred.is_empty() {
            requeue_unprocessed(pool, rollup_id, &poison_gaps, deferred);
        }

        // ── Transient abort: re-queue survivors + remainder, minimal. ──
        // A cut empties the inbound queue, so phase 2 can't also fail here.
        // Asserted so a future edit can't double-push the survivors.
        debug_assert!(
            transient.is_none() || deferred_was_empty,
            "budget cut and transient abort are mutually exclusive"
        );
        if let Some((err, rest)) = transient {
            event!(
                name: "eez.composer.phase2.transient",
                Level::WARN,
                rollup_id,
                error = %err,
                survivors = survivors.len(),
                "transient compose failure; re-queueing and degrading to minimal postBatch this slot",
            );
            // Both phases contribute; the pool is owed its own FIFO order.
            let mut requeue = survivors;
            requeue.extend(rest);
            requeue_unprocessed(pool, rollup_id, &poison_gaps, requeue);
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                )
                .await;
        }

        // Nothing composed (all evicted or over budget). Still post a minimal
        // batch so L1 keeps tracking L2's progression.
        if survivors.is_empty() {
            let evicted = poison.len() + stale.len();
            event!(
                name: "eez.composer.phase2.all_poison",
                Level::WARN,
                rollup_id,
                evicted,
                "no held tx survived the drain (stale, deterministic failure, or over the gas budget); emitting minimal postBatch",
            );
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                )
                .await;
        }

        // Past the drain the pool's order is all that matters again.
        let survivors: Vec<HeldTx> = restore_pool_order(survivors);

        // ── Rebuild the Sync block's system txs via THE canonical builder —
        // deriver-byte-identical two-phase SYSTEM_ADDRESS nonces (outbound loads
        // N.., then inbound deliveries N+K..) + interleaved order
        // [load,user,…,deliveries]. Handles inbound / outbound / mixed
        // uniformly. A failure is systemic (signing / nonce overflow) →
        // re-queue survivors, degrade to minimal.
        let pairs = match eez_protocol::system_tx::build_cross_chain_sync_pairs(
            &pending_out,
            &pending_in,
            &stf_cfg,
            nonce,
        ) {
            Ok(p) => p,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.sync_pairs_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    "build_cross_chain_sync_pairs failed; re-queueing survivors, degrading to minimal postBatch",
                );
                pool.push_front_batch(survivors);
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
        };

        // ── KEYSTONE ─────────────────────────────────────────────────
        // The block this drain appended tx-by-tx must be exactly what the
        // canonical builder — and therefore the deriver and the proof signer —
        // reconstructs from the same entries. Inequality is a composer bug, not
        // an input condition, so it degrades the slot loudly instead of posting
        // a block nobody else can rebuild.
        let canonical = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);
        if canonical != sync_txs {
            let first_divergent = canonical
                .iter()
                .zip(&sync_txs)
                .position(|(a, b)| a != b)
                .unwrap_or(canonical.len().min(sync_txs.len()));
            event!(
                name: "eez.composer.phase2.canonical_mismatch",
                Level::ERROR,
                rollup_id,
                canonical_len = canonical.len(),
                appended_len = sync_txs.len(),
                first_divergent,
                "the incrementally appended Sync block disagrees with the canonical rebuild — a composer bug, never bad input; re-queueing survivors, degrading to minimal postBatch",
            );
            pool.push_front_batch(survivors);
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                )
                .await;
        }

        // ── Build the rich Sync block + postBatch from survivors. A ──
        // ── build / prepare failure here is systemic (not one tx) →  ──
        // ── re-queue survivors and degrade to minimal.               ──
        let built = match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &sync_txs,
        ) {
            Ok(b) => b,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.build_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    "build_sync_block failed; re-queueing survivors, degrading to minimal postBatch",
                );
                pool.push_front_batch(survivors);
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
        };
        // Belt check: every tx was receipt-verified on the very prefix this
        // block re-executes, so a failure here means the block and the prefix
        // disagree. Nothing in this class may reach the proof signer.
        if let Some(first_failed) = built.tx_successes.iter().position(|success| !success) {
            event!(
                name: "eez.composer.phase2.final_receipt_failed",
                Level::ERROR,
                rollup_id,
                tx_index = first_failed,
                tx_count = built.tx_successes.len(),
                "a Sync-block tx reverted in the final build although it succeeded when appended; re-queueing survivors, degrading to minimal postBatch",
            );
            pool.push_front_batch(survivors);
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                )
                .await;
        }
        // Per-effect intermediate L2 roots: the prover requires each entry's
        // `newState` to be its own effect's root, not the final Sync-block root.
        // Failure here is systemic (like build/prepare) → degrade.
        let pair_roots = match sync_block_pair_roots(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &sync_txs,
            ctx.system_signer.address(),
            ctx.eezl2_address,
        ) {
            Ok(r) => r,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.pair_roots_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    "sync_block_pair_roots failed; re-queueing survivors, degrading to minimal postBatch",
                );
                pool.push_front_batch(survivors);
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
        };
        let comp_refs: Vec<&eez_protocol::Composition> = survivor_comps
            .iter()
            .map(|(composition, _)| composition)
            .collect();
        // Outbound user txs (the SyncPair user halves) travel in the sync-block
        // DA slot — the deriver can't reconstruct them from the postBatch entries
        // (only the system/load txs are). Empty for inbound-only.
        let outbound_user_txs: Vec<Bytes> =
            pairs.iter().filter_map(|p| p.user_tx.clone()).collect();
        // Per-slot phase: one merge over all survivor compositions, now that
        // the built block's state root and per-effect roots exist.
        let postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &comp_refs,
                parent_header,
                built.header.state_root(),
                Some(&built.block),
                &pair_roots,
                &outbound_entries,
                &outbound_user_txs,
                outbound_target_gas,
                bundle_target,
            )
            .await
            .and_then(|raw| {
                raw.ok_or_else(|| {
                    PreparePostBatchError::Build(format!(
                        "range exceeds the {} postBatch gas budget; bounded chunks cover it",
                        self.inner.emission.max_gas
                    ))
                })
            }) {
            Ok(r) => r,
            Err(PreparePostBatchError::Actionable(failure)) => {
                let Some(poison) = actionable_held_tx(
                    failure,
                    &survivors,
                    outbound_entries.len(),
                    &survivor_comps,
                )
                .cloned() else {
                    event!(
                        name: "eez.composer.prover.actionable_unresolved",
                        Level::ERROR,
                        rollup_id,
                        failure = %failure,
                        "validated proof failure has no matching HeldTx; counting one opaque settlement failure and degrading to minimal postBatch",
                    );
                    let recovery = recover_settlement_failure(
                        pool,
                        rollup_id,
                        survivors,
                        SettlementFailureSource::Prover,
                    );
                    event!(
                        name: "eez.composer.prover.failure_recovered",
                        Level::WARN,
                        rollup_id,
                        requeued = recovery.requeued,
                        evicted = recovery.evicted,
                        "opaque prover failure recovered with bounded transaction eviction",
                    );
                    return self
                        .dispatch_minimal_postbatch(
                            ctx,
                            rollup_id,
                            rollup,
                            parent_header,
                            timestamp,
                            suggested_fee_recipient,
                            bundle_target,
                        )
                        .await;
                };

                let (retry, evicted) = partition_retryable(survivors, &poison);
                event!(
                    name: "eez.composer.prover.poison_ejected",
                    Level::ERROR,
                    rollup_id,
                    failure = %failure,
                    tx_hash = %poison.hash,
                    sender = %poison.sender,
                    direction = ?poison.direction,
                    nonce = poison.nonce,
                    retrying = retry.len(),
                    "prover attributed the rejection to one held transaction; evicting its nonce-chain suffix for liveness; a valid transaction here indicates a Composer/prover disagreement",
                );
                for in_flight in &evicted {
                    event!(
                        name: "eez.composer.prover.poison_in_flight_evicted",
                        Level::ERROR,
                        rollup_id,
                        tx_hash = %in_flight.hash,
                        sender = %in_flight.sender,
                        direction = ?in_flight.direction,
                        nonce = in_flight.nonce,
                        gap_at = poison.nonce,
                        "transaction belongs to a prover-attributed nonce-chain suffix; evicting for liveness",
                    );
                }
                for queued in
                    pool.evict_chain_at_or_above(poison.sender, poison.direction, poison.nonce)
                {
                    event!(
                        name: "eez.composer.prover.poison_queued_evicted",
                        Level::ERROR,
                        rollup_id,
                        tx_hash = %queued.hash,
                        sender = %queued.sender,
                        direction = ?queued.direction,
                        nonce = queued.nonce,
                        gap_at = poison.nonce,
                        "queued transaction depends on a prover-attributed nonce; evicting suffix for liveness",
                    );
                }

                // Each recursive attempt receives strictly fewer held
                // transactions, bounding recomposition by the original drain
                // size. Exact-slot attempts also retain the timestamp-based
                // proof cutoff.
                // The recursive call owns `retry`; retain this level's exact
                // set so a pre-classification error can requeue it locally.
                let retry_after_error = retry.clone();
                let recomposed = Box::pin(self.compose_cross_chain_batch(
                    cc,
                    rollup_id,
                    retry,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                    bundle_target,
                ))
                .await;
                return match recomposed {
                    Ok(result) => Ok(result),
                    Err(error) => {
                        event!(
                            name: "eez.composer.prover.recompose_failed",
                            Level::ERROR,
                            rollup_id,
                            error = %error,
                            retrying = retry_after_error.len(),
                            "recomposition failed before classifying its candidates; re-queueing only the remaining candidates",
                        );
                        pool.push_front_batch(retry_after_error);
                        let fallback = self
                            .dispatch_minimal_postbatch(
                                ctx,
                                rollup_id,
                                rollup,
                                parent_header,
                                timestamp,
                                suggested_fee_recipient,
                                bundle_target,
                            )
                            .await
                            .unwrap_or_else(|fallback_error| {
                                event!(
                                    name: "eez.composer.prover.recompose_fallback_failed",
                                    Level::ERROR,
                                    rollup_id,
                                    error = %fallback_error,
                                    "minimal postBatch failed after recomposition error; Sequencer commits its fallback Sync block",
                                );
                                None
                            });
                        Ok(fallback)
                    }
                };
            }
            Err(PreparePostBatchError::Prover(error)) => {
                let retryable = error.retryable_kind().is_some();
                if retryable {
                    event!(
                        name: "eez.composer.phase2.prepare_failed",
                        Level::WARN,
                        rollup_id,
                        error = %error,
                        retryable,
                        failure_class = "expected_transient",
                        "transient proof episode failed; counting one settlement failure and degrading to minimal postBatch",
                    );
                } else {
                    event!(
                        name: "eez.composer.phase2.prepare_failed",
                        Level::ERROR,
                        rollup_id,
                        error = %error,
                        retryable,
                        failure_class = "unexpected_disagreement",
                        "unexpected proof rejection; possible Composer/prover disagreement; counting one settlement failure and degrading to minimal postBatch",
                    );
                }
                let recovery = recover_settlement_failure(
                    pool,
                    rollup_id,
                    survivors,
                    SettlementFailureSource::Prover,
                );
                event!(
                    name: "eez.composer.prover.failure_recovered",
                    Level::WARN,
                    rollup_id,
                    requeued = recovery.requeued,
                    evicted = recovery.evicted,
                    "prover failure recovered with bounded transaction eviction",
                );
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
            Err(PreparePostBatchError::Build(error)) => {
                event!(
                    name: "eez.composer.phase2.prepare_failed",
                    Level::WARN,
                    rollup_id,
                    error,
                    "postBatch construction failed; re-queueing survivors without charging a settlement attempt and degrading to minimal postBatch",
                );
                pool.push_front_batch(survivors);
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                        bundle_target,
                    )
                    .await;
            }
        };
        let total_entries: usize = comp_refs.iter().map(|c| c.source.batch.entries.len()).sum();

        // ── Dispatch the rich L1 bundle and register the optimistic block. ──
        let sync_height = built.header.number();
        // keccak of the raw EIP-2718 envelope IS the typed tx's hash —
        // recorded in the ledger so the finality audit can look up the
        // postBatch receipt.
        let post_batch_hash = alloy_primitives::keccak256(&postbatch_raw);
        let mut bundle: Vec<Bytes> = Vec::with_capacity(1 + survivors.len());
        bundle.push(postbatch_raw);
        // Only INBOUND survivors ride the L1 bundle (L1-signed, execute on L1).
        // Outbound survivors are L2-signed — they run in the L2 Sync block + DA,
        // never the L1 bundle (an L2 tx is invalid on L1).
        bundle.extend(
            survivors
                .iter()
                .filter(|h| h.direction == Direction::Inbound)
                .map(|h| h.raw_tx.clone()),
        );
        event!(
            name: "eez.composer.bundle.dispatched",
            Level::INFO,
            rollup_id,
            sync_height,
            tx_count = bundle.len(),
            entry_count = total_entries,
            evicted_poison = poison.len(),
            evicted_stale = stale.len(),
            "rich bundle dispatched to background observer; committing Sync block optimistically",
        );
        rollup.optimistic.begin(
            sync_height,
            post_batch_hash,
            parent_header.clone(),
            survivors,
        );
        self.spawn_bundle_observer(
            ctx,
            rollup_id,
            sync_height,
            bundle,
            built.header.state_root(),
            Arc::clone(&rollup.optimistic),
            bundle_target,
        );
        Ok(Some(SyncSlotBlock {
            payload: built.payload,
            header: built.header,
        }))
    }

    /// Build an empty Sync block, sign a leading-immediate-only postBatch
    /// covering `posted+1..=sync_height`, dispatch it to the background
    /// observer, and return the block for immediate commit. The Sync block is
    /// the last block of the batch range.
    ///
    /// If even the minimal postBatch can't be prepared, the block is
    /// still returned (commit-without-emission) — L2 cadence is
    /// unconditional; L1 catches up via the next emitted batch.
    async fn dispatch_minimal_postbatch(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
        suggested_fee_recipient: Address,
        bundle_target: BundleTarget,
    ) -> Result<Option<SyncSlotBlock>, String> {
        let empty_built = match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &[], // no system_txs — empty Sync block
        ) {
            Ok(built) => built,
            Err(err) => {
                event!(
                    name: "eez.composer.phase1.build_failed",
                    Level::ERROR,
                    rollup_id,
                    error = %err,
                    "minimal Sync block build failed; Sequencer's commit_one fallback takes over",
                );
                return Ok(None);
            }
        };

        let minimal_postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &[], // no compositions → leading immediate only
                parent_header,
                empty_built.header.state_root(),
                Some(&empty_built.block),
                &[], // no cross-chain effects → no per-effect roots
                &[], // no outbound entries
                &[], // no outbound user txs
                0,   // no inline outbound target calls
                bundle_target,
            )
            .await
            .and_then(|raw| {
                raw.ok_or_else(|| {
                    PreparePostBatchError::Build(format!(
                        "range exceeds the {} postBatch gas budget; bounded chunks cover it",
                        self.inner.emission.max_gas
                    ))
                })
            }) {
            Ok(raw) => raw,
            Err(err) => {
                // The whole range will not fit; settle a bounded prefix of it so
                // the range shrinks and the next slot's rich batch can fit.
                let cursor = rollup.l1_head.cursor();
                let sync_height = parent_header.number() + 1;
                if let Err(chunk_err) = self
                    .emit_historical_chunk(ctx, rollup_id, rollup, cursor, sync_height)
                    .await
                {
                    event!(
                        name: "eez.composer.emission.prefix_fallback_failed",
                        Level::ERROR,
                        rollup_id,
                        cursor,
                        sync_height,
                        error = %chunk_err,
                        "neither the full range nor a bounded prefix could be emitted",
                    );
                }
                event!(
                    name: "eez.composer.phase1.prepare_failed",
                    Level::ERROR,
                    rollup_id,
                    error = %err,
                    "minimal postBatch prepare failed; committing Sync block without emission — L1 catches up next slot",
                );
                return Ok(Some(SyncSlotBlock {
                    payload: empty_built.payload,
                    header: empty_built.header,
                }));
            }
        };
        let sync_height = empty_built.header.number();
        // keccak of the raw EIP-2718 envelope IS the typed tx's hash —
        // recorded in the ledger so the finality audit can look up the
        // postBatch receipt.
        let post_batch_hash = alloy_primitives::keccak256(&minimal_postbatch_raw);
        event!(
            name: "eez.composer.phase1.bundle.dispatched",
            Level::INFO,
            rollup_id,
            sync_height,
            "minimal postBatch dispatched to background observer (leading immediate only)",
        );
        rollup.optimistic.begin(
            sync_height,
            post_batch_hash,
            parent_header.clone(),
            Vec::new(),
        );
        self.spawn_bundle_observer(
            ctx,
            rollup_id,
            sync_height,
            vec![minimal_postbatch_raw],
            empty_built.header.state_root(),
            Arc::clone(&rollup.optimistic),
            bundle_target,
        );
        Ok(Some(SyncSlotBlock {
            payload: empty_built.payload,
            header: empty_built.header,
        }))
    }

    /// The cadence block for every no-emission path (gate-blocked, deferred-
    /// late, historical chunk) — one shape, the one a deriver rebuilds from L1.
    fn build_empty_slot_block(
        &self,
        rollup: &RollupState<L2>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
    ) -> Option<SyncSlotBlock> {
        match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            Address::ZERO,
            &[],
        ) {
            Ok(built) => Some(SyncSlotBlock {
                payload: built.payload,
                header: built.header,
            }),
            Err(err) => {
                event!(
                    name: "eez.composer.sync_slot.build_failed",
                    Level::ERROR,
                    error = %err,
                    "empty Sync block build failed; Sequencer's commit_one fallback takes over",
                );
                None
            }
        }
    }

    /// A leading-immediate-only postBatch ending at a PAST on-grid empty block.
    /// `cursor` must be the emission decision's read, not a fresh one.
    async fn emit_historical_chunk(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
        sync_height: u64,
    ) -> Result<(), String> {
        let limits = self.inner.emission;
        let k = u64::from(limits.timing.k());
        // Under the cap the cap-bounded boundary is the sync height itself, which
        // is not a PAST block — step one K down so a range too big can still shrink.
        let snapped = limits
            .timing
            .historical_chunk_boundary(cursor, sync_height, limits.max_blocks)
            .unwrap_or_else(|| sync_height.saturating_sub(k));
        if snapped <= cursor {
            return Err(format!(
                "no on-grid boundary above cursor {cursor} below sync height {sync_height}"
            ));
        }
        // Candidates highest-first; each is priced exactly by
        // `prepare_post_batch_raw`, which returns `Ok(None)` when the range does
        // not fit the gas budget — then step back a whole K and re-prepare. The
        // gate precedes witness assembly and proving, so a rejected candidate
        // costs one block walk plus one encode.
        let candidates = self.empty_chunk_boundaries(rollup, cursor, snapped)?;
        let provider = rollup.l2_provider.as_ref();
        let mut over_budget = 0usize;
        for boundary in candidates {
            let boundary_header = provider
                .sealed_header(boundary)
                .map_err(|e| format!("sealed_header({boundary}): {e}"))?
                .ok_or_else(|| format!("local L2 header at boundary {boundary} missing"))?;
            let boundary_parent = provider
                .sealed_header(boundary - 1)
                .map_err(|e| format!("sealed_header({}): {e}", boundary - 1))?
                .ok_or_else(|| format!("local L2 header at {} missing", boundary - 1))?;

            let Some(raw) = self
                .prepare_post_batch_raw(
                    ctx,
                    rollup_id,
                    &[], // no compositions → leading immediate only
                    &boundary_parent,
                    boundary_header.state_root(),
                    None, // terminal is committed → witnesses come from the store
                    &[],
                    &[],
                    &[],
                    0, // no inline outbound target calls
                    BundleTarget::NextBlock,
                )
                .await
                .map_err(|error| error.to_string())?
            else {
                over_budget += 1;
                continue;
            };

            event!(
                name: "eez.composer.emission.historical_chunk",
                Level::INFO,
                rollup_id,
                cursor,
                boundary,
                sync_height,
                span = boundary - cursor,
                backlog = sync_height - cursor,
                over_budget,
                "settlement backlog exceeds the batch cap; settling a bounded historical chunk",
            );

            let post_batch_hash = alloy_primitives::keccak256(&raw);
            rollup
                .optimistic
                .begin(boundary, post_batch_hash, boundary_parent, Vec::new());
            // A past terminal can't be pinned to an L1 slot, so the bundle takes
            // whichever block the relay lands it in.
            self.spawn_bundle_observer(
                ctx,
                rollup_id,
                boundary,
                vec![raw],
                boundary_header.state_root(),
                Arc::clone(&rollup.optimistic),
                BundleTarget::NextBlock,
            );
            return Ok(());
        }
        Err(format!(
            "every empty on-grid boundary in ({cursor}, {snapped}] exceeds the {} gas budget \
             ({over_budget} candidates priced); retrying next slot",
            self.inner.emission.max_gas,
        ))
    }

    /// On-grid heights in `(cursor, snapped]` whose block is EMPTY, highest
    /// first. Empty because the prover binds every settling-block tx to a
    /// claimed entry, so a tx-bearing terminal is unattestable for an
    /// anchor-only batch (§7c). Gas is NOT considered here — the caller prices
    /// each candidate on its fully encoded batch.
    fn empty_chunk_boundaries(
        &self,
        rollup: &RollupState<L2>,
        cursor: u64,
        snapped: u64,
    ) -> Result<Vec<u64>, String> {
        let k = u64::from(self.inner.emission.timing.k());
        let provider = rollup.l2_provider.as_ref();
        // Walk parent hashes rather than numbers: `block_by_number` races reth's
        // provider index for the newest block.
        let mut empty_at: Vec<bool> = Vec::new();
        let mut hash = provider
            .sealed_header(snapped)
            .map_err(|e| format!("sealed_header({snapped}): {e}"))?
            .ok_or_else(|| format!("local L2 header at {snapped} missing"))?
            .hash();
        let mut number = snapped;
        while number > cursor {
            let header = provider
                .sealed_header_by_hash(hash)
                .map_err(|e| format!("sealed_header_by_hash({hash}, n={number}): {e}"))?
                .ok_or_else(|| format!("local L2 header {hash} (n={number}) missing"))?;
            empty_at
                .push(header.transactions_root() == alloy_consensus::constants::EMPTY_ROOT_HASH);
            hash = header.parent_hash();
            number -= 1;
        }
        let step = usize::try_from(k).map_err(|e| format!("K overflows usize: {e}"))?;
        if step == 0 {
            return Err("K is zero".to_string());
        }
        let boundaries: Vec<u64> = (0..empty_at.len())
            .step_by(step)
            .filter(|&i| empty_at[i])
            .map(|i| snapped - i as u64)
            .collect();
        if boundaries.is_empty() {
            return Err(format!(
                "no empty on-grid boundary in ({cursor}, {snapped}]; retrying next slot"
            ));
        }
        Ok(boundaries)
    }

    /// Spawn the background bundle-observer task. It owns the submission inputs
    /// and records only the verdict in the optimistic ledger; it holds neither
    /// the Composer nor the block committer, so chain recovery stays in slot
    /// context through `recover_failed_batch`.
    fn spawn_bundle_observer(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        sync_height: u64,
        bundle: Vec<Bytes>,
        expected_final_state: B256,
        optimistic: Arc<OptimisticallyIncluded>,
        target: BundleTarget,
    ) {
        let submitter = ctx.submitter.clone();
        tokio::spawn(observe_bundle_outcome(
            rollup_id,
            sync_height,
            bundle,
            expected_final_state,
            optimistic,
            submitter,
            target,
        ));
    }

    /// Build + sign the L1 `postBatch` raw tx for a Sync slot's
    /// compositions, covering blocks `posted+1..=parent+1` (intermediate
    /// blocks + the new Sync block at `parent+1`, always the range's last
    /// block). Returns only the signed EIP-2718 `postBatch` bytes; the caller
    /// appends surviving inbound user transactions to the L1 bundle.
    ///
    /// A later `postAndVerifyBatch` for the same rollup and L1 block replaces the
    /// earlier deferred queue. The composer therefore merges one Sync slot into
    /// one batch so no earlier entries are lost. Entries are ordered as the
    /// leading anchor, outbound immediate entries in drain order, then inbound
    /// deferred entries in drain order. One proof covers the merged batch.
    ///
    /// **Chained state updates**: this function
    /// stitches the merged entries so `entries[k].currentState ==
    /// entries[k-1].newState` per rollup; EEZ.sol's `_applyStateUpdates`
    /// enforces the chain on L1 (`StateRootMismatch` revert) regardless of
    /// proof system. Each effect entry's `newState` is its per-effect root from
    /// `pair_roots` (verified by the proof signer's effect-prefix checks); the
    /// last is the final Sync-block root. `sync_block_state_root` is the
    /// required settlement-chain endpoint.
    ///
    /// `sync_block` is the terminal, `Some` only while freshly built; `None`
    /// (historical chunk) takes its witness from the store like the rest.
    ///
    /// `outbound_target_gas`: probed cost of the target calls EEZ.sol runs
    /// inline, which calldata size cannot predict.
    /// # Errors
    ///
    /// Returns an error for missing chain data, inconsistent IDs or roots,
    /// malformed batch ranges, witness or proof failure, transaction signing,
    /// or RPC data required to build the submission.
    async fn prepare_post_batch_raw(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        compositions: &[&eez_protocol::Composition],
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        sync_block_state_root: B256,
        sync_block: Option<
            &reth_primitives_traits::RecoveredBlock<reth_ethereum_primitives::Block>,
        >,
        pair_roots: &[B256],
        outbound_entries: &[eez_protocol::abi::ExecutionEntrySol],
        outbound_user_txs: &[Bytes],
        outbound_target_gas: u64,
        bundle_target: BundleTarget,
    ) -> Result<Option<Bytes>, PreparePostBatchError> {
        use alloy_sol_types::SolCall;
        use eez_protocol::abi::{RollupIdWithProofSystemsSol, postAndVerifyBatchCall};

        // Merge inbound compositions' L1 source batches, or start empty so an
        // idle Sync slot can carry only the leading immediate entry that
        // advances L1's stored root.
        let mut batch = if compositions.is_empty() {
            eez_protocol::EvmBatch::default()
        } else {
            let mut b = compositions[0].source.batch.clone();
            for c in &compositions[1..] {
                b.entries.extend(c.source.batch.entries.iter().cloned());
                b.staticEntries
                    .extend(c.source.batch.staticEntries.iter().cloned());
            }
            b
        };

        // Prepend one leading immediate entry (`proxyEntryHash == 0`)
        // covering all L2 effects before the sync block — EEZ.sol drains
        // it inline during postAndVerifyBatch, applying its state update
        // against L1's recorded root.
        //
        // `currentState` = L2.stateRoot(posted) (the L1-confirmed cursor)
        // — must equal L1.config.stateRoot at postBatch time so the
        // deriver's check_claimed_state agrees. `newState` initially equals the
        // pre-Sync root so later effect entries chain from it; an anchor-only
        // batch replaces it below with the empty Sync block's final root.
        let posted = self
            .inner
            .rollups
            .get(&rollup_id)
            .ok_or_else(|| format!("unknown rollup_id {rollup_id}"))?
            .l1_head
            .cursor();
        let pre_state_root: B256 = {
            let h = self
                .inner
                .rollups
                .get(&rollup_id)
                .ok_or_else(|| format!("unknown rollup_id {rollup_id}"))?
                .l2_provider
                .sealed_header(posted)
                .map_err(|e| format!("sealed_header({posted}): {e}"))?
                .ok_or_else(|| format!("local L2 header at {posted} missing"))?;
            h.state_root()
        };
        let pre_sync_state_root = parent_header.state_root();
        let immediate_entry = eez_protocol::abi::ExecutionEntrySol {
            stateUpdates: vec![eez_protocol::abi::StateUpdateSol {
                rollupId: rollup_id,
                currentState: pre_state_root,
                newState: pre_sync_state_root,
                etherDelta: alloy_primitives::I256::ZERO,
            }],
            proxyEntryHash: B256::ZERO,
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            rollingHash: B256::ZERO,
            destinationRollupId: rollup_id,
            success: true,
            returnData: Bytes::new(),
        };
        batch.entries.insert(0, immediate_entry);

        // Splice OUTBOUND settlement entries after the leading anchor; its state
        // update is attached below. The contract drains the contiguous
        // `proxyEntryHash==0` run inline, so order must be
        // `[anchor | outbound | inbound]`. `dest=rid` is the settlement's source
        // rollup (not the call's MAINNET target); `_validateStructure` checks it.
        for (k, oe) in outbound_entries.iter().enumerate() {
            let mut entry = oe.clone();
            entry.destinationRollupId = rollup_id;
            batch.entries.insert(1 + k, entry);
        }

        // Deposit value for inbound deferred entries: the lean on-chain entry binds
        // V only in its `proxyEntryHash` preimage, so read V from the DA sidecar
        // (`targets[].batch`, same `proxyEntryHash`). Value-free → absent → 0.
        let inbound_ether: HashMap<B256, alloy_primitives::I256> = compositions
            .iter()
            .flat_map(|c| c.targets.iter())
            .flat_map(|t| t.batch.entries.iter())
            .filter_map(|e| {
                let v = e.l2ToL1Calls.first()?.value;
                if v.is_zero() {
                    return None;
                }
                alloy_primitives::I256::try_from(v)
                    .ok()
                    .map(|d| (e.proxyEntryHash, d))
            })
            .collect();

        // Cross-chain entries arrive with EMPTY `stateUpdates`; attach one chained
        // settlement state update to each (the anchor already has its own) — else
        // `_applyStateUpdates` no-ops and the L2 root never settles. Direction by
        // `proxyEntryHash`: outbound (== 0) → `-V` (via `outbound_ether_out`; None =
        // multi-call-with-value, unsupported → reject); inbound (!= 0) → `+V` deposit.
        // Value-free → 0.
        // `newState` = effect `k`'s per-effect root `pair_roots[k]`; entries are
        // ordered `[outbound… | inbound…]`, matching the Sync block's pair-ends.
        // The prover requires this exact per-entry value. `currentState` is fixed
        // by the stitch below.
        let mut effect_k = 0usize;
        for entry in &mut batch.entries {
            // Preserve the anchor's existing state update and fill only the
            // cross-chain effect entries, which arrive empty.
            if !entry.stateUpdates.is_empty() {
                continue;
            }
            let ether_delta = if entry.proxyEntryHash == B256::ZERO {
                let v = eez_protocol::entries::outbound_ether_out(entry).ok_or_else(|| {
                    format!(
                        "outbound entry: multi-call value not supported \
                         (l2ToL1Calls={})",
                        entry.l2ToL1Calls.len(),
                    )
                })?;
                if v.is_zero() {
                    alloy_primitives::I256::ZERO
                } else {
                    -alloy_primitives::I256::try_from(v)
                        .map_err(|e| format!("outbound etherOut {v} overflows I256: {e}"))?
                }
            } else {
                inbound_ether
                    .get(&entry.proxyEntryHash)
                    .copied()
                    .unwrap_or(alloy_primitives::I256::ZERO)
            };
            let new_state = *pair_roots.get(effect_k).ok_or_else(|| {
                format!(
                    "settlement stitch: effect entry {effect_k} has no per-effect root \
                     (only {} pair-end roots — pair-end/entry misalignment)",
                    pair_roots.len(),
                )
            })?;
            entry.stateUpdates = vec![eez_protocol::abi::StateUpdateSol {
                rollupId: rollup_id,
                currentState: B256::ZERO,
                newState: new_state,
                etherDelta: ether_delta,
            }];
            effect_k += 1;
        }
        if effect_k != pair_roots.len() {
            return Err(format!(
                "settlement stitch: {effect_k} effect entries but {} per-effect roots \
                 (pair-end/entry misalignment)",
                pair_roots.len(),
            )
            .into());
        }

        // Stitch the per-rollup state-update chain: EEZ.sol `_applyStateUpdates`
        // enforces `config.stateRoot == update.currentState` then sets it to
        // `newState`, so each entry's `currentState` must chain to the prior
        // entry's `newState`. This chains `pre_sync → R_0 → … → R_last (final
        // root)`, satisfying both EEZ.sol and the prover's effect-prefix gate.
        let mut running_roots: HashMap<u64, B256> = HashMap::new();
        for entry in &mut batch.entries {
            for update in &mut entry.stateUpdates {
                if let Some(prev_new) = running_roots.get(&update.rollupId).copied() {
                    update.currentState = prev_new;
                }
                running_roots.insert(update.rollupId, update.newState);
            }
        }

        // Anchor-only batch (no effects): the immediate is the last entry, so it
        // must carry the final root. An empty Sync block still mutates state
        // (EIP-2935 / EIP-4788 system writes), so `parent.stateRoot` differs from
        // the re-executed final root and the endpoint gate would fail. With
        // effects, the last effect's root already is the final root.
        if pair_roots.is_empty()
            && let Some(last) = batch.entries.last_mut()
        {
            for update in last.stateUpdates.iter_mut().rev() {
                if update.rollupId == rollup_id {
                    update.newState = sync_block_state_root;
                    break;
                }
            }
        }

        // The validating proof path enforces that the chain ends at the Sync
        // block's final root. Debug builds also check the local stitching
        // invariant here before the proof signer independently validates it.
        debug_assert_eq!(
            batch
                .entries
                .last()
                .and_then(|entry| entry.stateUpdates.last())
                .map(|update| update.newState),
            Some(sync_block_state_root),
            "settlement chain must end at the Sync-block state root",
        );

        // The L1 rolling seed commits the finalized ordered state updates, so
        // it can only be computed after the stitch above has set every
        // `currentState` and the anchor-only path has set its final root.
        eez_protocol::entries::finalize_l1_rolling_hashes(&mut batch)
            .map_err(|error| format!("finalize L1 rolling hashes: {error}"))?;

        // The contract drains the leading contiguous `proxyEntryHash == 0` run
        // inline: one anchor plus the outbound immediate entries.
        // Inbound deferred entries (proxyEntryHash != 0) queue for
        // `executeCrossChainCall` consumption. N=0 for inbound-only → 1.
        batch.immediateEntryCount = U256::from(1 + outbound_entries.len() as u64);

        // Registry-id settlement gate: refuse a batch carrying any non-registry
        // destinationRollupId (e.g. an un-rewritten MAINNET(0) outbound entry).
        ensure_batch_registry_native(&batch, rollup_id)?;
        batch.proofSystems = vec![ctx.ecdsa_proof_system_address];
        batch.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: rollup_id,
            proofSystemIndexes: vec![0u64],
        }];
        // Encode the full L2 block range this batch covers, not just the
        // Sync block: the composer accumulates intermediate live blocks between
        // Sync slots and the deriver must replay all of them. Range:
        // from = cursor+1 (first unposted L2
        // block), to = parent+1 (the Sync block).
        //
        // Intermediate blocks [from..to-1] are walked via parent-hash
        // from `parent_header` — `block_by_hash` is reliable for
        // canonical blocks, unlike `block_by_number`, which races reth's
        // provider index for the newest block. The Sync-block DA carries
        // outbound user transactions; its system transactions are reconstructed
        // by the deriver from the postBatch entries.
        let rollup =
            self.inner.rollups.get(&rollup_id).ok_or_else(|| {
                format!("unknown rollup_id {rollup_id} in prepare_post_batch_raw")
            })?;
        // Reuse the SAME cursor read that anchored the leading
        // immediate's currentState above — a second read could race the
        // Deriver's cursor advance and desync the callData range from
        // the state-update anchor (TOCTOU).
        let from = posted + 1;
        let sync_block_number = parent_header.number() + 1;
        if sync_block_number < from {
            return Err(format!(
                "sync block {sync_block_number} <= L1-confirmed cursor {posted}; \
                 composer is behind its own posted batches"
            )
            .into());
        }
        // Invariant 7: `compose_sync_slot` bounds every range, so reaching this
        // is a decision-layer bug, not an operating condition.
        let span = sync_block_number - from + 1;
        if span > self.inner.emission.max_blocks {
            return Err(format!(
                "batch range {from}..={sync_block_number} spans {span} blocks, over \
                 MAX_BLOCKS_PER_BATCH ({}) — emission bug",
                self.inner.emission.max_blocks,
            )
            .into());
        }
        let span_len = usize::try_from(span).map_err(|e| format!("batch span overflow: {e}"))?;
        let mut blocks_rev: Vec<Vec<Vec<u8>>> = Vec::with_capacity(span_len.saturating_sub(1));
        let mut cursor_hash = parent_header.hash();
        let mut cursor_number = parent_header.number();
        while cursor_number >= from {
            // `BlockSource::Any` so the lookup finds the parent even
            // while it's still "pending" in reth: at compose_sync_slot
            // time the Sequencer has done `newPayload(parent)` but the
            // promoting FCU fires on the next commit, so the parent is in
            // reth's tree but not yet canonical-head. Deeper ancestors
            // are already canonical, so `Any` finds them too.
            let block = rollup
                .l2_provider
                .find_block_by_hash(cursor_hash, BlockSource::Any)
                .map_err(|e| {
                    format!("l2_provider.find_block_by_hash({cursor_hash}, n={cursor_number}): {e}")
                })?
                .ok_or_else(|| {
                    format!("local L2 block hash {cursor_hash} (n={cursor_number}) missing")
                })?;
            let tx_bytes: Vec<Vec<u8>> = block
                .body()
                .transactions()
                .iter()
                .map(Encodable2718::encoded_2718)
                .collect();
            // Intermediate blocks may contain only user transactions; system
            // transactions belong to the Sync block. Reject both typed and
            // SYSTEM_ADDRESS-signed forms so an unrecovered failed Sync block
            // cannot reintroduce phantom cross-chain effects.
            for enc in &tx_bytes {
                let is_system = if enc.first() == Some(&0x7E) {
                    true
                } else {
                    use alloy_eips::eip2718::Decodable2718 as _;
                    use reth_primitives_traits::SignerRecoverable as _;
                    let mut raw: &[u8] = enc.as_slice();
                    let tx = reth_ethereum_primitives::TransactionSigned::decode_2718(&mut raw)
                        .map_err(|e| {
                            format!(
                                "system-tx guard: decode_2718 failed for a tx in \
                                 intermediate block {cursor_number}: {e}"
                            )
                        })?;
                    let signer = tx.recover_signer().map_err(|e| {
                        format!(
                            "system-tx guard: recover_signer failed for a tx in \
                             intermediate block {cursor_number}: {e}"
                        )
                    })?;
                    signer == ctx.system_signer.address()
                };
                if is_system {
                    return Err(format!(
                        "intermediate block {cursor_number} carries a system transaction; \
                         emission is blocked until the failed Sync block is recovered"
                    )
                    .into());
                }
            }
            blocks_rev.push(tx_bytes);
            if cursor_number == 0 {
                break;
            }
            cursor_hash = block.header().parent_hash();
            cursor_number -= 1;
        }
        blocks_rev.reverse();
        let mut blocks = blocks_rev;
        // Outbound user txs aren't reconstructible from the entries (only the load
        // is), so they travel in the Sync-block DA here; the deriver interleaves
        // them with the rebuilt loads. Inbound-only → empty.
        blocks.push(outbound_user_txs.iter().map(|b| b.to_vec()).collect());
        // Encoded `ExecutionEntrySol` values let followers reconstruct system
        // transactions. Outbound entries describe L1 settlement and L2 loads;
        // inbound target batches carry the L2 delivery inputs. Encode both into
        // `batch.callData`, which enters the public-input preimage as opaque data.
        use alloy_sol_types::SolValue as _;
        // The DA sidecar stores the full derivation entry set in canonical
        // order: outbound settlement entries first, then inbound deferred
        // entries. This matches the deriver's prefix split.
        let l2_entries_bytes: Vec<Vec<u8>> = outbound_entries
            .iter()
            .map(eez_protocol::abi::ExecutionEntrySol::abi_encode)
            .chain(
                compositions
                    .iter()
                    .flat_map(|c| c.targets.iter())
                    .flat_map(|t| t.batch.entries.iter())
                    .map(eez_protocol::abi::ExecutionEntrySol::abi_encode),
            )
            .collect();
        let payload = eez_payload_codec::encode(&blocks, &l2_entries_bytes)
            .map_err(|e| format!("eez_payload_codec::encode: {e}"))?;
        batch.callData = alloy_primitives::Bytes::from(payload);

        // Priced on the encoded candidate before witnesses and proving; `Ok(None)`
        // is over-budget, not an error, and the caller chooses what to do.
        let ceiling = self.inner.emission.max_gas;
        let mut sized = batch.clone();
        sized.proofs = vec![Bytes::from(vec![0xffu8; MAX_PROOF_BYTES])];
        let projected = postAndVerifyBatchCall { batch: sized }.abi_encode();
        // EIP-7623 charges max(standard, floor). Standard now covers the bytes the
        // drain could not see; floor is what a fat, entry-light batch hits.
        let projected_gas = projected_postbatch_gas(
            batch.entries.len() as u64,
            calldata_gas(&projected),
            outbound_target_gas,
        )
        .max(calldata_floor_gas(&projected));
        if projected_gas > ceiling {
            event!(
                name: "eez.composer.emission.candidate_over_budget",
                Level::DEBUG,
                rollup_id,
                from,
                to = sync_block_number,
                projected_gas,
                ceiling,
                calldata_bytes = projected.len(),
                "candidate range exceeds the postBatch gas budget",
            );
            return Ok(None);
        }

        // Prove the assembled window (proofs[] empty — not part of the
        // publicInputsHash). Mock ignores the context; a remote prover re-executes
        // `blocks`. Settlement path, off block production.
        let block_witnesses = match self.inner.witness_source.as_ref() {
            // Remote-prover mode. Intermediate blocks `[from..sync)` are committed
            // (served by the witness store); a freshly-built endpoint isn't, so
            // capture it here from the in-memory block.
            Some(src) => {
                // Witness generation is a CPU-heavy trie walk / re-exec. Run it on
                // the blocking pool so it can't stall async worker threads on the
                // settlement path. (Store hits are cheap; the rare store miss and
                // the endpoint capture are the heavy parts.)
                let src = Arc::clone(src);
                let l2_provider = Arc::clone(
                    &self
                        .inner
                        .rollups
                        .get(&rollup_id)
                        .ok_or_else(|| format!("unknown rollup_id {rollup_id}"))?
                        .l2_provider,
                );
                let evm_config = self.inner.evm_config.clone();
                let terminal_block = sync_block.cloned();
                tokio::task::spawn_blocking(move || -> Result<Vec<BlockWitness>, String> {
                    let mut ws = (from..sync_block_number)
                        .map(|n| src.block_witness(n))
                        .collect::<Result<Vec<_>, String>>()
                        .map_err(|e| format!("witness_source: {e}"))?;
                    match terminal_block {
                        // Just-built: nothing can serve an uncommitted block, and
                        // its parent state is hot, so re-execute in-memory.
                        Some(block) => ws.push(
                            block_witness(
                                l2_provider.as_ref(),
                                &evm_config,
                                &block,
                                ExecutionWitnessMode::Legacy,
                            )
                            .map_err(|e| {
                                format!(
                                    "terminal-block witness (block {}): {e}",
                                    block.header().number()
                                )
                            })?,
                        ),
                        // Committed (historical chunk): from the store — an
                        // in-memory capture needs a possibly-pruned parent state.
                        None => ws.push(
                            src.block_witness(sync_block_number)
                                .map_err(|e| format!("witness_source: {e}"))?,
                        ),
                    }
                    Ok(ws)
                })
                .await
                .map_err(|e| format!("witness spawn_blocking join: {e}"))??
            }
            // Tests may use a lightweight prover without a witness source.
            None => Vec::new(),
        };
        let proving_ctx = ProvingContext {
            rollup_id,
            from_block: from,
            to_block: sync_block_number,
            batch: batch.clone(),
            blocks: block_witnesses,
            l1_block_hash: None, // timeless batch (blockNumber 0)
        };
        let proof = match prove_with_retry(
            self.inner.prover.as_ref(),
            proving_ctx,
            self.inner.emission.timing,
            bundle_target,
        )
        .await
        {
            Ok(proof) => proof,
            Err(error) => {
                let Some(failure) = error.actionable_failure() else {
                    return Err(PreparePostBatchError::Prover(error));
                };
                if let Err(validation_error) =
                    validate_actionable_prover_failure(failure, &batch, sync_block)
                {
                    event!(
                        name: "eez.composer.prover.actionable_invalid",
                        Level::ERROR,
                        rollup_id,
                        failure = %failure,
                        error = %validation_error,
                        "prover supplied actionable failure details that do not match the current request; treating the rejection as opaque",
                    );
                    return Err(PreparePostBatchError::Prover(error));
                }
                return Err(PreparePostBatchError::Actionable(failure));
            }
        };
        batch.proofs = vec![proof];

        let calldata = postAndVerifyBatchCall {
            batch: batch.clone(),
        }
        .abi_encode();

        // Log both settlement anchors. `StateRootMismatch` means
        // `current_state` disagreed with L1's registered root at submission;
        // an over-budget `floor_gas` names the too-fat case.
        event!(
            name: "eez.composer.postbatch.anchors",
            Level::INFO,
            rollup_id,
            posted,
            from,
            sync_block_number,
            span,
            calldata_bytes = calldata.len(),
            floor_gas = calldata_floor_gas(&calldata),
            current_state = %pre_state_root,
            claimed_final = %sync_block_state_root,
            "postBatch anchors: currentState at cursor, claimed final at Sync block",
        );

        // Read the deployment-specific EEZ registry address from the
        // environment and reject missing or malformed values.
        let eez_address = std::env::var("EEZ_REGISTRY_ADDRESS")
            .ok()
            .and_then(|s| s.parse::<Address>().ok())
            .ok_or("EEZ_REGISTRY_ADDRESS missing or not a valid address")?;

        Ok(sign_post_batch_tx(
            &ctx.l1_poster_signer,
            &ctx.l1_provider,
            eez_address,
            calldata,
            ctx.l1_chain_id,
            ctx.l1_post_batch_priority_fee,
            self.inner.emission.max_gas,
        )
        .await
        .map(Some)?)
    }
}

/// Refuse to settle a batch carrying any `destinationRollupId` / `sourceRollupId`
/// that isn't this rollup's registry id — a wiring bug (e.g. an outbound entry
/// whose `dest` stayed at the call's MAINNET(0) target) that L1 would misattribute
/// and that folds into the `publicInputsHash`. Guards the outbound `dest=rid` rewrite.
fn ensure_batch_registry_native(
    batch: &eez_protocol::EvmBatch,
    expected_rollup_id: u64,
) -> Result<(), String> {
    for (i, entry) in batch.entries.iter().enumerate() {
        if entry.destinationRollupId != expected_rollup_id {
            return Err(format!(
                "entry[{i}].destinationRollupId = {} is not the configured registry id {expected_rollup_id} — \
                 a non-registry id reached the settlement batch (composition must be registry-native)",
                entry.destinationRollupId,
            ));
        }
        for (j, call) in entry.l2ToL1Calls.iter().enumerate() {
            if call.sourceRollupId != expected_rollup_id {
                return Err(format!(
                    "entry[{i}].l2ToL1Calls[{j}].sourceRollupId = {} is not the configured registry id {expected_rollup_id}",
                    call.sourceRollupId,
                ));
            }
        }
    }
    Ok(())
}

/// Background bundle observer — verdict recording ONLY. Marks the ledger
/// entry Settled or Failed; never mutates chain state. The destructive
/// Recovery for failed entries runs at the next Sync slot through
/// `Composer::recover_failed_batch`, serialized with Sequencer commits.
///
/// "Settled" requires an `L2ExecutionPerformed` in the inclusion block
/// whose `newState` equals `expected_final_state` (the built Sync block's
/// root) — the leading immediate advancing L1 partway doesn't count.
async fn observe_bundle_outcome(
    rollup_id: u64,
    sync_height: u64,
    bundle: Vec<Bytes>,
    expected_final_state: B256,
    optimistic: Arc<OptimisticallyIncluded>,
    submitter: Submitter,
    target: BundleTarget,
) {
    let outcome = submitter
        .send_bundle(&bundle, target, Some(expected_final_state))
        .await;
    // Only an included submission with the expected state transition settles.
    // Every other outcome remains recoverable at the next Sync slot.
    let settled = matches!(
        outcome,
        Ok(SendOutcome::Included {
            state_applied: true,
            ..
        })
    );
    match &outcome {
        Ok(
            o @ SendOutcome::Included {
                state_applied: false,
                ..
            },
        ) => event!(
            name: "eez.composer.bundle.observed",
            Level::ERROR,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?o,
            "postBatch was included without the expected state transition; possible Composer/prover/settlement-contract disagreement",
        ),
        Ok(o @ SendOutcome::Dropped { .. }) => event!(
            name: "eez.composer.bundle.observed",
            Level::WARN,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?o,
            "bundle was dropped before settlement; re-queueing transactions for recovery",
        ),
        Ok(o) => event!(
            name: "eez.composer.bundle.observed",
            Level::INFO,
            event_name = "eez.composer.bundle.observed",
            rollup_id,
            sync_height,
            settled,
            outcome = ?o,
            "bundle outcome observed",
        ),
        Err(err) => event!(
            name: "eez.composer.bundle.observe_failed",
            Level::WARN,
            rollup_id,
            sync_height,
            error = %err,
            "bundle submission/observation errored; treating as a drop (re-queue; slot recovery re-verifies via cursor)",
        ),
    }
    if settled {
        optimistic.mark_settled(sync_height);
    } else {
        // slot_skipped = the drop was NOT attributable to the bundled txs →
        // requeue without counting an attempt toward poison-eviction.
        let slot_skipped = match &outcome {
            // Never reached the relay, so it says nothing about the txs. A
            // relay-side REJECTION is L1Error::Submission and still counts.
            Err(err) if err.is_transport() => true,
            // Pin unsatisfiable (block ts != pin, or unreadable), not the txs.
            _ => match target {
                BundleTarget::Exact { block, timestamp } => {
                    submitter.block_timestamp(block).await.ok().flatten() != Some(timestamp)
                }
                BundleTarget::NextBlock => false,
            },
        };
        optimistic.mark_failed(sync_height, slot_skipped);
    }
}

/// Sign an EIP-1559 L1 transaction used for `postAndVerifyBatch` submission.
///
/// Sets `max_priority_fee_per_gas` from the caller (so we can order
/// the postBatch ahead of the held user_tx) and `max_fee_per_gas` to
/// `2 * base_fee + priority_fee` per the standard EIP-1559 formula.
///
/// # Errors
///
/// Returns a `String` error if the pending-nonce or latest-block RPC fails, or
/// if transaction signing fails.
async fn sign_post_batch_tx(
    signer: &alloy_signer_local::PrivateKeySigner,
    provider: &alloy_provider::RootProvider,
    eez_address: Address,
    calldata: Vec<u8>,
    chain_id: u64,
    priority_fee: u128,
    max_gas: u64,
) -> Result<Bytes, String> {
    use alloy_consensus::TxEip1559;
    use alloy_eips::BlockNumberOrTag;
    use alloy_network::TxSignerSync;
    use alloy_primitives::TxKind;
    use alloy_provider::Provider as _;
    use reth_ethereum_primitives::{Transaction, TransactionSigned};

    let from = signer.address();
    let nonce = provider
        .get_transaction_count(from)
        .pending()
        .await
        .map_err(|e| format!("get_transaction_count({from}): {e}"))?;

    // Dev-reth base fee is 0 in practice; pull it anyway so this
    // path stays correct on non-dev chains.
    let latest = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .map_err(|e| format!("get_block latest: {e}"))?
        .ok_or_else(|| "get_block latest: None".to_string())?;
    let base_fee = u128::from(latest.header.base_fee_per_gas.unwrap_or(0));
    let max_fee_per_gas = base_fee.saturating_mul(2).saturating_add(priority_fee);

    // Only gas USED is charged, so sign at the ceiling and let the drain's
    // projection bound the batch.
    let gas_limit = max_gas;
    // A calldata floor over the cap is invalid at ANY gas limit. Refusing means
    // no emission this slot; bounded chunks cover the range later.
    let floor_gas = calldata_floor_gas(&calldata);
    if floor_gas > max_gas {
        return Err(format!(
            "postBatch calldata floor {floor_gas} exceeds EEZ_MAX_POSTBATCH_GAS {max_gas} \
             ({} calldata bytes); refusing emission — bounded chunks cover the range",
            calldata.len(),
        ));
    }

    // Diagnostic only — from the outside a drained poster looks exactly like a
    // dead relay. Don't abort; the tx still lands if the base fee settles.
    let required = U256::from(gas_limit).saturating_mul(U256::from(max_fee_per_gas));
    if let Ok(balance) = provider.get_balance(from).await
        && balance < required
    {
        event!(
            name: "eez.composer.postbatch.poster_underfunded",
            Level::ERROR,
            poster = %from,
            balance = %balance,
            required = %required,
            gas_limit,
            max_fee_per_gas,
            "poster balance below postBatch gas reserve; fund the poster or the batch will not be included",
        );
    }

    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas: priority_fee,
        to: TxKind::Call(eez_address),
        value: U256::ZERO,
        access_list: alloy_eips::eip2930::AccessList::default(),
        input: calldata.into(),
    };
    let sig = signer
        .sign_transaction_sync(&mut tx)
        .map_err(|e| format!("sign postBatch tx: {e}"))?;
    let signed = TransactionSigned::new_unhashed(Transaction::Eip1559(tx), sig);
    let mut buf = Vec::with_capacity(512);
    signed.encode_2718(&mut buf);
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::TxHash;
    use alloy_sol_types::SolValue as _;

    #[test]
    fn submission_identity_must_match_post_batch_signer() {
        let poster = Address::repeat_byte(0xa);
        assert!(ensure_submission_identity(poster, poster).is_ok());

        let err = ensure_submission_identity(poster, Address::repeat_byte(0xb)).unwrap_err();
        assert!(matches!(
            err,
            ComposerConfigError::SubmissionIdentityMismatch {
                submitter_poster,
                post_batch_signer,
            } if submitter_poster == poster && post_batch_signer == Address::repeat_byte(0xb)
        ));
    }

    fn held(sender: Address, direction: Direction, nonce: u64, hash_byte: u8) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![hash_byte; 4]),
            hash: TxHash::repeat_byte(hash_byte),
            attempts: 0,
            max_fee_per_gas: u128::from(hash_byte),
            priority_fee_per_gas: u128::from(hash_byte),
            sender,
            nonce,
            direction,
        }
    }

    #[test]
    fn poison_gap_matches_only_same_chain_higher_nonce() {
        let sender = Address::repeat_byte(0xa);
        let other = Address::repeat_byte(0xb);
        let gaps = vec![(sender, Direction::Inbound, 3)];

        assert_eq!(
            poison_gap_for(&gaps, &held(sender, Direction::Inbound, 4, 1)),
            Some(3)
        );
        assert_eq!(
            poison_gap_for(&gaps, &held(sender, Direction::Inbound, 3, 2)),
            None
        );
        assert_eq!(
            poison_gap_for(&gaps, &held(sender, Direction::Outbound, 4, 3)),
            None
        );
        assert_eq!(
            poison_gap_for(&gaps, &held(other, Direction::Inbound, 4, 4)),
            None
        );
    }

    #[test]
    fn push_poison_root_records_gap_once() {
        let sender = Address::repeat_byte(0xc);
        let tx = held(sender, Direction::Inbound, 7, 1);
        let mut poison = Vec::new();
        let mut gaps = Vec::new();

        push_poison_root(&mut poison, &mut gaps, tx.clone());
        push_poison_root(&mut poison, &mut gaps, tx);

        assert_eq!(poison.len(), 2);
        assert_eq!(gaps, vec![(sender, Direction::Inbound, 7)]);
    }

    #[test]
    fn settlement_failures_increment_once_per_episode_and_preserve_fifo() {
        let pool = HeldPool::new();
        let first = held(Address::repeat_byte(0xa), Direction::Inbound, 0, 1);
        let second = held(Address::repeat_byte(0xb), Direction::Inbound, 0, 2);
        pool.push_contiguous(first.clone(), 0).unwrap();
        pool.push_contiguous(second.clone(), 0).unwrap();

        let first_failure =
            recover_settlement_failure(&pool, 1, pool.pop_all(), SettlementFailureSource::Prover);
        assert_eq!(
            first_failure,
            SettlementFailureOutcome {
                requeued: 2,
                evicted: 0,
            }
        );
        let after_first = pool.pop_all();
        assert_eq!(
            after_first.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![first.hash, second.hash]
        );
        assert!(after_first.iter().all(|tx| tx.attempts == 1));

        let second_failure =
            recover_settlement_failure(&pool, 1, after_first, SettlementFailureSource::Relay);
        assert_eq!(
            second_failure,
            SettlementFailureOutcome {
                requeued: 2,
                evicted: 0,
            }
        );
        let after_second = pool.pop_all();
        assert_eq!(
            after_second.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![first.hash, second.hash]
        );
        assert!(after_second.iter().all(|tx| tx.attempts == 2));
        pool.release_in_flight_batch(&after_second);
    }

    #[test]
    fn settlement_failure_limit_evicts_current_and_queued_nonce_suffix() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xa);
        let mut root = held(sender, Direction::Inbound, 0, 1);
        root.attempts = MAX_BUNDLE_ATTEMPTS - 1;
        let in_flight_suffix = held(sender, Direction::Inbound, 1, 2);
        let queued_suffix = held(sender, Direction::Inbound, 2, 3);
        let opposite_direction = held(sender, Direction::Outbound, 0, 4);
        let independent = held(Address::repeat_byte(0xb), Direction::Inbound, 0, 5);
        pool.push_contiguous(root, 0).unwrap();
        pool.push_contiguous(in_flight_suffix, 0).unwrap();
        pool.push_contiguous(queued_suffix, 0).unwrap();
        let failed = pool.pop_n(2);
        pool.push_contiguous(opposite_direction.clone(), 0).unwrap();
        pool.push_contiguous(independent.clone(), 0).unwrap();

        let recovery =
            recover_settlement_failure(&pool, 1, failed, SettlementFailureSource::Prover);
        assert_eq!(
            recovery,
            SettlementFailureOutcome {
                requeued: 0,
                evicted: 3,
            }
        );
        let remaining = pool.pop_all();
        assert_eq!(
            remaining.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![opposite_direction.hash, independent.hash]
        );
        pool.release_in_flight_batch(&remaining);
    }

    #[test]
    fn recursive_error_requeues_only_candidates_remaining_after_prover_eviction() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xc);
        let poison = held(sender, Direction::Inbound, 0, 1);
        let suffix = held(sender, Direction::Inbound, 1, 2);
        let independent = held(Address::repeat_byte(0xd), Direction::Inbound, 0, 3);
        pool.push_contiguous(poison.clone(), 0).unwrap();
        pool.push_contiguous(suffix.clone(), 0).unwrap();
        pool.push_contiguous(independent.clone(), 0).unwrap();

        let survivors = pool.pop_all();
        let (retry, evicted) = partition_retryable(survivors, &poison);
        assert_eq!(
            evicted.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![poison.hash, suffix.hash]
        );
        assert!(
            pool.evict_chain_at_or_above(poison.sender, poison.direction, poison.nonce)
                .is_empty()
        );

        // Mirrors the recursive-error arm: only its owned retry set returns to
        // the pool; the outer original drain must never be requeued.
        pool.push_front_batch(retry);
        let queued = pool.pop_all();
        assert_eq!(
            queued.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![independent.hash]
        );
        pool.release_in_flight_batch(&queued);
        assert!(pool.push_contiguous(poison, 0).is_ok());
    }

    #[test]
    fn inbound_owner_mapping_follows_post_batch_entry_order() {
        fn entry(marker: u8) -> eez_protocol::abi::ExecutionEntrySol {
            eez_protocol::abi::ExecutionEntrySol {
                returnData: Bytes::from(vec![marker]),
                ..Default::default()
            }
        }

        fn composition(
            entries: Vec<eez_protocol::abi::ExecutionEntrySol>,
        ) -> eez_protocol::Composition {
            eez_protocol::Composition {
                source: eez_protocol::SourceComposition {
                    rollup_id: eez_protocol::RollupId(1),
                    batch: eez_protocol::EvmBatch {
                        entries,
                        ..Default::default()
                    },
                },
                targets: Vec::new(),
            }
        }

        let first = held(Address::repeat_byte(0xa), Direction::Inbound, 0, 0xa);
        let second = held(Address::repeat_byte(0xb), Direction::Inbound, 0, 0xb);
        let compositions = vec![
            (composition(vec![entry(0xa1), entry(0xa2)]), first.hash),
            (composition(vec![entry(0xb1)]), second.hash),
        ];

        let outbound = [entry(0x01), entry(0x02)];
        // Canonical postBatch order is anchor, outbound entries, then the
        // inbound entries grouped by their owning composition.
        let mut entries = vec![entry(0)];
        entries.extend(outbound.iter().cloned());
        entries.extend(
            compositions
                .iter()
                .flat_map(|(composition, _)| composition.source.batch.entries.iter().cloned()),
        );
        let batch = eez_protocol::EvmBatch {
            entries,
            ..Default::default()
        };
        assert_eq!(
            batch
                .entries
                .iter()
                .map(|entry| entry.returnData[0])
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 0xa1, 0xa2, 0xb1]
        );

        let survivors = vec![first.clone(), second.clone()];
        for (entry_index, expected) in [(4, first.hash), (5, second.hash)] {
            let failure = ActionableProverFailure::Inbound {
                entry_index,
                entry_hash: alloy_primitives::keccak256(batch.entries[entry_index].abi_encode()),
            };
            validate_actionable_prover_failure(failure, &batch, None).unwrap();
            assert_eq!(
                actionable_held_tx(failure, &survivors, outbound.len(), &compositions)
                    .unwrap()
                    .hash,
                expected
            );
        }
    }

    #[test]
    fn stale_partition_is_canonical_directional_and_ordered() {
        let sender = Address::repeat_byte(0xd);
        let other = Address::repeat_byte(0xe);
        let drained = vec![
            held(sender, Direction::Inbound, 1, 1),
            held(sender, Direction::Inbound, 3, 3),
            held(sender, Direction::Outbound, 2, 2),
            held(sender, Direction::Outbound, 4, 4),
            held(other, Direction::Inbound, 0, 5),
            held(sender, Direction::Inbound, 2, 6),
        ];
        let source_nonces = HashMap::from([
            ((sender, Direction::Inbound), 3),
            ((sender, Direction::Outbound), 3),
        ]);

        let (fresh, stale) = partition_stale(drained, &source_nonces);

        assert_eq!(
            fresh.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![
                TxHash::repeat_byte(3),
                TxHash::repeat_byte(4),
                TxHash::repeat_byte(5),
            ]
        );
        assert_eq!(
            stale.iter().map(|tx| tx.hash).collect::<Vec<_>>(),
            vec![
                TxHash::repeat_byte(1),
                TxHash::repeat_byte(2),
                TxHash::repeat_byte(6),
            ]
        );
    }

    #[test]
    fn eip7623_floor_matches_hand_computed_vector() {
        // 21000 + 10 * (zeros + 4*nonzeros).
        assert_eq!(calldata_floor_gas(&[]), 21_000);
        // 3 zeros (3 tokens) + 2 non-zeros (8 tokens) = 11 tokens = 110 gas.
        assert_eq!(calldata_floor_gas(&[0, 0xab, 0, 0xcd, 0]), 21_110);
        // A realistic batch's floor alone outgrows a multi-million fixed limit.
        assert!(calldata_floor_gas(&vec![0xff; 200 * 1024]) > 4_000_000);
    }

    #[test]
    fn gas_budget_out_of_range_clamps_to_default() {
        // In range → honoured.
        assert_eq!(clamp_max_postbatch_gas(12_000_000), 12_000_000);
        assert_eq!(
            clamp_max_postbatch_gas(MIN_VIABLE_POSTBATCH_GAS),
            MIN_VIABLE_POSTBATCH_GAS
        );
        assert_eq!(
            clamp_max_postbatch_gas(MIN_VIABLE_POSTBATCH_GAS - 1),
            DEFAULT_MAX_POSTBATCH_GAS
        );
        assert_eq!(
            clamp_max_postbatch_gas(DEFAULT_MAX_POSTBATCH_GAS),
            DEFAULT_MAX_POSTBATCH_GAS
        );
        assert_eq!(clamp_max_postbatch_gas(1), DEFAULT_MAX_POSTBATCH_GAS);
        // Above the EIP-7825 tx gas cap no tx is valid at any block limit.
        assert_eq!(
            clamp_max_postbatch_gas(DEFAULT_MAX_POSTBATCH_GAS + 1),
            DEFAULT_MAX_POSTBATCH_GAS
        );
    }

    /// The forge pin test re-declares both constants as literals, so a change
    /// here would leave it measuring the old value in silence. Bind them.
    #[test]
    fn solidity_pin_test_mirrors_the_rust_gas_pins() {
        let sol = include_str!("../../../contracts/test/PostBatchGasPins.t.sol");
        for (name, rust) in [
            ("POSTBATCH_BASE_GAS_PIN", POSTBATCH_BASE_GAS_PIN),
            ("POSTBATCH_ENTRY_GAS_PIN", POSTBATCH_ENTRY_GAS_PIN),
        ] {
            let decl = format!("uint256 private constant {name} = ");
            let rest = sol
                .split(&decl)
                .nth(1)
                .unwrap_or_else(|| panic!("{name} is not declared in PostBatchGasPins.t.sol"));
            let literal: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '_')
                .collect();
            let sol_value: u64 = literal
                .replace('_', "")
                .parse()
                .unwrap_or_else(|e| panic!("{name} literal {literal:?}: {e}"));
            assert_eq!(
                sol_value, rust,
                "{name} drifted: Solidity pins {sol_value}, Rust projects {rust}",
            );
        }
    }

    /// Any ceiling the clamp accepts must let the drain take at least one tx.
    #[test]
    fn the_lowest_accepted_ceiling_still_admits_one_held_tx() {
        let mut budget = PostBatchGasBudget::new(MIN_VIABLE_POSTBATCH_GAS);
        assert!(
            budget.try_accept(TxL1Gas {
                entries: 1,
                calldata_gas: 0,
                target_gas: 0,
            }),
            "the lowest accepted ceiling admits no tx — every drain would evict \
             its first held tx as poison",
        );
    }

    /// The pre-prove gate sizes the batch with a `MAX_PROOF_BYTES` stand-in for
    /// the proof it does not have yet. That is only safe if the stand-in is a
    /// worst case: longer than any real proof, and priced at the dearer
    /// non-zero-byte rate. Both halves are pinned here — if a proof system ever
    /// returns something bigger, this fails before the gate turns optimistic and
    /// the signing-side ceiling has to catch it instead.
    #[test]
    fn proof_placeholder_is_a_worst_case() {
        /// r‖s‖v — the attestation `ECDSAProofSystem` verifies.
        const ECDSA_PROOF_BYTES: usize = 65;
        const { assert!(MAX_PROOF_BYTES >= ECDSA_PROOF_BYTES) };
        // Non-zero bytes are 4 EIP-7623 tokens, zeros are 1, so an all-0xff
        // stand-in prices above any real proof of the same length.
        let placeholder = [0xffu8; MAX_PROOF_BYTES];
        assert!(
            calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0x11u8; ECDSA_PROOF_BYTES])
        );
        assert!(calldata_floor_gas(&placeholder) >= calldata_floor_gas(&[0u8; MAX_PROOF_BYTES]));
        // Monotonic in length, so a shorter real proof can never cost more.
        assert!(calldata_floor_gas(&[0xffu8; 96]) <= calldata_floor_gas(&placeholder));
    }

    #[test]
    fn empty_candidate_grid_is_highest_first() {
        // The pure half of boundary selection: which on-grid offsets below the
        // snapped height are empty. Regression: an earlier revision folded a
        // per-block framing allowance into the same vector that carried the
        // emptiness flag, so NO candidate ever qualified and emission died.
        let pick = |empty: &[bool], k: usize| -> Vec<usize> {
            (0..empty.len()).step_by(k).filter(|&i| empty[i]).collect()
        };
        assert_eq!(pick(&[true; 12], 5), vec![0, 5, 10]);
        // Snapped height carries txs → the next K down leads.
        let mut e = [true; 12];
        e[0] = false;
        assert_eq!(pick(&e, 5), vec![5, 10]);
        // Only off-grid blocks empty → nothing to settle at.
        let mut e = [false; 12];
        e[3] = true;
        assert!(pick(&e, 5).is_empty());
        // Range shorter than one step still offers the snapped height.
        assert_eq!(pick(&[true, true, true], 5), vec![0]);
    }

    #[test]
    fn grid_aligned_cap_rounds_down_to_k_multiples() {
        // Already aligned → untouched. The default (300) divides by every K a
        // sane deployment uses (12s/2s→6, 12s/3s→4, 5s/1s→5), so it rarely moves.
        assert_eq!(grid_aligned_cap(300, 5), 300);
        assert_eq!(grid_aligned_cap(300, 6), 300);
        assert_eq!(grid_aligned_cap(300, 4), 300);
        // Not aligned → the largest multiple below, since a boundary can only
        // land on the grid; the remainder was never spendable.
        assert_eq!(grid_aligned_cap(300, 7), 294);
        assert_eq!(grid_aligned_cap(102, 5), 100);
        // Below one K there is no reachable grid height above the cursor.
        assert_eq!(grid_aligned_cap(3, 5), 5);
        assert_eq!(grid_aligned_cap(5, 5), 5);
    }

    /// Gas one outbound target call used on L1, measured on a kurtosis devnet
    /// (the same run measured ~336k marginal per outbound entry in total).
    const MEASURED_TARGET_GAS: u64 = 47_763;

    /// An outbound settlement entry of realistic size: one L1 call with a
    /// 4-byte selector plus two words of arguments.
    fn outbound_entry() -> eez_protocol::abi::ExecutionEntrySol {
        eez_protocol::abi::ExecutionEntrySol {
            l2ToL1Calls: vec![eez_protocol::abi::L2ToL1CallSol {
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: Address::repeat_byte(0x11),
                sourceRollupId: 1,
                targetAddress: Address::repeat_byte(0x22),
                value: U256::ZERO,
                data: Bytes::from(vec![0x55u8; 68]),
            }],
            destinationRollupId: 1,
            success: true,
            ..Default::default()
        }
    }

    #[test]
    fn projection_charges_the_pins_the_calldata_and_the_probed_target() {
        // The cheapest postBatch: tx base + base pin + one leading entry.
        assert_eq!(
            projected_postbatch_gas(1, 0, 0),
            21_000 + POSTBATCH_BASE_GAS_PIN + POSTBATCH_ENTRY_GAS_PIN
        );
        // Entries are linear in the pin; calldata and probed target gas add raw.
        assert_eq!(
            projected_postbatch_gas(4, 500, 100),
            21_000 + POSTBATCH_BASE_GAS_PIN + 4 * POSTBATCH_ENTRY_GAS_PIN + 600
        );
        // Standard EIP-7623 rates, well under the 10-per-token floor the
        // signing path refuses on.
        assert_eq!(calldata_gas(&[]), 0);
        assert_eq!(calldata_gas(&[0, 0xab, 0, 0xcd, 0]), 3 * 4 + 2 * 16);
        // The bug this replaces: at a 50-tx bundle cap the real cost passes the
        // EIP-7825 per-tx cap, so no gas limit makes the batch postable.
        assert!(
            projected_postbatch_gas(51, 0, 50 * MEASURED_TARGET_GAS) > DEFAULT_MAX_POSTBATCH_GAS,
            "50 outbound entries must be recognised as unpostable",
        );
    }

    #[test]
    fn drain_budget_cuts_the_accepted_prefix_below_the_cap() {
        let entries = [outbound_entry()];
        let raw_tx = vec![0x77u8; 180];
        let cost = projected_tx_l1_gas(&entries, &entries, &raw_tx, MEASURED_TARGET_GAS);
        // Every term is charged: one entry, the inline call, and the calldata for
        // the entry in `batch.entries` plus its DA copy and the raw tx.
        assert_eq!(cost.entries, 1);
        assert_eq!(cost.target_gas, MEASURED_TARGET_GAS);
        let encoded = {
            use alloy_sol_types::SolValue as _;
            calldata_gas(&entries[0].abi_encode())
        };
        assert_eq!(cost.calldata_gas, 2 * encoded + calldata_gas(&raw_tx));

        let mut budget = PostBatchGasBudget::new(DEFAULT_MAX_POSTBATCH_GAS);
        // The drain leaves the belt's margin below the cap, or the belt refuses
        // what the drain accepted and the same set requeues forever.
        assert_eq!(
            budget.cap,
            DEFAULT_MAX_POSTBATCH_GAS - POSTBATCH_DRAIN_MARGIN
        );
        let per_tx = POSTBATCH_ENTRY_GAS_PIN + cost.calldata_gas + cost.target_gas;
        let expected = (budget.cap - budget.projected) / per_tx;
        let mut accepted = 0u64;
        // The drain never offers more than the per-bundle cap.
        while accepted < 50 && budget.try_accept(cost) {
            accepted += 1;
        }
        assert_eq!(accepted, expected);
        // The cut is what makes the batch postable at all.
        assert!(accepted < 50);
        assert!(budget.projected <= DEFAULT_MAX_POSTBATCH_GAS);
        // A rejected tx leaves the projection untouched, so the accepted prefix
        // is exactly what the batch will carry.
        let projected = budget.projected;
        assert!(!budget.try_accept(cost));
        assert_eq!(budget.projected, projected);
        // The drain and the prepare-time belt agree on the same entries: the
        // leading immediate plus one per accepted tx.
        assert_eq!(
            projected,
            projected_postbatch_gas(
                accepted + 1,
                accepted * cost.calldata_gas,
                accepted * MEASURED_TARGET_GAS,
            )
        );
    }

    /// Two outbound pairs near the EIP-7825 cap overflow a 30M block, so the
    /// second must be refused before `build_sync_block` hard-errors on it.
    #[test]
    fn block_gas_fit_refuses_the_second_fat_outbound_pair() {
        // Live shapes: 30M block, 2M `loadExecutionTable`, a user tx declaring
        // 16.7M and burning 13.3M of it.
        const LOAD: u64 = 2_000_000;
        const USER_DECLARED: u64 = 16_700_000;
        const USER_BURNED: u64 = 13_300_000;
        const LOAD_BURNED: u64 = 100_000;
        let pair = LOAD + USER_DECLARED;

        // Pair 1 fits an empty block.
        assert_eq!(
            block_gas_fit(0, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept,
        );
        // Pair 2 sees the burn of pair 1 and does not.
        let after_one = LOAD_BURNED + USER_BURNED;
        assert_eq!(
            block_gas_fit(after_one, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Defer,
            "the second fat pair must be deferred, not built",
        );
        // Ground truth for the deferral: the builder itself would refuse pair 2's
        // user tx, since its declared limit exceeds what the block has left.
        assert!(USER_DECLARED > BUILDER_GAS_LIMIT - after_one - LOAD_BURNED);

        // Exact boundary: equal fits, one over defers.
        assert_eq!(
            block_gas_fit(BUILDER_GAS_LIMIT - pair, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept,
        );
        assert_eq!(
            block_gas_fit(BUILDER_GAS_LIMIT - pair + 1, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Defer,
        );

        // Head of line: a pair over the whole block is unfittable at any prefix,
        // so it is evicted rather than deferred forever.
        assert_eq!(
            block_gas_fit(0, BUILDER_GAS_LIMIT + 1, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
        assert_eq!(
            block_gas_fit(after_one, BUILDER_GAS_LIMIT + 1, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
        // An undecodable tx counts as the whole block, so it can never be
        // mistaken for free space.
        assert_eq!(
            declared_gas_limit(&Bytes::from_static(b"not a tx")),
            u64::MAX
        );
        assert_eq!(
            block_gas_fit(0, u64::MAX, BUILDER_GAS_LIMIT),
            BlockGasFit::Unfittable,
        );
    }

    #[test]
    fn stale_partition_fails_open_without_a_source_nonce() {
        let tx = held(Address::repeat_byte(0xf), Direction::Outbound, 9, 1);
        let (fresh, stale) = partition_stale(vec![tx.clone()], &HashMap::new());
        assert_eq!(fresh[0].hash, tx.hash);
        assert!(stale.is_empty());

        let (fresh, stale) = partition_stale(Vec::new(), &HashMap::new());
        assert!(fresh.is_empty());
        assert!(stale.is_empty());
    }
}
