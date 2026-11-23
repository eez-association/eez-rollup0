//! Sync block composition: drain the held pool, simulate, and build the Sync block.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::Provider as _;
use eez_driver::{BUILDER_GAS_LIMIT, BundleRelease, SyncSlotBlock};
use eez_evm::EezEvmConfig;
use eez_l1::BundleTarget;
use reth_primitives_traits::AlloyBlockHeader;
use reth_storage_api::{BlockReader, StateProvider, StateProviderFactory, TransactionsProvider};
use tracing::{Level, event};

use super::batcher::{
    BlobBytes, Dispatched, PostBatchGasBudget, PreparePostBatchError, SignedPostBatch, SlotTarget,
    Staged,
};
use super::blob::{blob_stream_len, target_blob_limit};
use super::recovery::{
    MAX_BUNDLE_ATTEMPTS, SettlementFailureSource, recover_prover_failure,
    recover_settlement_failure,
};
use super::{Composer, CrossChainExecCtx, CrossChainWiring};
use crate::held_pool::HeldTx;
use crate::ingress::Direction;
use crate::local::{
    BuildError, InboundL2TargetSession, L1SlotState, L1TargetSession, build::SyncBlockState,
    build_sync_block, sync_block_candidates,
};
use crate::prover_retry::{actionable_held_tx, partition_retryable};
use crate::rollup::RollupState;

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
/// by the drain as poison, state-dependent, or transient. A source simulation
/// that merely reverts is not an error here — it finalizes to an empty
/// composition, classified downstream.
// Preserve the protocol crate's structured public error type.
#[allow(clippy::result_large_err)]
#[tracing::instrument(skip_all, fields(tx_len = raw_tx.len(), %entry_rollup_id))]
fn compose_crosschain(
    cc: &CrossChainWiring,
    entry_rollup_id: eez_protocol::RollupId,
    entry_client: &crate::local::LocalChainClient,
    raw_tx: &[u8],
    sessions: SlotSessions,
    source_state: &mut crate::local::sim::SimState,
    source_env: &reth_evm::EvmEnvFor<EezEvmConfig>,
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
        return Err(eez_protocol::ExecutorError::Unavailable(format!(
            "composition dispatched to rollup {unseeded}, which has no slot session; \
                 it would execute unchained against chain state instead of this slot's state"
        ))
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

/// What the drain can do with a pair wanting `declared` gas on a Sync block
/// that already holds `gas_used`, or with a tx against the batch's blob bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlockGasFit {
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

/// The declared limit and active cap when an envelope can never fit.
const fn gas_limit_violation(declared: u64, cap: Option<u64>) -> Option<(u64, u64)> {
    match cap {
        Some(cap) if declared > cap => Some((declared, cap)),
        _ => None,
    }
}

/// The refused pair's numbers, for the block-gas cut event.
#[derive(Debug, Clone, Copy)]
struct BlockGasCut {
    gas_used: u64,
    declared: u64,
}

/// The budget that ran out first and ended the drain.
#[derive(Debug, Clone, Copy)]
enum BudgetCut {
    /// The postBatch gas budget; the refused tx's projected cost, for the cut
    /// event.
    PostBatchGas { rejected_cost: u64 },
    /// The L2 block's gas, not the postBatch's.
    BlockGas(BlockGasCut),
    /// The blob byte budget.
    Blob,
}

/// How the cross-chain drain ended. The first refusal or failure ends it, so
/// at most one of a cut or a transient abort can happen.
enum DrainEnd {
    /// No cut or transient abort ended the drain early.
    Complete,
    /// A budget cut: `rest` is the tx that did not fit plus everything after
    /// it, owed back to the pool unpenalized.
    Cut {
        cut: BudgetCut,
        rest: Vec<(usize, HeldTx)>,
    },
    /// A transient failure aborts the slot: `rest` is the failing tx plus the
    /// unprocessed remainder of both phases, re-queued with the survivors.
    Transient {
        error: String,
        rest: Vec<(usize, HeldTx)>,
    },
}

/// Classify a [`compose_crosschain`] failure. `true` = DETERMINISTIC:
/// the composition is structurally invalid for this tx (no cross-chain
/// call / revert / bad encoding), so the tx is poison and must be
/// evicted before it can enter — and perpetually drop — a bundle.
/// `false` = STATE-DEPENDENT OR TRANSIENT, which a retry may clear. The drain
/// handles insufficient funds separately; other non-poison errors abort the
/// slot and re-queue. Capacity is deferred before this classification when an
/// accepted prefix exists; in an empty block it means the group cannot fit.
fn sim_error_is_poison(err: &eez_protocol::ComposerError) -> bool {
    use eez_protocol::{ComposerError, ExecutorError};
    match err {
        // Protocol failures = the composition itself is invalid
        // (EmptyCalls, broken chaining, unknown target, bad encoding) —
        // deterministic for this tx.
        ComposerError::Protocol(_) => true,
        // Executor failures: poison EXCEPT the clearly-transient ones.
        ComposerError::Executor(ee) => !matches!(
            ee,
            ExecutorError::Unavailable(_)
                | ExecutorError::Provider(_)
                | ExecutorError::Missing(_)
                | ExecutorError::InsufficientFunds { .. }
        ),
        // Lifecycle / internal (misconfigured, lock poisoned, double
        // register) — not the tx's fault → retry, don't evict.
        _ => false,
    }
}

fn sim_error_is_insufficient_funds(err: &eez_protocol::ComposerError) -> bool {
    use eez_protocol::{ComposerError, ExecutorError};
    matches!(
        err,
        ComposerError::Executor(error)
            if matches!(error, ExecutorError::InsufficientFunds { .. })
    )
}

/// L1-confirmed escrow (`rollups(rid).etherBalance`) an outbound withdrawal draws
/// down. `None` on any read failure, so the caller skips this early rejection;
/// the on-chain escrow check remains authoritative.
async fn read_rollup_escrow(
    provider: &alloy_provider::RootProvider,
    registry: Address,
    rid: u64,
) -> Option<U256> {
    crate::rollup::read_registered_rollup(provider, registry, rid)
        .await
        .ok()
        .map(|rollup| rollup.etherBalance)
}

/// Read L1 nonces for inbound senders at `at`.
pub(super) async fn inbound_source_nonces_for_drain(
    ctx: &CrossChainExecCtx,
    rollup_id: u64,
    drained: &[HeldTx],
    at: alloy_eips::BlockId,
) -> HashMap<(Address, Direction), u64> {
    let mut source_nonces = HashMap::new();
    for tx in drained {
        let key = (tx.sender, tx.direction);
        if tx.direction != Direction::Inbound || source_nonces.contains_key(&key) {
            continue;
        }
        match ctx
            .l1_provider
            .get_transaction_count(tx.sender)
            .block_id(at)
            .await
        {
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
                "canonical source-chain nonce read failed",
            ),
        }
    }
    source_nonces
}

/// Read outbound nonces from the Sync block's parent state.
pub(super) fn outbound_source_nonces_for_drain(
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
                "L2 source nonce read failed",
            ),
        }
    }
    source_nonces
}

pub(super) fn partition_stale(
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

fn evict_gas_limit_violation(
    rollup_id: u64,
    tx_idx: usize,
    tx: HeldTx,
    declared: u64,
    cap: u64,
    poison: &mut Vec<HeldTx>,
    poison_gaps: &mut Vec<(Address, Direction, u64)>,
) {
    event!(
        name: "eez.composer.cc_compose.tx_gas_unfittable",
        Level::WARN,
        rollup_id,
        tx_idx,
        tx_hash = %tx.hash,
        direction = ?tx.direction,
        user_declared = declared,
        user_gas_limit_cap = cap,
        "user transaction exceeds the source chain's active per-transaction gas cap; evicting (resubmit with a lower gas limit)",
    );
    push_poison_root(poison, poison_gaps, tx);
}

fn push_deferred_root(
    deferred: &mut Vec<(usize, HeldTx)>,
    deferred_gaps: &mut Vec<(Address, Direction, u64)>,
    idx: usize,
    tx: HeldTx,
) {
    let gap = (tx.sender, tx.direction, tx.nonce);
    if !deferred_gaps.contains(&gap) {
        deferred_gaps.push(gap);
    }
    deferred.push((idx, tx));
}

/// Count one failed inclusion attempt at the point where the actual root is
/// classified as underfunded. Its untried nonce suffix never calls this helper.
fn advance_funding_retry(rollup_id: u64, mut tx: HeldTx) -> (HeldTx, bool) {
    tx.attempts = tx.attempts.saturating_add(1);
    let exhausted = tx.attempts >= MAX_BUNDLE_ATTEMPTS;
    if exhausted {
        event!(
            name: "eez.composer.cc_compose.funding_retry_exhausted",
            Level::ERROR,
            rollup_id,
            tx_hash = %tx.hash,
            sender = %tx.sender,
            nonce = tx.nonce,
            attempts = tx.attempts,
            "underfunded outbound tx exhausted MAX_BUNDLE_ATTEMPTS; evicting its nonce chain (resubmit after funding)",
        );
    } else {
        event!(
            name: "eez.composer.cc_compose.funding_retry",
            Level::INFO,
            rollup_id,
            tx_hash = %tx.hash,
            sender = %tx.sender,
            nonce = tx.nonce,
            attempts = tx.attempts,
            "temporarily underfunded outbound tx re-queued for a later slot",
        );
    }
    (tx, exhausted)
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
    txs: &[eez_protocol::system_tx::PlannedTransaction],
    encoded: &mut Vec<Bytes>,
) -> Result<Vec<alloy_primitives::Log>, (usize, BuildError)> {
    let mut logs = Vec::new();
    for (at, plan) in txs.iter().enumerate() {
        let tx = prefix
            .materialize(plan)
            .map_err(|msg| (at, BuildError::ExecuteTx { idx: at, msg }))?;
        match prefix.execute_tx(&tx) {
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
        encoded.push(tx);
    }
    Ok(logs)
}

/// Take the L1 session's accumulated effects for commit into the slot's state.
/// The payload shape is pinned by `L1TargetSession::checkpoint`: a boxed
/// `Cache` and nothing else.
fn take_l1_cache(
    sessions: &mut SlotSessions,
    l1_rollup_id: eez_protocol::RollupId,
) -> Result<evm2::evm::Cache, String> {
    let mut session = sessions
        .remove(&l1_rollup_id)
        .ok_or_else(|| format!("composition returned no session for L1 rollup {l1_rollup_id}"))?;
    let snapshot = session
        .checkpoint()
        .map_err(|e| format!("L1 session checkpoint: {e}"))?;
    snapshot
        .downcast::<evm2::evm::Cache>()
        .map(|cache| *cache)
        .map_err(|_e| "L1 session checkpoint payload is not a Cache".to_owned())
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
    /// Steady Sync slot: drain the held pool and compose its transactions
    /// into a rich Sync block, re-queueing the drain if composition fails
    /// before classifying it.
    pub(super) async fn compose_steady_slot(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
        bundle_target: BundleTarget,
        cursor: u64,
    ) -> Option<SyncSlotBlock> {
        let pool = rollup.held_pool.as_ref();
        let parent_number = parent_header.number();
        let pool_len_before = pool.len();
        // Caps how many held transactions one bundle attempts and therefore how
        // many an atomic-relay drop re-queues. L1 block gas remains the hard
        // protocol constraint, so deployments may need a lower cap.
        let drained = pool.pop_n(self.inner.emission.max_user_txs);
        // When the pool is empty, do not return early. This slot attempts a
        // minimal postBatch whose leading immediate entry advances L1's stored
        // root to the empty Sync block's final state root. Without it, L1's view
        // would stop advancing while L2 continues producing blocks. The L1
        // bundle contains only `postBatch` in this path.
        let drained_count = drained.len();
        // Per-slot drain visibility (pool depth vs how many txs this bundle
        // took).
        event!(
            name: "eez.composer.sync_slot.drain",
            Level::INFO,
            event_name = "eez.composer.sync_slot.drain",
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
            &self.inner.cross_chain,
            rollup_id,
            drained.clone(),
            SlotTarget {
                parent_header,
                timestamp,
                fee_recipient: rollup.config.fee_recipient,
                bundle_target,
            },
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

    /// Compose the drained transactions in canonical order over the slot's
    /// chained execution contexts, build the Sync block, register optimistic
    /// state, spawn L1 submission observation, and return the block for
    /// immediate L2 commit.
    ///
    /// Cadence: `CompositionBuilder::finalize` runs once per drained tx (via
    /// [`CrossChainDrain::compose_outbound`] and
    /// [`CrossChainDrain::compose_inbound`], each seeded with the slot's live
    /// sessions);
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
        slot: SlotTarget<'_>,
    ) -> Result<Option<SyncSlotBlock>, String> {
        let SlotTarget {
            parent_header,
            timestamp,
            fee_recipient: suggested_fee_recipient,
            bundle_target,
        } = slot;
        let ctx = cc.exec_ctx.as_ref();
        // Ordinary source nonces remain parent-state inputs; native identity
        // is derived from the pinned intended L1 parent below.
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
        let stf_cfg = eez_protocol::system_tx::SystemTxContext {
            eezl2_address: ctx.eezl2_address,
            l2_chain_id: ctx.l2_chain_id,
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
                .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                .await;
        }

        let outbound_source_nonces =
            outbound_source_nonces_for_drain(state.as_ref(), rollup_id, &drained);
        drop(state);
        // Sized here, before classification, so an error requeues the drain
        // untouched. Uses the fork allowance the post resolves, and reserves the
        // committed range and terminal metadata before admitting held txs.
        let blob = match &ctx.blob_posting {
            None => None,
            Some(config) => {
                let max_blobs = target_blob_limit(
                    &ctx.l1_provider,
                    config,
                    self.inner.emission.timing.l1_block_time(),
                    bundle_target,
                )
                .await?
                .max_blobs;
                let from = rollup
                    .l1_head
                    .cursor()
                    .checked_add(1)
                    .ok_or("DA cursor overflow")?;
                let mut blocks = self.committed_da_blocks(rollup, parent_header, from)?;
                blocks.push(eez_payload_codec::SpanBlock {
                    beneficiary: suggested_fee_recipient.into_array(),
                    extra_data: eez_driver::BUILDER_EXTRA_DATA.to_vec(),
                    transactions: Vec::new(),
                });
                let used = blob_stream_len(rollup_id, &blocks, &[])?;
                // Catch-up ranges may span several intervals. Only the trailing
                // K blocks define the empty-interval eviction floor; preserve their
                // beneficiary/extra-data runs, but discard committed transactions.
                let k = usize::try_from(self.inner.emission.timing.k())
                    .map_err(|error| format!("DA interval overflow: {error}"))?;
                let empty_interval: Vec<_> = blocks[blocks.len().saturating_sub(k)..]
                    .iter()
                    .map(|block| eez_payload_codec::SpanBlock {
                        transactions: Vec::new(),
                        ..block.clone()
                    })
                    .collect();
                Some(BlobBytes {
                    limit: max_blobs.saturating_mul(eez_payload_codec::LOGICAL_BYTES_PER_BLOB),
                    used,
                    floor: blob_stream_len(rollup_id, &empty_interval, &[])?,
                })
            }
        };
        // ── Slot execution contexts (design §2) ──────────────────────
        // One L1 state pinned at the anchor and one live Sync-block prefix,
        // both advanced only by accepted transactions. Failing to open either
        // is transient: nothing has been consumed, so the whole drain goes
        // back to the pool untouched.
        let local = &cc.local;
        let l2_dyn: Arc<dyn crate::local::provider::StateSnapshotProvider> =
            rollup.l2_provider.clone();
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
            })
            .and_then(|(state, draft)| {
                if let BundleTarget::Exact { block, .. } = bundle_target
                    && state.anchor.number().checked_add(1) != Some(block)
                {
                    return Err("live settlement must extend its intended L1 parent".into());
                }
                let cap = match bundle_target {
                    BundleTarget::Exact { timestamp, .. } => local
                        .l1_entry
                        .next_declared_tx_gas_limit_cap(state.anchor.header(), timestamp),
                    // A target without a timestamp inherits the anchor's rules.
                    BundleTarget::NextBlock | BundleTarget::ExactBlock { .. } => local
                        .l1_entry
                        .declared_tx_gas_limit_cap_at(state.anchor.header()),
                }
                .map_err(|error| format!("resolve L1 transaction gas cap: {error}"))?;
                let live_source = eez_protocol::native_source::SourceEntry::new(
                    ctx.l2_chain_id,
                    state.anchor.hash(),
                    0,
                )?;
                Ok((state, draft, cap, live_source))
            });
        let (l1_state, draft, inbound_gas_limit_cap, live_source) = match slot_ctx {
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
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                    .await;
            }
        };
        let mut source_nonces = inbound_source_nonces_for_drain(
            ctx,
            rollup_id,
            &drained,
            alloy_eips::BlockId::latest(),
        )
        .await;
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

        // The draft's EVM rules govern outbound envelopes.
        let outbound_gas_limit_cap = draft.declared_tx_gas_limit_cap();
        event!(
            name: "eez.composer.phase2.slot_anchored",
            Level::INFO,
            rollup_id,
            l1_anchor = l1_state.anchor.number(),
            l1_anchor_hash = %l1_state.anchor.hash(),
            drained = drained.len(),
            "L1 state pinned and Sync block prefix opened for this slot",
        );

        let mut drain = CrossChainDrain {
            rollup_id,
            live_source,
            l1_state,
            draft,
            sync_txs: Vec::new(),
            system_txs_appended: 0,
            survivors: Vec::with_capacity(drained.len()),
            survivor_comps: Vec::with_capacity(drained.len()),
            pending_out: Vec::new(),
            pending_in: Vec::new(),
            outbound_entries: Vec::new(),
            outbound_target_gas: 0,
            escrow_remaining: None,
            poison: Vec::new(),
            poison_gaps: Vec::new(),
            funding_deferred: Vec::new(),
            funding_gaps: Vec::new(),
            budget: PostBatchGasBudget {
                blob,
                ..PostBatchGasBudget::new(
                    self.inner.emission.max_gas,
                    self.inner.emission.attesters,
                )
            },
            end: DrainEnd::Complete,
        };

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

        drain
            .compose_outbound(cc, &stf_cfg, outbound_gas_limit_cap, &reopen, outbounds)
            .await;
        // An outbound abort or cut moves the inbounds into its re-queue set.
        match &mut drain.end {
            DrainEnd::Cut { rest, .. } | DrainEnd::Transient { rest, .. } => {
                rest.append(&mut inbounds);
            }
            DrainEnd::Complete => {
                drain.compose_inbound(cc, &stf_cfg, inbound_gas_limit_cap, reopen, inbounds);
            }
        }
        // Boxed: the drain holds two live execution contexts, so its future is
        // past the inline-size budget.
        Box::pin(self.finish_cross_chain_batch(cc, rollup, slot, &stf_cfg, stale.len(), drain))
            .await
    }

    /// Settle the drain: run the budget-cut and poison dispositions, re-queue
    /// what stays held, then build the Sync block and its postBatch from the
    /// accepted prefix — or degrade to a minimal postBatch when nothing
    /// survived or a systemic step failed.
    async fn finish_cross_chain_batch(
        &self,
        cc: &CrossChainWiring,
        rollup: &RollupState<L2>,
        slot: SlotTarget<'_>,
        stf_cfg: &eez_protocol::system_tx::SystemTxContext,
        evicted_stale: usize,
        mut drain: CrossChainDrain,
    ) -> Result<Option<SyncSlotBlock>, String> {
        let SlotTarget {
            parent_header,
            timestamp,
            fee_recipient: suggested_fee_recipient,
            bundle_target,
        } = slot;
        let ctx = cc.exec_ctx.as_ref();
        let pool = rollup.held_pool.as_ref();
        drain.handle_budget_cut();
        let CrossChainDrain {
            rollup_id,
            live_source,
            l1_state,
            draft,
            sync_txs,
            survivors,
            survivor_comps,
            pending_out,
            pending_in,
            outbound_entries,
            outbound_target_gas,
            poison,
            poison_gaps,
            mut funding_deferred,
            end,
            ..
        } = drain;

        // Evict the poison txs' gapped higher nonces from the pool — once
        // a sender's nonce N is evicted, N+1.. can never land.
        for tx in &poison {
            // Inclusive eviction releases the poison root's reservation too.
            let evicted = pool.evict_chain_at_or_above(tx.sender, tx.direction, tx.nonce);
            event!(
                name: "eez.composer.cc_compose.poison_eviction_completed",
                Level::WARN,
                event_name = "eez.composer.cc_compose.poison_eviction_completed",
                rollup_id,
                tx_hash = %tx.hash,
                sender = %tx.sender,
                nonce = tx.nonce,
                direction = ?tx.direction,
                "poison transaction and its nonce-chain suffix evicted",
            );
            for t in evicted {
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

        let deferred = match end {
            DrainEnd::Complete => Vec::new(),
            DrainEnd::Cut { rest, .. } => rest,
            // ── Transient abort: re-queue survivors + remainder, minimal. ──
            DrainEnd::Transient { error, rest } => {
                event!(
                    name: "eez.composer.phase2.transient",
                    Level::WARN,
                    rollup_id,
                    error = %error,
                    survivors = survivors.len(),
                    "transient compose failure; re-queueing and degrading to minimal postBatch this slot",
                );
                // Both phases contribute; the pool is owed its own FIFO order.
                let mut requeue = funding_deferred;
                requeue.extend(survivors);
                requeue.extend(rest);
                requeue_unprocessed(pool, rollup_id, &poison_gaps, requeue);
                return self
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                    .await;
            }
        };

        funding_deferred.extend(deferred);
        if !funding_deferred.is_empty() {
            requeue_unprocessed(pool, rollup_id, &poison_gaps, funding_deferred);
        }

        // Nothing composed (all evicted or deferred). Still post a minimal
        // batch so L1 keeps tracking L2's progression.
        if survivors.is_empty() {
            let evicted = poison.len() + evicted_stale;
            event!(
                name: "eez.composer.phase2.all_poison",
                Level::WARN,
                rollup_id,
                evicted,
                "no held tx survived the drain (stale, deterministic failure, temporarily underfunded, or over the gas budget); emitting minimal postBatch",
            );
            return self
                .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                .await;
        }

        // Past the drain the pool's order is all that matters again.
        let survivors: Vec<HeldTx> = restore_pool_order(survivors);

        // Reconstruct canonical plans independently of the accepted byte list,
        // using the actual preceding gas recorded by the draft. A mismatch
        // aborts posting and requeues survivors without re-executing the block.
        let (pairs, canonical) = match eez_protocol::system_tx::build_cross_chain_sync_pairs(
            &pending_out,
            &pending_in,
            stf_cfg,
            live_source,
        )
        .and_then(|pairs| {
            let bytes = draft
                .materialize_prefix(&eez_protocol::system_tx::interleave_sync_block_txs(&pairs))?;
            Ok((pairs, bytes))
        }) {
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
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                    .await;
            }
        };

        // ── KEYSTONE ─────────────────────────────────────────────────
        // The block this drain appended tx-by-tx must be exactly what the
        // canonical builder — and therefore the deriver and the proof signer —
        // reconstructs from the same entries. Inequality is a composer bug, not
        // an input condition, so it degrades the slot loudly instead of posting
        // a block nobody else can rebuild.
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
                .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
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
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
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
                .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                .await;
        }
        // Per-effect intermediate L2 roots: the prover requires each entry's
        // `newRoot` to be its own effect's root, not the final Sync-block root.
        // Failure here is systemic (like build/prepare) → degrade.
        let candidates = match sync_block_candidates(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &sync_txs,
        ) {
            Ok(r) => r,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.pair_roots_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    "sync_block_candidates failed; re-queueing survivors, degrading to minimal postBatch",
                );
                pool.push_front_batch(survivors);
                return self
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
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
                &built.header,
                Some(&built.block),
                &candidates,
                &outbound_entries,
                &outbound_user_txs,
                outbound_target_gas,
                bundle_target,
                Some(eez_protocol::l1_context::LiveSettlementContext {
                    parent_hash: l1_state.anchor.hash(),
                    timestamp,
                }),
            )
            .await
            .and_then(|raw| {
                raw.ok_or_else(|| {
                    PreparePostBatchError::Build(
                        "range exceeds the postBatch gas or blob budget; bounded chunks cover it"
                            .into(),
                    )
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
                        .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
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
                let recomposed =
                    Box::pin(self.compose_cross_chain_batch(cc, rollup_id, retry, slot)).await;
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
                                slot,
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
                        "transient proof episode failed; recovering candidates and degrading to minimal postBatch",
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
                let recovery = recover_prover_failure(pool, rollup_id, survivors, &error);
                event!(
                    name: "eez.composer.prover.failure_recovered",
                    Level::WARN,
                    rollup_id,
                    requeued = recovery.requeued,
                    evicted = recovery.evicted,
                    "prover failure recovered",
                );
                return self
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
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
                    .dispatch_minimal_postbatch(ctx, rollup_id, rollup, slot)
                    .await;
            }
        };
        let total_entries: usize = comp_refs.iter().map(|c| c.source.batch.entries.len()).sum();

        // ── Dispatch the rich L1 bundle and register the optimistic block. ──
        let sync_height = built.header.number();
        let SignedPostBatch {
            raw: postbatch_raw,
            hash: post_batch_hash,
        } = postbatch_raw;
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
            name: "eez.composer.bundle.staged",
            Level::INFO,
            event_name = "eez.composer.bundle.staged",
            rollup_id,
            sync_height,
            post_batch_hash = %post_batch_hash,
            tx_count = bundle.len(),
            entry_count = total_entries,
            evicted_poison = poison.len(),
            evicted_stale,
            "rich bundle staged; sent when the Sync block commits",
        );
        rollup.optimistic.begin(
            sync_height,
            post_batch_hash,
            parent_header.clone(),
            built.header.hash(),
            survivors,
        );
        let (release, released) = BundleRelease::new();
        self.spawn_bundle_observer(
            ctx,
            rollup_id,
            sync_height,
            bundle,
            post_batch_hash,
            built.header.hash(),
            Arc::clone(&rollup.optimistic),
            bundle_target,
            Some(Staged {
                released,
                dispatched: Dispatched::Rich {
                    entry_count: total_entries,
                    evicted_poison: poison.len(),
                    evicted_stale,
                },
            }),
        );
        Ok(Some(SyncSlotBlock {
            payload: built.payload,
            header: built.header,
            release: Some(release),
        }))
    }

    /// The cadence block for every no-emission path (gate-blocked, deferred-
    /// late, historical chunk) — one shape, the one a deriver rebuilds from L1.
    pub(super) fn build_empty_slot_block(
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
            rollup.config.fee_recipient,
            &[],
        ) {
            Ok(built) => Some(SyncSlotBlock {
                payload: built.payload,
                header: built.header,
                release: None,
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
}

/// Execution state and transaction disposition shared by the two drain phases
/// of [`Composer::compose_cross_chain_batch`].
struct CrossChainDrain {
    /// The rollup being drained.
    rollup_id: u64,
    /// Native group identity anchored to the intended L1 parent.
    live_source: eez_protocol::native_source::SourceEntry,
    l1_state: L1SlotState,
    draft: SyncBlockState,
    /// The Sync block's txs in canonical order, exactly as accepted: the
    /// list the prefix is rebuilt from and the keystone assert compares the
    /// canonical rebuild against.
    sync_txs: Vec<Bytes>,
    /// One ordinal per complete native group in the original candidate.
    system_txs_appended: u64,
    /// Drain indices ride along so a re-queue can restore pool order across
    /// the two direction phases.
    survivors: Vec<(usize, HeldTx)>,
    /// Inbound survivors' compositions (their `source.batch` = the L1
    /// deferred entries) feed `prepare_post_batch_raw`'s merge.
    /// Keep each inbound composition attached to its HeldTx. The same
    /// compositions feed batch assembly and actionable entry resolution.
    survivor_comps: Vec<(eez_protocol::Composition, B256)>,
    /// Staged for the post-drain canonical rebuild, which must reproduce
    /// `sync_txs` byte-for-byte. `pending_out` pairs an outbound settlement
    /// entry with its user tx; `pending_in` holds inbound target-side
    /// derivation entries; and `outbound_entries` holds settlement entries
    /// spliced into `postBatch`.
    pending_out: Vec<(eez_protocol::abi::ExecutionEntrySol, Bytes)>,
    pending_in: Vec<eez_protocol::abi::ExecutionEntrySol>,
    outbound_entries: Vec<eez_protocol::abi::ExecutionEntrySol>,
    /// Probed gas of the accepted outbound target calls; they re-execute
    /// inside postAndVerifyBatch, so its gas limit must cover them.
    outbound_target_gas: u64,
    /// Escrow drawn down per outbound withdrawal (read once, lazily) so several
    /// in one slot can't collectively over-drain. `None` = not yet read.
    escrow_remaining: Option<U256>,
    poison: Vec<HeldTx>,
    poison_gaps: Vec<(Address, Direction, u64)>,
    /// A temporarily underfunded outbound root and its same-sender nonce
    /// suffix stay held, while unrelated outbounds and all inbounds may
    /// still settle and change the balance for the next slot.
    funding_deferred: Vec<(usize, HeldTx)>,
    funding_gaps: Vec<(Address, Direction, u64)>,
    /// Gas and bytes, not the tx cap, bound this bundle; overflow stays
    /// held for later slots.
    budget: PostBatchGasBudget,
    /// Set by the cut or transient abort that ended the drain, if any.
    end: DrainEnd,
}

impl CrossChainDrain {
    /// Log the budget cut that ended the drain, if any, evicting the first
    /// held transaction when it alone exceeds the postBatch gas budget.
    fn handle_budget_cut(&mut self) {
        let rollup_id = self.rollup_id;
        // ── Budget cut: settle what fits, keep the rest held. ─────────
        // Normal under load. Before the poison sweep, so an eviction here rides
        // the same nonce cascade.
        if let DrainEnd::Cut { cut, rest } = &mut self.end {
            let deferred_count = rest.len();
            let accepted_count = self.survivors.len();
            // A block-gas cut needs gas on the block, which needs an accepted tx,
            // so the eviction branch below can only ever be an L1-budget cut.
            debug_assert!(
                accepted_count > 0 || !matches!(cut, BudgetCut::BlockGas(_)),
                "a block-gas cut cannot happen with nothing accepted"
            );
            let rejected_cost = match cut {
                BudgetCut::PostBatchGas { rejected_cost } => *rejected_cost,
                BudgetCut::BlockGas(_) | BudgetCut::Blob => 0,
            };
            if accepted_count == 0 && !matches!(cut, BudgetCut::Blob) {
                // It lost against the whole budget, so no slot is roomier and
                // re-queueing would block the FIFO forever. It sits first.
                let (_, head) = rest.remove(0);
                event!(
                    name: "eez.composer.sync_slot.gas_budget_cut",
                    Level::WARN,
                    rollup_id,
                    accepted_count,
                    deferred_count,
                    projected_gas = self.budget.projected,
                    rejected_cost,
                    budget = self.budget.cap,
                    tx_hash = %head.hash,
                    "the first held tx alone exceeds the postBatch gas budget; it can never settle — evicting it (resubmit a cheaper call)",
                );
                push_poison_root(&mut self.poison, &mut self.poison_gaps, head);
            } else if matches!(cut, BudgetCut::Blob) {
                event!(
                    name: "eez.composer.sync_slot.blob_budget_cut",
                    Level::INFO,
                    rollup_id,
                    accepted_count,
                    deferred_count,
                    blob = ?self.budget.blob,
                    "blob byte budget reached; remaining held transactions stay queued",
                );
            } else if let BudgetCut::BlockGas(BlockGasCut { gas_used, declared }) = *cut {
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
                    projected_gas = self.budget.projected,
                    rejected_cost,
                    budget = self.budget.cap,
                    "postBatch gas budget reached; {{deferred_count}} held tx(s) stay queued for a later Sync slot",
                );
            }
        }
    }

    /// Phase 1: compose the outbound (L2→L1) transactions onto the drain.
    async fn compose_outbound(
        &mut self,
        cc: &CrossChainWiring,
        stf_cfg: &eez_protocol::system_tx::SystemTxContext,
        outbound_gas_limit_cap: Option<u64>,
        reopen: impl Fn(&[Bytes]) -> Result<SyncBlockState, BuildError>,
        outbounds: Vec<(usize, HeldTx)>,
    ) {
        let rollup_id = self.rollup_id;
        let local = &cc.local;
        let ctx = cc.exec_ctx.as_ref();
        // ── PHASE 1 — OUTBOUND (L2→L1) ───────────────────────────────
        // Source-sim runs on a fork of the Sync block against the L2 ENTRY
        // client (the L2 follower errors `Unavailable`); the L1 side executes
        // real `_processL2ToL1Calls` frames on a fork of the state. On accept the
        // canonical `[load, user]` pair extends the block and the L1 frames
        // commit to the state.
        let mut out_iter = outbounds.into_iter();
        while let Some((idx, held)) = out_iter.next() {
            if let Some(gap_at) = poison_gap_for(&self.poison_gaps, &held) {
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
                self.poison.push(held);
                continue;
            }
            if let Some(gap_at) = poison_gap_for(&self.funding_gaps, &held) {
                event!(
                    name: "eez.composer.cc_compose.outbound_funding_chain_deferred",
                    Level::INFO,
                    rollup_id,
                    tx_idx = idx,
                    tx_hash = %held.hash,
                    sender = %held.sender,
                    nonce = held.nonce,
                    gap_at,
                    "same-sender tx above an underfunded outbound nonce; holding for a later slot",
                );
                self.funding_deferred.push((idx, held));
                continue;
            }

            // Reject against the draft's cap before simulation can mark it poison.
            let user_declared = declared_gas_limit(&held.raw_tx);
            if let Some((declared, cap)) =
                gas_limit_violation(user_declared, outbound_gas_limit_cap)
            {
                evict_gas_limit_violation(
                    rollup_id,
                    idx,
                    held,
                    declared,
                    cap,
                    &mut self.poison,
                    &mut self.poison_gaps,
                );
                continue;
            }

            let contexts = L1TargetSession::new(&self.l1_state, local.l1_entry.clone())
                .map_err(|e| format!("L1TargetSession::new: {e}"))
                .and_then(|exec| {
                    self.draft
                        .fork()
                        .map(|fork| (exec, fork))
                        .map_err(|e| format!("SyncBlockState::fork: {e}"))
                });
            let (l1_exec, mut l2_fork) = match contexts {
                Ok(contexts) => contexts,
                Err(e) => {
                    self.end = DrainEnd::Transient {
                        error: format!("outbound execution contexts tx#{idx}: {e}"),
                        rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                    };
                    break;
                }
            };
            let sessions = seed_session(cc.entry_rollup_id, l1_exec);
            let (state, env) = l2_fork.state_and_env();
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
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
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
                            event_name = "eez.composer.cc_compose.outbound_multicall_unsupported",
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            entries = l1_entries.len(),
                            "outbound tx made multiple cross-chain calls; evicting because one source transaction cannot back multiple entries",
                        );
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                        continue;
                    }
                    // Defer a withdrawal that currently exceeds the rollup's L1
                    // escrow; an inbound deposit may make it valid next slot.
                    // "ether out" is the amount of Ether being withdrawn in this outbound settlement entry.
                    // If missing, the entry is malformed and must be evicted.
                    // Review once reentrancy is available
                    let Some(need) = eez_protocol::entries::outbound_ether_out(&l1_entries[0])
                    else {
                        event!(name: "eez.composer.cc_compose.outbound_ether_out_missing", Level::WARN, rollup_id, tx_idx = idx, tx_hash = %held.hash, "outbound tx is missing ether out entry, likely malformed; evicting");
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                        continue;
                    };
                    // Check here (cheap early eviction, before any append work);
                    // the debit happens at accept below.
                    if need > U256::ZERO {
                        if self.escrow_remaining.is_none() {
                            self.escrow_remaining =
                                read_rollup_escrow(&ctx.l1_provider, ctx.eez_registry, rollup_id)
                                    .await;
                        }
                        if let Some(avail) = self.escrow_remaining
                            && need > avail
                        {
                            event!(
                                name: "eez.composer.cc_compose.outbound_over_escrow",
                                Level::INFO,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                need = %need,
                                escrow = %avail,
                                "outbound withdrawal exceeds current L1 rollup escrow; applying bounded funding retry to this sender's nonce chain",
                            );
                            let (held, exhausted) = advance_funding_retry(rollup_id, held);
                            if exhausted {
                                push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            } else {
                                push_deferred_root(
                                    &mut self.funding_deferred,
                                    &mut self.funding_gaps,
                                    idx,
                                    held,
                                );
                            }
                            continue;
                        } else if self.escrow_remaining.is_none() {
                            event!(
                                name: "eez.composer.cc_compose.escrow_read_failed",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                need = %need,
                                "escrow read returned None; over-escrow check skipped and withdrawal admitted (the bundle may revert on L1 if the rollup balance is insufficient)",
                            );
                        }
                    }
                    // `[load, user]` must fit the Sync block or `build_sync_block`
                    // hard-errors. Before the L1 budget, so a refusal costs nothing.
                    // Native loads use the remaining budget. The append below
                    // checks the user's declared limit after actual load gas.
                    let declared = user_declared;
                    match block_gas_fit(self.draft.gas_used(), declared, BUILDER_GAS_LIMIT) {
                        BlockGasFit::Accept => {}
                        BlockGasFit::Defer => {
                            self.end = DrainEnd::Cut {
                                cut: BudgetCut::BlockGas(BlockGasCut {
                                    gas_used: self.draft.gas_used(),
                                    declared,
                                }),
                                rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                            };
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
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            continue;
                        }
                    }
                    // Commit both projections only after the block append succeeds.
                    let mut candidate_budget = self.budget;
                    match candidate_budget.try_accept_da(rollup_id, &l1_entries, Some(&held.raw_tx))
                    {
                        Ok(BlockGasFit::Accept) => {}
                        Ok(BlockGasFit::Unfittable) => {
                            event!(
                                name: "eez.composer.cc_compose.blob_unfittable",
                                Level::WARN,
                                rollup_id,
                                tx_hash = %held.hash,
                                "held transaction alone exceeds blob capacity; evicting its nonce suffix",
                            );
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            continue;
                        }
                        Ok(BlockGasFit::Defer) => {
                            self.end = DrainEnd::Cut {
                                cut: BudgetCut::Blob,
                                rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                            };
                            break;
                        }
                        Err(error) => {
                            self.end = DrainEnd::Transient {
                                error,
                                rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                            };
                            break;
                        }
                    }
                    // Gate before accept: this entry settles inline, so over
                    // the cap the whole bundle dies silently (invariant 7).
                    let cost = candidate_budget.tx_cost(
                        &l1_entries,
                        &l1_entries,
                        &held.raw_tx,
                        target_gas,
                    );
                    if !candidate_budget.try_accept(cost) {
                        self.end = DrainEnd::Cut {
                            cut: BudgetCut::PostBatchGas {
                                rejected_cost: candidate_budget.standalone_cost(cost),
                            },
                            rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                        };
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
                        stf_cfg,
                        self.live_source.with_index(self.system_txs_appended),
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
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            continue;
                        }
                    };
                    let plans = eez_protocol::system_tx::interleave_sync_block_txs(&pairs_k);
                    let mut pair_txs = Vec::new();
                    let pair_logs = match append_and_execute(&mut self.draft, &plans, &mut pair_txs)
                    {
                        Ok(logs) => logs,
                        Err((at, why)) => {
                            if matches!(why, BuildError::Capacity) && !self.sync_txs.is_empty() {
                                self.end = DrainEnd::Cut {
                                    cut: BudgetCut::BlockGas(BlockGasCut {
                                        gas_used: self.draft.gas_used(),
                                        declared: user_declared,
                                    }),
                                    rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                                };
                                break;
                            }
                            // A backing-store failure says nothing about the tx —
                            // abort the slot instead of evicting a valid pair.
                            if matches!(&why, BuildError::Provider(_)) {
                                self.end = DrainEnd::Transient {
                                    error: format!("outbound append tx#{idx} at {at}: {why}"),
                                    rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                                };
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
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            // A failed append may be half-applied; the accepted
                            // list is the only truth to rebuild from.
                            self.draft = match reopen(&self.sync_txs) {
                                Ok(rebuilt) => rebuilt,
                                Err(e) => {
                                    self.end = DrainEnd::Transient {
                                        error: format!(
                                            "Sync block prefix rebuild after tx#{idx}: {e}"
                                        ),
                                        rest: abort_rest(None, &mut out_iter, Vec::new()),
                                    };
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
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                        self.draft = match reopen(&self.sync_txs) {
                            Ok(rebuilt) => rebuilt,
                            Err(e) => {
                                self.end = DrainEnd::Transient {
                                    error: format!("Sync block prefix rebuild after tx#{idx}: {e}"),
                                    rest: abort_rest(None, &mut out_iter, Vec::new()),
                                };
                                break;
                            }
                        };
                        continue;
                    }
                    self.sync_txs.extend(pair_txs);
                    self.system_txs_appended += pairs_k.len() as u64;
                    // Debit at accept, not at the check above: a tx evicted in
                    // between never draws the escrow down on L1, and a budget
                    // reduced for it would evict later legitimate withdrawals.
                    if let Some(avail) = self.escrow_remaining {
                        self.escrow_remaining = Some(avail.saturating_sub(need));
                    }

                    // The state advances only now, behind the accepted block.
                    match take_l1_cache(&mut sessions, cc.entry_rollup_id) {
                        Ok(cache) => self.l1_state.cache = cache,
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
                            self.end = DrainEnd::Transient {
                                error: format!("L1 session hand-off tx#{idx}: {e}"),
                                rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                            };
                            break;
                        }
                    }

                    self.budget = candidate_budget;
                    self.pending_out
                        .push((l1_entries[0].clone(), held.raw_tx.clone()));
                    self.outbound_entries.extend(l1_entries);
                    self.outbound_target_gas = self.outbound_target_gas.saturating_add(target_gas);
                    // NOT pushed to `survivor_comps`: its `source.batch` is OUR
                    // L2's entries (a dest=MAINNET call that must not settle on
                    // L1). The L1 settlement is `outbound_entries`, spliced
                    // separately with dest=rid.
                    self.survivors.push((idx, held));
                }
                Err(e) if sim_error_is_insufficient_funds(&e) => {
                    event!(
                        name: "eez.composer.cc_compose.outbound_funding_deferred",
                        Level::INFO,
                        rollup_id,
                        tx_idx = idx,
                        tx_hash = %held.hash,
                        sender = %held.sender,
                        nonce = held.nonce,
                        error = %e,
                        "outbound target lacks funds; applying bounded funding retry while inbounds continue",
                    );
                    let (held, exhausted) = advance_funding_retry(rollup_id, held);
                    if exhausted {
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                    } else {
                        push_deferred_root(
                            &mut self.funding_deferred,
                            &mut self.funding_gaps,
                            idx,
                            held,
                        );
                    }
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
                    push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                }
                Err(e) => {
                    self.end = DrainEnd::Transient {
                        error: format!("compose_crosschain outbound tx#{idx}: {e}"),
                        rest: abort_rest(Some((idx, held)), &mut out_iter, Vec::new()),
                    };
                    break;
                }
            }
        }
    }

    /// Phase 2: compose the inbound (L1→L2) transactions onto the drain, after
    /// every outbound pair.
    fn compose_inbound(
        &mut self,
        cc: &CrossChainWiring,
        stf_cfg: &eez_protocol::system_tx::SystemTxContext,
        inbound_gas_limit_cap: Option<u64>,
        reopen: impl Fn(&[Bytes]) -> Result<SyncBlockState, BuildError>,
        inbounds: Vec<(usize, HeldTx)>,
    ) {
        let rollup_id = self.rollup_id;
        let local = &cc.local;
        // ── PHASE 2 — INBOUND (L1→L2) ────────────────────────────────
        // Source-sim runs the L1 user tx on a fork of the state; the L2 side
        // probes the canonical delivery on a fork of the block and reads the
        // claim off the real `EEZL2 → proxy` frame. On accept the delivery
        // extends the block and the source fork's writes become the state.
        // Skipped after a phase-1 abort or cut; the caller re-queues these txs.
        let mut in_iter = inbounds.into_iter();
        while let Some((idx, held)) = in_iter.next() {
            if let Some(gap_at) = poison_gap_for(&self.poison_gaps, &held) {
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
                self.poison.push(held);
                continue;
            }

            // Source simulation is uncapped; enforce the L1 envelope cap first.
            let user_declared = declared_gas_limit(&held.raw_tx);
            if let Some((declared, cap)) = gas_limit_violation(user_declared, inbound_gas_limit_cap)
            {
                evict_gas_limit_violation(
                    rollup_id,
                    idx,
                    held,
                    declared,
                    cap,
                    &mut self.poison,
                    &mut self.poison_gaps,
                );
                continue;
            }

            let contexts = self
                .draft
                .fork()
                .map_err(|e| format!("SyncBlockState::fork: {e}"))
                .map(|fork| {
                    InboundL2TargetSession::new(
                        fork,
                        stf_cfg.clone(),
                        self.live_source.with_index(self.system_txs_appended),
                    )
                })
                .and_then(|probe| {
                    self.l1_state
                        .fork_state(&local.l1_entry)
                        .map(|source| (probe, source))
                        .map_err(|e| format!("L1SlotState::fork_state: {e}"))
                });
            let (probe, (mut l1_fork, l1_env)) = match contexts {
                Ok(contexts) => contexts,
                Err(e) => {
                    self.end = DrainEnd::Transient {
                        error: format!("inbound execution contexts tx#{idx}: {e}"),
                        rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                    };
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
                &l1_env,
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
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                        continue;
                    }

                    // Commit both projections only after the block append succeeds.
                    let mut candidate_budget = self.budget;
                    match candidate_budget.try_accept_da(rollup_id, &target_entries, None) {
                        Ok(BlockGasFit::Accept) => {}
                        Ok(BlockGasFit::Unfittable) => {
                            event!(
                                name: "eez.composer.cc_compose.blob_unfittable",
                                Level::WARN,
                                rollup_id,
                                tx_hash = %held.hash,
                                "held transaction alone exceeds blob capacity; evicting its nonce suffix",
                            );
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            continue;
                        }
                        Ok(BlockGasFit::Defer) => {
                            self.end = DrainEnd::Cut {
                                cut: BudgetCut::Blob,
                                rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                            };
                            break;
                        }
                        Err(error) => {
                            self.end = DrainEnd::Transient {
                                error,
                                rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                            };
                            break;
                        }
                    }
                    // Same gate, no target gas: a deferred entry only queues
                    // here, and its L1 half is a separate bundled tx.
                    let cost = candidate_budget.tx_cost(
                        &composition.source.batch.entries,
                        &target_entries,
                        &[],
                        0,
                    );
                    if !candidate_budget.try_accept(cost) {
                        self.end = DrainEnd::Cut {
                            cut: BudgetCut::PostBatchGas {
                                rejected_cost: candidate_budget.standalone_cost(cost),
                            },
                            rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                        };
                        break;
                    }
                    // ── ACCEPT ───────────────────────────────────────
                    let deliveries = match target_entries
                        .iter()
                        .map(eez_protocol::entries::InboundSidecar::try_from)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| error.to_string())
                        .and_then(|inbound| {
                            eez_protocol::system_tx::build_inbound_system_txs(
                                &inbound,
                                stf_cfg,
                                self.live_source.with_index(self.system_txs_appended),
                            )
                        }) {
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
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
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
                            push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                            continue;
                        }
                    };
                    // The delivery re-runs the on-chain claim compare, so this
                    // append IS the verifier: a revert means the claims and the
                    // block disagree and the tx must go.
                    let plans = deliveries
                        .into_iter()
                        .map(eez_protocol::system_tx::PlannedTransaction::Native)
                        .collect::<Vec<_>>();
                    let mut deliveries = Vec::new();
                    if let Err((at, why)) =
                        append_and_execute(&mut self.draft, &plans, &mut deliveries)
                    {
                        if matches!(why, BuildError::Capacity) && !self.sync_txs.is_empty() {
                            self.end = DrainEnd::Cut {
                                cut: BudgetCut::BlockGas(BlockGasCut {
                                    gas_used: self.draft.gas_used(),
                                    declared: eez_primitives::SYSTEM_TX_GAS_CAP,
                                }),
                                rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                            };
                            break;
                        }
                        // A backing-store failure says nothing about the tx —
                        // abort the slot instead of evicting a valid delivery.
                        if matches!(&why, BuildError::Provider(_)) {
                            self.end = DrainEnd::Transient {
                                error: format!("inbound append tx#{idx} at {at}: {why}"),
                                rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                            };
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
                        push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                        self.draft = match reopen(&self.sync_txs) {
                            Ok(rebuilt) => rebuilt,
                            Err(e) => {
                                self.end = DrainEnd::Transient {
                                    error: format!("Sync block prefix rebuild after tx#{idx}: {e}"),
                                    rest: abort_rest(None, &mut in_iter, Vec::new()),
                                };
                                break;
                            }
                        };
                        continue;
                    }
                    self.system_txs_appended += deliveries.len() as u64;
                    self.sync_txs.extend(deliveries);
                    // The source fork carries the L1 user tx's committed writes;
                    // later inbound sims must observe them (design §4 step 6).
                    self.l1_state.cache = l1_fork.into_cache();

                    let target_count = target_entries.len();
                    self.pending_in.extend(target_entries);
                    event!(
                        name: "eez.composer.cc_compose.tx",
                        Level::INFO,
                        rollup_id,
                        tx_idx = idx,
                        target_count,
                        "composition produced {{target_count}} target(s) for held tx #{{tx_idx}}",
                    );
                    self.budget = candidate_budget;
                    self.survivor_comps.push((composition, held.hash));
                    self.survivors.push((idx, held));
                }
                Err(eez_protocol::ComposerError::Executor(
                    eez_protocol::ExecutorError::Capacity,
                )) if !self.sync_txs.is_empty() => {
                    self.end = DrainEnd::Cut {
                        cut: BudgetCut::BlockGas(BlockGasCut {
                            gas_used: self.draft.gas_used(),
                            declared: eez_primitives::SYSTEM_TX_GAS_CAP,
                        }),
                        rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                    };
                    break;
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
                    push_poison_root(&mut self.poison, &mut self.poison_gaps, held);
                }
                Err(e) => {
                    // Transient (provider / transport / unavailable) —
                    // abort the slot, re-queue this tx + the remainder.
                    self.end = DrainEnd::Transient {
                        error: format!("compose_crosschain inbound tx#{idx}: {e}"),
                        rest: abort_rest(Some((idx, held)), &mut in_iter, Vec::new()),
                    };
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "native_gas_tests.rs"]
mod native_gas_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composer::tests::{held, signed_user_tx};
    use crate::held_pool::HeldPool;
    use crate::prover_retry::validate_actionable_prover_failure;
    use alloy_primitives::TxHash;
    use alloy_sol_types::SolValue as _;
    use eez_prover::ActionableProverFailure;

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
    fn insufficient_target_funds_are_not_poison() {
        let error =
            eez_protocol::ComposerError::Executor(eez_protocol::ExecutorError::InsufficientFunds {
                available: U256::ZERO,
                required: U256::from(1),
            });

        assert!(sim_error_is_insufficient_funds(&error));
        assert!(!sim_error_is_poison(&error));
    }

    #[test]
    fn push_deferred_root_records_nonce_gap_and_index() {
        let sender = Address::repeat_byte(0xd);
        let tx = held(sender, Direction::Outbound, 2, 1);
        let mut deferred = Vec::new();
        let mut gaps = Vec::new();

        push_deferred_root(&mut deferred, &mut gaps, 4, tx.clone());
        push_deferred_root(&mut deferred, &mut gaps, 5, tx);

        assert_eq!(deferred.len(), 2);
        assert_eq!(deferred[0].0, 4);
        assert_eq!(deferred[1].0, 5);
        assert_eq!(gaps, vec![(sender, Direction::Outbound, 2)]);
    }

    #[test]
    fn funding_retry_exhausts_on_the_third_attempt() {
        let sender = Address::repeat_byte(0xd);
        for (attempts, expected, exhausted) in [(0, 1, false), (1, 2, false), (2, 3, true)] {
            let mut tx = held(sender, Direction::Outbound, 0, 1);
            tx.attempts = attempts;
            let (tx, actual_exhausted) = advance_funding_retry(1, tx);
            assert_eq!(tx.attempts, expected);
            assert_eq!(actual_exhausted, exhausted);
        }
    }

    #[test]
    fn funding_retry_limit_evicts_the_full_nonce_suffix() {
        let pool = HeldPool::new();
        let sender = Address::repeat_byte(0xd);
        let mut root = held(sender, Direction::Outbound, 0, 1);
        root.attempts = MAX_BUNDLE_ATTEMPTS - 1;
        let in_flight_suffix = held(sender, Direction::Outbound, 1, 2);
        let queued_suffix = held(sender, Direction::Outbound, 2, 3);
        let opposite_direction = held(sender, Direction::Inbound, 0, 4);
        let independent = held(Address::repeat_byte(0xe), Direction::Outbound, 0, 5);
        pool.push_contiguous(root, 0).unwrap();
        pool.push_contiguous(in_flight_suffix, 0).unwrap();
        pool.push_contiguous(queued_suffix, 0).unwrap();
        let mut deferred: Vec<_> = pool.pop_n(2).into_iter().enumerate().collect();
        pool.push_contiguous(opposite_direction.clone(), 0).unwrap();
        pool.push_contiguous(independent.clone(), 0).unwrap();

        let mut poison = Vec::new();
        let mut poison_gaps = Vec::new();
        let (_, root) = deferred.remove(0);
        let (root, exhausted) = advance_funding_retry(1, root);
        assert!(exhausted);
        push_poison_root(&mut poison, &mut poison_gaps, root);
        assert_eq!(poison.len(), 1);
        assert_eq!(poison_gaps, vec![(sender, Direction::Outbound, 0)]);
        for tx in &poison {
            let _ = pool.evict_chain_at_or_above(tx.sender, tx.direction, tx.nonce);
        }
        requeue_unprocessed(&pool, 1, &poison_gaps, deferred);

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

    /// Two outbound pairs near the EIP-7825 cap overflow a 30M block, so the
    /// second must be refused before `build_sync_block` hard-errors on it.
    #[test]
    fn block_gas_fit_refuses_the_second_fat_outbound_pair() {
        // The early gate checks the signed user's declared gas. The actual
        // load is metered before the user is admitted by append_and_execute.
        const USER_DECLARED: u64 = 16_700_000;
        const USER_BURNED: u64 = 13_300_000;
        const LOAD_BURNED: u64 = 100_000;
        let pair = USER_DECLARED;

        // Pair 1 fits an empty block.
        assert_eq!(
            block_gas_fit(0, pair, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept
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
    fn eip_7825_user_cap_is_independent_of_sync_block_fit() {
        // An invalid user limit can still fit in the aggregate Sync-block budget.
        const EIP_7825_TX_GAS_LIMIT: u64 = alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
        let invalid_user_limit = EIP_7825_TX_GAS_LIMIT + 1;
        let pair_limit = invalid_user_limit;

        assert_eq!(
            block_gas_fit(0, pair_limit, BUILDER_GAS_LIMIT),
            BlockGasFit::Accept,
            "block capacity alone must not be treated as user-transaction validity",
        );
        assert!(invalid_user_limit > EIP_7825_TX_GAS_LIMIT);
        assert!(pair_limit < BUILDER_GAS_LIMIT);
    }

    #[test]
    fn gas_limit_violation_uses_the_decoded_eip_7825_boundary_only_when_active() {
        const EIP_7825_TX_GAS_LIMIT: u64 = alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
        let at_cap = declared_gas_limit(&signed_user_tx(EIP_7825_TX_GAS_LIMIT));
        let over_cap = declared_gas_limit(&signed_user_tx(EIP_7825_TX_GAS_LIMIT + 1));

        assert_eq!(
            gas_limit_violation(at_cap, Some(EIP_7825_TX_GAS_LIMIT)),
            None
        );
        assert_eq!(
            gas_limit_violation(over_cap, Some(EIP_7825_TX_GAS_LIMIT)),
            Some((EIP_7825_TX_GAS_LIMIT + 1, EIP_7825_TX_GAS_LIMIT)),
        );
        assert_eq!(gas_limit_violation(over_cap, None), None);
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
