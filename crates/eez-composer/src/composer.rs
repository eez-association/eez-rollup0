//! [`Composer`]: the umbrella that owns each rollup's cross-chain
//! produce → prove → submit path.
//!
//! Two entry points:
//!
//! - `compose_sync_slot` — called by the Sequencer on each Sync slot.
//!   Drains the rollup's [`HeldPool`](crate::HeldPool), runs every held
//!   tx through the cross-chain `EvmComposer`, builds the rich Sync
//!   block, and submits `[postBatch, user_tx…]` as one atomic bundle:
//!   committed to L2 optimistically, observed in the background, reorged
//!   on failure (see [`crate::optimistic`]).
//! - [`Composer::run`] — follows the shared `L1Watcher` event stream,
//!   logging confirmed vs external `BatchPosted` for this rollup. The
//!   L1-confirmed cursor + batch index are advanced by the Deriver (sole
//!   writer of [`L1CanonicalHead`](eez_l1::L1CanonicalHead)).
//!
//! Each batch is proved by the shared [`Prover`] and sent via the shared
//! [`Submitter`](eez_l1::Submitter) bundle relay.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::{Address, B256, Bytes, U256};
use async_trait::async_trait;
use eez_driver::{BlockCommitterHandle, ParentContext, SyncSlotBlock, SyncSlotComposer};
use eez_l1::{BundleTarget, L1Event, L1Watcher, SendOutcome, Submitter};
use eez_prover::Prover;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{BlockReader, BlockSource, StateProviderFactory, TransactionsProvider};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::held_pool::HeldTx;
use crate::local::build_sync_block;
use crate::optimistic::OptimisticallyIncluded;
use crate::rollup::RollupState;

/// Runtime config for the cross-chain execution path on Sync slots.
/// `Composer::new` accepts `Option<Arc<CrossChainExecCtx>>`; `Some`
/// means a wired `EvmComposer` and the keys/addresses needed to sign
/// the L2 system txs that the composer's
/// `simulate_and_resolve` returns as raw `(load_table_payload,
/// execute_payload)` bytes.
///
/// Owned by `eez-node` at startup and shared via `Arc` because the
/// `PrivateKeySigner` is bigger than two-line clone-cheap.
#[derive(Clone)]
pub struct CrossChainExecCtx {
    /// Signing key for SYSTEM_ADDRESS — must match CCM-L2's
    /// `SYSTEM_ADDRESS` immutable. Used to wrap each composer-
    /// produced `(load_table_payload, execute_payload)` pair into
    /// two signed legacy L2 txs.
    pub system_signer: alloy_signer_local::PrivateKeySigner,
    /// L2 CCM-L2 address (where SYSTEM_ADDRESS calls both
    /// `loadExecutionTable` and `executeIncomingCrossChainCall`).
    pub ccm_l2_address: Address,
    /// L2 chain id for EIP-155 signing.
    pub l2_chain_id: u64,
    /// L2 system tx gas_price (legacy). 1 gwei is plenty above
    /// devnet basefee.
    pub l2_gas_price: u128,
    /// Per-tx gas limit for the load + execute system txs. Matches
    /// the reference `EXECUTE_INCOMING_GAS_LIMIT` (~2M).
    pub l2_gas_limit: u64,
    /// Alloy provider for the embedded L1 RPC. Used to sign the
    /// `postBatch` tx (nonce + fee reads). Submission itself goes
    /// through `submitter`.
    pub l1_provider: alloy_provider::RootProvider,
    /// Shared `Submitter` handle — the single L1 submission path.
    /// `Submitter` is internally `Arc<Inner>`, so `Clone` is cheap.
    /// `compose_sync_slot` hands it `[postBatch, user_tx_1, …]` via
    /// `Submitter::send_bundle`; the Submitter owns the transport
    /// decision (atomic `eth_sendBundle` on relays that support it,
    /// ordered mempool submission on plain execution RPCs like dev
    /// reth / anvil).
    pub submitter: eez_l1::Submitter,
    /// L1 EOA whose key signs the `postBatch` tx. Different from
    /// `system_signer` (which is the L2 SYSTEM_ADDRESS). For dev
    /// smoke this is typically the hardhat #0 deployer key; in
    /// production this is the based-rollup composer's L1 wallet.
    pub l1_poster_signer: alloy_signer_local::PrivateKeySigner,
    /// L1 chain id for EIP-155 signing of the `postBatch` tx.
    pub l1_chain_id: u64,
    /// L1 priority fee for the `postBatch` tx, in wei per gas.
    /// Must exceed any held user `raw_tx`'s priority fee so that
    /// dev-reth's payload builder orders `postBatch` first in
    /// the L1 block. Default: 10 gwei (well above the smoke's
    /// `cast mktx --gas-price 2 gwei` user_tx).
    pub l1_post_batch_priority_fee: u128,
    /// MockECDSAProofSystem address on L1, embedded in
    /// `batch.proofSystems[0]`. The on-chain `EEZ.postAndVerifyBatch`
    /// iterates `proofSystems[]` and calls `verify` on each — the
    /// mock accepts any 65-byte ECDSA sig over its fixed
    /// `MOCK_PROVER_DIGEST` from the configured signer.
    pub mock_proof_system_address: Address,
    /// L2 rollup id, embedded in
    /// `batch.rollupIdsWithProofSystems[0].rollupId` so the L1
    /// registry routes the per-rollup state delta correctly.
    pub l2_rollup_id: u64,
}

impl std::fmt::Debug for CrossChainExecCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainExecCtx")
            .field("system_address", &self.system_signer.address())
            .field("ccm_l2_address", &self.ccm_l2_address)
            .field("l2_chain_id", &self.l2_chain_id)
            .field("l2_gas_price", &self.l2_gas_price)
            .field("l2_gas_limit", &self.l2_gas_limit)
            .finish_non_exhaustive()
    }
}

/// Relay-drop retries before a held user_tx is evicted as probable
/// poison. Poison is normally caught at COMPOSE time (a tx whose
/// `simulate_and_resolve` deterministically fails — e.g. a wrong-proxy
/// tx → `EmptyCalls`, or a revert — is evicted before it can enter a
/// bundle). Under strict all-or-nothing bundles a bundle that still
/// DROPS is relay bad luck, so its txs are re-queued; this bound only
/// backstops poison the compose-time sim view missed (rbuilder sims
/// against a slightly different post-postBatch state). After this many
/// consecutive drops the tx is evicted loudly (with the nonce-cascade)
/// so it can't block the FIFO queue forever.
pub const MAX_BUNDLE_ATTEMPTS: u32 = 3;

/// Classify a `simulate_and_resolve` failure. `true` = DETERMINISTIC:
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
                | ExecutorErrorKind::Transport(_)
                | ExecutorErrorKind::Missing(_)
        ),
        // Lifecycle / internal (misconfigured, lock poisoned, double
        // register) — not the tx's fault → retry, don't evict.
        _ => false,
    }
}

/// Composer umbrella. Cheaply [`Clone`]able (`Arc<Inner>`).
#[derive(Clone)]
pub struct Composer<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

struct Inner<L2: BlockReader> {
    /// Per-rollup state. One entry today; `HashMap` keeps the routing
    /// shape ready for multi-L2.
    rollups: HashMap<u64, RollupState<L2>>,
    /// Shared across rollups: one prover, one submitter, one `L1Watcher`.
    prover: Arc<dyn Prover>,
    submitter: Submitter,
    l1_watcher: L1Watcher,
    /// EVM config — used by [`build_sync_block`] to construct the
    /// per-Sync-slot block via reth-evm `BlockBuilder`.
    evm_config: EthEvmConfig,
    /// Cross-chain composer: per-tx `simulate_and_resolve` orchestrator
    /// generic over `EvmProtocol`. `None` when L1 isn't wired (e.g.
    /// standalone / follower modes). When `Some`, `compose_sync_slot`
    /// dispatches each held tx through it to get the
    /// `Composition<EvmProtocol>` (L2 destination effects + L1
    /// `ExecutionEntry`s).
    evm_composer: Option<eez_evm_inspector::EvmComposer>,
    /// Runtime context (signer + L2 chain config) for wrapping the
    /// composer's `(load_table_payload, execute_payload)` byte
    /// outputs into signed L2 system txs. Must be `Some` whenever
    /// `evm_composer` is `Some`; both come from the same `eez-node`
    /// startup wiring step.
    cc_exec_ctx: Option<Arc<CrossChainExecCtx>>,
    /// Handle to the `BlockCommitter` actor (the sole engine-API
    /// owner). Set once at startup via [`Composer::set_committer`]
    /// after the Sequencer spawns the actor. The bundle-observer task
    /// uses it to reorg the L2 head when an optimistically-committed
    /// Sync block's bundle fails on L1.
    committer: std::sync::OnceLock<BlockCommitterHandle<EthEngineTypes>>,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_ids", &self.inner.rollups.keys().collect::<Vec<_>>())
            .field("prover", &self.inner.prover)
            .field("submitter", &self.inner.submitter)
            .field("l1_watcher", &self.inner.l1_watcher)
            .finish()
    }
}

impl<L2> Composer<L2>
where
    L2: BlockReader<Header = alloy_consensus::Header> + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
{
    /// Constructs the umbrella. Synchronous — does no I/O.
    #[must_use]
    pub fn new(
        rollups: HashMap<u64, RollupState<L2>>,
        prover: Arc<dyn Prover>,
        submitter: Submitter,
        l1_watcher: L1Watcher,
        evm_config: EthEvmConfig,
        evm_composer: Option<eez_evm_inspector::EvmComposer>,
        cc_exec_ctx: Option<Arc<CrossChainExecCtx>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                rollups,
                prover,
                submitter,
                l1_watcher,
                evm_config,
                evm_composer,
                cc_exec_ctx,
                committer: std::sync::OnceLock::new(),
            }),
        }
    }

    /// Wire the `BlockCommitter` handle after the Sequencer spawns the
    /// actor. Must be called before the first Sync slot in cross-chain
    /// mode; the bundle-observer task needs it to reorg the L2 head on
    /// bundle failure. Second and later calls are no-ops.
    pub fn set_committer(&self, handle: BlockCommitterHandle<EthEngineTypes>) {
        let _ = self.inner.committer.set(handle);
    }

    /// Run loop. Drains the `L1Watcher` broadcast: logs own/external
    /// `BatchPosted` attribution and drives optimistic recovery on
    /// `Reorg`/`Finalized` (the cursor and its retreats are owned by
    /// the Deriver via the shared `L1CanonicalHead`).
    ///
    /// Cross-chain Sync-slot composition is driven separately through
    /// the [`SyncSlotComposer`] trait (the Sequencer calls
    /// `compose_sync_slot` on its schedule), so this loop takes no
    /// batch-candidate input. Exits when the L1 event stream closes —
    /// the upstream `L1Watcher` task has exited.
    pub async fn run(self) {
        let mut l1_events = self.inner.l1_watcher.subscribe();
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
                    if let Some(pool) = rollup.held_pool.as_ref() {
                        for tx in txs {
                            pool.push(tx);
                        }
                    }
                }
            }
            Ok(L1Event::Finalized { block_number, .. }) => {
                // Settled batches at or below L1 finality can never be
                // rolled out — but "Settled" is the observer's/cursor's
                // belief. Finality audit backstop: before discarding
                // each ledger entry, verify its postBatch receipt still
                // exists on L1. A missing receipt means a reorg rolled
                // the batch out and the L1Watcher missed it — phantom
                // effects on L2 (invariant 7: never drop blind). The
                // receipt checks are async and `on_l1_event` is sync,
                // so the audit runs in a spawned task per rollup.
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
                                    if let Some(pool) = held_pool.as_ref() {
                                        pool.push_front_batch(txs);
                                    }
                                }
                                Err(err) => {
                                    // Inconclusive: re-queueing txs that
                                    // actually landed would burn nonces
                                    // and poison the next bundle's
                                    // simulation — scream instead of
                                    // guessing (invariant 7).
                                    event!(
                                        name: "eez.composer.finality_audit.check_failed",
                                        Level::ERROR,
                                        rollup_id,
                                        sync_height,
                                        post_batch_hash = %post_batch_hash,
                                        error = %err,
                                        "finality-audit receipt lookup failed; ledger entry dropped UNAUDITED",
                                    );
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
    /// Drain `rollup_id`'s `HeldPool`, build the Sync block carrying the
    /// drained txs, and hand it back for the Sequencer to commit.
    /// Returns `None` (→ vanilla pool-driven Sync commit) when the rollup
    /// is unknown, has no `HeldPool`, or the pool is empty this slot.
    ///
    /// With a cross-chain `EvmComposer` wired, each drained tx runs
    /// through `simulate_and_resolve` and the rich Sync block + atomic L1
    /// bundle dispatch via `compose_via_evm_composer` (optimistic).
    /// Without one (no embedded L1), the drained txs commit as ordinary
    /// type-0x2 calls — the standalone build+commit fallback.
    async fn compose_sync_slot(
        &self,
        rollup_id: u64,
        parent: ParentContext,
        timestamp: u64,
    ) -> Option<SyncSlotBlock> {
        event!(
            name: "eez.composer.sync_slot.invoked",
            Level::INFO,
            rollup_id,
            timestamp,
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
        if !rollup.l1_head.is_initialized() {
            event!(
                name: "eez.composer.sync_slot.deriver_not_ready",
                Level::DEBUG,
                rollup_id,
                "deriver has not completed an L1 sync yet; skipping batch composition",
            );
            return None;
        }
        let Some(pool) = rollup.held_pool.as_ref() else {
            event!(
                name: "eez.composer.sync_slot.no_pool",
                Level::WARN,
                rollup_id,
                "rollup has no HeldPool configured",
            );
            return None;
        };
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
        rollup.optimistic.resolve_below_cursor(cursor);

        // ── Slot-context failure recovery ────────────────────────────
        // The observer task only RECORDS verdicts; the destructive
        // recovery happens here, serialized with the Sequencer's own
        // commits (the slot loop is sequential — no commit can race
        // this) and with the Deriver (reconcile lock). By now the
        // failed Sync block has either committed (head ≥ height →
        // reorg it out) or permanently didn't (stale-parent bail —
        // nothing to roll back).
        if self.inner.cc_exec_ctx.is_some() {
            if let Some(failed) = rollup.optimistic.take_failed_for_recovery(cursor) {
                return self.recover_failed_batch(rollup_id, rollup, failed).await;
            }
        }

        let blocked =
            self.inner.cc_exec_ctx.is_some() && rollup.optimistic.blocking_height(cursor).is_some();
        if blocked {
            event!(
                name: "eez.composer.sync_slot.bundle_in_flight",
                Level::INFO,
                rollup_id,
                cursor,
                parent_number,
                "previous postBatch unresolved; committing Sync block without emission this slot",
            );
            return match build_sync_block(
                rollup.l2_provider.as_ref(),
                &self.inner.evm_config,
                &parent_header,
                timestamp,
                suggested_fee_recipient,
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
                        rollup_id,
                        error = %err,
                        "empty Sync block build failed; Sequencer's commit_one fallback takes over",
                    );
                    None
                }
            };
        }

        let pool_len_before = pool.len();
        // Cap drain to 3 user_txs per bundle. rbuilder-chiado has shown
        // partial-inclusion when bundles carry more than ~3 user_txs:
        // postBatch lands, but only a prefix of the user_txs makes it
        // into the block — the rest are silently excluded by rbuilder
        // and effectively lost. Capping keeps every bundle's contents
        // 100% atomic; a backlog spills into the next Sync slot.
        const MAX_USER_TXS_PER_BUNDLE: usize = 3;
        let drained = pool.pop_n(MAX_USER_TXS_PER_BUNDLE);
        // NOTE: do NOT early-exit on empty pool. Every unblocked Sync
        // slot still emits a postBatch carrying the leading immediate
        // entry (which advances L1's stored stateRoot to the L2
        // stateRoot at sync_block - 1). Without this, L1's view of the
        // rollup state stops advancing while the composer keeps
        // producing L2 blocks — the chains diverge in time. An
        // "empty-pool" postBatch has ZERO deferred entries and no
        // user_txs bundled with it; the bundle is just `[postBatch]`.
        let drained_count = drained.len();
        let _ = pool_len_before;

        // If the EvmComposer + exec ctx are wired, route each held L1
        // raw_tx through `simulate_and_resolve` to detect cross-chain
        // proxy calls. The cross-chain path also builds the Sync block
        // locally (via reth-evm) and stamps that block's state root
        // into each postBatch's stateDelta.newState before sending —
        // so the deriver's `check_claimed_state` validates against the
        // same root reth will produce when it ingests the payload.
        //
        // Cross-chain path returns the already-built Sync block so we
        // don't redo the work here. Standalone / no-L1 path falls back
        // to constructing an empty Sync block from the drained raw_txs.
        if let Some(ctx) = self.inner.cc_exec_ctx.as_ref() {
            // Cross-chain mode is authoritative: `compose_via_evm_composer`
            // builds the Sync block, registers the drained txs in the
            // optimistic ledger, spawns the bundle observer, and
            // returns the block for IMMEDIATE commit — L1 settlement
            // is observed in the background and reconciled
            // retroactively (re-push + reorg on failure). Do NOT fall
            // through to the `build_sync_block` branch below —
            // `drained` are L1 user txs (type-0x2 EOA calls targeting
            // CCM-L1), not L2 system txs.
            if let Some(evm_composer) = self.inner.evm_composer.as_ref() {
                return match self
                    .compose_via_evm_composer(
                        evm_composer,
                        ctx,
                        rollup_id,
                        drained,
                        &parent_header,
                        timestamp,
                        suggested_fee_recipient,
                    )
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
                        event!(
                            name: "eez.composer.cc_compose.failed",
                            Level::ERROR,
                            rollup_id,
                            error = %err,
                            "cross-chain compose failed; Sequencer will commit an empty Sync block via fallback",
                        );
                        None
                    }
                };
            }

            if !drained.is_empty() {
                event!(
                    name: "eez.composer.sync_slot.no_evm_composer_held_txs",
                    Level::WARN,
                    rollup_id,
                    tx_count = drained_count,
                    "held L1 txs require EvmComposer; re-queueing and emitting minimal postBatch",
                );
                if let Some(pool) = rollup.held_pool.as_ref() {
                    pool.push_front_batch(drained);
                }
            }

            return match self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    &parent_header,
                    timestamp,
                    suggested_fee_recipient,
                )
                .await
            {
                Ok(Some(built)) => {
                    event!(
                        name: "eez.composer.sync_slot.built_minimal",
                        Level::INFO,
                        rollup_id,
                        parent_number,
                        timestamp,
                        "built Sync block and dispatched minimal postBatch",
                    );
                    Some(built)
                }
                Ok(None) => None,
                Err(err) => {
                    event!(
                        name: "eez.composer.phase1.failed",
                        Level::ERROR,
                        rollup_id,
                        error = %err,
                        "minimal postBatch dispatch failed; Sequencer will commit an empty Sync block via fallback",
                    );
                    None
                }
            };
        }

        let drained_raw_txs: Vec<Bytes> = drained.iter().map(|h| h.raw_tx.clone()).collect();
        match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            &parent_header,
            timestamp,
            suggested_fee_recipient,
            &drained_raw_txs,
        ) {
            Ok(built) => {
                event!(
                    name: "eez.composer.sync_slot.built",
                    Level::INFO,
                    rollup_id,
                    tx_count = drained_count,
                    parent_number,
                    timestamp,
                    "built Sync block (fallback) carrying {{tx_count}} held tx(s)",
                );
                Some(SyncSlotBlock {
                    payload: built.payload,
                    header: built.header,
                })
            }
            Err(err) => {
                event!(
                    name: "eez.composer.sync_slot.build_failed",
                    Level::ERROR,
                    rollup_id,
                    tx_count = drained_count,
                    error = %err,
                    "build_sync_block failed; dropping held txs and falling back to vanilla Sync block",
                );
                None
            }
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
    /// Recover a failed optimistic batch (slot context): reorg the L2
    /// head to the failed Sync block's parent if the block actually
    /// landed, then re-push its user_txs (skipping burned nonces).
    /// Returns `None` so the slot yields — the Sequencer's stale-parent
    /// bail + next trigger's `Catchup` rebuild the chain with correct
    /// (+l2_block_time) timestamps, which a same-slot rebuild on the
    /// retreated parent could not honor.
    async fn recover_failed_batch(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        failed: crate::optimistic::FailedBatch,
    ) -> Option<SyncSlotBlock> {
        let sync_height = failed.sync_height;
        let Some(committer) = self.inner.committer.get() else {
            event!(
                name: "eez.composer.recovery.no_committer",
                Level::ERROR,
                rollup_id,
                sync_height,
                "committer handle not wired; cannot recover failed batch",
            );
            rollup.optimistic.reinsert_failed(failed);
            return None;
        };
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
                event!(
                    name: "eez.composer.recovery.stale_verdict",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    cursor = rollup.l1_head.cursor(),
                    "failure verdict is stale — Deriver confirmed the batch from L1; dropping recovery",
                );
                return None;
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
                    return None;
                }
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
        }
        // Re-push user_txs whose nonce survived — to the FRONT of the
        // pool, ahead of anything submitted since, so user ordering is
        // preserved across retries. An included-but-reverted tx has a
        // burned nonce — re-bundling it would poison the next bundle's
        // simulation.
        if let Some(pool) = rollup.held_pool.as_ref() {
            let submitter = &self.inner.submitter;
            let mut keep: Vec<crate::HeldTx> = Vec::with_capacity(failed.txs.len());
            let mut dropped = 0usize;
            let mut evicted_chains: Vec<(alloy_primitives::Address, u64)> = Vec::new();
            for mut tx in failed.txs {
                if let Ok(true) = submitter.receipt_exists(tx.hash).await {
                    dropped += 1;
                    event!(
                        name: "eez.composer.recovery.nonce_burned",
                        Level::WARN,
                        rollup_id,
                        tx_hash = %tx.hash,
                        "user_tx already has an L1 receipt; not re-queueing (user must resubmit)",
                    );
                } else {
                    // A relay drop did NOT burn the nonce (the tx
                    // never executed), so re-queue for a fresh
                    // attempt. Poison is normally caught at compose
                    // time; this bounded retry only backstops poison
                    // the compose-time sim missed (rbuilder sims
                    // against a slightly different post-postBatch
                    // state). After MAX_BUNDLE_ATTEMPTS consecutive
                    // drops, evict loudly (with the nonce-cascade) so
                    // a residual poison tx can't block the FIFO queue
                    // forever. User resubmits.
                    tx.attempts += 1;
                    if tx.attempts >= MAX_BUNDLE_ATTEMPTS {
                        dropped += 1;
                        evicted_chains.push((tx.sender, tx.nonce));
                        event!(
                            name: "eez.composer.recovery.poison_evicted",
                            Level::WARN,
                            rollup_id,
                            tx_hash = %tx.hash,
                            sender = %tx.sender,
                            nonce = tx.nonce,
                            attempts = tx.attempts,
                            "user_tx evicted after MAX_BUNDLE_ATTEMPTS relay drops (likely poison the compose-time sim missed); resubmit required",
                        );
                    } else {
                        keep.push(tx);
                    }
                }
            }
            // Nonce-chain cascade: evicting (sender, N) makes every
            // same-sender tx with nonce > N permanently invalid (the
            // gap never fills — the evicted nonce only lands if the
            // user resubmits, which produces a NEW tx). Leaving them
            // queued poisons every future bundle they ride and breaks
            // OTHER senders' chains as collateral — the exact cascade
            // that bricked a run's user EOA. Drop them from both the
            // keep list and the pool, loudly.
            for (sender, nonce) in &evicted_chains {
                keep.retain(|t| {
                    let cascade = t.sender == *sender && t.nonce > *nonce;
                    if cascade {
                        dropped += 1;
                        event!(
                            name: "eez.composer.recovery.nonce_chain_evicted",
                            Level::WARN,
                            rollup_id,
                            tx_hash = %t.hash,
                            sender = %t.sender,
                            nonce = t.nonce,
                            gap_at = nonce,
                            "same-sender tx above an evicted nonce; gapped chain can never land — evicted (resubmit in order)",
                        );
                    }
                    !cascade
                });
                for t in pool.drain_sender_above(*sender, *nonce) {
                    dropped += 1;
                    event!(
                        name: "eez.composer.recovery.nonce_chain_evicted",
                        Level::WARN,
                        rollup_id,
                        tx_hash = %t.hash,
                        sender = %t.sender,
                        nonce = t.nonce,
                        gap_at = nonce,
                        "same-sender pooled tx above an evicted nonce; gapped chain can never land — evicted (resubmit in order)",
                    );
                }
            }
            let re_pushed = keep.len();
            pool.push_front_batch(keep);
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
        None
    }

    /// For each drained held tx, call `EvmComposer::simulate_and_resolve`
    /// + wrap the composer-produced calldata pairs into signed L2
    /// system txs targeting CCM-L2, build the Sync block, register the
    /// drained txs in the optimistic ledger, spawn the bundle-observer
    /// task, and return the block for immediate L2 commit.
    ///
    /// Reference shape — `sync-rollups-composer` exposes the
    /// composition's payloads via RPC and lets the caller wrap them
    /// (`crates/based-rollup/src/composer_rpc/l1_to_l2/...`); we do
    /// the wrapping internally because our composer + bundler live in
    /// the same process.
    async fn compose_via_evm_composer(
        &self,
        evm_composer: &eez_evm_inspector::EvmComposer,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        drained: Vec<HeldTx>,
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        timestamp: u64,
        suggested_fee_recipient: Address,
    ) -> Result<Option<SyncSlotBlock>, String> {
        // Read SYSTEM_ADDRESS nonce at the parent state — the next
        // signed system tx must use this. We sign multiple txs per
        // composition (load_table + execute per target), so the
        // local `nonce` counter advances per-signed-tx within this
        // function.
        let parent_hash = parent_header.hash();
        let rollup =
            self.inner.rollups.get(&rollup_id).ok_or_else(|| {
                format!("unknown rollup_id {rollup_id} in compose_via_evm_composer")
            })?;
        let state = rollup
            .l2_provider
            .state_by_block_hash(parent_hash)
            .map_err(|e| format!("state_by_block_hash({parent_hash}): {e}"))?;
        let system_address = ctx.system_signer.address();
        let mut nonce = state
            .account_nonce(&system_address)
            .map_err(|e| format!("account_nonce({system_address}): {e}"))?
            .unwrap_or(0);

        let stf_cfg = eez_evm::system_tx::SystemTxContext {
            system_signer: ctx.system_signer.clone(),
            ccm_l2_address: ctx.ccm_l2_address,
            l2_chain_id: ctx.l2_chain_id,
            l2_gas_price: ctx.l2_gas_price,
            l2_gas_limit: ctx.l2_gas_limit,
            this_rollup_id: rollup_id,
        };

        // ─── Optimistic two-phase compose ────────────────────────────
        // The Sync block commits to L2 immediately; the L1 bundle is
        // observed by a background task (see [`crate::optimistic`]).
        // Two paths, both keeping the Sync block as the range's LAST
        // block:
        // - **Rich (Phase 2):** non-empty drain + every build step
        //   succeeds → bundle `[postBatch_with_entries, user_tx_1, …]`,
        //   rich Sync block carrying the cross-chain system txs.
        // - **Minimal (Phase 1):** empty drain, or a Phase 2 build
        //   failure (user_txs re-queued) → bundle `[postBatch]` with the
        //   leading immediate only, empty Sync block — keeps L1's
        //   recorded stateRoot tracking L2.

        // ── Per-tx compose with poison isolation ─────────────────────
        // Simulate each held tx independently:
        //   - Ok                        → survivor (gets bundled).
        //   - deterministic sim failure → POISON (e.g. a wrong-proxy tx
        //     → EmptyCalls, or a revert): evict it here (+ nonce-cascade)
        //     so it can never freeze the pool, and keep composing the
        //     survivors.
        //   - transient sim failure     → abort the slot: re-queue
        //     everything, degrade to a minimal postBatch, retry next.
        // Only sim-clean survivors are bundled, so any later bundle DROP
        // is relay bad luck rather than poison.
        if drained.is_empty() {
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                )
                .await;
        }

        let mut survivors: Vec<HeldTx> = Vec::with_capacity(drained.len());
        let mut survivor_comps: Vec<eez_protocol::Composition<eez_evm::EvmProtocol>> =
            Vec::with_capacity(drained.len());
        let mut system_txs: Vec<Bytes> = Vec::with_capacity(drained.len() * 2);
        let mut poison: Vec<HeldTx> = Vec::new();
        // On a transient failure we abort the slot; this holds the error
        // string + the txs still needing re-queue (the failing tx + the
        // unprocessed remainder; survivors are added below).
        let mut transient: Option<(String, Vec<HeldTx>)> = None;

        let mut iter = drained.into_iter().enumerate();
        while let Some((idx, held)) = iter.next() {
            match evm_composer
                .simulate_and_resolve(held.raw_tx.as_ref())
                .await
            {
                Ok(composition) => {
                    let target_entries: Vec<_> = composition
                        .targets
                        .iter()
                        .flat_map(|t| t.batch.entries().iter().cloned())
                        .collect();
                    match eez_evm::system_tx::build_inbound_system_txs(
                        &target_entries,
                        &stf_cfg,
                        nonce,
                    ) {
                        Ok(inbound_txs) => {
                            let target_count = inbound_txs.len();
                            nonce = nonce.saturating_add(target_count as u64);
                            system_txs.extend(inbound_txs);
                            event!(
                                name: "eez.composer.cc_compose.tx",
                                Level::INFO,
                                rollup_id,
                                tx_idx = idx,
                                target_count,
                                "simulate_and_resolve produced {{target_count}} target(s) for held tx #{{tx_idx}}",
                            );
                            survivor_comps.push(composition);
                            survivors.push(held);
                        }
                        Err(e) => {
                            // System-tx signing/encoding failure — not
                            // this tx's fault. Abort the slot; retry next.
                            let mut rest = vec![held];
                            rest.extend(iter.by_ref().map(|(_, h)| h));
                            transient =
                                Some((format!("build_inbound_system_txs tx#{idx}: {e}"), rest));
                            break;
                        }
                    }
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
                    poison.push(held);
                }
                Err(e) => {
                    // Transient (provider / transport / unavailable) —
                    // abort the slot, re-queue this tx + the remainder.
                    let mut rest = vec![held];
                    rest.extend(iter.by_ref().map(|(_, h)| h));
                    transient = Some((format!("simulate_and_resolve tx#{idx}: {e}"), rest));
                    break;
                }
            }
        }

        // Evict the poison txs' gapped higher nonces from the pool — once
        // a sender's nonce N is evicted, N+1.. can never land.
        if let Some(pool) = rollup.held_pool.as_ref() {
            for tx in &poison {
                for t in pool.drain_sender_above(tx.sender, tx.nonce) {
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
        }

        // ── Transient abort: re-queue survivors + remainder, minimal. ──
        if let Some((err, rest)) = transient {
            event!(
                name: "eez.composer.phase2.transient",
                Level::WARN,
                rollup_id,
                error = %err,
                survivors = survivors.len(),
                "transient compose failure; re-queueing and degrading to minimal postBatch this slot",
            );
            if let Some(pool) = rollup.held_pool.as_ref() {
                let mut requeue = survivors;
                requeue.extend(rest);
                pool.push_front_batch(requeue);
            }
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
                )
                .await;
        }

        // Every held tx was poison (evicted) → nothing to compose. Still
        // emit a minimal postBatch so L1 keeps tracking L2's progression.
        if survivors.is_empty() {
            event!(
                name: "eez.composer.phase2.all_poison",
                Level::WARN,
                rollup_id,
                evicted = poison.len(),
                "all held txs failed simulation deterministically; emitting minimal postBatch",
            );
            return self
                .dispatch_minimal_postbatch(
                    ctx,
                    rollup_id,
                    rollup,
                    parent_header,
                    timestamp,
                    suggested_fee_recipient,
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
            &system_txs,
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
                if let Some(pool) = rollup.held_pool.as_ref() {
                    pool.push_front_batch(survivors);
                }
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                    )
                    .await;
            }
        };
        let comp_refs: Vec<&eez_protocol::Composition<eez_evm::EvmProtocol>> =
            survivor_comps.iter().collect();
        let postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &comp_refs,
                parent_header,
                built.header.state_root(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                event!(
                    name: "eez.composer.phase2.prepare_failed",
                    Level::WARN,
                    rollup_id,
                    error = %e,
                    "prepare_post_batch_raw failed; re-queueing survivors, degrading to minimal postBatch",
                );
                if let Some(pool) = rollup.held_pool.as_ref() {
                    pool.push_front_batch(survivors);
                }
                return self
                    .dispatch_minimal_postbatch(
                        ctx,
                        rollup_id,
                        rollup,
                        parent_header,
                        timestamp,
                        suggested_fee_recipient,
                    )
                    .await;
            }
        };
        let total_entries: usize = comp_refs
            .iter()
            .map(|c| c.source.batch.entries().len())
            .sum();

        // ── Dispatch: rich bundle [postBatch, ...survivors], commit. ──
        let sync_height = built.header.number();
        // keccak of the raw EIP-2718 envelope IS the typed tx's hash —
        // recorded in the ledger so the finality audit can look up the
        // postBatch receipt.
        let post_batch_hash = alloy_primitives::keccak256(&postbatch_raw);
        let mut bundle: Vec<Bytes> = Vec::with_capacity(1 + survivors.len());
        bundle.push(postbatch_raw);
        bundle.extend(survivors.iter().map(|h| h.raw_tx.clone()));
        event!(
            name: "eez.composer.bundle.dispatched",
            Level::INFO,
            rollup_id,
            sync_height,
            tx_count = bundle.len(),
            entry_count = total_entries,
            evicted_poison = poison.len(),
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
        );
        Ok(Some(SyncSlotBlock {
            payload: built.payload,
            header: built.header,
        }))
    }

    /// Phase 1: build an empty Sync block, sign a leading-immediate-only
    /// postBatch covering `posted+1..=sync_height`, dispatch it to the
    /// background observer, return the block for immediate commit. The
    /// Sync block stays the LAST block of the batch range.
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
    ) -> Result<Option<SyncSlotBlock>, String> {
        let empty_built = build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &[], // no system_txs — empty Sync block
        )
        .map_err(|e| format!("build_sync_block (empty): {e}"))?;

        let minimal_postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &[], // no compositions → leading immediate only
                parent_header,
                empty_built.header.state_root(),
            )
            .await
        {
            Ok(raw) => raw,
            Err(err) => {
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
        );
        Ok(Some(SyncSlotBlock {
            payload: empty_built.payload,
            header: empty_built.header,
        }))
    }

    /// Spawn the background bundle-observer task. The observer only
    /// records the verdict in the ledger — all chain mutations happen
    /// in slot context (`recover_failed_batch`), so the task captures
    /// nothing but the submitter, the ledger, and the expected final
    /// state root.
    fn spawn_bundle_observer(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        sync_height: u64,
        bundle: Vec<Bytes>,
        expected_final_state: B256,
        optimistic: Arc<OptimisticallyIncluded>,
    ) {
        let submitter = ctx.submitter.clone();
        tokio::spawn(observe_bundle_outcome(
            rollup_id,
            sync_height,
            bundle,
            expected_final_state,
            optimistic,
            submitter,
        ));
    }

    /// Build + sign the L1 `postBatch` raw tx for a Sync slot's
    /// compositions, covering blocks `posted+1..=parent+1` (intermediate
    /// blocks + the new Sync block at `parent+1`, always the range's last
    /// block). Returns EIP-2718 bytes so the caller can bundle the tx
    /// with the N user_tx forwards.
    ///
    /// EEZ.sol's `lastVerifiedBlock` guard (lines 65-70) allows at most
    /// one `postAndVerifyBatch` per rollupId per L1 block, so every entry
    /// drained in one Sync slot merges into ONE batch: `entries[]` and
    /// `l1ToL2lookupCalls[]` concatenated in submission order (FIFO-
    /// matching the deferred-consumption queue), `transientExecutionEntryCount`
    /// summed. A single `self.inner.prover` proof covers the merged batch.
    ///
    /// **Chained stateDeltas** (Rollup-1 §1 invariant 6): this function
    /// stitches the merged entries so `entries[k].currentState ==
    /// entries[k-1].newState` per rollup; EEZ.sol's `_applyStateDeltas`
    /// enforces the chain on L1 (`StateRootMismatch` revert) regardless of
    /// proof system. `sync_block_state_root` (the locally-built Sync
    /// block's actual root — rich for Phase 2, empty for Phase 1) anchors
    /// the last entry's `newState` for our rollup.
    ///
    /// # Errors
    ///
    /// `String` error if `compositions` is empty, any composition has no
    /// entries, or the prover / fee estimation / signing fails.
    async fn prepare_post_batch_raw(
        &self,
        ctx: &CrossChainExecCtx,
        rollup_id: u64,
        compositions: &[&eez_protocol::Composition<eez_evm::EvmProtocol>],
        parent_header: &reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
        sync_block_state_root: B256,
    ) -> Result<Bytes, String> {
        use alloy_sol_types::SolCall;
        use eez_evm::types::{RollupIdWithProofSystemsSol, postAndVerifyBatchCall};

        // Empty compositions is a VALID case: an empty HeldPool Sync
        // slot still emits a postBatch carrying just the leading
        // immediate entry so L1's stored stateRoot tracks the L2
        // progression. We build the batch from scratch (no per-tx
        // batch to merge from); only the leading immediate entry +
        // proof-system metadata go in.

        let proof = self
            .inner
            .prover
            .prove(eez_prover::ProvingContext)
            .await
            .map_err(|e| format!("prover.prove: {e}"))?;

        // Take the first composition's batch as the template, then
        // merge every later composition's entries + lookupCalls into
        // it. Empty compositions → build a fresh empty batch shell;
        // the leading immediate entry below is the entire payload.
        let mut batch = if compositions.is_empty() {
            eez_evm::EvmBatch::default()
        } else {
            let mut b = compositions[0].source.batch.clone();
            for c in &compositions[1..] {
                b.inner
                    .entries
                    .extend(c.source.batch.entries().iter().cloned());
                b.inner
                    .l1ToL2lookupCalls
                    .extend(c.source.batch.inner.l1ToL2lookupCalls.iter().cloned());
            }
            b
        };

        // Prepend ONE leading immediate entry (`proxyEntryHash == 0`)
        // covering all L2 effects before the sync block — EEZ.sol drains
        // it inline during postAndVerifyBatch, applying its stateDelta
        // against L1's recorded root.
        //
        // `currentState` = L2.stateRoot(posted) (the L1-confirmed cursor)
        // — must equal L1.config.stateRoot at postBatch time so the
        // deriver's check_claimed_state agrees. `newState` = L2 at
        // sync_block-1 (`parent_header.state_root()`), lumping every
        // pre-sync block's effects into one stateDelta.
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
        let rollup_id_u256 = U256::from(rollup_id);
        let immediate_entry = eez_evm::types::ExecutionEntrySol {
            stateDeltas: vec![eez_evm::types::StateDeltaSol {
                rollupId: rollup_id_u256,
                currentState: pre_state_root,
                newState: pre_sync_state_root,
                etherDelta: alloy_primitives::I256::ZERO,
            }],
            proxyEntryHash: B256::ZERO,
            destinationRollupId: rollup_id_u256,
            L2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            callCount: U256::ZERO,
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        };
        batch.inner.entries.insert(0, immediate_entry);

        // Stitch the per-rollup stateDelta chain across all entries
        // (leading immediate + N deferred cross-chain entries).
        // EEZ.sol's `_applyStateDeltas` enforces
        // `config.stateRoot == delta.currentState` entry-by-entry
        // and then sets `config.stateRoot = delta.newState`. The
        // running-root map preserves each rollup's first
        // `currentState` (= the chain anchor) and chains subsequent
        // entries to the prior delta's `newState`.
        let mut running_roots: HashMap<U256, B256> = HashMap::new();
        for entry in &mut batch.inner.entries {
            for delta in &mut entry.stateDeltas {
                if let Some(prev_new) = running_roots.get(&delta.rollupId).copied() {
                    delta.currentState = prev_new;
                }
                running_roots.insert(delta.rollupId, delta.newState);
            }
        }

        // Anchor the LAST entry's `newState` for OUR rollup to the
        // locally-built Sync block's actual root (`sync_block_state_root`).
        // Intermediate deferred entries keep their simulated newStates —
        // internally consistent (each curr = prior new); only the final
        // newState matters externally, so once every deferred entry
        // consumes, L1's stored stateRoot lands on L2's actual root.
        if let Some(last_entry) = batch.inner.entries.last_mut() {
            for delta in last_entry.stateDeltas.iter_mut().rev() {
                if delta.rollupId == rollup_id_u256 {
                    delta.newState = sync_block_state_root;
                    break;
                }
            }
        }

        // transientExecutionEntryCount = 1 — only the leading immediate
        // entry should be drained inline at EEZ.sol:386. The remaining
        // cross-chain entries have proxyEntryHash != 0 → queue for
        // deferred consumption via executeCrossChainCall.
        batch.inner.transientExecutionEntryCount = U256::from(1);
        batch.inner.proofSystems = vec![ctx.mock_proof_system_address];
        batch.inner.rollupIdsWithProofSystems = vec![RollupIdWithProofSystemsSol {
            rollupId: U256::from(ctx.l2_rollup_id),
            proofSystemIndex: vec![0u64],
        }];
        batch.inner.proofs = vec![proof];
        // Encode the full L2 block range this batch covers, not just the
        // Sync block: the composer accumulates K-1 intermediate Live
        // blocks between Sync slots (Rollup-1 §1.3) and the deriver must
        // replay all of them. Range: from = cursor+1 (first unposted L2
        // block), to = parent+1 (the Sync block).
        //
        // Intermediate blocks [from..to-1] are walked via parent-hash
        // from `parent_header` — `block_by_hash` is reliable for
        // canonical blocks, unlike `block_by_number`, which races reth's
        // provider-index for the newest block. The Sync block (to) is
        // empty per Rollup-1 §8.3 — its system tx is reconstructed by the
        // deriver from the postBatch entries, not carried in callData.
        let rollup =
            self.inner.rollups.get(&rollup_id).ok_or_else(|| {
                format!("unknown rollup_id {rollup_id} in prepare_post_batch_raw")
            })?;
        // Reuse the SAME cursor read that anchored the leading
        // immediate's currentState above — a second read could race the
        // Deriver's cursor advance and desync the callData range from
        // the stateDelta anchor (TOCTOU).
        let from = posted + 1;
        let sync_block_number = parent_header.number() + 1;
        if sync_block_number < from {
            return Err(format!(
                "sync block {sync_block_number} <= L1-confirmed cursor {posted}; \
                 composer is behind its own posted batches"
            ));
        }
        // The one line that answers "why did this postBatch fail" —
        // grep `postbatch.anchors` and compare `current_state` to L1's
        // stored root (`rollups(rid).stateRoot`) at submission time:
        // a mismatch is EEZ.sol's StateRootMismatch / 0x78cb4214.
        event!(
            name: "eez.composer.postbatch.anchors",
            Level::INFO,
            rollup_id,
            posted,
            from,
            sync_block_number,
            current_state = %pre_state_root,
            claimed_final = %sync_block_state_root,
            "postBatch anchors: currentState at cursor, claimed final at Sync block",
        );
        let span_len = usize::try_from(sync_block_number - from + 1)
            .map_err(|e| format!("batch span overflow: {e}"))?;
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
            // Refuse-to-emit guard (invariant 7): intermediates carry
            // ONLY user txs (system txs live exclusively in the Sync
            // block, reconstructed deriver-side — Rollup-1 §8). A
            // failed-but-not-yet-recovered optimistic Sync block in this
            // range still holds its system txs; serializing it here would
            // launder phantom cross-chain effects into L1-accepted
            // history. Detect both framings (type-0x7E per Rollup-1 §5.3,
            // and the SYSTEM_ADDRESS legacy framing) and degrade — the
            // slot commits without emission.
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
                        "intermediate block {cursor_number} carries type-0x7E system txs — \
                         un-recovered failed Sync block in range; emission blocked until \
                         recovery (invariant 7)"
                    ));
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
        // Sync block entry: empty per Rollup-1 §8.3 — system tx is
        // reconstructed by the deriver from the postBatch's entries,
        // not transported in callData. Holds for BOTH phases (empty
        // Phase 1 Sync block and rich Phase 2 Sync block).
        blocks.push(Vec::new());
        // L2-shape entries for system-tx reconstruction by external
        // followers. The L1 batch's `entries[]` carries the DEPOSIT-
        // shape entries (callCount=0, no L2ToL1Calls) for value-bearing
        // calls; those don't carry the inbound call params the L2
        // system tx needs. The L2-shape entries live in
        // `composition.targets[].batch` (built by
        // `protocol.build_batch(source=L2)`). We serialize each via
        // `SolValue::abi_encode` and ship them through codec v2 in
        // `batch.callData` — the contract treats callData as opaque
        // (only hashes it for proof binding, `EEZ.sol:596`), so this
        // is a follower-only DA channel.
        use alloy_sol_types::SolValue as _;
        let l2_entries_bytes: Vec<Vec<u8>> = compositions
            .iter()
            .flat_map(|c| c.targets.iter())
            .flat_map(|t| t.batch.entries().iter())
            .map(eez_evm::types::ExecutionEntrySol::abi_encode)
            .collect();
        let payload = eez_payload_codec::encode(&blocks, &l2_entries_bytes)
            .map_err(|e| format!("eez_payload_codec::encode: {e}"))?;
        batch.inner.callData = alloy_primitives::Bytes::from(payload);

        let calldata = postAndVerifyBatchCall {
            batch: batch.inner.clone(),
        }
        .abi_encode();

        // EEZ registry address is per-deployment; read directly from
        // env. Loud failure on absence/garbage (invariant 7) — a
        // postBatch signed to Address::ZERO would silently no-op on
        // L1 with nothing but WARN-level breadcrumbs.
        let eez_address = std::env::var("EEZ_REGISTRY_ADDRESS")
            .ok()
            .and_then(|s| s.parse::<Address>().ok())
            .ok_or("EEZ_REGISTRY_ADDRESS missing or not a valid address")?;

        sign_post_batch_tx(
            &ctx.l1_poster_signer,
            &ctx.l1_provider,
            eez_address,
            calldata,
            ctx.l1_chain_id,
            ctx.l1_post_batch_priority_fee,
        )
        .await
    }
}

/// Background bundle observer — verdict recording ONLY. Marks the ledger
/// entry Settled or Failed; never mutates chain state. The destructive
/// recovery for Failed entries runs at the next Sync slot
/// (`Composer::recover_failed_batch`), serialized with the Sequencer's
/// commits — closing the race where a fast failure verdict lands before
/// the Sync block's own commit.
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
) {
    let outcome = submitter
        .send_bundle(&bundle, BundleTarget::NextBlock, Some(expected_final_state))
        .await;
    // Under strict all-or-nothing bundles, `Included` ⟹ every tx
    // succeeded ⟹ settled; the only failure is a drop (postBatch had no
    // receipt by its target block), which can't distinguish relay bad
    // luck from a would-revert tx. Poison is caught at compose time
    // instead (see `compose_via_evm_composer`), so a drop reaching here
    // is treated as bad luck and re-queued by slot-context recovery.
    let settled = matches!(
        outcome,
        Ok(SendOutcome::Included {
            state_applied: true,
            ..
        })
    );
    match &outcome {
        Ok(o) => event!(
            name: "eez.composer.bundle.observed",
            Level::INFO,
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
        optimistic.mark_failed(sync_height);
    }
}

/// Sign an EIP-1559 L1 tx (used for the `postBatch` submission).
///
/// Sets `max_priority_fee_per_gas` from the caller (so we can order
/// the postBatch ahead of the held user_tx) and `max_fee_per_gas` to
/// `2 * base_fee + priority_fee` per the standard EIP-1559 formula.
///
/// # Errors
///
/// Returns a `String` error if RPC calls fail (chain id, nonce, base
/// fee, EIP-1559 fee estimation) or if signing fails.
async fn sign_post_batch_tx(
    signer: &alloy_signer_local::PrivateKeySigner,
    provider: &alloy_provider::RootProvider,
    eez_address: Address,
    calldata: Vec<u8>,
    chain_id: u64,
    priority_fee: u128,
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

    // Gas budget for postAndVerifyBatch: per-rollup verification +
    // entry apply. 10M is plenty for a single-PS single-entry batch
    // on the smoke; the dev chain's block gas limit is 30M.
    // 4M leaves enough headroom under chiado's 17M block gas limit for
    // the bundled user_txs to fit in the same block. Actual postBatch
    // gas usage is ~500K for our smoke's entry counts; 4M is ~8x
    // safety. Original 10M caused bundles to fail rbuilder's
    // "bundle fits in block" check.
    const POST_BATCH_GAS_LIMIT: u64 = 4_000_000;

    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: POST_BATCH_GAS_LIMIT,
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

// Legacy system-tx signing helpers (`sign_legacy_system_tx` /
// `_with_value`) moved into `eez_evm::system_tx` as part of the
// composer↔deriver single-source-of-truth STF refactor. Call
// `eez_evm::system_tx::build_inbound_system_txs(...)` from new code.
