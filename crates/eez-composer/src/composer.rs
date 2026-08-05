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
    BlockCommitterHandle, ParentContext, SyncSlotBlock, SyncSlotComposer, SyncSlotMode,
    witness::{ExecutionWitnessMode, block_witness},
};
use eez_l1::{BundleTarget, L1Event, L1Watcher, SendOutcome, Submitter};
use eez_prover::{BlockWitness, Prover};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{
    BlockReader, BlockSource, StateProvider, StateProviderFactory, TransactionsProvider,
};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::held_pool::HeldTx;
use crate::ingress::Direction;
use crate::local::{build_sync_block, sync_block_pair_roots};
use crate::optimistic::OptimisticallyIncluded;
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
    /// Default entry-chain client used for source transaction simulation.
    pub entry_client: Arc<dyn eez_protocol::executor::ChainClient + Send + Sync>,
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
    /// L2 ENTRY client for OUTBOUND (L2→L1) source simulation. An
    /// outbound tx originates on this L2, so its `simulate_and_resolve`
    /// must run against an L2 entry (the L2 follower's `ChainClient`
    /// errors `Unavailable` for source sim).
    pub l2_entry_client: Arc<dyn eez_protocol::executor::ChainClient + Send + Sync>,
}

impl CrossChainWiring {
    /// Detect cross-chain proxy calls and return the final
    /// [`eez_protocol::Composition`].
    ///
    /// # Errors
    ///
    /// Returns [`eez_protocol::ComposerErrorKind::Executor`] if
    /// simulation fails.
    /// Returns [`eez_protocol::ComposerErrorKind::Protocol`] if entry
    /// building or finalization fails.
    // Preserve the protocol crate's structured public error type.
    #[allow(clippy::result_large_err)]
    #[tracing::instrument(skip(self, raw_tx), fields(tx_len = raw_tx.len()))]
    pub fn simulate_and_resolve(
        &self,
        raw_tx: &[u8],
    ) -> eez_protocol::ComposerResult<eez_protocol::Composition> {
        // Use the configured entry rollup and client. Callers that select the
        // entry per transaction use `simulate_and_resolve_recorded_for`.
        self.simulate_and_resolve_recorded_for(self.entry_rollup_id, &*self.entry_client, raw_tx)
    }

    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve) but
    /// with an explicitly-chosen entry — `entry_id` + the `entry_client`
    /// that runs source simulation. The explicit entry lets one wiring
    /// compose either direction — `(L1, L1 client)` for an inbound L1→L2 call,
    /// `(L2, L2 client)` for an outbound L2→L1 call — picked per tx by
    /// the drain. The dispatch rollup map remains the wiring's full
    /// registration set.
    ///
    /// # Errors
    ///
    /// Same as [`simulate_and_resolve`](Self::simulate_and_resolve).
    // Preserve the protocol crate's structured public error type.
    #[allow(clippy::result_large_err)]
    pub fn simulate_and_resolve_recorded_for(
        &self,
        entry_id: eez_protocol::RollupId,
        entry_client: &(dyn eez_protocol::executor::ChainClient + Send + Sync),
        raw_tx: &[u8],
    ) -> eez_protocol::ComposerResult<eez_protocol::Composition> {
        use eez_protocol::composition::Rollup;

        tracing::info!(
            name: "composer.simulate.start",
            %entry_id,
            tx_len = raw_tx.len(),
            rollup_count = self.rollups.len(),
            "simulate_and_resolve: starting composition pipeline"
        );

        // Assemble registered clients and configs with lazy sessions.
        let mut rollups: HashMap<eez_protocol::RollupId, Rollup> =
            HashMap::with_capacity(self.rollups.len());
        for (rollup_id, (client, config)) in &self.rollups {
            rollups.insert(
                *rollup_id,
                Rollup {
                    client: Arc::clone(client),
                    session: None,
                    config: config.clone(),
                },
            );
        }

        // Drive source simulation, including proxy dispatch through the
        // builder, then materialize the composition's source and target batches.
        // Capture the count first because `finalize` consumes the builder.
        let mut builder = eez_protocol::CompositionBuilder::new(entry_id, rollups);
        entry_client
            .simulate_source_tx(raw_tx.to_vec(), &mut builder)
            .map_err(eez_protocol::CompositionError::from)?;
        let recorded_count = builder.recorded_count();
        let composition = builder.finalize()?;

        tracing::info!(
            name: "composer.simulate.complete",
            target_count = composition.targets.len(),
            recorded = recorded_count,
            "composition complete"
        );

        Ok(composition)
    }
}

impl std::fmt::Debug for CrossChainWiring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainWiring")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("rollups", &self.rollups.len())
            .finish()
    }
}

/// Relay-drop retries before a held user_tx is evicted as probable
/// poison. Poison is normally caught at COMPOSE time (a tx whose
/// `simulate_and_resolve` deterministically fails — e.g. a wrong-proxy
/// tx → `EmptyCalls`, or a revert — is evicted before it can enter a
/// bundle). An atomic-relay drop re-queues its transactions; this bound
/// backstops poison the compose-time simulation missed. After this many
/// consecutive drops, the transaction and its nonce-dependent suffix are
/// evicted so they cannot block the FIFO queue indefinitely.
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

/// Composer umbrella. Cheaply [`Clone`]able (`Arc<Inner>`).
#[derive(Clone)]
pub struct Composer<L2: BlockReader> {
    inner: Arc<Inner<L2>>,
}

struct Inner<L2: BlockReader> {
    /// Per-rollup state keyed by rollup ID.
    rollups: HashMap<u64, RollupState<L2>>,
    /// Shared across rollups: one prover, one submitter, one `L1Watcher`.
    prover: Arc<dyn Prover>,
    submitter: Submitter,
    l1_watcher: L1Watcher,
    /// EVM config — used by [`build_sync_block`] to construct the
    /// per-Sync-slot block via reth-evm `BlockBuilder`.
    evm_config: EthEvmConfig,
    /// Cross-chain clients and execution context. `None` when L1 is not wired,
    /// such as in standalone or follower modes.
    cross_chain: Option<CrossChainWiring>,
    /// Handle to the `BlockCommitter` actor (the sole engine-API
    /// owner). Set once at startup via [`Composer::set_committer`]
    /// after the Sequencer spawns the actor. Slot-context recovery uses it to
    /// reorg an optimistically committed Sync block after L1 failure.
    committer: std::sync::OnceLock<BlockCommitterHandle<EthEngineTypes>>,
    /// Per-block witnesses for [`eez_prover::ProvingContext::blocks`]. Set only
    /// in remote-prover mode; `None` (mock) leaves `blocks` empty.
    witness_source: std::sync::OnceLock<Arc<dyn eez_prover::ProvingWitnessSource>>,
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
        cross_chain: Option<CrossChainWiring>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                rollups,
                prover,
                submitter,
                l1_watcher,
                evm_config,
                cross_chain,
                committer: std::sync::OnceLock::new(),
                witness_source: std::sync::OnceLock::new(),
            }),
        }
    }

    /// Wire the proving-witness source (remote-prover mode only). Later calls no-op.
    pub fn set_witness_source(&self, src: Arc<dyn eez_prover::ProvingWitnessSource>) {
        let _ = self.inner.witness_source.set(src);
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
                        pool.push_front_batch(txs);
                    }
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
                                    if let Some(pool) = held_pool.as_ref() {
                                        pool.release_in_flight_batch(&txs);
                                    }
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
                                    if let Some(pool) = held_pool.as_ref() {
                                        pool.release_in_flight_batch(&txs);
                                    }
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
        let Some(pool) = rollup.held_pool.as_ref() else {
            event!(
                name: "eez.composer.sync_slot.no_pool",
                Level::WARN,
                rollup_id,
                "rollup has no HeldPool configured",
            );
            return None;
        };
        let Some(cc) = self.inner.cross_chain.as_ref() else {
            if !pool.is_empty() {
                event!(
                    name: "eez.composer.sync_slot.cross_chain_unavailable",
                    Level::ERROR,
                    rollup_id,
                    tx_count = pool.len(),
                    "cross-chain composition is unavailable; retaining held transactions",
                );
            }
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
            return self.recover_failed_batch(rollup_id, rollup, failed).await;
        }

        let blocked = rollup.optimistic.blocking_height(cursor).is_some();
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
        // Cap user transactions per submission. A smaller batch limits how many
        // transactions an atomic-relay drop re-queues. Overflow drains over
        // later slots; three is a default, not a builder limit.
        let max_user_txs = std::env::var("EEZ_MAX_USER_TXS_PER_BUNDLE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(3);
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
        match self
            .compose_cross_chain_batch(
                cc,
                rollup_id,
                drained.clone(),
                &parent_header,
                timestamp,
                suggested_fee_recipient,
                bundle_target,
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
            return None;
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
                if let Some(pool) = rollup.held_pool.as_ref() {
                    pool.release_in_flight_batch(&failed.txs);
                }
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
            let mut keep: Vec<crate::HeldTx> = Vec::with_capacity(failed.txs.len());
            let mut release: Vec<crate::HeldTx> = Vec::new();
            let mut dropped = 0usize;
            let mut evicted_chains: Vec<(alloy_primitives::Address, Direction, u64)> = Vec::new();
            // slot_skipped: re-queue without counting toward poison-eviction
            // (a skipped slot heals via a fresh pin next tick).
            let slot_skipped = failed.slot_skipped;
            for mut tx in failed.txs {
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
                } else if slot_skipped {
                    keep.push(tx);
                } else {
                    // A relay drop on a BUILT slot did NOT burn the nonce
                    // (the tx never executed), so re-queue for a fresh
                    // attempt. Poison is normally caught at compose
                    // time; this bounded retry only backstops poison
                    // the compose-time sim missed (rbuilder sims
                    // against a slightly different post-postBatch
                    // state). After MAX_BUNDLE_ATTEMPTS such drops,
                    // evict loudly (with the nonce-cascade) so a
                    // residual poison tx can't block the FIFO queue
                    // forever. User resubmits.
                    tx.attempts += 1;
                    if tx.attempts >= MAX_BUNDLE_ATTEMPTS {
                        dropped += 1;
                        release.push(tx.clone());
                        evicted_chains.push((tx.sender, tx.direction, tx.nonce));
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
            // Evict the same-sender nonce suffix with its root. Without the
            // missing nonce, later transactions cannot execute and would keep
            // invalidating batches that include them.
            for (sender, direction, nonce) in &evicted_chains {
                keep.retain(|t| {
                    // The root is already in `release`; remove only its suffix here.
                    let cascade =
                        t.sender == *sender && t.direction == *direction && t.nonce > *nonce;
                    if cascade {
                        dropped += 1;
                        release.push(t.clone());
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
                for t in pool.evict_chain_at_or_above(*sender, *direction, *nonce) {
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
            pool.release_in_flight_batch(&release);
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

    /// Simulate each drained transaction, construct canonical L2 system
    /// transactions, build the Sync block, register optimistic state, spawn L1
    /// submission observation, and return the block for immediate L2 commit.
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

        // ─── Optimistic two-phase compose ────────────────────────────
        // The Sync block commits to L2 immediately; the L1 bundle is
        // observed by a background task (see [`crate::optimistic`]).
        // Two paths keep the Sync block as the range's last
        // block:
        // - **Rich:** at least one transaction survives and every build step
        //   succeeds. The L1 bundle contains `postBatch` plus inbound survivors;
        //   the Sync block contains system transactions and outbound user halves.
        // - **Minimal:** the drain is empty, no transaction survives, or a rich
        //   build step fails. The L1 bundle contains only `postBatch` with the
        //   leading immediate entry, and the Sync block is empty.

        // ── Per-tx compose with poison isolation ─────────────────────
        // Simulate each held tx independently:
        //   - Ok                        → survivor, routed by direction.
        //   - deterministic sim failure → POISON (e.g. a wrong-proxy tx
        //     → EmptyCalls, or a revert): evict it here (+ nonce-cascade)
        //     so it can never freeze the pool, and keep composing the
        //     survivors.
        //   - transient sim failure     → abort the slot: re-queue
        //     everything, degrade to a minimal postBatch, retry next.
        // Only simulation-clean survivors reach the rich path. Recovery still
        // caps retries because relay-side simulation may observe different state.
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
        if let Some(pool) = rollup.held_pool.as_ref() {
            pool.release_in_flight_batch(&stale);
        }

        let mut survivors: Vec<HeldTx> = Vec::with_capacity(drained.len());
        // Inbound survivors' compositions (their `source.batch` = the L1
        // deferred entries) feed `prepare_post_batch_raw`'s merge.
        let mut survivor_comps: Vec<eez_protocol::Composition> = Vec::with_capacity(drained.len());
        // Staged, not built inline: system txs are built ONCE post-drain via the
        // canonical `build_cross_chain_sync_pairs` (matches the deriver).
        // `pending_out` pairs an outbound settlement entry with its user tx;
        // `pending_in` holds inbound target-side derivation entries; and
        // `outbound_entries` holds settlement entries spliced into `postBatch`.
        let mut pending_out: Vec<(eez_protocol::abi::ExecutionEntrySol, Bytes)> = Vec::new();
        let mut pending_in: Vec<eez_protocol::abi::ExecutionEntrySol> = Vec::new();
        let mut outbound_entries: Vec<eez_protocol::abi::ExecutionEntrySol> = Vec::new();
        // Escrow drawn down per outbound withdrawal (read once, lazily) so several
        // in one slot can't collectively over-drain. `None` = not yet read.
        let mut escrow_remaining: Option<U256> = None;
        let mut poison: Vec<HeldTx> = Vec::new();
        let mut poison_gaps: Vec<(Address, Direction, u64)> = Vec::new();
        // On a transient failure we abort the slot; this holds the error
        // string + the txs still needing re-queue (the failing tx + the
        // unprocessed remainder; survivors are added below).
        let mut transient: Option<(String, Vec<HeldTx>)> = None;

        let mut iter = drained.into_iter().enumerate();
        while let Some((idx, held)) = iter.next() {
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
            // ── OUTBOUND (L2→L1) arm. Source-sim runs against the L2 ENTRY client
            // (the L2 follower errors `Unavailable`). Stage each (settlement entry,
            // its user tx); the load tx is built post-drain by the canonical
            // builder. Zero entries → poison.
            if held.direction == Direction::Outbound {
                match cc.simulate_and_resolve_recorded_for(
                    eez_protocol::RollupId(rollup_id),
                    cc.l2_entry_client.as_ref(),
                    held.raw_tx.as_ref(),
                ) {
                    Ok(composition) => {
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
                        // `reject_multicall` covers multiple calls within one entry;
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
                        let Some(need) = eez_protocol::entries::outbound_ether_out(&l1_entries[0])
                        else {
                            event!(name: "eez.composer.cc_compose.outbound_ether_out_missing", Level::WARN, rollup_id, tx_idx = idx, tx_hash = %held.hash, "outbound tx is missing ether out entry, likely malformed; evicting");
                            push_poison_root(&mut poison, &mut poison_gaps, held);
                            continue;
                        };
                        if need > U256::ZERO {
                            if escrow_remaining.is_none() {
                                escrow_remaining =
                                    read_rollup_escrow(&ctx.l1_provider, rollup_id).await;
                            }
                            if let Some(avail) = escrow_remaining {
                                if need > avail {
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
                                escrow_remaining = Some(avail - need);
                            }
                        }
                        for oe in &l1_entries {
                            pending_out.push((oe.clone(), held.raw_tx.clone()));
                        }
                        outbound_entries.extend(l1_entries);
                        // NOT pushed to `survivor_comps`: its `source.batch` is OUR
                        // L2's entries (a dest=MAINNET call that must not settle on
                        // L1). The L1 settlement is `outbound_entries`, spliced
                        // separately with dest=rid.
                        survivors.push(held);
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
                        let mut rest = vec![held];
                        rest.extend(iter.by_ref().map(|(_, h)| h));
                        transient = Some((
                            format!("simulate_and_resolve_recorded_for tx#{idx}: {e}"),
                            rest,
                        ));
                        break;
                    }
                }
                continue;
            }

            // ── INBOUND (L1→L2) arm. Stage target-side derivation entries
            // for L2 delivery; system transactions follow all outbound loads.
            match cc.simulate_and_resolve(held.raw_tx.as_ref()) {
                Ok(composition) => {
                    let target_entries: Vec<_> = composition
                        .targets
                        .iter()
                        .flat_map(|t| t.batch.entries.iter().cloned())
                        .collect();
                    let target_count = target_entries.len();
                    pending_in.extend(target_entries);
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
                let mut cascade_evicted = Vec::new();
                requeue.retain(|tx| {
                    let Some(gap_at) = poison_gap_for(&poison_gaps, tx) else {
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

        // Every held tx was poison (evicted) → nothing to compose. Still
        // emit a minimal postBatch so L1 keeps tracking L2's progression.
        if survivors.is_empty() {
            let evicted = poison.len() + stale.len();
            event!(
                name: "eez.composer.phase2.all_poison",
                Level::WARN,
                rollup_id,
                evicted,
                "all held txs were stale or failed simulation deterministically; emitting minimal postBatch",
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

        // ── Build the Sync block's system txs via THE canonical builder —
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
                        bundle_target,
                    )
                    .await;
            }
        };

        // ── Build the rich Sync block + postBatch from survivors. A ──
        // ── build / prepare failure here is systemic (not one tx) →  ──
        // ── re-queue survivors and degrade to minimal.               ──
        let sync_txs = eez_protocol::system_tx::interleave_sync_block_txs(&pairs);
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
                        bundle_target,
                    )
                    .await;
            }
        };
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
                        bundle_target,
                    )
                    .await;
            }
        };
        let comp_refs: Vec<&eez_protocol::Composition> = survivor_comps.iter().collect();
        // Outbound user txs (the SyncPair user halves) travel in the sync-block
        // DA slot — the deriver can't reconstruct them from the postBatch entries
        // (only the system/load txs are). Empty for inbound-only.
        let outbound_user_txs: Vec<Bytes> =
            pairs.iter().filter_map(|p| p.user_tx.clone()).collect();
        let postbatch_raw = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &comp_refs,
                parent_header,
                built.header.state_root(),
                &built.block,
                &pair_roots,
                &outbound_entries,
                &outbound_user_txs,
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
                &empty_built.block,
                &[], // no cross-chain effects → no per-effect roots
                &[], // no outbound entries
                &[], // no outbound user txs
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
            bundle_target,
        );
        Ok(Some(SyncSlotBlock {
            payload: empty_built.payload,
            header: empty_built.header,
        }))
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
        sync_block: &reth_primitives_traits::RecoveredBlock<reth_ethereum_primitives::Block>,
        pair_roots: &[B256],
        outbound_entries: &[eez_protocol::abi::ExecutionEntrySol],
        outbound_user_txs: &[Bytes],
    ) -> Result<Bytes, String> {
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
            ));
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
        // invariant here, including when a mock prover is configured.
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
            ));
        }
        // Log both settlement anchors. `StateRootMismatch` means
        // `current_state` disagreed with L1's registered root at submission.
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

        // Prove the assembled window (proofs[] empty — not part of the
        // publicInputsHash). Mock ignores the context; a remote prover re-executes
        // `blocks`. Settlement path, off block production.
        let block_witnesses = match self.inner.witness_source.get() {
            // Remote-prover mode. Intermediate blocks `[from..sync)` are committed
            // (served by the witness store); the just-built endpoint isn't, so
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
                let terminal_block = sync_block.clone();
                tokio::task::spawn_blocking(move || -> Result<Vec<BlockWitness>, String> {
                    let mut ws = (from..sync_block_number)
                        .map(|n| src.block_witness(n))
                        .collect::<Result<Vec<_>, String>>()
                        .map_err(|e| format!("witness_source: {e}"))?;
                    // Endpoint (the just-built, uncommitted Sync block) is captured
                    // in-memory — no store or provider can serve an uncommitted block.
                    ws.push(
                        block_witness(
                            l2_provider.as_ref(),
                            &evm_config,
                            &terminal_block,
                            ExecutionWitnessMode::Legacy,
                        )
                        .map_err(|e| {
                            format!(
                                "terminal-block witness (block {}): {e}",
                                terminal_block.header().number()
                            )
                        })?,
                    );
                    Ok(ws)
                })
                .await
                .map_err(|e| format!("witness spawn_blocking join: {e}"))??
            }
            // Mock mode: the mock prover ignores per-block witnesses.
            None => Vec::new(),
        };
        let proving_ctx = eez_prover::ProvingContext {
            rollup_id,
            from_block: from,
            to_block: sync_block_number,
            batch: batch.clone(),
            blocks: block_witnesses,
            l1_block_hash: None, // timeless batch (blockNumber 0)
        };
        let proof = self
            .inner
            .prover
            .prove(proving_ctx)
            .await
            .map_err(|e| format!("prover.prove: {e}"))?;
        batch.proofs = vec![proof];

        let calldata = postAndVerifyBatchCall {
            batch: batch.clone(),
        }
        .abi_encode();

        // Read the deployment-specific EEZ registry address from the
        // environment and reject missing or malformed values.
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
        // slot_skipped = pinned slot didn't land as pinned (block ts != pin, or
        // unreadable) → not the tx's fault; requeue without poison-eviction.
        let slot_skipped = match target {
            BundleTarget::Exact { block, timestamp } => {
                submitter.block_timestamp(block).await.ok().flatten() != Some(timestamp)
            }
            BundleTarget::NextBlock => false,
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

    // The fixed four-million-gas budget targets the current bounded,
    // single-proof-system profile and leaves headroom under Chiado's 17M limit
    // for bundled inbound transactions; no dynamic estimate is performed.
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::TxHash;

    fn held(sender: Address, direction: Direction, nonce: u64, hash_byte: u8) -> HeldTx {
        HeldTx {
            raw_tx: Bytes::from(vec![hash_byte; 4]),
            hash: TxHash::repeat_byte(hash_byte),
            attempts: 0,
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
