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
use eez_driver::{
    BlockCommitterHandle, ParentContext, SyncSlotBlock, SyncSlotComposer, SyncSlotMode,
};
use eez_l1::{BundleTarget, L1Event, L1Watcher, SendOutcome, Submitter};
use eez_prover::Prover;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::{AlloyBlockHeader, Block, BlockBody};
use reth_storage_api::{BlockReader, BlockSource, StateProviderFactory, TransactionsProvider};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::held_pool::HeldTx;
use crate::ingress::Direction;
use crate::local::build_sync_block;
use crate::optimistic::OptimisticallyIncluded;
use crate::rollup::RollupState;

/// Outcome of `prepare_post_batch_raw`: either a ready-to-dispatch L1 tx
/// (the synchronous mock-proof path) or a deferred settlement that must
/// wait for the out-of-process prover's real attestation before it can be
/// signed and posted.
///
/// `Ready` carries the fully-signed `postAndVerifyBatch` raw tx (EIP-2718).
/// `Deferred` carries the assembled batch (with a placeholder/mock proof in
/// `proofs[]` that `apply_proof` will overwrite) plus the recomputed
/// `publicInputsHash` — the key under which the prover's signature lands in
/// the shared `ProofStore`. Only produced when `Composer::deferred_post()`
/// (i.e. `EEZ_PROOF_SYSTEM_KIND=real`, the real on-chain `ECDSAProofSystem`).
enum PostBatchOutcome {
    /// Synchronous path (mock proof system): the raw L1 tx is ready now.
    Ready(Bytes),
    /// Deferred path (real proof system): post fires when the prover's
    /// attestation arrives in the `ProofStore`, keyed by `public_inputs_hash`.
    Deferred {
        batch: Box<eez_evm::EvmBatch>,
        public_inputs_hash: B256,
        /// The L1-confirmed cursor snapshot the batch's OD-5 anchor was
        /// computed from (`stateDeltas[0].currentState = state(posted)`).
        /// MUST be threaded to `record_posted_window` so `from_block =
        /// posted+1` derives from the SAME snapshot as the anchor — a
        /// catch-up burst can advance the cursor between two reads,
        /// desyncing them and breaking the prover's OD-5 anchor check.
        posted: u64,
    },
}

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

/// Default maximum L2 width for one historical patch-recovery batch. A value of
/// 0 in `EEZ_PATCH_RECOVERY_MAX_SPAN` disables automatic patch recovery.
const DEFAULT_PATCH_RECOVERY_MAX_SPAN: u64 = 1024;

fn patch_recovery_max_span_from_env() -> Option<u64> {
    match std::env::var("EEZ_PATCH_RECOVERY_MAX_SPAN") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(span) => Some(span),
            Err(err) => {
                event!(
                    name: "eez.composer.patch_recovery.bad_span_env",
                    Level::WARN,
                    value = %raw,
                    error = %err,
                    default = DEFAULT_PATCH_RECOVERY_MAX_SPAN,
                    "invalid EEZ_PATCH_RECOVERY_MAX_SPAN; using default",
                );
                Some(DEFAULT_PATCH_RECOVERY_MAX_SPAN)
            }
        },
        Err(std::env::VarError::NotPresent) => Some(DEFAULT_PATCH_RECOVERY_MAX_SPAN),
        Err(err) => {
            event!(
                name: "eez.composer.patch_recovery.span_env_error",
                Level::WARN,
                error = %err,
                default = DEFAULT_PATCH_RECOVERY_MAX_SPAN,
                "could not read EEZ_PATCH_RECOVERY_MAX_SPAN; using default",
            );
            Some(DEFAULT_PATCH_RECOVERY_MAX_SPAN)
        }
    }
}

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
    /// L2 ENTRY client (`Role::Entry`, `EvmL2Style`) for OUTBOUND L2→L1
    /// source simulation: the drain runs an L2→L1 user tx through
    /// `simulate_and_resolve_recorded_for(L2, this, …)` to detect the
    /// cross-chain call. DISTINCT from the L2 FOLLOWER in `rollups` — a
    /// follower's `simulate_source_tx` errors `Unavailable`. `None` when the
    /// embedded L1 / `EvmComposer` isn't wired. Consumed by the drain's
    /// outbound branch.
    l2_entry_client: Option<
        std::sync::Arc<
            dyn eez_protocol::executor::EntryChainClient<Protocol = eez_evm::EvmProtocol>
                + Send
                + Sync,
        >,
    >,
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
    /// Prover-feed sink (P4-a): the settling block's `PostBatch`, keyed by the
    /// Sync block NUMBER, for the witness task to ship on
    /// `ControlEvent.composition`. Keyed by NUMBER not hash because the committer
    /// rebuilds the block from attrs (`process_sequence`) so the committed hash
    /// differs from the composer's predicted `built.header.hash()` — but the block
    /// NUMBER is deterministic on both sides. `OnceLock`-set after construction
    /// (`set_postbatch_sink`); `None` until wired (composer-mode only). The
    /// `std::Mutex` is held only for the brief insert.
    postbatch_sink:
        std::sync::OnceLock<Arc<std::sync::Mutex<HashMap<u64, eez_control_rpc::v1::PostBatch>>>>,
    /// Prover-feed RETURN store (P4-b-full): verified attestations the composer's
    /// `ProofSink` records, keyed by publicInputsHash. Set ONLY in DEFERRED-POST
    /// mode (`EEZ_PROOF_SYSTEM_KIND=real`): its presence switches the composer
    /// from self-signing the mock to waiting for the prover's real signature.
    /// `None` = synchronous mock post (unchanged).
    proof_store: std::sync::OnceLock<crate::proof_sink::ProofStore>,
    /// Composer-driven prover ledger (Phase 1, DARK). Each deferred post
    /// records its `[posted+1 .. sync_height]` window here; the `ProofSink`
    /// flips `attested` + advances the verified frontier when the matching
    /// attestation lands. Set only alongside `proof_store` (deferred-post).
    /// `None` = ledger off (no behavior change). Populated-but-not-driving in
    /// Phase 1; the dispatch driver (Phase 2) reads it.
    posted_windows: std::sync::OnceLock<crate::posted_windows::PostedWindows>,
    /// Maximum L2 width for one historical patch-recovery batch. `None` disables
    /// automatic patch recovery.
    patch_recovery_max_span: Option<u64>,
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
        l2_entry_client: Option<
            std::sync::Arc<
                dyn eez_protocol::executor::EntryChainClient<Protocol = eez_evm::EvmProtocol>
                    + Send
                    + Sync,
            >,
        >,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                rollups,
                prover,
                submitter,
                l1_watcher,
                evm_config,
                evm_composer,
                l2_entry_client,
                cc_exec_ctx,
                committer: std::sync::OnceLock::new(),
                postbatch_sink: std::sync::OnceLock::new(),
                proof_store: std::sync::OnceLock::new(),
                posted_windows: std::sync::OnceLock::new(),
                patch_recovery_max_span: patch_recovery_max_span_from_env(),
            }),
        }
    }

    /// Wire the prover-feed PostBatch sink (P4-a). The witness task
    /// (eez-node) shares the same `Arc` and drains it to fill each settling
    /// block's `ControlEvent.composition`. Called once at startup, before the
    /// first Sync slot. No-op (silently ignored) if already set.
    pub fn set_postbatch_sink(
        &self,
        sink: Arc<std::sync::Mutex<HashMap<u64, eez_control_rpc::v1::PostBatch>>>,
    ) {
        let _ = self.inner.postbatch_sink.set(sink);
    }

    /// Wire the prover-feed RETURN store (P4-b-full deferred post). Sharing the
    /// same `ProofStore` the `ProofSink` fills, its presence puts the composer in
    /// DEFERRED-POST mode: it builds + holds each settling batch and posts it only
    /// once the prover's verified attestation lands (vs self-signing the mock).
    /// Called once at startup. No-op if already set.
    pub fn set_proof_store(&self, store: crate::proof_sink::ProofStore) {
        let _ = self.inner.proof_store.set(store);
    }

    /// `true` when the composer is in deferred-post mode (a proof store is wired).
    #[must_use]
    pub fn deferred_post(&self) -> bool {
        self.inner.proof_store.get().is_some()
    }

    /// Wire the composer-driven prover ledger (Phase 1). Sharing the same
    /// [`PostedWindows`](crate::posted_windows::PostedWindows) the `ProofSink`
    /// attests into, the composer records each deferred window it posts so the
    /// frontier and the (Phase 2) dispatch driver have a single source of
    /// truth. DARK in Phase 1 — recording only, no behavior change. Called
    /// once at startup alongside [`set_proof_store`](Self::set_proof_store).
    /// No-op if already set.
    pub fn set_posted_windows(&self, windows: crate::posted_windows::PostedWindows) {
        let _ = self.inner.posted_windows.set(windows);
    }

    /// Phase 1 (dark): record the just-deferred `[posted+1 .. sync_height]`
    /// window in the composer-driven ledger, if wired. Returns `false` when the
    /// L1 cursor advanced after the batch was prepared, making the batch's anchor
    /// stale before it can be dispatched; the caller must fail the optimistic
    /// entry so the next slot rebuilds from the fresh cursor. Recording ONLY — the
    /// `ProofSink` flips `attested` + advances the verified frontier when the
    /// prover's attestation lands (keyed by `public_inputs_hash`). No-op when
    /// the ledger is off (synchronous mock post, or deferred-without-ledger).
    /// `posted` is the L1-confirmed cursor the batch anchored to, so
    /// `from_block = posted+1` matches the OD-5 batch boundary; `current_state`
    /// is the batch's first-entry `StateDelta.currentState` (the OD-5 anchor),
    /// both HINTS the prover re-derives from `abi_calldata`.
    ///
    /// CONSENSUS-CRITICAL: `posted` is passed in (the SAME cursor snapshot the
    /// caller used to compute the OD-5 anchor `state(posted)`) — it is NOT
    /// re-read from `l1_head.cursor()` here. A catch-up burst can advance the
    /// cursor between two reads, so the anchor and `from_block` would desync
    /// (anchor = `state(posted_early)` but `from_block = posted_late+1`),
    /// breaking the prover's OD-5 anchor check and halting settlement.
    fn record_posted_window(
        &self,
        rollup_id: u64,
        sync_height: u64,
        batch: &eez_evm::EvmBatch,
        public_inputs_hash: B256,
        posted: u64,
    ) -> bool {
        let Some(windows) = self.inner.posted_windows.get() else {
            return true;
        };
        let Some(rollup) = self.inner.rollups.get(&rollup_id) else {
            event!(
                name: "eez.composer.posted_window.unknown_rollup",
                Level::ERROR,
                rollup_id,
                sync_height,
                "cannot record deferred window for unknown rollup",
            );
            return false;
        };
        let latest_cursor = rollup.l1_head.cursor();
        if latest_cursor > posted {
            event!(
                name: "eez.composer.posted_window.stale_before_spawn",
                Level::WARN,
                rollup_id,
                sync_height,
                posted,
                latest_cursor,
                public_inputs_hash = %public_inputs_hash,
                "L1 cursor advanced after postBatch preparation; refusing stale deferred window so the next slot rebuilds",
            );
            return false;
        }
        let post_batch =
            crate::post_batch_msg::build_post_batch_msg(batch, self.inner.prover.vkey(), None);
        let current_state = batch
            .inner
            .entries
            .first()
            .and_then(|e| e.stateDeltas.first())
            .map_or(B256::ZERO, |d| d.currentState);
        windows.record_posted(crate::posted_windows::PostedWindow {
            from_block: posted.saturating_add(1),
            to_block: sync_height,
            rollup_id,
            public_inputs_hash,
            current_state,
            post_batch: Some(post_batch),
            attested: false,
            fast_forwarded: false,
            pending_l1: false,
        });
        true
    }

    fn abandon_unsubmitted_window(
        &self,
        rollup_id: u64,
        sync_height: u64,
        public_inputs_hash: B256,
        reason: &'static str,
    ) {
        let Some(windows) = self.inner.posted_windows.get() else {
            return;
        };
        if let Some(window) = windows.abandon_unsubmitted(sync_height) {
            event!(
                name: "eez.composer.posted_window.abandoned_unsubmitted",
                Level::WARN,
                rollup_id,
                sync_height,
                reason,
                public_inputs_hash = %public_inputs_hash,
                window_public_inputs_hash = %window.public_inputs_hash,
                "removed deferred posted-window entry before L1 submission",
            );
        }
    }

    fn abandon_oversized_inflight(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
        sync_height: u64,
    ) -> bool {
        let Some(max_span) = self.inner.patch_recovery_max_span else {
            return false;
        };
        let width = sync_height.saturating_sub(cursor);
        if width <= max_span {
            return false;
        }
        if let Some(windows) = self.inner.posted_windows.get() {
            let frontier = windows.mark_fast_forwarded(sync_height);
            event!(
                name: "eez.composer.patch_recovery.fast_forwarded",
                Level::WARN,
                rollup_id,
                cursor,
                sync_height,
                width,
                max_span,
                verified_frontier = frontier,
                "oversized deferred window abandoned as a recovery coverage gap",
            );
        }
        rollup.optimistic.mark_failed(sync_height, false);
        event!(
            name: "eez.composer.patch_recovery.abandon_inflight",
            Level::WARN,
            rollup_id,
            cursor,
            sync_height,
            width,
            max_span,
            "marked oversized in-flight deferred post failed so slot-context recovery can reopen the gate",
        );
        true
    }

    /// The attester address of the configured prover — the prover-feed
    /// `ProofSink` verifies returned attestations recover to it. The vkey is
    /// `bytes32(uint160(address))`, so the address is its low 20 bytes.
    #[must_use]
    pub fn prover_address(&self) -> Address {
        Address::from_slice(&self.inner.prover.vkey()[12..])
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
                // Fix-1 (composer-driven): a confirmed batch advanced the L1
                // cursor (the Deriver already appended it before this event
                // fires). Advance the verified frontier to the cursor — every
                // window at or below it SETTLED on L1, which required
                // `ECDSAProofSystem.verify` (the attester's signature), so it is
                // verified by transitivity THROUGH L1 even without a returned
                // attestation. This stops the driven prover re-verifying
                // confirmed windows (the live frontier-stall) and lets it skip a
                // settled-but-ring-evicted gap. `mark_settled_on_l1` is monotone
                // (reorg-safe). No-op when the ledger is off (self-sign). Fires
                // on ANY confirmed batch (ours or external — the cursor advances
                // by transitivity regardless of who posted).
                if let Some(windows) = self.posted_windows.get() {
                    for (rollup_id, rollup) in &self.rollups {
                        let cursor = rollup.l1_head.cursor();
                        let update = windows.mark_settled_on_l1(*rollup_id, cursor);
                        for stale in update.pruned_straddlers {
                            if let Some(owner) = self.rollups.get(&stale.rollup_id) {
                                owner.optimistic.mark_failed(stale.to_block, false);
                            }
                            event!(
                                name: "eez.composer.posted_window.stale_l1_cursor",
                                Level::WARN,
                                rollup_id = stale.rollup_id,
                                sync_height = stale.to_block,
                                from_block = stale.from_block,
                                l1_cursor = cursor,
                                public_inputs_hash = %stale.public_inputs_hash,
                                "posted window straddles an L1-confirmed cursor; marking optimistic entry failed for recovery",
                            );
                        }
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
                    // Composer-driven REORG SAFETY (critical with competing
                    // composers): demote any verified/pending window above the
                    // retreated cursor back to dispatchable, so the reorged-out
                    // range (possibly a COMPETITOR's batch that had advanced our
                    // rollup, then itself reorged) is RE-VERIFIED before it can
                    // re-settle — else monotone mark_settled_on_l1 leaves it
                    // falsely resolved (a coverage hole). Same new_cursor basis as
                    // the optimistic re-queue. Runs even when txs is empty below.
                    if let Some(windows) = self.posted_windows.get() {
                        windows.demote_above_cursor(new_cursor);
                    }
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
    async fn recover_sync_slot_without_emission(
        &self,
        rollup_id: u64,
        parent: ParentContext,
    ) -> bool {
        let parent_number = parent.header.number();
        event!(
            name: "eez.composer.sync_slot.recovery_only",
            Level::WARN,
            rollup_id,
            parent_number,
            "sync-slot recovery requested without Sync emission",
        );
        let Some(rollup) = self.inner.rollups.get(&rollup_id) else {
            event!(
                name: "eez.composer.sync_slot.recovery_unknown_rollup",
                Level::ERROR,
                rollup_id,
                known_rollups = ?self.inner.rollups.keys().collect::<Vec<_>>(),
                "recovery-only sync slot called for unknown rollup_id",
            );
            return false;
        };
        if self.inner.evm_composer.is_none() {
            return false;
        }

        let cursor = rollup.l1_head.cursor();
        rollup.optimistic.resolve_below_cursor(cursor);

        let mut recovered = false;
        if let Some(failed) = rollup.optimistic.take_failed_for_recovery(cursor) {
            let sync_height = failed.sync_height;
            event!(
                name: "eez.composer.sync_slot.recovery_failed_batch",
                Level::WARN,
                rollup_id,
                cursor,
                parent_number,
                sync_height,
                "recovering failed optimistic batch while Sync emission is capped",
            );
            let _ = self.recover_failed_batch(rollup_id, rollup, failed).await;
            recovered = true;
        }

        let cursor = rollup.l1_head.cursor();
        if let Some(blocking_height) = rollup.optimistic.blocking_height(cursor) {
            let abandoned =
                self.abandon_oversized_inflight(rollup_id, rollup, cursor, blocking_height);
            event!(
                name: "eez.composer.sync_slot.recovery_blocked",
                Level::WARN,
                rollup_id,
                cursor,
                parent_number,
                blocking_height,
                abandoned,
                "cap recovery is waiting for the in-flight settlement gate to resolve before posting a historical patch",
            );
            return recovered || abandoned;
        }

        match self
            .try_schedule_patch_recovery(rollup_id, rollup, parent_number)
            .await
        {
            Ok(true) => true,
            Ok(false) => recovered,
            Err(err) => {
                event!(
                    name: "eez.composer.patch_recovery.failed",
                    Level::ERROR,
                    rollup_id,
                    cursor,
                    parent_number,
                    error = %err,
                    "recovery-only sync slot failed to schedule bounded historical patch",
                );
                recovered
            }
        }
    }

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
        if self.inner.evm_composer.is_some() {
            if let Some(failed) = rollup.optimistic.take_failed_for_recovery(cursor) {
                return self.recover_failed_batch(rollup_id, rollup, failed).await;
            }
        }

        let blocking_height = self
            .inner
            .evm_composer
            .as_ref()
            .and_then(|_| rollup.optimistic.blocking_height(cursor));
        if let Some(blocking_height) = blocking_height {
            let abandoned =
                self.abandon_oversized_inflight(rollup_id, rollup, cursor, blocking_height);
            if abandoned {
                event!(
                    name: "eez.composer.patch_recovery.abandoned_gate",
                    Level::WARN,
                    rollup_id,
                    cursor,
                    parent_number,
                    blocking_height,
                    "oversized in-flight gate was abandoned; next capped slot will recover it without rewinding L2",
                );
            }
            event!(
                name: "eez.composer.sync_slot.bundle_in_flight",
                Level::INFO,
                rollup_id,
                cursor,
                parent_number,
                blocking_height,
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

        if self.inner.evm_composer.is_some() {
            match self
                .try_schedule_patch_recovery(rollup_id, rollup, parent_number)
                .await
            {
                Ok(true) => {
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
                                "empty Sync block build failed after arming patch recovery; Sequencer fallback takes over",
                            );
                            None
                        }
                    };
                }
                Ok(false) => {}
                Err(err) => {
                    event!(
                        name: "eez.composer.patch_recovery.failed",
                        Level::ERROR,
                        rollup_id,
                        parent_number,
                        error = %err,
                        "patch recovery scheduling failed; falling back to normal Sync composition",
                    );
                }
            }
        }

        // Catchup: structural-only — skip the drain, emit a minimal postBatch
        // (cross-chain stays pooled for the next Steady slot).
        if matches!(mode, SyncSlotMode::Catchup) {
            let (Some(_), Some(ctx)) = (
                self.inner.evm_composer.as_ref(),
                self.inner.cc_exec_ctx.as_ref(),
            ) else {
                return None;
            };
            return self
                .dispatch_minimal_postbatch(
                    ctx,
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
        // Cap drain to 10 user_txs per bundle. A backlog spills into the
        // next Sync slot instead of letting one slot grow without bound.
        //
        // rbuilder-chiado has shown partial-inclusion when bundles carry
        // too many user_txs:
        // postBatch lands, but only a prefix of the user_txs makes it
        // into the block — the rest are silently excluded by rbuilder
        // and effectively lost. Capping keeps every bundle's contents
        // 100% atomic; a backlog spills into the next Sync slot.
        const MAX_USER_TXS_PER_BUNDLE: usize = 10;
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
        if let (Some(evm_composer), Some(ctx)) = (
            self.inner.evm_composer.as_ref(),
            self.inner.cc_exec_ctx.as_ref(),
        ) {
            // Cross-chain mode is authoritative: `compose_via_evm_composer`
            // builds the Sync block, registers the drained txs in the
            // optimistic ledger, spawns the bundle observer, and
            // returns the block for IMMEDIATE commit — L1 settlement
            // is observed in the background and reconciled
            // retroactively (re-push + reorg on failure). Do NOT fall
            // through to the `build_sync_block` branch below —
            // `drained` are L1 user txs (type-0x2 EOA calls targeting
            // CCM-L1), not L2 system txs.
            return match self
                .compose_via_evm_composer(
                    evm_composer,
                    ctx,
                    rollup_id,
                    drained,
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
    fn choose_patch_recovery_target(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        cursor: u64,
        parent_number: u64,
        max_span: u64,
    ) -> Result<
        Option<(
            u64,
            reth_primitives_traits::SealedHeader<alloy_consensus::Header>,
            B256,
        )>,
        String,
    > {
        let target_max = cursor.saturating_add(max_span).min(parent_number);
        if target_max <= cursor {
            return Ok(None);
        }

        for target in (cursor + 1..=target_max).rev() {
            let target_header = rollup
                .l2_provider
                .sealed_header(target)
                .map_err(|e| format!("sealed_header({target}): {e}"))?
                .ok_or_else(|| format!("local L2 header at {target} missing"))?;
            let block = rollup
                .l2_provider
                .find_block_by_hash(target_header.hash(), BlockSource::Any)
                .map_err(|e| {
                    format!(
                        "l2_provider.find_block_by_hash({}, n={target}): {e}",
                        target_header.hash()
                    )
                })?
                .ok_or_else(|| format!("local L2 block at {target} missing"))?;
            if block.body().transactions().iter().next().is_some() {
                continue;
            }
            let parent = target.checked_sub(1).ok_or("patch target underflow")?;
            let parent_header = rollup
                .l2_provider
                .sealed_header(parent)
                .map_err(|e| format!("sealed_header({parent}): {e}"))?
                .ok_or_else(|| format!("local L2 header at {parent} missing"))?;
            event!(
                name: "eez.composer.patch_recovery.target",
                Level::INFO,
                rollup_id,
                cursor,
                target,
                max_span,
                "selected empty historical block for bounded patch recovery",
            );
            return Ok(Some((target, parent_header, target_header.state_root())));
        }

        event!(
            name: "eez.composer.patch_recovery.no_empty_target",
            Level::WARN,
            rollup_id,
            cursor,
            target_max,
            max_span,
            "no empty historical block found within patch span; normal composition remains blocked on proof",
        );
        Ok(None)
    }

    async fn try_schedule_patch_recovery(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        parent_number: u64,
    ) -> Result<bool, String> {
        let Some(max_span) = self.inner.patch_recovery_max_span else {
            return Ok(false);
        };
        if !self.deferred_post() || self.inner.posted_windows.get().is_none() {
            return Ok(false);
        }
        let Some(ctx) = self.inner.cc_exec_ctx.as_ref() else {
            return Ok(false);
        };
        let cursor = rollup.l1_head.cursor();
        let gap = parent_number.saturating_sub(cursor);
        if gap < max_span {
            return Ok(false);
        }
        let Some((target, parent_header, target_state_root)) =
            self.choose_patch_recovery_target(rollup_id, rollup, cursor, parent_number, max_span)?
        else {
            return Ok(false);
        };

        event!(
            name: "eez.composer.patch_recovery.arm",
            Level::WARN,
            rollup_id,
            cursor,
            parent_number,
            target,
            gap,
            patch_width = target.saturating_sub(cursor),
            max_span,
            "arming bounded historical patch batch instead of a current-tip giant batch",
        );

        match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &[],
                &parent_header,
                target_state_root,
                &[],
                &[],
            )
            .await?
        {
            PostBatchOutcome::Deferred {
                batch,
                public_inputs_hash,
                posted,
            } => {
                if posted != cursor {
                    return Err(format!(
                        "patch recovery cursor changed while preparing batch: started {cursor}, prepared {posted}"
                    ));
                }
                rollup.optimistic.begin(
                    target,
                    B256::ZERO,
                    target_state_root,
                    parent_header,
                    Vec::new(),
                );
                let recorded = self.record_posted_window(
                    rollup_id,
                    target,
                    &batch,
                    public_inputs_hash,
                    posted,
                );
                if recorded {
                    self.spawn_deferred_post(
                        rollup_id,
                        target,
                        posted,
                        *batch,
                        public_inputs_hash,
                        Vec::new(),
                        target_state_root,
                        Arc::clone(&rollup.optimistic),
                    );
                    Ok(true)
                } else {
                    rollup.optimistic.mark_failed(target, false);
                    Ok(false)
                }
            }
            PostBatchOutcome::Ready(_) => Err(
                "patch recovery unexpectedly built a synchronous postBatch outside deferred mode"
                    .to_owned(),
            ),
        }
    }

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
            // In based competition, though, cursor height alone is not
            // enough: another composer may have advanced the frontier
            // with a different root. Only skip recovery if our own
            // postBatch receipt reached the exact final root this
            // optimistic block committed locally.
            if rollup.l1_head.cursor() >= sync_height {
                match self
                    .inner
                    .submitter
                    .receipt_reached_state(failed.post_batch_hash, failed.expected_final_state)
                    .await
                {
                    Ok(true) => {
                        event!(
                            name: "eez.composer.recovery.stale_verdict",
                            Level::WARN,
                            rollup_id,
                            sync_height,
                            cursor = rollup.l1_head.cursor(),
                            post_batch_hash = %failed.post_batch_hash,
                            expected_final_state = %failed.expected_final_state,
                            "failure verdict is stale — our postBatch settled the optimistic root; keeping block",
                        );
                        rollup.optimistic.reinsert_settled(failed);
                        return None;
                    }
                    Ok(false) => {
                        event!(
                            name: "eez.composer.recovery.cursor_passed_different_root",
                            Level::WARN,
                            rollup_id,
                            sync_height,
                            cursor = rollup.l1_head.cursor(),
                            post_batch_hash = %failed.post_batch_hash,
                            expected_final_state = %failed.expected_final_state,
                            "cursor passed failed Sync height, but this postBatch did not settle the optimistic root; recovering local block",
                        );
                    }
                    Err(err) => {
                        event!(
                            name: "eez.composer.recovery.stale_verdict_check_failed",
                            Level::ERROR,
                            rollup_id,
                            sync_height,
                            cursor = rollup.l1_head.cursor(),
                            post_batch_hash = %failed.post_batch_hash,
                            expected_final_state = %failed.expected_final_state,
                            error = %err,
                            "could not verify failed postBatch root; keeping entry Failed for retry",
                        );
                        rollup.optimistic.reinsert_failed(failed);
                        return None;
                    }
                }
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
            let mut evicted_chains: Vec<(alloy_primitives::Address, Direction, u64)> = Vec::new();
            // slot_skipped: re-queue without counting toward poison-eviction
            // (a skipped slot heals via a fresh pin next tick).
            let slot_skipped = failed.slot_skipped;
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
            // Nonce-chain cascade: evicting (sender, N) makes every
            // same-sender tx with nonce > N permanently invalid (the
            // gap never fills — the evicted nonce only lands if the
            // user resubmits, which produces a NEW tx). Leaving them
            // queued poisons every future bundle they ride and breaks
            // OTHER senders' chains as collateral — the exact cascade
            // that bricked a run's user EOA. Drop them from both the
            // keep list and the pool, loudly.
            for (sender, direction, nonce) in &evicted_chains {
                keep.retain(|t| {
                    let cascade =
                        t.sender == *sender && t.direction == *direction && t.nonce > *nonce;
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
                for t in pool.drain_sender_above(*sender, *direction, *nonce) {
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

    /// Restart/persistence recovery for an optimistic rich Sync block
    /// whose in-memory ledger entry was lost before slot-context
    /// recovery ran. The invariant-7 postBatch guard detects the
    /// symptom: a type-0x7E/SYSTEM_ADDRESS tx appears in an
    /// unconfirmed intermediate range (`cursor+1..sync-1`). Since the
    /// block is above the L1-confirmed cursor, it is unsafe local
    /// history; roll back to the parent of the earliest offending block
    /// and let the sequencer rebuild from there.
    ///
    /// This cannot requeue the original L1 user txs because the
    /// volatile `HeldTx` records died with the process. It only removes
    /// phantom L2 effects so settlement can resume; users resubmit
    /// unlanded L1 txs normally.
    async fn recover_untracked_system_tx_block(
        &self,
        rollup_id: u64,
        rollup: &RollupState<L2>,
        ctx: &CrossChainExecCtx,
    ) -> bool {
        let Some(committer) = self.inner.committer.get() else {
            event!(
                name: "eez.composer.recovery.untracked.no_committer",
                Level::ERROR,
                rollup_id,
                "committer handle not wired; cannot recover untracked failed Sync block",
            );
            return false;
        };

        let _guard = committer.begin_reconcile().await;
        let cursor = rollup.l1_head.cursor();
        let head = committer.last_header();
        if head.number() <= cursor {
            return false;
        }

        let mut walk_hash = head.hash();
        let mut walk_number = head.number();
        let mut earliest_offending = None;

        while walk_number > cursor {
            let block = match rollup
                .l2_provider
                .find_block_by_hash(walk_hash, BlockSource::Any)
            {
                Ok(Some(block)) => block,
                Ok(None) => {
                    event!(
                        name: "eez.composer.recovery.untracked.missing_block",
                        Level::ERROR,
                        rollup_id,
                        cursor,
                        head = head.number(),
                        walk_number,
                        walk_hash = %walk_hash,
                        "cannot scan untracked optimistic range; block missing",
                    );
                    return false;
                }
                Err(err) => {
                    event!(
                        name: "eez.composer.recovery.untracked.block_read_failed",
                        Level::ERROR,
                        rollup_id,
                        cursor,
                        head = head.number(),
                        walk_number,
                        walk_hash = %walk_hash,
                        error = %err,
                        "cannot scan untracked optimistic range",
                    );
                    return false;
                }
            };

            for enc in block
                .body()
                .transactions()
                .iter()
                .map(Encodable2718::encoded_2718)
            {
                let is_system = if enc.first() == Some(&0x7E) {
                    true
                } else {
                    use alloy_eips::eip2718::Decodable2718 as _;
                    use reth_primitives_traits::SignerRecoverable as _;
                    let mut raw: &[u8] = enc.as_slice();
                    let tx =
                        match reth_ethereum_primitives::TransactionSigned::decode_2718(&mut raw) {
                            Ok(tx) => tx,
                            Err(err) => {
                                event!(
                                    name: "eez.composer.recovery.untracked.decode_failed",
                                    Level::ERROR,
                                    rollup_id,
                                    cursor,
                                    walk_number,
                                    error = %err,
                                    "cannot decode tx while scanning untracked optimistic range",
                                );
                                return false;
                            }
                        };
                    match tx.recover_signer() {
                        Ok(signer) => signer == ctx.system_signer.address(),
                        Err(err) => {
                            event!(
                                name: "eez.composer.recovery.untracked.signer_failed",
                                Level::ERROR,
                                rollup_id,
                                cursor,
                                walk_number,
                                error = %err,
                                "cannot recover tx signer while scanning untracked optimistic range",
                            );
                            return false;
                        }
                    }
                };
                if is_system {
                    earliest_offending = Some(walk_number);
                }
            }

            walk_hash = block.header().parent_hash();
            walk_number -= 1;
        }

        let Some(offending_height) = earliest_offending else {
            return false;
        };
        let target_height = offending_height.saturating_sub(1);
        if target_height < cursor {
            event!(
                name: "eez.composer.recovery.untracked.below_cursor",
                Level::ERROR,
                rollup_id,
                cursor,
                offending_height,
                target_height,
                "refusing to roll back below L1-confirmed cursor",
            );
            return false;
        }

        let target = match rollup.l2_provider.sealed_header(target_height) {
            Ok(Some(header)) => header,
            Ok(None) => {
                event!(
                    name: "eez.composer.recovery.untracked.target_missing",
                    Level::ERROR,
                    rollup_id,
                    cursor,
                    offending_height,
                    target_height,
                    "cannot recover untracked failed Sync block; target header missing",
                );
                return false;
            }
            Err(err) => {
                event!(
                    name: "eez.composer.recovery.untracked.target_read_failed",
                    Level::ERROR,
                    rollup_id,
                    cursor,
                    offending_height,
                    target_height,
                    error = %err,
                    "cannot recover untracked failed Sync block; target read failed",
                );
                return false;
            }
        };

        if let Err(err) = committer.reorg_to(target).await {
            event!(
                name: "eez.composer.recovery.untracked.reorg_failed",
                Level::ERROR,
                rollup_id,
                cursor,
                head = head.number(),
                offending_height,
                target_height,
                error = %err,
                "reorg_to failed while recovering untracked failed Sync block",
            );
            return false;
        }

        event!(
            name: "eez.composer.recovery.untracked.rolled_back",
            Level::WARN,
            rollup_id,
            cursor,
            old_head = head.number(),
            offending_height,
            target_height,
            "untracked failed Sync block recovered; unsafe local chain rolled back to parent",
        );
        true
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
        bundle_target: BundleTarget,
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
        // SYSTEM_ADDRESS nonce at the Sync block's parent — the SAME point the
        // deriver reads (system_address_nonce_at(from_block-1)). NOT advanced
        // inline during the drain: the canonical build_cross_chain_sync_pairs
        // assigns it two-phase post-drain so a mixed slot matches the deriver.
        let nonce = state
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
                    bundle_target,
                )
                .await;
        }

        let mut survivors: Vec<HeldTx> = Vec::with_capacity(drained.len());
        let mut survivor_comps: Vec<eez_protocol::Composition<eez_evm::EvmProtocol>> =
            Vec::with_capacity(drained.len());
        // Cross-chain work STAGED in drain order; the SYSTEM_ADDRESS system txs +
        // their nonces + the Sync-block order are assigned AFTER the drain by the
        // canonical `build_cross_chain_sync_pairs` (NOT inline) so a MIXED slot's
        // Sync block is byte-identical to the deriver's two-phase reconstruction.
        // pending_out: each (outbound L1 settlement entry, its consuming user tx);
        // pending_in: the inbound deferred target entries.
        let mut pending_out: Vec<(eez_evm::types::ExecutionEntrySol, alloy_primitives::Bytes)> =
            Vec::new();
        let mut pending_in: Vec<eez_evm::types::ExecutionEntrySol> = Vec::new();
        // OUTBOUND L1 settlement entries (proxyEntryHash==0 + non-empty
        // l2ToL1Calls) accumulated for the postBatch splice (A2.3d). Empty for
        // an inbound-only slot → no splice, byte-identical postBatch.
        let mut outbound_entries: Vec<eez_evm::types::ExecutionEntrySol> = Vec::new();
        let mut poison: Vec<HeldTx> = Vec::new();
        // On a transient failure we abort the slot; this holds the error
        // string + the txs still needing re-queue (the failing tx + the
        // unprocessed remainder; survivors are added below).
        let mut transient: Option<(String, Vec<HeldTx>)> = None;

        let mut iter = drained.into_iter().enumerate();
        while let Some((idx, held)) = iter.next() {
            // ── OUTBOUND L2→L1 arm (A2.3b) ──────────────────────────────
            // Source-sim via the L2 ENTRY client (the follower's
            // simulate_source_tx errors Unavailable). The L1 settlement
            // entries land in `composition.targets[L1].batch`; lower each to a
            // lean L2 deferred entry + its loadExecutionTable, and pair the
            // load tx with the user tx (SyncPair). R1: zero L1 entries → POISON
            // (never a silent survivor; never reaches build_inbound_system_txs).
            if held.direction == Direction::Outbound {
                let Some(l2_entry) = self.inner.l2_entry_client.as_deref() else {
                    event!(
                        name: "eez.composer.cc_compose.outbound_no_l2_entry",
                        Level::WARN,
                        rollup_id,
                        tx_hash = %held.hash,
                        "outbound tx but no L2 entry client wired (no embedded L1); evicting",
                    );
                    poison.push(held);
                    continue;
                };
                match evm_composer
                    .simulate_and_resolve_recorded_for(
                        eez_protocol::RollupId(rollup_id),
                        l2_entry,
                        held.raw_tx.as_ref(),
                    )
                    .await
                {
                    Ok((composition, _recorded)) => {
                        let l1_entries: Vec<eez_evm::types::ExecutionEntrySol> = composition
                            .targets
                            .iter()
                            .flat_map(|t| t.batch.entries().iter().cloned())
                            .collect();
                        if l1_entries.is_empty() {
                            event!(
                                name: "eez.composer.cc_compose.outbound_no_entries",
                                Level::WARN,
                                rollup_id,
                                tx_idx = idx,
                                tx_hash = %held.hash,
                                sender = %held.sender,
                                nonce = held.nonce,
                                "outbound tx produced no L1 settlement entry; evicting (resubmit required)",
                            );
                            poison.push(held);
                            continue;
                        }
                        // STAGE each outbound L1 settlement entry with its
                        // consuming user tx (this held tx's executeCrossChainCall).
                        // The loadExecutionTable system tx + its two-phase nonce
                        // are built post-drain by the canonical builder, in the
                        // deriver-matching order — NOT inline in drain order.
                        for oe in &l1_entries {
                            pending_out.push((oe.clone(), held.raw_tx.clone()));
                        }
                        outbound_entries.extend(l1_entries);
                        // Do NOT push the outbound composition to `survivor_comps`:
                        // that list feeds `prepare_post_batch_raw`'s merge, which
                        // splices each composition's `source.batch` (the ENTRY
                        // chain's batch) into the postBatch. For an OUTBOUND call
                        // the entry chain is OUR L2, so `source.batch` is the L2's
                        // own entries (carrying the L2→L1 call as a
                        // destinationRollupId=MAINNET(0) entry) — which must NOT
                        // settle on L1. The L1 settlement entries are exactly
                        // `outbound_entries` (the zk-poster `targets[L1].batch`),
                        // spliced separately with destinationRollupId=rid. Merging
                        // `source.batch` here double-counted the call AND
                        // re-introduced a dest=0 entry the registry gate (correctly)
                        // rejects. Inbound survivors still push their composition
                        // below so their `source.batch` (the L1 deferred entries)
                        // merges as before.
                        survivors.push(held);
                    }
                    Err(e) if sim_error_is_poison(&e) => {
                        event!(
                            name: "eez.composer.cc_compose.outbound_poison",
                            Level::WARN,
                            rollup_id,
                            tx_idx = idx,
                            tx_hash = %held.hash,
                            sender = %held.sender,
                            nonce = held.nonce,
                            error = %e,
                            "outbound tx fails simulation deterministically; evicting",
                        );
                        poison.push(held);
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
                    // STAGE the inbound deferred target entries; the
                    // executeIncomingCrossChainCall delivery txs + their two-phase
                    // nonces are built post-drain by the canonical builder (after
                    // all outbound loads), matching the deriver's order.
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
                for t in pool.drain_sender_above(tx.sender, tx.direction, tx.nonce) {
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
                    bundle_target,
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
                    bundle_target,
                )
                .await;
        }

        // ── Build the Sync block's cross-chain system txs via THE canonical
        // builder — deriver-byte-identical two-phase SYSTEM_ADDRESS nonces
        // (outbound loads N.., THEN inbound deliveries N+K..) + interleaved order
        // [load,user,…,deliveries], independent of drain order (A2b). A failure
        // is systemic (signing / nonce overflow) → re-queue survivors, degrade.
        let pairs = match eez_evm::system_tx::build_cross_chain_sync_pairs(
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
        let built = match build_sync_block(
            rollup.l2_provider.as_ref(),
            &self.inner.evm_config,
            parent_header,
            timestamp,
            suggested_fee_recipient,
            &eez_evm::system_tx::interleave_sync_block_txs(&pairs),
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
        let comp_refs: Vec<&eez_protocol::Composition<eez_evm::EvmProtocol>> =
            survivor_comps.iter().collect();
        // Outbound user txs (the SyncPair user halves) travel in the sync-block
        // DA slot — the deriver can't reconstruct them from the postBatch
        // entries (only the system/load txs are). Empty for inbound-only.
        let outbound_user_txs: Vec<Bytes> =
            pairs.iter().filter_map(|p| p.user_tx.clone()).collect();
        let outcome = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &comp_refs,
                parent_header,
                built.header.state_root(),
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
                if e.contains("un-recovered failed Sync block")
                    && self
                        .recover_untracked_system_tx_block(rollup_id, rollup, ctx)
                        .await
                {
                    return Ok(None);
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
        let total_entries: usize = comp_refs
            .iter()
            .map(|c| c.source.batch.entries().len())
            .sum();

        // ── Dispatch: rich bundle [postBatch, ...survivors], commit. ──
        let sync_height = built.header.number();
        match outcome {
            PostBatchOutcome::Ready(postbatch_raw) => {
                // keccak of the raw EIP-2718 envelope IS the typed tx's hash —
                // recorded in the ledger so the finality audit can look up the
                // postBatch receipt.
                let post_batch_hash = alloy_primitives::keccak256(&postbatch_raw);
                let mut bundle: Vec<Bytes> = Vec::with_capacity(1 + survivors.len());
                bundle.push(postbatch_raw);
                // Only INBOUND survivors ride the L1 bundle (L1-signed, execute
                // on L1). Outbound survivors are L2-signed — they run in the L2
                // sync block + travel in the DA slot, never the L1 bundle (an L2
                // tx is invalid on L1). A2.3e.
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
                    "rich bundle dispatched to background observer; committing Sync block optimistically",
                );
                rollup.optimistic.begin(
                    sync_height,
                    post_batch_hash,
                    built.header.state_root(),
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
            }
            PostBatchOutcome::Deferred {
                batch,
                public_inputs_hash,
                posted,
            } => {
                // Real proof system: the post fires when the prover attests.
                // The Sync block still commits now (L2 cadence is uncondi-
                // tional); the deferred task signs + dispatches the bundle
                // once the attestation lands in the ProofStore.
                //
                // Register the one-in-flight gate SYNCHRONOUSLY here — exactly
                // like the Ready arm above — BEFORE returning the committed
                // block, with a placeholder postBatch hash the deferred task
                // fills once it signs. This (a) holds the `survivors` in the
                // ledger for recovery, and (b) closes `blocking_height` before
                // the next Sync slot runs, so at most one deferred post is in
                // flight per rollup. Without it, two settling slots within the
                // ~30s proof window would both anchor `currentState` to the
                // same frozen cursor and the second would revert
                // StateRootMismatch on L1 (burning its user-tx nonces). The raw
                // envelopes go to the task for the bundle; the owning `HeldTx`s
                // stay in the ledger for `take_failed_for_recovery`.
                // Bundle is inbound-only (outbound survivors run in the L2 block
                // + DA, never the L1 bundle); the full `survivors` still go to
                // the optimistic ledger below for recovery. A2.3e.
                let survivor_raws: Vec<Bytes> = survivors
                    .iter()
                    .filter(|h| h.direction == Direction::Inbound)
                    .map(|h| h.raw_tx.clone())
                    .collect();
                event!(
                    name: "eez.composer.deferred.armed",
                    Level::INFO,
                    rollup_id,
                    sync_height,
                    entry_count = total_entries,
                    evicted_poison = poison.len(),
                    public_inputs_hash = %public_inputs_hash,
                    "deferred post armed; gate closed, awaiting prover attestation; committing Sync block optimistically",
                );
                rollup.optimistic.begin(
                    sync_height,
                    B256::ZERO, // placeholder; spawn_deferred_post fills the real hash on sign
                    built.header.state_root(),
                    parent_header.clone(),
                    survivors,
                );
                // Phase 1 (dark): record this window in the composer-driven
                // ledger BEFORE `*batch` is moved into the deferred task.
                let recorded = self.record_posted_window(
                    rollup_id,
                    sync_height,
                    &batch,
                    public_inputs_hash,
                    posted,
                );
                if recorded {
                    self.spawn_deferred_post(
                        rollup_id,
                        sync_height,
                        posted,
                        *batch,
                        public_inputs_hash,
                        survivor_raws,
                        built.header.state_root(),
                        Arc::clone(&rollup.optimistic),
                    );
                } else {
                    rollup.optimistic.mark_failed(sync_height, false);
                }
            }
        }
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
        bundle_target: BundleTarget,
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

        let outcome = match self
            .prepare_post_batch_raw(
                ctx,
                rollup_id,
                &[], // no compositions → leading immediate only
                parent_header,
                empty_built.header.state_root(),
                &[], // no outbound entries in a minimal postBatch
                &[], // no outbound user txs in a minimal postBatch
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
                if err.contains("un-recovered failed Sync block")
                    && self
                        .recover_untracked_system_tx_block(rollup_id, rollup, ctx)
                        .await
                {
                    return Ok(None);
                }
                return Ok(Some(SyncSlotBlock {
                    payload: empty_built.payload,
                    header: empty_built.header,
                }));
            }
        };
        let sync_height = empty_built.header.number();
        match outcome {
            PostBatchOutcome::Ready(minimal_postbatch_raw) => {
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
                    empty_built.header.state_root(),
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
            }
            PostBatchOutcome::Deferred {
                batch,
                public_inputs_hash,
                posted,
            } => {
                // Register the one-in-flight gate SYNCHRONOUSLY (placeholder
                // hash, no survivors for an empty slot) so the next slot blocks
                // until this deferred post resolves — see the rich arm + the
                // `spawn_deferred_post` gate-invariant doc.
                event!(
                    name: "eez.composer.phase1.deferred.armed",
                    Level::INFO,
                    rollup_id,
                    sync_height,
                    public_inputs_hash = %public_inputs_hash,
                    "minimal postBatch deferred; gate closed, awaiting prover attestation (leading immediate only)",
                );
                rollup.optimistic.begin(
                    sync_height,
                    B256::ZERO, // placeholder; spawn_deferred_post fills the real hash on sign
                    empty_built.header.state_root(),
                    parent_header.clone(),
                    Vec::new(),
                );
                // Phase 1 (dark): record this window in the composer-driven
                // ledger BEFORE `*batch` is moved into the deferred task.
                let recorded = self.record_posted_window(
                    rollup_id,
                    sync_height,
                    &batch,
                    public_inputs_hash,
                    posted,
                );
                if recorded {
                    self.spawn_deferred_post(
                        rollup_id,
                        sync_height,
                        posted,
                        *batch,
                        public_inputs_hash,
                        Vec::new(),
                        empty_built.header.state_root(),
                        Arc::clone(&rollup.optimistic),
                    );
                } else {
                    rollup.optimistic.mark_failed(sync_height, false);
                }
            }
        }
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

    /// Deferred-post dispatch (real proof system). The settling block has
    /// already committed; this task waits for the out-of-process prover to
    /// attest the `publicInputsHash` — its ECDSA signature lands in the shared
    /// `ProofStore` via `ProofSinkSvc::with_store` — then fills `batch.proofs[]`
    /// with that real attestation (`apply_proof`), signs the `postAndVerifyBatch`
    /// L1 tx via the shared `finalize_post_batch_tx` seam, and dispatches the
    /// optimistic bundle exactly like the synchronous path.
    ///
    /// **Gate invariant (consensus-critical).** The caller has ALREADY
    /// registered this height in the optimistic ledger (`optimistic.begin`,
    /// SYNCHRONOUSLY at slot time, holding the `HeldTx` survivors for recovery)
    /// — so the one-in-flight gate (`blocking_height`) is closed before the next
    /// Sync slot runs and only ONE deferred post can be in flight per rollup.
    /// Without that, two settling slots within the proof window would both
    /// anchor `currentState` to the same frozen cursor and the second would
    /// revert `StateRootMismatch` on L1 (burning its bundled user-tx nonces).
    /// This task therefore NEVER calls `begin`; it only resolves the
    /// pre-registered entry:
    /// - success → `set_post_batch_hash` (real hash) + dispatch the observer,
    ///   which marks it Settled/Failed;
    /// - timeout / no-store / no-ctx / finalize-failure → `mark_failed`, so the
    ///   next slot's `take_failed_for_recovery` reorgs the abandoned Sync block
    ///   out and re-queues the survivors (and the gate reopens for
    ///   re-derivation). Abandoning WITHOUT `mark_failed` would leave the entry
    ///   Pending forever → permanent emission livelock.
    ///
    /// `survivor_raws` are just the raw user-tx envelopes for the bundle; the
    /// owning `HeldTx`s live in the ledger entry (for recovery), mirroring the
    /// synchronous path's dual use of `survivors`.
    ///
    /// The wait is bounded (~30s, 150 × 200ms): on Chiado the settling proof
    /// window is ~2s, so an attestation should arrive well within budget.
    fn spawn_deferred_post(
        &self,
        rollup_id: u64,
        sync_height: u64,
        posted: u64,
        mut batch: eez_evm::EvmBatch,
        public_inputs_hash: B256,
        survivor_raws: Vec<Bytes>,
        expected_final_state: B256,
        optimistic: Arc<OptimisticallyIncluded>,
    ) {
        // Clone the `Arc<Inner>` directly rather than `self.clone()`: the
        // derived `Clone` carries a spurious `L2: Clone` bound, so `self.clone()`
        // would clone the `&self` reference (which can't escape into the task).
        let this = Self {
            inner: Arc::clone(&self.inner),
        };
        let store = self.inner.proof_store.get().cloned();
        tokio::spawn(async move {
            let Some(store) = store else {
                event!(
                    name: "eez.composer.deferred.no_store",
                    Level::ERROR,
                    rollup_id,
                    sync_height,
                    "deferred post spawned without a proof store — abandoning; marking failed for recovery",
                );
                this.abandon_unsubmitted_window(
                    rollup_id,
                    sync_height,
                    public_inputs_hash,
                    "no_store",
                );
                optimistic.mark_failed(sync_height, false);
                return;
            };

            // Poll the ProofStore for the prover's attestation (keyed by the
            // recomputed publicInputsHash). The ProofSink only records a
            // signature AFTER it has verified ecrecover == the registered
            // attester, so any entry here is already trustworthy.
            // Scale the attestation wait with the backlog width. A fixed 30s
            // poll is fine in steady state (the prover attests a small window in
            // seconds), but after a settlement freeze the prover must
            // RECONSTRUCT the witnesses for a large `[posted+1 .. sync_height]`
            // backlog from the L2 archive — two RPC round-trips per block
            // (debug_executionWitness + debug_getRawBlock). Measured ~109ms/block
            // and CLIMBING as the node fills (the native re-execution itself is
            // ~0.25ms/block — the witness SOURCING dominates, not the prover).
            // Abandoning before the backfill finishes means `posted` never
            // advances and the next directive re-dictates a wider range from the
            // frozen L1 cursor — a bootstrap livelock. Budget ~400ms/block
            // (≈3.7x the measured rate, headroom for the climb) + a 30s base,
            // each poll 200ms. The cap is intentionally hours, not minutes: once
            // the first catch-up batch posts, `posted` jumps to the backlog tip
            // and later windows are small; timing out a live prover before that
            // only makes the next proof wider and slower.
            let backlog = sync_height.saturating_sub(posted);
            const DEFERRED_PROOF_POLL_MS: u64 = 200;
            const MAX_DEFERRED_PROOF_POLLS: u64 = 144_000; // 8h
            let uncapped_polls = 150u64.saturating_add(backlog.saturating_mul(2));
            let max_polls = uncapped_polls.min(MAX_DEFERRED_PROOF_POLLS);
            event!(
                name: "eez.composer.deferred.wait_budget",
                Level::INFO,
                rollup_id,
                sync_height,
                posted,
                backlog,
                wait_secs = max_polls.saturating_mul(DEFERRED_PROOF_POLL_MS) / 1_000,
                capped = max_polls < uncapped_polls,
                "deferred post waiting for prover attestation",
            );
            let mut sig = None;
            for _ in 0..max_polls {
                if !optimistic.is_pending(sync_height) {
                    event!(
                        name: "eez.composer.deferred.abandoned",
                        Level::WARN,
                        rollup_id,
                        sync_height,
                        public_inputs_hash = %public_inputs_hash,
                        "deferred post abandoned by recovery before attestation arrived; dropping task",
                    );
                    this.abandon_unsubmitted_window(
                        rollup_id,
                        sync_height,
                        public_inputs_hash,
                        "abandoned_before_attestation",
                    );
                    return;
                }
                if let Some(s) = store
                    .lock()
                    .ok()
                    .and_then(|mut m| m.remove(&public_inputs_hash))
                {
                    sig = Some(s);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(DEFERRED_PROOF_POLL_MS)).await;
            }
            let Some(sig) = sig else {
                event!(
                    name: "eez.composer.deferred.timeout",
                    Level::ERROR,
                    rollup_id,
                    sync_height,
                    public_inputs_hash = %public_inputs_hash,
                    "deferred post timed out waiting for prover attestation — marking failed; next slot recovers",
                );
                this.abandon_unsubmitted_window(
                    rollup_id,
                    sync_height,
                    public_inputs_hash,
                    "timeout",
                );
                optimistic.mark_failed(sync_height, false);
                return;
            };
            if !optimistic.is_pending(sync_height) {
                event!(
                    name: "eez.composer.deferred.late_proof_abandoned",
                    Level::WARN,
                    rollup_id,
                    sync_height,
                    public_inputs_hash = %public_inputs_hash,
                    "deferred proof arrived after recovery abandoned the window; not submitting stale postBatch",
                );
                this.abandon_unsubmitted_window(
                    rollup_id,
                    sync_height,
                    public_inputs_hash,
                    "late_proof_abandoned",
                );
                return;
            }

            // Fill the real attestation (= `proof_sink::apply_proof`): the mock
            // signature placed in `proofs[]` at prepare time is overwritten by
            // the prover's ECDSA signature over the publicInputsHash, which is
            // what the on-chain `ECDSAProofSystem.verify` recovers.
            batch.inner.proofs = vec![sig];

            let Some(ctx) = this.inner.cc_exec_ctx.clone() else {
                event!(
                    name: "eez.composer.deferred.no_ctx",
                    Level::ERROR,
                    rollup_id,
                    sync_height,
                    "deferred post has no cross-chain exec ctx — abandoning; marking failed for recovery",
                );
                this.abandon_unsubmitted_window(
                    rollup_id,
                    sync_height,
                    public_inputs_hash,
                    "no_ctx",
                );
                optimistic.mark_failed(sync_height, false);
                return;
            };

            match this.finalize_post_batch_tx(&batch, ctx.as_ref()).await {
                Ok(raw) => {
                    let post_batch_hash = alloy_primitives::keccak256(&raw);
                    let mut bundle: Vec<Bytes> = Vec::with_capacity(1 + survivor_raws.len());
                    bundle.push(raw);
                    bundle.extend(survivor_raws);
                    event!(
                        name: "eez.composer.deferred.dispatched",
                        Level::INFO,
                        rollup_id,
                        sync_height,
                        tx_count = bundle.len(),
                        public_inputs_hash = %public_inputs_hash,
                        "deferred post: prover attestation applied, dispatching bundle to background observer",
                    );
                    // Fill the real postBatch hash into the gate entry the caller
                    // pre-registered at slot time; the observer (spawned next)
                    // flips it Pending→Settled/Failed.
                    optimistic.set_post_batch_hash(sync_height, post_batch_hash);
                    // Composer-driven: this window is now ATTESTED + SUBMITTED to
                    // L1 (the bundle is in flight). Mark it pending so the dispatch
                    // STOPS re-issuing its directive (the prover already verified
                    // it); it resolves on L1 confirm (mark_settled_on_l1) or is
                    // demoted on an L1 reorg.
                    if let Some(windows) = this.inner.posted_windows.get() {
                        windows.mark_deferred_pending(public_inputs_hash);
                    }
                    this.spawn_bundle_observer(
                        ctx.as_ref(),
                        rollup_id,
                        sync_height,
                        bundle,
                        expected_final_state,
                        optimistic,
                        BundleTarget::NextBlock,
                    );
                }
                Err(e) => {
                    event!(
                        name: "eez.composer.deferred.finalize_failed",
                        Level::ERROR,
                        rollup_id,
                        sync_height,
                        error = %e,
                        "deferred post: finalize_post_batch_tx failed — marking failed; next slot recovers",
                    );
                    this.abandon_unsubmitted_window(
                        rollup_id,
                        sync_height,
                        public_inputs_hash,
                        "finalize_failed",
                    );
                    optimistic.mark_failed(sync_height, false);
                }
            }
        });
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
        outbound_entries: &[eez_evm::types::ExecutionEntrySol],
        outbound_user_txs: &[Bytes],
    ) -> Result<PostBatchOutcome, String> {
        use eez_evm::types::RollupIdWithProofSystemsSol;

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
        //
        // CONSENSUS-CRITICAL: this single `posted` snapshot is the OD-5
        // anchor's basis and is threaded out via `PostBatchOutcome::Deferred`
        // to `record_posted_window`, which derives `from_block = posted+1`
        // from it. A catch-up burst can advance `l1_head.cursor()` between
        // two separate reads, so the anchor (`state(posted)`) and `from_block`
        // MUST come from this ONE read — never re-read the cursor downstream.
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
            l2ToL1Calls: Vec::new(),
            expectedL1ToL2Calls: Vec::new(),
            expectedLookups: Vec::new(),
            callCount: U256::ZERO,
            returnData: Bytes::new(),
            rollingHash: B256::ZERO,
        };
        batch.inner.entries.insert(0, immediate_entry);

        // Splice OUTBOUND L2→L1 entries right after the leading anchor (the
        // contiguous proxyEntryHash==0 run): [anchor | outbound_1..N | inbound
        // deferred]. Each arrives with EMPTY stateDeltas (`build_l1_postbatch`
        // emits them empty), so CREATE one chained settlement delta per entry —
        // without it `_applyStateDeltas` no-ops and the L2 root is silently
        // unsettled (CRITICAL-1). `currentState` is a placeholder fixed by the
        // stitch below; `newState` = the user-tx-inclusive sync-block root.
        // Empty for an inbound-only slot → byte-identical to before.
        for (k, oe) in outbound_entries.iter().enumerate() {
            let mut entry = oe.clone();
            // An OUTBOUND L2→L1 settlement entry belongs to OUR rollup's
            // settlement (the SOURCE rollup whose batch this is), so its
            // destinationRollupId is `rid` — NOT MAINNET(0), the call's target.
            // The composition groups the call under its target rollup
            // (MAINNET=0), but the protocol's canonical structure requires the
            // source rollup id: see `IntegrationTestBridge.t.sol` (an immediate
            // entry with an L1-targeted `l2ToL1Call` sets
            // `destinationRollupId = L2_ROLLUP_ID`), which is exactly what
            // `EEZ.sol`'s `_validateStructure` membership-checks (MAINNET(0) can
            // never be a registered member). Execution routes off each call's
            // `sourceRollupId` (`EEZ.sol _processNCalls`), never the entry's
            // destinationRollupId, so this is purely the settlement-attribution
            // id. Without this, `assert_batch_registry_native` (correctly)
            // rejects the dest=0 entry and the slot degrades to an empty batch
            // → the cross-chain call never settles on L1.
            entry.destinationRollupId = rollup_id_u256;
            // OUTBOUND value-bearing withdrawal: the entry's l2ToL1Calls carry V
            // (the value the user burned to SYSTEM_ADDRESS on L2). L1 execution
            // sends V from the rollup's escrowed etherBalance to the L1 target
            // (etherOut += V), so the settlement delta must DEBIT the rollup by V:
            // etherDelta = -etherOut. Satisfies EEZ's invariant
            // `totalEtherDelta == _entryEtherIn - etherOut` for an immediate
            // (_entryEtherIn == 0): `-V == 0 - V`. `outbound_ether_out` recovers V
            // (or 0 for a failed/value-free call, via the entry's rollingHash) —
            // None = unsupported multi-call-with-value, which we reject rather than
            // mis-book. Value-free (incl. multi-call): Some(0) -> etherDelta=0 ->
            // byte-identical. The rollup must hold >= V (funded by a prior inbound
            // deposit), else L1 reverts InsufficientRollupBalance.
            let ether_out = eez_evm::entries::outbound_ether_out(&entry).ok_or_else(|| {
                format!(
                    "outbound entry {k}: multi-call value not supported \
                     (callCount={}, l2ToL1Calls={})",
                    entry.callCount,
                    entry.l2ToL1Calls.len(),
                )
            })?;
            let ether_delta = if ether_out.is_zero() {
                alloy_primitives::I256::ZERO
            } else {
                -alloy_primitives::I256::try_from(ether_out).map_err(|e| {
                    format!("outbound entry {k}: etherOut {ether_out} overflows I256: {e}")
                })?
            };
            entry.stateDeltas = vec![eez_evm::types::StateDeltaSol {
                rollupId: rollup_id_u256,
                currentState: B256::ZERO,
                newState: sync_block_state_root,
                etherDelta: ether_delta,
            }];
            batch.inner.entries.insert(1 + k, entry);
        }

        // INBOUND deferred entries (proxyEntryHash != 0) arrive from the
        // survivor composition's `source.batch` with EMPTY stateDeltas — the
        // lean `build_l1_inbound_entry` shape defers the settlement delta to
        // here (its own comment: "the settlement delta is attached
        // downstream"). The anchor + outbound splices above each create one;
        // the inbound deferred entry had none. Attach one chained settlement
        // delta each — the inbound mirror of the outbound splice. WITHOUT it,
        // the bundled user_tx's `executeCrossChainCall` consume runs
        // `_applyStateDeltas([])` (a no-op): EEZ emits NO `L2ExecutionPerformed`
        // for the consume, so the deriver's per-block `settled_count` (it
        // counts that event, submitter.rs) misses it → `consumed_deferred = 0`
        // → it truncates the inbound delivery and never re-derives the L2
        // `executeIncomingCrossChainCall` system tx (the L2 call silently never
        // lands; Value stays 0). `currentState` is a placeholder fixed by the
        // stitch below; `newState` = the user-tx-inclusive sync-block root (the
        // post-delivery L2 root, set as the chain terminus by the anchor logic
        // below). No-op for an outbound-only slot (no proxyEntryHash != 0
        // entries) → byte-identical.
        // INBOUND value-bearing deposits. The lean on-chain inbound entry binds
        // the user's msg.value ONLY inside its proxyEntryHash preimage (its
        // l2ToL1Calls is empty). The POPULATED sidecar entry (targets[].batch —
        // same proxyEntryHash, since for an L1-originated inbound both hash
        // (rid, target, value, data, source, MAINNET=0)) carries the value
        // explicitly in l2ToL1Calls[0].value. Map proxyEntryHash -> +value so the
        // settlement delta below credits the rollup's L1 etherBalance by exactly
        // the deposited V. This satisfies EEZ's per-entry invariant
        // `totalEtherDelta == _entryEtherIn - etherOut` when the bundled L1 user tx
        // consumes the entry: _entryEtherIn = V (the value the user sent), etherOut
        // = 0 (a lean inbound entry has no l2ToL1Calls), so totalEtherDelta must be
        // +V. Value-free inbound: value==0 → absent from the map → etherDelta=0 →
        // byte-identical to before.
        let inbound_ether: HashMap<B256, alloy_primitives::I256> = compositions
            .iter()
            .flat_map(|c| c.targets.iter())
            .flat_map(|t| t.batch.entries().iter())
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

        for entry in &mut batch.inner.entries {
            if entry.proxyEntryHash != B256::ZERO && entry.stateDeltas.is_empty() {
                let ether_delta = inbound_ether
                    .get(&entry.proxyEntryHash)
                    .copied()
                    .unwrap_or(alloy_primitives::I256::ZERO);
                entry.stateDeltas = vec![eez_evm::types::StateDeltaSol {
                    rollupId: rollup_id_u256,
                    currentState: B256::ZERO,
                    newState: sync_block_state_root,
                    etherDelta: ether_delta,
                }];
            }
        }

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
        // Anchor the user-tx-inclusive root to the LAST OUTBOUND entry's delta
        // (proxyEntryHash==0 + non-empty l2ToL1Calls) — its L2→L1 call settles
        // IN this block. Else (inbound-only) anchor the last entry, whose
        // deferred chain lands on the same root once consumed. (Q2: explicit
        // last-outbound is future-proof for the mixed A2b batch, where
        // `entries.last()` is an inbound deferred entry, not the outbound one.)
        let anchor_idx = batch
            .inner
            .entries
            .iter()
            .rposition(|e| e.proxyEntryHash == B256::ZERO && !e.l2ToL1Calls.is_empty())
            .or_else(|| batch.inner.entries.len().checked_sub(1));
        if let Some(idx) = anchor_idx {
            if let Some(anchor_entry) = batch.inner.entries.get_mut(idx) {
                for delta in anchor_entry.stateDeltas.iter_mut().rev() {
                    if delta.rollupId == rollup_id_u256 {
                        delta.newState = sync_block_state_root;
                        break;
                    }
                }
            }
        }

        // transientExecutionEntryCount = 1 — only the leading immediate
        // entry should be drained inline at EEZ.sol:386. The remaining
        // cross-chain entries have proxyEntryHash != 0 → queue for
        // deferred consumption via executeCrossChainCall.
        // 1 leading anchor immediate + N outbound immediates (proxyEntryHash==0)
        // drained inline; inbound deferred entries (proxyEntryHash != 0) queue
        // for `executeCrossChainCall` consumption. N=0 for inbound-only → 1.
        batch.inner.transientExecutionEntryCount = U256::from(1 + outbound_entries.len() as u64);
        // Registry-id settlement gate (Phase E / C1) — refuse to settle a batch
        // carrying any non-registry id. See `assert_batch_registry_native`.
        assert_batch_registry_native(&batch, rollup_id_u256)?;
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
        // Sync block entry: the SYSTEM txs (exec_incoming / loadExecutionTable)
        // are reconstructed by the deriver from the postBatch entries, NOT
        // transported. The OUTBOUND user txs are NOT reconstructible (only the
        // system tx is), so they travel HERE in callData; the deriver
        // interleaves them with the reconstructed loads (A2.4). Inbound-only →
        // empty, exactly as Rollup-1 §8.3 (A2.3e / Q3).
        blocks.push(outbound_user_txs.iter().map(|b| b.to_vec()).collect());
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
        // The DA sidecar = the COMPLETE derivation entry set, in canonical order:
        // OUTBOUND L1-shape settlement entries (proxyEntryHash==0, populated
        // l2ToL1Calls — the deriver lowers them to loadExecutionTable) FIRST, THEN
        // the inbound deferred entries (composition targets). The deriver ALWAYS
        // uses this sidecar when non-empty (no on-chain fallback), so a MIXED slot
        // no longer drops the outbound — the dominant A2b blocker (a non-empty
        // inbound sidecar used to SUPPRESS the on-chain outbound). Single-direction
        // is preserved: outbound-only → [outbound] (the deriver reconstructs the
        // same loads it got from the on-chain fallback — it reads l2ToL1Calls[0] /
        // returnData, NOT stateDeltas); inbound-only → [inbound] (unchanged).
        let l2_entries_bytes: Vec<Vec<u8>> = outbound_entries
            .iter()
            .map(eez_evm::types::ExecutionEntrySol::abi_encode)
            .chain(
                compositions
                    .iter()
                    .flat_map(|c| c.targets.iter())
                    .flat_map(|t| t.batch.entries().iter())
                    .map(eez_evm::types::ExecutionEntrySol::abi_encode),
            )
            .collect();
        let payload = eez_payload_codec::encode(&blocks, &l2_entries_bytes)
            .map_err(|e| format!("eez_payload_codec::encode: {e}"))?;
        batch.inner.callData = alloy_primitives::Bytes::from(payload);

        // Prover-feed (P4-a): hand the settling block's PostBatch to the witness
        // task so it rides this Sync block's `ControlEvent.composition`. eez0's
        // batch is TIMELESS (blockNumber=0) → no l1_block_hash. Best-effort —
        // the sink is `None` outside composer-mode, and `build_post_batch_msg`
        // clears `proofs[]` (the prover fills them after attesting).
        if let Some(sink) = self.inner.postbatch_sink.get() {
            let pb =
                crate::post_batch_msg::build_post_batch_msg(&batch, self.inner.prover.vkey(), None);
            if let Ok(mut map) = sink.lock() {
                map.insert(sync_block_number, pb);
            }
        }

        // Deferred path (real proof system): the postBatch can't be signed
        // yet — its `proofs[]` must carry the prover's ECDSA attestation over
        // the publicInputsHash, which arrives out-of-process AFTER this block
        // commits. Recompute the hash now (the key the prover's signature
        // lands under in the ProofStore) and hand the assembled batch to the
        // caller, who spawns the deferred-dispatch task. KEEP the mock proof
        // already set in `proofs[]` above — it's harmlessly overwritten by
        // `apply_proof`, and `public_inputs_hashes` ignores `proofs[]`.
        if self.deferred_post() {
            let public_inputs_hash = eez_evm::public_inputs::public_inputs_hashes(
                &batch,
                self.inner.prover.vkey(),
                None,
            )
            .map_err(|e| format!("public_inputs_hashes (deferred): {e}"))?
            .first()
            .copied()
            .ok_or("public_inputs_hashes returned no hashes (deferred)")?;
            return Ok(PostBatchOutcome::Deferred {
                batch: Box::new(batch),
                public_inputs_hash,
                posted,
            });
        }

        // Synchronous path (mock proof system): encode + sign via the shared
        // seam now. The deferred path reuses the SAME seam after filling
        // proofs[] from the prover's real attestation.
        Ok(PostBatchOutcome::Ready(
            self.finalize_post_batch_tx(&batch, ctx).await?,
        ))
    }

    /// Encode the FILLED batch (entries + callData + `proofs[]` all set) as
    /// `EEZ.postAndVerifyBatch` calldata and sign the L1 postBatch tx — the seam
    /// the deferred post (P4-b-full) reuses: build the batch once, fill `proofs[]`
    /// from the prover's attestation (`crate::proof_sink::apply_proof`) when it
    /// arrives, then call this to produce the raw L1 tx.
    async fn finalize_post_batch_tx(
        &self,
        batch: &eez_evm::EvmBatch,
        ctx: &CrossChainExecCtx,
    ) -> Result<Bytes, String> {
        use alloy_sol_types::SolCall as _;
        use eez_evm::types::postAndVerifyBatchCall;

        let calldata = postAndVerifyBatchCall {
            batch: batch.inner.clone(),
        }
        .abi_encode();

        // EEZ registry address is per-deployment; read directly from env. Loud
        // failure on absence/garbage (invariant 7) — a postBatch signed to
        // Address::ZERO would silently no-op on L1 with only WARN breadcrumbs.
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
    target: BundleTarget,
) {
    let outcome = submitter
        .send_bundle(&bundle, target, Some(expected_final_state))
        .await;
    // Strict all-or-nothing: Included ⟹ settled; the only failure is a drop.
    // Poison is caught at compose time, so a drop here is bad luck — recovery
    // recomposes a FRESH tx next trigger (the builder ignores re-sends).
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
    // entry apply. Actual postBatch gas usage is ~500K-600K even for a
    // wide catch-up span (measured 564600 for a ~13k-block span); 1.5M
    // is ~2.5x safety and still well under chiado's 17M block gas limit.
    //
    // WHY LOWER THAN 4M (2026-06-24): EIP-1559 reserves gas_limit*maxFee
    // upfront. At 4M*10gwei that reserve is ~0.04 xDAI; the poster ran
    // OUT of affordable balance there (settlement froze with the poster
    // at 0.0397). 1.5M cuts the reserve to ~0.015, so a low-balance
    // poster can still post (the actual CHARGE is gas_used, unchanged).
    const POST_BATCH_GAS_LIMIT: u64 = 1_500_000;

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

/// Registry-id settlement gate (Phase E / C1, ported from based-rollup
/// `composer-lib/post_batch_submitter.rs`): refuse to settle a batch carrying
/// any id that isn't this rollup's configured registry id. A non-registry id
/// reaching the settlement batch is a WIRING BUG — the L1 would misattribute
/// the batch, and these ids fold into the `publicInputsHash`. Trivially passes
/// today (single rollup, `chainId == registry == 1`); guards future
/// multi-rollup / `chainId != registry` topologies.
fn assert_batch_registry_native(batch: &eez_evm::EvmBatch, rid: U256) -> Result<(), String> {
    for (i, entry) in batch.inner.entries.iter().enumerate() {
        if entry.destinationRollupId != rid {
            return Err(format!(
                "entry[{i}].destinationRollupId = {} is not the configured registry id {rid} — \
                 a non-registry id reached the settlement batch (composition must be registry-native)",
                entry.destinationRollupId,
            ));
        }
        for (j, call) in entry.l2ToL1Calls.iter().enumerate() {
            if call.sourceRollupId != rid {
                return Err(format!(
                    "entry[{i}].l2ToL1Calls[{j}].sourceRollupId = {} is not the configured registry id {rid}",
                    call.sourceRollupId,
                ));
            }
        }
    }
    for (i, lookup) in batch.inner.l1ToL2lookupCalls.iter().enumerate() {
        if lookup.destinationRollupId != rid {
            return Err(format!(
                "l1ToL2lookupCalls[{i}].destinationRollupId = {} is not the configured registry id {rid}",
                lookup.destinationRollupId,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eez_evm::types::{ExecutionEntrySol, L2ToL1CallSol, LookupCallSol};

    fn entry_with(dest: u64, src: u64) -> ExecutionEntrySol {
        ExecutionEntrySol {
            stateDeltas: Vec::new(),
            proxyEntryHash: B256::ZERO,
            destinationRollupId: U256::from(dest),
            l2ToL1Calls: vec![L2ToL1CallSol {
                targetAddress: Address::ZERO,
                value: U256::ZERO,
                data: Bytes::new(),
                sourceAddress: Address::ZERO,
                sourceRollupId: U256::from(src),
                revertSpan: U256::ZERO,
            }],
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

    #[test]
    fn registry_native_batch_passes() {
        let rid = U256::from(1u64);
        let mut batch = eez_evm::EvmBatch::default();
        batch.inner.entries.push(entry_with(1, 1));
        batch.inner.l1ToL2lookupCalls.push(lookup_with(1));
        assert!(assert_batch_registry_native(&batch, rid).is_ok());
    }

    #[test]
    fn foreign_entry_destination_id_rejected() {
        let rid = U256::from(1u64);
        let mut batch = eez_evm::EvmBatch::default();
        batch.inner.entries.push(entry_with(2, 1));
        assert!(assert_batch_registry_native(&batch, rid).is_err());
    }

    #[test]
    fn foreign_call_source_id_rejected() {
        let rid = U256::from(1u64);
        let mut batch = eez_evm::EvmBatch::default();
        batch.inner.entries.push(entry_with(1, 7));
        assert!(assert_batch_registry_native(&batch, rid).is_err());
    }

    #[test]
    fn foreign_lookup_destination_id_rejected() {
        let rid = U256::from(1u64);
        let mut batch = eez_evm::EvmBatch::default();
        batch.inner.l1ToL2lookupCalls.push(lookup_with(9));
        assert!(assert_batch_registry_native(&batch, rid).is_err());
    }
}
