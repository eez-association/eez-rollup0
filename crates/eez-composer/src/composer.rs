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
//! Each batch is attested by the shared [`AttestationQuorum`] and sent via the shared
//! [`Submitter`] bundle relay.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_eips::Encodable2718;
use alloy_primitives::Address;
use async_trait::async_trait;
use eez_driver::{
    BlockCommitterHandle, ParentContext, RollupTiming, SyncSlotBlock, SyncSlotComposer,
    SyncSlotMode,
};
use eez_engine::EezEngineTypes;
use eez_evm::EezEvmConfig;
use eez_l1::{L1Event, Submitter};
use eez_timing::SharedClock;
use reth_primitives_traits::AlloyBlockHeader;
use reth_storage_api::{BlockReader, StateProviderFactory, TransactionsProvider};
use tokio::sync::broadcast;
use tracing::{Level, event};

use crate::attestation_quorum::AttestationQuorum;
use crate::rollup::RollupState;

mod batcher;
mod blob;
mod budget;
mod compose;
mod post_batch;
mod recovery;
use batcher::{EmissionLimits, SlotTarget, bundle_targets_for_slot, ensure_submission_identity};
pub use blob::BlobPostingConfig;
use blob::blob_fee;
pub use recovery::MAX_BUNDLE_ATTEMPTS;
use recovery::RecoveryOutcome;

/// Shared contract identities, public L2 parameters, and L1 submission resources
/// for cross-chain composition on Sync slots. Owned by `eez-node` at startup
/// and carried inside [`CrossChainWiring`].
#[derive(Clone)]
pub struct CrossChainExecCtx {
    /// `EEZL2` address, where SYSTEM_ADDRESS calls both
    /// `loadExecutionTable` and `executeIncomingCrossChainCall`.
    pub eezl2_address: Address,
    /// L2 chain id for native transaction replay protection.
    pub l2_chain_id: u64,
    /// Alloy provider for the embedded L1 RPC. Used to sign the
    /// `postAndVerifyBatch` transaction (nonce + fee reads). Submission goes
    /// through `submitter`.
    pub l1_provider: alloy_provider::RootProvider,
    /// Shared `Submitter` handle — the single L1 submission path.
    /// `Submitter` is internally `Arc<Inner>`, so `Clone` is cheap.
    /// `compose_sync_slot` hands it `[postBatch, inbound_user_tx_1, …]` via
    /// `Submitter::send_bundle`; outbound user transactions execute in the L2
    /// Sync block instead. The Submitter owns the transport decision (atomic
    /// `eth_sendBundle` on supporting relays, the postBatch alone through the
    /// mempool on plain execution RPCs).
    pub submitter: eez_l1::Submitter,
    /// L1 EOA whose key signs the `postAndVerifyBatch` transaction. Different from
    /// the reserved L2 SYSTEM_ADDRESS. For dev
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
    /// The EEZ registry, which names the settled rollup's manager contract
    /// (`Rollup.sol`). Several attesters read its `threshold()` and their
    /// registration for every batch, since the owner may change them.
    pub eez_registry: Address,
    /// Blob posting configuration; `None` selects calldata.
    pub blob_posting: Option<BlobPostingConfig>,
}

impl std::fmt::Debug for CrossChainExecCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainExecCtx")
            .field("system_address", &eez_primitives::SYSTEM_ADDRESS)
            .field("eezl2_address", &self.eezl2_address)
            .field("l2_chain_id", &self.l2_chain_id)
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
    /// Runtime context for deriving and encoding L2 system transactions from
    /// composition batches.
    pub exec_ctx: Arc<CrossChainExecCtx>,
    /// Concrete local clients for slot-scoped chained composition, un-erased so
    /// the drain can reach `L1SlotState` and `simulate_source_tx_on`. The same
    /// instances registered in `rollups`; they share one overlay channel.
    pub local: crate::local::LocalComposeClients,
}

impl std::fmt::Debug for CrossChainWiring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossChainWiring")
            .field("entry_rollup_id", &self.entry_rollup_id)
            .field("rollups", &self.rollups.len())
            .finish()
    }
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

struct Inner<L2: BlockReader> {
    /// Per-rollup state keyed by rollup ID.
    rollups: HashMap<u64, RollupState<L2>>,
    /// Shared across rollups: one attester set, one submitter.
    quorum: AttestationQuorum,
    submitter: Submitter,
    /// EVM config — used by [`build_sync_block`](crate::local::build_sync_block) to construct the
    /// per-Sync-slot block via reth-evm `BlockBuilder`.
    evm_config: EezEvmConfig,
    /// Cross-chain clients and execution context, guaranteed by the
    /// `eez-composer` entrypoint.
    cross_chain: CrossChainWiring,
    /// Handle to the binary-owned `BlockCommitter` actor (the sole engine-API
    /// owner), shared with the Sequencer and Deriver. Slot-context recovery
    /// uses it to reorg an optimistically committed Sync block after L1 failure.
    committer: BlockCommitterHandle<EezEngineTypes>,
    /// Per-block witnesses for [`eez_prover::ProvingContext::blocks`]. `None`
    /// means the configured in-process prover does not require block witnesses;
    /// it does not mean that the Composer has no prover.
    witness_source: Option<Arc<dyn eez_prover::ProvingWitnessSource>>,
    /// Bounds on what one postBatch may settle. Read from env once here so
    /// the emission decision, the boundary math, and the span guard in
    /// `prepare_post_batch_raw` can never disagree about the cap mid-run.
    emission: EmissionLimits,
    /// Wall clock for the proof-start cutoff.
    clock: SharedClock,
}

impl<L2: BlockReader> std::fmt::Debug for Composer<L2> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Composer")
            .field("rollup_ids", &self.inner.rollups.keys().collect::<Vec<_>>())
            .field("quorum", &self.inner.quorum)
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
        quorum: AttestationQuorum,
        evm_config: EezEvmConfig,
        cross_chain: CrossChainWiring,
        committer: BlockCommitterHandle<EezEngineTypes>,
        witness_source: Option<Arc<dyn eez_prover::ProvingWitnessSource>>,
        timing: RollupTiming,
        clock: SharedClock,
    ) -> Result<Self, ComposerConfigError> {
        let submitter = cross_chain.exec_ctx.submitter.clone();
        let submitter_poster = submitter.poster_address();
        let post_batch_signer = cross_chain.exec_ctx.l1_poster_signer.address();
        ensure_submission_identity(submitter_poster, post_batch_signer)?;

        let emission = EmissionLimits::from_env(timing, quorum.len() as u64);
        Ok(Self {
            inner: Arc::new(Inner {
                rollups,
                quorum,
                submitter,
                evm_config,
                cross_chain,
                committer,
                witness_source,
                emission,
                clock,
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
        // Steady bundles pin both block and timestamp. Ordinary catch-up
        // markers remain next-available so an L1 reorg cannot strand them,
        // while cap-triggered historical chunks name the known next block
        // without a timestamp pin. None is reserved for deferred-late paths.
        let (bundle_target, historical_target) =
            bundle_targets_for_slot(target_l1_block, timestamp, mode);
        event!(
            name: "eez.composer.sync_slot.invoked",
            Level::INFO,
            event_name = "eez.composer.sync_slot.invoked",
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
        let cc = &self.inner.cross_chain;
        // Use the Sequencer-supplied parent header directly (it reflects
        // the just-committed block via `last_header`'s mirror); a
        // best-block re-lookup can race reth's provider-index and build
        // on a stale parent (see [`eez_driver::ParentContext`]).
        let parent_header = parent.header;
        let parent_number = parent_header.number();
        let suggested_fee_recipient = rollup.config.fee_recipient;

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
        if let Some(outcome) = self.reconcile_slot_batches(rollup_id, rollup, cursor).await {
            // A moved head makes this slot's parent stale — the Sequencer's bail
            // covers it. An UNCHANGED head (reinsert / stale-verdict) still owes
            // the slot a block, and returning None there would let the
            // mempool-fed commit_one fallback mint a tx-bearing grid block.
            return match outcome {
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
        // Over the fee cap no blob post can land, so nothing is composed until
        // the fee falls; held transactions wait uncharged.
        if let Some(config) = &cc.exec_ctx.blob_posting
            && let Err(error) = blob_fee(&cc.exec_ctx.l1_provider, config).await
        {
            event!(
                name: "eez.composer.sync_slot.blob_fee_unavailable",
                Level::WARN,
                rollup_id,
                error = %error,
                "no landable blob fee; committing Sync block without emission this slot",
            );
            return self.build_empty_slot_block(rollup, &parent_header, timestamp);
        }

        // ── Bounded emission ─────────────────────────────────────────
        // A stall holds `cursor` still while the head keeps cadence, growing the
        // range by K a slot until the signer's window wedges it.
        if sync_height.saturating_sub(cursor) > self.inner.emission.max_blocks {
            if let Err(err) = self
                .emit_historical_chunk(
                    &cc.exec_ctx,
                    rollup_id,
                    rollup,
                    cursor,
                    sync_height,
                    historical_target,
                )
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
        // emission only. Ordered after bounded emission on purpose — this path
        // supplies NextBlock, so its historical chunk carries no pin and
        // remains valid; checking first would starve a chronically late node
        // (slow prover, relay latency) of settlement entirely.
        if matches!(mode, SyncSlotMode::Empty) {
            return self.build_empty_slot_block(rollup, &parent_header, timestamp);
        }

        // Catchup: structural-only — skip the drain, emit a minimal postBatch
        // (cross-chain stays pooled for the next Steady slot).
        if matches!(mode, SyncSlotMode::Catchup) {
            let slot = SlotTarget {
                parent_header: &parent_header,
                timestamp,
                fee_recipient: suggested_fee_recipient,
                bundle_target, // catch-up → NextBlock (unpinned)
            };
            return self
                .dispatch_minimal_postbatch(&cc.exec_ctx, rollup_id, rollup, slot)
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

        self.compose_steady_slot(
            rollup_id,
            rollup,
            &parent_header,
            timestamp,
            bundle_target,
            cursor,
        )
        .await
    }

    /// Run recovery on every tick, not just Sync slots, so a mid-slot verdict
    /// isn't held for K blocks. Idempotent: no Failed entry means no work.
    async fn recover_failed(&self, rollup_id: u64) {
        let Some(rollup) = self.inner.rollups.get(&rollup_id) else {
            return;
        };
        // Deliberately ignored: a tick owes no block, and the Sequencer
        // re-reads the head right after this hook returns.
        let _ = self
            .recover_batches(rollup_id, rollup, rollup.l1_head.cursor())
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::held_pool::HeldTx;
    use crate::ingress::Direction;
    use alloy_primitives::{B256, Bytes, TxHash, U256};

    pub(super) fn held(sender: Address, direction: Direction, nonce: u64, hash_byte: u8) -> HeldTx {
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

    pub(super) fn signed_user_tx(gas_limit: u64) -> Bytes {
        use alloy_consensus::{SignableTransaction as _, TxEip1559, TxEnvelope};
        use alloy_network::{TxSignerSync as _, eip2718::Encodable2718 as _};
        use alloy_primitives::TxKind;
        use alloy_signer_local::PrivateKeySigner;

        let signer = PrivateKeySigner::from_bytes(&B256::with_last_byte(1)).unwrap();
        let mut tx = TxEip1559 {
            chain_id: 6290,
            nonce: 0,
            gas_limit,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let signature = signer.sign_transaction_sync(&mut tx).unwrap();
        Bytes::from(TxEnvelope::from(tx.into_signed(signature)).encoded_2718())
    }
}
