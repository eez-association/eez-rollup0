//! Engine-API consumer that advances the chain one block per scheduler tick.
//!
//! [`Sequencer::run`] is the long-running loop. Each iteration either:
//!
//! 1. Receives a [`ProposalRequest`](crate::ProposalRequest) from the
//!    scheduler and runs [`Sequencer::advance`]: sends
//!    `engine_forkchoiceUpdated` with fresh payload attributes, resolves the
//!    returned payload id, sends `engine_newPayload`, and updates the local
//!    head cursor. Or
//! 2. Re-publishes the current forkchoice state on a slower timer so the
//!    engine's view doesn't drift during quiet periods.
//!
//! Stage 1 hard-codes Ethereum's payload types and the attribute construction.
//! When stage 4 introduces composer-supplied sync-block contents, the
//! [`EthAttributesBuilder`] grows additional fields (e.g., a "system txs"
//! input) rather than the [`Sequencer`] loop itself.

use core::fmt;
use std::{collections::VecDeque, sync::Arc, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_rpc_types_engine::ForkchoiceState;
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthPayloadAttributes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{BuiltPayload, PayloadKind, PayloadTypes};
use reth_primitives_traits::{AlloyBlockHeader, HeaderTy, SealedHeader, SealedHeaderFor};
use reth_storage_api::BlockReader;
use tracing::{Level, event};

use crate::error::{DriverError, DriverResult};
use crate::scheduler::{ProposalRequest, Scheduler};

/// How many recent block hashes we retain for safe/finalized forkchoice
/// tracking.
///
/// Reth treats forkchoice state as `head/safe/finalized` triplets. Until L1
/// derivation lands (stage 3), we pick "safe = head - 32" and
/// "finalized = head - 64" — large enough that those pointers don't move
/// every block (so a downstream observer sees a stable safe head) but small
/// enough that we don't unnecessarily delay finalization signals. At a 2s
/// block time, 64 blocks ≈ 2 minutes of history.
const FORKCHOICE_HISTORY: usize = 64;
const SAFE_LAG: usize = 32;
const FINALIZED_LAG: usize = 64;

/// How often the sequencer re-publishes the current forkchoice state.
///
/// Even when no new block is being proposed, sending an FCU keeps reth's
/// engine convinced the chain is alive. Mirrors the cadence reth's own dev
/// miner uses.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Builds [`EthPayloadAttributes`] for the next block.
///
/// Stage 1 produces minimal valid attributes: a strictly-increasing timestamp
/// honoring the scheduler's target, a placeholder `prev_randao`, a
/// caller-configured `suggested_fee_recipient`, and the post-Shanghai /
/// post-Cancun fields filled in based on the chainspec.
///
/// Stage 3 will replace the placeholder `prev_randao` with an L1-derived
/// value per the protocol spec (§13.14). Stage 4 will add a hook for
/// composer-supplied system transactions.
#[derive(Debug, Clone)]
pub struct EthAttributesBuilder<ChainSpec> {
    chain_spec: Arc<ChainSpec>,
    suggested_fee_recipient: Address,
}

impl<ChainSpec> EthAttributesBuilder<ChainSpec> {
    /// Creates a builder using the given chainspec and a zero fee recipient.
    #[must_use]
    pub const fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            chain_spec,
            suggested_fee_recipient: Address::ZERO,
        }
    }

    /// Configures the `suggested_fee_recipient` field on built attributes.
    #[must_use]
    pub const fn with_fee_recipient(mut self, recipient: Address) -> Self {
        self.suggested_fee_recipient = recipient;
        self
    }
}

impl<ChainSpec> EthAttributesBuilder<ChainSpec>
where
    ChainSpec: EthChainSpec + EthereumHardforks + Send + Sync + 'static,
{
    /// Builds attributes for the block following `parent` with timestamp
    /// `target_timestamp` (clamped upward to preserve strict monotonicity).
    #[must_use]
    pub fn build(
        &self,
        parent: &SealedHeader<ChainSpec::Header>,
        target_timestamp: u64,
    ) -> EthPayloadAttributes {
        let timestamp = core::cmp::max(parent.timestamp().saturating_add(1), target_timestamp);
        EthPayloadAttributes {
            timestamp,
            // TODO(stage 3): derive from L1 per spec §13.14. Zeroed for
            // stage 1 — no security depends on prev_randao yet.
            prev_randao: B256::ZERO,
            suggested_fee_recipient: self.suggested_fee_recipient,
            withdrawals: self
                .chain_spec
                .is_shanghai_active_at_timestamp(timestamp)
                .then(Default::default),
            parent_beacon_block_root: self
                .chain_spec
                .is_cancun_active_at_timestamp(timestamp)
                .then_some(B256::ZERO),
            // Amsterdam-fork addition; not active for stage-1 dev chains.
            slot_number: None,
        }
    }
}

/// Drives a reth node by translating scheduler proposals into engine-API
/// `forkchoiceUpdated` + `newPayload` round-trips.
pub struct Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    attributes: EthAttributesBuilder<ChainSpec>,
    to_engine: ConsensusEngineHandle<T>,
    payload_builder: PayloadBuilderHandle<T>,
    scheduler: Scheduler,
    last_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    recent_hashes: VecDeque<B256>,
}

impl<T, ChainSpec> fmt::Debug for Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequencer")
            .field("head", &self.recent_hashes.back())
            .field("recent_hashes", &self.recent_hashes.len())
            .finish_non_exhaustive()
    }
}

impl<T, ChainSpec> Sequencer<T, ChainSpec>
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes>,
    ChainSpec: EthChainSpec<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>
        + EthereumHardforks
        + Send
        + Sync
        + 'static,
{
    /// Constructs a new sequencer by reading the current best block from
    /// `provider` and binding the engine handles.
    ///
    /// # Errors
    ///
    /// Returns [`DriverError::is_provider`] if the provider lookups fail, or
    /// [`DriverError::is_missing_header`] if the best block doesn't have a
    /// header (only happens during a brief startup race).
    pub fn new(
        provider: &impl BlockReader<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>,
        attributes: EthAttributesBuilder<ChainSpec>,
        to_engine: ConsensusEngineHandle<T>,
        scheduler: Scheduler,
        payload_builder: PayloadBuilderHandle<T>,
    ) -> DriverResult<Self> {
        let best = provider
            .best_block_number()
            .map_err(DriverError::provider)?;
        let last_header = provider
            .sealed_header(best)
            .map_err(DriverError::provider)?
            .ok_or_else(|| DriverError::missing_header(best))?;
        let recent_hashes = VecDeque::from([last_header.hash()]);
        Ok(Self {
            attributes,
            to_engine,
            payload_builder,
            scheduler,
            last_header,
            recent_hashes,
        })
    }

    /// Runs the sequencer loop until cancellation.
    ///
    /// Errors during an advance are logged and the loop continues — a single
    /// failed FCU / `newPayload` round-trip shouldn't kill the node.
    /// Recoverable failures show up in metrics and traces; permanent failure
    /// modes require an explicit shutdown signal (not yet wired).
    pub async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                req = self.scheduler.next() => {
                    if let Err(err) = self.advance(req).await {
                        event!(
                            name: "eez.sequencer.advance.failed",
                            Level::ERROR,
                            error = %err,
                            "advance failed: {{error}}",
                        );
                    }
                }
                _ = fcu_interval.tick() => {
                    if let Err(err) = self.refresh_forkchoice().await {
                        event!(
                            name: "eez.sequencer.fcu.failed",
                            Level::WARN,
                            error = %err,
                            "forkchoice refresh failed: {{error}}",
                        );
                    }
                }
            }
        }
    }

    fn forkchoice_state(&self) -> ForkchoiceState {
        let head = *self
            .recent_hashes
            .back()
            .expect("recent_hashes is never empty post-new");
        let safe = *self
            .recent_hashes
            .get(self.recent_hashes.len().saturating_sub(SAFE_LAG))
            .expect("len >= 1 keeps index valid under saturating_sub");
        let finalized = *self
            .recent_hashes
            .get(self.recent_hashes.len().saturating_sub(FINALIZED_LAG))
            .expect("len >= 1 keeps index valid under saturating_sub");
        ForkchoiceState {
            head_block_hash: head,
            safe_block_hash: safe,
            finalized_block_hash: finalized,
        }
    }

    async fn refresh_forkchoice(&self) -> DriverResult<()> {
        let state = self.forkchoice_state();
        let res = self
            .to_engine
            .fork_choice_updated(state, None)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !res.is_valid() {
            return Err(DriverError::invalid_forkchoice(format!("{res:?}")));
        }
        Ok(())
    }

    async fn advance(&mut self, req: ProposalRequest) -> DriverResult<()> {
        let attrs = self
            .attributes
            .build(&self.last_header, req.target_timestamp);
        let timestamp = attrs.timestamp;

        let fcu = self
            .to_engine
            .fork_choice_updated(self.forkchoice_state(), Some(attrs))
            .await
            .map_err(DriverError::engine_rpc)?;
        if !fcu.is_valid() {
            return Err(DriverError::invalid_forkchoice(format!("{fcu:?}")));
        }
        let payload_id = fcu.payload_id.ok_or_else(DriverError::payload_missing)?;

        let payload = self
            .payload_builder
            .resolve_kind(payload_id, PayloadKind::WaitForPending)
            .await
            .ok_or_else(DriverError::payload_missing)?
            .map_err(DriverError::engine_rpc)?;

        let header = payload.block().sealed_header().clone();
        let block_number = header.number();
        let block_hash = header.hash();
        let exec_payload = T::block_to_payload(payload.block().clone());

        let np = self
            .to_engine
            .new_payload(exec_payload)
            .await
            .map_err(DriverError::engine_rpc)?;
        if !np.is_valid() {
            return Err(DriverError::invalid_payload(format!("{np:?}")));
        }

        self.recent_hashes.push_back(block_hash);
        if self.recent_hashes.len() > FORKCHOICE_HISTORY {
            self.recent_hashes.pop_front();
        }
        self.last_header = header;

        event!(
            name: "eez.sequencer.block.produced",
            Level::INFO,
            slot.kind = %req.kind,
            block.number = block_number,
            block.hash = %block_hash,
            block.timestamp = timestamp,
            "produced block {{block.number}} hash={{block.hash}} ts={{block.timestamp}}",
        );

        Ok(())
    }
}
