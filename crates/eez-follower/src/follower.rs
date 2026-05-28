//! Sequencer-RPC unsafe-head follower.
//!
//! Safe and finalized are owned by the L1 deriver through
//! `BlockCommitterHandle::advance_safe_finalized`. This task only asks
//! reth to point unsafe head at the sequencer's latest block while that
//! head remains compatible with the L1-derived anchors.

use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, RootProvider};
use eez_driver::{BlockCommitterHandle, ForkchoiceOutcome, Scheduler};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_primitives_traits::SealedHeader;
use tracing::{Level, event};

use crate::error::FollowerError;

/// Keepalive cadence — re-publish the current forkchoice state so reth's
/// engine view doesn't drift during quiet periods.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Polls the sequencer RPC for unsafe heads and routes every engine call
/// through the shared [`BlockCommitterHandle`].
#[derive(Debug)]
pub(crate) struct Follower {
    committer: BlockCommitterHandle<EthEngineTypes>,
    sequencer_rpc: RootProvider,
    scheduler: Scheduler,
    last_head: Option<alloy_primitives::B256>,
}

impl Follower {
    pub(crate) fn new(
        committer: BlockCommitterHandle<EthEngineTypes>,
        sequencer_rpc: RootProvider,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            committer,
            sequencer_rpc,
            scheduler,
            last_head: None,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                _ = self.scheduler.next() => {
                    if let Err(err) = self.advance().await {
                        event!(
                            name: "eez.follower.advance.failed",
                            Level::WARN,
                            error = %err,
                            "sequencer unsafe-head advance failed; will retry",
                        );
                    }
                }
                _ = fcu_interval.tick() => {
                    if let Err(err) = self.committer.refresh_forkchoice().await {
                        event!(
                            name: "eez.follower.fcu_refresh.failed",
                            Level::WARN,
                            error = %err,
                            "forkchoice refresh failed",
                        );
                    }
                }
            }
        }
    }

    async fn advance(&mut self) -> Result<(), FollowerError> {
        let Some(block) = self
            .sequencer_rpc
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await
            .map_err(|e| FollowerError::Rpc(e.to_string()))?
        else {
            event!(
                name: "eez.follower.block.not_yet_produced",
                Level::DEBUG,
                "sequencer reports no latest block yet",
            );
            return Ok(());
        };

        let number = block.header.number;
        let hash = block.header.hash;
        if self.last_head == Some(hash) {
            return Ok(());
        }

        let header = SealedHeader::new(block.header.inner, hash);
        match self.committer.advance_unsafe_head(header).await {
            Ok(ForkchoiceOutcome::Valid) => {
                self.last_head = Some(hash);
                event!(
                    name: "eez.follower.head.advanced",
                    Level::INFO,
                    block.number = number,
                    block.hash = %hash,
                    "follower advanced unsafe head to sequencer block",
                );
                Ok(())
            }
            Ok(ForkchoiceOutcome::Syncing) => {
                event!(
                    name: "eez.follower.head.syncing",
                    Level::INFO,
                    block.number = number,
                    block.hash = %hash,
                    "reth accepted sequencer head as a sync target",
                );
                Ok(())
            }
            Ok(ForkchoiceOutcome::InvalidState) => {
                event!(
                    name: "eez.follower.head.inconsistent",
                    Level::WARN,
                    block.number = number,
                    block.hash = %hash,
                    "sequencer head is incompatible with current L1-derived safe/finalized anchors; dropping unsafe head",
                );
                Ok(())
            }
            Err(err) => Err(FollowerError::Driver(err.to_string())),
        }
    }
}
