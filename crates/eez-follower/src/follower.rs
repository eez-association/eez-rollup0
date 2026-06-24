//! Sequencer-RPC unsafe-head follower.
//!
//! Safe and finalized are owned by the L1 deriver through
//! `BlockCommitterHandle::advance_safe_finalized`. This task only asks
//! reth to point unsafe head at the sequencer's latest block while that
//! head remains compatible with the L1-derived anchors.
//!
//! Compatibility is enforced here, not by reth: the engine only requires
//! safe/finalized to be *known*, so it would canonicalize a head whose
//! ancestry conflicts with our safe block. Before each FCU we accept heads
//! already on the local canonical chain, otherwise walk the candidate's local
//! ancestry to the safe height and reject proven conflicts; unverifiable
//! (unsynced) ancestry stays optimistic.

use std::time::Duration;

use alloy_eips::{BlockNumHash, BlockNumberOrTag};
use alloy_provider::{Provider, RootProvider};
use eez_driver::{BlockCommitterHandle, ForkchoiceOutcome};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_primitives_traits::SealedHeader;
use reth_storage_api::{BlockIdReader, HeaderProvider};
use tracing::{Level, event};

use crate::error::FollowerError;

/// Keepalive cadence — re-publish the current forkchoice state so reth's
/// engine view doesn't drift during quiet periods.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Upper bound on the candidate→safe ancestry walk. Beyond this, a fully
/// local gap is treated as unverifiable rather than scanned unboundedly.
const MAX_ANCESTRY_WALK: u64 = 1024;

/// Verdict of checking a candidate head against the local safe anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SafeCompat {
    /// The candidate provably descends from the safe block.
    Extends,
    /// The candidate provably does NOT descend from the safe block.
    Conflicts,
    /// Ancestry is not locally known (not yet synced) — optimistic.
    Unverifiable,
}

/// Polls the sequencer RPC for unsafe heads and routes every engine call
/// through the shared [`BlockCommitterHandle`].
#[derive(Debug)]
pub(crate) struct Follower<P> {
    committer: BlockCommitterHandle<EthEngineTypes>,
    sequencer_rpc: RootProvider,
    /// Local chain reader — resolves the current safe anchor and the
    /// candidate head's ancestry for the compatibility check.
    local: P,
    /// Cadence for polling the sequencer RPC for new unsafe heads. The
    /// branch's eez-driver has no lightweight scheduler type (slot-based
    /// scheduling replaced it), so the follower drives its own interval.
    poll_interval: Duration,
    last_head: Option<alloy_primitives::B256>,
}

impl<P> Follower<P>
where
    P: HeaderProvider<Header = alloy_consensus::Header> + BlockIdReader,
{
    pub(crate) fn new(
        committer: BlockCommitterHandle<EthEngineTypes>,
        sequencer_rpc: RootProvider,
        local: P,
        poll_interval: Duration,
    ) -> Self {
        Self {
            committer,
            sequencer_rpc,
            local,
            poll_interval,
            last_head: None,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut poll = tokio::time::interval(self.poll_interval);
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                _ = poll.tick() => {
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

        // Reject candidates whose ancestry provably conflicts with the
        // L1-derived safe anchor before they reach the engine.
        if self.check_extends_safe(&block.header.inner, hash)? == SafeCompat::Conflicts {
            event!(
                name: "eez.follower.head.conflicts_safe",
                Level::WARN,
                block.number = number,
                block.hash = %hash,
                "sequencer head does not descend from the L1-derived safe block (stale or \
                 race-losing branch); skipping",
            );
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

    /// Checks whether `candidate` descends from the current safe block. Steady
    /// state O(1): if the candidate is already local canonical, the local
    /// forkchoice invariant proves it extends safe; otherwise the first parent
    /// lookup usually resolves or misses (unverifiable).
    fn check_extends_safe(
        &self,
        candidate: &alloy_consensus::Header,
        candidate_hash: alloy_primitives::B256,
    ) -> Result<SafeCompat, FollowerError> {
        let Some(safe) = self
            .local
            .safe_block_num_hash()
            .map_err(|e| FollowerError::Provider(format!("safe_block_num_hash: {e}")))?
        else {
            // No safe anchor recorded yet — nothing to conflict with.
            return Ok(SafeCompat::Extends);
        };
        let BlockNumHash {
            number: safe_number,
            hash: safe_hash,
        } = safe;

        // A head at the safe height must BE the safe block; below it,
        // it can never extend it.
        if candidate.number == safe_number {
            return Ok(if candidate_hash == safe_hash {
                SafeCompat::Extends
            } else {
                SafeCompat::Conflicts
            });
        }
        if candidate.number < safe_number {
            return Ok(SafeCompat::Conflicts);
        }

        // If reth already has this exact block on its canonical chain, it must
        // extend the local safe anchor. Otherwise, walk the candidate's explicit
        // parent-hash chain below.
        if self
            .local
            .sealed_header(candidate.number)
            .map_err(|e| {
                FollowerError::Provider(format!("sealed_header({}): {e}", candidate.number))
            })?
            .is_some_and(|local| local.hash() == candidate_hash)
        {
            return Ok(SafeCompat::Extends);
        }

        let mut cursor_hash = candidate.parent_hash;
        let mut cursor_number = candidate.number - 1;
        let mut reads = 0;
        while cursor_number > safe_number {
            if reads >= MAX_ANCESTRY_WALK {
                return Ok(SafeCompat::Unverifiable);
            }
            let Some(parent) = self
                .local
                .header(cursor_hash)
                .map_err(|e| FollowerError::Provider(format!("header({cursor_hash}): {e}")))?
            else {
                return Ok(SafeCompat::Unverifiable);
            };
            cursor_hash = parent.parent_hash;
            cursor_number -= 1;
            reads += 1;
        }
        Ok(if cursor_hash == safe_hash {
            SafeCompat::Extends
        } else {
            SafeCompat::Conflicts
        })
    }
}
