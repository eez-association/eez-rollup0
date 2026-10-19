//! Authenticated Rollup0 blob-settlement context.

use std::{fmt, sync::Arc};

use alloy_primitives::{Address, B256, Bytes};
use async_trait::async_trait;
use eez_blob::{BlobSchedule, L1SlotClock};
use eez_control_rpc::v2;
use eez_protocol::Rollup0Deployment;
use thiserror::Error;

/// Caller-proposed L1 target before authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1PostingContext {
    pub chain_id: u64,
    pub target_timestamp: u64,
    pub parent_number: u64,
    pub parent_hash: B256,
}

/// Rollup0's EEZ entry and the manager's active wrapper at one L1 block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementState {
    /// L2 block hash EEZ stores as the rollup's settled root.
    pub root: B256,
    /// `rollupContract`: the manager EEZ calls for this rollup.
    pub manager: Address,
    pub settlement_wrapper: Address,
}

/// Values derived from one coherent, canonical L1 snapshot. Only
/// [`AppendixDBlobAuthority::authenticate`] constructs one.
#[derive(Debug, Clone)]
pub struct AuthenticatedBlobContext {
    custom_data: Bytes,
    max_blobs: usize,
    sync_interval: u64,
}

impl AuthenticatedBlobContext {
    pub(crate) fn custom_data(&self) -> Bytes {
        self.custom_data.clone()
    }

    pub(crate) const fn max_blobs(&self) -> usize {
        self.max_blobs
    }

    pub(crate) const fn sync_interval(&self) -> u64 {
        self.sync_interval
    }
}

/// Failure categories preserve the RPC contract across L1 implementations.
#[derive(Debug, Error)]
pub enum BlobContextError {
    #[error("invalid posting context: {0}")]
    Invalid(&'static str),
    #[error("posting target is no longer canonical")]
    StaleTarget,
    #[error("authenticated L1 evidence is unavailable: {0}")]
    Unavailable(String),
    #[error("selected parent is not the Ethereum-confirmed Rollup0 settled cursor")]
    WrongSettledCursor,
    #[error("L1 manager or active settlement wrapper differs from the configured deployment")]
    WrongDeployment,
}

/// Canonical facts required from a prover-owned L1 source.
#[async_trait]
pub trait Rollup0L1Source: Send + Sync + fmt::Debug {
    /// Chain ID served by this source.
    async fn chain_id(&self) -> Result<u64, BlobContextError>;

    /// Canonical block hash and timestamp at `number`.
    async fn canonical_header(&self, number: u64) -> Result<Option<(B256, u64)>, BlobContextError>;

    /// Rollup0's [`SettlementState`] after the named canonical L1 parent.
    async fn settlement_state(
        &self,
        l1_parent: L1PostingContext,
    ) -> Result<SettlementState, BlobContextError>;
}

/// Trusted blob-settlement configuration: the deployment, the L1 slot grid
/// and blob schedule shared with the Composer, and the L2 Sync interval.
#[derive(Debug, Clone)]
pub struct AppendixDBlobConfig {
    pub deployment: Rollup0Deployment,
    pub clock: L1SlotClock,
    pub schedule: BlobSchedule,
    /// L2 blocks per Sync interval (`K`); a candidate ends on this grid.
    pub sync_interval: u64,
}

/// Blob-settlement authority built over an independently configured canonical L1 source.
#[derive(Debug)]
pub struct AppendixDBlobAuthority {
    source: Arc<dyn Rollup0L1Source>,
    config: AppendixDBlobConfig,
}

impl AppendixDBlobAuthority {
    pub fn new(
        source: Arc<dyn Rollup0L1Source>,
        config: AppendixDBlobConfig,
    ) -> Result<Self, BlobContextError> {
        // A Sync slot needs Live blocks around it, so K >= 2 (RollupTiming).
        let deployment = &config.deployment;
        if deployment.l1_chain_id == 0 || deployment.l2_chain_id == 0 || config.sync_interval < 2 {
            return Err(BlobContextError::Invalid(
                "blob-settlement deployment configuration",
            ));
        }
        Ok(Self { source, config })
    }

    pub(crate) async fn authenticate(
        &self,
        requested: L1PostingContext,
        selected_parent: B256,
    ) -> Result<AuthenticatedBlobContext, BlobContextError> {
        let deployment = &self.config.deployment;
        if requested.chain_id != deployment.l1_chain_id {
            return Err(BlobContextError::Invalid("L1 chain identity"));
        }
        // A source on another chain is the operator's fault, not the caller's.
        let served = self.source.chain_id().await?;
        if served != deployment.l1_chain_id {
            return Err(BlobContextError::Unavailable(format!(
                "L1 source serves chain {served}"
            )));
        }
        let Some((canonical_hash, parent_timestamp)) = self
            .source
            .canonical_header(requested.parent_number)
            .await?
        else {
            return Err(BlobContextError::Unavailable(
                "canonical posting parent".to_owned(),
            ));
        };
        if canonical_hash != requested.parent_hash {
            return Err(BlobContextError::StaleTarget);
        }
        if requested.target_timestamp <= parent_timestamp {
            return Err(BlobContextError::Invalid(
                "target timestamp is not after parent",
            ));
        }
        if !self.config.clock.is_slot_start(requested.target_timestamp) {
            return Err(BlobContextError::Invalid(
                "target is not on an L1 slot boundary",
            ));
        }
        // The signed domain binds the live wrapper, so rotation stops signing.
        let state = self.source.settlement_state(requested).await?;
        if state.manager != deployment.manager
            || state.settlement_wrapper != deployment.settlement_wrapper
        {
            return Err(BlobContextError::WrongDeployment);
        }
        if state.root != selected_parent {
            return Err(BlobContextError::WrongSettledCursor);
        }
        let max_blobs = self
            .config
            .schedule
            .max_blobs_at(requested.target_timestamp)
            .ok_or(BlobContextError::Invalid(
                "no blob fork is active at the target",
            ))?;
        let Some((final_hash, final_timestamp)) = self
            .source
            .canonical_header(requested.parent_number)
            .await?
        else {
            return Err(BlobContextError::StaleTarget);
        };
        if final_hash != requested.parent_hash || final_timestamp != parent_timestamp {
            return Err(BlobContextError::StaleTarget);
        }
        Ok(AuthenticatedBlobContext {
            custom_data: deployment.custom_data(requested.target_timestamp, requested.parent_hash),
            max_blobs,
            sync_interval: self.config.sync_interval,
        })
    }
}

pub(crate) fn parse_posting_context(
    value: v2::L1PostingContext,
) -> Result<L1PostingContext, BlobContextError> {
    if value.chain_id == 0 || value.target_timestamp == 0 || value.parent_number == u64::MAX {
        return Err(BlobContextError::Invalid("scalar fields"));
    }
    let parent_hash = B256::try_from(value.parent_hash.as_slice())
        .map_err(|_| BlobContextError::Invalid("parent hash length"))?;
    if parent_hash.is_zero() {
        return Err(BlobContextError::Invalid("zero parent hash"));
    }
    Ok(L1PostingContext {
        chain_id: value.chain_id,
        target_timestamp: value.target_timestamp,
        parent_number: value.parent_number,
        parent_hash,
    })
}

pub(crate) type SharedBlobContextAuthority = Arc<AppendixDBlobAuthority>;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use alloy_primitives::{address, b256};

    use super::*;

    #[derive(Debug)]
    struct Source;

    #[async_trait]
    impl Rollup0L1Source for Source {
        async fn chain_id(&self) -> Result<u64, BlobContextError> {
            Ok(1)
        }

        async fn canonical_header(
            &self,
            number: u64,
        ) -> Result<Option<(B256, u64)>, BlobContextError> {
            Ok((number == 10).then_some((B256::repeat_byte(0xaa), 112)))
        }

        async fn settlement_state(
            &self,
            _l1_parent: L1PostingContext,
        ) -> Result<SettlementState, BlobContextError> {
            Ok(settlement_state())
        }
    }

    #[derive(Debug)]
    struct ReorgingSource(AtomicUsize);

    #[async_trait]
    impl Rollup0L1Source for ReorgingSource {
        async fn chain_id(&self) -> Result<u64, BlobContextError> {
            Ok(1)
        }

        async fn canonical_header(
            &self,
            _number: u64,
        ) -> Result<Option<(B256, u64)>, BlobContextError> {
            let call = self.0.fetch_add(1, Ordering::Relaxed);
            let hash = if call == 0 {
                B256::repeat_byte(0xaa)
            } else {
                B256::repeat_byte(0xcc)
            };
            Ok(Some((hash, 112)))
        }

        async fn settlement_state(
            &self,
            _l1_parent: L1PostingContext,
        ) -> Result<SettlementState, BlobContextError> {
            Ok(settlement_state())
        }
    }

    fn settlement_state() -> SettlementState {
        SettlementState {
            root: B256::repeat_byte(0xbb),
            manager: config().deployment.manager,
            settlement_wrapper: config().deployment.settlement_wrapper,
        }
    }

    fn config() -> AppendixDBlobConfig {
        AppendixDBlobConfig {
            deployment: Rollup0Deployment {
                l1_chain_id: 1,
                eez: address!("1111111111111111111111111111111111111111"),
                manager: address!("2222222222222222222222222222222222222222"),
                settlement_wrapper: address!("3333333333333333333333333333333333333333"),
                eez_rollup_id: 7,
                l2_chain_id: 10,
            },
            clock: L1SlotClock::new(100, 12).unwrap(),
            schedule: "0:6:2:eip4844".parse().unwrap(),
            sync_interval: 6,
        }
    }

    fn authority() -> AppendixDBlobAuthority {
        AppendixDBlobAuthority::new(Arc::new(Source), config()).unwrap()
    }

    #[tokio::test]
    async fn authenticates_canonical_slot_cursor_domain_and_fork_limit() {
        let target = L1PostingContext {
            chain_id: 1,
            target_timestamp: 124,
            parent_number: 10,
            parent_hash: B256::repeat_byte(0xaa),
        };
        let context = authority()
            .authenticate(target, B256::repeat_byte(0xbb))
            .await
            .unwrap();
        assert_eq!(context.max_blobs(), 2);
        assert_eq!(context.sync_interval(), 6);
        assert_eq!(
            context.custom_data(),
            config()
                .deployment
                .custom_data(124, B256::repeat_byte(0xaa)),
        );
    }

    #[tokio::test]
    async fn rejects_stale_target_off_slot_and_wrong_settled_cursor() {
        let base = L1PostingContext {
            chain_id: 1,
            target_timestamp: 124,
            parent_number: 10,
            parent_hash: B256::repeat_byte(0xaa),
        };
        let selected = B256::repeat_byte(0xbb);
        assert!(matches!(
            authority()
                .authenticate(
                    L1PostingContext {
                        parent_hash: b256!(
                            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        ),
                        ..base
                    },
                    selected
                )
                .await,
            Err(BlobContextError::StaleTarget)
        ));
        assert!(matches!(
            authority()
                .authenticate(
                    L1PostingContext {
                        target_timestamp: 125,
                        ..base
                    },
                    selected
                )
                .await,
            Err(BlobContextError::Invalid(_))
        ));
        assert!(matches!(
            authority()
                .authenticate(base, B256::repeat_byte(0xcc))
                .await,
            Err(BlobContextError::WrongSettledCursor)
        ));
        let rotated = AppendixDBlobConfig {
            deployment: Rollup0Deployment {
                settlement_wrapper: address!("4444444444444444444444444444444444444444"),
                ..config().deployment
            },
            ..config()
        };
        let rotated = AppendixDBlobAuthority::new(Arc::new(Source), rotated).unwrap();
        assert!(matches!(
            rotated.authenticate(base, selected).await,
            Err(BlobContextError::WrongDeployment)
        ));
    }

    #[tokio::test]
    async fn rejects_reorg_during_snapshot_reads() {
        let authority =
            AppendixDBlobAuthority::new(Arc::new(ReorgingSource(AtomicUsize::new(0))), config())
                .unwrap();
        let result = authority
            .authenticate(
                L1PostingContext {
                    chain_id: 1,
                    target_timestamp: 124,
                    parent_number: 10,
                    parent_hash: B256::repeat_byte(0xaa),
                },
                B256::repeat_byte(0xbb),
            )
            .await;
        assert!(matches!(result, Err(BlobContextError::StaleTarget)));
    }
}
