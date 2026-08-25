//! Signed P2P unsafe-block follower.
//!
//! Safe and finalized are owned by the L1 deriver through
//! `BlockCommitterHandle::advance_safe_finalized`. This task authenticates
//! complete sequencer payloads, checks their ancestry against those anchors,
//! and imports them through the same serialized Engine API actor.
//!
//! Compatibility is enforced here, not by reth: the engine only requires
//! safe/finalized to be *known*, so it would canonicalize a head whose
//! ancestry conflicts with our safe block. Before each FCU we accept heads
//! already on the local canonical chain, otherwise walk the candidate's local
//! ancestry to the safe height. Unverifiable ancestry fails closed until a
//! missing-parent sync path supplies the gap.

use std::time::Duration;

use alloy_consensus::BlockHeader;
use alloy_eips::BlockNumHash;
use alloy_primitives::Address;
use eez_driver::BlockCommitterHandle;
use eez_p2p::{NetworkEvent, NetworkHandle, verify_payload};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_primitives_traits::SealedHeader;
use reth_storage_api::{BlockIdReader, HeaderProvider};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{Level, event};

/// Keepalive cadence: re-publish the current forkchoice state so reth's
/// engine view does not drift during quiet periods.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Upper bound on the candidate→safe ancestry walk. Beyond this, a fully
/// local gap is treated as unverifiable rather than scanned unboundedly.
const MAX_ANCESTRY_WALK: u64 = 1024;

#[derive(Debug, Error)]
enum FollowerError {
    /// Signed block authentication, SSZ, or block-hash failure.
    #[error("invalid signed unsafe block: {0}")]
    Protocol(String),

    /// Execution payload could not be converted into a block header.
    #[error("invalid execution payload: {0}")]
    Payload(String),

    /// Local chain provider failure while validating a candidate head.
    #[error("local provider error: {0}")]
    Provider(String),

    /// Shared driver/committer error.
    #[error("driver error: {0}")]
    Driver(String),
}

/// Verdict of checking a candidate head against the local safe anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SafeCompat {
    /// The candidate provably descends from the safe block.
    Extends,
    /// The candidate provably does not descend from the safe block.
    Conflicts,
    /// Ancestry is not locally known yet, so keep it optimistic.
    Unverifiable,
}

/// Verifies P2P unsafe blocks and routes every engine call through the shared
/// [`BlockCommitterHandle`].
#[derive(Debug)]
pub(crate) struct UnsafeHeadFollower<P> {
    committer: BlockCommitterHandle<EthEngineTypes>,
    chain_id: u64,
    authorized_signer: Address,
    network_events: mpsc::Receiver<NetworkEvent>,
    /// Keeps the network command side alive for this task's lifetime.
    _network: NetworkHandle,
    /// Local chain reader: resolves the current safe anchor and the
    /// candidate head's ancestry for the compatibility check.
    local: P,
    last_head: Option<alloy_primitives::B256>,
}

impl<P> UnsafeHeadFollower<P>
where
    P: HeaderProvider<Header = alloy_consensus::Header> + BlockIdReader,
{
    pub(crate) fn new(
        committer: BlockCommitterHandle<EthEngineTypes>,
        local: P,
        chain_id: u64,
        authorized_signer: Address,
        network_events: mpsc::Receiver<NetworkEvent>,
        network: NetworkHandle,
    ) -> Self {
        Self {
            committer,
            chain_id,
            authorized_signer,
            network_events,
            _network: network,
            local,
            last_head: None,
        }
    }

    pub(crate) async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(FCU_REFRESH);
        loop {
            tokio::select! {
                event = self.network_events.recv() => {
                    let Some(event) = event else {
                        event!(
                            name: "eez.node.follower.p2p.stopped",
                            Level::ERROR,
                            "unsafe-block P2P event stream stopped",
                        );
                        return;
                    };
                    if let NetworkEvent::Message(message) = event
                        && let Err(err) = self.advance(&message).await
                    {
                        event!(
                            name: "eez.node.follower.advance.failed",
                            Level::WARN,
                            error = %err,
                            "signed unsafe-block import failed",
                        );
                    }
                }
                _ = fcu_interval.tick() => {
                    if let Err(err) = self.committer.refresh_forkchoice().await {
                        event!(
                            name: "eez.node.follower.fcu_refresh.failed",
                            Level::WARN,
                            error = %err,
                            "forkchoice refresh failed",
                        );
                    }
                }
            }
        }
    }

    async fn advance(&mut self, message: &[u8]) -> Result<(), FollowerError> {
        let payload = verify_payload(message, self.chain_id, self.authorized_signer)
            .map_err(|error| FollowerError::Protocol(error.to_string()))?;
        let number = payload.block_number();
        let hash = payload.block_hash();
        if self.last_head == Some(hash) {
            return Ok(());
        }
        let block = payload
            .clone()
            .into_block_raw()
            .map_err(|error| FollowerError::Payload(error.to_string()))?;
        let header = SealedHeader::new(block.header, hash);

        // Exclude a deriver safe-head move between the ancestry verdict and
        // newPayload + FCU. The committer actor still serializes engine calls.
        let _reconcile_guard = self.committer.begin_reconcile().await;
        let current_head = self.committer.last_header();
        let compatibility = if header.number() == current_head.number().saturating_add(1)
            && header.parent_hash() == current_head.hash()
        {
            SafeCompat::Extends
        } else {
            check_extends_safe(&self.local, &header, hash)?
        };

        // Reject candidates whose ancestry provably conflicts with the
        // L1-derived safe anchor before they reach the engine.
        match compatibility {
            SafeCompat::Extends => {}
            SafeCompat::Conflicts => {
                event!(
                    name: "eez.node.follower.head.conflicts_safe",
                    Level::WARN,
                    block.number = number,
                    block.hash = %hash,
                    "signed unsafe block does not descend from the L1-derived safe block; skipping",
                );
                return Ok(());
            }
            SafeCompat::Unverifiable => {
                event!(
                    name: "eez.node.follower.head.missing_ancestry",
                    Level::WARN,
                    block.number = number,
                    block.hash = %hash,
                    "signed unsafe block ancestry is not locally available; skipping",
                );
                return Ok(());
            }
        }

        self.committer
            .commit_derived(payload, header, false)
            .await
            .map_err(|error| FollowerError::Driver(error.to_string()))?;
        self.last_head = Some(hash);
        event!(
            name: "eez.node.follower.head.advanced",
            Level::INFO,
            block.number = number,
            block.hash = %hash,
            "follower imported verified sequencer payload and advanced unsafe head",
        );
        Ok(())
    }
}

/// Checks whether `candidate` descends from the current safe block. Steady
/// state O(1): if the candidate is already local canonical, the local
/// forkchoice invariant proves it extends safe; otherwise the first parent
/// lookup usually resolves or misses (unverifiable).
fn check_extends_safe<P>(
    local: &P,
    candidate: &alloy_consensus::Header,
    candidate_hash: alloy_primitives::B256,
) -> Result<SafeCompat, FollowerError>
where
    P: HeaderProvider<Header = alloy_consensus::Header> + BlockIdReader,
{
    let Some(safe) = local
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

    // A head at the safe height must be the safe block; below it,
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
    if local
        .sealed_header(candidate.number)
        .map_err(|e| FollowerError::Provider(format!("sealed_header({}): {e}", candidate.number)))?
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
        let Some(parent) = local
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use alloy_consensus::Header;
    use alloy_primitives::{B256, BlockHash, BlockNumber};
    use reth_chainspec::ChainInfo;
    use reth_storage_api::{BlockHashReader, BlockNumReader, errors::provider::ProviderResult};

    use super::*;

    #[derive(Default)]
    struct MockLocal {
        safe: Option<BlockNumHash>,
        canonical: BTreeMap<BlockNumber, (BlockHash, Header)>,
        by_hash: HashMap<BlockHash, Header>,
    }

    impl MockLocal {
        fn with_safe(number: BlockNumber, hash: BlockHash) -> Self {
            Self {
                safe: Some(BlockNumHash { number, hash }),
                ..Default::default()
            }
        }

        fn insert_canonical(&mut self, hash: BlockHash, header: Header) {
            self.by_hash.insert(hash, header.clone());
            self.canonical.insert(header.number, (hash, header));
        }

        fn insert_known(&mut self, hash: BlockHash, header: Header) {
            self.by_hash.insert(hash, header);
        }
    }

    impl HeaderProvider for MockLocal {
        type Header = Header;

        fn header(&self, block_hash: BlockHash) -> ProviderResult<Option<Self::Header>> {
            Ok(self.by_hash.get(&block_hash).cloned())
        }

        fn header_by_number(&self, num: u64) -> ProviderResult<Option<Self::Header>> {
            Ok(self.canonical.get(&num).map(|(_, header)| header.clone()))
        }

        fn headers_range(
            &self,
            range: impl std::ops::RangeBounds<BlockNumber>,
        ) -> ProviderResult<Vec<Self::Header>> {
            Ok(self
                .canonical
                .iter()
                .filter(|(number, _)| range.contains(number))
                .map(|(_, (_, header))| header.clone())
                .collect())
        }

        fn sealed_header(
            &self,
            number: BlockNumber,
        ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
            Ok(self
                .canonical
                .get(&number)
                .map(|(hash, header)| SealedHeader::new(header.clone(), *hash)))
        }

        fn sealed_headers_while(
            &self,
            range: impl std::ops::RangeBounds<BlockNumber>,
            mut predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
        ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
            let mut headers = Vec::new();
            for (number, (hash, header)) in &self.canonical {
                if !range.contains(number) {
                    continue;
                }
                let sealed = SealedHeader::new(header.clone(), *hash);
                if !predicate(&sealed) {
                    break;
                }
                headers.push(sealed);
            }
            Ok(headers)
        }
    }

    impl BlockHashReader for MockLocal {
        fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
            Ok(self.canonical.get(&number).map(|(hash, _)| *hash))
        }

        fn canonical_hashes_range(
            &self,
            start: BlockNumber,
            end: BlockNumber,
        ) -> ProviderResult<Vec<B256>> {
            Ok((start..end)
                .filter_map(|number| self.canonical.get(&number).map(|(hash, _)| *hash))
                .collect())
        }
    }

    impl BlockNumReader for MockLocal {
        fn chain_info(&self) -> ProviderResult<ChainInfo> {
            let Some((number, (hash, _))) = self.canonical.iter().next_back() else {
                return Ok(ChainInfo::default());
            };
            Ok(ChainInfo {
                best_hash: *hash,
                best_number: *number,
            })
        }

        fn best_block_number(&self) -> ProviderResult<BlockNumber> {
            Ok(self.chain_info()?.best_number)
        }

        fn last_block_number(&self) -> ProviderResult<BlockNumber> {
            self.best_block_number()
        }

        fn block_number(&self, hash: B256) -> ProviderResult<Option<BlockNumber>> {
            Ok(self.by_hash.get(&hash).map(|header| header.number))
        }
    }

    impl BlockIdReader for MockLocal {
        fn pending_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(None)
        }

        fn safe_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(self.safe)
        }

        fn finalized_block_num_hash(&self) -> ProviderResult<Option<BlockNumHash>> {
            Ok(None)
        }
    }

    fn hash(byte: u8) -> BlockHash {
        B256::from([byte; 32])
    }

    fn header(number: BlockNumber, parent_hash: BlockHash) -> Header {
        Header {
            number,
            parent_hash,
            ..Default::default()
        }
    }

    #[test]
    fn candidate_extends_when_no_safe_anchor() {
        let local = MockLocal::default();
        let candidate = header(1, hash(0));

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(1)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn candidate_at_safe_height_must_match_safe_hash() {
        let safe_hash = hash(10);
        let local = MockLocal::with_safe(10, safe_hash);
        let candidate = header(10, hash(9));

        assert_eq!(
            check_extends_safe(&local, &candidate, safe_hash).unwrap(),
            SafeCompat::Extends
        );
        assert_eq!(
            check_extends_safe(&local, &candidate, hash(99)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn candidate_below_safe_conflicts() {
        let local = MockLocal::with_safe(10, hash(10));
        let candidate = header(9, hash(8));

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(9)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn candidate_directly_above_safe_checks_parent_hash() {
        let safe_hash = hash(10);
        let local = MockLocal::with_safe(10, safe_hash);

        assert_eq!(
            check_extends_safe(&local, &header(11, safe_hash), hash(11)).unwrap(),
            SafeCompat::Extends
        );
        assert_eq!(
            check_extends_safe(&local, &header(11, hash(99)), hash(11)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn canonical_candidate_extends_without_parent_walk() {
        let mut local = MockLocal::with_safe(10, hash(10));
        let candidate = header(12, hash(99));
        local.insert_canonical(hash(12), candidate.clone());

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(12)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn walks_known_ancestry_to_safe() {
        let mut local = MockLocal::with_safe(10, hash(10));
        local.insert_known(hash(12), header(12, hash(11)));
        local.insert_known(hash(11), header(11, hash(10)));

        let candidate = header(13, hash(12));

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(13)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn known_ancestry_conflict_at_safe_height() {
        let mut local = MockLocal::with_safe(10, hash(10));
        local.insert_known(hash(12), header(12, hash(11)));
        local.insert_known(hash(11), header(11, hash(99)));

        let candidate = header(13, hash(12));

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(13)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn missing_ancestry_is_unverifiable() {
        let local = MockLocal::with_safe(10, hash(10));
        let candidate = header(12, hash(11));

        assert_eq!(
            check_extends_safe(&local, &candidate, hash(12)).unwrap(),
            SafeCompat::Unverifiable
        );
    }
}
