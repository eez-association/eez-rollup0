//! Signed-head unsafe-head follower.
//!
//! Safe and finalized are owned by the L1 deriver through
//! `BlockCommitterHandle::advance_safe_finalized`. This task polls the
//! sequencer's `eez_getSignedHead`, keeps the latest verified head of each
//! signing source, and moves the unsafe head to the highest-version candidate
//! that descends from the L1-derived safe block. Missing ancestors are fetched
//! by hash, checked against the requested hash, and imported through the
//! committer under the reconcile lock.
//!
//! Descent from safe is enforced here, not by reth: the engine only requires
//! safe/finalized to be *known*, so it would canonicalize a head whose
//! ancestry conflicts with our safe block.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_consensus::Header;
use alloy_eips::BlockNumHash;
use alloy_primitives::{Address, B256, Bytes};
use alloy_provider::{Provider, RootProvider};
use eez_driver::BlockCommitterHandle;
use eez_l1::L1CanonicalHead;
use eez_primitives::{Block, engine::EezEngineTypes};
use eez_seq_sig::{Envelope, HeadDomain, HeadTracker, Observation, VerifiedHead, decode_block};
use reth_payload_primitives::PayloadTypes as _;
use reth_primitives_traits::{SealedBlock, SealedHeader};
use reth_storage_api::{BlockIdReader, BlockNumReader, HeaderProvider};
use thiserror::Error;
use tracing::{Level, event};

/// Keepalive cadence: re-publish the current forkchoice state so reth's
/// engine view does not drift during quiet periods.
const FCU_REFRESH: Duration = Duration::from_secs(1);

/// Upper bound on each ancestry walk (fetching back to a local block, and the
/// local walk down to safe); beyond it the candidate is unverifiable.
const MAX_ANCESTRY_WALK: u64 = 1024;

/// Bound on remembered rejections: a rogue signer can mint new hashes forever.
const MAX_LOGGED_REJECTIONS: usize = 256;

#[derive(Debug, Error)]
pub(crate) enum FollowerError {
    /// Sequencer JSON-RPC transport, decode or missing-block failure.
    #[error("sequencer RPC error: {0}")]
    Rpc(String),

    /// Local chain provider failure while validating a candidate head.
    #[error("local provider error: {0}")]
    Provider(String),

    /// Shared driver/committer error.
    #[error("driver error: {0}")]
    Driver(String),

    /// No locally known ancestor within [`MAX_ANCESTRY_WALK`] blocks.
    #[error("no local ancestor within {depth} blocks")]
    Unverifiable { depth: u64 },

    /// The target is too far ahead of the local head to walk back from.
    #[error(
        "target {target} is more than {MAX_ANCESTRY_WALK} ahead of local head {local}; waiting for L1 derivation"
    )]
    Ahead { target: u64, local: u64 },

    /// The source answered a block request with a different block.
    #[error("requested block {requested}, got {got}")]
    BadAncestor { requested: B256, got: B256 },
}

/// Verdict of checking a candidate head against the local safe anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SafeCompat {
    /// The candidate provably descends from the safe block.
    Extends,
    /// The candidate provably does not descend from the safe block.
    Conflicts,
    /// Ancestry down to the safe height is not locally known.
    Unverifiable,
}

/// Serves consensus-RLP blocks by hash.
pub(crate) trait BlockSource {
    async fn raw_block(&self, hash: B256) -> Result<Option<Bytes>, FollowerError>;
}

impl BlockSource for RootProvider {
    async fn raw_block(&self, hash: B256) -> Result<Option<Bytes>, FollowerError> {
        self.raw_request("eez_getRawBlock".into(), (hash,))
            .await
            .map_err(|e| FollowerError::Rpc(format!("eez_getRawBlock({hash}): {e}")))
    }
}

/// What a target needs before import.
#[derive(Debug)]
pub(crate) struct Missing {
    /// First locally known block on the target's ancestry.
    pub fork_point: SealedHeader,
    /// Unknown blocks, oldest first; empty if the target is local.
    pub blocks: Vec<SealedBlock<Block>>,
}

/// Polls the sequencer RPC for signed heads and routes every engine call
/// through the shared [`BlockCommitterHandle`].
#[derive(Debug)]
pub(crate) struct UnsafeHeadFollower<P> {
    committer: BlockCommitterHandle<EezEngineTypes>,
    sequencer_rpc: RootProvider,
    /// Local chain reader: resolves the current safe anchor and the
    /// candidate head's ancestry for the compatibility check.
    local: P,
    /// The Deriver's L1 batch index, which can run ahead of reth's safe block.
    l1_head: Arc<L1CanonicalHead>,
    domain: HeadDomain,
    /// The only signer whose heads are followed.
    sequencer_address: Address,
    /// Cadence for polling the sequencer RPC for new signed heads.
    poll_interval: Duration,
    tracker: HeadTracker,
    rejections: Rejections,
}

impl<P> UnsafeHeadFollower<P>
where
    P: HeaderProvider<Header = Header> + BlockIdReader,
{
    pub(crate) fn new(
        committer: BlockCommitterHandle<EezEngineTypes>,
        sequencer_rpc: RootProvider,
        local: P,
        l1_head: Arc<L1CanonicalHead>,
        domain: HeadDomain,
        sequencer_address: Address,
        poll_interval: Duration,
    ) -> Self {
        Self {
            committer,
            sequencer_rpc,
            local,
            l1_head,
            domain,
            sequencer_address,
            poll_interval,
            tracker: HeadTracker::default(),
            rejections: Rejections::default(),
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
                            name: "eez.node.follower.advance.failed",
                            Level::WARN,
                            error = %err,
                            "sequencer unsafe-head advance failed; will retry",
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

    async fn advance(&mut self) -> Result<(), FollowerError> {
        let envelope: Option<Envelope> = self
            .sequencer_rpc
            .raw_request("eez_getSignedHead".into(), ())
            .await
            .map_err(|e| FollowerError::Rpc(format!("eez_getSignedHead: {e}")))?;
        match envelope {
            Some(envelope) => self.observe(envelope),
            None => event!(
                name: "eez.node.follower.head.unsigned",
                Level::DEBUG,
                "sequencer has not signed a head yet",
            ),
        }
        self.follow_best().await
    }

    fn observe(&mut self, envelope: Envelope) {
        let (number, hash) = (envelope.head.number, envelope.head.block_hash);
        let head = match envelope.verify(&self.domain, self.sequencer_address) {
            Ok(head) => head,
            Err(err) => {
                self.rejections.warn(number, hash, &err.to_string());
                return;
            }
        };
        match self.tracker.observe(head, now_ms()) {
            Observation::New => {}
            Observation::Duplicate | Observation::Stale => event!(
                name: "eez.node.follower.head.ignored",
                Level::DEBUG,
                block.number = number,
                block.hash = %hash,
                "signed head is not newer than its source's latest",
            ),
            Observation::Future => {
                self.rejections
                    .warn(number, hash, "epoch is ahead of the local clock");
            }
            Observation::Equivocation { kept } => {
                if self.rejections.first(hash, "equivocation") {
                    event!(
                        name: "eez.node.follower.head.equivocation",
                        Level::ERROR,
                        block.number = number,
                        block.hash = %hash,
                        kept = %kept,
                        "sequencer signed two heads with one version; keeping the first",
                    );
                }
            }
        }
    }

    /// Adopt the highest-version candidate that descends from safe.
    async fn follow_best(&mut self) -> Result<(), FollowerError> {
        for head in self.tracker.candidates() {
            let claim = &head.claim;
            let (number, hash) = (claim.number, claim.block_hash);
            // The claim alone can prove a conflict, so a source whose ancestors
            // nobody serves stops blocking lower candidates once safe passes it.
            let at = BlockNumHash::new(number, hash);
            if check_extends_safe(&self.local, at, claim.parent_hash)? == SafeCompat::Conflicts {
                self.rejections.warn(number, hash, "conflicts with safe");
                continue;
            }
            let missing = match self.fetch(head).await {
                Ok(missing) => missing,
                Err(err @ FollowerError::Ahead { .. }) => {
                    if self.rejections.first(hash, "ahead") {
                        event!(
                            name: "eez.node.follower.head.ahead",
                            Level::INFO,
                            block.number = number,
                            block.hash = %hash,
                            reason = %err,
                            "signed head not followed yet",
                        );
                    }
                    return Ok(());
                }
                Err(err) => {
                    // Unknown ancestry is not a conflict, so a lower version must not win.
                    self.rejections.warn(number, hash, &err.to_string());
                    return Ok(());
                }
            };
            // Held from the safe check through import, so no Deriver batch reconcile interleaves.
            let _guard = self.committer.begin_reconcile().await;
            // The Deriver indexes a batch before moving safe to it, so the heights
            // in between are L1-settled and must not be rewritten.
            let indexed = self.l1_head.last_indexed_l2();
            let safe = self
                .local
                .safe_block_num_hash()
                .map_err(|e| FollowerError::Provider(format!("safe_block_num_hash: {e}")))?
                .map_or(0, |s| s.number);
            if indexed > safe {
                event!(
                    name: "eez.node.follower.head.held",
                    Level::DEBUG,
                    indexed,
                    safe,
                    "safe lags the L1 batch index; holding the head",
                );
                return Ok(());
            }
            let fork_point = &missing.fork_point;
            let verdict =
                check_extends_safe(&self.local, fork_point.num_hash(), fork_point.parent_hash)?;
            if verdict != SafeCompat::Extends {
                let reason = format!(
                    "fork point {} ({}) does not extend safe: {verdict:?}",
                    fork_point.number,
                    fork_point.hash()
                );
                self.rejections.warn(number, hash, &reason);
                if verdict == SafeCompat::Conflicts {
                    continue;
                }
                return Ok(());
            }
            let imported = missing.blocks.len();
            if self.import(missing).await? {
                event!(
                    name: "eez.node.follower.head.advanced",
                    Level::INFO,
                    event_name = "eez.node.follower.head.advanced",
                    block.number = number,
                    block.hash = %hash,
                    imported,
                    "follower adopted the signed head",
                );
            }
            return Ok(());
        }
        Ok(())
    }

    async fn fetch(&self, head: &VerifiedHead) -> Result<Missing, FollowerError> {
        let fetched;
        let target = match &head.block {
            Some(block) => block,
            None => {
                fetched = fetch_block(&self.sequencer_rpc, head.claim.block_hash).await?;
                &fetched
            }
        };
        fetch_missing(&self.local, &self.sequencer_rpc, target).await
    }

    /// Returns whether the head moved.
    async fn import(&self, missing: Missing) -> Result<bool, FollowerError> {
        let driver = |e: eez_driver::DriverError| FollowerError::Driver(e.to_string());
        if missing.blocks.is_empty() {
            if missing.fork_point.hash() == self.committer.last_header().hash() {
                return Ok(false);
            }
            self.committer
                .reorg_to(missing.fork_point)
                .await
                .map_err(driver)?;
            return Ok(true);
        }
        for block in missing.blocks {
            let header = block.clone_sealed_header();
            self.committer
                .commit_derived(EezEngineTypes::block_to_payload(block, None), header, false)
                .await
                .map_err(driver)?;
        }
        Ok(true)
    }
}

/// Rejections already logged, so a head the RPC keeps returning warns once.
#[derive(Debug, Default)]
struct Rejections(HashSet<(B256, String)>);

impl Rejections {
    fn first(&mut self, hash: B256, reason: &str) -> bool {
        if self.0.len() >= MAX_LOGGED_REJECTIONS {
            self.0.clear();
        }
        self.0.insert((hash, reason.to_owned()))
    }

    fn warn(&mut self, number: u64, hash: B256, reason: &str) {
        if self.first(hash, reason) {
            event!(
                name: "eez.node.follower.head.rejected",
                Level::WARN,
                event_name = "eez.node.follower.head.rejected",
                block.number = number,
                block.hash = %hash,
                reason,
                "signed head not followed",
            );
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Fetches one block by hash, refusing any block but the one requested.
async fn fetch_block<S: BlockSource>(
    source: &S,
    hash: B256,
) -> Result<SealedBlock<Block>, FollowerError> {
    let rlp = source
        .raw_block(hash)
        .await?
        .ok_or_else(|| FollowerError::Rpc(format!("block {hash} not served")))?;
    let block = decode_block(&rlp).map_err(|e| FollowerError::Rpc(format!("block {hash}: {e}")))?;
    if block.hash() != hash {
        return Err(FollowerError::BadAncestor {
            requested: hash,
            got: block.hash(),
        });
    }
    Ok(block)
}

/// Walks `target`'s parent hashes back to the first locally known block,
/// fetching each unknown one from `source`.
async fn fetch_missing<P, S>(
    local: &P,
    source: &S,
    target: &SealedBlock<Block>,
) -> Result<Missing, FollowerError>
where
    P: HeaderProvider<Header = Header> + BlockNumReader,
    S: BlockSource,
{
    // The fork point is local, so at or below the local head: no walk could fit the cap.
    let best = local
        .best_block_number()
        .map_err(|e| FollowerError::Provider(format!("best_block_number: {e}")))?;
    if target.number > best.saturating_add(MAX_ANCESTRY_WALK) {
        return Err(FollowerError::Ahead {
            target: target.number,
            local: best,
        });
    }
    let mut blocks = Vec::new();
    let mut hash = target.hash();
    loop {
        if let Some(header) = local
            .header(hash)
            .map_err(|e| FollowerError::Provider(format!("header({hash}): {e}")))?
        {
            blocks.reverse();
            return Ok(Missing {
                fork_point: SealedHeader::new(header, hash),
                blocks,
            });
        }
        let depth = blocks.len() as u64;
        if depth >= MAX_ANCESTRY_WALK {
            return Err(FollowerError::Unverifiable { depth });
        }
        let block = if depth == 0 {
            target.clone()
        } else {
            fetch_block(source, hash).await?
        };
        hash = block.parent_hash;
        blocks.push(block);
    }
}

/// Checks whether `candidate` descends from the current safe block. Steady
/// state O(1): if the candidate is already local canonical, the local
/// forkchoice invariant proves it extends safe; otherwise the first parent
/// lookup usually resolves or misses (unverifiable).
fn check_extends_safe<P>(
    local: &P,
    candidate: BlockNumHash,
    parent_hash: B256,
) -> Result<SafeCompat, FollowerError>
where
    P: HeaderProvider<Header = Header> + BlockIdReader,
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
        return Ok(if candidate.hash == safe_hash {
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
        .is_some_and(|local| local.hash() == candidate.hash)
    {
        return Ok(SafeCompat::Extends);
    }

    let mut cursor_hash = parent_hash;
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

    use alloy_primitives::{BlockHash, BlockNumber};
    use reth_chainspec::ChainInfo;
    use reth_storage_api::{BlockHashReader, errors::provider::ProviderResult};

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

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(1, hash(1)), hash(0)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn candidate_at_safe_height_must_match_safe_hash() {
        let safe_hash = hash(10);
        let local = MockLocal::with_safe(10, safe_hash);

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(10, safe_hash), hash(9)).unwrap(),
            SafeCompat::Extends
        );
        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(10, hash(99)), hash(9)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn candidate_below_safe_conflicts() {
        let local = MockLocal::with_safe(10, hash(10));

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(9, hash(9)), hash(8)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn candidate_directly_above_safe_checks_parent_hash() {
        let safe_hash = hash(10);
        let local = MockLocal::with_safe(10, safe_hash);
        let candidate = BlockNumHash::new(11, hash(11));

        assert_eq!(
            check_extends_safe(&local, candidate, safe_hash).unwrap(),
            SafeCompat::Extends
        );
        assert_eq!(
            check_extends_safe(&local, candidate, hash(99)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn canonical_candidate_extends_without_parent_walk() {
        let mut local = MockLocal::with_safe(10, hash(10));
        local.insert_canonical(hash(12), header(12, hash(99)));

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(12, hash(12)), hash(99)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn walks_known_ancestry_to_safe() {
        let mut local = MockLocal::with_safe(10, hash(10));
        local.insert_known(hash(12), header(12, hash(11)));
        local.insert_known(hash(11), header(11, hash(10)));

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(13, hash(13)), hash(12)).unwrap(),
            SafeCompat::Extends
        );
    }

    #[test]
    fn known_ancestry_conflict_at_safe_height() {
        let mut local = MockLocal::with_safe(10, hash(10));
        local.insert_known(hash(12), header(12, hash(11)));
        local.insert_known(hash(11), header(11, hash(99)));

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(13, hash(13)), hash(12)).unwrap(),
            SafeCompat::Conflicts
        );
    }

    #[test]
    fn missing_ancestry_is_unverifiable() {
        let local = MockLocal::with_safe(10, hash(10));

        assert_eq!(
            check_extends_safe(&local, BlockNumHash::new(12, hash(12)), hash(11)).unwrap(),
            SafeCompat::Unverifiable
        );
    }

    impl BlockSource for HashMap<B256, Bytes> {
        async fn raw_block(&self, hash: B256) -> Result<Option<Bytes>, FollowerError> {
            Ok(self.get(&hash).cloned())
        }
    }

    fn block(number: BlockNumber, parent_hash: B256, salt: u8) -> SealedBlock<Block> {
        SealedBlock::seal_slow(Block {
            header: Header {
                number,
                parent_hash,
                extra_data: Bytes::from(vec![salt]),
                ..Default::default()
            },
            body: Default::default(),
        })
    }

    fn rlp(block: &SealedBlock<Block>) -> Bytes {
        Bytes::from(alloy_rlp::encode(block.clone_block()))
    }

    #[tokio::test]
    async fn fetch_stops_at_first_local_block() {
        let b0 = block(0, B256::ZERO, 0);
        let b1 = block(1, b0.hash(), 0);
        let b2 = block(2, b1.hash(), 0);
        let mut local = MockLocal::default();
        local.insert_canonical(b0.hash(), b0.header().clone());
        let remote = HashMap::from([(b1.hash(), rlp(&b1))]);

        let missing = fetch_missing(&local, &remote, &b2).await.unwrap();

        assert_eq!(missing.fork_point.hash(), b0.hash());
        let hashes: Vec<_> = missing.blocks.iter().map(SealedBlock::hash).collect();
        assert_eq!(hashes, vec![b1.hash(), b2.hash()]);
    }

    #[tokio::test]
    async fn fetch_known_target_has_no_blocks() {
        let b0 = block(0, B256::ZERO, 0);
        let mut local = MockLocal::default();
        local.insert_canonical(b0.hash(), b0.header().clone());

        let missing = fetch_missing(&local, &HashMap::new(), &b0).await.unwrap();

        assert_eq!(missing.fork_point.hash(), b0.hash());
        assert!(missing.blocks.is_empty());
    }

    #[tokio::test]
    async fn fetch_rejects_ancestor_with_wrong_hash() {
        let b0 = block(0, B256::ZERO, 0);
        let b1 = block(1, b0.hash(), 0);
        let bx = block(1, b0.hash(), 0xff);
        let b2 = block(2, b1.hash(), 0);
        let mut local = MockLocal::default();
        local.insert_canonical(b0.hash(), b0.header().clone());
        let remote = HashMap::from([(b1.hash(), rlp(&bx))]);

        let err = fetch_missing(&local, &remote, &b2).await.unwrap_err();

        assert!(
            matches!(err, FollowerError::BadAncestor { requested, got }
                if requested == b1.hash() && got == bx.hash()),
            "{err}"
        );
    }

    #[tokio::test]
    async fn fetch_is_unverifiable_past_walk_cap() {
        let mut chain = vec![block(0, B256::ZERO, 0)];
        for number in 1..MAX_ANCESTRY_WALK + 2 {
            chain.push(block(number, chain[chain.len() - 1].hash(), 0));
        }
        let remote: HashMap<_, _> = chain.iter().map(|b| (b.hash(), rlp(b))).collect();
        let target = chain.last().unwrap();
        // An unrelated local head at the target's height lets the walk start.
        let mut local = MockLocal::default();
        local.insert_canonical(hash(0xee), header(target.number, hash(0xed)));

        let err = fetch_missing(&local, &remote, target).await.unwrap_err();

        assert!(matches!(err, FollowerError::Unverifiable { .. }), "{err}");
    }

    #[tokio::test]
    async fn fetch_skips_walk_when_target_is_beyond_cap() {
        let b0 = block(0, B256::ZERO, 0);
        let mut local = MockLocal::default();
        local.insert_canonical(b0.hash(), b0.header().clone());
        let target = block(MAX_ANCESTRY_WALK + 1, hash(0xaa), 0);

        // Any fetch from the empty remote would fail as "not served" instead.
        let err = fetch_missing(&local, &HashMap::new(), &target)
            .await
            .unwrap_err();

        assert!(matches!(err, FollowerError::Ahead { .. }), "{err}");
    }
}
