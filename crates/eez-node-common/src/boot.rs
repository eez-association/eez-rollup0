//! Boot: anchor the L2 chain at L1's finalized root, through peers or L1 alone.

use std::time::{Duration, Instant};

use alloy_consensus::Header;
use alloy_eips::Encodable2718;
use alloy_primitives::B256;
use eez_deriver::{Deriver, DeriverBoot, DeriverError};
use eez_driver::{BlockCommitterHandle, ForkchoiceOutcome};
use eez_l1::{BatchRecord, FinalizedCommitment, L1Error, L1Reader, L1Result, PostId};
use eez_primitives::engine::EezEngineTypes;
use reth_primitives_traits::SealedHeader;
use reth_provider::StateProviderFactory;
use reth_storage_api::{
    BlockIdReader, BlockNumReader, BlockReader, HeaderProvider, ReceiptProvider,
    TransactionsProvider,
};
use tracing::{Level, event};

use crate::ReadinessProbe;

/// Pace of the boot loops: L1 re-reads while waiting, and `sync_to` FCUs.
const SYNC_POLL: Duration = Duration::from_secs(2);
/// A status WARN repeats at most this often while boot waits.
const STATUS_INTERVAL: Duration = Duration::from_secs(30);

const BOOT_CATCH_UP_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
const BOOT_CATCH_UP_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
/// ~15 min at the capped backoff: outlasts a restarting L1, but a permanently
/// refused RPC call still surfaces as an exit.
const BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES: u32 = 32;

/// Where a booting node gets the L2 history below L1's finalized root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// From devp2p peers. Never falls back to L1.
    Peers,
    /// Derived from L1 alone (`EEZ_SYNC_L1_ONLY`).
    L1Only,
}

impl SyncMode {
    /// Where the Deriver's history starts in this mode.
    #[must_use]
    pub fn deriver_boot(self) -> DeriverBoot {
        match self {
            Self::Peers => DeriverBoot::Peers,
            Self::L1Only => DeriverBoot::L1Only {
                checkpoint_dir: crate::read_checkpoint_dir(),
            },
        }
    }

    /// The L1 block boot needs served: L1-only scans from the deploy block.
    #[must_use]
    pub const fn readiness_probe(self, deploy_block: u64) -> ReadinessProbe {
        match self {
            Self::Peers => ReadinessProbe::Finalized,
            Self::L1Only => ReadinessProbe::DeployBlock(deploy_block),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Peers => "peers",
            Self::L1Only => "l1_only",
        }
    }
}

/// Parses `EEZ_SYNC_L1_ONLY`; unset or empty reads as [`SyncMode::Peers`].
/// Empty counts as unset because compose files pass `${VAR:-}`.
fn parse_sync_mode(raw: Option<&str>) -> eyre::Result<SyncMode> {
    match raw {
        None | Some("" | "0" | "false") => Ok(SyncMode::Peers),
        Some("1" | "true") => Ok(SyncMode::L1Only),
        Some(other) => Err(eyre::eyre!(
            "EEZ_SYNC_L1_ONLY must be 1, true, 0 or false, got {other:?}"
        )),
    }
}

/// Reads [`SyncMode`] from `EEZ_SYNC_L1_ONLY`.
///
/// # Errors
///
/// A value other than `1`, `true`, `0`, `false` or empty, or one that isn't Unicode.
pub fn sync_mode_from_env() -> eyre::Result<SyncMode> {
    match std::env::var("EEZ_SYNC_L1_ONLY") {
        Ok(raw) => parse_sync_mode(Some(&raw)),
        Err(std::env::VarError::NotPresent) => parse_sync_mode(None),
        Err(err) => Err(eyre::eyre!("EEZ_SYNC_L1_ONLY: {err}")),
    }
}

/// One step of the peers-mode boot, decided from L1's finalized commitment.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BootStep {
    /// L1 can't name the anchor yet; the cause.
    Wait(String),
    /// The rollup isn't registered at `l1_block`, so no post predates it.
    SeedGenesis { l1_block: u64, l1_hash: B256 },
    /// The local canonical chain holds the root at `l2_block`.
    Seed {
        l1_block: u64,
        l1_hash: B256,
        l2_block: u64,
    },
    /// The local chain lacks the root.
    Sync { root: B256 },
}

/// Decides the boot step. `local_root_number` gives a hash's height on the
/// local canonical chain. Errs on a read that retrying can't fix.
fn decide(
    commitment: L1Result<Option<FinalizedCommitment>>,
    local_root_number: impl FnOnce(B256) -> Option<u64>,
) -> eyre::Result<BootStep> {
    let FinalizedCommitment {
        l1_block,
        l1_hash,
        root,
    } = match commitment {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return Ok(BootStep::Wait("L1 has no finalized block yet".into())),
        Err(err) if err.is_retryable() => return Ok(BootStep::Wait(err.to_string())),
        Err(err) => return Err(eyre::eyre!("reading the L1-finalized rollup root: {err}")),
    };
    let Some(root) = root else {
        return Ok(BootStep::SeedGenesis { l1_block, l1_hash });
    };
    Ok(match local_root_number(root) {
        Some(l2_block) => BootStep::Seed {
            l1_block,
            l1_hash,
            l2_block,
        },
        None => BootStep::Sync { root },
    })
}

/// Depth below the L1 head past which an unserved block is pruned, not
/// lagging: two Ethereum epochs, beyond finality.
const L1_HISTORY_MISSING_DEPTH: u64 = 64;

/// Whether L1 no longer serves `block`, rather than not serving it yet.
fn l1_history_missing(block: u64, l1_head: u64) -> bool {
    block.saturating_add(L1_HISTORY_MISSING_DEPTH) < l1_head
}

/// Due at once, then at most every [`STATUS_INTERVAL`].
#[derive(Debug, Default)]
struct StatusEvery(Option<Instant>);

impl StatusEvery {
    fn due(&mut self) -> bool {
        let now = Instant::now();
        if self
            .0
            .is_some_and(|last| now.duration_since(last) < STATUS_INTERVAL)
        {
            return false;
        }
        self.0 = Some(now);
        true
    }
}

/// The index record that marks every post at or before L1 block `l1_block`
/// as processed (`log_index` past any real log), with L2 settled to `l2_block`.
fn seed_record(l1_block: u64, l1_hash: B256, l2_block: u64) -> BatchRecord {
    BatchRecord {
        post: PostId {
            l1_block,
            l1_block_hash: l1_hash,
            log_index: u64::MAX,
        },
        tx_hash: B256::ZERO,
        last_l2_block: l2_block,
    }
}

/// `hash`'s height when it is on the local canonical chain.
fn canonical_number<P: BlockNumReader>(provider: &P, hash: B256) -> eyre::Result<Option<u64>> {
    let Some(number) = provider.block_number(hash)? else {
        return Ok(None);
    };
    Ok((provider.block_hash(number)? == Some(hash)).then_some(number))
}

fn header_by_number<P>(provider: &P, number: u64) -> eyre::Result<SealedHeader<Header>>
where
    P: HeaderProvider<Header = Header>,
{
    provider
        .sealed_header(number)?
        .ok_or_else(|| eyre::eyre!("no local header at L2 block {number}"))
}

fn header_by_hash<P>(provider: &P, hash: B256) -> eyre::Result<SealedHeader<Header>>
where
    P: HeaderProvider<Header = Header>,
{
    provider
        .sealed_header_by_hash(hash)?
        .ok_or_else(|| eyre::eyre!("no local header for L2 block {hash}"))
}

/// Polls `is_syncing` every [`SYNC_POLL`] until reth is idle, sending it nothing.
/// Spawn the committer only after: a restart can resume reth's backfill, and a
/// head read before it ends is stale.
pub async fn wait_for_idle_engine(is_syncing: impl Fn() -> bool) {
    let mut busy = StatusEvery::default();
    while is_syncing() {
        if busy.due() {
            event!(
                name: "eez.node.sync.engine_busy",
                Level::WARN,
                "waiting for reth to finish its own sync before boot sends a forkchoice update",
            );
        }
        tokio::time::sleep(SYNC_POLL).await;
    }
}

/// Anchors the local chain in `mode` and runs the Deriver's boot catch-up;
/// returns the L1 watcher's seed. Call once reth is idle, with the committer
/// spawned after that ([`wait_for_idle_engine`]).
///
/// # Errors
///
/// An L1 read that retrying can't fix, a local provider or engine failure, a
/// datadir off L1's chain, or history L1 no longer serves in L1-only mode.
pub async fn boot<P, L2>(
    mode: SyncMode,
    provider: &P,
    committer: &BlockCommitterHandle<EezEngineTypes>,
    deriver: &Deriver<L2>,
    l1_reader: &L1Reader,
    peer_count: impl Fn() -> usize,
    role: &'static str,
) -> eyre::Result<(u64, B256)>
where
    P: BlockIdReader + HeaderProvider<Header = Header>,
    L2: BlockReader<Header = Header> + StateProviderFactory + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
    <L2 as ReceiptProvider>::Receipt: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
{
    event!(
        name: "eez.node.sync.mode",
        Level::INFO,
        event_name = "eez.node.sync.mode",
        mode = mode.name(),
        role,
        "booting the L2 chain",
    );
    match mode {
        SyncMode::Peers => {
            let seed = boot_from_peers(provider, committer, l1_reader, peer_count).await?;
            deriver.seed(seed);
            boot_catch_up(deriver, l1_reader, mode, role).await
        }
        SyncMode::L1Only => {
            // Read before the scan: the scan must end up holding this root.
            let before = match l1_reader.finalized_commitment().await {
                Ok(before) => before,
                Err(error) => {
                    event!(
                        name: "eez.node.sync.history_check_skipped",
                        Level::WARN,
                        %error,
                        "L1's finalized root is unreadable; boot skips checking that the local chain holds it",
                    );
                    None
                }
            };
            let seed = boot_catch_up(deriver, l1_reader, mode, role).await?;
            ensure_l1_history(provider, before)?;
            Ok(seed)
        }
    }
}

/// Syncs the local chain to L1's finalized root through peers; returns the
/// Deriver's seed. Waits while L1 or the peers can't serve it, never falling
/// back to L1. Its caller already waited for reth to be idle.
async fn boot_from_peers<P>(
    provider: &P,
    committer: &BlockCommitterHandle<EezEngineTypes>,
    l1_reader: &L1Reader,
    peer_count: impl Fn() -> usize,
) -> eyre::Result<BatchRecord>
where
    P: BlockIdReader + HeaderProvider<Header = Header>,
{
    let mut waiting = StatusEvery::default();
    let mut waiting_for_peers = StatusEvery::default();
    // The first sync rewinds to the local finalized block; later top-ups build on it.
    let mut rewound: Option<SealedHeader<Header>> = None;
    loop {
        let commitment = l1_reader.finalized_commitment().await;
        let l1_finalized = commitment
            .as_ref()
            .ok()
            .copied()
            .flatten()
            .map(|c| c.l1_block);
        // The lookup can't return an error through `decide`; a failed local read stops boot.
        let mut lookup_err = None;
        let step = decide(commitment, |root| {
            canonical_number(provider, root).unwrap_or_else(|err| {
                lookup_err = Some(err);
                None
            })
        })?;
        if let Some(err) = lookup_err {
            return Err(err);
        }

        let root = match step {
            BootStep::Wait(cause) => {
                if waiting.due() {
                    event!(
                        name: "eez.node.sync.waiting",
                        Level::WARN,
                        cause,
                        "waiting for L1 to name the finalized L2 root",
                    );
                }
                tokio::time::sleep(SYNC_POLL).await;
                continue;
            }
            BootStep::SeedGenesis { l1_block, l1_hash } => {
                return Ok(seed_record(l1_block, l1_hash, 0));
            }
            BootStep::Seed {
                l1_block,
                l1_hash,
                l2_block,
            } => {
                let header = header_by_number(provider, l2_block)?;
                let hash = header.hash();
                committer.advance_safe_finalized(header, hash).await?;
                return Ok(seed_record(l1_block, l1_hash, l2_block));
            }
            BootStep::Sync { root } => root,
        };

        let f = match &rewound {
            Some(f) => f.clone(),
            None => {
                // Drop any local tail above a block L1 finalized, so reth syncs forward from it.
                let f = match provider.finalized_block_num_hash()? {
                    Some(finalized) => header_by_hash(provider, finalized.hash)?,
                    None => header_by_number(provider, 0)?,
                };
                committer
                    .advance_safe_finalized(f.clone(), f.hash())
                    .await?;
                committer.reorg_to(f.clone()).await?;
                rewound.insert(f).clone()
            }
        };

        loop {
            match committer.sync_to(root).await? {
                ForkchoiceOutcome::Valid => break,
                ForkchoiceOutcome::Syncing => {
                    if waiting_for_peers.due() {
                        event!(
                            name: "eez.node.sync.waiting_for_peers",
                            Level::WARN,
                            event_name = "eez.node.sync.waiting_for_peers",
                            peers = peer_count(),
                            local_best = provider.best_block_number()?,
                            l1_finalized,
                            target = %root,
                            "syncing L2 history from peers to the L1-finalized root",
                        );
                    }
                    tokio::time::sleep(SYNC_POLL).await;
                }
                ForkchoiceOutcome::InvalidState => {
                    return Err(eyre::eyre!(
                        "reth rejected the forkchoice to L1-finalized root {root} as inconsistent"
                    ));
                }
            }
        }

        // reth is at the root; an FCU here would still carry the old safe `f`.
        committer
            .adopt_synced(header_by_hash(provider, root)?)
            .await?;

        // `f`'s parent, not `f`: a resume can replace `f` with an L1-settled sibling.
        if f.number > 0 && provider.block_hash(f.number - 1)? != Some(f.parent_hash) {
            return Err(eyre::eyre!(
                "datadir is not on L1's chain: block {} {} is no longer canonical after syncing to L1-finalized root {}",
                f.number - 1,
                f.parent_hash,
                root,
            ));
        }
    }
}

/// Runs the Deriver's boot catch-up, retrying while the L1 source catches up.
/// `role` tags the events' `mode` field. Errs on exhausted L1 transport, an
/// L1-only scan past pruned L1 history, or any error a retry can't fix.
async fn boot_catch_up<L2>(
    deriver: &Deriver<L2>,
    l1_reader: &L1Reader,
    mode: SyncMode,
    role: &'static str,
) -> eyre::Result<(u64, B256)>
where
    L2: BlockReader<Header = Header> + StateProviderFactory + Send + Sync + 'static,
    <L2 as TransactionsProvider>::Transaction: Encodable2718,
    <L2 as ReceiptProvider>::Receipt: alloy_consensus::TxReceipt<Log = alloy_primitives::Log>,
{
    let mut retry_delay = BOOT_CATCH_UP_INITIAL_RETRY_DELAY;
    let mut attempts = 0_u64;
    let mut transport_failures = 0_u32;
    loop {
        let err = match deriver.catch_up_with_seed().await {
            Ok(seed) => return Ok(seed),
            Err(err) => err,
        };
        // L1-only has nowhere else to read history, so waiting on it can't help.
        if mode == SyncMode::L1Only
            && let DeriverError::L1Scan(L1Error::SourceIncomplete { block, .. }) = &err
            && let Ok(readiness) = l1_reader.readiness().await
            && l1_history_missing(*block, readiness.head_block_number)
        {
            event!(
                name: "eez.node.deriver.boot_catch_up.history_incomplete",
                Level::ERROR,
                mode = role,
                block,
                l1_head = readiness.head_block_number,
                error = %err,
                "L1 no longer serves a block L1-only derivation needs",
            );
            return Err(eyre::eyre!(
                "L1 history incomplete: L1 no longer serves block {block}"
            ));
        }
        match &err {
            DeriverError::L1Scan(L1Error::Provider(_))
                if {
                    transport_failures += 1;
                    transport_failures >= BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES
                } =>
            {
                event!(
                    name: "eez.node.deriver.boot_catch_up.transport_exhausted",
                    Level::ERROR,
                    mode = role,
                    transport_failures,
                    error = %err,
                    "L1 transport kept failing during boot catch-up; the endpoint is likely refusing a call we need, not merely unreachable",
                );
                return Err(eyre::eyre!(
                    "boot-time deriver catch_up gave up after {transport_failures} L1 transport failures: {err}"
                ));
            }
            DeriverError::L1Scan(l1) if l1.is_retryable() => {
                attempts += 1;
                event!(
                    name: "eez.node.deriver.boot_catch_up.source_incomplete",
                    Level::WARN,
                    mode = role,
                    attempts,
                    retry_delay_secs = retry_delay.as_secs(),
                    error = %err,
                    "boot-time catch_up could not read all L1 source data yet; retrying before starting L1-active tasks",
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = Duration::from_secs(
                    retry_delay
                        .as_secs()
                        .saturating_mul(2)
                        .min(BOOT_CATCH_UP_MAX_RETRY_DELAY.as_secs()),
                );
            }
            _ => {
                event!(
                    name: "eez.node.deriver.boot_catch_up.failed",
                    Level::ERROR,
                    event_name = "eez.node.deriver.boot_catch_up.failed",
                    mode = role,
                    error = %err,
                    "boot-time catch_up failed; refusing to start L1-active tasks before reconciliation",
                );
                return Err(eyre::eyre!("boot-time deriver catch_up failed: {err}"));
            }
        }
    }
}

/// Checks that the local chain holds the finalized root an L1-only scan passed.
fn ensure_l1_history<P>(provider: &P, before: Option<FinalizedCommitment>) -> eyre::Result<()>
where
    P: BlockNumReader,
{
    let Some(FinalizedCommitment {
        l1_block,
        root: Some(root),
        ..
    }) = before
    else {
        return Ok(());
    };
    if canonical_number(provider, root)?.is_none() {
        return Err(eyre::eyre!(
            "L1 history incomplete: L1-only derivation passed L1 block {l1_block} but the local chain lacks its finalized root {root}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::ChainInfo;
    use reth_storage_api::{BlockHashReader, errors::ProviderResult};

    const L1_HASH: B256 = B256::repeat_byte(0xf1);
    const ROOT: B256 = B256::repeat_byte(0xa7);

    fn commitment(root: Option<B256>) -> FinalizedCommitment {
        FinalizedCommitment {
            l1_block: 40,
            l1_hash: L1_HASH,
            root,
        }
    }

    #[test]
    fn parse_sync_mode_table() {
        for raw in [None, Some(""), Some("0"), Some("false")] {
            assert_eq!(parse_sync_mode(raw).unwrap(), SyncMode::Peers, "{raw:?}");
        }
        for raw in ["1", "true"] {
            assert_eq!(
                parse_sync_mode(Some(raw)).unwrap(),
                SyncMode::L1Only,
                "{raw}"
            );
        }
        assert!(parse_sync_mode(Some("yes")).is_err());
    }

    #[test]
    fn decide_table() {
        let not_local = |_| None;
        assert!(matches!(
            decide(Ok(None), not_local).unwrap(),
            BootStep::Wait(_)
        ));
        assert_eq!(
            decide(Ok(Some(commitment(None))), not_local).unwrap(),
            BootStep::SeedGenesis {
                l1_block: 40,
                l1_hash: L1_HASH
            }
        );
        assert_eq!(
            decide(Ok(Some(commitment(Some(ROOT)))), |hash| (hash == ROOT)
                .then_some(7))
            .unwrap(),
            BootStep::Seed {
                l1_block: 40,
                l1_hash: L1_HASH,
                l2_block: 7
            }
        );
        assert!(decide(Err(L1Error::Config("zero root".into())), not_local).is_err());
    }

    #[test]
    fn local_without_root_syncs() {
        assert_eq!(
            decide(Ok(Some(commitment(Some(ROOT)))), |_| None).unwrap(),
            BootStep::Sync { root: ROOT }
        );
    }

    #[test]
    fn retryable_commitment_error_waits() {
        assert!(matches!(
            decide(Err(L1Error::Provider("x".into())), |_| None).unwrap(),
            BootStep::Wait(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn boot_waits_out_a_busy_engine() {
        let polls = std::cell::Cell::new(0);
        wait_for_idle_engine(|| {
            polls.set(polls.get() + 1);
            polls.get() < 3
        })
        .await;
        assert_eq!(polls.get(), 3);
    }

    /// Knows `block` at `number`; the canonical chain holds `canonical` there.
    /// Not `MockEthProvider`: its `block_hash` scans a randomly seeded map, so
    /// two headers at one height give a random answer.
    struct Fork {
        block: B256,
        number: u64,
        canonical: B256,
    }

    impl BlockHashReader for Fork {
        fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
            Ok((number == self.number).then_some(self.canonical))
        }

        fn canonical_hashes_range(&self, _start: u64, _end: u64) -> ProviderResult<Vec<B256>> {
            unimplemented!()
        }
    }

    impl BlockNumReader for Fork {
        fn chain_info(&self) -> ProviderResult<ChainInfo> {
            unimplemented!()
        }

        fn best_block_number(&self) -> ProviderResult<u64> {
            unimplemented!()
        }

        fn last_block_number(&self) -> ProviderResult<u64> {
            unimplemented!()
        }

        fn block_number(&self, hash: B256) -> ProviderResult<Option<u64>> {
            Ok((hash == self.block).then_some(self.number))
        }
    }

    #[test]
    fn ensure_l1_history_needs_the_root_canonical() {
        let fork = |canonical| Fork {
            block: ROOT,
            number: 5,
            canonical,
        };
        let sibling = B256::repeat_byte(0x5b);

        assert!(ensure_l1_history(&fork(sibling), None).is_ok());
        assert!(ensure_l1_history(&fork(ROOT), Some(commitment(Some(ROOT)))).is_ok());
        let err = ensure_l1_history(&fork(sibling), Some(commitment(Some(ROOT)))).unwrap_err();
        assert!(err.to_string().contains("L1 history incomplete"), "{err}");
    }

    #[test]
    fn missing_history_detected() {
        assert!(l1_history_missing(100, 165));
        assert!(!l1_history_missing(100, 164));
        assert!(!l1_history_missing(100, 100));
    }
}
