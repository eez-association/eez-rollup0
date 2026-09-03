//! Persistent per-block witness store for the composer-controlled prover
//! (remote mode).
//!
//! At commit, the committer feeds each produced block's hash to [`run_capture`],
//! which re-executes it (parent state still fresh) and persists the augmented
//! witness. [`NodeWitnessSource`] serves the committed window of
//! [`eez_prover::ProvingContext::blocks`] from the store, re-execing on demand
//! only for the newest blocks not yet drained.
//!
//! Persistent (mdbx), not RAM: the node is non-archival, so by settlement time an
//! older block's parent state is pruned — a RAM store lost on restart couldn't
//! re-derive it, stalling a restart with an unsettled backlog. Dedicated env
//! (never reth's node DB), keyed by block number.
//!
//! Purge floor = L1-FINALIZED (not merely-posted) L2 height: an L1 reorg could
//! roll a posted batch out and need its witness back. The endpoint (uncommitted
//! Sync block) is captured by the composer's `endpoint_witness`, not here.

use std::path::Path;
use std::sync::Arc;

use alloy_consensus::Header;
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{B256, Bytes};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_driver::witness::{ExecutionWitnessMode, block_witness};
use eez_prover::{BlockWitness, ProvingWitnessSource};
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::ConfigureEvm;
use reth_libmdbx::{DatabaseFlags, Environment, Geometry, WriteFlags};
use reth_storage_api::{BlockReader, HeaderProvider, StateProviderFactory, TransactionVariant};
use tokio::sync::mpsc;
use tracing::{Level, event};

const GIGABYTE: usize = 1024 * 1024 * 1024;
/// mdbx reserves this as sparse virtual address space (no preallocation) and
/// grows the file lazily, so a generous cap just means "never `MAP_FULL`".
const MAP_MAX_BYTES: usize = 256 * GIGABYTE;
const GROWTH_STEP: isize = 1024 * 1024 * 1024;

/// Persistent witness store: a dedicated mdbx env keyed by block number
/// (big-endian, so key order == numeric order → ordered purge). Cheap `Arc`
/// clone; mdbx MVCC needs no extra lock.
pub struct WitnessDb {
    env: Environment,
}

impl std::fmt::Debug for WitnessDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WitnessDb").finish_non_exhaustive()
    }
}

impl WitnessDb {
    /// Open (creating if absent) the witness env at `path`.
    ///
    /// # Errors
    /// Filesystem or mdbx open failures.
    pub fn open(path: &Path) -> eyre::Result<Self> {
        std::fs::create_dir_all(path)?;
        let mut builder = Environment::builder();
        builder.set_geometry(Geometry {
            size: Some(0..MAP_MAX_BYTES),
            growth_step: Some(GROWTH_STEP),
            shrink_threshold: None,
            page_size: None,
        });
        let env = builder
            .open(path)
            .map_err(|e| eyre::eyre!("open witness env at {}: {e}", path.display()))?;
        let txn = env.begin_rw_txn()?;
        txn.create_db(None, DatabaseFlags::empty())?; // default (unnamed) table
        txn.commit()?;
        Ok(Self { env })
    }

    /// Persist `bw` under its block number (overwrites on reorg re-capture).
    ///
    /// # Errors
    /// Serialization or mdbx write failures.
    pub fn put(&self, number: u64, bw: &BlockWitness) -> eyre::Result<()> {
        let bytes = postcard::to_allocvec(&StoredWitness::from(bw))
            .map_err(|e| eyre::eyre!("encode witness {number}: {e}"))?;
        let txn = self.env.begin_rw_txn()?;
        let db = txn.open_db(None)?;
        txn.put(db.dbi(), number.to_be_bytes(), &bytes, WriteFlags::UPSERT)?;
        txn.commit()?;
        Ok(())
    }

    /// Read the witness at `number`, or `None` if absent.
    ///
    /// # Errors
    /// mdbx read or deserialization failures.
    pub fn get(&self, number: u64) -> eyre::Result<Option<BlockWitness>> {
        let txn = self.env.begin_ro_txn()?;
        let db = txn.open_db(None)?;
        let raw: Option<Vec<u8>> = txn.get(db.dbi(), &number.to_be_bytes())?;
        match raw {
            Some(bytes) => {
                let stored: StoredWitness = postcard::from_bytes(&bytes)
                    .map_err(|e| eyre::eyre!("decode witness {number}: {e}"))?;
                Ok(Some(stored.into()))
            }
            None => Ok(None),
        }
    }

    /// Delete every witness with block number `< below`, returning the count.
    /// Call with the L1-FINALIZED L2 height — those blocks can't need re-settlement.
    ///
    /// # Errors
    /// mdbx write failures.
    pub fn purge_settled(&self, below: u64) -> eyre::Result<usize> {
        let below_be = below.to_be_bytes();
        let txn = self.env.begin_rw_txn()?;
        let db = txn.open_db(None)?;
        // Collect-then-delete in one txn: mdbx cursor-during-delete is subtle.
        let mut doomed: Vec<[u8; 8]> = Vec::new();
        {
            let mut cursor = txn.cursor(db.dbi())?;
            let mut entry: Option<([u8; 8], ())> = cursor.first()?;
            while let Some((key, ())) = entry {
                if key >= below_be {
                    break; // ordered keys → the rest are all >= floor
                }
                doomed.push(key);
                entry = cursor.next()?;
            }
        }
        for key in &doomed {
            txn.del(db.dbi(), key, None)?;
        }
        txn.commit()?;
        Ok(doomed.len())
    }
}

/// On-disk mirror of [`BlockWitness`] (postcard). Scalars are raw bytes to avoid
/// depending on alloy's optional `serde` feature; `ExecutionWitness` self-serdes.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredWitness {
    number: u64,
    hash: [u8; 32],
    parent_hash: [u8; 32],
    rlp: Vec<u8>,
    witness: ExecutionWitness,
}

impl From<&BlockWitness> for StoredWitness {
    fn from(b: &BlockWitness) -> Self {
        Self {
            number: b.number,
            hash: b.hash.0,
            parent_hash: b.parent_hash.0,
            rlp: b.rlp.to_vec(),
            witness: b.witness.clone(),
        }
    }
}

impl From<StoredWitness> for BlockWitness {
    fn from(s: StoredWitness) -> Self {
        Self {
            number: s.number,
            hash: B256::from(s.hash),
            parent_hash: B256::from(s.parent_hash),
            rlp: Bytes::from(s.rlp),
            witness: s.witness,
        }
    }
}

/// Shared handle to the persistent witness store.
pub type WitnessStore = Arc<WitnessDb>;

/// Open the dedicated witness environment at `path`.
///
/// # Errors
/// See [`WitnessDb::open`].
pub fn new_store(path: &Path) -> eyre::Result<WitnessStore> {
    Ok(Arc::new(WitnessDb::open(path)?))
}

/// The composer's [`ProvingWitnessSource`]: reads a captured witness from the
/// persistent store, falling back to on-demand re-exec for the newest blocks the
/// capture task hasn't drained yet (parent state still fresh).
#[derive(Clone)]
pub struct NodeWitnessSource<P, E> {
    store: WitnessStore,
    provider: P,
    evm_config: E,
}

impl<P, E> NodeWitnessSource<P, E> {
    #[must_use]
    pub const fn new(store: WitnessStore, provider: P, evm_config: E) -> Self {
        Self {
            store,
            provider,
            evm_config,
        }
    }
}

impl<P, E> std::fmt::Debug for NodeWitnessSource<P, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeWitnessSource").finish_non_exhaustive()
    }
}

impl<P, E> ProvingWitnessSource for NodeWitnessSource<P, E>
where
    P: BlockReader<Block = Block>
        + StateProviderFactory
        + HeaderProvider<Header = Header>
        + Send
        + Sync,
    E: ConfigureEvm<Primitives = EthPrimitives> + Send + Sync,
{
    fn block_witness(&self, number: u64) -> Result<BlockWitness, String> {
        match self.store.get(number) {
            // Reorg guard: the store is keyed by block number, so a witness
            // captured before a reorg can be stale. Trust the hit only if its
            // hash matches the current canonical block; otherwise fall through to
            // re-exec the canonical block by number.
            Ok(Some(bw)) => match self.provider.block_hash(number) {
                Ok(Some(canon)) if canon == bw.hash => return Ok(bw),
                Ok(_) => {
                    event!(
                        name: "eez.node.witness_store.stale_reorg",
                        Level::WARN,
                        number,
                        stored = %bw.hash,
                        "stored witness hash != canonical block; re-executing (reorg)",
                    );
                }
                Err(e) => {
                    event!(
                        name: "eez.node.witness_store.canon_hash_failed",
                        Level::WARN,
                        number,
                        error = %e,
                        "canonical hash lookup failed; re-executing on-demand",
                    );
                }
            },
            Ok(None) => {} // not captured yet → re-exec fallback below
            Err(e) => {
                // Unexpected; fall back to re-exec rather than fail the slot.
                event!(
                    name: "eez.node.witness_store.read_failed",
                    Level::WARN,
                    number,
                    error = %e,
                    "witness store read failed; falling back to on-demand re-exec",
                );
            }
        }
        build_block_witness(
            &self.provider,
            &self.evm_config,
            BlockHashOrNumber::Number(number),
        )
    }
}

/// Re-execute a committed block on its parent state → augmented witness.
/// Blocking (trie walk); call under `spawn_blocking`.
fn build_block_witness<P, E>(
    provider: &P,
    evm_config: &E,
    id: BlockHashOrNumber,
) -> Result<BlockWitness, String>
where
    P: BlockReader<Block = Block> + StateProviderFactory + HeaderProvider<Header = Header>,
    E: ConfigureEvm<Primitives = EthPrimitives>,
{
    let block = provider
        .recovered_block(id, TransactionVariant::WithHash)
        .map_err(|e| format!("fetch block {id:?}: {e}"))?
        .ok_or_else(|| format!("witness for block {id:?} not captured yet"))?;
    // Legacy + removal-closure augmentation: the intermediate-per-tx-root nodes
    // the prover's re-execution needs (MPT completeness fix).
    block_witness(provider, evm_config, &block, ExecutionWitnessMode::Legacy)
        .map_err(|e| format!("witness for block {id:?}: {e}"))
}

/// Drain committed-block hashes, capture each block's witness at commit (parent
/// state fresh), persist it, and purge everything the L1 has FINALIZED
/// (`settled_floor`). Runs until the channel closes.
pub async fn run_capture<P, E, F>(
    mut rx: mpsc::UnboundedReceiver<B256>,
    store: WitnessStore,
    provider: P,
    evm_config: E,
    settled_floor: F,
) where
    P: BlockReader<Block = Block>
        + StateProviderFactory
        + HeaderProvider<Header = Header>
        + Clone
        + Send
        + Sync
        + 'static,
    E: ConfigureEvm<Primitives = EthPrimitives> + Clone + Send + Sync + 'static,
    F: Fn() -> u64 + Send + 'static,
{
    while let Some(hash) = rx.recv().await {
        let provider = provider.clone();
        let evm_config = evm_config.clone();
        let built = tokio::task::spawn_blocking(move || {
            build_block_witness(&provider, &evm_config, BlockHashOrNumber::Hash(hash))
        })
        .await;
        match built {
            Ok(Ok(bw)) => {
                let number = bw.number;
                if let Err(e) = store.put(number, &bw) {
                    event!(
                        name: "eez.node.witness_capture.persist_failed",
                        Level::ERROR,
                        number,
                        error = %e,
                        "persisting captured witness failed",
                    );
                }
                // Drop finalized witnesses; cheap when nothing is below the floor.
                match store.purge_settled(settled_floor()) {
                    Ok(n) if n > 0 => event!(
                        name: "eez.node.witness_store.purged",
                        Level::DEBUG,
                        removed = n,
                        "purged finalized witnesses",
                    ),
                    Ok(_) => {}
                    Err(e) => event!(
                        name: "eez.node.witness_store.purge_failed",
                        Level::WARN,
                        error = %e,
                        "purging finalized witnesses failed",
                    ),
                }
            }
            Ok(Err(e)) => event!(
                name: "eez.node.witness_capture.failed",
                Level::WARN,
                %hash,
                error = %e,
                "witness capture failed for a committed block; composer will retry the Sync slot",
            ),
            Err(e) => event!(
                name: "eez.node.witness_capture.join_error",
                Level::ERROR,
                %hash,
                error = %e,
                "witness capture task panicked",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StoredWitness, WitnessDb};
    use alloy_primitives::{B256, Bytes};
    use alloy_rpc_types_debug::ExecutionWitness;
    use eez_prover::BlockWitness;

    fn sample(number: u64) -> BlockWitness {
        BlockWitness {
            number,
            hash: B256::repeat_byte(number as u8),
            parent_hash: B256::repeat_byte(number.wrapping_sub(1) as u8),
            rlp: Bytes::from(vec![number as u8; 4]),
            witness: ExecutionWitness {
                state: vec![Bytes::from(vec![1, 2, 3])],
                codes: vec![],
                keys: vec![],
                headers: vec![],
            },
        }
    }

    /// put → get round-trips through postcard, survives a close+reopen
    /// (restart), and `purge_settled` removes exactly the below-floor prefix.
    #[test]
    fn persist_get_purge_restart() {
        let dir = tempfile::tempdir().unwrap();
        for n in [10u64, 11, 12] {
            let db = WitnessDb::open(dir.path()).unwrap();
            db.put(n, &sample(n)).unwrap();
        } // each `db` drops → env closed; reopening below == a restart

        let db = WitnessDb::open(dir.path()).unwrap();
        // Reads survive restart.
        let got = db.get(11).unwrap().expect("11 present after restart");
        assert_eq!(got.number, 11);
        assert_eq!(got.rlp, Bytes::from(vec![11u8; 4]));
        assert!(db.get(99).unwrap().is_none());

        // Purge < 12 removes 10 and 11, keeps 12.
        assert_eq!(db.purge_settled(12).unwrap(), 2);
        assert!(db.get(10).unwrap().is_none());
        assert!(db.get(11).unwrap().is_none());
        assert!(db.get(12).unwrap().is_some());
        // Idempotent: nothing left below the floor.
        assert_eq!(db.purge_settled(12).unwrap(), 0);
    }

    #[test]
    fn stored_witness_roundtrip() {
        let bw = sample(7);
        let bytes = postcard::to_allocvec(&StoredWitness::from(&bw)).unwrap();
        let back: BlockWitness = postcard::from_bytes::<StoredWitness>(&bytes)
            .unwrap()
            .into();
        assert_eq!(back.number, bw.number);
        assert_eq!(back.hash, bw.hash);
        assert_eq!(back.parent_hash, bw.parent_hash);
        assert_eq!(back.rlp, bw.rlp);
        assert_eq!(back.witness.state, bw.witness.state);
    }
}
