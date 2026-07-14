//! Commit-time witness capture for the composer-controlled prover (remote mode).
//!
//! The block committer emits each PRODUCED block's hash the instant it commits
//! canonically. [`run_capture`] re-executes that block RIGHT THEN — while its
//! parent state is still retained — and stores the augmented execution witness in
//! a [`WitnessStore`] keyed by block number. [`NodeWitnessSource`] (the composer's
//! [`ProvingWitnessSource`]) fills the committed part of
//! [`eez_prover::ProvingContext::blocks`] from the store, with an on-demand
//! re-exec fallback for the newest committed blocks the capture task hasn't
//! drained yet (their parent state is still fresh).
//!
//! Capturing at commit — rather than regenerating everything on demand at
//! settlement — is what keeps this sound on a non-archival node: by settlement
//! time an older block's parent state is pruned, so its witness must already be
//! stored (captured when the state was fresh). The settlement window's ENDPOINT —
//! the Sync block the composer just built but has NOT committed — is not served
//! here at all: no store or provider can serve an uncommitted block, so the
//! composer captures that one directly from its in-memory block (eez-composer's
//! `endpoint_witness`). In mock mode no store/task is wired and `blocks` stays
//! empty.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use alloy_consensus::Header;
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use eez_driver::witness::{ExecutionWitnessMode, block_witness};
use eez_prover::{BlockWitness, ProvingWitnessSource};
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::ConfigureEvm;
use reth_storage_api::{BlockReader, HeaderProvider, StateProviderFactory, TransactionVariant};
use tokio::sync::mpsc;
use tracing::{Level, event};

/// Per-block augmented witnesses, keyed by L2 block number. Written by
/// [`run_capture`] at commit-time, read by [`NodeWitnessSource`].
pub type WitnessStore = Arc<Mutex<BTreeMap<u64, BlockWitness>>>;

/// Retain at most this many blocks behind the newest captured one (bounds memory;
/// far more than any settlement window `[posted+1 .. sync]`).
const RETAIN_BEHIND_TIP: u64 = 4096;

/// Fresh empty [`WitnessStore`].
#[must_use]
pub fn new_store() -> WitnessStore {
    Arc::new(Mutex::new(BTreeMap::new()))
}

/// The composer's [`ProvingWitnessSource`]: reads a pre-captured witness from the
/// shared [`WitnessStore`]. A miss ("not captured yet") is a transient capture
/// lag, not a hard failure — the composer defers the Sync slot and retries next
/// trigger; the witness is soundly built at commit (fresh state), just a beat late.
#[derive(Clone)]
pub struct NodeWitnessSource<P, E> {
    store: WitnessStore,
    provider: P,
    evm_config: E,
}

impl<P, E> NodeWitnessSource<P, E> {
    /// Read pre-captured witnesses from `store`, falling back to an on-demand
    /// re-exec (`provider`/`evm_config`) on a miss — the newest blocks the
    /// commit-time capture task hasn't drained yet, whose parent state is still
    /// retained (so the re-exec is sound). Older blocks (state since pruned) are
    /// served from the store, captured when their state was fresh.
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
        if let Some(bw) = self
            .store
            .lock()
            .map_err(|_| "witness store poisoned".to_string())?
            .get(&number)
            .cloned()
        {
            return Ok(bw);
        }
        // Store miss: the capture task hasn't drained this (newest) block yet.
        // Its parent state is still retained, so re-execute on the spot.
        build_block_witness(
            &self.provider,
            &self.evm_config,
            BlockHashOrNumber::Number(number),
        )
    }
}

/// Re-execute one committed block (by hash) on its parent state and build its
/// augmented [`BlockWitness`]. Blocking (trie walk) — call under `spawn_blocking`.
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
    // `Legacy` + the removal-closure augmentation: carries the intermediate-per-tx
    // -root nodes the prover's re-execution needs (the MPT completeness fix).
    block_witness(provider, evm_config, &block, ExecutionWitnessMode::Legacy)
        .map_err(|e| format!("witness for block {id:?}: {e}"))
}

/// Drain committed-block hashes from the committer's witness feed, capture each
/// block's augmented witness AT COMMIT (parent state still fresh), and store it —
/// bounding the store to the last [`RETAIN_BEHIND_TIP`] blocks. Runs until the
/// channel closes.
pub async fn run_capture<P, E>(
    mut rx: mpsc::UnboundedReceiver<B256>,
    store: WitnessStore,
    provider: P,
    evm_config: E,
) where
    P: BlockReader<Block = Block>
        + StateProviderFactory
        + HeaderProvider<Header = Header>
        + Clone
        + Send
        + Sync
        + 'static,
    E: ConfigureEvm<Primitives = EthPrimitives> + Clone + Send + Sync + 'static,
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
                if let Ok(mut m) = store.lock() {
                    m.insert(number, bw);
                    let cutoff = number.saturating_sub(RETAIN_BEHIND_TIP);
                    while let Some((&low, _)) = m.iter().next() {
                        if low < cutoff {
                            m.remove(&low);
                        } else {
                            break;
                        }
                    }
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
