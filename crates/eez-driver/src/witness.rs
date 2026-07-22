//! Per-block execution-witness generation for the proving feed (Phase 0a).
//!
//! Replicates reth's `debug_executionWitness` path as a standalone
//! function so the block committer can call it once per canonical block:
//!   1. re-execute the block on its parent state → exact access set,
//!   2. record it (`ExecutionWitnessRecord`, cheap cache walk, no trie),
//!   3. one batched trie pass → the trie nodes + ancestor headers.
//!
//! Mirrors `reth_engine_tree`'s invalid-block witness hook
//! (`re_execute_block`) and `DebugApi::debug_execution_witness_for_block`.
//! The node's prover feed defaults to `ExecutionWitnessMode::Legacy`
//! (ZisK's `native-validate` needs it for outbound/cross-chain blocks);
//! `Canonical` is the minimal witness (sorted, deduped, no empty nodes).
//!
//! Why re-execute instead of capturing during the build: the witness must
//! be **exact** (the prover requires a minimal witness), and only an
//! execution of the *final* tx list yields that. The Normal build path
//! (mempool fill) warms a superset; `newPayload` validation would be exact
//! but lives in reth's engine tree (a deep fork). Re-executing the
//! canonical block here is `debug_executionWitness`'s own approach and is
//! cheap for small L2 blocks.

use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_prover::BlockWitness;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_primitives_traits::RecoveredBlock;
use reth_revm::{database::StateProviderDatabase, witness::ExecutionWitnessRecord};
use reth_storage_api::{HeaderProvider, StateProviderFactory};
use reth_trie::{HashedPostState, HashedStorage};
use std::collections::HashSet;
use tracing::{debug, trace};
// Re-exported so the prover-feed binary can pick a mode without depending on reth-trie.
pub use reth_trie::ExecutionWitnessMode;

/// Parse a `--witness.mode` flag value into an [`ExecutionWitnessMode`].
///
/// `canonical` = the minimized v2 format; `legacy` = the older format some
/// stateless validators (e.g. ZisK's) expect. Used by the prover-feed
/// binary (prover-chain P3).
pub fn witness_mode_from_str(s: &str) -> eyre::Result<ExecutionWitnessMode> {
    match s.to_ascii_lowercase().as_str() {
        "canonical" => Ok(ExecutionWitnessMode::Canonical),
        "legacy" => Ok(ExecutionWitnessMode::Legacy),
        other => eyre::bail!("unknown witness mode {other:?} (expected `canonical` or `legacy`)"),
    }
}

/// Re-execute `block` on its parent state and produce its execution witness
/// (trie nodes + contract codes + key preimages + ancestor headers).
///
/// The endpoint witness is augmented with a removals-first closure over every
/// touched storage slot. EEZ's prover recomputes intermediate per-transaction
/// state roots, not only the final block root. A parent->final witness can be
/// sufficient for whole-block stateless execution while still missing sibling
/// nodes needed to collapse a storage trie after an intermediate deletion. The
/// extra closure supplies those content-addressed nodes without changing the
/// consumer: validators still compute roots independently and reject corrupted
/// or unrelated nodes by hash mismatch.
///
/// `provider` must expose the block's parent state. When the parent is an
/// in-memory (not-yet-persisted) block, the memory-overlay provider folds
/// the parent's `TrieInput` in internally — no manual `TrieInput` needed.
///
/// The block's txs are executed exactly as committed, so the recorded
/// access set is exact; the removal-closure augmentation is intentionally a
/// proof superset for intermediate roots.
/// Build the full [`BlockWitness`] for an already-recovered block: its consensus
/// RLP plus the augmented execution witness the prover's re-execution needs.
///
/// The caller supplies the block — freshly built (the composer's Sync block, not
/// yet committed) or fetched from the provider (the node's commit-time capture) —
/// and this re-executes it on its parent's state to record the witness. Single
/// source of truth for turning a block into prover input.
///
/// # Errors
///
/// Propagates re-execution / witness-augmentation failures.
pub fn block_witness<P, E>(
    provider: &P,
    evm_config: &E,
    block: &RecoveredBlock<Block>,
    mode: ExecutionWitnessMode,
) -> eyre::Result<BlockWitness>
where
    P: StateProviderFactory + HeaderProvider<Header = Header>,
    E: ConfigureEvm<Primitives = EthPrimitives>,
{
    let witness = block_execution_witness(provider, evm_config, block, mode)?;
    Ok(BlockWitness {
        number: block.header().number(),
        hash: block.hash(),
        parent_hash: block.header().parent_hash(),
        rlp: alloy_rlp::encode(block.clone().into_sealed_block().into_block()).into(),
        witness,
    })
}

pub fn block_execution_witness<P, E>(
    provider: &P,
    evm_config: &E,
    block: &RecoveredBlock<Block>,
    mode: ExecutionWitnessMode,
) -> eyre::Result<ExecutionWitness>
where
    P: StateProviderFactory + HeaderProvider<Header = Header>,
    E: ConfigureEvm<Primitives = EthPrimitives>,
{
    let parent_hash = block.header().parent_hash();
    let block_number = block.header().number();
    trace!(
        target: "eez::witness",
        block_number,
        %parent_hash,
        "block_execution_witness: re-executing block on parent state",
    );

    // 1. Re-execute the exact canonical block on the parent state. The
    //    executor builds its own `State` over the parent provider; reads
    //    warm its cache → that cache is the exact access set.
    let mut executor = evm_config.batch_executor(StateProviderDatabase::new(
        provider.state_by_block_hash(parent_hash)?,
    ));
    executor.execute_one(block)?;
    let state = executor.into_state();

    // 2. Record the access set (cheap: cache walk, no trie).
    let record = ExecutionWitnessRecord::from_executed_state(&state, mode);
    let (removal_state, removal_accounts, removal_slots) = touched_storage_removal_state(&record)?;

    // 3. One batched trie pass against the parent → trie nodes + headers.
    //    Re-open the parent provider (the first was consumed by the executor).
    let state_provider = provider.state_by_block_hash(parent_hash)?;
    let mut witness =
        record.into_execution_witness(state_provider.as_ref(), provider, block_number, mode)?;
    let endpoint_state_nodes = witness.state.len();

    // 4. EEZ-specific augmentation: ask reth's trie witness builder for the
    //    removals-first witness of every touched storage slot. This triggers
    //    reth's existing blinded-sibling proof requests for branch collapse
    //    cases that are invisible in the final post-state witness.
    let extra_state_nodes = if removal_slots == 0 {
        0
    } else {
        let extra_state = state_provider.as_ref().witness(
            Default::default(),
            removal_state,
            ExecutionWitnessMode::Legacy,
        )?;
        extend_unique_by_hash(&mut witness.state, extra_state)
    };

    debug!(
        target: "eez::witness",
        block_number,
        state_nodes = witness.state.len(),
        endpoint_state_nodes,
        removal_accounts,
        removal_slots,
        extra_state_nodes,
        codes = witness.codes.len(),
        keys = witness.keys.len(),
        headers = witness.headers.len(),
        "block_execution_witness: generated augmented witness",
    );
    Ok(witness)
}

fn touched_storage_removal_state(
    record: &ExecutionWitnessRecord,
) -> eyre::Result<(HashedPostState, usize, usize)> {
    let mut state = HashedPostState::with_capacity(record.hashed_state.storages.len());
    let mut account_count = 0usize;
    let mut slot_count = 0usize;

    for (&hashed_address, storage) in &record.hashed_state.storages {
        if storage.storage.is_empty() {
            continue;
        }

        let account = record
            .hashed_state
            .accounts
            .get(&hashed_address)
            .copied()
            .ok_or_else(|| {
                eyre::eyre!(
                    "execution witness record has storage changes for {hashed_address} \
                     but no matching account entry"
                )
            })?;
        state.accounts.insert(hashed_address, account);
        account_count += 1;

        let mut removals = HashedStorage::default();
        for &hashed_slot in storage.storage.keys() {
            removals.storage.insert(hashed_slot, U256::ZERO);
            slot_count += 1;
        }
        state.storages.insert(hashed_address, removals);
    }

    Ok((state, account_count, slot_count))
}

fn extend_unique_by_hash(target: &mut Vec<Bytes>, extra: impl IntoIterator<Item = Bytes>) -> usize {
    let mut seen: HashSet<B256> = target.iter().map(keccak256).collect();
    let mut added = 0usize;

    for node in extra {
        if seen.insert(keccak256(&node)) {
            target.push(node);
            added += 1;
        }
    }

    added
}

#[cfg(test)]
mod tests {
    use super::extend_unique_by_hash;
    use alloy_primitives::Bytes;

    /// The removal-closure augmentation dedups by node hash: only genuinely new
    /// nodes are appended, and the reported count matches.
    #[test]
    fn extend_unique_by_hash_dedups() {
        let mut target = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let added = extend_unique_by_hash(
            &mut target,
            [
                Bytes::from_static(b"b"), // dup of existing
                Bytes::from_static(b"c"), // new
                Bytes::from_static(b"c"), // dup within extra
            ],
        );
        assert_eq!(added, 1, "only 'c' is new");
        assert_eq!(target.len(), 3);
        assert_eq!(target[2], Bytes::from_static(b"c"));
    }
}
