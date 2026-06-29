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
//! Uses `ExecutionWitnessMode::Canonical` → the minimal witness (sorted,
//! deduped, no empty nodes).
//!
//! Why re-execute instead of capturing during the build: the witness must
//! be **exact** (the prover requires a minimal witness), and only an
//! execution of the *final* tx list yields that. The Normal build path
//! (mempool fill) warms a superset; `newPayload` validation would be exact
//! but lives in reth's engine tree (a deep fork). Re-executing the
//! canonical block here is `debug_executionWitness`'s own approach and is
//! cheap for small L2 blocks.
//!
//! Lifted verbatim from based-rollup `reth-node/src/witness.rs` (the
//! in-process witness re-exec). The fold into a control plane + the call
//! site in the committer are the next steps (prover-chain P1).

use alloy_consensus::{BlockHeader, Header};
use alloy_rpc_types_debug::ExecutionWitness;
use reth_ethereum_primitives::{Block, EthPrimitives};
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_primitives_traits::RecoveredBlock;
use reth_revm::{database::StateProviderDatabase, witness::ExecutionWitnessRecord};
use reth_storage_api::{HeaderProvider, StateProviderFactory};
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

/// Re-execute `block` on its parent state and produce its exact, minimal
/// execution witness (trie nodes + contract codes + key preimages +
/// ancestor headers).
///
/// `provider` must expose the block's parent state. When the parent is an
/// in-memory (not-yet-persisted) block, the memory-overlay provider folds
/// the parent's `TrieInput` in internally — no manual `TrieInput` needed.
///
/// The block's txs are executed exactly as committed, so the recorded
/// access set is exact (never a superset).
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

    // 3. One batched trie pass against the parent → trie nodes + headers.
    //    Re-open the parent provider (the first was consumed by the executor).
    let state_provider = provider.state_by_block_hash(parent_hash)?;
    let witness =
        record.into_execution_witness(state_provider.as_ref(), provider, block_number, mode)?;

    debug!(
        target: "eez::witness",
        block_number,
        state_nodes = witness.state.len(),
        codes = witness.codes.len(),
        keys = witness.keys.len(),
        headers = witness.headers.len(),
        "block_execution_witness: generated minimal witness",
    );
    Ok(witness)
}
