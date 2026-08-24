//! Build a Sync block from cross-chain system transactions.
//!
//! Manual block-construction path — bypasses reth's payload builder
//! because Sync blocks carry only cross-chain system txs (Rollup-1 §5),
//! never pool txs. Uses the same reth-evm `BlockBuilder` machinery the
//! Deriver uses to replay L1-derived blocks
//! (`eez_deriver::Deriver::execute_block`).
//!
//! [`SyncBlockState`] exposes the same construction as a LIVE, incrementally
//! extended state: the Sync block under construction is itself the composition
//! session, so a claim read off it is the value the committed block produces.

use alloy_consensus::{BlockHeader, Header, Transaction};
use alloy_eips::Decodable2718;
use alloy_primitives::{Address, B256, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use eez_driver::{BUILDER_EXTRA_DATA, BUILDER_GAS_LIMIT};
use reth_chainspec::EthereumHardforks;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_ethereum_primitives::{Block, TransactionSigned};
use reth_evm::{
    ConfigureEvm, Evm as _, EvmEnvFor, InspectorFor, NextBlockEnvAttributes, execute::BlockBuilder,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{
    Recovered, RecoveredBlock, SealedHeader, SignedTransaction, SignerRecoverable,
};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::{StateProviderBox, StateProviderFactory};
use revm::database::{CacheState, State};
use revm::inspector::NoOpInspector;
use revm::{
    DatabaseCommit,
    context::result::{EVMError, ExecutionResult},
};
use std::sync::Arc;
use thiserror::Error;

/// The revm state a Sync block is built over: the parent state provider plus
/// every change committed by the block's txs so far.
pub type DraftDb = State<StateProviderDatabase<StateProviderBox>>;

/// Errors raised by [`build_sync_block`].
#[derive(Debug, Error)]
pub enum BuildError {
    /// Provider lookup (state, header) failed.
    #[error("provider: {0}")]
    Provider(String),
    /// EIP-2718 decode failed for the held tx at `idx`.
    #[error("decode tx #{idx}: {msg}")]
    DecodeTx { idx: usize, msg: String },
    /// Signer recovery failed for the held tx at `idx`.
    #[error("recover signer for tx #{idx}")]
    RecoverSigner { idx: usize },
    /// Revm rejected the tx at execution time (gas, balance, …).
    #[error("execute tx #{idx}: {msg}")]
    ExecuteTx { idx: usize, msg: String },
    /// `reth-evm` block-builder primitive surfaced an error during
    /// setup, pre-execution changes, or `finish`.
    #[error("block builder: {0}")]
    Builder(String),
}

impl BuildError {
    /// Whether this is a backing-store failure (reth MDBX read) rather than
    /// anything the transaction did — transient, so a caller must retry rather
    /// than blame the tx.
    #[must_use]
    pub const fn is_provider(&self) -> bool {
        matches!(self, Self::Provider(_))
    }
}

/// Built Sync-block artifact ready for [`commit_derived`].
///
/// [`commit_derived`]: eez_driver::BlockCommitterHandle::commit_derived
#[derive(Debug)]
pub struct BuiltSyncBlock {
    /// Engine-API payload — pass to `commit_derived`.
    pub payload: ExecutionData,
    /// Sealed header of the new block (cursor mirror in the committer).
    pub header: SealedHeader<Header>,
    /// The recovered Sync block. Remote-prover mode captures the endpoint witness
    /// from here, since the block isn't committed yet (no store can serve it).
    pub block: RecoveredBlock<Block>,
    /// Receipt-level success per tx, in block order. The composer gates a
    /// dispatch on every system tx and outbound user tx having succeeded — a
    /// reverted delivery means the block's claims contradict its execution.
    pub tx_successes: Vec<bool>,
}

/// The next-block attributes every Sync-block execution path shares.
///
/// `prev_randao` is L1-derived (protocol §13.14); left zero here to match the
/// Deriver's path. Hardfork-gated fields (parent_beacon_block_root,
/// withdrawals) and `extra_data` / `gas_limit` mirror the Deriver's
/// `execute_block` attrs so all three STF paths (sequencer pool, deriver
/// replay, sync-slot composer) produce identical headers — otherwise
/// `check_claimed_state` diverges where Cancun/Shanghai isn't active at
/// genesis.
fn next_block_attributes(
    evm_config: &EthEvmConfig,
    timestamp: u64,
    suggested_fee_recipient: Address,
) -> NextBlockEnvAttributes {
    let chain_spec = evm_config.chain_spec();
    NextBlockEnvAttributes {
        timestamp,
        suggested_fee_recipient,
        prev_randao: B256::ZERO,
        gas_limit: BUILDER_GAS_LIMIT,
        parent_beacon_block_root: chain_spec
            .is_cancun_active_at_timestamp(timestamp)
            .then_some(B256::ZERO),
        withdrawals: chain_spec
            .is_shanghai_active_at_timestamp(timestamp)
            .then(alloy_eips::eip4895::Withdrawals::default),
        extra_data: Bytes::from_static(BUILDER_EXTRA_DATA),
        slot_number: None,
    }
}

/// Decode a raw EIP-2718 tx and recover its signer. `idx` is the tx's position
/// in the block, for error reporting.
fn recover_tx(raw: &Bytes, idx: usize) -> Result<Recovered<TransactionSigned>, BuildError> {
    let tx =
        TransactionSigned::decode_2718(&mut raw.as_ref()).map_err(|e| BuildError::DecodeTx {
            idx,
            msg: e.to_string(),
        })?;
    SignedTransaction::try_into_recovered(tx).map_err(|_| BuildError::RecoverSigner { idx })
}

/// Open the revm state for a block built on `parent_hash`, optionally
/// preloaded with an already-warmed cache.
fn open_draft_db(
    provider: &dyn StateProviderFactory,
    parent_hash: B256,
    cache: Option<CacheState>,
) -> Result<DraftDb, BuildError> {
    let state_provider = provider
        .state_by_block_hash(parent_hash)
        .map_err(|e| BuildError::Provider(format!("state_by_block_hash({parent_hash}): {e}")))?;
    let mut builder = State::builder()
        .with_database(StateProviderDatabase::new(state_provider))
        .with_bundle_update();
    if let Some(cache) = cache {
        builder = builder.with_cached_prestate(cache);
    }
    Ok(builder.build())
}

/// Build a Sync block on top of `parent`, executing `sync_txs` in order. The
/// list is mixed: outbound pairs interleave a system load with its user tx.
///
/// The returned [`BuiltSyncBlock`] is committed via
/// [`BlockCommitterHandle::commit_derived`] — same engine-API tail
/// the Deriver uses for L1-derived blocks (single source of truth for
/// `newPayload` + head-FCU).
///
/// # Errors
///
/// See [`BuildError`].
///
/// [`BlockCommitterHandle::commit_derived`]: eez_driver::BlockCommitterHandle::commit_derived
pub fn build_sync_block<P>(
    l2_provider: &P,
    evm_config: &EthEvmConfig,
    parent: &SealedHeader<Header>,
    timestamp: u64,
    suggested_fee_recipient: Address,
    sync_txs: &[Bytes],
) -> Result<BuiltSyncBlock, BuildError>
where
    P: StateProviderFactory,
{
    let parent_hash = parent.hash();
    let state_provider = l2_provider
        .state_by_block_hash(parent_hash)
        .map_err(|e| BuildError::Provider(format!("state_by_block_hash({parent_hash}): {e}")))?;
    let state_db = StateProviderDatabase::new(state_provider.as_ref());
    let mut db = State::builder()
        .with_database(state_db)
        .with_bundle_update()
        .build();

    let attributes = next_block_attributes(evm_config, timestamp, suggested_fee_recipient);

    let mut builder = evm_config
        .builder_for_next_block(&mut db, parent, attributes)
        .map_err(|e| BuildError::Builder(format!("builder_for_next_block: {e}")))?;

    builder
        .apply_pre_execution_changes()
        .map_err(|e| BuildError::Builder(format!("apply_pre_execution_changes: {e}")))?;

    for (idx, tx_bytes) in sync_txs.iter().enumerate() {
        builder
            .execute_transaction(recover_tx(tx_bytes, idx)?)
            .map_err(|e| BuildError::ExecuteTx {
                idx,
                msg: e.to_string(),
            })?;
    }

    let outcome = builder
        .finish(state_provider.as_ref(), None)
        .map_err(|e| BuildError::Builder(format!("finish: {e}")))?;
    let tx_successes = outcome
        .execution_result
        .receipts
        .iter()
        .map(|receipt| receipt.success)
        .collect();
    let block = outcome.block;
    let sealed_block = block.sealed_block().clone();
    let header = sealed_block.sealed_header().clone();
    let payload = <EthEngineTypes as PayloadTypes>::block_to_payload(sealed_block, None);
    Ok(BuiltSyncBlock {
        payload,
        header,
        block,
        tx_successes,
    })
}

/// Outcome of one transaction executed on a live prefix state.
///
/// A reverted tx is a successful *execution* with `success == false` — only a
/// tx revm refuses outright (nonce, balance, decode) is a [`BuildError`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxOutcome {
    /// Receipt-level status: false for a revert or a halt.
    pub success: bool,
    /// Gas charged to the tx (after refunds), as the receipt reports it.
    pub gas_used: u64,
    /// Return data if `success`, revert data otherwise; empty if the tx halted
    /// without output.
    pub output: Bytes,
}

fn exec_outcome<H>(result: &ExecutionResult<H>) -> TxOutcome {
    TxOutcome {
        success: result.is_success(),
        gas_used: result.tx_gas_used(),
        output: result.output().cloned().unwrap_or_default(),
    }
}

/// Execute one raw tx against `state` under `evm_env` and commit it — the
/// per-tx half of `BlockBuilder::execute_transaction` (`alloy_evm`'s
/// `EthBlockExecutor` transacts and commits the same way; the rest of its work
/// is receipts and block-gas accounting, which carry no state).
///
/// Cumulative block gas is therefore NOT tracked here; the final
/// [`build_sync_block`] over the accumulated tx list is what enforces it.
///
/// The uninspected path passes [`NoOpInspector`], the same inspector reth uses
/// when a caller supplies none.
fn execute_and_commit_inspected<I>(
    evm_config: &EthEvmConfig,
    state: &mut DraftDb,
    evm_env: &EvmEnvFor<EthEvmConfig>,
    raw: &Bytes,
    idx: usize,
    inspector: I,
) -> Result<TxOutcome, BuildError>
where
    I: for<'db> InspectorFor<EthEvmConfig, &'db mut DraftDb>,
{
    let recovered = recover_tx(raw, idx)?;
    let mut evm = evm_config.evm_with_env_and_inspector(state, evm_env.clone(), inspector);
    // A database read failure is the store being unavailable, not the tx being
    // invalid: the two classes drive opposite recoveries in the drain.
    let result = evm.transact(&recovered).map_err(|e| match e {
        EVMError::Database(err) => BuildError::Provider(format!("execute tx #{idx}: {err}")),
        other => BuildError::ExecuteTx {
            idx,
            msg: other.to_string(),
        },
    })?;
    let outcome = exec_outcome(&result.result);
    evm.db_mut().commit(result.state);
    Ok(outcome)
}

/// Live, incrementally extended execution state of the Sync block under
/// construction: the parent state, the block's env, and every tx appended so
/// far.
///
/// Same parent, same [`next_block_attributes`], same pre-execution changes and
/// same per-tx execution as [`build_sync_block`], so "L2 state at tx k" here is
/// the state the committed block has at tx k. Post-execution changes
/// (withdrawals, balance increments) are NOT applied — those belong to block
/// close, not to mid-block state.
pub struct SyncBlockState {
    provider: Arc<dyn StateProviderFactory>,
    evm_config: EthEvmConfig,
    parent_hash: B256,
    evm_env: EvmEnvFor<EthEvmConfig>,
    state: DraftDb,
    /// Number of txs applied — the next tx's block position.
    applied: usize,
}

impl std::fmt::Debug for SyncBlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncBlockState")
            .field("parent_hash", &self.parent_hash)
            .field("applied", &self.applied)
            .finish_non_exhaustive()
    }
}

impl SyncBlockState {
    /// Open the state of a Sync block on `parent` whose first `prefix_txs` have
    /// executed.
    ///
    /// # Errors
    ///
    /// See [`BuildError`].
    pub fn open(
        provider: Arc<dyn StateProviderFactory>,
        evm_config: &EthEvmConfig,
        parent: &SealedHeader<Header>,
        timestamp: u64,
        suggested_fee_recipient: Address,
        prefix_txs: &[Bytes],
    ) -> Result<Self, BuildError> {
        let parent_hash = parent.hash();
        let mut state = open_draft_db(provider.as_ref(), parent_hash, None)?;
        let attributes = next_block_attributes(evm_config, timestamp, suggested_fee_recipient);
        let evm_env = evm_config
            .next_evm_env(parent, &attributes)
            .map_err(|e| BuildError::Builder(format!("next_evm_env: {e}")))?;

        // Drive the prefix through the real block builder, then drop it WITHOUT
        // `finish`: that applies the pre-execution changes exactly once and
        // leaves `state` at the mid-block point appended txs continue from.
        {
            let mut builder = evm_config
                .builder_for_next_block(&mut state, parent, attributes)
                .map_err(|e| BuildError::Builder(format!("builder_for_next_block: {e}")))?;
            builder
                .apply_pre_execution_changes()
                .map_err(|e| BuildError::Builder(format!("apply_pre_execution_changes: {e}")))?;
            for (idx, raw) in prefix_txs.iter().enumerate() {
                builder
                    .execute_transaction(recover_tx(raw, idx)?)
                    .map_err(|e| BuildError::ExecuteTx {
                        idx,
                        msg: e.to_string(),
                    })?;
            }
        }

        Ok(Self {
            provider,
            evm_config: evm_config.clone(),
            parent_hash,
            evm_env,
            state,
            applied: prefix_txs.len(),
        })
    }

    /// Execute one raw EIP-2718 tx with full real-STF semantics and commit it,
    /// extending the prefix by one.
    ///
    /// # Errors
    ///
    /// See [`BuildError`].
    pub fn execute_tx(&mut self, raw: &Bytes) -> Result<TxOutcome, BuildError> {
        let outcome = execute_and_commit_inspected(
            &self.evm_config,
            &mut self.state,
            &self.evm_env,
            raw,
            self.applied,
            NoOpInspector,
        )?;
        self.applied += 1;
        Ok(outcome)
    }

    /// Open a throwaway fork of this state: a fresh revm state over the same
    /// parent state provider, preloaded with a clone of this state's cache and
    /// running the same env. Nothing executed on the fork touches the block.
    ///
    /// # Errors
    ///
    /// See [`BuildError`].
    pub fn fork(&mut self) -> Result<SyncBlockFork, BuildError> {
        let state = open_draft_db(
            self.provider.as_ref(),
            self.parent_hash,
            Some(self.state.cache.clone()),
        )?;
        Ok(SyncBlockFork {
            evm_config: self.evm_config.clone(),
            evm_env: self.evm_env.clone(),
            state,
            applied: self.applied,
        })
    }
}

/// Restore point of a [`SyncBlockFork`]. Forks never merge transitions, so the
/// cache carries every state effect a rollback must undo; `applied` rides along
/// so a rewound fork also reports the right tx position.
#[derive(Debug)]
pub struct ForkSnapshot {
    cache: CacheState,
    applied: usize,
}

/// A throwaway copy of a [`SyncBlockState`] for simulation: probes and source
/// sims run here, and only their accepted effects are appended to the real
/// block.
pub struct SyncBlockFork {
    evm_config: EthEvmConfig,
    evm_env: EvmEnvFor<EthEvmConfig>,
    state: DraftDb,
    applied: usize,
}

impl std::fmt::Debug for SyncBlockFork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncBlockFork")
            .field("applied", &self.applied)
            .finish_non_exhaustive()
    }
}

impl SyncBlockFork {
    /// Execute one raw EIP-2718 tx on the fork and commit it there.
    ///
    /// # Errors
    ///
    /// See [`BuildError`].
    pub fn execute_tx(&mut self, raw: &Bytes) -> Result<TxOutcome, BuildError> {
        let outcome = execute_and_commit_inspected(
            &self.evm_config,
            &mut self.state,
            &self.evm_env,
            raw,
            self.applied,
            NoOpInspector,
        )?;
        self.applied += 1;
        Ok(outcome)
    }

    /// [`Self::execute_tx`] under an inspector — the probe path, where the
    /// inspector captures the inner frame's outcome as the claim.
    ///
    /// # Errors
    ///
    /// See [`BuildError`].
    pub fn execute_tx_inspected<I>(
        &mut self,
        raw: &Bytes,
        inspector: I,
    ) -> Result<TxOutcome, BuildError>
    where
        I: for<'db> InspectorFor<EthEvmConfig, &'db mut DraftDb>,
    {
        let outcome = execute_and_commit_inspected(
            &self.evm_config,
            &mut self.state,
            &self.evm_env,
            raw,
            self.applied,
            inspector,
        )?;
        self.applied += 1;
        Ok(outcome)
    }

    /// Restore point for composition rollback.
    pub fn snapshot(&self) -> ForkSnapshot {
        ForkSnapshot {
            cache: self.state.cache.clone(),
            applied: self.applied,
        }
    }

    /// Roll back to a [`Self::snapshot`].
    pub fn restore(&mut self, snapshot: ForkSnapshot) {
        self.state.cache = snapshot.cache;
        self.applied = snapshot.applied;
    }

    /// Raw state + env, for callers that drive their own EVM (a source
    /// simulation threading its inspector through this fork).
    pub fn state_and_env(&mut self) -> (&mut DraftDb, &EvmEnvFor<EthEvmConfig>) {
        (&mut self.state, &self.evm_env)
    }
}

/// Per-effect intermediate L2 state roots — the root after each cross-chain
/// effect's tx group (its pair-end), in tx order, one per effect.
///
/// The prover requires each settlement entry's `newState` to equal its effect's
/// root (not the final Sync-block root), so the composer fills them with these.
/// Computed by rebuilding the Sync block on each pair-end prefix of `sync_txs`;
/// since our L2 blocks are state no-ops past the effects, a prefix block's root
/// equals the full block's root at that tx. Settlement path only.
///
/// # Errors
///
/// See [`BuildError`]. A sync tx that fails to decode is treated as a non-system
/// tx (pair-end), matching the prover's fail-safe flagging.
pub fn sync_block_pair_roots<P>(
    l2_provider: &P,
    evm_config: &EthEvmConfig,
    parent: &SealedHeader<Header>,
    timestamp: u64,
    suggested_fee_recipient: Address,
    sync_txs: &[Bytes],
    system_address: Address,
    eezl2_address: Address,
) -> Result<Vec<B256>, BuildError>
where
    P: StateProviderFactory,
{
    // Per-tx system flags must match the proof signer's classification so
    // pair-end positions agree on both sides.
    let flags: Vec<bool> = sync_txs
        .iter()
        .map(|raw| {
            let Ok(tx) = TransactionSigned::decode_2718(&mut raw.as_ref()) else {
                return false;
            };
            let to = tx.to();
            tx.recover_signer().is_ok_and(|signer| {
                eez_protocol::settlement::is_system_tx(signer, to, system_address, eezl2_address)
            })
        })
        .collect();

    eez_protocol::settlement::pair_end_positions(&flags)
        .into_iter()
        .map(|p| {
            build_sync_block(
                l2_provider,
                evm_config,
                parent,
                timestamp,
                suggested_fee_recipient,
                &sync_txs[..=p],
            )
            .map(|prefix| prefix.header.state_root())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::TxLegacy;
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{TxKind, U256, address, bytes};
    use alloy_signer_local::PrivateKeySigner;
    use reth_chainspec::ChainSpecBuilder;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};

    const TIMESTAMP: u64 = 1_012;
    const FEE_RECIPIENT: Address = address!("0000000000000000000000000000000000000f00");
    const BOB: Address = address!("000000000000000000000000000000000000b0b0");
    const STORE: Address = address!("00000000000000000000000000000000005107e0");
    const REVERTER: Address = address!("000000000000000000000000000000000000bad0");

    /// Everything the two execution paths need: one provider, one env, one
    /// tx list.
    struct Fixture {
        provider: MockEthProvider,
        evm_config: EthEvmConfig,
        parent: SealedHeader<Header>,
        txs: Vec<Bytes>,
    }

    fn test_signer() -> PrivateKeySigner {
        PrivateKeySigner::from_bytes(&B256::with_last_byte(7)).expect("test key")
    }

    fn sign_tx(nonce: u64, to: Address, value: U256, gas_limit: u64) -> Bytes {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 1,
            gas_limit,
            to: TxKind::Call(to),
            value,
            input: Bytes::new(),
        };
        let sig = test_signer()
            .sign_transaction_sync(&mut tx)
            .expect("sign test tx");
        let signed =
            TransactionSigned::new_unhashed(reth_ethereum_primitives::Transaction::Legacy(tx), sig);
        let mut buf = Vec::new();
        signed.encode_2718(&mut buf);
        Bytes::from(buf)
    }

    fn fixture() -> Fixture {
        let evm_config = EthEvmConfig::new(Arc::new(
            ChainSpecBuilder::mainnet().cancun_activated().build(),
        ));

        let provider: MockEthProvider = MockEthProvider::new();
        provider.add_account(
            test_signer().address(),
            ExtendedAccount::new(0, U256::from(1_000_000_000_000_000_000_u128)),
        );
        // `SSTORE 1 -> slot 0`, then return the 32-byte word 42: the write makes
        // a repeat call cheap, so gas alone shows whether the prefix chained.
        provider.add_account(
            STORE,
            ExtendedAccount::new(0, U256::ZERO)
                .with_bytecode(bytes!("6001600055602a60005260206000f3")),
        );
        // Revert with `0xdeadbeef`.
        provider.add_account(
            REVERTER,
            ExtendedAccount::new(0, U256::ZERO).with_bytecode(bytes!("63deadbeef6000526004601cfd")),
        );

        let parent = SealedHeader::seal_slow(Header {
            number: 1,
            timestamp: 1_000,
            gas_limit: BUILDER_GAS_LIMIT,
            base_fee_per_gas: Some(0),
            excess_blob_gas: Some(0),
            blob_gas_used: Some(0),
            ..Default::default()
        });

        let txs = vec![
            sign_tx(0, BOB, U256::from(1), 21_000),
            sign_tx(1, STORE, U256::ZERO, 100_000),
            sign_tx(2, REVERTER, U256::ZERO, 100_000),
            sign_tx(3, STORE, U256::ZERO, 100_000),
            sign_tx(4, BOB, U256::from(1), 21_000),
        ];

        Fixture {
            provider,
            evm_config,
            parent,
            txs,
        }
    }

    impl Fixture {
        fn build(&self, txs: &[Bytes]) -> BuiltSyncBlock {
            build_sync_block(
                &self.provider,
                &self.evm_config,
                &self.parent,
                TIMESTAMP,
                FEE_RECIPIENT,
                txs,
            )
            .expect("build sync block")
        }

        fn open(&self, prefix: &[Bytes]) -> SyncBlockState {
            SyncBlockState::open(
                Arc::new(self.provider.clone()),
                &self.evm_config,
                &self.parent,
                TIMESTAMP,
                FEE_RECIPIENT,
                prefix,
            )
            .expect("open prefix state")
        }

        /// Gas charged to each tx INSIDE `build_sync_block`, read off the header
        /// of every tx prefix — the arbiter the block itself agrees with.
        fn builder_gas(&self) -> Vec<u64> {
            let cumulative: Vec<u64> = (0..=self.txs.len())
                .map(|k| self.build(&self.txs[..k]).header.gas_used())
                .collect();
            cumulative.windows(2).map(|w| w[1] - w[0]).collect()
        }
    }

    /// THE equivalence guard: appending txs to a live [`SyncBlockState`]
    /// executes them exactly as `build_sync_block`'s block builder does. Gas is
    /// the witness — it is state-dependent (the second `SSTORE` is ~20k cheaper
    /// only if the first one's write is visible), so equal per-tx gas plus
    /// equal receipt status means both paths ran the same STF over the same
    /// intermediate states.
    #[test]
    fn prefix_state_execution_matches_build_sync_block() {
        let f = fixture();
        let full = f.build(&f.txs);
        assert_eq!(
            full.tx_successes,
            vec![true, true, false, true, true],
            "receipt statuses, including the reverting tx",
        );
        let builder_gas = f.builder_gas();
        // Ground truth, so the comparisons below can't pass on empty numbers.
        assert_eq!(builder_gas[0], 21_000, "plain transfer");

        let mut prefix = f.open(&[]);
        let outcomes: Vec<TxOutcome> = f
            .txs
            .iter()
            .map(|raw| prefix.execute_tx(raw).expect("execute on prefix"))
            .collect();

        for (idx, outcome) in outcomes.iter().enumerate() {
            assert_eq!(
                outcome.success, full.tx_successes[idx],
                "tx #{idx} status disagrees with the built block",
            );
            assert_eq!(
                outcome.gas_used, builder_gas[idx],
                "tx #{idx} gas disagrees with the built block",
            );
        }

        // A revert is an outcome, not an error, and carries its data.
        assert_eq!(outcomes[2].output, bytes!("deadbeef"));
        assert_eq!(U256::from_be_slice(&outcomes[1].output), U256::from(42));
        // Non-vacuous: the repeat SSTORE is cheap ONLY because the first one is
        // visible, so this comparison fails if `open` restarts from the parent.
        assert!(
            outcomes[1].gas_used > outcomes[3].gas_used + 15_000,
            "prefix must chain: {} vs {}",
            outcomes[1].gas_used,
            outcomes[3].gas_used,
        );
    }

    /// `open(prefix)` lands on the state the block builder holds at that tx:
    /// executing tx k over prefix `[0..k]` reproduces tx k's in-block outcome
    /// for every k — builder-executed prefix, `transact`-executed tail.
    #[test]
    fn prefix_open_matches_the_same_position_in_the_block() {
        let f = fixture();
        let full = f.build(&f.txs);
        let builder_gas = f.builder_gas();

        for (k, raw) in f.txs.iter().enumerate() {
            let outcome = f.open(&f.txs[..k]).execute_tx(raw).expect("execute tx k");
            assert_eq!(outcome.success, full.tx_successes[k], "tx #{k} status");
            assert_eq!(outcome.gas_used, builder_gas[k], "tx #{k} gas");
        }
    }

    /// A fork sees the prefix but never writes back to it, and `restore` is a
    /// real restore point — for the cache and for the tx position.
    #[test]
    fn fork_is_isolated_and_snapshot_restore_rewinds() {
        let f = fixture();
        let builder_gas = f.builder_gas();
        let mut prefix = f.open(&f.txs[..1]);

        let mut fork = prefix.fork().expect("fork");
        let on_fork = fork.execute_tx(&f.txs[1]).expect("execute on fork");
        assert_eq!(on_fork.gas_used, builder_gas[1]);

        // The fork's write never reached the block: the same tx still applies
        // to the prefix, with the same result.
        let on_block = prefix.execute_tx(&f.txs[1]).expect("execute on prefix");
        assert_eq!(on_block, on_fork, "fork must not advance the block");

        let snapshot = fork.snapshot();
        let applied_before = fork.applied;
        let first = fork.execute_tx(&f.txs[2]).expect("probe run");
        fork.restore(snapshot);
        assert_eq!(
            fork.applied, applied_before,
            "restore must rewind the tx position with the cache",
        );
        let second = fork.execute_tx(&f.txs[2]).expect("replay after restore");
        assert_eq!(first, second, "restore must rewind the probe's effects");
        assert!(!first.success);

        // Non-vacuous: without the rewind the nonce is spent and revm rejects
        // the replay outright.
        assert!(
            fork.execute_tx(&f.txs[2]).is_err(),
            "a spent nonce must be rejected — the restore above is what makes \
             the replay legal",
        );
    }
}
