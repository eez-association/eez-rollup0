//! Simulation state shared by every composer execution path.
//!
//! [`SimState`] is a state provider plus every read and accepted write so far.
//! Each transaction runs on a fresh EVM whose accepted overlay is this state's
//! cache, so it sees every earlier accepted write; the caller decides whether
//! its own writes are kept.

use std::mem;

use alloy_consensus::{TxLegacy, transaction::Recovered};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use eez_evm::{EezEvmConfig, EezEvmTx, EezEvmTypes};
use evm2::{
    EvmFeatures, ExecutedTx, Inspector, PendingState, TxResultWithState,
    ethereum::TxEnvelope,
    evm::{Cache, CacheDB, Db},
    registry::HandlerResult,
};
use reth_evm::{BlockExecutorFactory, ConfigureEvm, EvmEnvFor, database::StateProviderDatabase};
use reth_storage_api::{EvmStateProviderAdapter, StateProvider, StateProviderBox};

type ProviderDb = Db<StateProviderDatabase<EvmStateProviderAdapter<StateProviderBox>>>;

/// A state under simulation: a state provider plus every read and accepted
/// write.
pub struct SimState {
    db: CacheDB<ProviderDb>,
}

impl std::fmt::Debug for SimState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimState")
            .field("accounts", &self.db.cache.accounts.len())
            .finish_non_exhaustive()
    }
}

impl SimState {
    /// Opens `provider`'s state with `cache` already accepted over it.
    pub(crate) fn new(provider: StateProviderBox, cache: Cache) -> Self {
        let mut db = CacheDB::new(Db::new(StateProviderDatabase::new(
            provider.into_evm_state_provider(),
        )));
        db.cache = cache;
        Self { db }
    }

    /// Every read and accepted write so far.
    pub(crate) const fn cache(&self) -> &Cache {
        &self.db.cache
    }

    /// Replaces the accepted state, rolling back to a [`Self::cache`] clone.
    pub(crate) fn set_cache(&mut self, cache: Cache) {
        self.db.cache = cache;
    }

    /// Takes the accepted state.
    pub(crate) fn into_cache(self) -> Cache {
        self.db.cache
    }

    /// Accepts a cache built over this state, such as a block builder's.
    pub(crate) fn merge(&mut self, cache: Cache) {
        self.db.cache.merge(cache);
    }

    /// The database a block builder runs over to start this state.
    pub(crate) const fn database_mut(&mut self) -> &mut CacheDB<ProviderDb> {
        &mut self.db
    }

    /// Runs `tx` under `env`. Its writes stay detached until [`Self::commit`].
    pub(crate) fn transact(
        &mut self,
        evm_config: &EezEvmConfig,
        env: &EvmEnvFor<EezEvmConfig>,
        tx: &Recovered<EezEvmTx>,
        inspector: Option<&mut dyn Inspector<EezEvmTypes>>,
    ) -> HandlerResult<TxResultWithState<EezEvmTypes>> {
        // Without logs the composer cannot check the calls a tx made.
        debug_assert!(
            env.version.features.contains(EvmFeatures::LOG_COLLECTION),
            "simulation must collect logs"
        );
        let mut evm = evm_config
            .block_executor_factory()
            .evm_with_env(&mut self.db.db, env.clone());
        mem::swap(&mut evm.overlay_db_mut().cache, &mut self.db.cache);
        let result = match inspector {
            Some(inspector) => evm.transact_with_inspector(tx, inspector),
            None => evm.transact(tx),
        }
        .map(ExecutedTx::detach);
        mem::swap(&mut evm.overlay_db_mut().cache, &mut self.db.cache);
        result
    }

    /// Accepts a transaction's writes.
    pub(crate) fn commit(&mut self, pending: &PendingState) {
        self.db.commit_pending(pending);
    }
}

/// An unsigned, fee-free call from `caller`, for frames the protocol runs on a
/// sender's behalf.
pub(crate) fn synthetic_call(
    chain_id: u64,
    caller: Address,
    to: Address,
    input: Bytes,
    value: U256,
    gas_limit: u64,
) -> Recovered<EezEvmTx> {
    let tx = TxLegacy {
        chain_id: Some(chain_id),
        gas_limit,
        to: TxKind::Call(to),
        value,
        input,
        ..Default::default()
    };
    Recovered::new_unchecked(EezEvmTx::Ethereum(TxEnvelope::Legacy(tx)), caller)
}

/// Relaxes `env` for synthetic calls: no fee market, no sender nonce, balance
/// or code checks, and no gas caps.
pub(crate) fn disable_checks(env: &mut EvmEnvFor<EezEvmConfig>) {
    env.version.features.remove(
        EvmFeatures::BASE_FEE_CHECK
            | EvmFeatures::BALANCE_CHECK
            | EvmFeatures::NONCE_CHECK
            | EvmFeatures::EIP3607
            | EvmFeatures::BLOCK_GAS_LIMIT_CHECK,
    );
    disable_tx_gas_caps(env);
}

/// Lifts the per-transaction gas caps.
pub(crate) fn disable_tx_gas_caps(env: &mut EvmEnvFor<EezEvmConfig>) {
    env.version.tx_gas_limit_cap = u64::MAX;
    env.version.tx_total_gas_cap = u64::MAX;
}

/// Restores `caller`'s nonce in `pending` to its pre-transaction value.
///
/// Synthetic calls are sent by contracts or placeholder accounts: the chain's
/// manager, the `Address::ZERO` proxy-creation caller, or a CREATE2 proxy slot
/// nobody has deployed yet. The EVM bumps a sender's nonce; on a contract that
/// nonce only governs CREATE, and every proxy is CREATE2, so keeping the bump
/// drifts the simulation from the chain. On an undeployed proxy slot it also
/// fails EIP-684's "code or nonce non-zero" check when the protocol later
/// deploys the real `CrossChainProxy` there.
pub(crate) fn reset_frame_caller_nonce(pending: &mut PendingState, caller: Address) {
    let Some(changes) = pending.account_changes_for(&caller) else {
        return;
    };
    let original = changes.original.cloned();
    let Some(mut current) = changes.current.cloned() else {
        return;
    };
    current.nonce = original.as_ref().map_or(0, |account| account.nonce);
    // EIP-161: without the bump, an account the call created is empty, so it
    // stays absent.
    let current = (original.is_some() || !current.is_empty()).then_some(current);
    pending.insert_account(caller, original, current);
}
