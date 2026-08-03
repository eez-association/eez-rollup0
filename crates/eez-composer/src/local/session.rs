//! Stateful per-source-tx target-chain execution session.
//!
//! [`LocalExecutionSession`] accumulates target-chain state across
//! calls within one source transaction. It drives a direct call to the
//! destination contract with the proxy address as `msg.sender`
//! (CREATE2-derived).
//!
//! Also hosts the reth helpers
//! ([`disable_checks`], [`compute_state_root`]).

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};

use eez_evm_inspector::{OverlayChannelHandle, SessionInspectorFactory};
use reth_evm::{ConfigureEvm, Evm as _};
use reth_evm_ethereum::EthEvmConfig;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{BlockNumReader, StateProvider, StateProviderFactory};
use reth_trie_common::{HashedPostState, KeccakKeyHasher};
use revm::DatabaseCommit;
use revm::database::CacheState;

use eez_protocol::{
    CallMode, CompositionBuilder, ExecutionOutcome, ExecutionRequest, ExecutorError,
    ExecutorErrorKind, ExecutorResult, RollupId, TargetExecutionSession,
};

use super::provider::ChainProvider;

/// Canonical `CrossChainProxy` creation code, compiled from
/// `sync-rollups-protocol/src/base/CrossChainProxy.sol` with the settings in
/// `sync-rollups-protocol/foundry.toml` (solc 0.8.34, optimizer 200, via IR).
///
/// The constructor's ABI-encoded manager address is appended at runtime. The
/// fixed-vector checks in this module and `scripts/update-eezl2-genesis.sh`
/// deliberately fail if this bytecode drifts from the protocol contract.
const CROSS_CHAIN_PROXY_CREATION_CODE: &[u8] = &alloy_primitives::hex!(
    "60a080604052346101285761002a9061055a8038038091610020828561012c565b833981019061014f565b80608052478061005b575b6040516103eb908161016f82396080518181816101670152818161030901526103850152f35b6040516316ae1c2d60e21b815291602090839060049082906001600160a01b03165afa801561011d575f9283928392839283916100ee575b505af1503d156100e9573d6001600160401b0381116100d557604051906100c4601f8201601f19166020018361012c565b81525f60203d92013e5b5f80610035565b634e487b7160e01b5f52604160045260245ffd5b6100ce565b610110915060203d602011610116575b610108818361012c565b81019061014f565b5f610093565b503d6100fe565b6040513d5f823e3d90fd5b5f80fd5b601f909101601f19168101906001600160401b038211908210176100d557604052565b9081602091031261012857516001600160a01b0381168103610128579056fe608060405260043610610299575f3560e01c80638205f3e11461002b57639f149e1b03610299576100ab565b60603660031901126100a7576004356001600160a01b03811681036100a7576024359067ffffffffffffffff821682036100a7576044359167ffffffffffffffff83116100a757366023840112156100a75782600401359267ffffffffffffffff84116100a75736602485830101116100a7576024019161015f565b5f80fd5b346100a7575f3660031901126100a7573330036100c7575f805d005b610299565b908092918237015f815290565b634e487b7160e01b5f52604160045260245ffd5b90601f8019910116810190811067ffffffffffffffff82111761010f57604052565b6100d9565b67ffffffffffffffff811161010f57601f01601f191660200190565b3d1561015a573d9061014182610114565b9161014f60405193846100ed565b82523d5f602084013e565b606090565b9192909190337f00000000000000000000000000000000000000000000000000000000000000006001600160a01b0316036100c7575f93849367ffffffffffffffff16806101dc57506101b7604051809381936100cc565b039134905af16101c5610130565b905b156101d457602081519101f35b602081519101fd5b906101ec604051809481936100cc565b03923491f16101f9610130565b906101c7565b6001600160a01b0390911681526040602082018190528101829052606091805f848401375f828201840152601f01601f1916010190565b6020818303126100a75780519067ffffffffffffffff82116100a7570181601f820112156100a75780519061026a82610114565b9261027860405194856100ed565b828452602083830101116100a757815f9260208093018386015e8301015290565b5f806040516020810190639f149e1b60e01b8252600481526102bc6024826100ed565b519082306103e8f16102cc610130565b5061035a575f80604051602081019063189a256f60e11b8252610305816102f73633602484016101ff565b03601f1981018352826100ed565b51907f00000000000000000000000000000000000000000000000000000000000000005afa610332610130565b90805b61034657156101d457602081519101f35b90806020806101f993518301019101610236565b5f806040516020810190639af5325960e01b8252610380816102f73633602484016101ff565b5190347f00000000000000000000000000000000000000000000000000000000000000005af16103ae610130565b908061033556fea2646970667358221220902b4168f100d5946f93f0c89bec6fccc1768036fe918e981cd09f859fd2316964736f6c63430008220033"
);

fn proxy_init_code_hash(manager: Address) -> B256 {
    let mut init_code = Vec::with_capacity(CROSS_CHAIN_PROXY_CREATION_CODE.len() + 32);
    init_code.extend_from_slice(CROSS_CHAIN_PROXY_CREATION_CODE);
    init_code.extend_from_slice(&[0; 12]);
    init_code.extend_from_slice(manager.as_slice());
    keccak256(init_code)
}

fn compute_proxy_address(
    manager: Address,
    init_code_hash: B256,
    source_address: Address,
    source_rollup: RollupId,
) -> Address {
    let mut salt_preimage = [0; 28];
    salt_preimage[..8].copy_from_slice(&source_rollup.0.to_be_bytes());
    salt_preimage[8..].copy_from_slice(source_address.as_slice());
    manager.create2(keccak256(salt_preimage), init_code_hash)
}

/// Gas limit for direct target-chain calls during inspection. Generous
/// enough for worst-case dev-chain paths; too low → silent reverts.
pub(super) const DIRECT_CALL_GAS_LIMIT: u64 = 30_000_000;

/// Stateful target-chain execution session.
///
/// Opened by `LocalChainClient::begin_execution_session` (the
/// `ChainClient` trait method, not an inherent fn); owned by the
/// `CompositionBuilder` through a `Rollup.session` slot for the
/// lifetime of one composition.
///
/// ## Limitation: direct call, not full CCM path
///
/// The session calls the destination contract directly with the proxy
/// address as `msg.sender` (computed via CREATE2). This gives the
/// source simulation synchronous return data, but it does not reproduce
/// the full `executeIncomingCrossChainCall` path.
pub struct LocalExecutionSession {
    evm_config: EthEvmConfig,
    state: State<StateProviderDatabase<reth_storage_api::StateProviderBox>>,
    evm_env: reth_evm::EvmEnvFor<EthEvmConfig>,
    state_provider: reth_storage_api::StateProviderBox,
    current_root: B256,
    chain_id: u64,
    ccm_address: Address,
    proxy_init_code_hash: B256,
    /// When `Some`, `execute()` runs each target call under a
    /// [`eez_evm_inspector::SessionInspector`] built from this factory — forwarding
    /// detected proxy CALLs to the dispatcher. `None` means a plain
    /// direct execute with no inspection layer (used for the entry
    /// role's target-session path when the topology has no nested
    /// reentry into the entry chain). The factory holds this chain's
    /// `proxy_lookup` + `rollup_id`, so the inspector's `caller_id`
    /// matches the chain it runs on.
    inspector_factory: Option<SessionInspectorFactory>,
    /// Overlay channel write-back handle.
    /// `Some(channel)` when the entry rollup's session is opened on
    /// behalf of a nested-back-to-entry dispatch — this session writes
    /// its post-execute `state.cache.clone()` into `channel.overlay_cache`
    /// so source-sim's inspector can apply the diff onto source's
    /// journal. `None` for follower sessions and for entry sessions
    /// opened outside the overlay context.
    overlay_channel: Option<OverlayChannelHandle>,
}

impl std::fmt::Debug for LocalExecutionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutionSession")
            .field("current_root", &self.current_root)
            .field("chain_id", &self.chain_id)
            .field("ccm_address", &self.ccm_address)
            .field("has_inspector_factory", &self.inspector_factory.is_some())
            .finish_non_exhaustive()
    }
}

impl LocalExecutionSession {
    /// Create from a `ChainProvider`. Opens latest state.
    ///
    /// `inspector_factory` pins this session's target-side inspection
    /// policy. When supplied, each `execute` call runs under a
    /// [`eez_evm_inspector::SessionInspector`] so proxy CALLs
    /// detected during the target-chain execution dispatch back
    /// through the supplied `dispatcher` and are recorded into the
    /// composition's preorder `recorded[..]` slice. When `None`, the
    /// session executes without an inspector — used for the entry
    /// rollup's session in topologies where no inspector dispatches
    /// back to the entry chain.
    ///
    /// `cache` is the overlay-path preload: when an inner
    /// target-session frame dispatches back to the entry rollup, the
    /// entry session is lazily opened with a clone of source-sim's
    /// in-flight cache (via the
    /// [`eez_evm_inspector::OverlayChannel`] side-channel), so
    /// the nested L1 call observes source-sim's pending writes —
    /// load-bearing for fixtures that mutate L1 state inside a single
    /// source tx (e.g. `flash-loan`). `None` opens a fresh State.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorErrorKind::Provider`] if reading the latest
    /// block number or header fails, or if opening the state provider
    /// fails.
    pub fn new(
        provider: &ChainProvider,
        ccm_address: Address,
        inspector_factory: Option<SessionInspectorFactory>,
        cache: Option<CacheState>,
        overlay_channel: Option<OverlayChannelHandle>,
    ) -> ExecutorResult<Self> {
        let num = provider
            .provider
            .best_block_number()
            .map_err(provider_err)?;
        tracing::debug!(block = num, "target session: best block number");

        let header = provider
            .headers
            .header_by_number(num)
            .map_err(provider_err)?
            .ok_or_else(|| ExecutorError::from(ExecutorErrorKind::Missing("target header")))?;
        let current_root = header.state_root;
        tracing::debug!(
            block = num,
            state_root = %current_root,
            "target session: opened header"
        );

        let state_prov = provider.provider.latest().map_err(provider_err)?;
        let root_prov = provider.provider.latest().map_err(provider_err)?;

        let mut evm_env = provider.evm_config.evm_env(&header).map_err(evm_err)?;
        let chain_id = evm_env.cfg_env.chain_id;
        disable_checks(&mut evm_env);

        let db = StateProviderDatabase::new(state_prov);
        let mut builder = State::builder().with_database(db).with_bundle_update();
        if let Some(cache) = cache {
            builder = builder.with_cached_prestate(cache);
        }
        let state = builder.build();

        tracing::debug!(
            block = num,
            chain_id,
            %ccm_address,
            %current_root,
            "target session: ready"
        );

        Ok(Self {
            evm_config: provider.evm_config.clone(),
            state,
            evm_env,
            state_provider: root_prov,
            current_root,
            chain_id,
            ccm_address,
            proxy_init_code_hash: proxy_init_code_hash(ccm_address),
            inspector_factory,
            overlay_channel,
        })
    }

    /// Uninspected direct-call path: a plain `evm.transact` plus
    /// invariant-7 bookkeeping. Used when this session was opened with
    /// `inspector_factory = None` (entry rollup).
    fn execute_internal(
        &mut self,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        let tx_env = self.build_tx_env(destination, calldata, value, source_address, source_rollup);
        let caller = tx_env.caller;
        let pre_root = self.current_root;
        let (return_data, gas_used, success, mut changes) = {
            let mut evm = self
                .evm_config
                .evm_with_env(&mut self.state, self.evm_env.clone());
            let result = evm.transact(tx_env).map_err(evm_err)?;
            (
                result.result.output().cloned().unwrap_or_default(),
                result.result.tx_gas_used(),
                result.result.is_success(),
                result.state,
            )
        };
        restore_caller_nonce(&mut changes, caller);
        self.commit_and_finish(pre_root, return_data, gas_used, success, changes)
    }

    /// Inspected direct-call path. Runs the target-chain tx under the
    /// supplied [`eez_evm_inspector::SessionInspector`] so proxy CALLs detected during
    /// execution dispatch back through the underlying `Dispatcher`.
    ///
    /// The session takes the inspector by value (non-`'static`
    /// borrow) — it can't outlive this function call anyway, and the
    /// reth `evm_with_env_and_inspector` API wants ownership. Dispatch
    /// errors are surfaced via the inspector's `take_error` path on
    /// drop; we promote them into `Err` before returning the outcome.
    ///
    /// # Pitfall #3 guard
    ///
    /// The target-side inspector fires from inside a scoped OS thread
    /// whose tokio entry is `Handle::block_on(...)`. This function
    /// body MUST NOT introduce a real `.await` — doing so would try
    /// to park back onto the outer runtime while the scoped thread
    /// is blocking a worker, producing a starvation deadlock. See
    /// the amendment C15 regression test.
    fn execute_internal_with_inspector(
        &mut self,
        inspector: eez_evm_inspector::SessionInspector<'_>,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        let tx_env = self.build_tx_env(destination, calldata, value, source_address, source_rollup);
        let caller = tx_env.caller;
        let pre_root = self.current_root;
        let (return_data, gas_used, success, mut changes, inspector_error) = {
            let mut evm = self.evm_config.evm_with_env_and_inspector(
                &mut self.state,
                self.evm_env.clone(),
                inspector,
            );
            let result = evm.transact(tx_env).map_err(evm_err)?;
            let inspector_error = evm.inspector_mut().take_error();
            (
                result.result.output().cloned().unwrap_or_default(),
                result.result.tx_gas_used(),
                result.result.is_success(),
                result.state,
                inspector_error,
            )
        };
        if let Some(err) = inspector_error {
            return Err(err);
        }
        restore_caller_nonce(&mut changes, caller);
        self.commit_and_finish(pre_root, return_data, gas_used, success, changes)
    }

    /// Shared post-commit bookkeeping: commit the revm bundle, compute
    /// the post-state root, advance `current_root`, log, and emit an
    /// [`ExecutionOutcome`](eez_protocol::ExecutionOutcome).
    fn commit_and_finish(
        &mut self,
        pre_root: B256,
        return_data: Bytes,
        gas_used: u64,
        success: bool,
        changes: revm::primitives::map::AddressHashMap<revm::state::Account>,
    ) -> ExecutorResult<eez_protocol::ExecutionOutcome> {
        self.state.commit(changes);

        let post_root = compute_state_root(&mut self.state, self.state_provider.as_ref())?;
        self.current_root = post_root;

        // Overlay write-back: when this session is the entry rollup's
        // overlay session (channel handle installed at construction),
        // publish the post-execute cache for the source-sim inspector
        // to diff-apply onto source's journal after `block_in_place`
        // returns. The cache here is the cumulative state after every
        // overlay call so far, since multiple overlay executes within
        // one source-sim hook reuse the same session.
        //
        // Also append `post_root` to the channel's per-tx roots —
        // nested entries attributed to the entry rollup pull
        // `newState` from this list during `build_batch`. Order is
        // chronological dispatch order, matching the cursor
        // `build_batch` walks for the entry rollup.
        //
        // Stack-based push: the inspector at the dispatching frame
        // pops the top after `block_in_place` returns and applies the
        // diff. Stack semantics let nested re-entries chain
        // post-caches through their respective inspector pops without
        // collision.
        if let Some(channel) = &self.overlay_channel {
            channel.push_post_cache(self.state.cache.clone());
            channel.append_post_root(post_root.0);
        }

        tracing::debug!(
            success = success,
            gas = gas_used,
            pre = %pre_root,
            post = %post_root,
            return_len = return_data.len(),
            "target call completed");

        Ok(eez_protocol::ExecutionOutcome::Resolved {
            return_data: return_data.to_vec(),
            pre_state_root: pre_root.0,
            post_state_root: post_root.0,
            gas_used,
            success,
        })
    }

    /// Build the `TxEnv` for a direct call on the target chain. Shared
    /// between inspected and uninspected paths so the revm input shape
    /// is identical regardless of inspection policy.
    fn build_tx_env(
        &self,
        destination: &Address,
        calldata: &Bytes,
        value: &U256,
        source_address: &Address,
        source_rollup: RollupId,
    ) -> revm::context::TxEnv {
        let caller = compute_proxy_address(
            self.ccm_address,
            self.proxy_init_code_hash,
            *source_address,
            source_rollup,
        );

        tracing::trace!(
            dest = %destination,
            proxy_caller = %caller,
            source_addr = %source_address,
            %source_rollup,
            value = %value,
            calldata_len = calldata.len(),
            "executing direct call on target chain");

        revm::context::TxEnv {
            caller,
            gas_limit: DIRECT_CALL_GAS_LIMIT,
            kind: alloy_primitives::TxKind::Call(*destination),
            data: calldata.clone(),
            value: *value,
            chain_id: Some(self.chain_id),
            ..Default::default()
        }
    }
}

impl TargetExecutionSession for LocalExecutionSession {
    fn execute(
        &mut self,
        req: ExecutionRequest,
        dispatcher: &mut CompositionBuilder,
    ) -> ExecutorResult<ExecutionOutcome> {
        if req.call_mode == CallMode::Static {
            return Err(ExecutorErrorKind::Unavailable(
                "static target execution is not implemented".to_owned(),
            )
            .into());
        }
        // Pitfall #3 invariant: this method runs SYNCHRONOUSLY under
        // the caller's `Handle::block_on`. Do NOT introduce a real
        // `` on I/O here or in `execute_internal*` — the target-
        // side inspector dispatches from a scoped OS thread that holds
        // a tokio worker, and a real await would park the outer
        // runtime.
        let outcome = if let Some(factory) = self.inspector_factory.clone() {
            let inspector = factory.build(dispatcher);
            self.execute_internal_with_inspector(
                inspector,
                &req.target_address,
                &req.data,
                &req.value,
                &req.source_address,
                req.source_rollup_id,
            )?
        } else {
            let _ = dispatcher;
            self.execute_internal(
                &req.target_address,
                &req.data,
                &req.value,
                &req.source_address,
                req.source_rollup_id,
            )?
        };

        Ok(outcome)
    }

    fn checkpoint(&mut self) -> ExecutorResult<eez_protocol::SessionSnapshot> {
        // Opaque snapshot carrying the current root only. The full
        // revm `State<DB>` deep-clone remains known implementation debt.
        Ok(Box::new(self.current_root.0) as Box<dyn std::any::Any + Send>)
    }

    fn rollback(&mut self, snapshot: eez_protocol::SessionSnapshot) -> ExecutorResult<()> {
        let root: [u8; 32] = *snapshot.downcast::<[u8; 32]>().map_err(|_e| {
            ExecutorError::from(ExecutorErrorKind::Encoding(
                "LocalExecutionSession::rollback: snapshot type mismatch".into(),
            ))
        })?;
        self.current_root = revm::primitives::B256::from(root);
        Ok(())
    }
}

/// Restore the caller's nonce in `changes` to its pre-tx (`original_info`)
/// value.
///
/// The session calls each cross-chain target with
/// `msg.sender = compute_proxy_address(source_address, source_rollup)` —
/// a deterministic CREATE2 slot that the protocol later deploys via
/// `createCrossChainProxy`. revm's tx execution increments the caller's
/// nonce by 1 (real-chain semantics for EOA senders), but in our session
/// the caller is a CONTRACT slot, not an EOA. Bumping its nonce makes
/// the address fail EIP-684's "code or nonce non-zero" collision check
/// when a downstream `Bridge.bridgeTokens` later tries to CREATE2 a real
/// `CrossChainProxy` at the same slot — burning ~28M gas and reverting
/// the whole session with empty data.
///
/// Restoring `info.nonce` to `original_info.nonce` (the disk value
/// before the tx) keeps the slot fresh for the deterministic CREATE2.
/// The real chain's flow handles this via
/// `CCM.{_processCallAtScope,executeIncomingCrossChainCall}` which
/// CREATE2-deploys the proxy BEFORE forwarding the call; our direct-
/// call session shortcut needs the equivalent invariant restored
/// post-hoc.
fn restore_caller_nonce(
    changes: &mut revm::primitives::map::AddressHashMap<revm::state::Account>,
    caller: Address,
) {
    if let Some(account) = changes.get_mut(&caller) {
        account.info.nonce = account.original_info.nonce;
    }
}

pub(super) fn disable_checks(env: &mut reth_evm::EvmEnvFor<EthEvmConfig>) {
    env.cfg_env.disable_base_fee = true;
    env.cfg_env.disable_balance_check = true;
    env.cfg_env.disable_nonce_check = true;
    env.cfg_env.disable_eip3607 = true;
    env.cfg_env.disable_block_gas_limit = true;
    env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);
}

pub(super) fn compute_state_root<DB: StateProvider>(
    state: &mut State<StateProviderDatabase<DB>>,
    provider: &dyn StateProvider,
) -> ExecutorResult<B256> {
    state.merge_transitions(revm::database::states::bundle_state::BundleRetention::Reverts);
    let hashed = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.bundle_state.state());
    provider.state_root(hashed).map_err(provider_err)
}

pub(super) fn provider_err(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> ExecutorError {
    ExecutorError::provider(e)
}

pub(super) fn evm_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ExecutorError {
    ExecutorError::evm(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_create2_formula_matches_canonical_fixed_vector() {
        let manager = alloy_primitives::address!("4200000000000000000000000000000000000007");
        let original = alloy_primitives::address!("11223344556677889900aabbccddeeff00112233");
        let init_code_hash = proxy_init_code_hash(manager);

        assert_eq!(CROSS_CHAIN_PROXY_CREATION_CODE.len(), 1_370);
        assert_eq!(
            keccak256(CROSS_CHAIN_PROXY_CREATION_CODE),
            alloy_primitives::b256!(
                "f4bc7a948c6ada02e821e1800f18aa19c418a22155897ec758332847505618c6"
            )
        );
        assert_eq!(
            init_code_hash,
            alloy_primitives::b256!(
                "7c63e527554027dc06e5f9aebc6a153ec5e54dbf588ac2238a2776bf3c9fdc31"
            )
        );
        assert_eq!(
            compute_proxy_address(manager, init_code_hash, original, RollupId(0)),
            alloy_primitives::address!("0093d413a54e6e751f882d69785d90fb5d0646a4")
        );
    }
}
