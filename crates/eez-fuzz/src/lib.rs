//! Fuzz harness for `eez_protocol::compose_transaction`.
//!
//! Strategy: boot the cross-chain world ONCE in
//! revm, freeze it into in-process `MockEthProvider`s, build the production
//! `LocalChainClient`s over them, then fire many `raw_tx`s at the frozen world
//! (`compose_transaction` is read-only against the providers, so the boot cost
//! amortises to zero across a fuzz campaign).
//!
//! The reusable harness lives here as a library so both the integration tests
//! (`tests/compose_e2e.rs`) and the `cargo-fuzz` target (`fuzz/`) share one
//! World boot + generator + execute/ratify oracle.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use alloy_consensus::{Header, TxEip1559, TypedTransaction};
use alloy_eips::Encodable2718;
use alloy_network::{Ethereum, EthereumWallet, NetworkWallet};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, address, hex};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolValue, sol};
use eez_composer::LocalChainClient;
use eez_evm::{ChainDialect, EvmProtocol};
use eez_protocol::{
    ChainClient, Composition, CompositionResult, DEFAULT_CCM_GAS_LIMIT, EntryChainClient,
    ProxyLookupConfig, Rollup, RollupId, TargetConfig, compose_transaction,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_storage_api::StateProviderFactory;
use revm::context::ContextTr;
use revm::context::TxEnv;
use revm::context::result::{ExecutionResult, Output};
use revm::database::{CacheDB, EmptyDB};
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

mod generator;
pub use generator::*;

mod assertions;
pub use assertions::*;

mod sequence;
pub use sequence::*;

mod replay;
pub use replay::*;
sol! {
    function registerRollup(address rollupContract, bytes32 initialState) external returns (uint256);
    function createCrossChainProxy(address originalAddress, uint256 originalRollupId) external returns (address);
    function computeCrossChainProxyAddress(address originalAddress, uint256 originalRollupId) external view returns (address);
    function proxy() external view returns (address);
    function setViaProxy(uint256 v) external;
}

/// L2 rollup id the cross-chain proxy targets (registered as rollup #1).
pub const L2_ROLLUP_ID: u64 = 1;
/// Deterministic L2 address `Value` will live at (deployer's first CREATE).
pub const VALUE_L2_ADDR: Address = address!("0x1111111111111111111111111111111111111111");
/// Proof-system authorized signer (also seeds the rollup vkey).
pub const SIGNER: Address = address!("0x00000000000000000000000000000000000000a1");
/// EEZL2 system address (authorized to load execution tables on L2).
pub const SYSTEM_ADDR: Address = address!("0x00000000000000000000000000000000000000a2");
/// Deployer EOA used to send the world's creation txs.
pub const DEPLOYER: Address = address!("0x00000000000000000000000000000000000000d0");

/// Run a sequence of deployer-signed txs against a fresh funded `CacheDB`,
/// returning the frozen provider. `build` issues txs via the `send` closure
/// (CREATE or CALL) and returns whatever addresses the caller wants to keep.
pub fn boot<R>(
    build: impl FnOnce(&mut dyn FnMut(TxKind, Vec<u8>) -> ExecutionResult) -> R,
) -> (MockEthProvider, R) {
    let mut cache = CacheDB::<EmptyDB>::default();
    cache.insert_account_info(
        DEPLOYER,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );
    let mut evm = Context::mainnet().with_db(cache).build_mainnet();
    let mut nonce = 0u64;
    let mut send = |kind: TxKind, data: Vec<u8>| -> ExecutionResult {
        let n = nonce;
        nonce += 1;
        evm.transact_commit(TxEnv {
            caller: DEPLOYER,
            kind,
            data: data.into(),
            gas_limit: 16_000_000,
            nonce: n,
            chain_id: Some(1),
            ..Default::default()
        })
        .expect("tx execution")
    };
    let out = build(&mut send);
    (freeze(evm.db()), out)
}

/// Assert the frozen provider serves non-empty code at each address — the
/// fixture's structural invariant, checked on every boot (so the revm-deploy →
/// snapshot bridge is validated everywhere, with no standalone "world boots" test).
fn assert_world_has_code(provider: &MockEthProvider, addrs: &[Address]) {
    let state = provider.latest().expect("latest state");
    for a in addrs {
        assert!(
            state
                .account_code(a)
                .expect("account_code")
                .is_some_and(|c| !c.is_empty()),
            "fixture invariant: frozen world must serve code at {a}",
        );
    }
}

/// L2 follower world: `Value` (deployed first → deterministic address) + EEZL2.
pub fn boot_l2_world() -> (MockEthProvider, Address, Address) {
    let (provider, (value, eezl2)) = boot(|send| {
        // Value FIRST so its address is the deployer's nonce-0 CREATE — stable
        // and known before we build the L1 proxy that targets it.
        let value = created(send(
            TxKind::Create,
            init(
                "contracts/out/Value.sol/Value.json",
                (U256::ZERO,).abi_encode_params(),
            ),
        ));
        let eezl2 = created(send(
            TxKind::Create,
            init(
                "sync-rollups-protocol/out/EEZL2.sol/EEZL2.json",
                (U256::from(L2_ROLLUP_ID), SYSTEM_ADDR).abi_encode_params(),
            ),
        ));
        (value, eezl2)
    });
    assert_world_has_code(&provider, &[value, eezl2]);
    (provider, value, eezl2)
}

/// Depth-2 L2 world: EEZL2 + `innerProxy`(InnerValue@L1, rollup 0) +
/// `NestedValue`(innerProxy). Returns `(provider, nested_value_addr, eezl2)`.
/// Calling `NestedValue.setValue` fires a nested cross-chain call to
/// `innerProxy`, so composing through it records dispatch at depth > 1.
pub fn boot_l2_world_nested(inner_value_l1: Address) -> (MockEthProvider, Address, Address) {
    let (provider, (nested, eezl2)) = boot(|send| {
        // EEZL2 manager — must exist before we register a proxy on it.
        let eezl2 = created(send(
            TxKind::Create,
            init(
                "sync-rollups-protocol/out/EEZL2.sol/EEZL2.json",
                (U256::from(L2_ROLLUP_ID), SYSTEM_ADDR).abi_encode_params(),
            ),
        ));
        // innerProxy = CrossChainProxy(InnerValue@L1, rollup 0) on EEZL2 — a
        // CROSS-rollup proxy (L2→L1). Same-rollup reentry is rejected by the
        // composer (`InvalidReentry`), so the nested target lives on the entry
        // rollup; the overlay inspector detects this proxy during L2 sim.
        let inner_proxy = Address::abi_decode(&call_output(send(
            TxKind::Call(eezl2),
            createCrossChainProxyCall {
                originalAddress: inner_value_l1,
                originalRollupId: U256::ZERO,
            }
            .abi_encode(),
        )))
        .expect("decode innerProxy");
        // NestedValue wraps innerProxy — its setValue fires the depth-2 call.
        let nested = created(send(
            TxKind::Create,
            init(
                "contracts/out/NestedValue.sol/NestedValue.json",
                (inner_proxy,).abi_encode_params(),
            ),
        ));
        (nested, eezl2)
    });
    (provider, nested, eezl2)
}

/// Load a contract's creation bytecode from a forge artifact JSON.
pub fn creation_bytecode(artifact_rel: &str) -> Bytes {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(artifact_rel);
    let json: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&path).expect("open artifact"))
            .expect("parse artifact");
    let obj = json["bytecode"]["object"]
        .as_str()
        .expect("bytecode.object");
    Bytes::from(hex::decode(obj.trim_start_matches("0x")).expect("decode bytecode"))
}

/// Snapshot a revm `CacheDB`'s accounts (code + storage) into an in-process
/// `MockEthProvider` that serves them through `StateProviderFactory::latest()`.
pub fn freeze(db: &CacheDB<EmptyDB>) -> MockEthProvider {
    let provider = MockEthProvider::new();
    for (addr, acc) in &db.cache.accounts {
        let mut ext = ExtendedAccount::new(acc.info.nonce, acc.info.balance);
        if let Some(code) = &acc.info.code {
            ext = ext.with_bytecode(code.original_bytes());
        }
        ext = ext.extend_storage(acc.storage.iter().map(|(k, v)| (B256::from(*k), *v)));
        provider.add_account(*addr, ext);
    }
    // Head header so `best_block_number` / `header_by_number(latest)` resolve
    // (what `LocalChainClient::simulate_source_tx` reads). `MockEthProvider`'s
    // `.latest()` serves the accounts map regardless of `state_root`.
    let header = Header {
        number: 0,
        gas_limit: 30_000_000,
        timestamp: 1,
        base_fee_per_gas: Some(0),
        // Cancun is active in the DEV chainspec → blob fields must be set.
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        ..Default::default()
    };
    provider.add_header(header.hash_slow(), header);
    provider
}

/// Append ABI-encoded constructor args to a contract's creation bytecode.
pub fn init(artifact_rel: &str, encoded_args: Vec<u8>) -> Vec<u8> {
    let mut v = creation_bytecode(artifact_rel).to_vec();
    v.extend(encoded_args);
    v
}

/// Extract the created contract address from a CREATE `ExecutionResult`.
pub fn created(r: ExecutionResult) -> Address {
    match r {
        ExecutionResult::Success {
            output: Output::Create(_, Some(a)),
            ..
        } => a,
        other => panic!("expected CREATE success, got: {other:?}"),
    }
}

/// Extract the return bytes from a CALL `ExecutionResult`.
pub fn call_output(r: ExecutionResult) -> Bytes {
    match r {
        ExecutionResult::Success {
            output: Output::Call(b),
            ..
        } => b,
        other => panic!("expected CALL success, got: {other:?}"),
    }
}

/// L1 entry world: EEZ + MockPS + Rollup + registerRollup(L2) +
/// createCrossChainProxy(value_l2, L2) + SetterWrapper. Returns the frozen
/// provider and `(eez, proxy, setter)`.
pub fn boot_l1_world(value_l2: Address) -> (MockEthProvider, Address, Address, Address) {
    let (provider, (eez, proxy, setter)) = boot(|send| build_l1_world(send, value_l2));
    assert_world_has_code(&provider, &[eez, setter]);
    assert_ne!(proxy, Address::ZERO, "fixture invariant: proxy registered");
    (provider, eez, proxy, setter)
}

/// L1 entry world for depth-2: deploys `InnerValue` (a `Value`) as the FIRST
/// tx — so its address is the deterministic `DEPLOYER.create(0)` that the L2
/// `innerProxy` targets — then the normal entry world pointing the L1 proxy
/// at `nested_value`.
pub fn boot_l1_world_nested(nested_value: Address) -> (MockEthProvider, Address, Address, Address) {
    let (provider, (eez, proxy, setter)) = boot(|send| {
        let _inner = created(send(
            TxKind::Create,
            init(
                "contracts/out/Value.sol/Value.json",
                (U256::ZERO,).abi_encode_params(),
            ),
        ));
        build_l1_world(send, nested_value)
    });
    (provider, eez, proxy, setter)
}

/// Deploy the entry-world contracts via `send` and register a cross-chain
/// proxy for `value_l2`. Returns `(eez, proxy, setter)`.
fn build_l1_world(
    send: &mut dyn FnMut(TxKind, Vec<u8>) -> ExecutionResult,
    value_l2: Address,
) -> (Address, Address, Address) {
    // 1. EEZ registry (no constructor).
    let eez = created(send(
        TxKind::Create,
        init("contracts/out/EEZ.sol/EEZ.json", Vec::new()),
    ));

    // 2. MockECDSAProofSystem(authorizedSigner).
    let mock_ps = created(send(
        TxKind::Create,
        init(
            "contracts/out/MockECDSAProofSystem.sol/MockECDSAProofSystem.json",
            (SIGNER,).abi_encode_params(),
        ),
    ));

    // 3. Rollup(rollupsRegistry, owner, threshold, proofSystems[], vkeys[]).
    let vkey = {
        let mut b = [0u8; 32];
        b[12..].copy_from_slice(SIGNER.as_slice());
        B256::from(b)
    };
    let rollup_mgr = created(send(
        TxKind::Create,
        init(
            "contracts/out/Rollup.sol/Rollup.json",
            (eez, DEPLOYER, U256::from(1u8), vec![mock_ps], vec![vkey]).abi_encode_params(),
        ),
    ));

    // 4. EEZ.registerRollup(rollupMgr, initialState) -> rollupId (== 1).
    let rid = U256::abi_decode(&call_output(send(
        TxKind::Call(eez),
        registerRollupCall {
            rollupContract: rollup_mgr,
            initialState: B256::repeat_byte(0xab),
        }
        .abi_encode(),
    )))
    .expect("decode rollupId");
    assert_eq!(rid, U256::from(L2_ROLLUP_ID), "first registered rollup id");

    // 5. EEZ.createCrossChainProxy(value_l2, l2RollupId) -> proxy address.
    let proxy = Address::abi_decode(&call_output(send(
        TxKind::Call(eez),
        createCrossChainProxyCall {
            originalAddress: value_l2,
            originalRollupId: U256::from(L2_ROLLUP_ID),
        }
        .abi_encode(),
    )))
    .expect("decode proxy address");

    // 6. SetterWrapper(proxy) — the L1 caller that reaches the proxy at depth>1.
    let setter = created(send(
        TxKind::Create,
        init(
            "contracts/out/SetterWrapper.sol/SetterWrapper.json",
            (proxy,).abi_encode_params(),
        ),
    ));

    (eez, proxy, setter)
}

/// Build a `TargetConfig<EvmProtocol>` mirroring `eez-node` main.rs wiring.
fn target_cfg(ccm: Address, system: Address, dialect: ChainDialect) -> TargetConfig<EvmProtocol> {
    TargetConfig {
        ccm_address: ccm,
        system_address: system,
        ccm_gas_limit: DEFAULT_CCM_GAS_LIMIT,
        proxy_lookup: ProxyLookupConfig {
            contract_address: ccm,
            authorized_proxies_slot: dialect.proxy_lookup_slot(),
        },
        dialect,
        // In-process follower targets settle by replaying the CCM-verify txs,
        // not by trusting a remote session root.
        settles_via_session_root: false,
    }
}

/// Sign a `SetterWrapper.setViaProxy(v)` source tx (EIP-1559, zero-fee so no
/// balance is required; source-sim disables the nonce check).
pub async fn sign_setter_call(setter: Address, v: u64, chain_id: u64) -> Vec<u8> {
    let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("signer");
    let tx = TxEip1559 {
        chain_id,
        nonce: 0,
        gas_limit: 5_000_000,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(setter),
        value: U256::ZERO,
        access_list: Default::default(),
        input: setViaProxyCall { v: U256::from(v) }.abi_encode().into(),
    };
    let wallet = EthereumWallet::from(signer);
    let env = NetworkWallet::<Ethereum>::sign_transaction(&wallet, TypedTransaction::Eip1559(tx))
        .await
        .expect("sign source tx");
    env.encoded_2718()
}

/// Concrete `LocalChainClient` over the in-process test provider.
pub type Lcc = LocalChainClient<MockEthProvider, EthEvmConfig>;

/// The booted cross-chain world: production clients over frozen providers,
/// plus the trigger-contract dictionary. Boot ONCE; `compose` is read-only
/// against the providers, so a campaign reuses the same `World`.
pub struct World {
    pub entry: Arc<Lcc>,
    pub follower: Arc<Lcc>,
    pub eez: Address,
    pub eezl2: Address,
    /// Dictionary of trigger contracts whose calls fire a cross-chain
    /// dispatch (today: the single `SetterWrapper`), each with its callable
    /// ABI. The fuzz generator draws from here — the "restrict the address
    /// space" requirement (see the `compose` fuzz target).
    pub triggers: Vec<Trigger>,
    pub chain_id: u64,
    /// Frozen L2 state, retained so the execute+ratify oracle can replay the
    /// composition's L2 target payloads against the real bytecode.
    pub l2_provider: MockEthProvider,
    /// The ultimate cross-chain target contract on L2 (`Value`) whose slot-0
    /// `value` the settle path mutates — the SETTLED-state oracle reads it
    /// back after replaying the inbound system tx. Checking storage (not the
    /// composer's claimed return data) is what catches "the cross-chain call
    /// never really ran" past a mock prover.
    pub value_l2: Address,
    /// Which registered rollup is the composition *entry* (where the source tx
    /// is simulated). `RollupId(0)` (L1) for the implemented L1→L2 direction;
    /// `RollupId(L2_ROLLUP_ID)` for the L2→L1 direction built by
    /// [`World::boot_l2_entry`]. `compose` passes this to `compose_transaction`
    /// and `rollups` hands the entry client to whichever id matches.
    pub entry_id: RollupId,
}

impl World {
    /// Single-hop world: L1 `SetterWrapper` → proxy(`Value`@L2). One
    /// cross-chain dispatch (depth 1).
    pub fn boot() -> Self {
        let (l2_provider, value, eezl2) = boot_l2_world();
        let (l1_provider, eez, _proxy, setter) = boot_l1_world(value);
        Self::assemble(l1_provider, l2_provider, eez, eezl2, setter, value)
    }

    /// Depth-2 world: L1 `SetterWrapper` → proxy(`NestedValue`@L2), and
    /// `NestedValue.setValue` itself calls a cross-rollup proxy(`InnerValue`@L1)
    /// — so composing fires cross-chain dispatch at depth > 1, exercising the
    /// LIFO overlay push/pop pairing.
    pub fn boot_nested() -> Self {
        // InnerValue is the first CREATE in the L1 boot, so its address is the
        // deterministic `DEPLOYER.create(0)`; the L2 innerProxy targets it.
        let inner_value_l1 = DEPLOYER.create(0);
        let (l2_provider, nested_value, eezl2) = boot_l2_world_nested(inner_value_l1);
        let (l1_provider, eez, _proxy, setter) = boot_l1_world_nested(nested_value);
        // Settle target for the nested path is InnerValue@L1; the depth-2 test
        // is ignored, so pass nested_value as a placeholder.
        Self::assemble(l1_provider, l2_provider, eez, eezl2, setter, nested_value)
    }

    /// Build the production clients over frozen providers (shared by the
    /// single-hop and nested boots).
    fn assemble(
        l1_provider: MockEthProvider,
        l2_provider: MockEthProvider,
        eez: Address,
        eezl2: Address,
        setter: Address,
        settle_target: Address,
    ) -> Self {
        let chain_spec = reth_chainspec::DEV.clone();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let entry = LocalChainClient::new_entry(
            l1_provider,
            evm_config.clone(),
            chain_spec.clone(),
            RollupId(0),
            eez,
            eez,
            ChainDialect::EvmL1Style,
        );
        let follower = LocalChainClient::new_follower(
            l2_provider.clone(),
            evm_config,
            chain_spec,
            RollupId(L2_ROLLUP_ID),
            eezl2,
            eezl2,
            ChainDialect::EvmL2Style,
        );
        World {
            entry,
            follower,
            eez,
            eezl2,
            l2_provider,
            value_l2: settle_target,
            triggers: vec![Trigger {
                address: setter,
                calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
            }],
            chain_id: reth_chainspec::DEV.chain.id(),
            entry_id: RollupId(0),
        }
    }

    /// **L2→L1 entry world** — the direction the composer does *not* implement.
    ///
    /// Same contracts as [`boot_nested`](World::boot_nested), but the roles are
    /// reversed: the **L2** chain is the entry (a user tx originates there) and
    /// **L1** is the follower target. The L2 trigger is `NestedValue`, whose
    /// `setValue(v)` calls `innerProxy` — an L2-side `CrossChainProxy` for
    /// `InnerValue`@L1 (rollup 0). So a source tx to `NestedValue.setValue` is
    /// exactly "an L2 tx that calls a proxy of an L1 contract".
    ///
    /// The composer's `finalize` skips the entry rollup and CCM-verifies every
    /// follower; here the follower is an `EvmL1Style` chain, which has no
    /// `loadExecutionTable` / `executeIncomingCrossChainCall` system-tx path —
    /// so this boot drives the harness straight onto the unimplemented path.
    /// `value_l2` is repurposed as the L1 settle target (`InnerValue`@L1).
    pub fn boot_l2_entry() -> Self {
        // InnerValue is the first CREATE in the L1 boot → deterministic
        // `DEPLOYER.create(0)`, which the L2 `innerProxy` targets.
        let inner_value_l1 = DEPLOYER.create(0);
        let (l2_provider, nested_value, eezl2) = boot_l2_world_nested(inner_value_l1);
        let (l1_provider, eez, _proxy, _setter) = boot_l1_world_nested(nested_value);

        let chain_spec = reth_chainspec::DEV.clone();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        // Entry = L2 (EvmL2Style): source sim runs here, reads
        // `EEZL2.authorizedProxies` and detects `innerProxy`.
        let entry = LocalChainClient::new_entry(
            l2_provider.clone(),
            evm_config.clone(),
            chain_spec.clone(),
            RollupId(L2_ROLLUP_ID),
            eezl2,
            eezl2,
            ChainDialect::EvmL2Style,
        );
        // Follower = L1 (EvmL1Style): the dispatch target (rollup 0).
        let follower = LocalChainClient::new_follower(
            l1_provider,
            evm_config,
            chain_spec,
            RollupId(0),
            eez,
            eez,
            ChainDialect::EvmL1Style,
        );
        World {
            entry,
            follower,
            eez,
            eezl2,
            l2_provider,
            value_l2: inner_value_l1,
            triggers: vec![Trigger {
                address: nested_value,
                calls: vec![CallSpec::from_sig("setValue(uint256)", 1)],
            }],
            chain_id: reth_chainspec::DEV.chain.id(),
            entry_id: RollupId(L2_ROLLUP_ID),
        }
    }

    /// The runtime dictionary the fuzz generator draws from.
    pub fn dict(&self) -> Dict {
        Dict {
            chain_id: self.chain_id,
            triggers: self.triggers.clone(),
            keys: vec![PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("key")],
        }
    }

    /// Rebuild the per-call rollups map (cheap: `Arc` clones + a 2-entry map).
    ///
    /// The per-rollup *config* is fixed by id (rollup 0 = L1/`EvmL1Style`,
    /// rollup 1 = L2/`EvmL2Style`); only the *client* depends on direction —
    /// the `EntryChainClient` (`self.entry`) is slotted under `self.entry_id`
    /// and the follower client under the other id. For the L1→L2 boot this
    /// reproduces the original map exactly.
    pub fn rollups(&self) -> HashMap<RollupId, Rollup<EvmProtocol>> {
        let entry_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> =
            self.entry.clone();
        let follower_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> =
            self.follower.clone();
        let client_for = |id: RollupId| {
            if id == self.entry_id {
                entry_cc.clone()
            } else {
                follower_cc.clone()
            }
        };
        let mut m = HashMap::new();
        m.insert(
            RollupId(0),
            Rollup {
                client: client_for(RollupId(0)),
                session: None,
                config: target_cfg(self.eez, Address::ZERO, ChainDialect::EvmL1Style),
                initial_state_root: [0u8; 32],
            },
        );
        m.insert(
            RollupId(L2_ROLLUP_ID),
            Rollup {
                client: client_for(RollupId(L2_ROLLUP_ID)),
                session: None,
                config: target_cfg(self.eezl2, SYSTEM_ADDR, ChainDialect::EvmL2Style),
                initial_state_root: [0u8; 32],
            },
        );
        m
    }

    /// Drive the function under test against the frozen world, entering on
    /// `self.entry_id` (L1 for the implemented direction, L2 for the
    /// unimplemented L2→L1 direction).
    pub async fn compose(&self, raw_tx: &[u8]) -> CompositionResult<Composition<EvmProtocol>> {
        let entry_ec: Arc<dyn EntryChainClient<Protocol = EvmProtocol> + Send + Sync> =
            self.entry.clone();
        compose_transaction(
            &EvmProtocol,
            entry_ec.as_ref(),
            raw_tx,
            self.entry_id,
            self.rollups(),
        )
        .await
    }
}

#[cfg(test)]
mod direction_tests {
    use super::*;

    /// One synthetic `FuzzTx`, both directions resolved against the matching
    /// world dict. Mirrors what the `compose` fuzz target does per input.
    fn tx(direction: Direction) -> FuzzTx {
        FuzzTx {
            direction,
            trigger_sel: 0,
            method_sel: 0,
            signer_sel: 0,
            nonce: 0,
            value: 0,
            args: [7, 0, 0, 0],
        }
    }

    /// Baseline: the implemented L1→L2 direction composes AND its L2 inbound
    /// ratifies + settles the destination — proves the harness/oracle work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn l1_to_l2_composes_and_settles() {
        let world = World::boot();
        let (raw, expected) = tx(Direction::L1ToL2).resolve_and_sign(&world.dict());
        let comp = world.compose(&raw).await.expect("L1→L2 must compose");
        world.assert_executes_and_ratifies(&comp, Some(expected));
    }

    /// The direction under review (an L2 tx that calls a proxy of an L1
    /// contract). Locks the CURRENT reality: the composer has no L2-as-entry
    /// settling path, so dispatching the L2→L1 call drives the `EvmL1Style`
    /// follower onto a path it can't honor and `compose` returns `Err`
    /// (`target transaction 0 reverted`). This is the "ignored / reverts in L2"
    /// case from review, now reproduced deterministically. The day the composer
    /// grows real L2→L1 support, this test flips and tells us so.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn l2_to_l1_is_rejected_today() {
        let world = World::boot_l2_entry();
        let (raw, _expected) = tx(Direction::L2ToL1).resolve_and_sign(&world.dict());
        let res = world.compose(&raw).await;
        assert!(
            res.is_err(),
            "L2→L1 unexpectedly produced a composition — the direction may now \
             be supported; revisit the oracle. Got: {res:?}",
        );
        // Pin the shape of the rejection so a *different* failure mode (e.g. a
        // panic, or a silently-empty Ok) is caught as a regression.
        let msg = format!("{}", res.unwrap_err());
        assert!(
            msg.contains("reverted"),
            "L2→L1 rejected, but not via the expected target-revert path: {msg}",
        );
    }
}
