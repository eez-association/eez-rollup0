//! e2e / fuzz harness for `eez_protocol::compose_transaction`.
//!
//! Strategy (see `docs/FUZZ_TESTING.md`): boot the cross-chain world ONCE in
//! revm, freeze it into in-process `MockEthProvider`s, build the production
//! `LocalChainClient`s over them, then fire many `raw_tx`s at the frozen world
//! (`compose_transaction` is read-only against the providers, so the boot cost
//! amortises to zero across a fuzz campaign).
//!
//! This module currently lands the foundation: the **bridge spike** —
//! revm-deploy → `CacheDB` snapshot → `MockEthProvider` → read back. Everything
//! downstream (full L1+L2 world, `compose_transaction`, execute+ratify oracle)
//! builds on this bridge.

use std::path::PathBuf;

use std::collections::HashMap;
use std::sync::Arc;

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{Header, SignableTransaction, Transaction, TxEip1559, TxEnvelope, TypedTransaction};
use alloy_eips::Encodable2718;
use alloy_eips::eip2718::Decodable2718;
use alloy_network::{Ethereum, EthereumWallet, NetworkWallet, TxSignerSync};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, address, hex, keccak256};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolValue, sol};
use eez_composer::LocalChainClient;
use eez_evm::{ChainDialect, EvmProtocol};
use arbitrary::{Arbitrary, Unstructured};
use eez_protocol::{
    ChainClient, Composition, CompositionResult, DEFAULT_CCM_GAS_LIMIT, EntryChainClient,
    ProxyLookupConfig, Rollup, RollupId, TargetConfig, compose_transaction,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_revm::database::StateProviderDatabase;
use reth_storage_api::StateProviderFactory;
use revm::context::ContextTr;
use revm::context::TxEnv;
use revm::context::result::{ExecutionResult, Output};
use revm::database::{CacheDB, EmptyDB};
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

sol! {
    function registerRollup(address rollupContract, bytes32 initialState) external returns (uint256);
    function createCrossChainProxy(address originalAddress, uint256 originalRollupId) external returns (address);
    function computeCrossChainProxyAddress(address originalAddress, uint256 originalRollupId) external view returns (address);
    function proxy() external view returns (address);
    function setViaProxy(uint256 v) external;
}

/// L2 rollup id the cross-chain proxy targets (registered as rollup #1).
const L2_ROLLUP_ID: u64 = 1;
/// Deterministic L2 address `Value` will live at (deployer's first CREATE).
const VALUE_L2_ADDR: Address = address!("0x1111111111111111111111111111111111111111");
/// Proof-system authorized signer (also seeds the rollup vkey).
const SIGNER: Address = address!("0x00000000000000000000000000000000000000a1");
/// EEZL2 system address (authorized to load execution tables on L2).
const SYSTEM_ADDR: Address = address!("0x00000000000000000000000000000000000000a2");

/// Run a sequence of deployer-signed txs against a fresh funded `CacheDB`,
/// returning the frozen provider. `build` issues txs via the `send` closure
/// (CREATE or CALL) and returns whatever addresses the caller wants to keep.
fn boot<R>(build: impl FnOnce(&mut dyn FnMut(TxKind, Vec<u8>) -> ExecutionResult) -> R) -> (MockEthProvider, R) {
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

/// L2 follower world: `Value` (deployed first → deterministic address) + EEZL2.
fn boot_l2_world() -> (MockEthProvider, Address, Address) {
    let (provider, (value, eezl2)) = boot(|send| {
        // Value FIRST so its address is the deployer's nonce-0 CREATE — stable
        // and known before we build the L1 proxy that targets it.
        let value = created(send(
            TxKind::Create,
            init("contracts/out/Value.sol/Value.json", (U256::ZERO,).abi_encode_params()),
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
    (provider, value, eezl2)
}

/// Depth-2 L2 world: `InnerValue` + EEZL2 + `innerProxy`(InnerValue, self-rollup)
/// + `NestedValue`(innerProxy). Returns `(provider, nested_value_addr, eezl2)`.
/// Calling `NestedValue.setValue` fires a nested cross-chain call to
/// `innerProxy`, so composing through it records dispatch at depth > 1.
fn boot_l2_world_nested() -> (MockEthProvider, Address, Address) {
    let (provider, (nested, eezl2)) = boot(|send| {
        // InnerValue (a plain `Value`) — the innermost target.
        let inner_value = created(send(
            TxKind::Create,
            init("contracts/out/Value.sol/Value.json", (U256::ZERO,).abi_encode_params()),
        ));
        // EEZL2 manager — must exist before we register a proxy on it.
        let eezl2 = created(send(
            TxKind::Create,
            init(
                "sync-rollups-protocol/out/EEZL2.sol/EEZL2.json",
                (U256::from(L2_ROLLUP_ID), SYSTEM_ADDR).abi_encode_params(),
            ),
        ));
        // innerProxy = CrossChainProxy(InnerValue, L2) on EEZL2 — a self-rollup
        // reentrant proxy the overlay inspector detects during L2 sim.
        let inner_proxy = Address::abi_decode(&call_output(send(
            TxKind::Call(eezl2),
            createCrossChainProxyCall {
                originalAddress: inner_value,
                originalRollupId: U256::from(L2_ROLLUP_ID),
            }
            .abi_encode(),
        )))
        .expect("decode innerProxy");
        // NestedValue wraps innerProxy — its setValue fires the depth-2 call.
        let nested = created(send(
            TxKind::Create,
            init("contracts/out/NestedValue.sol/NestedValue.json", (inner_proxy,).abi_encode_params()),
        ));
        (nested, eezl2)
    });
    (provider, nested, eezl2)
}

#[test]
fn boot_l2_world_deploys_target() {
    let (provider, value, eezl2) = boot_l2_world();
    let state = provider.latest().expect("latest");
    assert!(
        state.account_code(&value).expect("code").is_some_and(|c| !c.is_empty()),
        "L2 world serves Value code",
    );
    assert!(
        state.account_code(&eezl2).expect("code").is_some_and(|c| !c.is_empty()),
        "L2 world serves EEZL2 code",
    );
}

/// Deployer EOA used to send the world's creation txs.
const DEPLOYER: Address = address!("0x00000000000000000000000000000000000000d0");

/// Load a contract's creation bytecode from a forge artifact JSON.
fn creation_bytecode(artifact_rel: &str) -> Bytes {
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
fn freeze(db: &CacheDB<EmptyDB>) -> MockEthProvider {
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

#[test]
fn bridge_deploy_snapshot_readback() {
    // Fund the deployer in a fresh in-memory DB.
    let mut cache = CacheDB::<EmptyDB>::default();
    cache.insert_account_info(
        DEPLOYER,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );

    // Deploy `Value` (the L2 cross-chain target) via revm. Append the
    // ABI-encoded `constructor(uint256 initial)` arg (initial = 0).
    let mut evm = Context::mainnet().with_db(cache).build_mainnet();
    let mut data = creation_bytecode("contracts/out/Value.sol/Value.json").to_vec();
    data.extend_from_slice(&B256::ZERO.0); // uint256 initial = 0
    let tx = TxEnv {
        caller: DEPLOYER,
        kind: TxKind::Create,
        data: data.into(),
        gas_limit: 8_000_000,
        nonce: 0,
        chain_id: Some(1),
        ..Default::default()
    };
    let value_addr = match evm.transact_commit(tx).expect("deploy tx") {
        ExecutionResult::Success {
            output: Output::Create(_, Some(addr)),
            ..
        } => addr,
        other => panic!("deploy did not create a contract: {other:?}"),
    };

    // Freeze the post-deploy state into an in-process reth provider.
    let db = evm.db();
    let provider = freeze(db);

    // The bridge: the provider must serve the deployed runtime code.
    let state = provider.latest().expect("latest state");
    let code = state.account_code(&value_addr).expect("account_code");
    assert!(
        code.is_some_and(|c| !c.is_empty()),
        "MockEthProvider must serve the deployed Value runtime code",
    );
}

/// Append ABI-encoded constructor args to a contract's creation bytecode.
fn init(artifact_rel: &str, encoded_args: Vec<u8>) -> Vec<u8> {
    let mut v = creation_bytecode(artifact_rel).to_vec();
    v.extend(encoded_args);
    v
}

fn created(r: ExecutionResult) -> Address {
    match r {
        ExecutionResult::Success {
            output: Output::Create(_, Some(a)),
            ..
        } => a,
        other => panic!("expected CREATE success, got: {other:?}"),
    }
}

fn call_output(r: ExecutionResult) -> Bytes {
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
fn boot_l1_world(value_l2: Address) -> (MockEthProvider, Address, Address, Address) {
    let (provider, (eez, proxy, setter)) = boot(|send| {
        // 1. EEZ registry (no constructor).
        let eez = created(send(TxKind::Create, init("contracts/out/EEZ.sol/EEZ.json", Vec::new())));

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
    });
    (provider, eez, proxy, setter)
}

#[test]
fn boot_l1_world_registers_proxy() {
    let (provider, eez, proxy, setter) = boot_l1_world(VALUE_L2_ADDR);
    let state = provider.latest().expect("latest");
    assert!(
        state.account_code(&eez).expect("code").is_some_and(|c| !c.is_empty()),
        "frozen world serves EEZ code",
    );
    assert!(
        state.account_code(&setter).expect("code").is_some_and(|c| !c.is_empty()),
        "frozen world serves SetterWrapper code",
    );
    assert_ne!(proxy, Address::ZERO, "proxy registered");
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
    }
}

/// Sign a `SetterWrapper.setViaProxy(v)` source tx (EIP-1559, zero-fee so no
/// balance is required; source-sim disables the nonce check).
async fn sign_setter_call(setter: Address, v: u64, chain_id: u64) -> Vec<u8> {
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
type Lcc = LocalChainClient<MockEthProvider, EthEvmConfig>;

/// The booted cross-chain world: production clients over frozen providers,
/// plus the trigger-contract dictionary. Boot ONCE; `compose` is read-only
/// against the providers, so a campaign reuses the same `World`.
struct World {
    entry: Arc<Lcc>,
    follower: Arc<Lcc>,
    eez: Address,
    eezl2: Address,
    /// Dictionary of trigger contracts whose calls fire a cross-chain
    /// dispatch (today: the single `SetterWrapper`), each with its callable
    /// ABI. The fuzz generator draws from here — the "restrict the address
    /// space" requirement (see `docs/FUZZ_TESTING.md`).
    triggers: Vec<Trigger>,
    chain_id: u64,
    /// Frozen L2 state, retained so the execute+ratify oracle can replay the
    /// composition's L2 target payloads against the real bytecode.
    l2_provider: MockEthProvider,
}

impl World {
    /// Single-hop world: L1 `SetterWrapper` → proxy(`Value`@L2). One
    /// cross-chain dispatch (depth 1).
    fn boot() -> Self {
        let (l2_provider, value, eezl2) = boot_l2_world();
        let (l1_provider, eez, _proxy, setter) = boot_l1_world(value);
        Self::assemble(l1_provider, l2_provider, eez, eezl2, setter)
    }

    /// Depth-2 world: L1 `SetterWrapper` → proxy(`NestedValue`@L2), and
    /// `NestedValue.setValue` itself calls a self-rollup proxy(`Value`@L2) —
    /// so composing fires cross-chain dispatch at depth > 1, exercising the
    /// LIFO overlay push/pop pairing.
    fn boot_nested() -> Self {
        let (l2_provider, nested_value, eezl2) = boot_l2_world_nested();
        let (l1_provider, eez, _proxy, setter) = boot_l1_world(nested_value);
        Self::assemble(l1_provider, l2_provider, eez, eezl2, setter)
    }

    /// Build the production clients over frozen providers (shared by the
    /// single-hop and nested boots).
    fn assemble(
        l1_provider: MockEthProvider,
        l2_provider: MockEthProvider,
        eez: Address,
        eezl2: Address,
        setter: Address,
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
            triggers: vec![Trigger {
                address: setter,
                calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
            }],
            chain_id: reth_chainspec::DEV.chain.id(),
        }
    }

    /// The runtime dictionary the fuzz generator draws from.
    fn dict(&self) -> Dict {
        Dict {
            chain_id: self.chain_id,
            triggers: self.triggers.clone(),
            keys: vec![PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("key")],
        }
    }

    /// Rebuild the per-call rollups map (cheap: `Arc` clones + a 2-entry map).
    fn rollups(&self) -> HashMap<RollupId, Rollup<EvmProtocol>> {
        let entry_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> = self.entry.clone();
        let follower_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> =
            self.follower.clone();
        let mut m = HashMap::new();
        m.insert(
            RollupId(0),
            Rollup {
                client: entry_cc,
                session: None,
                config: target_cfg(self.eez, Address::ZERO, ChainDialect::EvmL1Style),
                initial_state_root: [0u8; 32],
            },
        );
        m.insert(
            RollupId(L2_ROLLUP_ID),
            Rollup {
                client: follower_cc,
                session: None,
                config: target_cfg(self.eezl2, SYSTEM_ADDR, ChainDialect::EvmL2Style),
                initial_state_root: [0u8; 32],
            },
        );
        m
    }

    /// Drive the function under test against the frozen world.
    async fn compose(&self, raw_tx: &[u8]) -> CompositionResult<Composition<EvmProtocol>> {
        let entry_ec: Arc<dyn EntryChainClient<Protocol = EvmProtocol> + Send + Sync> =
            self.entry.clone();
        compose_transaction(&EvmProtocol, entry_ec.as_ref(), raw_tx, RollupId(0), self.rollups()).await
    }

    /// Execute + ratify oracle (see `docs/FUZZ_TESTING.md`): replay the
    /// composition's OWN production payloads against the real frozen bytecode
    /// and assert they apply without revert.
    ///
    /// - L2 target (the real overlay oracle): the arriving system tx runs
    ///   `EEZL2.executeIncomingCrossChainCall`, which loads the table and
    ///   checks the rolling hash + call counts against the real bytecode — a
    ///   no-revert here ratifies the target. A broken overlay push/pop
    ///   pairing in `SessionInspector` makes the source sim record wrong
    ///   inner state/return-data, which surfaces here as a revert or
    ///   `RollingHashMismatch`.
    /// - L1 source: the composer emits the batch WITHOUT proofs — the
    ///   downstream poster signs it (the rollup's ECDSA proof over `SIGNER`)
    ///   before submitting, and `postVerifyAndExecuteOrSaveExecutionsFromBatch`
    ///   reverts `InvalidProofSystemConfig` on an unsigned batch
    ///   (`EEZ.sol:435`). That signature is downstream of the composer's
    ///   overlay logic (and `SIGNER` has no key here), so the L1 side is a
    ///   structural check; the L2 replay above is the ratification that
    ///   exercises the overlay pairing.
    fn assert_executes_and_ratifies(&self, comp: &Composition<EvmProtocol>) {
        for target in &comp.targets {
            if let Some(inbound) = &target.inbound_payload {
                let r = replay_once(
                    &self.l2_provider,
                    SYSTEM_ADDR,
                    self.eezl2,
                    inbound.clone(),
                    target.inbound_value,
                );
                assert!(r.is_success(), "L2 inbound execute+ratify reverted: {r:?}");
            } else {
                let r = replay_once(
                    &self.l2_provider,
                    SYSTEM_ADDR,
                    self.eezl2,
                    target.load_table_payload.clone(),
                    U256::ZERO,
                );
                assert!(r.is_success(), "L2 loadExecutionTable reverted: {r:?}");
            }
        }

        assert!(
            comp.source.entry_payload.len() > 4,
            "composer must emit a non-empty source entry-chain payload",
        );
    }
}

/// Apply one call tx against a frozen provider's state in revm and return the
/// result. The caller is overlaid as a funded nonce-0 EOA so system/deployer
/// senders need no prior funding or nonce bookkeeping.
fn replay_once(
    provider: &MockEthProvider,
    caller: Address,
    to: Address,
    data: Vec<u8>,
    value: U256,
) -> ExecutionResult {
    let sp = provider.latest().expect("latest state");
    let mut db = CacheDB::new(StateProviderDatabase::new(sp));
    db.insert_account_info(
        caller,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );
    let mut evm = Context::mainnet().with_db(db).build_mainnet();
    evm.transact_commit(TxEnv {
        caller,
        kind: TxKind::Call(to),
        data: data.into(),
        gas_limit: 16_000_000,
        nonce: 0,
        chain_id: Some(1),
        value,
        ..Default::default()
    })
    .expect("evm transact")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compose_setter_via_proxy() {
    let world = World::boot();
    let raw_tx = sign_setter_call(world.triggers[0].address, 42, world.chain_id).await;
    let composition = world.compose(&raw_tx).await.expect("compose_transaction");
    assert_eq!(
        composition.targets.len(),
        1,
        "cross-chain call must produce exactly one target composition",
    );
    world.assert_executes_and_ratifies(&composition);
}

// ─────────────────────────── raw_tx generator ───────────────────────────
//
// Structure-aware `raw_tx` generator. The address space is restricted by
// CONSTRUCTION: the fuzzer picks an *index* into the live trigger dict, never
// a raw 20-byte address — so a mutation swaps which trigger/method/signer
// fires (the 256-bit-address-EQ-no-gradient problem never bites), and every
// input dispatches into the cross-chain path instead of no-op'ing. See
// `docs/FUZZ_TESTING.md`.

/// A 4-byte selector + count of 32-byte static (uint256-shaped) args for one
/// trigger method. Declarative ABI table; dynamic args are out of scope.
#[derive(Clone, Debug)]
struct CallSpec {
    selector: [u8; 4],
    static_args: usize,
    /// Whether the trigger method accepts `msg.value`. Sending value to a
    /// non-payable method reverts before the proxy call fires (→ `EmptyCalls`),
    /// so the generator only attaches value when this is set.
    payable: bool,
}

impl CallSpec {
    /// A non-payable method from a canonical signature, e.g.
    /// `"setViaProxy(uint256)"`.
    fn from_sig(sig: &str, static_args: usize) -> Self {
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&keccak256(sig.as_bytes())[..4]);
        Self {
            selector,
            static_args,
            payable: false,
        }
    }
}

/// One trigger contract (reaches a proxy when called) + its callable methods.
#[derive(Clone, Debug)]
struct Trigger {
    address: Address,
    calls: Vec<CallSpec>,
}

/// Runtime dictionary the world fixture hands the generator: live triggers,
/// fixture signing keys, chain id.
#[derive(Debug)]
struct Dict {
    chain_id: u64,
    triggers: Vec<Trigger>,
    keys: Vec<PrivateKeySigner>,
}

/// Depth-2 nesting: the L2 target (`NestedValue`) itself fires a self-rollup
/// cross-chain call, so the composition records a nested action and the L2
/// inbound replay processes it (`_consumeNestedAction` + rolling-hash check).
/// A broken LIFO overlay push/pop pairing surfaces as a compose error or an
/// L2 `RollingHashMismatch` revert in the oracle.
///
/// TODO(nesting): the current `boot_nested` registers the inner proxy as a
/// SELF-rollup (L2→L2) call, which the composer rejects with `InvalidReentry`
/// (`composition.rs:734` — same-chain non-entry self-dispatch is disallowed).
/// Depth-2 must be CROSS-rollup (L2→L1): deploy `InnerValue` on L1 at a
/// deterministic address and point `innerProxy` at it with `originalRollupId
/// = 0`. Ignored until that topology lands.
#[ignore = "needs cross-rollup nested topology; self-rollup reentry is rejected"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compose_nested_depth2_ratifies() {
    let world = World::boot_nested();
    let raw_tx = sign_setter_call(world.triggers[0].address, 7, world.chain_id).await;
    let composition = world.compose(&raw_tx).await.expect("compose nested depth-2");

    let nested_actions: usize = composition
        .targets
        .iter()
        .flat_map(|t| t.batch.inner.entries.iter())
        .map(|e| e.expectedL1ToL2Calls.len())
        .sum();
    assert!(nested_actions > 0, "nested world must record a depth-2 nested action");

    world.assert_executes_and_ratifies(&composition);
}

/// Structure-aware fuzz input: indices into the dict + typed leaves. Fixed
/// width keeps libFuzzer mutations byte-stable (one byte → one choice).
#[derive(Debug, Arbitrary)]
struct FuzzTx {
    trigger_sel: u16,
    method_sel: u8,
    signer_sel: u8,
    nonce: u64,
    value: u64,
    args: [u128; 4],
}

impl FuzzTx {
    /// Resolve indices against `dict` and return signed EIP-2718 `raw_tx`
    /// bytes — the input `simulate_source_tx` decodes.
    fn resolve_and_sign(&self, dict: &Dict) -> Vec<u8> {
        let trig = &dict.triggers[(self.trigger_sel as usize) % dict.triggers.len()];
        let call = &trig.calls[(self.method_sel as usize) % trig.calls.len()];
        let signer = &dict.keys[(self.signer_sel as usize) % dict.keys.len()];

        let mut input = Vec::with_capacity(4 + 32 * call.static_args);
        input.extend_from_slice(&call.selector);
        for i in 0..call.static_args {
            let word = U256::from(self.args[i % self.args.len()]);
            input.extend_from_slice(&word.to_be_bytes::<32>());
        }
        let tx_value = if call.payable { U256::from(self.value) } else { U256::ZERO };
        sign_call(signer, dict.chain_id, self.nonce, trig.address, tx_value, input.into())
    }
}

/// Sign a zero-fee EIP-1559 call tx (no balance needed; source-sim disables
/// the nonce check) and return its EIP-2718 wire bytes.
fn sign_call(
    signer: &PrivateKeySigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value: U256,
    input: Bytes,
) -> Vec<u8> {
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 5_000_000,
        max_fee_per_gas: 0,
        max_priority_fee_per_gas: 0,
        to: TxKind::Call(to),
        value,
        access_list: Default::default(),
        input,
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign tx");
    TxEnvelope::from(tx.into_signed(sig)).encoded_2718()
}

/// Fuzz loop over the frozen world: many structured `raw_tx`s, each a
/// dictionary-drawn trigger. Oracle: every dispatching tx must compose AND
/// ratify against the real bytecode (see `assert_executes_and_ratifies`).
/// Deterministic seeds — no wall-clock / RNG.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fuzz_compose_dictionary() {
    let world = World::boot();
    let dict = world.dict();
    for seed in 0u64..128 {
        let mix = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let bytes: Vec<u8> = (0..6).flat_map(|i| (mix ^ (seed << i)).to_le_bytes()).collect();
        let mut u = Unstructured::new(&bytes);
        let Ok(input) = FuzzTx::arbitrary(&mut u) else {
            continue;
        };
        let raw_tx = input.resolve_and_sign(&dict);

        let composition = world
            .compose(&raw_tx)
            .await
            .unwrap_or_else(|e| panic!("compose failed for seed={seed}: {e:?}"));
        assert_eq!(
            composition.targets.len(),
            1,
            "dictionary trigger must compose to one target (seed={seed})",
        );
        world.assert_executes_and_ratifies(&composition);
    }
}

#[cfg(test)]
mod generator_tests {
    use super::*;

    /// Two synthetic triggers + a key — exercises the generator with no world.
    fn sample_dict() -> Dict {
        let set = CallSpec::from_sig("setViaProxy(uint256)", 1);
        Dict {
            chain_id: 1,
            triggers: vec![
                Trigger {
                    address: Address::repeat_byte(0xA1),
                    calls: vec![set.clone()],
                },
                Trigger {
                    address: Address::repeat_byte(0xB2),
                    calls: vec![set],
                },
            ],
            keys: vec![PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).unwrap()],
        }
    }

    #[test]
    fn generated_tx_decodes_recovers_and_targets_a_trigger() {
        let dict = sample_dict();
        let input = FuzzTx {
            trigger_sel: 1,
            method_sel: 0,
            signer_sel: 0,
            nonce: 7,
            value: 0,
            args: [42, 0, 0, 0],
        };
        let raw = input.resolve_and_sign(&dict);
        let env = TxEnvelope::decode_2718(&mut raw.as_slice()).expect("decode_2718");

        assert_eq!(env.to().expect("call"), dict.triggers[1].address);
        assert_eq!(env.recover_signer().expect("recover"), dict.keys[0].address());
        assert_eq!(&env.input()[..4], &CallSpec::from_sig("setViaProxy(uint256)", 1).selector);
        assert_eq!(U256::from_be_slice(&env.input()[4..36]), U256::from(42u64));
        assert_eq!(env.nonce(), 7);
    }

    #[test]
    fn arbitrary_indices_always_resolve_within_dict() {
        let dict = sample_dict();
        for seed in 0u8..32 {
            let bytes = [seed; 64];
            let input = FuzzTx::arbitrary(&mut Unstructured::new(&bytes)).expect("arbitrary");
            let raw = input.resolve_and_sign(&dict);
            let env = TxEnvelope::decode_2718(&mut raw.as_slice()).expect("decode_2718");
            let to = env.to().expect("call");
            assert!(dict.triggers.iter().any(|t| t.address == to), "to={to} not a trigger");
            env.recover_signer().expect("recover");
        }
    }
}
