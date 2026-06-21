//! Stateful op-sequence fuzzing: a `Program` is an `Arbitrary` `Vec<Op>`
//! executed against a MUTABLE dual-chain base, so deploys / registrations /
//! interactions accumulate and later steps see earlier effects.
//!
//! Coverage comes from COUPLING the steps through a growing live dictionary
//! (EF/CF-style address propagation): `Deploy` mints a `Value`, `RegisterProxy`
//! wraps a minted value into a proxy + `SetterWrapper` (growing the trigger
//! dict), and `Interact` can only fire through a live trigger. Random
//! independent steps would no-op; coupled steps build real cross-chain
//! topology and evolve state across the sequence.
//!
//! Oracle: every `Interact` composes, settles the L2 inbound back into the
//! mutable L2 base, then asserts the destination `Value` holds the value the
//! generator intended — cumulative, last-writer-wins per target.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolValue};
use arbitrary::{Arbitrary, Unstructured};
use eez_composer::LocalChainClient;
use eez_evm::{ChainDialect, EvmProtocol};
use eez_fuzz::{
    CallSpec, Dict, FuzzTx, Trigger, call_output, createCrossChainProxyCall, created, freeze, init,
    registerRollupCall, DEPLOYER, L2_ROLLUP_ID, SIGNER, SYSTEM_ADDR,
};
use eez_protocol::{
    ChainClient, Composition, CompositionResult, DEFAULT_CCM_GAS_LIMIT, EntryChainClient,
    ProxyLookupConfig, Rollup, RollupId, TargetConfig, compose_transaction,
};
use reth_evm_ethereum::EthEvmConfig;
use revm::context::ContextTr;
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::database::{CacheDB, EmptyDB};
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

/// One step of a program. `#[derive(Arbitrary)]` over `Vec<Op>` makes the
/// sequence length emergent (continue/stop falls out of the input bytes).
#[derive(Debug, Arbitrary)]
enum Op {
    /// Deploy a fresh `Value` on L2 → a candidate cross-chain target.
    Deploy,
    /// Wrap a deployed value (by index) in an L1 proxy + `SetterWrapper` →
    /// grows the live trigger dict.
    RegisterProxy { value_idx: u16 },
    /// Fire a user tx through a live trigger (the single-tx generator).
    Interact(FuzzTx),
}

#[derive(Debug, Arbitrary)]
struct Program {
    ops: Vec<Op>,
}

/// Mutable dual-chain world: L1 (EEZ + Rollup) and L2 (EEZL2) persist as
/// `CacheDB`s that every op mutates; clients are rebuilt per `Interact` from
/// fresh freezes.
struct SeqWorld {
    l1: CacheDB<EmptyDB>,
    l2: CacheDB<EmptyDB>,
    eez: Address,
    eezl2: Address,
    /// Deployed L2 `Value`s (cross-chain targets), in deploy order.
    values: Vec<Address>,
    /// Live trigger dict (grows with `RegisterProxy`).
    dict: Dict,
    /// settle target (the L2 `Value`) per dict trigger, index-aligned.
    settle_targets: Vec<Address>,
    /// Cumulative expected slot-0 per target (last-writer-wins).
    expected: HashMap<Address, U256>,
}

/// Fund an account in a cache so it can send txs.
fn fund(cache: &mut CacheDB<EmptyDB>, who: Address) {
    cache.insert_account_info(
        who,
        AccountInfo {
            balance: U256::from(10u128).pow(U256::from(24u8)),
            nonce: 0,
            ..Default::default()
        },
    );
}

/// Run one tx against a persistent cache (nonce read from state, mutation
/// committed back). Returns the result.
fn run_tx(cache: &mut CacheDB<EmptyDB>, caller: Address, kind: TxKind, data: Vec<u8>) -> ExecutionResult {
    let nonce = cache.cache.accounts.get(&caller).map(|a| a.info.nonce).unwrap_or(0);
    let taken = std::mem::take(cache);
    let mut evm = Context::mainnet().with_db(taken).build_mainnet();
    let r = evm
        .transact_commit(TxEnv {
            caller,
            kind,
            data: data.into(),
            gas_limit: 16_000_000,
            nonce,
            chain_id: Some(1),
            ..Default::default()
        })
        .expect("tx execution");
    *cache = evm.db().clone();
    r
}

/// Read slot-0 of an account directly from a committed cache.
fn read_slot0(cache: &CacheDB<EmptyDB>, addr: Address) -> U256 {
    cache
        .cache
        .accounts
        .get(&addr)
        .and_then(|a| a.storage.get(&U256::ZERO).copied())
        .unwrap_or(U256::ZERO)
}

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

impl SeqWorld {
    fn new() -> Self {
        // ── L2 base: EEZL2 (eezl2 = DEPLOYER.create(0)). ──
        let mut l2 = CacheDB::<EmptyDB>::default();
        fund(&mut l2, DEPLOYER);
        fund(&mut l2, SYSTEM_ADDR);
        let eezl2 = created(run_tx(
            &mut l2,
            DEPLOYER,
            TxKind::Create,
            init(
                "sync-rollups-protocol/out/EEZL2.sol/EEZL2.json",
                (U256::from(L2_ROLLUP_ID), SYSTEM_ADDR).abi_encode_params(),
            ),
        ));

        // ── L1 base: EEZ + MockPS + Rollup + registerRollup(L2). ──
        let mut l1 = CacheDB::<EmptyDB>::default();
        fund(&mut l1, DEPLOYER);
        let eez = created(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Create,
            init("contracts/out/EEZ.sol/EEZ.json", Vec::new()),
        ));
        let mock_ps = created(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Create,
            init(
                "contracts/out/MockECDSAProofSystem.sol/MockECDSAProofSystem.json",
                (SIGNER,).abi_encode_params(),
            ),
        ));
        let vkey = {
            let mut b = [0u8; 32];
            b[12..].copy_from_slice(SIGNER.as_slice());
            B256::from(b)
        };
        let rollup_mgr = created(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Create,
            init(
                "contracts/out/Rollup.sol/Rollup.json",
                (eez, DEPLOYER, U256::from(1u8), vec![mock_ps], vec![vkey]).abi_encode_params(),
            ),
        ));
        let rid = U256::abi_decode(&call_output(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Call(eez),
            registerRollupCall {
                rollupContract: rollup_mgr,
                initialState: B256::repeat_byte(0xab),
            }
            .abi_encode(),
        )))
        .expect("decode rollupId");
        assert_eq!(rid, U256::from(L2_ROLLUP_ID), "first registered rollup id");

        SeqWorld {
            l1,
            l2,
            eez,
            eezl2,
            values: Vec::new(),
            dict: Dict {
                chain_id: reth_chainspec::DEV.chain.id(),
                triggers: Vec::new(),
                keys: vec![PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("key")],
            },
            settle_targets: Vec::new(),
            expected: HashMap::new(),
        }
    }

    /// Build the production clients over the CURRENT frozen base + the rollups
    /// map, then compose.
    async fn compose(&self, raw: &[u8]) -> CompositionResult<Composition<EvmProtocol>> {
        let chain_spec = reth_chainspec::DEV.clone();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        let entry = LocalChainClient::new_entry(
            freeze(&self.l1),
            evm_config.clone(),
            chain_spec.clone(),
            RollupId(0),
            self.eez,
            self.eez,
            ChainDialect::EvmL1Style,
        );
        let follower = LocalChainClient::new_follower(
            freeze(&self.l2),
            evm_config,
            chain_spec,
            RollupId(L2_ROLLUP_ID),
            self.eezl2,
            self.eezl2,
            ChainDialect::EvmL2Style,
        );
        let entry_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> = entry.clone();
        let follower_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> = follower.clone();
        let mut rollups = HashMap::new();
        rollups.insert(
            RollupId(0),
            Rollup {
                client: entry_cc,
                session: None,
                config: target_cfg(self.eez, Address::ZERO, ChainDialect::EvmL1Style),
                initial_state_root: [0u8; 32],
            },
        );
        rollups.insert(
            RollupId(L2_ROLLUP_ID),
            Rollup {
                client: follower_cc,
                session: None,
                config: target_cfg(self.eezl2, SYSTEM_ADDR, ChainDialect::EvmL2Style),
                initial_state_root: [0u8; 32],
            },
        );
        let entry_ec: Arc<dyn EntryChainClient<Protocol = EvmProtocol> + Send + Sync> = entry;
        compose_transaction(&EvmProtocol, entry_ec.as_ref(), raw, RollupId(0), rollups).await
    }

    /// Settle a composition's L2 inbound back into the mutable L2 base so the
    /// next step sees the effect.
    fn settle_l2(&mut self, comp: &Composition<EvmProtocol>) {
        for t in &comp.targets {
            if let Some(inbound) = &t.inbound_payload {
                run_tx(&mut self.l2, SYSTEM_ADDR, TxKind::Call(self.eezl2), inbound.clone());
            }
        }
    }

    async fn run(&mut self, program: Program) {
        for op in program.ops {
            match op {
                Op::Deploy => {
                    let v = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init("contracts/out/Value.sol/Value.json", (U256::ZERO,).abi_encode_params()),
                    ));
                    self.values.push(v);
                }
                Op::RegisterProxy { value_idx } => {
                    if self.values.is_empty() {
                        continue;
                    }
                    let value = self.values[(value_idx as usize) % self.values.len()];
                    let proxy = Address::abi_decode(&call_output(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: value,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )))
                    .expect("decode proxy");
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init("contracts/out/SetterWrapper.sol/SetterWrapper.json", (proxy,).abi_encode_params()),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(value);
                }
                Op::Interact(tx) => {
                    if self.dict.triggers.is_empty() {
                        continue;
                    }
                    let tidx = (tx.trigger_sel as usize) % self.dict.triggers.len();
                    let settle_target = self.settle_targets[tidx];
                    let (raw, predicted) = tx.resolve_and_sign(&self.dict);
                    // Compose errors (EmptyCalls, decode, …) are valid rejections.
                    let Ok(comp) = self.compose(&raw).await else {
                        continue;
                    };
                    self.settle_l2(&comp);
                    self.expected.insert(settle_target, predicted);
                    assert_eq!(
                        read_slot0(&self.l2, settle_target),
                        predicted,
                        "Interact: target {settle_target} settled wrong value",
                    );
                }
            }
        }
        // Cumulative invariant: every target holds its last intended value.
        for (target, want) in &self.expected {
            assert_eq!(read_slot0(&self.l2, *target), *want, "cumulative: target {target}");
        }
    }
}

/// Hand-built program: deploy two values, register both, interact each, then
/// re-interact the first — state must evolve (last-writer-wins).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_builds_topology_and_evolves_state() {
    fn ix(trigger: u16, v: u128) -> Op {
        Op::Interact(FuzzTx {
            trigger_sel: trigger,
            method_sel: 0,
            signer_sel: 0,
            nonce: 0,
            value: 0,
            args: [v, 0, 0, 0],
        })
    }
    let mut w = SeqWorld::new();
    w.run(Program {
        ops: vec![
            Op::Deploy,
            Op::Deploy,
            Op::RegisterProxy { value_idx: 0 },
            Op::RegisterProxy { value_idx: 1 },
            ix(0, 5),
            ix(1, 9),
            ix(0, 7), // overwrite target 0
        ],
    })
    .await;
    assert_eq!(w.dict.triggers.len(), 2, "two triggers registered");
    assert_eq!(read_slot0(&w.l2, w.settle_targets[0]), U256::from(7u64), "target0 last write");
    assert_eq!(read_slot0(&w.l2, w.settle_targets[1]), U256::from(9u64), "target1");
}

/// Arbitrary-generated programs: deterministic seeds, must never panic and the
/// cumulative oracle must hold (compose errors are tolerated).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fuzz_program_sequences() {
    for seed in 0u64..48 {
        let mix = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let bytes: Vec<u8> = (0..16).flat_map(|i| (mix ^ (seed << (i % 13))).to_le_bytes()).collect();
        let Ok(program) = Program::arbitrary(&mut Unstructured::new(&bytes)) else {
            continue;
        };
        let mut w = SeqWorld::new();
        w.run(program).await;
    }
}
