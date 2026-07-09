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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    CallSpec, DEPLOYER, Dict, Direction, FuzzTx, L2_ROLLUP_ID, SIGNER, SYSTEM_ADDR, Trigger,
    call_output, createCrossChainProxyCall, created, freeze, init, registerRollupCall,
};
use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{SolCall, SolValue};
use arbitrary::Arbitrary;
use eez_composer::LocalChainClient;
use eez_evm::entries::encode_execute_incoming;
use eez_evm::types::loadExecutionTableCall;
use eez_evm::{ChainDialect, EvmProtocol};
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

/// How a trigger's settled effect is predicted and checked. One per dict
/// trigger, set when the `Register*`/`Deploy*` op wires it up.
#[derive(Clone, Copy, Debug)]
enum SettleMode {
    /// slot-0 becomes the arg (`Value`, multi-call, two-diff, `NestedValue`).
    SetsArg,
    /// `RevertableValue`: reverts on odd args, so an odd arg leaves slot-0 at
    /// its prior value (the try/catch wrapper continues) and an even arg sets it.
    RevertOnOdd,
    /// `ForceRevertWrapper` (`revertSpan`): the call succeeds then its span is
    /// force-discarded, so slot-0 ALWAYS stays at its prior value.
    AlwaysUnchanged,
    /// Non-slot-0 effect (a `Bridge` value transfer settles as a balance). The
    /// step composes + settles but the cumulative slot-0 oracle skips it; the
    /// hand-written port test asserts the real effect.
    Skip,
}

/// One step of a program. `#[derive(Arbitrary)]` over `Vec<Op>` makes the
/// sequence length emergent (continue/stop falls out of the input bytes).
#[derive(Debug, Arbitrary)]
pub enum Op {
    /// Deploy a fresh `Value` on L2 → a candidate cross-chain target.
    Deploy,
    /// Wrap a deployed value (by index) in an L1 proxy + `SetterWrapper` →
    /// grows the live trigger dict.
    RegisterProxy { value_idx: u16 },
    /// Deploy a fresh `Value` reached via a `MultiSetterWrapper` that hits the
    /// proxy TWICE in one tx → the composer records two cross-chain entries with
    /// the same `proxyEntryHash` (multi-entry / sequential-cursor path).
    /// Self-contained (deploys its own target).
    RegisterMultiCall,
    /// Deploy a `RevertableValue` (reverts on odd args) reached via a try/catch
    /// `RevertTolerantWrapper` → an `Interact` with an odd arg drives the
    /// cross-chain natural-revert path (`CALL_END(success=false)`), the L2 state
    /// stays unchanged. Self-contained (deploys its own target).
    RegisterRevertTolerant,
    /// Deploy a `TwoProxySetter` reaching two DIFFERENT proxies/targets in one
    /// tx → the composer records two entries with different `proxyEntryHash`es
    /// (the multi-call-two-diff shape). Self-contained (deploys both targets).
    RegisterTwoDiff,
    /// Fire a user tx through a live trigger (the single-tx generator).
    Interact(FuzzTx),
    /// Build an L2→L1 relay: `InnerValue` on L1, an L2-side proxy for it
    /// (rollup 0 = entry), a `NestedValue` on L2 wrapping that proxy, then an
    /// L1 proxy + `SetterWrapper` for the `NestedValue` (a live trigger). An
    /// `Interact` through this trigger makes the L2 target call a proxy that
    /// targets L1 — the "L2 tx calls a proxy on L1" topology under question.
    DeployRelayToL1,
    /// Recursive deep-nesting: wrap the current chain top (or a fresh leaf
    /// `Value`) in a `NestedValue` via a SAME-ROLLUP self-referential L2 proxy
    /// (the `deepNested` e2e topology). Each `DeployNested` adds one level, so a
    /// run of N of them + an `Interact` builds depth-N — emergent depth the
    /// fuzzer can discover by chaining ops (address propagation).
    DeployNested,
    /// Deploy a `Value` reached via a `ForceRevertWrapper`: the cross-chain
    /// `setValue` succeeds but runs inside a self-call that always reverts, so
    /// its span must be force-discarded (`revertSpan`) — the target stays
    /// UNCHANGED. Self-contained. (`revertCounter` e2e shape.)
    RegisterForceRevert,
    /// Deploy a `Bridge` (sender on L1 → L2 proxy → receiver on L2) and register
    /// a payable `bridge()` trigger: an `Interact` forwards `msg.value` across
    /// the chain. The settled effect is the receiver's BALANCE, not slot-0, so
    /// the cumulative oracle skips it. Self-contained. (`bridge` e2e shape.)
    RegisterBridge,
}

/// One program step: an op plus the cross-chain DIRECTION it acts in. Pairing
/// direction *beside* the op (not inside each variant) gives the fuzzer a
/// uniform `[dir][op]` byte layout — a clean 1-bit gradient at a fixed offset —
/// and every op gets the direction axis for free. Register ops consume `dir`
/// (which entry side to build the topology on); `Deploy` ignores it; `Interact`
/// derives direction from the trigger it fires through, not from its own step.
#[derive(Debug, Arbitrary)]
pub struct Step {
    pub op: Op,
    pub dir: Direction,
}

#[derive(Debug, Arbitrary)]
pub struct Program {
    steps: Vec<Step>,
}

/// Mutable dual-chain world: L1 (EEZ + Rollup) and L2 (EEZL2) persist as
/// `CacheDB`s that every op mutates; clients are rebuilt per `Interact` from
/// fresh freezes.
pub struct SeqWorld {
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
    /// How each trigger's settled effect is predicted/checked, index-aligned
    /// with `settle_targets`. Generalizes the original "revertable" bit so the
    /// backlog ops (force-revert span, bridge value transfer) share one oracle.
    settle_mode: Vec<SettleMode>,
    /// Entry DIRECTION of each trigger's topology, index-aligned. An `Interact`
    /// reads this to enter `compose` on the side the trigger's contracts
    /// physically live (L1 for `L1ToL2`, L2 for the `L2ToL1` mirror).
    trigger_dir: Vec<Direction>,
    /// Cumulative expected slot-0 per target (last-writer-wins).
    expected: HashMap<Address, U256>,
    /// Settle targets that live on L1 (reached via an `L2ToL1` trigger), so the
    /// cumulative oracle reads their slot-0 from the L1 base, not L2.
    l1_settled: HashSet<Address>,
    /// Current top of the recursive nesting chain (`DeployNested`) — the L2
    /// `NestedValue` a further `DeployNested` wraps. `None` until the first one.
    nest_top: Option<Address>,
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
fn run_tx(
    cache: &mut CacheDB<EmptyDB>,
    caller: Address,
    kind: TxKind,
    data: Vec<u8>,
) -> ExecutionResult {
    run_tx_value(cache, caller, kind, data, U256::ZERO)
}

/// Like [`run_tx`] but with an explicit `msg.value` — the inbound settlement tx
/// (`executeIncomingCrossChainCall`) enforces `msg.value == value`.
fn run_tx_value(
    cache: &mut CacheDB<EmptyDB>,
    caller: Address,
    kind: TxKind,
    data: Vec<u8>,
    value: U256,
) -> ExecutionResult {
    let nonce = cache
        .cache
        .accounts
        .get(&caller)
        .map(|a| a.info.nonce)
        .unwrap_or(0);
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
            value,
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

/// Like `call_output` but returns `None` on a reverted/failed CALL instead of
/// panicking. Setup ops use this so a duplicate `createCrossChainProxy`
/// (CREATE2 collision → revert) skips gracefully — the fuzzer should find
/// COMPOSER bugs, not harness fragility.
fn call_ok(r: ExecutionResult) -> Option<alloy_primitives::Bytes> {
    match r {
        ExecutionResult::Success {
            output: revm::context::result::Output::Call(b),
            ..
        } => Some(b),
        _ => None,
    }
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
        // In-process follower targets settle by replaying the CCM-verify txs,
        // not by trusting a remote session root.
        settles_via_session_root: false,
    }
}

/// A booted-but-empty base world (EEZ+Rollup on L1, EEZL2 on L2). Boot ONCE
/// and `fork` it per program — cloning the `CacheDB`s skips the expensive
/// contract-deploy bootstrap, so the fuzzer runs ~100x faster than re-booting.
pub struct SeqBase {
    l1: CacheDB<EmptyDB>,
    l2: CacheDB<EmptyDB>,
    eez: Address,
    eezl2: Address,
    /// A ready L1 trigger (`SetterWrapper`) + its L2 settle target (`Value`),
    /// deployed ONCE in the base so a forked sequence can `Interact` from op 1
    /// — every tx accumulates on the same already-deployed state, no redeploy.
    setter: Address,
    value: Address,
}

impl SeqWorld {
    /// Fresh world per program — boots the base then forks it. Prefer
    /// `boot_base` + `fork` in a campaign to amortize the bootstrap.
    pub fn new() -> Self {
        Self::fork(&Self::boot_base())
    }

    /// Fork a fresh program world off a pre-booted base (cheap `CacheDB` clone).
    pub fn fork(base: &SeqBase) -> Self {
        SeqWorld {
            l1: base.l1.clone(),
            l2: base.l2.clone(),
            eez: base.eez,
            eezl2: base.eezl2,
            // The base trigger is live from op 1 — a sequence of bare `Interact`s
            // accumulates on `base.value`'s state with no setup ops needed.
            values: vec![base.value],
            dict: Dict {
                chain_id: reth_chainspec::DEV.chain.id(),
                triggers: vec![Trigger {
                    address: base.setter,
                    calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                }],
                keys: vec![PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("key")],
            },
            // Base trigger is pre-registered at index 0 (not revertable), so
            // `settle_mode` starts aligned with `settle_targets`.
            settle_targets: vec![base.value],
            settle_mode: vec![SettleMode::SetsArg],
            trigger_dir: vec![Direction::L1ToL2],
            expected: HashMap::new(),
            l1_settled: HashSet::new(),
            nest_top: None,
        }
    }

    /// Bootstrap the base contracts once (the expensive part).
    pub fn boot_base() -> SeqBase {
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

        // ── Ready trigger: Value@L2 + L1 proxy + SetterWrapper. ──
        let value = created(run_tx(
            &mut l2,
            DEPLOYER,
            TxKind::Create,
            init(
                "contracts/out/Value.sol/Value.json",
                (U256::ZERO,).abi_encode_params(),
            ),
        ));
        let proxy = Address::abi_decode(&call_output(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Call(eez),
            createCrossChainProxyCall {
                originalAddress: value,
                originalRollupId: U256::from(L2_ROLLUP_ID),
            }
            .abi_encode(),
        )))
        .expect("decode base proxy");
        let setter = created(run_tx(
            &mut l1,
            DEPLOYER,
            TxKind::Create,
            init(
                "contracts/out/SetterWrapper.sol/SetterWrapper.json",
                (proxy,).abi_encode_params(),
            ),
        ));

        SeqBase {
            l1,
            l2,
            eez,
            eezl2,
            setter,
            value,
        }
    }

    /// Build the production clients over the CURRENT frozen base + the rollups
    /// map, then compose.
    async fn compose(
        &self,
        raw: &[u8],
        dir: Direction,
    ) -> CompositionResult<Composition<EvmProtocol>> {
        let chain_spec = reth_chainspec::DEV.clone();
        let evm_config = EthEvmConfig::new(chain_spec.clone());
        // Per-rollup CONFIG is fixed by id (rollup 0 = L1/`EvmL1Style`, rollup
        // `L2_ROLLUP_ID` = L2/`EvmL2Style`); only which CLIENT is the source-sim
        // `entry` vs the dispatch `follower` — and the `entry_id` — depend on
        // direction. Mirrors `lib.rs` `World::rollups`/`compose`.
        let l1 = || {
            (
                freeze(&self.l1),
                RollupId(0),
                self.eez,
                ChainDialect::EvmL1Style,
            )
        };
        let l2 = || {
            (
                freeze(&self.l2),
                RollupId(L2_ROLLUP_ID),
                self.eezl2,
                ChainDialect::EvmL2Style,
            )
        };
        let ((e_db, entry_id, e_addr, e_dialect), (f_db, f_id, f_addr, f_dialect)) = match dir {
            Direction::L1ToL2 => (l1(), l2()),
            Direction::L2ToL1 => (l2(), l1()),
        };
        let entry = LocalChainClient::new_entry(
            e_db,
            evm_config.clone(),
            chain_spec.clone(),
            entry_id,
            e_addr,
            e_addr,
            e_dialect,
        );
        let follower = LocalChainClient::new_follower(
            f_db, evm_config, chain_spec, f_id, f_addr, f_addr, f_dialect,
        );
        let entry_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> = entry.clone();
        let follower_cc: Arc<dyn ChainClient<Protocol = EvmProtocol> + Send + Sync> =
            follower.clone();
        let client_for = |id: RollupId| {
            if id == entry_id {
                entry_cc.clone()
            } else {
                follower_cc.clone()
            }
        };
        let mut rollups = HashMap::new();
        rollups.insert(
            RollupId(0),
            Rollup {
                client: client_for(RollupId(0)),
                session: None,
                config: target_cfg(self.eez, Address::ZERO, ChainDialect::EvmL1Style),
                initial_state_root: [0u8; 32],
            },
        );
        rollups.insert(
            RollupId(L2_ROLLUP_ID),
            Rollup {
                client: client_for(RollupId(L2_ROLLUP_ID)),
                session: None,
                config: target_cfg(self.eezl2, SYSTEM_ADDR, ChainDialect::EvmL2Style),
                initial_state_root: [0u8; 32],
            },
        );
        let entry_ec: Arc<dyn EntryChainClient<Protocol = EvmProtocol> + Send + Sync> = entry;
        compose_transaction(&EvmProtocol, entry_ec.as_ref(), raw, entry_id, rollups).await
    }

    /// Settle a composition's L1→L2 inbound entries back into the mutable L2
    /// base so the next step sees the effect. Mirrors the deriver's
    /// [`build_inbound_system_txs`](eez_evm::system_tx): for each L1 deferred
    /// entry destined for L2, run the self-contained
    /// `EEZL2.executeIncomingCrossChainCall` (it self-loads the execution table
    /// AND delivers the call through the lazily-created source proxy in ONE
    /// system tx — no separate proxy address to thread). Entries for other
    /// rollups (an L2→L1 outbound settles on L1 via a different path) are
    /// skipped.
    fn settle(&mut self, comp: &Composition<EvmProtocol>) {
        for t in &comp.targets {
            // Only L1→L2 inbound (an L2 target) settles here; an L2→L1 outbound
            // settles on L1 via a different (load + user-tx) path.
            if t.rollup_id != RollupId(L2_ROLLUP_ID) {
                continue;
            }
            // The target's `load_table_payload` is `loadExecutionTable(entries)`;
            // each entry's `incomingCalls[0]` is the inbound call. Rebuild the
            // self-contained `executeIncomingCrossChainCall` from it (self-loads
            // the table + delivers via the lazily-created source proxy).
            let Ok(decoded) = loadExecutionTableCall::abi_decode(&t.load_table_payload) else {
                continue;
            };
            for entry in decoded.entries {
                let Some(call) = entry.incomingCalls.first() else {
                    continue;
                };
                let target = call.targetAddress;
                let value = call.value;
                let data = call.data.clone();
                let source = call.sourceAddress;
                let src_rollup = RollupId(u64::try_from(call.sourceRollupId).unwrap_or(u64::MAX));
                let calldata =
                    encode_execute_incoming(target, value, data, source, src_rollup, entry.clone());
                run_tx_value(
                    &mut self.l2,
                    SYSTEM_ADDR,
                    TxKind::Call(self.eezl2),
                    calldata,
                    value,
                );
            }
        }
    }

    pub async fn run(&mut self, program: Program) {
        for Step { op, dir } in program.steps {
            match op {
                Op::Deploy => {
                    let v = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
                    self.values.push(v);
                }
                Op::RegisterProxy { value_idx } => match dir {
                    // L1→L2 (implemented): wrap a minted L2 `Value` in an L1
                    // proxy + `SetterWrapper`; the user tx enters on L1.
                    Direction::L1ToL2 => {
                        if self.values.is_empty() {
                            continue;
                        }
                        let value = self.values[(value_idx as usize) % self.values.len()];
                        let Some(out) = call_ok(run_tx(
                            &mut self.l1,
                            DEPLOYER,
                            TxKind::Call(self.eez),
                            createCrossChainProxyCall {
                                originalAddress: value,
                                originalRollupId: U256::from(L2_ROLLUP_ID),
                            }
                            .abi_encode(),
                        )) else {
                            continue; // already-registered proxy → CREATE2 collision → skip
                        };
                        let proxy = Address::abi_decode(&out).expect("decode proxy");
                        let setter = created(run_tx(
                            &mut self.l1,
                            DEPLOYER,
                            TxKind::Create,
                            init(
                                "contracts/out/SetterWrapper.sol/SetterWrapper.json",
                                (proxy,).abi_encode_params(),
                            ),
                        ));
                        self.dict.triggers.push(Trigger {
                            address: setter,
                            calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                        });
                        self.settle_targets.push(value);
                        self.settle_mode.push(SettleMode::SetsArg);
                        self.trigger_dir.push(Direction::L1ToL2);
                    }
                    // L2→L1 MIRROR: target `Value` on L1, an L2-side proxy for it
                    // (rollup 0 = L1), and an L2 `SetterWrapper` trigger; the user
                    // tx enters on L2. Self-contained (`value_idx` unused — the L2
                    // value pool can't host an L1 target).
                    Direction::L2ToL1 => {
                        let target = created(run_tx(
                            &mut self.l1,
                            DEPLOYER,
                            TxKind::Create,
                            init(
                                "contracts/out/Value.sol/Value.json",
                                (U256::ZERO,).abi_encode_params(),
                            ),
                        ));
                        let Some(out) = call_ok(run_tx(
                            &mut self.l2,
                            DEPLOYER,
                            TxKind::Call(self.eezl2),
                            createCrossChainProxyCall {
                                originalAddress: target,
                                originalRollupId: U256::ZERO,
                            }
                            .abi_encode(),
                        )) else {
                            continue;
                        };
                        let proxy = Address::abi_decode(&out).expect("decode l2 proxy");
                        let setter = created(run_tx(
                            &mut self.l2,
                            DEPLOYER,
                            TxKind::Create,
                            init(
                                "contracts/out/SetterWrapper.sol/SetterWrapper.json",
                                (proxy,).abi_encode_params(),
                            ),
                        ));
                        self.dict.triggers.push(Trigger {
                            address: setter,
                            calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                        });
                        self.settle_targets.push(target);
                        self.settle_mode.push(SettleMode::SetsArg);
                        self.trigger_dir.push(Direction::L2ToL1);
                    }
                },
                Op::RegisterMultiCall => {
                    let value = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
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
                    // MultiSetterWrapper reaches the proxy twice → two same-hash entries.
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/MultiSetterWrapper.sol/MultiSetterWrapper.json",
                            (proxy,).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(value);
                    self.settle_mode.push(SettleMode::SetsArg);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::RegisterRevertTolerant => {
                    // RevertableValue (reverts on odd) reached via a try/catch
                    // wrapper that does NOT require success.
                    let value = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/RevertableValue.sol/RevertableValue.json",
                            Vec::new(),
                        ),
                    ));
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
                        init(
                            "contracts/out/RevertTolerantWrapper.sol/RevertTolerantWrapper.json",
                            (proxy,).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(value);
                    self.settle_mode.push(SettleMode::RevertOnOdd);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::RegisterTwoDiff => {
                    // Two distinct L2 Values + two proxies → different proxyEntryHashes.
                    let value_a = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
                    let value_b = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
                    let proxy_a = Address::abi_decode(&call_output(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: value_a,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )))
                    .expect("decode proxy a");
                    let proxy_b = Address::abi_decode(&call_output(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: value_b,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )))
                    .expect("decode proxy b");
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/TwoProxySetter.sol/TwoProxySetter.json",
                            (proxy_a, proxy_b).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    // Oracle checks target A; the ratify replay covers both.
                    self.settle_targets.push(value_a);
                    self.settle_mode.push(SettleMode::SetsArg);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::DeployRelayToL1 => {
                    // L1 inner target.
                    let inner_l1 = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
                    // L2-side proxy for inner_l1 on the ENTRY rollup (0).
                    let Some(out) = call_ok(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Call(self.eezl2),
                        createCrossChainProxyCall {
                            originalAddress: inner_l1,
                            originalRollupId: U256::ZERO,
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let l2_inner_proxy = Address::abi_decode(&out).expect("decode l2 inner proxy");
                    // NestedValue on L2 wrapping the L1-targeting proxy.
                    let nested = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/NestedValue.sol/NestedValue.json",
                            (l2_inner_proxy,).abi_encode_params(),
                        ),
                    ));
                    // L1 proxy + SetterWrapper for NestedValue@L2 → live trigger.
                    let Some(out) = call_ok(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: nested,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let l1_proxy = Address::abi_decode(&out).expect("decode l1 proxy");
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/SetterWrapper.sol/SetterWrapper.json",
                            (l1_proxy,).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(nested);
                    self.settle_mode.push(SettleMode::SetsArg);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::DeployNested => {
                    // Wrap the current chain top (or a fresh leaf Value) in a
                    // NestedValue via a SAME-ROLLUP self-referential L2 proxy
                    // (the deepNested e2e topology) → one more depth level.
                    let inner = match self.nest_top {
                        Some(top) => top,
                        None => created(run_tx(
                            &mut self.l2,
                            DEPLOYER,
                            TxKind::Create,
                            init(
                                "contracts/out/Value.sol/Value.json",
                                (U256::ZERO,).abi_encode_params(),
                            ),
                        )),
                    };
                    // Self-referential L2 proxy for `inner` (originalRollupId = L2).
                    let Some(out) = call_ok(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Call(self.eezl2),
                        createCrossChainProxyCall {
                            originalAddress: inner,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let inner_proxy = Address::abi_decode(&out).expect("decode inner proxy");
                    let nested = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/NestedValue.sol/NestedValue.json",
                            (inner_proxy,).abi_encode_params(),
                        ),
                    ));
                    // L1 trigger for the outermost NestedValue.
                    let Some(out) = call_ok(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: nested,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let l1_proxy = Address::abi_decode(&out).expect("decode l1 proxy");
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/SetterWrapper.sol/SetterWrapper.json",
                            (l1_proxy,).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(nested);
                    self.settle_mode.push(SettleMode::SetsArg);
                    self.trigger_dir.push(Direction::L1ToL2);
                    self.nest_top = Some(nested);
                }
                Op::RegisterForceRevert => {
                    // revertSpan shape: setValue lands cross-chain then its span
                    // is force-discarded, so the target stays UNCHANGED.
                    let value = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Value.sol/Value.json",
                            (U256::ZERO,).abi_encode_params(),
                        ),
                    ));
                    let Some(out) = call_ok(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: value,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let proxy = Address::abi_decode(&out).expect("decode proxy");
                    let setter = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/ForceRevertWrapper.sol/ForceRevertWrapper.json",
                            (proxy,).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: setter,
                        calls: vec![CallSpec::from_sig("setViaProxy(uint256)", 1)],
                    });
                    self.settle_targets.push(value);
                    self.settle_mode.push(SettleMode::AlwaysUnchanged);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::RegisterBridge => {
                    // L1 BridgeSender → L2-proxy (value-bearing call) → L2
                    // BridgeReceiver. Settled effect is the receiver's BALANCE.
                    let receiver = created(run_tx(
                        &mut self.l2,
                        DEPLOYER,
                        TxKind::Create,
                        init("contracts/out/Bridge.sol/BridgeReceiver.json", Vec::new()),
                    ));
                    let Some(out) = call_ok(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Call(self.eez),
                        createCrossChainProxyCall {
                            originalAddress: receiver,
                            originalRollupId: U256::from(L2_ROLLUP_ID),
                        }
                        .abi_encode(),
                    )) else {
                        continue;
                    };
                    let l2_proxy = Address::abi_decode(&out).expect("decode bridge proxy");
                    let sender = created(run_tx(
                        &mut self.l1,
                        DEPLOYER,
                        TxKind::Create,
                        init(
                            "contracts/out/Bridge.sol/BridgeSender.json",
                            (l2_proxy, receiver).abi_encode_params(),
                        ),
                    ));
                    self.dict.triggers.push(Trigger {
                        address: sender,
                        calls: vec![CallSpec::payable_from_sig("bridge()", 0)],
                    });
                    self.settle_targets.push(receiver);
                    self.settle_mode.push(SettleMode::Skip);
                    self.trigger_dir.push(Direction::L1ToL2);
                }
                Op::Interact(tx) => {
                    if self.dict.triggers.is_empty() {
                        continue;
                    }
                    let tidx = (tx.trigger_sel as usize) % self.dict.triggers.len();
                    // Direction is the TRIGGER's, not this step's: the contracts
                    // physically determine which side the user tx enters from.
                    let tdir = self.trigger_dir[tidx];
                    let settle_target = self.settle_targets[tidx];
                    let (raw, intended) = tx.resolve_and_sign(&self.dict);
                    let prior = self
                        .expected
                        .get(&settle_target)
                        .copied()
                        .unwrap_or(U256::ZERO);
                    // Predict the settled slot-0 from the trigger's mode: a
                    // revertable target keeps `prior` on an odd (reverting) arg,
                    // a force-revert span always keeps `prior`, otherwise the
                    // target takes the intended arg.
                    let predicted = match self.settle_mode[tidx] {
                        SettleMode::RevertOnOdd if intended.bit(0) => prior,
                        SettleMode::AlwaysUnchanged => prior,
                        _ => intended,
                    };
                    // Compose errors (EmptyCalls, decode, …) are valid rejections.
                    let Ok(comp) = self.compose(&raw, tdir).await else {
                        continue;
                    };
                    self.settle(&comp);
                    // `Skip` targets settle a non-slot-0 effect (bridge balance);
                    // the cumulative oracle ignores them, the port test checks them.
                    if matches!(self.settle_mode[tidx], SettleMode::Skip) {
                        continue;
                    }
                    // The target lives on L1 for an L2→L1 trigger, else on L2;
                    // record which so the cumulative check reads the right chain.
                    let on_l1 = matches!(tdir, Direction::L2ToL1);
                    if on_l1 {
                        self.l1_settled.insert(settle_target);
                    }
                    self.expected.insert(settle_target, predicted);
                    let got = read_slot0(if on_l1 { &self.l1 } else { &self.l2 }, settle_target);
                    assert_eq!(
                        got, predicted,
                        "Interact: target {settle_target} settled wrong value",
                    );
                }
            }
        }
        // Cumulative invariant: every target holds its last intended value,
        // read from the chain it settles on (L1 for L2→L1 targets, else L2).
        for (target, want) in &self.expected {
            let cache = if self.l1_settled.contains(target) {
                &self.l1
            } else {
                &self.l2
            };
            assert_eq!(
                read_slot0(cache, *target),
                *want,
                "cumulative: target {target}"
            );
        }
    }
}

impl Program {
    /// Construct a hand-written L1→L2 program (every step enters on L1, the
    /// implemented direction) — lets curated L1 ports reuse the same `Op`
    /// vocabulary the fuzzer explores (the "one model, two uses" goal).
    pub fn new(ops: Vec<Op>) -> Self {
        Self::mixed(ops.into_iter().map(|op| (op, Direction::L1ToL2)).collect())
    }

    /// Construct a program with an explicit DIRECTION per step — for curated
    /// mixed-direction ports (e.g. the `*L2` cases entering on L2).
    pub fn mixed(steps: Vec<(Op, Direction)>) -> Self {
        Program {
            steps: steps
                .into_iter()
                .map(|(op, dir)| Step { op, dir })
                .collect(),
        }
    }
}

impl SeqWorld {
    /// Slot-0 of the settle target at dict-trigger `idx` — for curated cases to
    /// assert the destination contract's settled state directly. Reads the
    /// chain the target settles on (L1 for an `L2ToL1` trigger, else L2).
    pub fn target_value(&self, idx: usize) -> U256 {
        let cache = match self.trigger_dir[idx] {
            Direction::L2ToL1 => &self.l1,
            Direction::L1ToL2 => &self.l2,
        };
        read_slot0(cache, self.settle_targets[idx])
    }

    /// L2 balance of the settle target at dict-trigger `idx` — for `Skip`-mode
    /// triggers (e.g. `RegisterBridge`) whose settled effect is a value
    /// transfer, not slot-0.
    pub fn target_balance(&self, idx: usize) -> U256 {
        self.l2
            .cache
            .accounts
            .get(&self.settle_targets[idx])
            .map(|a| a.info.balance)
            .unwrap_or(U256::ZERO)
    }
}

impl Default for SeqWorld {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interact(trigger: u16, v: u128) -> Op {
        Op::Interact(FuzzTx {
            // The op-sequence harness drives the L1-entry world.
            direction: crate::Direction::L1ToL2,
            trigger_sel: trigger,
            method_sel: 0,
            signer_sel: 0,
            nonce: 0,
            value: 0,
            args: [v, 0, 0, 0],
        })
    }

    /// `RegisterMultiCall`'s trigger reaches the proxy twice; the composer must
    /// produce a multi-entry composition that still settles the target to `v`
    /// (last write wins). Asserts it actually composed + settled (a compose
    /// error would leave slot-0 at 0 and fail here, not silently skip).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multicall_op_settles_target() {
        let base = SeqWorld::boot_base();
        let mut w = SeqWorld::fork(&base);
        // Base trigger is index 0; the multi-call trigger registers at 1.
        w.run(Program::new(vec![Op::RegisterMultiCall, interact(1, 42)]))
            .await;
        assert_eq!(
            read_slot0(&w.l2, w.settle_targets[1]),
            U256::from(42u64),
            "multi-call trigger must compose + settle the target to v",
        );
    }

    /// `RegisterRevertTolerant`: an even arg settles the target; a following odd
    /// arg drives the cross-chain revert through the try/catch wrapper and must
    /// leave the target at its prior (even) value — never corrupt it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revert_tolerant_op_keeps_prior_on_revert() {
        let base = SeqWorld::boot_base();
        let mut w = SeqWorld::fork(&base);
        w.run(Program::new(vec![
            Op::RegisterRevertTolerant,
            interact(1, 8),
            interact(1, 7),
        ]))
        .await;
        assert_eq!(
            read_slot0(&w.l2, w.settle_targets[1]),
            U256::from(8u64),
            "even settles to 8; odd reverts and leaves the target unchanged",
        );
    }

    /// `RegisterTwoDiff`: one tx reaches two different proxies → two entries with
    /// different `proxyEntryHash`es; target A must settle to `v`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_diff_op_settles_target() {
        let base = SeqWorld::boot_base();
        let mut w = SeqWorld::fork(&base);
        w.run(Program::new(vec![Op::RegisterTwoDiff, interact(1, 13)]))
            .await;
        assert_eq!(
            read_slot0(&w.l2, w.settle_targets[1]),
            U256::from(13u64),
            "two-diff: target A must compose + settle to v",
        );
    }
}
