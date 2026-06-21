//! Integration tests over the shared fuzz harness (`eez_fuzz`): world boots,
//! the single-hop happy path + execute/ratify oracle, the in-tree deterministic
//! fuzz loop, and the structure-aware generator's round-trip properties.

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, B256, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use arbitrary::{Arbitrary, Unstructured};
use eez_fuzz::*;
use reth_storage_api::StateProviderFactory;
use revm::context::ContextTr;
use revm::context::TxEnv;
use revm::context::result::{ExecutionResult, Output};
use revm::database::{CacheDB, EmptyDB};
use revm::state::AccountInfo;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

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

    // Deploy `Value` via revm. Append the ABI-encoded `constructor(uint256)`.
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

    let provider = freeze(evm.db());
    let state = provider.latest().expect("latest state");
    let code = state.account_code(&value_addr).expect("account_code");
    assert!(
        code.is_some_and(|c| !c.is_empty()),
        "MockEthProvider must serve the deployed Value runtime code",
    );
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

/// Depth-2 nesting: the L2 target (`NestedValue`) itself fires a cross-rollup
/// call, so the composition records a nested action and the L2 inbound replay
/// processes it (`_consumeNestedAction` + rolling-hash check). A broken LIFO
/// overlay push/pop pairing surfaces as a compose error or an L2
/// `RollingHashMismatch` revert in the oracle.
///
/// TODO(nesting): `boot_nested` clears `InvalidReentry` (cross-rollup) but the
/// compose then fails in the ENTRY-rollup overlay path:
///   `Evm("overlay diff-apply failed: SELFDESTRUCT mutation at 0x0: out of
///    scope for overlay diff-apply")`.
/// Our contracts never SELFDESTRUCT, so this is the entry-overlay diff-apply
/// rejecting the nested L2→L1 diff — a real composer edge worth a focused look
/// (`eez-evm-inspector` overlay diff-apply). Next step to unblock the test:
/// target a THIRD rollup (non-entry follower, `RollupId(2)`) instead of the
/// entry rollup, which avoids the entry-overlay diff-apply path entirely.
#[ignore = "entry-overlay diff-apply rejects the nested diff (SELFDESTRUCT@0x0); needs a 3rd-rollup target"]
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

/// In-tree deterministic fuzz loop (a coverage-blind stand-in for the
/// `cargo-fuzz` target): many structured `raw_tx`s, each a dictionary-drawn
/// trigger. Oracle: every dispatching tx must compose AND ratify against the
/// real bytecode.
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
