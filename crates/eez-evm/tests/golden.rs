//! Golden execution fixtures, read from the provers/ submodule
//! (`stateless-ecdsa/testing/evm-golden/execution.json`).
//!
//! Each scenario is a genesis plus a chain of blocks, recorded with the
//! post-state root, receipts root, logs bloom and gas used of every block (or
//! that the block is invalid). Every EEZ EVM replays the same blocks and must
//! reproduce every value exactly: the node's `crates/eez-evm` and the signer's
//! `eez-prover-evm` in provers, which records them.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use alloy_consensus::{TxReceipt, proofs::calculate_receipt_root};
use alloy_genesis::{Genesis, GenesisAccount};
use alloy_primitives::{Address, B256, Bloom, Bytes, KECCAK256_EMPTY, U256, keccak256, logs_bloom};
use eez_evm::EezEvmConfig;
use eez_primitives::Block;
use evm2::{
    bytecode::Bytecode,
    evm::{AccountInfo, InMemoryDB},
};
use reth_chainspec::ChainSpec;
use reth_evm::BundleState;
use reth_evm::{ConfigureEvm, execute::Executor};
use reth_primitives_traits::SealedBlock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Fixtures {
    scenarios: Vec<Scenario>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    genesis: Genesis,
    /// RLP-encoded blocks, executed in order on the genesis state.
    blocks: Vec<Bytes>,
    expected: Vec<Outcome>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Outcome {
    /// The block fails execution; the state is left at the previous block.
    invalid: bool,
    state_root: B256,
    receipts_root: B256,
    logs_bloom: Bloom,
    gas_used: u64,
}

fn fixtures_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../provers/stateless-ecdsa/testing/evm-golden/execution.json")
}

#[test]
fn golden_blocks_replay_exactly() {
    let path = fixtures_path();
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "read {}: {error} (check out provers/: git submodule update --init provers)",
            path.display()
        )
    });
    let fixtures: Fixtures = serde_json::from_str(&raw).expect("parse golden fixtures");
    assert!(!fixtures.scenarios.is_empty());
    for scenario in &fixtures.scenarios {
        assert_eq!(
            replay(&scenario.genesis, &scenario.blocks),
            scenario.expected,
            "scenario {}",
            scenario.name
        );
    }
}

/// Executes `blocks` in order and returns each block's outcome.
fn replay(genesis: &Genesis, blocks: &[Bytes]) -> Vec<Outcome> {
    let config = EezEvmConfig::new(Arc::new(ChainSpec::from_genesis(genesis.clone()))).unwrap();
    let mut state = genesis.alloc.clone();
    blocks
        .iter()
        .map(|rlp| {
            let block = alloy_rlp::decode_exact::<Block>(rlp).expect("decode block");
            let block = SealedBlock::seal_slow(block)
                .try_recover()
                .expect("recover senders");
            let Ok(output) = config.executor(database(&state)).execute(&block) else {
                return Outcome {
                    invalid: true,
                    ..Default::default()
                };
            };
            apply(&mut state, &output.state);
            let receipts = &output.result.receipts;
            Outcome {
                invalid: false,
                state_root: alloy_trie::root::state_root_ref_unhashed(&state),
                receipts_root: calculate_receipt_root(
                    &receipts
                        .iter()
                        .map(TxReceipt::with_bloom_ref)
                        .collect::<Vec<_>>(),
                ),
                logs_bloom: logs_bloom(receipts.iter().flat_map(TxReceipt::logs)),
                gas_used: output.result.gas_used,
            }
        })
        .collect()
}

fn database(state: &BTreeMap<Address, GenesisAccount>) -> InMemoryDB {
    let mut db = InMemoryDB::default();
    for (address, account) in state {
        let code = account.code.clone().map(Bytecode::new_raw);
        db.insert_account_info(
            address,
            AccountInfo {
                balance: account.balance,
                nonce: account.nonce.unwrap_or_default(),
                code_hash: code.as_ref().map_or(KECCAK256_EMPTY, Bytecode::hash_slow),
                code,
                ..Default::default()
            },
        );
        for (slot, value) in account.storage.iter().flatten() {
            db.insert_account_storage(
                address,
                &U256::from_be_bytes(slot.0),
                &U256::from_be_bytes(value.0),
            );
        }
    }
    db
}

/// Applies one block's changes to the full state.
fn apply(state: &mut BTreeMap<Address, GenesisAccount>, bundle: &BundleState) {
    for (address, account) in &bundle.state {
        let Some(info) = &account.info else {
            state.remove(address);
            continue;
        };
        let entry = state.entry(*address).or_default();
        if account.was_destroyed() {
            entry.storage = None;
        }
        entry.balance = info.balance;
        entry.nonce = (info.nonce != 0).then_some(info.nonce);
        if info.code_hash == KECCAK256_EMPTY {
            entry.code = None;
        } else if entry.code.as_ref().map(keccak256) != Some(info.code_hash) {
            let code = info
                .code
                .clone()
                .or_else(|| bundle.contracts.get(&info.code_hash).cloned())
                .expect("changed code is in the bundle");
            entry.code = Some(code.original_bytes());
        }
        let storage = entry.storage.get_or_insert_with(BTreeMap::new);
        for (slot, value) in &account.storage {
            let slot = B256::from(*slot);
            if value.present_value.is_zero() {
                storage.remove(&slot);
            } else {
                storage.insert(slot, B256::from(value.present_value));
            }
        }
        if storage.is_empty() {
            entry.storage = None;
        }
    }
}
