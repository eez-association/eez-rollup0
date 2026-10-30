//! Canonical closed-prefix roots without finalizing the continuing execution.
//!
//! Pinned Stateless snapshots cumulative transaction deltas before `finish`.
//! A private trie adapter applies Rollup0's balance cleanup to those disposable
//! snapshots. The actual terminal execution output is then checked again with
//! the ordinary trie: checkpoint projection must never repair an invalid block.

use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, U256, keccak256, map::B256IndexMap};
use alloy_rpc_types_debug::ExecutionWitness;
use eez_primitives::{Block, Receipt};
use reth_chainspec::ChainSpec;
use reth_primitives_traits::{Account, RecoveredBlock};
use reth_trie_common::{HashedPostState, KeccakKeyHasher, TrieAccount};
use stateless_reth::StatelessValidationWithStateCheckpointsOutput;
use stateless_reth::validation::{
    StatelessValidationError, stateless_validation_recovered_with_trie_and_state_checkpoints,
};
use stateless_tries::{
    StatelessTrie, StatelessTrieError, WitnessDbError, default::StatelessSparseTrie,
};

use crate::{EezEvmConfig, balances::PROTOCOL_BALANCE_ACCOUNTS};

/// Validate a Rollup0 block and derive closed-prefix roots in one EVM replay.
///
/// Checkpoints include finalization cleanup on copies; subsequent transactions
/// and FIFO observations still execute on the unfinalized prefix. Indices must
/// be strictly increasing and in bounds. Settlement callers select only complete
/// groups. This function does not authorize an intermediate native load as a
/// standalone block or as a settlement boundary.
///
/// # Errors
///
/// Retains Stateless's witness, consensus, execution and index checks. Also
/// rejects a terminal root that matches only after checkpoint projection: the
/// actual executor output must already contain the block-finalization transition.
pub fn validate_with_closed_prefixes(
    block: RecoveredBlock<Block>,
    witness: ExecutionWitness,
    chain_spec: Arc<ChainSpec>,
    evm_config: EezEvmConfig,
    transaction_indices: &[stateless_reth::CheckpointAt],
) -> Result<StatelessValidationWithStateCheckpointsOutput<Receipt>, StatelessValidationError> {
    let raw_witness = witness.clone();
    let output = stateless_validation_recovered_with_trie_and_state_checkpoints::<
        ClosedPrefixTrie,
        _,
        _,
        eez_primitives::EezPrimitives,
    >(block, witness, chain_spec, evm_config, transaction_indices)?;
    verify_unmodified_terminal(&output.validation, raw_witness)?;
    Ok(output)
}

fn verify_unmodified_terminal(
    output: &stateless_reth::StatelessValidationOutput<Receipt>,
    witness: ExecutionWitness,
) -> Result<(), StatelessValidationError> {
    let (mut trie, _) = StatelessSparseTrie::new(witness, output.pre_state_root)?;
    let bundle = &output.execution_output.state;
    let mut state = HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle.state());
    // Preserve pinned Stateless's handling of destroyed/recreated storage,
    // including blinded old slots not represented by the new storage delta.
    for (address, account) in bundle.state() {
        if account.was_destroyed() {
            let hash = keccak256(address);
            trie.clear_storage(hash);
            state.storages.entry(hash).or_default();
        }
    }
    let got = trie.calculate_state_root(state)?;
    if got != output.post_state_root {
        return Err(StatelessValidationError::PostStateRootMismatch {
            got,
            expected: output.post_state_root,
        });
    }
    Ok(())
}

#[derive(Debug)]
struct ClosedPrefixTrie(StatelessSparseTrie);

impl StatelessTrie for ClosedPrefixTrie {
    fn new(
        witness: ExecutionWitness,
        root: B256,
    ) -> Result<(Self, B256IndexMap<Bytes>), StatelessTrieError> {
        let (trie, code) = StatelessSparseTrie::new(witness, root)?;
        Ok((Self(trie), code))
    }

    fn account(&self, address: Address) -> Result<Option<TrieAccount>, WitnessDbError> {
        self.0.account(address)
    }

    fn storage(&self, address: Address, slot: U256) -> Result<U256, WitnessDbError> {
        self.0.storage(address, slot)
    }

    fn clear_storage(&mut self, address: B256) {
        self.0.clear_storage(address);
    }

    fn calculate_state_root(
        &mut self,
        mut state: HashedPostState,
    ) -> Result<B256, StatelessTrieError> {
        for address in PROTOCOL_BALANCE_ACCOUNTS {
            let hash = keccak256(address);
            let account = match state.accounts.get(&hash) {
                Some(account) => *account,
                None => self
                    .0
                    .account(address)
                    .map_err(|_| StatelessTrieError::StatelessStateRootCalculationFailed)?
                    .map(Account::from),
            };
            if let Some(mut account) = account
                && !account.balance.is_zero()
            {
                account.balance = U256::ZERO;
                // Match the EIP-161 state-clear transition when touching a newly
                // empty account; missing/already-zero accounts remain untouched.
                if account.is_empty() {
                    state.accounts.insert(hash, None);
                    state.storages.remove(&hash);
                    self.0.clear_storage(hash);
                } else {
                    state.accounts.insert(hash, Some(account));
                }
            }
        }
        self.0.calculate_state_root(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::constants::EMPTY_ROOT_HASH;
    use revm::{
        DatabaseCommit,
        database::{InMemoryDB, State, states::bundle_state::BundleRetention},
        state::{Account as EvmAccount, AccountInfo, EvmState},
    };

    fn empty_witness() -> ExecutionWitness {
        ExecutionWitness {
            state: vec![Bytes::from_static(&[0x80])],
            ..Default::default()
        }
    }

    fn draft() -> State<InMemoryDB> {
        let mut state = State::builder()
            .with_database(InMemoryDB::default())
            .with_bundle_update()
            .build();
        let mut accounts = EvmState::default();
        for (address, nonce, code_hash) in [
            (
                eez_primitives::SYSTEM_ADDRESS,
                0,
                alloy_consensus::constants::KECCAK_EMPTY,
            ),
            (eez_primitives::EEZL2_ADDRESS, 1, keccak256([0])),
            (
                Address::with_last_byte(73),
                2,
                alloy_consensus::constants::KECCAK_EMPTY,
            ),
        ] {
            let mut account = EvmAccount::from(AccountInfo {
                balance: U256::from(19),
                nonce,
                code_hash,
                ..Default::default()
            });
            account.mark_touch();
            // State::commit requires the original account to be cached first.
            state.load_cache_account(address).unwrap();
            accounts.insert(address, account);
        }
        state.commit(accounts);
        state
    }

    #[test]
    fn closed_prefix_root_matches_executor_cleanup_without_mutating_the_draft() {
        let mut state = draft();
        state.merge_transitions(BundleRetention::PlainState);
        let raw = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.bundle_state.state());
        let (mut projected, _) = ClosedPrefixTrie::new(empty_witness(), EMPTY_ROOT_HASH).unwrap();
        let projected_root = projected.calculate_state_root(raw.clone()).unwrap();
        assert_eq!(
            state.bundle_state.state[&eez_primitives::SYSTEM_ADDRESS]
                .info
                .as_ref()
                .unwrap()
                .balance,
            U256::from(19)
        );
        let mut cleanup = EvmState::default();
        crate::balances::clear_protocol_balances(&mut state, &mut cleanup).unwrap();
        state.commit(cleanup);
        state.merge_transitions(BundleRetention::PlainState);
        let closed =
            HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.bundle_state.state());
        let (mut trie, _) = StatelessSparseTrie::new(empty_witness(), EMPTY_ROOT_HASH).unwrap();
        assert_eq!(projected_root, trie.calculate_state_root(closed).unwrap());
        let (mut raw_trie, _) = StatelessSparseTrie::new(empty_witness(), EMPTY_ROOT_HASH).unwrap();
        assert_ne!(projected_root, raw_trie.calculate_state_root(raw).unwrap());
    }

    #[test]
    fn closing_a_prefix_cleans_untouched_inventory_from_its_witness() {
        use alloy_rlp::Encodable as _;
        fn leaf(account: TrieAccount) -> Bytes {
            let mut path = vec![0x20];
            path.extend_from_slice(keccak256(eez_primitives::SYSTEM_ADDRESS).as_slice());
            let encoded = alloy_rlp::encode(account);
            let mut node = Vec::new();
            alloy_rlp::Header {
                list: true,
                payload_length: path.as_slice().length() + encoded.as_slice().length(),
            }
            .encode(&mut node);
            path.as_slice().encode(&mut node);
            encoded.as_slice().encode(&mut node);
            node.into()
        }
        for nonce in [0, 7] {
            let mut account = TrieAccount {
                nonce,
                balance: U256::from(23),
                ..Default::default()
            };
            let before = leaf(account);
            let witness = ExecutionWitness {
                state: vec![before.clone()],
                ..Default::default()
            };
            let (mut trie, _) = ClosedPrefixTrie::new(witness, keccak256(&before)).unwrap();
            account.balance = U256::ZERO;
            let expected = if nonce == 0 {
                EMPTY_ROOT_HASH
            } else {
                keccak256(leaf(account))
            };
            assert_eq!(
                trie.calculate_state_root(HashedPostState::default())
                    .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn checkpoint_projection_cannot_hide_missing_terminal_cleanup() {
        let mut state = draft();
        state.merge_transitions(BundleRetention::PlainState);
        let raw = HashedPostState::from_bundle_state::<KeccakKeyHasher>(state.bundle_state.state());
        let (mut projected, _) = ClosedPrefixTrie::new(empty_witness(), EMPTY_ROOT_HASH).unwrap();
        let closed_root = projected.calculate_state_root(raw).unwrap();
        let output = stateless_reth::StatelessValidationOutput {
            block_hash: B256::ZERO,
            pre_state_root: EMPTY_ROOT_HASH,
            post_state_root: closed_root,
            block_access_list: None,
            execution_output: reth_evm::execute::BlockExecutionOutput {
                state: state.take_bundle(),
                result: Default::default(),
            },
        };
        assert!(
            matches!(verify_unmodified_terminal(&output, empty_witness()),
            Err(StatelessValidationError::PostStateRootMismatch { expected, .. }) if expected == closed_root)
        );
    }
}
