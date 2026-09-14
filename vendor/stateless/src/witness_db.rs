//! Serve EVM state reads from a verified witness trie, bytecode, and ancestor hashes.

use alloc::{collections::btree_map::BTreeMap, format};
use alloy_primitives::{Address, B256, Bytes, U256, map::B256IndexMap};
use revm_bytecode::Bytecode;
use revm_database_interface::Database;
use revm_state::AccountInfo;
use tries::{StatelessTrie, WitnessDbError};

#[derive(Debug)]
pub(crate) struct WitnessDatabase<'a, T>
where
    T: StatelessTrie,
{
    block_hashes_by_block_number: BTreeMap<u64, B256>,
    bytecode: B256IndexMap<Bytes>,
    trie: &'a T,
}

impl<'a, T> WitnessDatabase<'a, T>
where
    T: StatelessTrie,
{
    /// The caller must verify the parent-state trie, bytecode hashes, and
    /// contiguous ancestor chain before constructing the execution database.
    pub(crate) const fn new(
        trie: &'a T,
        bytecode: B256IndexMap<Bytes>,
        ancestor_hashes: BTreeMap<u64, B256>,
    ) -> Self {
        Self {
            trie,
            block_hashes_by_block_number: ancestor_hashes,
            bytecode,
        }
    }
}

impl<T> Database for WitnessDatabase<'_, T>
where
    T: StatelessTrie,
{
    type Error = WitnessDbError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.trie.account(address).map(|opt| {
            opt.map(|account| AccountInfo {
                balance: account.balance,
                nonce: account.nonce,
                code_hash: account.code_hash,
                code: None,
                account_id: None,
            })
        })
    }

    fn storage(&mut self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.trie.storage(address, slot)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let raw = self.bytecode.get(&code_hash).cloned().ok_or_else(|| {
            WitnessDbError::TrieWitness(format!("bytecode for {code_hash} not found"))
        })?;
        Ok(Bytecode::new_raw(raw))
    }

    fn block_hash(&mut self, block_number: u64) -> Result<B256, Self::Error> {
        self.block_hashes_by_block_number
            .get(&block_number)
            .copied()
            .ok_or(WitnessDbError::StateNotFound(block_number))
    }
}
