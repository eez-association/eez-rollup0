//! Validate blocks against execution witnesses without a persistent state database.
//!
//! Witnesses are checked against the parent state root; execution results are
//! checked against the block's consensus commitments. The local extension accepts
//! generic recovered transaction and receipt types so EEZ native blocks use the
//! same validation algorithms. See `README.eez.md` for upstream provenance.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/stateless/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![no_std]

extern crate alloc;

mod recover_block;

use alloy_genesis::ChainConfig;
#[doc(inline)]
pub use recover_block::UncompressedPublicKey;
#[doc(inline)]
pub use recover_block::recover_block_with_public_keys;
#[doc(inline)]
pub use tries::StatelessTrie;
#[doc(inline)]
pub use validation::BlockStateCheckpoints;
#[doc(inline)]
pub use validation::StatelessValidationOutput;
#[doc(inline)]
pub use validation::StatelessValidationWithStateCheckpointsOutput;
#[doc(inline)]
pub use validation::TransactionStateCheckpoint;
#[doc(inline)]
pub use validation::stateless_validation;
#[doc(inline)]
pub use validation::stateless_validation_recovered;
#[doc(inline)]
pub use validation::stateless_validation_recovered_with_state_checkpoints;
#[doc(inline)]
pub use validation::stateless_validation_recovered_with_trie;
#[doc(inline)]
pub use validation::stateless_validation_recovered_with_trie_and_state_checkpoints;
#[doc(inline)]
pub use validation::stateless_validation_with_trie;

pub mod validation;
pub(crate) mod witness_db;

#[doc(inline)]
pub use alloy_rpc_types_debug::ExecutionWitness;

pub use alloy_genesis::Genesis;

use reth_ethereum_primitives::Block;

#[serde_with::serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StatelessInput {
    pub block: Block,
    pub witness: ExecutionWitness,
    #[serde_as(as = "alloy_genesis::serde_bincode_compat::ChainConfig<'_>")]
    pub chain_config: ChainConfig,
}
