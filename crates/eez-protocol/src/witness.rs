//! EVM state witness — inert placeholder.
//!
//! The real proving witness (`alloy_rpc_types_debug::ExecutionWitness`) is
//! served by reth-node's `eez_executionWitness` and shipped via the composer
//! control feed; this type only satisfies the protocol's Serialize +
//! `DeserializeOwned` bounds.

use serde::{Deserialize, Serialize};

/// Inert placeholder for the state witness. The real witness
/// is `alloy_rpc_types_debug::ExecutionWitness`, pulled from reth
/// (`eez_executionWitness`) and shipped on the composer control feed — it
/// does not flow through this type.
///
/// The real witness contains:
/// - `state`: hashed trie node preimages
/// - `codes`: contract bytecodes accessed during execution
/// - `keys`: account/storage key preimages
/// - `headers`: RLP-encoded block headers for BLOCKHASH
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[allow(missing_docs)]
pub struct EvmWitness {
    pub state: Vec<Vec<u8>>,
    pub codes: Vec<Vec<u8>>,
    pub keys: Vec<Vec<u8>>,
    pub headers: Vec<Vec<u8>>,
}
