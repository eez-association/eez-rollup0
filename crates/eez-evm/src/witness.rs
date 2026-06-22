//! EVM state witness — for stateless execution replay by the zisk prover.
//!
//! In Phase 2 (Steps 17-19), this will use `alloy_rpc_types_debug::ExecutionWitness`
//! which is the format consumed by zisk-eth-client. For now, a placeholder
//! that satisfies the Serialize + `DeserializeOwned` bounds.

use serde::{Deserialize, Serialize};

/// Placeholder EVM witness. Will be replaced by a newtype around
/// `alloy_rpc_types_debug::ExecutionWitness` in Phase 2 when
/// `WitnessRecordingDB` is implemented.
///
/// The real witness contains:
/// - `state`: hashed trie node preimages
/// - `codes`: contract bytecodes accessed during execution
/// - `keys`: account/storage key preimages
/// - `headers`: RLP-encoded block headers for BLOCKHASH
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EvmWitness {
    pub state: Vec<Vec<u8>>,
    pub codes: Vec<Vec<u8>>,
    pub keys: Vec<Vec<u8>>,
    pub headers: Vec<Vec<u8>>,
}
