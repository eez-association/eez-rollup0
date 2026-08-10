//! EVM state overlay — accumulated state changes equivalent to reth's `BundleState`.
//!
//! Currently carried as an empty default
//! on checkpoints — gRPC Execute is a probe; the upstream
//! overlay-continuation (server-side state reconstruction) was not
//! vendored. Canonical ordering: accounts by address, storage by key.

use alloy_primitives::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};

/// Accumulated EVM state changes.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EvmOverlay {
    /// Account changes, sorted by address (canonical ordering).
    pub accounts: Vec<AccountOverlay>,
    /// Contract bytecode deployed during execution.
    pub contracts: Vec<ContractCode>,
}

/// Single account's state change.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountOverlay {
    /// Address whose account state changed.
    pub address: Address,
    /// Account lifecycle status (mirrors revm's `AccountStatus`).
    pub status: AccountStatus,
    /// Current account info (after calls). None = doesn't exist.
    pub info: Option<AccountInfo>,
    /// Original account info (before calls). None = didn't exist.
    pub original_info: Option<AccountInfo>,
    /// Derived from `status.was_destroyed()`. Explicit for serialization clarity.
    /// Validation: must be consistent with status.
    pub wipe_storage: bool,
    /// Storage slot changes, sorted by key (canonical ordering).
    pub storage: Vec<StorageOverlay>,
}

/// Account lifecycle — mirrors revm's `AccountStatus` with pinned discriminants.
/// Determines how the state DB interprets missing storage slots.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum AccountStatus {
    LoadedNotExisting = 0,
    Loaded = 1,
    LoadedEmptyEIP161 = 2,
    InMemoryChange = 3,
    Changed = 4,
    Destroyed = 5,
    DestroyedChanged = 6,
    DestroyedAgain = 7,
}

impl AccountStatus {
    /// `true` if the account was destroyed during execution.
    pub fn was_destroyed(self) -> bool {
        matches!(
            self,
            Self::Destroyed | Self::DestroyedChanged | Self::DestroyedAgain
        )
    }
}

/// Account balance, nonce, and code hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountInfo {
    /// Account balance in wei.
    pub balance: U256,
    /// Account nonce.
    pub nonce: u64,
    /// Hash of the account's deployed bytecode.
    pub code_hash: B256,
}

/// Storage slot with original and present values.
/// Both needed for correct state root computation and rollback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageOverlay {
    /// The storage slot key.
    pub key: U256,
    /// Value before any calls (or before wipe).
    pub previous_or_original: U256,
    /// Value after the latest call.
    pub present: U256,
}

/// Contract bytecode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContractCode {
    /// Hash of the bytecode.
    pub code_hash: B256,
    /// Raw bytecode bytes.
    pub code: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_status_all_variants_roundtrip() {
        let statuses = [
            AccountStatus::LoadedNotExisting,
            AccountStatus::Loaded,
            AccountStatus::LoadedEmptyEIP161,
            AccountStatus::InMemoryChange,
            AccountStatus::Changed,
            AccountStatus::Destroyed,
            AccountStatus::DestroyedChanged,
            AccountStatus::DestroyedAgain,
        ];
        for s in statuses {
            let json = serde_json::to_string(&s).unwrap();
            let back: AccountStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn was_destroyed_consistency() {
        assert!(!AccountStatus::Loaded.was_destroyed());
        assert!(!AccountStatus::Changed.was_destroyed());
        assert!(AccountStatus::Destroyed.was_destroyed());
        assert!(AccountStatus::DestroyedChanged.was_destroyed());
        assert!(AccountStatus::DestroyedAgain.was_destroyed());
    }

    #[test]
    fn overlay_serde_roundtrip() {
        let overlay = EvmOverlay {
            accounts: vec![AccountOverlay {
                address: Address::ZERO,
                status: AccountStatus::Changed,
                info: Some(AccountInfo {
                    balance: U256::from(100),
                    nonce: 1,
                    code_hash: B256::ZERO,
                }),
                original_info: Some(AccountInfo {
                    balance: U256::from(50),
                    nonce: 0,
                    code_hash: B256::ZERO,
                }),
                wipe_storage: false,
                storage: vec![StorageOverlay {
                    key: U256::from(1),
                    previous_or_original: U256::ZERO,
                    present: U256::from(42),
                }],
            }],
            contracts: vec![],
        };
        let json = serde_json::to_string(&overlay).unwrap();
        let back: EvmOverlay = serde_json::from_str(&json).unwrap();
        assert_eq!(overlay, back);
    }
}
