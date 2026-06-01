//! Storage layout for the `authorizedProxies` mapping.
//!
//! Two EVM contracts hold this mapping:
//!
//! - **L1 `EEZ.sol`** — `authorizedProxies` at slot
//!   [`ROLLUPS_AUTHORIZED_PROXIES_SLOT`] (3). Source side for L1→L2
//!   composition. Storage layout per upstream `EEZ.sol:111-129`:
//!   `rollupCounter` (0), `rollups` (1), `verificationByRollup` (2),
//!   **`authorizedProxies` (3)**. The pre-multi-prover `Rollups.sol`
//!   layout had `authorizedProxies` at slot 5 because of two extra
//!   global tables (`executions`, `staticCalls`, `executionIndex`)
//!   that the per-rollup queue model replaced with the
//!   `verificationByRollup` mapping.
//! - **L2 `CrossChainManagerL2.sol`** — `authorizedProxies` at slot
//!   [`CCM_AUTHORIZED_PROXIES_SLOT`] (2). Source side for L2→L1
//!   composition. Storage layout per upstream
//!   `CrossChainManagerL2.sol:26-32`: `executions` (0),
//!   `lookupCalls` (1), **`authorizedProxies` (2)**, `lastLoadBlock`
//!   (3), `executionIndex` (4). `ROLLUP_ID` and `SYSTEM_ADDRESS` are
//!   immutables (no storage slot).
//!
//! The slot is a compile-time property of the contract's storage
//! layout. If the Solidity declaration order changes, update the
//! constant; the E2E byte-equality check against the reference
//! protocol catches drift loudly.
//!
//! Both constants are consumed at config build time as the
//! `authorized_proxies_slot: u8` field on
//! [`eez_protocol::ProxyLookupConfig`]. The inspector reads
//! [`proxy_mapping_key`] / [`decode_proxy_value`] directly via the
//! `u8` slot.

use alloy_primitives::{keccak256, Address, B256, U256};
use eez_protocol::RollupId;

/// Storage slot of `authorizedProxies` on `EEZ.sol` (L1).
///
/// Used by the entry-rollup proxy-lookup configuration. Matches the
/// position of the `mapping(address => ProxyInfo) authorizedProxies`
/// declaration in the contract — slot 3 under the multi-prover
/// layout (was 5 under the pre-multi-prover `Rollups.sol`).
pub const ROLLUPS_AUTHORIZED_PROXIES_SLOT: u8 = 3;

/// Storage slot of `authorizedProxies` on `CrossChainManagerL2.sol` (L2).
///
/// Used by the follower-rollup proxy-lookup configuration. Matches
/// the position of the `mapping(address => ProxyInfo) authorizedProxies`
/// declaration in the contract.
pub const CCM_AUTHORIZED_PROXIES_SLOT: u8 = 2;

/// Information about a registered cross-chain proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyInfo {
    /// The real destination address on the target rollup.
    pub original_address: Address,
    /// The rollup ID of the target chain.
    pub original_rollup_id: RollupId,
}

/// Compute the Solidity storage key for `authorizedProxies[addr]`.
///
/// Layout: `mapping(address => packed ProxyInfo)` at storage `slot`.
/// Key = `keccak256(abi.encodePacked(address, slot))` where both are
/// left-padded to 32 bytes.
///
/// ```text
/// [0..12]  = zero padding (address is 20 bytes)
/// [12..32] = the proxy address
/// [32..63] = zero padding (slot is u8)
/// [63]     = the storage slot number
/// ```
#[must_use]
pub fn proxy_mapping_key(addr: Address, slot: u8) -> B256 {
    let mut data = [0u8; 64];
    data[12..32].copy_from_slice(addr.as_ref());
    data[63] = slot;
    keccak256(data)
}

/// Decode a packed `uint256` storage word into [`ProxyInfo`].
///
/// Returns `None` if `value` is zero — Solidity's default for an
/// unset mapping entry, i.e. "this address is not a registered proxy".
///
/// The Solidity contract packs proxy info as:
/// ```text
/// bytes [0..4]   — unused (padding)
/// bytes [4..12]  — original_rollup_id (u64, big-endian)
/// bytes [12..32] — original_address   (20 bytes)
/// ```
#[must_use]
pub fn decode_proxy_value(value: U256) -> Option<ProxyInfo> {
    if value == U256::ZERO {
        return None;
    }
    let bytes = value.to_be_bytes::<32>();
    let original_address = Address::from_slice(&bytes[12..32]);
    let mut rollup_bytes = [0u8; 8];
    rollup_bytes.copy_from_slice(&bytes[4..12]);
    let original_rollup_id = RollupId(u64::from_be_bytes(rollup_bytes));
    Some(ProxyInfo {
        original_address,
        original_rollup_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_constants() {
        // Compile-time constants from the contracts' storage layout.
        // Any change here must be paired with a matching change in the
        // Solidity declaration order.
        assert_eq!(ROLLUPS_AUTHORIZED_PROXIES_SLOT, 3);
        assert_eq!(CCM_AUTHORIZED_PROXIES_SLOT, 2);
    }

    #[test]
    fn decode_zero_returns_none() {
        // Solidity's default for an unread mapping entry is zero.
        // `None` = "not a registered proxy" vs an address literally at 0x0.
        assert!(decode_proxy_value(U256::ZERO).is_none());
    }

    #[test]
    fn decode_non_zero_round_trip() {
        let addr = Address::from([0xAB; 20]);
        let rollup_id: u64 = 42;

        // Pack: bytes[4..12] = rollup_id, bytes[12..32] = address
        let mut packed = [0u8; 32];
        packed[4..12].copy_from_slice(&rollup_id.to_be_bytes());
        packed[12..32].copy_from_slice(addr.as_ref());

        let info = decode_proxy_value(U256::from_be_bytes(packed))
            .expect("non-zero packed value decodes to Some");
        assert_eq!(info.original_address, addr);
        assert_eq!(info.original_rollup_id, RollupId(rollup_id));
    }

    #[test]
    fn mapping_key_deterministic() {
        let addr = Address::from([0x11; 20]);

        let k1 = proxy_mapping_key(addr, 3);
        let k2 = proxy_mapping_key(addr, 3);
        assert_eq!(k1, k2, "same input -> same key");

        let k3 = proxy_mapping_key(addr, 1);
        assert_ne!(k1, k3, "different slot -> different key");
    }

    #[test]
    fn mapping_key_matches_named_slots() {
        // Guards against a refactor that changes one source of truth
        // without the other: the named slot constants and the raw-u8
        // helper must agree.
        let addr = Address::from([0xCD; 20]);

        assert_eq!(
            proxy_mapping_key(addr, ROLLUPS_AUTHORIZED_PROXIES_SLOT),
            proxy_mapping_key(addr, 3),
        );
        assert_eq!(
            proxy_mapping_key(addr, CCM_AUTHORIZED_PROXIES_SLOT),
            proxy_mapping_key(addr, 2),
        );
    }
}
