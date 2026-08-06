//! Storage layout for the `authorizedProxies` mapping.
//!
//! Two EVM contracts hold this mapping. Both inherit `authorizedProxies`
//! from the shared abstract parent `EEZBase`, which declares it as its
//! first (and only) non-transient storage slot — so the slot number is
//! **0** on both children:
//!
//! - **L1 `EEZ.sol`** — slot [`EEZ_AUTHORIZED_PROXIES_SLOT`] (0).
//!   Source side for L1→L2 composition. The persistent L1 layout begins:
//!   `authorizedProxies` (0, from `EEZBase`), `rollupCounter` (1),
//!   `rollups` (2), and `verificationByRollup` (3).
//! - **L2 `EEZL2.sol`** — slot [`EEZL2_AUTHORIZED_PROXIES_SLOT`] (0).
//!   Source side for L2→L1 composition. The persistent L2 layout is:
//!   `authorizedProxies` (0, from `EEZBase`), `entries` (1),
//!   `staticEntries` (2), `lastLoadBlock` (3), `entryIndex` (4).
//!   `ROLLUP_ID` and `SYSTEM_ADDRESS` are immutables (no storage slot).
//!
//! The two constants are equal (both 0) but kept distinct to document
//! intent at call sites and to leave room for re-divergence if upstream
//! ever moves either mapping off `EEZBase`.
//!
//! The slot is a compile-time property of the contract's storage
//! layout. If the upstream Solidity declaration order changes, update
//! the constant.
//!
//! Both constants are consumed at config build time as the
//! `authorized_proxies_slot: u8` field on
//! [`crate::ProxyLookupConfig`]. The inspector reads
//! [`proxy_mapping_key`] / [`decode_proxy_value`] directly via the
//! `u8` slot.

use crate::RollupId;
use alloy_primitives::{Address, B256, U256, keccak256};

/// Storage slot of `authorizedProxies` on `EEZ.sol` (L1).
///
/// Used by the entry-rollup proxy-lookup configuration. The mapping
/// is declared on the abstract parent `EEZBase` as its first
/// non-transient storage variable, so the slot is 0 on every
/// `EEZBase` subclass — including `EEZ` (L1).
pub const EEZ_AUTHORIZED_PROXIES_SLOT: u8 = 0;

/// Storage slot of `authorizedProxies` on `EEZL2.sol` (L2).
///
/// Used by the follower-rollup proxy-lookup configuration. Same
/// reasoning as [`EEZ_AUTHORIZED_PROXIES_SLOT`] — inherited from
/// `EEZBase` at slot 0.
pub const EEZL2_AUTHORIZED_PROXIES_SLOT: u8 = 0;

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
/// Key = `keccak256(abi.encode(address, uint256(slot)))`, where both values
/// occupy one left-padded 32-byte ABI word.
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
/// Returns `None` unless the packed `isProxy` flag is the canonical
/// Solidity `true` value. The other fields are not evidence of registration.
///
/// The Solidity contract packs proxy info as:
/// ```text
/// bytes [0..3]   — unused (padding)
/// bytes [3..11]  — original_rollup_id (u64, big-endian)
/// bytes [11..31] — original_address   (20 bytes)
/// byte  [31]     — is_proxy           (`1` when registered)
/// ```
#[must_use]
pub fn decode_proxy_value(value: U256) -> Option<ProxyInfo> {
    let bytes = value.to_be_bytes::<32>();
    if bytes[31] != 1 {
        return None;
    }
    let original_address = Address::from_slice(&bytes[11..31]);
    let mut rollup_bytes = [0u8; 8];
    rollup_bytes.copy_from_slice(&bytes[3..11]);
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
        // upstream Solidity declaration order.
        assert_eq!(EEZ_AUTHORIZED_PROXIES_SLOT, 0);
        assert_eq!(EEZL2_AUTHORIZED_PROXIES_SLOT, 0);
    }

    #[test]
    fn decode_zero_returns_none() {
        assert!(decode_proxy_value(U256::ZERO).is_none());
    }

    #[test]
    fn decode_rejects_fields_without_registration_flag() {
        let mut packed = [0u8; 32];
        packed[3..11].copy_from_slice(&42u64.to_be_bytes());
        packed[11..31].fill(0xab);

        assert!(decode_proxy_value(U256::from_be_bytes(packed)).is_none());
    }

    #[test]
    fn decode_matches_solidity_storage_layout_vector() {
        // `forge inspect EEZ storage-layout`: `isProxy` offset 0,
        // `originalAddress` offset 1, `originalRollupId` offset 21.
        let packed = "0x000000010203040506070811223344556677889900aabbccddeeff0011223301"
            .parse::<U256>()
            .expect("valid storage word");

        let info = decode_proxy_value(packed).expect("registered proxy");
        assert_eq!(
            info.original_address,
            "0x11223344556677889900aabbccddeeff00112233"
                .parse::<Address>()
                .expect("valid address")
        );
        assert_eq!(info.original_rollup_id, RollupId(0x0102_0304_0506_0708));
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
            proxy_mapping_key(addr, EEZ_AUTHORIZED_PROXIES_SLOT),
            proxy_mapping_key(addr, 0),
        );
        assert_eq!(
            proxy_mapping_key(addr, EEZL2_AUTHORIZED_PROXIES_SLOT),
            proxy_mapping_key(addr, 0),
        );
    }
}
