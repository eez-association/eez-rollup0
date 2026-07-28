//! Storage layout for the `authorizedProxies` mapping.

use crate::RollupId;
use alloy_primitives::{Address, B256, U256, keccak256};

/// Storage slot of `authorizedProxies` on `EEZ.sol` (L1).
///
/// Used by the entry-rollup proxy-lookup configuration. The mapping
/// is declared on the abstract parent `EEZBase` as its first
/// non-transient storage variable, so the slot is 0 on every
/// `EEZBase` subclass — including `EEZ` (L1).
pub const ROLLUPS_AUTHORIZED_PROXIES_SLOT: u8 = 0;

/// Storage slot of `authorizedProxies` on `EEZL2.sol` (L2).
///
/// Used by the follower-rollup proxy-lookup configuration. Same
/// reasoning as [`ROLLUPS_AUTHORIZED_PROXIES_SLOT`] — inherited from
/// `EEZBase` at slot 0.
pub const CCM_AUTHORIZED_PROXIES_SLOT: u8 = 0;

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

/// Combined proxy-lookup configuration for a registered rollup.
///
/// Bundles the storage-contract address and the storage slot index
/// where that contract holds its `authorizedProxies` mapping.
///
/// Constructed at `main.rs` startup from the rollup's role:
/// - L1-style client (entry-as-L1 or follower-as-L1):
///   `contract_address = rollups_address`,
///   `authorized_proxies_slot = 0` (`EEZ.authorizedProxies` —
///   inherited from `EEZBase` at slot 0).
/// - L2-style client:
///   `contract_address = ccm_address`,
///   `authorized_proxies_slot = 0`
///   (`EEZL2.authorizedProxies` — inherited from `EEZBase`
///   at slot 0).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyLookupConfig {
    /// Address of the contract holding `authorizedProxies` on this chain.
    pub contract_address: Address,
    /// Storage slot index where `authorizedProxies` lives on
    /// `contract_address`. The inspector reads
    /// `keccak256(addr ++ slot)` to find a registered proxy.
    pub authorized_proxies_slot: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_constants() {
        // Compile-time constants from the contracts' storage layout.
        // Any change here must be paired with a matching change in the
        // upstream Solidity declaration order.
        assert_eq!(ROLLUPS_AUTHORIZED_PROXIES_SLOT, 0);
        assert_eq!(CCM_AUTHORIZED_PROXIES_SLOT, 0);
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
            proxy_mapping_key(addr, 0),
        );
        assert_eq!(
            proxy_mapping_key(addr, CCM_AUTHORIZED_PROXIES_SLOT),
            proxy_mapping_key(addr, 0),
        );
    }
}
