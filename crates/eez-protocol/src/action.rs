//! Cross-chain call-hash and per-rollup state-root slot derivation.
//!
//! The protocol formula hashes the call kind, source pair, target pair, value,
//! call gas, and calldata. Most paths commit zero call gas; calls leaving an
//! L2 may instead commit the manager-entry `callGas`.

use crate::RollupId;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::SolValue;

/// Execution mode committed by the common cross-chain call hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallMode {
    /// Normal call, where state changes are permitted.
    Mutable,
    /// Static context, where the EVM enforces read-only execution.
    Static,
}

impl CallMode {
    /// Convert the Solidity `isStatic` field into the typed execution mode.
    pub const fn from_is_static(is_static: bool) -> Self {
        if is_static {
            Self::Static
        } else {
            Self::Mutable
        }
    }

    /// Value encoded as the Solidity `isStatic` field.
    const fn is_static(self) -> bool {
        matches!(self, Self::Static)
    }
}

/// Fields encoded by the protocol's cross-chain call-hash formula.
///
/// Keeping the source and target names at the call site avoids silently
/// swapping the two address/rollup pairs.
#[derive(Clone, Copy, Debug)]
pub struct CallHashInput<'a> {
    /// Whether execution may modify destination-chain state.
    pub call_mode: CallMode,
    /// Address that originated the call on the source chain.
    pub source_address: Address,
    /// Rollup containing `source_address` (`0` denotes L1).
    pub source_rollup_id: RollupId,
    /// Address invoked on the destination chain.
    pub target_address: Address,
    /// Rollup containing `target_address` (`0` denotes L1).
    pub target_rollup_id: RollupId,
    /// Ether transferred by the cross-chain call.
    pub value: U256,
    /// Calldata sent to `target_address`.
    pub data: &'a [u8],
}

/// Compute the hash for a call leaving an L2.
///
/// `EEZL2` supplies the manager-entry `call_gas` value to the shared protocol
/// formula. The supported deployment uses `USE_GAS_LEFT = false`, so
/// production callers currently pass zero.
#[must_use]
pub fn l2_outbound_call_hash(input: CallHashInput<'_>, call_gas: u64) -> B256 {
    cross_chain_call_hash(input, call_gas)
}

/// Compute the protocol's cross-chain call hash with zero call gas.
///
/// Mirrors `EEZBase.computeCrossChainCallHash` with `callGas == 0`:
/// `keccak256(abi.encode(isStatic, sourceAddress, uint64(sourceRollupId),
/// targetAddress, uint64(targetRollupId), value, uint64(0), data))`.
/// Calls leaving an L2 use [`l2_outbound_call_hash`] to supply the observed
/// manager-entry gas when required.
#[must_use]
pub fn common_cross_chain_call_hash(input: CallHashInput<'_>) -> B256 {
    cross_chain_call_hash(input, 0)
}

fn cross_chain_call_hash(input: CallHashInput<'_>, call_gas: u64) -> B256 {
    keccak256(
        (
            input.call_mode.is_static(),
            input.source_address,
            input.source_rollup_id.0,
            input.target_address,
            input.target_rollup_id.0,
            input.value,
            call_gas,
            input.data,
        )
            .abi_encode_params(),
    )
}

/// Storage slot of `mapping(uint64 => RollupConfig) public rollups`
/// on `EEZ.sol` — slot 2, after `authorizedProxies` (0) and
/// `rollupCounter` (1). Verify with `forge inspect EEZ storage`.
const ROLLUPS_MAPPING_SLOT: u8 = 2;

/// Compute the Solidity storage slot for `rollups[rollupId].stateRoot`
/// on `EEZ.sol`.
///
/// The current `RollupConfig` layout is:
///
/// ```solidity
/// struct RollupConfig {
///     address rollupContract;   // +0
///     bytes32 stateRoot;        // +1
///     uint256 etherBalance;     // +2
/// }
/// ```
#[must_use]
pub fn compute_state_root_slot(rollup_id: RollupId) -> B256 {
    let mut data = [0u8; 64];
    // Solidity mapping keys occupy one 32-byte word.
    data[24..32].copy_from_slice(&rollup_id.0.to_be_bytes());
    data[63] = ROLLUPS_MAPPING_SLOT;
    let base = keccak256(data);
    // `stateRoot` is the second word in `RollupConfig`.
    B256::from(U256::from_be_bytes(base.0) + U256::from(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Bytes, address, b256};

    #[test]
    fn common_call_hash_matches_solidity_vectors() {
        let data = Bytes::from_static(&[1, 2, 3]);
        let input = CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(7),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(1),
            value: U256::ZERO,
            data: &data,
        };

        assert_eq!(
            common_cross_chain_call_hash(input),
            b256!("16b1575ff5a4ec44167aebf047dd46f77db3766f7481445ad09c8136bff735a8")
        );
        assert_eq!(
            common_cross_chain_call_hash(CallHashInput {
                call_mode: CallMode::Static,
                ..input
            }),
            b256!("4cf0f2738ced4dcd497cf8a081030f41c5dc588fbdcac75f3a217e979d19abe7")
        );
    }

    #[test]
    fn common_call_hash_matches_boundary_solidity_vector() {
        let data = Bytes::new();
        let input = CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(u64::MAX),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(u64::MAX - 1),
            value: U256::MAX,
            data: &data,
        };

        assert_eq!(
            common_cross_chain_call_hash(input),
            b256!("414b9d6bf91a3e266bcd34ddd870a53332107a606b6eda618455f9f940291e2b")
        );
    }

    #[test]
    fn l2_outbound_hash_matches_solidity_vectors() {
        let data = Bytes::from_static(&[1, 2, 3]);
        let input = CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(1),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(7),
            value: U256::from(1_000_000_000_000_000_000u128),
            data: &data,
        };

        assert_eq!(
            l2_outbound_call_hash(input, 0),
            b256!("9fd05cd7eebaf1d08b2961cb5d1237ef586cea58141270697a5509c6f3a03a37")
        );
        assert_eq!(
            l2_outbound_call_hash(input, 123_456),
            b256!("25400cdd749a1c3ac82f4e3093f0460afe21e718a545a96f9399b9ae486c99e4")
        );
        assert_eq!(
            l2_outbound_call_hash(
                CallHashInput {
                    call_mode: CallMode::Static,
                    ..input
                },
                0,
            ),
            b256!("a5aeac7d89f6ef62251b7ab3a1645a75f30a11d9f15627ea3885ae49dd0940d3")
        );
    }

    #[test]
    fn l2_outbound_hash_matches_boundary_solidity_vector() {
        let data = Bytes::new();
        let input = CallHashInput {
            call_mode: CallMode::Mutable,
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(u64::MAX),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(u64::MAX - 1),
            value: U256::MAX,
            data: &data,
        };

        assert_eq!(
            l2_outbound_call_hash(input, u64::MAX),
            b256!("7f04915c437db6536fe9d746b135ed834b391532e4be8beadd898ad1f592895f")
        );
    }

    #[test]
    fn state_root_slot_known_value() {
        let slot = compute_state_root_slot(RollupId(1));
        assert_ne!(slot, B256::ZERO);
        assert_eq!(slot, compute_state_root_slot(RollupId(1)));
        assert_ne!(
            compute_state_root_slot(RollupId(1)),
            compute_state_root_slot(RollupId(2))
        );
    }

    #[test]
    fn state_root_slot_known_value_for_rollup_one() {
        // Hard-coded oracle: `keccak256(abi.encode(uint256(1),
        // uint256(2))) + 1` — the slot of `rollups[1].stateRoot`
        // (`rollups` mapping at slot 2, `stateRoot` at +1). Computed
        // offline via `cast keccak` so this is an independent witness,
        // not a re-derivation of the function's own formula; fails
        // loudly if the mapping slot or `RollupConfig` shape moves.
        let slot = compute_state_root_slot(RollupId(1));
        let expected: B256 = "0xe90b7bceb6e7df5418fb78d8ee546e97c83a08bbccc01a0644d599ccd2a7c2e1"
            .parse()
            .expect("hex");
        assert_eq!(slot, expected, "slot {slot} != {expected}");
    }
}
