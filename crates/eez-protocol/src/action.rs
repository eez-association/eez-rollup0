//! Cross-chain call-hash and per-rollup state-root slot derivation.
//!
//! The common contract formula hashes the call kind, source pair, target pair,
//! value, and calldata. A mutable call leaving an L2 uses a distinct formula
//! that also includes the manager-entry `callGas`; see
//! [`l2_mutable_outbound_call_hash`].

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
    /// Value encoded as the Solidity `isStatic` field.
    const fn is_static(self) -> bool {
        matches!(self, Self::Static)
    }
}

/// Fields shared by the protocol's cross-chain call-hash formulas.
///
/// Keeping the source and target names at the call site avoids silently
/// swapping the two address/rollup pairs.
#[derive(Clone, Copy, Debug)]
pub struct CallHashInput<'a> {
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

/// Compute the hash for a mutable call leaving an L2.
///
/// Unlike the common L1/inbound formula, `EEZL2` includes the manager-entry
/// `call_gas` value between `value` and `data`. The supported deployment uses
/// `USE_GAS_LEFT = false`, so production callers currently pass zero.
#[must_use]
pub fn l2_mutable_outbound_call_hash(input: CallHashInput<'_>, call_gas: u64) -> B256 {
    keccak256(
        (
            false,
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

/// Compute the protocol's gas-free cross-chain call hash.
///
/// Mirrors `EEZBase.computeCrossChainCallHash`:
/// `keccak256(abi.encode(isStatic, sourceAddress, uint64(sourceRollupId),
/// targetAddress, uint64(targetRollupId), value, data))`.
/// Mutable calls leaving an L2 use [`l2_mutable_outbound_call_hash`] instead.
#[must_use]
pub fn common_cross_chain_call_hash(mode: CallMode, input: CallHashInput<'_>) -> B256 {
    keccak256(
        (
            mode.is_static(),
            input.source_address,
            input.source_rollup_id.0,
            input.target_address,
            input.target_rollup_id.0,
            input.value,
            input.data,
        )
            .abi_encode_params(),
    )
}

/// Storage slot of `mapping(uint256 => RollupConfig) public rollups`
/// on `EEZ.sol` — slot 2, after `authorizedProxies` (0) and
/// `rollupCounter` (1). Verify with `forge inspect EEZ storage`.
const ROLLUPS_MAPPING_SLOT: u8 = 2;

/// Compute the Solidity storage slot for `rollups[rollupId].stateRoot`
/// on `EEZ.sol`.
///
/// `RollupConfig` shape under the multi-prover refactor
/// (`EEZ.sol:24-28`):
///
/// ```solidity
/// struct RollupConfig {
///     address rollupContract;   // +0
///     bytes32 stateRoot;        // +1
///     uint256 etherBalance;     // +2
/// }
/// ```
///
/// The pre-refactor `Rollups.sol` shape had 4 fields with `stateRoot`
/// at +2; the multi-prover refactor dropped `owner` + `verificationKey`
/// from the central registry (vkeys moved onto the per-rollup
/// `IRollupContract`), shifting `stateRoot` to +1.
#[must_use]
pub fn compute_state_root_slot(rollup_id: RollupId) -> B256 {
    let mut data = [0u8; 64];
    // Left-pad u64 to uint256 (bytes 0-23 zero, 24-31 the value)
    data[24..32].copy_from_slice(&rollup_id.0.to_be_bytes());
    data[63] = ROLLUPS_MAPPING_SLOT;
    let base = keccak256(data);
    // stateRoot is at offset +1 within RollupConfig under the
    // multi-prover layout.
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
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(7),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(1),
            value: U256::ZERO,
            data: &data,
        };

        assert_eq!(
            common_cross_chain_call_hash(CallMode::Mutable, input),
            b256!("0aea0f2282e747ca563ff59f9dbd36570e9973cfc007abfa51893d3fb9aaefdf")
        );
        assert_eq!(
            common_cross_chain_call_hash(CallMode::Static, input),
            b256!("a03958bfe3866dabc6d8e5466965bdfe5f0368308af0d2069801e1562bcd35d0")
        );
    }

    #[test]
    fn common_call_hash_matches_boundary_solidity_vector() {
        let data = Bytes::new();
        let input = CallHashInput {
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(u64::MAX),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(u64::MAX - 1),
            value: U256::MAX,
            data: &data,
        };

        assert_eq!(
            common_cross_chain_call_hash(CallMode::Mutable, input),
            b256!("f149543f591e628d8247387fdf6780d6aee8c119258a34b348509695c202a1a1")
        );
    }

    #[test]
    fn l2_mutable_outbound_hash_matches_solidity_vectors() {
        let data = Bytes::from_static(&[1, 2, 3]);
        let input = CallHashInput {
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(1),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(7),
            value: U256::from(1_000_000_000_000_000_000u128),
            data: &data,
        };

        assert_eq!(
            l2_mutable_outbound_call_hash(input, 0),
            b256!("9fd05cd7eebaf1d08b2961cb5d1237ef586cea58141270697a5509c6f3a03a37")
        );
        assert_eq!(
            l2_mutable_outbound_call_hash(input, 123_456),
            b256!("25400cdd749a1c3ac82f4e3093f0460afe21e718a545a96f9399b9ae486c99e4")
        );
    }

    #[test]
    fn l2_mutable_outbound_hash_matches_boundary_solidity_vector() {
        let data = Bytes::new();
        let input = CallHashInput {
            source_address: address!("00000000000000000000000000000000000000bb"),
            source_rollup_id: RollupId(u64::MAX),
            target_address: address!("00000000000000000000000000000000000000aa"),
            target_rollup_id: RollupId(u64::MAX - 1),
            value: U256::MAX,
            data: &data,
        };

        assert_eq!(
            l2_mutable_outbound_call_hash(input, u64::MAX),
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
