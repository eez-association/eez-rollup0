//! Cross-chain call-hash and per-rollup state-root slot derivation.
//!
//! The common contract formula hashes the call kind, source pair, target pair,
//! value, and calldata. A mutable call leaving an L2 uses a distinct formula
//! that also includes the manager-entry `callGas`; see
//! [`l2_mutable_outbound_call_hash`].

use crate::RollupId;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::SolValue;

use crate::abi::ActionSol;

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
    pub data: &'a Bytes,
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

/// Compute the target-first six-field call hash.
///
/// `keccak256(abi.encode(targetRollupId, targetAddress, value, data,
/// sourceAddress, sourceRollupId))`.
///
/// This encoding has neither the call-kind discriminator used by the common
/// contract formula nor the `callGas` field used by mutable L2 outbound calls.
pub fn cross_chain_call_hash(
    target_rollup_id: RollupId,
    target_address: Address,
    value: U256,
    data: &Bytes,
    source_address: Address,
    source_rollup_id: RollupId,
) -> B256 {
    let action = ActionSol {
        targetRollupId: U256::from(target_rollup_id.0),
        targetAddress: target_address,
        value,
        data: data.to_vec().into(),
        sourceAddress: source_address,
        sourceRollupId: U256::from(source_rollup_id.0),
    };
    // `abi.encode(field1, field2, ...)` in Solidity uses the *params*
    // encoding (no wrapper-tuple offset). `SolValue::abi_encode` on a
    // sol!-generated struct emits the *standalone* encoding (32-byte
    // tuple offset prepended when the struct contains dynamic
    // members). Use `abi_encode_params` to match the on-chain
    // `abi.encode(...)` call byte-for-byte.
    keccak256(ActionSol::abi_encode_params(&action))
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
    use alloy_primitives::{address, b256};

    fn sample_args() -> (RollupId, Address, U256, Bytes, Address, RollupId) {
        (
            RollupId(1),
            address!("00000000000000000000000000000000000000aa"),
            U256::from(0),
            Bytes::from(vec![1u8, 2, 3]),
            address!("00000000000000000000000000000000000000bb"),
            RollupId(0),
        )
    }

    #[test]
    fn cross_chain_call_hash_deterministic() {
        let (a, b, c, ref d, e, f) = sample_args();
        let h1 = cross_chain_call_hash(a, b, c, d, e, f);
        let h2 = cross_chain_call_hash(a, b, c, d, e, f);
        assert_eq!(h1, h2);
    }

    #[test]
    fn cross_chain_call_hash_changes_with_target_rollup_id() {
        let (a, b, c, ref d, e, f) = sample_args();
        let h1 = cross_chain_call_hash(a, b, c, d, e, f);
        let h2 = cross_chain_call_hash(RollupId(2), b, c, d, e, f);
        assert_ne!(h1, h2);
    }

    #[test]
    fn cross_chain_call_hash_changes_with_target_address() {
        let (a, _, c, ref d, e, f) = sample_args();
        let b1 = address!("00000000000000000000000000000000000000aa");
        let b2 = address!("00000000000000000000000000000000000000ac");
        assert_ne!(
            cross_chain_call_hash(a, b1, c, d, e, f),
            cross_chain_call_hash(a, b2, c, d, e, f)
        );
    }

    #[test]
    fn cross_chain_call_hash_changes_with_value() {
        let (a, b, _, ref d, e, f) = sample_args();
        assert_ne!(
            cross_chain_call_hash(a, b, U256::ZERO, d, e, f),
            cross_chain_call_hash(a, b, U256::from(1u8), d, e, f)
        );
    }

    #[test]
    fn cross_chain_call_hash_changes_with_data() {
        let (a, b, c, _, e, f) = sample_args();
        let d1 = Bytes::from(vec![1u8, 2, 3]);
        let d2 = Bytes::from(vec![1u8, 2, 4]);
        assert_ne!(
            cross_chain_call_hash(a, b, c, &d1, e, f),
            cross_chain_call_hash(a, b, c, &d2, e, f)
        );
    }

    #[test]
    fn cross_chain_call_hash_changes_with_source_address() {
        let (a, b, c, ref d, _, f) = sample_args();
        let e1 = address!("00000000000000000000000000000000000000bb");
        let e2 = address!("00000000000000000000000000000000000000bc");
        assert_ne!(
            cross_chain_call_hash(a, b, c, d, e1, f),
            cross_chain_call_hash(a, b, c, d, e2, f)
        );
    }

    #[test]
    fn cross_chain_call_hash_changes_with_source_rollup_id() {
        let (a, b, c, ref d, e, _) = sample_args();
        assert_ne!(
            cross_chain_call_hash(a, b, c, d, e, RollupId(0)),
            cross_chain_call_hash(a, b, c, d, e, RollupId(7))
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
