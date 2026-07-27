//! Cross-chain call hash + per-rollup state-root slot derivation.
//!
//! # Cross-chain call hash (invariant 5)
//!
//! In the multi-prover protocol, every cross-chain call is identified
//! by a 6-field hash:
//!
//! ```text
//! crossChainCallHash = keccak256(abi.encode(
//!     targetRollupId,    // uint256
//!     targetAddress,     // address (20 bytes)
//!     value,             // uint256
//!     data,              // bytes
//!     sourceAddress,     // address
//!     sourceRollupId,    // uint256
//! ))
//! ```
//!
//! Tree position (caller's call index, expected-call index, parent
//! context) is folded into the entry-level rolling hash, not encoded
//! into the call hash itself. Reverts are tracked via `revertSpan`
//! on the [`crate::abi::L2ToL1CallSol`] slot.
//!
//! On-chain mirror: `EEZ.computeCrossChainCallHash` (public pure) at
//! `sync-rollups-protocol/src/EEZ.sol:1243`; same byte semantics in
//! `CrossChainManagerL2.computeCrossChainCallHash` at
//! `sync-rollups-protocol/src/CrossChainManagerL2.sol:532`.
//!
//! **Asymmetric on-chain field naming** (upstream rename caveat —
//! captured in invariant 5): the same 32-byte hash appears as
//! `ExecutionEntry.proxyEntryHash` (struct field) on entries and as
//! `LookupCall.crossChainCallHash` on lookup calls. Values are
//! byte-identical; only the struct-field names diverge.

use crate::RollupId;
use alloy_primitives::{Address, B256, Bytes, U256, keccak256};
use alloy_sol_types::SolValue;

use crate::abi::ActionSol;

/// Compute the 6-field cross-chain call hash (invariant 5).
///
/// `keccak256(abi.encode(targetRollupId, targetAddress, value, data,
/// sourceAddress, sourceRollupId))`. Byte-for-byte identical to
/// `EEZ.computeCrossChainCallHash` on L1 and
/// `CrossChainManagerL2.computeCrossChainCallHash` on L2. The hash
/// inputs are unchanged from the prior protocol; only the function
/// name rotated. The Rust function rename here mirrors the on-chain
/// rename.
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
    use alloy_primitives::address;

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
