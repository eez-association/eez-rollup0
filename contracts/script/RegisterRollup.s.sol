// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZ} from "eez-core-protocol/src/EEZ.sol";

/// @title RegisterRollup
/// @notice Calls `EEZ.registerRollup(rollupContract, initialState)`. The
///         registry assigns the next sequential `rollupId` (starting at
///         1 — `MAINNET_ROLLUP_ID = 0` is reserved for L1 self), emits
///         `RollupCreated(rollupId, rollupContract, initialState)`, and
///         calls back `Rollup.rollupContractRegistered(rollupId)` to
///         wire the new id into the manager.
///
///         Outputs: L2_ROLLUP_ID=<id>
///
///         Call shape:
///           forge script ... RegisterRollup
///             --sig "run(address,address,bytes32)" $EEZ $ROLLUP_CONTRACT $INITIAL_STATE
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         Args:
///           - eez            — central registry address.
///           - rollupContract — the per-rollup manager deployed by
///                              DeployRollup.
///           - initialState   — genesis state root for the new rollup
///                              (typically `bytes32(0)` for a dev L2
///                              that starts from an empty trie, or the
///                              L2 genesis state root if known).
contract RegisterRollup is Script {
    function run(address eez, address rollupContract, bytes32 initialState) external {
        vm.startBroadcast();
        uint64 rollupId = EEZ(eez).registerRollup(rollupContract, initialState);
        vm.stopBroadcast();
        console.log("L2_ROLLUP_ID=%s", rollupId);
    }
}
