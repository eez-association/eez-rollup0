// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";

/// @title DeployEEZ
/// @notice Deploys the central `EEZ.sol` registry on L1. Stateless deploy —
///         no constructor args; `rollupCounter` starts at 0 and each
///         `createRollup` bumps it.
///
///         Outputs: EEZ=<address>
///
///         Caller-supplied: `--private-key $PK` (must be funded on L1).
///         Default deployment via `forge script ... --broadcast`.
contract DeployEEZ is Script {
    function run() external {
        vm.startBroadcast();
        EEZ eez = new EEZ();
        vm.stopBroadcast();
        console.log("EEZ=%s", address(eez));
    }
}
