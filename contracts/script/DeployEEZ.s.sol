// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";

/// @title DeployEEZ
/// @notice Deploys the central `EEZ.sol` registry on L1 with the address that
///         receives ether recovered from pre-funded cross-chain proxies.
///
///         Outputs: EEZ=<address>
///
///         Call shape:
///           forge script ... DeployEEZ
///             --sig "run(address)" $RECOVERY_ADDRESS
///             --private-key $PK
///
///         The private key must be funded on L1.
///         Default deployment via `forge script ... --broadcast`.
contract DeployEEZ is Script {
    function run(address recoveryAddress) external {
        vm.startBroadcast();
        EEZ eez = new EEZ(recoveryAddress);
        vm.stopBroadcast();
        console.log("EEZ=%s", address(eez));
    }
}
