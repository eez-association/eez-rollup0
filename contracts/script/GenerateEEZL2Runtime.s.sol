// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZL2} from "sync-rollups-protocol/src/L2/EEZL2.sol";

/// @notice Prints constructor-patched EEZL2 runtime bytecode for a genesis predeploy.
contract GenerateEEZL2Runtime is Script {
    function run(uint64 rollupId, address systemAddress, bool useGasLeft) external {
        EEZL2 eezL2 = new EEZL2(rollupId, systemAddress, useGasLeft);
        console.logBytes(address(eezL2).code);
    }
}
