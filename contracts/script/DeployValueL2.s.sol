// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {Value} from "../src/Value.sol";

/// @title DeployValueL2
/// @notice Deploys `Value(initial)` on the L2 dev chain. Used by the
///         cross-chain setter smoke as the target contract a `setValue`
///         cross-chain call lands on.
///
///         Outputs: EEZ_VALUE_ADDRESS=<addr>
///
///         Call shape:
///           forge script ... DeployValueL2
///             --sig "run(uint256)" $INITIAL
///             --rpc-url $L2_RPC --broadcast --private-key $PK
contract DeployValueL2 is Script {
    function run(uint256 initial) external {
        vm.startBroadcast();
        Value v = new Value(initial);
        vm.stopBroadcast();
        console.log("EEZ_VALUE_ADDRESS=%s", address(v));
    }
}
