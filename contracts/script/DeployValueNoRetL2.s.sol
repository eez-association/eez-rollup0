// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {ValueNoRet} from "../src/ValueNoRet.sol";

/// @title DeployValueNoRetL2
/// @notice Deploys `ValueNoRet(initial)` on the L2 dev chain — a setter
///         whose `setValue` returns NOTHING (empty returnData). The
///         cross-chain contract-call control: exercises the
///         empty-returnData branch of the entry / rolling-hash path
///         alongside the return-bearing `Value`.
///
///         Outputs: EEZ_VALUE_NORET_ADDRESS=<addr>
///
///         Call shape:
///           forge script ... DeployValueNoRetL2
///             --sig "run(uint256)" $INITIAL
///             --rpc-url $L2_RPC --broadcast --private-key $PK
contract DeployValueNoRetL2 is Script {
    function run(uint256 initial) external {
        vm.startBroadcast();
        ValueNoRet v = new ValueNoRet(initial);
        vm.stopBroadcast();
        console.log("EEZ_VALUE_NORET_ADDRESS=%s", address(v));
    }
}
