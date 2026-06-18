// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {SetterWrapper} from "../src/SetterWrapper.sol";

/// @title DeploySetterWrapperL1
/// @notice Deploys `SetterWrapper(proxy)` on L1 — an L1 contract that
///         calls the setter `CrossChainProxy` internally, decodes the
///         `(changed, newValue)` the composer synthesizes back, and emits
///         it as `Wrapped`. This is the cross-chain RETURN-VALUE path: the
///         L2 `Value.setValue` return tuple surfaces in an L1 log.
///
///         Outputs: EEZ_SETTER_WRAPPER=<addr>
///
///         Call shape:
///           forge script ... DeploySetterWrapperL1
///             --sig "run(address)" $SETTER_PROXY
///             --rpc-url $L1_RPC --broadcast --private-key $PK
contract DeploySetterWrapperL1 is Script {
    function run(address proxy) external {
        vm.startBroadcast();
        SetterWrapper w = new SetterWrapper(proxy);
        vm.stopBroadcast();
        console.log("EEZ_SETTER_WRAPPER=%s", address(w));
    }
}
