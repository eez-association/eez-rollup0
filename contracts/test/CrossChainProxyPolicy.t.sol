// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZL2} from "eez-core-protocol/src/L2/EEZL2.sol";
import {EEZBase} from "eez-core-protocol/src/base/EEZBase.sol";

/// @notice Calls a proxy's runtime via DELEGATECALL rather than CALL.
contract DelegateProxyCaller {
    function invoke(address proxy, bytes calldata data) external returns (bool ok, bytes memory result) {
        (ok, result) = proxy.delegatecall(data);
    }
}

/// @notice Reject-policy scenarios that do not alter protocol behavior.
contract CrossChainProxyPolicyTest is Test {
    function testDelegatecallToProxyIsRejectedBeforeDestinationExecution() external {
        EEZL2 manager = new EEZL2(1, address(0xBEEF), false);
        address proxy = manager.createCrossChainProxy(address(0xCAFE), 0);
        DelegateProxyCaller caller = new DelegateProxyCaller();

        (bool ok, bytes memory revertData) = caller.invoke(proxy, hex"12345678");

        assertFalse(ok, "proxy code must not be usable through DELEGATECALL");
        assertEq(bytes4(revertData), EEZBase.UnauthorizedProxy.selector);
    }
}
