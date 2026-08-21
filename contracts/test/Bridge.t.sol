// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {BridgeReceiver, BridgeSender} from "../src/Bridge.sol";

contract BridgeTest is Test {
    function testSenderForwardsTheFullBridgeValue() external {
        PayableProxy proxy = new PayableProxy();
        BridgeReceiver destination = new BridgeReceiver();
        BridgeSender sender = new BridgeSender(address(proxy), address(destination));

        sender.bridge{value: 1 ether}();

        assertEq(address(proxy).balance, 1 ether);
        assertEq(sender.L2_PROXY(), address(proxy));
        assertEq(sender.L2_DESTINATION(), address(destination));
    }

    function testSenderRejectsFailedProxyCalls() external {
        BridgeSender sender = new BridgeSender(address(new RevertingProxy()), address(0xBEEF));

        vm.expectRevert("bridge failed");
        sender.bridge{value: 1 ether}();
    }

    function testReceiverAcceptsBridgedEther() external {
        BridgeReceiver receiver = new BridgeReceiver();

        (bool ok,) = address(receiver).call{value: 1 ether}("");

        assertTrue(ok);
        assertEq(address(receiver).balance, 1 ether);
    }
}

contract PayableProxy {
    receive() external payable {}
}

contract RevertingProxy {
    receive() external payable {
        revert("proxy rejected bridge");
    }
}
