// SPDX-License-Identifier: MIT
pragma solidity 0.8.34;

import {Test} from "forge-std/Test.sol";
import {Bridge} from "eez-core-protocol/src/periphery/Bridge.sol";
import {InitializedBridge} from "eez-core-protocol/script/DeployInitializedBridge.s.sol";

contract ReferenceBridgeTest is Test {
    function testDeploymentIsInitializedAndCannotBeTakenOver() external {
        BridgeManager manager = new BridgeManager();
        Bridge bridge = new InitializedBridge(address(manager), 1, address(this));
        assertEq(address(bridge.manager()), address(manager));
        assertEq(bridge.rollupId(), 1);
        assertEq(bridge.admin(), address(this));

        vm.prank(address(0xBAD));
        vm.expectRevert(Bridge.AlreadyInitialized.selector);
        bridge.initialize(address(manager), 0, address(0xBAD));
    }

    function testEtherUsesTheRecipientProxyInBothDirections() external {
        assertEtherForwarded(0, 1);
        assertEtherForwarded(1, 0);
    }

    function assertEtherForwarded(uint64 source, uint64 destination) internal {
        BridgeManager manager = new BridgeManager();
        Bridge bridge = new InitializedBridge(address(manager), source, address(this));
        address recipient = address(0xBEEF);
        bridge.bridgeEther{value: 1 ether}(destination, recipient);

        assertEq(manager.recipient(), recipient);
        assertEq(manager.destination(), destination);
        assertEq(address(manager.proxy()).balance, 1 ether);
        assertEq(address(bridge).balance, 0);
    }

    function testZeroValueCannotCreateAProxy() external {
        BridgeManager manager = new BridgeManager();
        Bridge bridge = new InitializedBridge(address(manager), 0, address(this));
        vm.expectRevert(Bridge.ZeroAmount.selector);
        bridge.bridgeEther(1, address(0xBEEF));
        assertEq(address(manager.proxy()), address(0));
    }

    function testFailedProxyCallDoesNotKeepTheSendersEther() external {
        BridgeManager manager = new BridgeManager();
        Bridge bridge = new InitializedBridge(address(manager), 0, address(this));
        address proxy = manager.computeCrossChainProxyAddress(address(0xBEEF), 1);
        vm.etch(proxy, hex"5f80fd");
        vm.expectRevert(abi.encodeWithSelector(Bridge.ProxyCallFailed.selector, bytes("")));
        bridge.bridgeEther{value: 1 ether}(1, address(0xBEEF));
        assertEq(address(bridge).balance, 0);
        assertEq(proxy.balance, 0);
    }
}

contract BridgeManager {
    EtherProxy public proxy;
    address public recipient;
    uint64 public destination;

    function computeCrossChainProxyAddress(address, uint64) external view returns (address) {
        return address(
            uint160(
                uint256(
                    keccak256(
                        abi.encodePacked(
                            bytes1(0xff), address(this), bytes32(0), keccak256(type(EtherProxy).creationCode)
                        )
                    )
                )
            )
        );
    }

    function createCrossChainProxy(address recipient_, uint64 destination_) external returns (address) {
        recipient = recipient_;
        destination = destination_;
        proxy = new EtherProxy{salt: bytes32(0)}();
        return address(proxy);
    }
}

contract EtherProxy {
    receive() external payable {}
}
