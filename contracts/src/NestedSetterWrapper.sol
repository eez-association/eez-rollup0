// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

contract NestedSetterInner {
    address public immutable proxy;
    uint256 public completedProxyCalls;

    constructor(address _proxy) {
        proxy = _proxy;
    }

    function setViaProxy(uint256 v) external {
        (bool ok, bytes memory ret) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok, "cross-chain setValue reverted");
        abi.decode(ret, (bool, uint256));
        completedProxyCalls++;
    }
}

contract NestedSetterOuter {
    address public immutable inner;

    constructor(address _inner) {
        inner = _inner;
    }

    function setViaInner(uint256 v) external {
        NestedSetterInner(inner).setViaProxy(v);
    }
}
