// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Reaches TWO different cross-chain proxies in one tx → the composer
///         records two entries with DIFFERENT `proxyEntryHash`es (the
///         multi-call-two-diff e2e shape). Both targets settle to `v`; the
///         harness oracle checks the first, the ratify replay checks both.
contract TwoProxySetter {
    address public immutable proxyA;
    address public immutable proxyB;

    constructor(address _a, address _b) {
        proxyA = _a;
        proxyB = _b;
    }

    function setViaProxy(uint256 v) external {
        (bool okA,) = proxyA.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(okA, "proxy A setValue reverted");
        (bool okB,) = proxyB.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(okB, "proxy B setValue reverted");
    }
}
