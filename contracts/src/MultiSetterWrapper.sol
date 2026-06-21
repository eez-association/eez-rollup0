// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Like `SetterWrapper`, but reaches its cross-chain proxy TWICE in one
///         tx — so the composer records two cross-chain calls with the SAME
///         `proxyEntryHash`, exercising multi-entry / sequential-cursor
///         consumption (the `multi-call-twice` e2e shape). Final settled value
///         is still `v` (last write wins), so the harness's `slot0 == v` oracle
///         holds unchanged.
contract MultiSetterWrapper {
    address public immutable proxy;

    event WrappedTwice(uint256 v, uint256 a, uint256 b);

    constructor(address _proxy) {
        proxy = _proxy;
    }

    /// Calls `proxy.setValue(v)` twice and decodes both synchronous results.
    function setViaProxy(uint256 v) external {
        (bool ok1, bytes memory r1) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok1, "first cross-chain setValue reverted");
        (bool ok2, bytes memory r2) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok2, "second cross-chain setValue reverted");
        (, uint256 a) = abi.decode(r1, (bool, uint256));
        (, uint256 b) = abi.decode(r2, (bool, uint256));
        emit WrappedTwice(v, a, b);
    }
}
