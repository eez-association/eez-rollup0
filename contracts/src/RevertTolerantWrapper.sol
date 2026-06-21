// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Like `SetterWrapper`, but does NOT `require(ok)` — it try/catches the
///         cross-chain call and CONTINUES regardless (the `revertContinue` e2e
///         shape). When the target reverts, the composer must record the failed
///         cross-chain call and the L2 state stays unchanged.
contract RevertTolerantWrapper {
    address public immutable proxy;

    event Tried(uint256 v, bool ok);

    constructor(address _proxy) {
        proxy = _proxy;
    }

    function setViaProxy(uint256 v) external {
        (bool ok,) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        emit Tried(v, ok); // intentionally no `require` — continue on revert
    }
}
