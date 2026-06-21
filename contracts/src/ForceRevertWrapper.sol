// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice The `revertCounter` (`revertSpan`) shape: the cross-chain `setValue`
///         runs inside a self-call that ALWAYS reverts, so its effect must be
///         force-discarded by the replaying side (the composer should emit a
///         `revertSpan`), while the rolling hash still commits to the
///         successful call. Settled state ends UNCHANGED.
contract ForceRevertWrapper {
    address public immutable proxy;

    constructor(address _proxy) {
        proxy = _proxy;
    }

    function setViaProxy(uint256 v) external {
        // Swallow the inner revert: the proxy call succeeded but its span is
        // rolled back.
        try this.inner(v) {} catch {}
    }

    function inner(uint256 v) external {
        require(msg.sender == address(this), "self only");
        (bool ok,) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok, "cross-chain setValue reverted");
        revert("force-revert span");
    }
}
