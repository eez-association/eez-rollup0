// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title NestedValue
/// @notice An L2 cross-chain target that ITSELF makes a cross-chain call
///         before setting its own value — used to drive depth-2 nesting in
///         the compose fuzz harness. Reached as the L2 side of an L1→L2 call
///         (L1 `SetterWrapper` → proxy(NestedValue)); when the composer
///         simulates `setValue` on the L2 overlay, this contract calls
///         `inner` — a `CrossChainProxy` registered in `EEZL2.authorizedProxies`
///         — so the overlay inspector detects + dispatches a SECOND
///         cross-chain call, exercising the LIFO overlay push/pop pairing at
///         depth > 1. Returns the same `(bool changed, uint256 newValue)`
///         tuple shape as `Value`, so the L1 `SetterWrapper` decode still
///         matches.
contract NestedValue {
    /// @notice A CrossChainProxy registered on THIS chain; calling it routes
    ///         through the manager as a nested cross-chain execution.
    address public immutable inner;
    uint256 public value;

    event NestedSet(uint256 v, bool innerChanged, uint256 innerNewValue);

    constructor(address _inner) {
        inner = _inner;
    }

    /// Makes the nested cross-chain call (`inner.setValue(v + 1)`), uses its
    /// synchronous result, then sets and returns its own value.
    function setValue(uint256 v) external returns (bool changed, uint256 newValue) {
        (bool ok, bytes memory ret) = inner.call(abi.encodeWithSignature("setValue(uint256)", v + 1));
        require(ok, "inner cross-chain setValue reverted");
        (bool innerChanged, uint256 innerNew) = abi.decode(ret, (bool, uint256));

        changed = value != v;
        value = v;
        emit NestedSet(v, innerChanged, innerNew);
        return (changed, v);
    }
}
