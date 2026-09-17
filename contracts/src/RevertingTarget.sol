// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Destination fixture whose calls fail in distinguishable ways.
/// @dev `calls` counts only the successful path, so a rolled-back frame is
///      observable as a counter that did not move.
contract RevertingTarget {
    error Rejected(uint256 seen);

    uint256 public calls;
    uint256 public lastValue;

    /// Reverts with a custom error carrying its argument.
    function revertCustom(uint256 v) external payable {
        revert Rejected(v);
    }

    /// Reverts with a plain string reason.
    function revertString(uint256) external payable {
        revert("reverting target");
    }

    /// Writes first, then reverts — the write must not survive.
    function writeThenRevert(uint256 v) external payable {
        calls++;
        lastValue = v;
        revert Rejected(v);
    }

    /// The succeeding control path, to prove the target is otherwise reachable.
    function succeed(uint256 v) external payable returns (uint256) {
        calls++;
        lastValue = v;
        return v;
    }
}
