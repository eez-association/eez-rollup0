// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Source-side caller that records a failed cross-chain call instead of
///         bubbling it, so the source transaction still succeeds and the exact
///         destination revert data stays readable from source state.
contract RevertBubbleWrapper {
    address public immutable proxy;
    uint256 public failures;
    uint256 public successes;
    uint256 public lastRevertLength;
    bytes32 public lastRevertHash;

    constructor(address _proxy) {
        proxy = _proxy;
    }

    function callAndRecord(bytes calldata data) external {
        (bool ok, bytes memory ret) = proxy.call(data);
        if (ok) {
            successes++;
        } else {
            failures++;
        }
        lastRevertLength = ret.length;
        lastRevertHash = keccak256(ret);
    }
}
