// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Accepts an empty-calldata cross-chain call.
/// @dev The counter makes an otherwise-empty call observable in the end-to-end
///      scenario suite; it deliberately returns no bytes.
contract EmptyCall {
    uint256 public calls;
    uint256 public received;
    uint256 public lastValue;

    fallback() external payable {
        require(msg.data.length == 0, "expected empty calldata");
        _record();
    }

    function setValue(uint256 next) external payable returns (uint256) {
        lastValue = next;
        _record();
        return next;
    }

    function _record() private {
        calls++;
        received += msg.value;
    }
}
