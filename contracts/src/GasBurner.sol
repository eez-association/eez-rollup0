// SPDX-License-Identifier: MIT
pragma solidity ^0.8.34;

/// Test target for deliveries that fit individually but exceed a block together.
contract GasBurner {
    uint256 public calls;

    function setValue(uint256 gasToBurn) external returns (bool, uint256) {
        uint256 start = gasleft();
        while (start - gasleft() < gasToBurn) {}
        calls++;
        return (true, calls);
    }
}
