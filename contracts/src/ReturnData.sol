// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// Test destination for cross-chain return-data preservation.
contract ReturnData {
    function echo(bytes calldata value) external pure returns (bytes memory) {
        return value;
    }

    function emptyBytes() external pure returns (bytes memory) {
        return bytes("");
    }
}
