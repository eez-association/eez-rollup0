// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// Source-side caller that preserves raw cross-chain return bytes for test
/// assertions. It distinguishes an ABI-encoded empty `bytes` result from a
/// call that returns no bytes at all.
contract ReturnDataWrapper {
    address public immutable proxy;
    uint256 public lastReturnDataLength;
    bytes32 public lastReturnDataHash;

    constructor(address _proxy) {
        proxy = _proxy;
    }

    function callAndRecord(bytes calldata data) external {
        (bool ok, bytes memory ret) = proxy.call(data);
        require(ok, "cross-chain call reverted");
        lastReturnDataLength = ret.length;
        lastReturnDataHash = keccak256(ret);
    }
}
