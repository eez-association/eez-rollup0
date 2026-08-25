// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// Stateful cross-chain test target: every call's return value depends on
/// prior calls in the same block, so co-bundled order-dependent claims are
/// exercised end to end (issue #88 repro).
contract Counter {
    uint256 public count;

    function increment() external returns (uint256 newCount) {
        count += 1;
        return count;
    }

    function add(uint256 x) external returns (uint256 newCount) {
        count += x;
        return count;
    }
}
