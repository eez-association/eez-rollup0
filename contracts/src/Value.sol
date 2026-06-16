// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title Value
/// @notice Minimal setter contract used by the cross-chain smoke harness.
///         Deployed on L2; written to via an L1-side `CrossChainProxy(Value, L2_ID)`
///         that routes the call through `EEZ.executeCrossChainCall` on L1 and is
///         materialized on L2 via `EEZL2.executeIncomingCrossChainCall`.
///         No access control — we are testing the cross-chain pipeline,
///         not Value's protection model.
contract Value {
    uint256 public value;

    event ValueSet(address indexed by, uint256 newValue);

    constructor(uint256 initial) {
        value = initial;
    }

    function setValue(uint256 v) external {
        value = v;
        emit ValueSet(msg.sender, v);
    }
}
