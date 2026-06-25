// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title ValuePayable
/// @notice Payable variant of `Value` for VALUE-BEARING cross-chain tests.
///         `setValue` is `payable` (accepts `msg.value`) and a bare `receive()`
///         takes value-only transfers, so the L2 target can RECEIVE the ETH a
///         value-bearing inbound (L1->L2) deposit delivers — the plain `Value`
///         reverts on any incoming ETH. No access control: we test the
///         cross-chain value pipeline, not access policy.
contract ValuePayable {
    uint256 public value;

    event ValueSet(address indexed by, uint256 newValue, uint256 valueReceived);

    constructor(uint256 initial) {
        value = initial;
    }

    /// Sets `value`, accepting any `msg.value`. Returns the same
    /// (changed, newValue) tuple as `Value.setValue` so the existing
    /// cross-chain return-data plumbing is reused unchanged.
    function setValue(uint256 v) external payable returns (bool changed, uint256 newValue) {
        changed = value != v;
        value = v;
        emit ValueSet(msg.sender, v, msg.value);
        return (changed, v);
    }

    /// Accept bare value transfers (a value-only inbound with empty calldata).
    receive() external payable {}
}
