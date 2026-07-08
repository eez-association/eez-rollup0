// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// Isolation control: identical to `Value` but `setValue` returns NOTHING
/// (empty returnData). Used to test whether cross-chain contract-call
/// settlement fails specifically because of non-empty return data
/// (rolling-hash / returnData path) vs. any contract call at all.
contract ValueNoRet {
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
