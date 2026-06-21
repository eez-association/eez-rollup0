// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice A `Value` that REVERTS on odd inputs — lets the fuzzer reach the
///         cross-chain natural-revert path (`CALL_END(success=false)` /
///         `LookupCall{failed:true}`) when reached through a try/catch wrapper.
contract RevertableValue {
    uint256 public value;

    function setValue(uint256 v) external returns (bool changed, uint256 newValue) {
        require(v % 2 == 0, "odd value rejected");
        changed = value != v;
        value = v;
        return (changed, v);
    }
}
