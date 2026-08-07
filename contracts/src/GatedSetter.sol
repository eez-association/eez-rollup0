// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

interface IValueReader {
    function value() external view returns (uint256);
}

/// Cross-direction dependency probe for the chained-simulation tests: a
/// source-chain contract that reads a local `Value`, requires it to match
/// `expected`, then makes a cross-chain call through `proxy`. When the
/// expected value is written by a same-slot transaction of the opposite
/// direction, only a state-chained simulation sees it.
contract GatedSetter {
    address public immutable gate;
    address public immutable proxy;

    constructor(address _gate, address _proxy) {
        gate = _gate;
        proxy = _proxy;
    }

    /// Requires `gate.value() == expected`, then `proxy.setValue(v)`.
    function setViaProxyIfValue(uint256 expected, uint256 v) external {
        require(IValueReader(gate).value() == expected, "gate: unexpected value");
        (bool ok,) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok, "cross-chain setValue reverted");
    }
}
