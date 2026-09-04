// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// Test wrapper for the "internal cross-chain call" pattern: a normal
/// L1 contract that calls a cross-chain proxy INTERNALLY (so the proxy
/// is reached at CALL depth > 1, not as the tx's top-level `to`), then
/// uses the synchronous result — decodes the L2 return tuple and emits
/// it. Exercises the composer's per-frame proxy detection
/// (eez-evm-inspector) + return-data synthesis (EEZBase
/// `entry.returnData`).
contract SetterWrapper {
    address public immutable proxy;
    uint256 public completedProxyCalls;
    bool public lastChanged;
    uint256 public lastNewValue;

    /// Carries the L2 `Value.setValue` return tuple back into an L1 log:
    /// `input` is what we sent, `changed`/`newValue` is what L2 returned.
    event Wrapped(uint256 input, bool ok, bool changed, uint256 newValue);

    constructor(address _proxy) {
        proxy = _proxy;
    }

    /// Calls `proxy.setValue(v)` (the cross-chain call), decodes the
    /// `(bool changed, uint256 newValue)` the composer synthesizes back
    /// into this frame, and emits it — "doing something with the L2
    /// result" entirely within the L1 tx.
    function setViaProxy(uint256 v) external {
        _setViaProxy(v);
    }

    /// Derives the proxy-call calldata from source-chain state. Separate calls
    /// composed together must therefore observe the preceding call count.
    function setNextValueViaProxy() external {
        _setViaProxy(completedProxyCalls + 1);
    }

    /// Calls the same proxy twice with identical calldata. Both invocations
    /// deliberately share their semantic cross-chain hash; their ordered entry
    /// positions, rather than the hash, distinguish them.
    function setSameValueTwice(uint256 v) external {
        _setViaProxy(v);
        _setViaProxy(v);
    }

    function _setViaProxy(uint256 v) private {
        (bool ok, bytes memory ret) = proxy.call(abi.encodeWithSignature("setValue(uint256)", v));
        require(ok, "cross-chain setValue reverted");
        (bool changed, uint256 newValue) = abi.decode(ret, (bool, uint256));
        completedProxyCalls++;
        lastChanged = changed;
        lastNewValue = newValue;
        emit Wrapped(v, ok, changed, newValue);
    }
}
