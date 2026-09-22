// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title ParityGate
/// @notice Forwards to `target`, but reverts on ODD L1 block numbers.
///
/// @dev A test fixture for partial consumption. The Composer simulates a
///      cross-chain call against the ANCHOR L1 block and the bundle lands one
///      or more blocks later, so a call composed at an even anchor passes
///      simulation and then reverts at inclusion. That is the only way to
///      produce a genuine partial settlement without racing a state write:
///      compose-time eviction catches every revert it can reproduce, so the
///      revert has to be invisible at compose time and certain at inclusion.
///
///      With the reverting tx whitelisted in `revertingTxHashes` the postBatch
///      still lands, its entry goes unconsumed, and every later entry fails its
///      own `currentState` precondition — so L1 halts at a prefix.
contract ParityGate {
    /// @notice Emitted instead of reverting when the gate is open, so a run can
    ///         tell "forwarded" from "never called".
    event Forwarded(uint256 blockNumber);

    error OddBlock(uint256 blockNumber);

    address public immutable target;

    constructor(address target_) {
        target = target_;
    }

    fallback() external payable {
        if (block.number % 2 == 1) revert OddBlock(block.number);
        emit Forwarded(block.number);
        (bool ok, bytes memory ret) = target.call{value: msg.value}(msg.data);
        if (!ok) {
            assembly {
                revert(add(ret, 0x20), mload(ret))
            }
        }
        assembly {
            return(add(ret, 0x20), mload(ret))
        }
    }
}
