// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @title ParityGate
/// @notice Forwards to `target`, but reverts on odd L1 block numbers.
contract ParityGate {
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
