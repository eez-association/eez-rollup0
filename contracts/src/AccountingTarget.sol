// SPDX-License-Identifier: MIT
pragma solidity ^0.8.34;

/// Test fixture for payable delivery, rollback and unsolicited protocol funds.
contract AccountingTarget {
    uint256 public calls;

    function accept() external payable {
        calls++;
    }

    function fail() external payable {
        calls++;
        revert("accounting test revert");
    }

    function force(address payable target) external payable {
        new AccountingForce{value: msg.value}(target);
    }

    function catchCall(address target) external payable {
        (bool ok,) = target.call{value: msg.value}("");
        require(!ok, "expected inner failure");
        calls++;
    }
}

contract AccountingForce {
    constructor(address payable target) payable {
        selfdestruct(target);
    }
}
