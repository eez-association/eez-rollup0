// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {SetterWrapper} from "../src/SetterWrapper.sol";

contract SetterWrapperTest is Test {
    function testDecodesTheCrossChainResult() external {
        SetterWrapper wrapper = new SetterWrapper(address(new ReturningProxy()));

        vm.expectEmit();
        emit SetterWrapper.Wrapped(42, true, true, 42);
        wrapper.setViaProxy(42);
    }
}

contract ReturningProxy {
    fallback() external {
        require(msg.sig == bytes4(keccak256("setValue(uint256)")), "unexpected selector");
        uint256 value = abi.decode(msg.data[4:], (uint256));
        bytes memory result = abi.encode(true, value);
        assembly {
            return(add(result, 32), mload(result))
        }
    }
}
