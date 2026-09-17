// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZ} from "eez-core-protocol/src/EEZ.sol";
import {EEZL2} from "eez-core-protocol/src/L2/EEZL2.sol";

contract CallHashVectorsTest is Test {
    EEZ private eez;
    EEZL2 private eezL2;

    function setUp() external {
        eez = new EEZ(address(0xDEAD));
        eezL2 = new EEZL2(1, address(0xBEEF), false);
    }

    function testCallHashVectors() external view {
        address source = address(0xBB);
        address target = address(0xAA);
        bytes memory data = hex"010203";

        assertEq(
            eez.computeCrossChainCallHash(false, source, 7, target, 1, 0, 0, data),
            0x16b1575ff5a4ec44167aebf047dd46f77db3766f7481445ad09c8136bff735a8
        );
        assertEq(
            eez.computeCrossChainCallHash(true, source, 7, target, 1, 0, 0, data),
            0x4cf0f2738ced4dcd497cf8a081030f41c5dc588fbdcac75f3a217e979d19abe7
        );
        assertEq(
            eezL2.computeCrossChainCallHash(false, source, 1, target, 7, 1 ether, 0, data),
            0x9fd05cd7eebaf1d08b2961cb5d1237ef586cea58141270697a5509c6f3a03a37
        );
        assertEq(
            eezL2.computeCrossChainCallHash(false, source, 1, target, 7, 1 ether, 123456, data),
            0x25400cdd749a1c3ac82f4e3093f0460afe21e718a545a96f9399b9ae486c99e4
        );

        assertEq(
            eez.computeCrossChainCallHash(
                false, source, type(uint64).max, target, type(uint64).max - 1, type(uint256).max, 0, hex""
            ),
            0x414b9d6bf91a3e266bcd34ddd870a53332107a606b6eda618455f9f940291e2b
        );
        assertEq(
            eezL2.computeCrossChainCallHash(
                false,
                source,
                type(uint64).max,
                target,
                type(uint64).max - 1,
                type(uint256).max,
                type(uint64).max,
                hex""
            ),
            0x7f04915c437db6536fe9d746b135ed834b391532e4be8beadd898ad1f592895f
        );
    }
}
