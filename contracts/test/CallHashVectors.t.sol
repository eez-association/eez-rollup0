// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";
import {EEZL2} from "sync-rollups-protocol/src/L2/EEZL2.sol";

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
            eez.computeCrossChainCallHash(false, source, 7, target, 1, 0, data),
            0x0aea0f2282e747ca563ff59f9dbd36570e9973cfc007abfa51893d3fb9aaefdf
        );
        assertEq(
            eez.computeCrossChainCallHash(true, source, 7, target, 1, 0, data),
            0xa03958bfe3866dabc6d8e5466965bdfe5f0368308af0d2069801e1562bcd35d0
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
                false, source, type(uint64).max, target, type(uint64).max - 1, type(uint256).max, hex""
            ),
            0xf149543f591e628d8247387fdf6780d6aee8c119258a34b348509695c202a1a1
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
