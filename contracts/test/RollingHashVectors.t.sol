// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";
import {EEZL2} from "sync-rollups-protocol/src/L2/EEZL2.sol";
import {StateUpdate} from "sync-rollups-protocol/src/interfaces/IEEZ.sol";

/// Exposes the pinned protocol's internal folds so the constants below come
/// from the Solidity implementation rather than from the Rust code under test.
contract L1RollingHashHarness is EEZ {
    constructor() EEZ(address(0xDEAD)) {}

    function foldEntry(
        StateUpdate[] memory updates,
        bytes32 proxyEntryHash,
        bytes32 callHash
    )
        external
        returns (
            bytes32 entryBegin,
            bytes32 callBegin,
            bytes32 callEnd,
            bytes32 nestedBegin,
            bytes32 nestedEnd,
            bytes32 callNotFound
        )
    {
        _rollingHashEntryBegin(updates, proxyEntryHash);
        entryBegin = _rollingHash;
        _rollingHashCallBegin(callHash);
        callBegin = _rollingHash;
        _rollingHashCallEnd(true, hex"aabbcc");
        callEnd = _rollingHash;
        _rollingHashNestedBegin(callHash);
        nestedBegin = _rollingHash;
        _rollingHashNestedEnd();
        nestedEnd = _rollingHash;
        _rollingHashCallNotFound(callHash);
        callNotFound = _rollingHash;
        _rollingHash = bytes32(0);
    }

    function foldStaticResults() external pure returns (bytes32 first, bytes32 second) {
        first = _rollingHashStaticResult(bytes32(0), true, hex"aabbcc");
        second = _rollingHashStaticResult(first, false, hex"deadbeef");
    }
}

contract L2RollingHashHarness is EEZL2 {
    constructor() EEZL2(1, address(0xBEEF), false) {}

    function entryBegin(bytes32 proxyEntryHash) external returns (bytes32 result) {
        _seedRollingHash(proxyEntryHash);
        result = _rollingHash;
        _rollingHash = bytes32(0);
    }
}

contract RollingHashVectorsTest is Test {
    L1RollingHashHarness private l1;
    L2RollingHashHarness private l2;

    function setUp() external {
        l1 = new L1RollingHashHarness();
        l2 = new L2RollingHashHarness();
    }

    function testRollingHashVectors() external {
        StateUpdate[] memory updates = new StateUpdate[](2);
        // The entry seed binds rollupId + currentState, but deliberately not
        // newState or etherDelta. The latter fields remain bound by entryHash.
        updates[0] = StateUpdate({
            rollupId: 1, currentState: bytes32(uint256(0x11)), newState: bytes32(uint256(0xAA)), etherDelta: 1
        });
        updates[1] = StateUpdate({
            rollupId: type(uint64).max,
            currentState: bytes32(uint256(0x22)),
            newState: bytes32(uint256(0xBB)),
            etherDelta: -1
        });

        bytes32 proxyEntryHash = bytes32(uint256(0x33));
        bytes32 callHash = bytes32(uint256(0x44));

        (
            bytes32 l1Seed,
            bytes32 afterCallBegin,
            bytes32 afterCallEnd,
            bytes32 afterNestedBegin,
            bytes32 afterNestedEnd,
            bytes32 afterNotFound
        ) = l1.foldEntry(updates, proxyEntryHash, callHash);
        bytes32 l2Seed = l2.entryBegin(proxyEntryHash);
        (bytes32 afterStaticSuccess, bytes32 afterStaticFailure) = l1.foldStaticResults();

        assertEq(l1Seed, 0xbccdfc431d3828e701fd170dd3c01a56fdcadfa6ac3c8bddae8a7e9b1bbff90b, "L1 entry seed");
        assertEq(l2Seed, 0x44496df070da3f045064f6d6f394484a8de10d5710290d619b67d975ec89320f, "L2 entry seed");
        assertEq(afterCallBegin, 0xe4f4cd44333ceae020b8bd8a009d7f36764ed6e17034e712246d9044a7095b5f, "CALL_BEGIN");
        assertEq(afterCallEnd, 0x0bb23fd1152f649e205070c2e3ce9b87c1eaf307e91050d283ae623288ba5653, "CALL_END");
        assertEq(afterNestedBegin, 0xe12d741005d0ef78d3a5789786f2a71f5ebeb6c4db6bf1588ca03ca1eb104e79, "NESTED_BEGIN");
        assertEq(afterNestedEnd, 0x4c93c6bbe364d4725275e853cd4fc9bb76683740a3d722dbbaa243b75d047298, "NESTED_END");
        assertEq(afterNotFound, 0xc1168e0ae843595280dec49290baca909088f6e5c97910c308afdf8d5410b3fc, "CALL_NOT_FOUND");
        assertEq(
            afterStaticSuccess,
            0x3ae2d1487308bbcef1156971182173344cb714be7b2583d95463d05e1e8257c4,
            "first static result"
        );
        assertEq(
            afterStaticFailure,
            0x4bdf2bdbf63bd9ba318a5bddc8075fd93cb7f8c7d8d489d2a38a4680098d2b3b,
            "second static result"
        );
    }
}
