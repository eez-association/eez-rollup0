// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";
import {EEZL2} from "sync-rollups-protocol/src/L2/EEZL2.sol";
import {Rollup} from "sync-rollups-protocol/src/rollupContract/Rollup.sol";

import {ECDSAProofSystem} from "../src/ECDSAProofSystem.sol";

contract DeploymentSmokeTest is Test {
    address private constant RECOVERY_ADDRESS = address(0xA11CE);
    address private constant OWNER = address(0xB0B);
    address private constant SIGNER = address(0xC0DE);
    address private constant SYSTEM_ADDRESS = address(0xD00D);
    bytes32 private constant INITIAL_STATE_ROOT = keccak256("eez deployment smoke initial state");

    function testFreshProtocolDeployment() external {
        EEZ eez = new EEZ(RECOVERY_ADDRESS);
        ECDSAProofSystem proofSystem = new ECDSAProofSystem(SIGNER);
        bytes32 vkey = bytes32(uint256(uint160(SIGNER)));

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = address(proofSystem);
        bytes32[] memory vkeys = new bytes32[](1);
        vkeys[0] = vkey;

        Rollup rollup = new Rollup(address(eez), OWNER, 1, proofSystems, vkeys);
        uint64 rollupId = eez.registerRollup(address(rollup), INITIAL_STATE_ROOT);

        (address registeredRollup, bytes32 stateRoot, uint256 etherBalance) = eez.rollups(rollupId);
        assertEq(eez.RECOVERY_ADDRESS(), RECOVERY_ADDRESS);
        assertEq(rollupId, 1);
        assertEq(rollup.rollupId(), rollupId);
        assertEq(registeredRollup, address(rollup));
        assertEq(stateRoot, INITIAL_STATE_ROOT);
        assertEq(etherBalance, 0);
        assertEq(rollup.verificationKey(address(proofSystem)), vkey);
        assertEq(proofSystem.signer(), SIGNER);

        EEZL2 eezL2 = new EEZL2(rollupId, SYSTEM_ADDRESS, false);
        assertNotEq(eezL2.SYSTEM_ADDRESS(), address(0));
        assertEq(eezL2.ROLLUP_ID(), rollupId);
        assertEq(eezL2.SYSTEM_ADDRESS(), SYSTEM_ADDRESS);
        assertEq(eezL2.RECOVERY_ADDRESS(), SYSTEM_ADDRESS);
        assertFalse(eezL2.USE_GAS_LEFT());
    }
}
