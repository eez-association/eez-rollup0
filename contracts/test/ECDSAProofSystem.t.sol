// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {ECDSAProofSystem} from "../src/ECDSAProofSystem.sol";

contract ECDSAProofSystemTest is Test {
    uint256 private constant SIGNER_KEY = 0xA11CE;
    uint256 private constant OTHER_KEY = 0xB0B;
    bytes32 private constant INPUT_HASH = keccak256("public inputs");
    uint256 private constant SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141;

    function testBindsSignatureToPublicInputs() external {
        ECDSAProofSystem verifier = new ECDSAProofSystem(vm.addr(SIGNER_KEY));
        bytes memory proof = _sign(SIGNER_KEY, INPUT_HASH);

        assertTrue(verifier.verify(proof, INPUT_HASH));
        assertFalse(verifier.verify(proof, keccak256("another batch")));
    }

    /// The verifier returns false instead of reverting so EEZ can surface its
    /// uniform InvalidProof error at the batch boundary.
    function testRejectsWrongSigner() external {
        ECDSAProofSystem verifier = new ECDSAProofSystem(vm.addr(SIGNER_KEY));

        assertFalse(verifier.verify(_sign(OTHER_KEY, INPUT_HASH), INPUT_HASH));
    }

    function testRejectsMalleableHighSSignature() external {
        ECDSAProofSystem verifier = new ECDSAProofSystem(vm.addr(SIGNER_KEY));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(SIGNER_KEY, INPUT_HASH);
        bytes memory malleated = abi.encodePacked(r, bytes32(SECP256K1_N - uint256(s)), v == 27 ? 28 : 27);

        assertFalse(verifier.verify(malleated, INPUT_HASH));
    }

    function _sign(uint256 key, bytes32 digest) private pure returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(key, digest);
        return abi.encodePacked(r, s, v);
    }
}
