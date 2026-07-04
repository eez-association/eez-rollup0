// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {ECDSAProofSystem} from "sync-rollups-protocol/src/proofSystems/ECDSAProofSystem.sol";

/// @title DeployECDSAProofSystem
/// @notice Deploys the REAL (submodule) ECDSAProofSystem — its `verify` recovers
///         the 65-byte ECDSA signature over the ACTUAL `publicInputsHash`, vs the
///         dev `MockECDSAProofSystem` which recovers over a FIXED digest. The
///         out-of-process prover (`eez-proverd`) signs exactly this hash, so a
///         rollup registered with this proof system is settled by the real
///         attestation rather than a self-signed mock.
///
///         Outputs: ECDSA_PS=<address>
///
///         Call shape:
///           forge script ... DeployECDSAProofSystem
///             --sig "run(address,address)" $OWNER $AUTHORIZED_SIGNER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         `$AUTHORIZED_SIGNER` is the attester whose key the prover signs with
///         (= `EEZ_PROOF_SIGNER_KEY`'s address, and the rollup's registered
///         `vkey = bytes32(uint160(signer))`). `$OWNER` can rotate it via
///         `setSigner`.
contract DeployECDSAProofSystem is Script {
    function run(address initialOwner, address authorizedSigner) external {
        vm.startBroadcast();
        ECDSAProofSystem ps = new ECDSAProofSystem(initialOwner, authorizedSigner);
        vm.stopBroadcast();
        console.log("ECDSA_PS=%s", address(ps));
    }
}
