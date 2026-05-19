// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {ECDSAProofSystem} from "../src/ECDSAProofSystem.sol";

/// @title DeployECDSAProofSystem
/// @notice Deploys the workspace-local dev-only ECDSA proof system. The
///         signer is fixed at construction; rotation = redeploy.
///
///         Outputs: ECDSA_PS=<address>
///
///         Call shape:
///           forge script ... DeployECDSAProofSystem
///             --sig "run(address)" $AUTHORIZED_SIGNER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         `$AUTHORIZED_SIGNER` is the address whose private key the
///         composer will sign each batch's `publicInputsHash` with —
///         typically `cast wallet address --private-key $PK_COMPOSER`.
contract DeployECDSAProofSystem is Script {
    function run(address authorizedSigner) external {
        vm.startBroadcast();
        ECDSAProofSystem ps = new ECDSAProofSystem(authorizedSigner);
        vm.stopBroadcast();
        console.log("ECDSA_PS=%s", address(ps));
    }
}
