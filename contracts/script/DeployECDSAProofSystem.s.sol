// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {ECDSAProofSystem} from "../src/ECDSAProofSystem.sol";

/// @title DeployECDSAProofSystem
/// @notice Deploys the real (publicInputsHash-binding) ECDSA proof system. The
///         attester is fixed at construction; rotation = redeploy.
///
///         Outputs: ECDSA_PS=<address>
///
///         Call shape:
///           forge script ... DeployECDSAProofSystem
///             --sig "run(address)" $ATTESTER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         `$ATTESTER` is the address whose key (on `eez-proverd`) signs the
///         recomputed `publicInputsHash`.
contract DeployECDSAProofSystem is Script {
    function run(address attester) external {
        vm.startBroadcast();
        ECDSAProofSystem ps = new ECDSAProofSystem(attester);
        vm.stopBroadcast();
        console.log("ECDSA_PS=%s", address(ps));
    }
}
