// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {MockECDSAProofSystem} from "../src/MockECDSAProofSystem.sol";

/// @title DeployMockECDSAProofSystem
/// @notice Deploys the workspace-local devnet-only mock proof system. The
///         signer is fixed at construction; rotation = redeploy.
///
///         Outputs: MOCK_PS=<address>
///
///         Call shape:
///           forge script ... DeployMockECDSAProofSystem
///             --sig "run(address)" $AUTHORIZED_SIGNER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         `$AUTHORIZED_SIGNER` is the address whose key signs the fixed
///         `MOCK_PROVER_DIGEST` baked into the contract. Off-chain
///         `MockEcdsaProver` in `eez-prover` signs the same digest.
contract DeployMockECDSAProofSystem is Script {
    function run(address authorizedSigner) external {
        vm.startBroadcast();
        MockECDSAProofSystem ps = new MockECDSAProofSystem(authorizedSigner);
        vm.stopBroadcast();
        console.log("MOCK_PS=%s", address(ps));
    }
}
