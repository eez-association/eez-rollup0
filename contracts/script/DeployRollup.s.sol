// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {Rollup} from "sync-rollups-protocol/src/rollupContract/Rollup.sol";

/// @title DeployRollup
/// @notice Deploys the per-rollup `IRollupContract` manager (reference
///         `Rollup.sol`) configured for a single-PS, threshold-1 setup:
///         the workspace-local `ECDSAProofSystem` with vkey =
///         `bytes32(uint256(uint160(authorizedSigner)))`.
///
///         Outputs: ROLLUP_CONTRACT=<address>
///
///         Call shape:
///           forge script ... DeployRollup
///             --sig "run(address,address,address,address)"
///                   $EEZ $ECDSA_PS $AUTHORIZED_SIGNER $OWNER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         Args:
///           - eez                — central registry address (from DeployEEZ).
///           - proofSystem        — workspace-local proof-system address
///                                  (from DeployECDSAProofSystem).
///           - authorizedSigner   — the same value passed to
///                                  DeployECDSAProofSystem; its address
///                                  becomes the vkey membership ticket
///                                  the registry stores.
///           - owner              — owner of the Rollup manager. The owner
///                                  can add / remove proof systems and
///                                  tune the threshold post-deploy.
///                                  Typically the same key driving every
///                                  other deploy (e.g.,
///                                  `cast wallet address --private-key $PK`).
///                                  Passed explicitly so the broadcaster
///                                  isn't conflated with the manager
///                                  owner — `msg.sender` inside a Foundry
///                                  `run()` is the script-runner default,
///                                  not the broadcaster.
contract DeployRollup is Script {
    error InvalidProofSystem();

    function run(
        address eez,
        address proofSystem,
        address authorizedSigner,
        address owner
    ) external {
        if (proofSystem == address(0)) revert InvalidProofSystem();

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = proofSystem;

        bytes32[] memory vkeys = new bytes32[](1);
        // The registry treats vkey as opaque (only checks non-zero +
        // membership). Embedding the signer address as the vkey gives
        // off-chain tooling a way to read the expected signer without
        // a separate getter, and guarantees a non-zero membership
        // ticket. See `ECDSAProofSystem.sol`'s `signer()` for the
        // actual signing-address surface (the verifier reads it from
        // its own storage, not from the vkey).
        vkeys[0] = bytes32(uint256(uint160(authorizedSigner)));

        vm.startBroadcast();
        Rollup rollup = new Rollup(
            eez,
            owner,
            /* threshold */ 1,
            proofSystems,
            vkeys
        );
        vm.stopBroadcast();
        console.log("ROLLUP_CONTRACT=%s", address(rollup));
    }
}
