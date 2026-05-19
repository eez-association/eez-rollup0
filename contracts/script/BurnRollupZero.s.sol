// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";
import {Rollup} from "sync-rollups-protocol/src/rollupContract/Rollup.sol";

/// @title BurnRollupZero
/// @notice Burns `rollupId = 0` (== `MAINNET_ROLLUP_ID`) on a freshly-
///         deployed `EEZ`. Required because `EEZ.createRollup` returns
///         `rollupCounter++` — the FIRST registration on a fresh
///         registry hands back id 0, which is reserved for L1 self
///         and is rejected by `postVerifyAndExecuteOrSaveExecutionsFromBatch`'s
///         strict-increasing-rollupId validation. Upstream tests + e2e
///         scripts handle this by registering a throwaway "burn"
///         manager first; this script bakes that step into the devnet
///         deploy flow so the real-rollup deploy
///         (`DeployRollup` → `RegisterRollup`) lands at `rollupId = 1`.
///
///         Outputs: BURN_ROLLUP=<address>
///                  BURN_ROLLUP_ID=<id>   (always 0; emitted defensively)
///
///         Call shape:
///           forge script ... BurnRollupZero
///             --sig "run(address,address,address,address)"
///                   $EEZ $ECDSA_PS $AUTHORIZED_SIGNER $OWNER
///             --rpc-url $L1_RPC --broadcast --private-key $PK
///
///         Args mirror `DeployRollup` — the burn rollup is a complete
///         `Rollup.sol` instance (PS-membership semantics matter at
///         construction even though id 0 is never posted to).
///
///         MUST run between `DeployECDSAProofSystem` and the first
///         `DeployRollup`/`RegisterRollup` pair. Safe to omit only if
///         the EEZ's `rollupCounter` is already non-zero (e.g.,
///         deploying additional rollups onto an existing registry).
contract BurnRollupZero is Script {
    error InvalidProofSystem();
    error UnexpectedBurnRollupId(uint256 returnedId);

    function run(
        address eez,
        address ecdsaProofSystem,
        address authorizedSigner,
        address owner
    ) external {
        if (ecdsaProofSystem == address(0)) revert InvalidProofSystem();

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = ecdsaProofSystem;

        bytes32[] memory vkeys = new bytes32[](1);
        vkeys[0] = bytes32(uint256(uint160(authorizedSigner)));

        vm.startBroadcast();
        Rollup burn = new Rollup(eez, owner, /* threshold */ 1, proofSystems, vkeys);
        uint256 burnId = EEZ(eez).createRollup(address(burn), bytes32(0));
        vm.stopBroadcast();

        // Defensive: a non-zero return means the EEZ wasn't fresh; that's
        // operationally fine (the real rollup will still get a unique id)
        // but is almost certainly a script-ordering bug — flag it loudly.
        if (burnId != 0) revert UnexpectedBurnRollupId(burnId);

        console.log("BURN_ROLLUP=%s", address(burn));
        console.log("BURN_ROLLUP_ID=%s", burnId);
    }
}
