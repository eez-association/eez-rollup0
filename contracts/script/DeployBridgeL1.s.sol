// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";
import {SafeCast} from "@openzeppelin/contracts/utils/math/SafeCast.sol";
import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";
import {BridgeSender} from "../src/Bridge.sol";

/// @notice Deploys the L1-side cross-chain bridge contracts:
///   1. Creates a `CrossChainProxy` on L1 via `EEZ.createCrossChainProxy`
///      that represents the L2 destination (BridgeReceiver). Anyone calling
///      this proxy on L1 with value forwards the call across to the L2
///      destination once the matching `postAndVerifyBatch` + system tx land.
///   2. Deploys `BridgeSender(l2Proxy, l2Destination)` — the user-facing
///      contract whose `bridge()` call sends value through `l2Proxy`.
///
/// Inputs (env):
///   - EEZ_REGISTRY_ADDRESS  : L1 EEZ.sol address (from `scripts/deploy.sh`).
///   - EEZ_L2_BRIDGE_RECEIVER: L2 BridgeReceiver address (predeploy at
///                              `0x4200…0008`).
///   - EEZ_ROLLUP_ID         : Rollup id this L2 belongs to (from deploy.sh).
///
/// Outputs:
///   - `L2_PROXY=<addr>` — the L1 `CrossChainProxy` (representing the
///     L2 destination on L1).
///   - `BRIDGE_SENDER=<addr>` — the user-facing L1 bridge entrypoint.
contract DeployBridgeL1 is Script {
    function run() external {
        address eezAddr = vm.envAddress("EEZ_REGISTRY_ADDRESS");
        address l2DestAddr = vm.envAddress("EEZ_L2_BRIDGE_RECEIVER");
        uint64 rollupId = SafeCast.toUint64(vm.envUint("EEZ_ROLLUP_ID"));

        EEZ eez = EEZ(eezAddr);

        vm.startBroadcast();

        // `createCrossChainProxy` is idempotent on the resulting address
        // (CREATE2 with `(l2Dest, rollupId)` as salt) — but the call can
        // revert if a proxy is already deployed there. Compute first;
        // create only if not already deployed.
        address l2Proxy = eez.computeCrossChainProxyAddress(l2DestAddr, rollupId);
        if (l2Proxy.code.length == 0) {
            address created = eez.createCrossChainProxy(l2DestAddr, rollupId);
            require(created == l2Proxy, "CrossChainProxy address drift");
        }

        BridgeSender sender = new BridgeSender(l2Proxy, l2DestAddr);

        vm.stopBroadcast();

        console.log("L2_PROXY=%s", l2Proxy);
        console.log("BRIDGE_SENDER=%s", address(sender));
        console.log("L2_DESTINATION=%s", l2DestAddr);
    }
}
