// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";

import {EEZ} from "sync-rollups-protocol/src/EEZ.sol";

/// @title CreateValueProxy
/// @notice Calls `EEZ.createCrossChainProxy(originalAddress, originalRollupId)`
///         to register the `(Value, L2_ROLLUP_ID)` proxy on L1 and emit
///         `CrossChainProxyCreated`. The smoke harness pre-creates the
///         proxy rather than relying on lazy-on-fallback deployment so
///         that the L1 entry's `proxyEntryHash` lookup hits a
///         registered `authorizedProxies` slot immediately.
///
///         Outputs: EEZ_VALUE_PROXY=<addr>
///
///         Call shape:
///           forge script ... CreateValueProxy
///             --sig "run(address,address,uint64)" $EEZ $VALUE $L2_ROLLUP_ID
///             --rpc-url $L1_RPC --broadcast --private-key $PK
contract CreateValueProxy is Script {
    function run(address eez, address valueAddr, uint64 l2RollupId) external {
        vm.startBroadcast();
        address proxy = EEZ(eez).createCrossChainProxy(valueAddr, l2RollupId);
        vm.stopBroadcast();
        console.log("EEZ_VALUE_PROXY=%s", proxy);
    }
}
