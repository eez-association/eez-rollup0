// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script} from "forge-std/Script.sol";
import {Vm} from "forge-std/Vm.sol";
import {console2 as console} from "forge-std/console2.sol";
import {ExecutionEntry as L2ExecutionEntry} from "src/interfaces/IEEZL2.sol";

/// Corrected L2 table verifier for sync-rollups-protocol 5c51e02.
///
/// The verifier at that commit omitted ExpectedLookup[] from the event topic
/// signature. The deployed EEZL2 contract includes it, so the original helper
/// never recognized ExecutionTableLoaded logs. This overlay changes only the
/// verifier; it does not alter the deployed protocol ABI.
contract VerifyL2Blocks5c is Script {
    bytes32 private constant SIG_TABLE_LOADED = keccak256(
        "ExecutionTableLoaded((bytes32,(address,uint256,bytes,address,uint256,uint256)[],(bytes32,uint256,bytes)[],(bytes32,bytes,bool,uint64,uint64,uint64,(address,uint256,bytes,address,uint256,uint256)[],(bytes32,uint256,bytes)[],uint256,bytes32)[],uint256,bytes,bytes32)[])"
    );

    function run(uint256[] calldata l2Blocks, address managerL2, bytes32[] calldata expectedEntryHashes)
        external
        view
    {
        bool[] memory found = new bool[](expectedEntryHashes.length);
        bytes32 tableTx;

        for (uint256 b = 0; b < l2Blocks.length; b++) {
            bytes32[] memory topics = new bytes32[](0);
            Vm.EthGetLogs[] memory logs = vm.eth_getLogs(l2Blocks[b], l2Blocks[b], managerL2, topics);
            for (uint256 i = 0; i < logs.length; i++) {
                if (logs[i].topics[0] != SIG_TABLE_LOADED) continue;
                tableTx = logs[i].transactionHash;
                L2ExecutionEntry[] memory entries = abi.decode(logs[i].data, (L2ExecutionEntry[]));
                for (uint256 e = 0; e < entries.length; e++) {
                    bytes32 actual = keccak256(abi.encode(entries[e].proxyEntryHash, entries[e].rollingHash));
                    for (uint256 x = 0; x < expectedEntryHashes.length; x++) {
                        if (actual == expectedEntryHashes[x]) found[x] = true;
                    }
                }
            }
        }

        for (uint256 i = 0; i < found.length; i++) {
            if (!found[i]) {
                console.log("FAIL: missing L2 entry %s", vm.toString(expectedEntryHashes[i]));
                revert("L2 table verification failed");
            }
        }

        console.log("PASS: %s/%s expected L2 entries verified", found.length, found.length);
        if (tableTx != bytes32(0)) console.log("L2_TABLE_TX=%s", vm.toString(tableTx));
    }
}
