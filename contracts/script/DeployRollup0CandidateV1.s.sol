// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Script, console} from "forge-std/Script.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

import {Rollup0Manager} from "../src/Rollup0Manager.sol";
import {Rollup0SettlementWrapper} from "../src/Rollup0SettlementWrapper.sol";

/// @title DeployRollup0CandidateV1
/// @notice Deploys the blob-settlement manager and wrapper for integration work.
/// @dev Do not use this deployment with the calldata composer. Candidate V1
///      requires blob batches posted through the wrapper.
contract DeployRollup0CandidateV1 is Script {
    error InvalidProofSystem();

    function run(
        address eez,
        address proofSystem,
        address authorizedSigner,
        address owner,
        uint256 l2ChainId
    )
        external
    {
        if (proofSystem == address(0)) revert InvalidProofSystem();

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = proofSystem;
        bytes32[] memory vkeys = new bytes32[](1);
        vkeys[0] = bytes32(uint256(uint160(authorizedSigner)));

        vm.startBroadcast();
        Rollup0Manager implementation = new Rollup0Manager(eez, l2ChainId);
        ERC1967Proxy proxy = new ERC1967Proxy(
            address(implementation), abi.encodeCall(Rollup0Manager.initialize, (owner, proofSystems, vkeys))
        );
        Rollup0Manager manager = Rollup0Manager(address(proxy));
        Rollup0SettlementWrapper wrapper = new Rollup0SettlementWrapper(eez, address(manager));
        manager.setSettlementWrapper(address(wrapper));
        vm.stopBroadcast();

        console.log("ROLLUP_CONTRACT=%s", address(manager));
        console.log("SETTLEMENT_WRAPPER=%s", address(wrapper));
    }
}
