// SPDX-License-Identifier: MIT
pragma solidity 0.8.34;

import {Test} from "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";

import {
    ExecutionEntry,
    ProofSystemBatchPerVerificationEntries,
    RollupIdWithProofSystems,
    RollupUpdate
} from "eez-core-protocol/src/interfaces/IEEZ.sol";
import {Rollup0Manager} from "../src/Rollup0Manager.sol";
import {Rollup0SettlementWrapper} from "../src/Rollup0SettlementWrapper.sol";

contract Rollup0SettlementTest is Test {
    uint64 private constant ROLLUP_ID = 7;
    uint256 private constant L2_CHAIN_ID = 12_345;
    bytes32 private constant PARENT = keccak256("parent");
    bytes32 private constant NEXT = keccak256("next");

    Rollup0Manager private manager;
    Rollup0SettlementWrapper private wrapper;
    bytes32 private _liveRoot;
    bytes private _observedCustomData;

    function setUp() external {
        Rollup0Manager implementation = new Rollup0Manager(address(this), L2_CHAIN_ID);
        address[] memory proofSystems = new address[](1);
        proofSystems[0] = address(0x1234);
        bytes32[] memory vkeys = new bytes32[](1);
        vkeys[0] = bytes32(uint256(1));
        ERC1967Proxy proxy = new ERC1967Proxy(
            address(implementation), abi.encodeCall(Rollup0Manager.initialize, (address(this), proofSystems, vkeys))
        );
        manager = Rollup0Manager(address(proxy));
        manager.rollupContractRegistered(ROLLUP_ID, address(this));
        wrapper = new Rollup0SettlementWrapper(address(this), address(manager));
        manager.setSettlementWrapper(address(wrapper));
        _liveRoot = PARENT;
    }

    function testThresholdAlwaysTracksActiveProofSystems() external {
        assertEq(manager.threshold(), 1);
        manager.addProofSystem(address(0x2345), bytes32(uint256(2)));
        assertEq(manager.threshold(), 2);
        manager.addProofSystem(address(0x3456), bytes32(uint256(3)));
        assertEq(manager.threshold(), 3);
        manager.addProofSystem(address(0x4567), bytes32(uint256(4)));
        assertEq(manager.threshold(), 3);
    }

    function testCandidateDomainMatchesManagerEncoding() external {
        manager.setSettlementWrapper(address(this));
        vm.roll(100);
        vm.warp(1_700_000_000);
        vm.setBlockhash(99, bytes32(uint256(0xBEEF)));
        manager.beginSettlement();

        bytes memory encoded = manager.getCustomData(type(uint64).max);
        (
            bytes32 tag,
            uint256 l1ChainId,
            address eez,
            address encodedManager,
            address encodedWrapper,
            uint256 rollupId,
            uint256 l2ChainId,
            uint256 timestamp,
            bytes32 parentHash
        ) = abi.decode(encoded, (bytes32, uint256, address, address, address, uint256, uint256, uint256, bytes32));

        assertEq(tag, manager.CANDIDATE_DOMAIN_TAG());
        assertEq(l1ChainId, block.chainid);
        assertEq(eez, address(this));
        assertEq(encodedManager, address(manager));
        assertEq(encodedWrapper, address(this));
        assertEq(rollupId, ROLLUP_ID);
        assertEq(l2ChainId, L2_CHAIN_ID);
        assertEq(timestamp, block.timestamp);
        assertEq(parentHash, bytes32(uint256(0xBEEF)));
        assertEq(encoded.length, 9 * 32);

        manager.endSettlement();
    }

    function testWrapperAppliesCanonicalAnchorWithExactBlobSet() external {
        bytes32[] memory hashes = new bytes32[](1);
        hashes[0] = bytes32(uint256(1));
        vm.blobhashes(hashes);

        wrapper.postAndVerifyBatch(_batch(1));

        assertEq(_liveRoot, NEXT);
        assertEq(_observedCustomData.length, 9 * 32);
        assertEq(manager.lastSettlementBlock(), block.number);
    }

    function testWrapperRejectsOutboundImmediatePrefix() external {
        ProofSystemBatchPerVerificationEntries memory batch = _batch(2);
        vm.expectRevert(Rollup0SettlementWrapper.InvalidImmediateEntryCount.selector);
        wrapper.postAndVerifyBatch(batch);
    }

    function testWrapperRejectsNonCandidateCarrierFields() external {
        ProofSystemBatchPerVerificationEntries memory batch = _batch(1);
        batch.blockNumber = 0;
        vm.expectRevert(Rollup0SettlementWrapper.InvalidBlockNumber.selector);
        wrapper.postAndVerifyBatch(batch);

        batch = _batch(1);
        batch.bindMsgSenderInPublicInput = true;
        vm.expectRevert(Rollup0SettlementWrapper.SenderBindingUnsupported.selector);
        wrapper.postAndVerifyBatch(batch);
    }

    function testWrapperRejectsMissingAndExtraBlobs() external {
        vm.expectRevert(abi.encodeWithSelector(Rollup0SettlementWrapper.MissingBlob.selector, 0));
        wrapper.postAndVerifyBatch(_batch(1));

        bytes32[] memory hashes = new bytes32[](2);
        hashes[0] = bytes32(uint256(1));
        hashes[1] = bytes32(uint256(2));
        vm.blobhashes(hashes);
        vm.expectRevert(abi.encodeWithSelector(Rollup0SettlementWrapper.ExtraBlob.selector, 1));
        wrapper.postAndVerifyBatch(_batch(1));
    }

    // Minimal EEZ surface used by the wrapper fixture.
    function rollups(uint64 id) external view returns (address rollupContract, bytes32 root, uint256 etherBalance) {
        assertEq(id, ROLLUP_ID);
        return (address(manager), _liveRoot, 0);
    }

    function postAndVerifyBatch(ProofSystemBatchPerVerificationEntries calldata batch) external {
        assertEq(msg.sender, address(wrapper));
        _observedCustomData = manager.getCustomData(batch.blockNumber);
        _liveRoot = batch.entries[0].rollupUpdates[0].newRoot;
    }

    function _batch(uint256 immediateCount) private pure returns (ProofSystemBatchPerVerificationEntries memory batch) {
        batch.blockNumber = type(uint64).max;
        batch.immediateEntryCount = immediateCount;
        batch.blobIndices = new uint256[](1);
        batch.blobIndices[0] = 0;
        batch.rollupIdsWithProofSystems = new RollupIdWithProofSystems[](1);
        batch.rollupIdsWithProofSystems[0].rollupId = ROLLUP_ID;
        batch.entries = new ExecutionEntry[](1);
        batch.entries[0].destinationRollupId = ROLLUP_ID;
        batch.entries[0].success = true;
        bytes32 statesHash = keccak256(abi.encodePacked(bytes32(0), ROLLUP_ID, PARENT));
        batch.entries[0].rollingHash = keccak256(abi.encodePacked(statesHash, bytes32(0)));
        batch.entries[0].rollupUpdates = new RollupUpdate[](1);
        batch.entries[0].rollupUpdates[0] =
            RollupUpdate({rollupId: ROLLUP_ID, etherDelta: 0, currentRoot: PARENT, newRoot: NEXT});
    }
}
