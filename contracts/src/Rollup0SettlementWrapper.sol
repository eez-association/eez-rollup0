// SPDX-License-Identifier: MIT
pragma solidity 0.8.34;

import {ExecutionEntry, ProofSystemBatchPerVerificationEntries} from "eez-core-protocol/src/interfaces/IEEZ.sol";
import {Rollup0Manager} from "./Rollup0Manager.sol";

interface IRollup0EEZ {
    function postAndVerifyBatch(ProofSystemBatchPerVerificationEntries calldata batch) external;

    function rollups(uint64 rollupId) external view returns (address rollupContract, bytes32 root, uint256 etherBalance);
}

/// @title Rollup0SettlementWrapper
/// @notice On-chain carrier and anchor gate for Rollup0 blob settlement.
contract Rollup0SettlementWrapper {
    IRollup0EEZ public immutable EEZ;
    Rollup0Manager public immutable manager;

    bool private transient _entered;

    error ReentrantSettlement();
    error Rollup0NotIncluded();
    error InvalidBlockNumber();
    error InvalidImmediateEntryCount();
    error InvalidImmediateStaticEntryCount();
    error SenderBindingUnsupported();
    error NonEmptyCallData();
    error InvalidBlobIndices();
    error MissingBlob(uint256 index);
    error ExtraBlob(uint256 index);
    error InvalidAnchor();
    error StaleAnchor(bytes32 liveRoot, bytes32 claimedRoot);
    error UnchangedAnchorRoot();
    error AnchorNotApplied(bytes32 expectedRoot, bytes32 liveRoot);

    constructor(address eez, address rollupManager) {
        if (eez == address(0) || rollupManager == address(0)) revert InvalidAnchor();
        EEZ = IRollup0EEZ(eez);
        manager = Rollup0Manager(rollupManager);
    }

    /// @notice Validate and settle one Rollup0 candidate without changing the inner EEZ batch.
    function postAndVerifyBatch(ProofSystemBatchPerVerificationEntries calldata batch) external {
        if (_entered) revert ReentrantSettlement();
        _entered = true;

        uint64 rollupId = manager.rollupId();
        bool included;
        for (uint256 i = 0; i < batch.rollupIdsWithProofSystems.length; i++) {
            if (batch.rollupIdsWithProofSystems[i].rollupId == rollupId) {
                included = true;
                break;
            }
        }
        if (!included) revert Rollup0NotIncluded();
        if (batch.blockNumber != type(uint64).max) revert InvalidBlockNumber();
        if (batch.immediateEntryCount != 1) revert InvalidImmediateEntryCount();
        if (batch.immediateStaticEntryCount != 0) revert InvalidImmediateStaticEntryCount();
        if (batch.bindMsgSenderInPublicInput) revert SenderBindingUnsupported();
        if (batch.callData.length != 0) revert NonEmptyCallData();

        uint256 blobCount = batch.blobIndices.length;
        if (blobCount == 0) revert InvalidBlobIndices();
        for (uint256 i = 0; i < blobCount; i++) {
            if (batch.blobIndices[i] != i) revert InvalidBlobIndices();
            if (blobhash(i) == bytes32(0)) revert MissingBlob(i);
        }
        if (blobhash(blobCount) != bytes32(0)) revert ExtraBlob(blobCount);

        if (batch.entries.length == 0) revert InvalidAnchor();
        ExecutionEntry calldata anchor = batch.entries[0];
        // EEZ verifies the anchor's seeded rolling hash; the root checks below bound the rest.
        if (
            anchor.destinationRollupId != rollupId || anchor.proxyEntryHash != bytes32(0)
                || anchor.rollupUpdates.length != 1 || anchor.rollupUpdates[0].rollupId != rollupId
                || anchor.rollupUpdates[0].etherDelta != 0 || anchor.l2ToL1Calls.length != 0
                || anchor.expectedL1ToL2Calls.length != 0 || anchor.returnData.length != 0 || !anchor.success
        ) revert InvalidAnchor();

        (, bytes32 liveRoot,) = EEZ.rollups(rollupId);
        bytes32 currentRoot = anchor.rollupUpdates[0].currentRoot;
        bytes32 newRoot = anchor.rollupUpdates[0].newRoot;
        if (liveRoot != currentRoot) revert StaleAnchor(liveRoot, currentRoot);
        if (newRoot == liveRoot) revert UnchangedAnchorRoot();

        manager.beginSettlement();
        EEZ.postAndVerifyBatch(batch);
        (, liveRoot,) = EEZ.rollups(rollupId);
        if (liveRoot != newRoot) revert AnchorNotApplied(newRoot, liveRoot);
        manager.endSettlement();

        _entered = false;
    }
}
