// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";

import {EEZ, ProofSystemBatchPerVerificationEntries, RollupIdWithProofSystems} from "eez-core-protocol/src/EEZ.sol";
import {Rollup} from "eez-core-protocol/src/rollupContract/Rollup.sol";
import {
    ExecutionEntry,
    ExpectedStateRootPerRollup,
    ExpectedL1ToL2Call,
    L2ToL1Call,
    StateUpdate,
    StaticExecutionEntry
} from "eez-core-protocol/src/interfaces/IEEZ.sol";
import {MockProofSystem} from "eez-core-protocol/test/mocks/MockProofSystem.sol";
import {ECDSAProofSystem} from "../src/ECDSAProofSystem.sol";

/// Solidity oracle for the two-stage public-input construction at the pinned
/// protocol revision. Each vector is also submitted to the real EEZ contract,
/// whose proof-system mock accepts only the oracle's expected hash.
contract PublicInputsHashVectorsTest is Test {
    bytes32 private constant EMPTY_ENTRY_SHARED = 0x9cb76f48824c4b86548d571badcc9a0542b46deb1bcbff5ecfa63b60d113a3dd;
    bytes32 private constant EMPTY_ENTRY_PUBLIC_INPUT =
        0xd27632863985c23ae62bb5420b9b1b8d01ac1e64e31d4367cc61b46db364b49f;
    bytes32 private constant FULL_ENTRY_HASH = 0x2c4c8cbc9b39743790f04a13406c6c0e3ab6ca0bf5acb3b923f5549d3aabb759;
    bytes32 private constant FULL_ENTRY_SHARED = 0xe5764fb1e66c094d0e624344415ffaf8d42687ef904dc61ea5b587f5d3e8b6a0;
    bytes32 private constant FULL_ENTRY_PUBLIC_INPUT =
        0x122a843ef260afd29aac762668d6f0c0f6f0a19c48734107ba770a8b81e1b8b0;
    bytes32 private constant MULTI_PS_SHARED = 0x58a63c74be1cc3cbd8a0dc74bf4c862a0b18e5013ef0eacec2bf2e319398d718;
    bytes32 private constant MULTI_PS_PUBLIC_INPUT_0 =
        0xb2121e16632ffd6860732ebe2af1a91ac4225b674667b95b3c8d5631598a7cb9;
    bytes32 private constant MULTI_PS_PUBLIC_INPUT_1 =
        0x3c9874af807b93a58190c46b40539cee1e70563191f9d809ed0220c03ace0c24;
    bytes32 private constant MULTI_PS_PUBLIC_INPUT_2 =
        0x70f74b384fe5841518e09eadcd178c46e6a7f62f9ca651e30c600ee23bb41a42;
    bytes32 private constant ALL_DIMENSIONS_STATIC_ENTRY_HASH =
        0x1a63bcaad1cc1d18331cee8e48f0074de3a9f1f887255d3dfdf44f62a08036c3;
    bytes32 private constant ALL_DIMENSIONS_BLOB_HASH = bytes32(uint256(0xB10B));
    bytes32 private constant ALL_DIMENSIONS_CUSTOM_BLOCK_HASH = bytes32(uint256(0xC0570D));
    bytes32 private constant ALL_DIMENSIONS_SHARED = 0x63337a6c8300ec03d53497260962ca38155db08c6b233088078cb26caf172845;
    bytes32 private constant ALL_DIMENSIONS_PUBLIC_INPUT =
        0x18742f97fac6fcd3ca33e57739399b92eb55a4cf3465327a9dd274199ef677ef;

    function testEmptyEntryVector() external {
        EEZ eez = new EEZ(address(0xDEAD));
        MockProofSystem proofSystem = new MockProofSystem();
        uint64 rollupId = _registerRollup(eez, _singleton(proofSystem), _singleton(bytes32(uint256(0x42))));

        ProofSystemBatchPerVerificationEntries memory batch = _emptyBatch();
        batch.proofSystems = _addresses(_singleton(proofSystem));
        batch.proofs = _proofs(1);
        batch.rollupIdsWithProofSystems = _assignments1(rollupId, 0);

        bytes32 shared = _shared(batch, _emptyCustomData(1), address(0));
        bytes32 publicInput = _publicInput(shared, _singleton(rollupId), _singleton(bytes32(uint256(0x42))));

        assertEq(shared, EMPTY_ENTRY_SHARED);
        assertEq(publicInput, EMPTY_ENTRY_PUBLIC_INPUT);

        proofSystem.setExpectedPublicInputsHash(publicInput);
        eez.postAndVerifyBatch(batch);
    }

    function testFullEntryVector() external {
        EEZ eez = new EEZ(address(0xDEAD));
        MockProofSystem proofSystem = new MockProofSystem();
        bytes32 initialState = bytes32(uint256(0x1111));
        uint64 rollupId =
            _registerRollup(eez, _singleton(proofSystem), _singleton(bytes32(uint256(0x42))), initialState);

        ProofSystemBatchPerVerificationEntries memory batch = _emptyBatch();
        batch.entries = new ExecutionEntry[](1);
        batch.entries[0] = _entry(rollupId, initialState);
        batch.callData = hex"0102030405";
        batch.proofSystems = _addresses(_singleton(proofSystem));
        batch.proofs = _proofs(1);
        batch.rollupIdsWithProofSystems = _assignments1(rollupId, 0);

        bytes32 entryHash = keccak256(abi.encode(batch.entries[0]));
        bytes32 shared = _shared(batch, _emptyCustomData(1), address(0));
        bytes32 publicInput = _publicInput(shared, _singleton(rollupId), _singleton(bytes32(uint256(0x42))));

        assertEq(entryHash, FULL_ENTRY_HASH);
        assertEq(shared, FULL_ENTRY_SHARED);
        assertEq(publicInput, FULL_ENTRY_PUBLIC_INPUT);

        proofSystem.setExpectedPublicInputsHash(publicInput);
        eez.postAndVerifyBatch(batch);
    }

    function testMultiRollupMultiProofSystemVector() external {
        EEZ eez = new EEZ(address(0xDEAD));
        MockProofSystem[] memory systems = _sortedProofSystems();

        MockProofSystem[] memory rollup1Systems = new MockProofSystem[](2);
        rollup1Systems[0] = systems[0];
        rollup1Systems[1] = systems[2];
        bytes32[] memory rollup1Vkeys = new bytes32[](2);
        rollup1Vkeys[0] = bytes32(uint256(0x42));
        rollup1Vkeys[1] = bytes32(uint256(0x99));
        uint64 rollup1 = _registerRollup(eez, rollup1Systems, rollup1Vkeys);

        MockProofSystem[] memory rollup2Systems = new MockProofSystem[](2);
        rollup2Systems[0] = systems[1];
        rollup2Systems[1] = systems[2];
        bytes32[] memory rollup2Vkeys = new bytes32[](2);
        rollup2Vkeys[0] = bytes32(uint256(0x43));
        rollup2Vkeys[1] = bytes32(uint256(0x44));
        uint64 rollup2 = _registerRollup(eez, rollup2Systems, rollup2Vkeys);

        ProofSystemBatchPerVerificationEntries memory batch = _emptyBatch();
        batch.callData = hex"aabbcc";
        batch.proofSystems = _addresses(systems);
        batch.proofs = _proofs(3);
        batch.rollupIdsWithProofSystems = _assignments2(rollup1, rollup2);

        bytes32 shared = _shared(batch, _emptyCustomData(2), address(0));
        bytes32 publicInput0 = _publicInput(shared, _singleton(rollup1), _singleton(rollup1Vkeys[0]));
        bytes32 publicInput1 = _publicInput(shared, _singleton(rollup2), _singleton(rollup2Vkeys[0]));
        uint64[] memory ps2Rollups = new uint64[](2);
        ps2Rollups[0] = rollup1;
        ps2Rollups[1] = rollup2;
        bytes32[] memory ps2Vkeys = new bytes32[](2);
        ps2Vkeys[0] = rollup1Vkeys[1];
        ps2Vkeys[1] = rollup2Vkeys[1];
        bytes32 publicInput2 = _publicInput(shared, ps2Rollups, ps2Vkeys);

        assertEq(shared, MULTI_PS_SHARED);
        assertEq(publicInput0, MULTI_PS_PUBLIC_INPUT_0);
        assertEq(publicInput1, MULTI_PS_PUBLIC_INPUT_1);
        assertEq(publicInput2, MULTI_PS_PUBLIC_INPUT_2);

        systems[0].setExpectedPublicInputsHash(publicInput0);
        systems[1].setExpectedPublicInputsHash(publicInput1);
        systems[2].setExpectedPublicInputsHash(publicInput2);
        eez.postAndVerifyBatch(batch);
    }

    function testAllSharedInputDimensionsVector() external {
        EEZ eez = new EEZ(address(0xDEAD));
        MockProofSystem proofSystem = new MockProofSystem();
        bytes32 initialState = bytes32(uint256(0x1111));
        uint64 rollupId =
            _registerRollup(eez, _singleton(proofSystem), _singleton(bytes32(uint256(0x42))), initialState);

        StaticExecutionEntry memory staticEntry;
        staticEntry.expectedStateRoots = new ExpectedStateRootPerRollup[](1);
        staticEntry.expectedStateRoots[0] = ExpectedStateRootPerRollup({rollupId: rollupId, stateRoot: initialState});
        staticEntry.proxyEntryHash = bytes32(uint256(0x5555));
        staticEntry.l2ToL1Calls = new L2ToL1Call[](0);
        staticEntry.rollingHash = bytes32(uint256(0x6666));
        staticEntry.destinationRollupId = rollupId;
        staticEntry.success = true;
        staticEntry.returnData = hex"cafe";

        ProofSystemBatchPerVerificationEntries memory batch = _emptyBatch();
        batch.staticEntries = new StaticExecutionEntry[](1);
        batch.staticEntries[0] = staticEntry;
        batch.blobIndices = new uint256[](1);
        batch.blobIndices[0] = 0;
        batch.callData = hex"a1b2c3";
        batch.proofSystems = _addresses(_singleton(proofSystem));
        batch.proofs = _proofs(1);
        batch.rollupIdsWithProofSystems = _assignments1(rollupId, 0);
        batch.blockNumber = 50;
        batch.bindMsgSenderInPublicInput = true;

        bytes32[] memory blobHashes = _singleton(ALL_DIMENSIONS_BLOB_HASH);
        bytes[] memory customData = new bytes[](1);
        customData[0] = abi.encode(uint256(0), ALL_DIMENSIONS_CUSTOM_BLOCK_HASH);
        address boundSender = address(0xBEEF);

        bytes32 staticEntryHash = keccak256(abi.encode(staticEntry));
        bytes32 shared = _shared(batch, blobHashes, customData, boundSender);
        bytes32 publicInput = _publicInput(shared, _singleton(rollupId), _singleton(bytes32(uint256(0x42))));

        assertEq(staticEntryHash, ALL_DIMENSIONS_STATIC_ENTRY_HASH);
        assertEq(shared, ALL_DIMENSIONS_SHARED);
        assertEq(publicInput, ALL_DIMENSIONS_PUBLIC_INPUT);

        vm.roll(100);
        vm.setBlockhash(50, ALL_DIMENSIONS_CUSTOM_BLOCK_HASH);
        vm.blobhashes(blobHashes);
        proofSystem.setExpectedPublicInputsHash(publicInput);
        vm.prank(boundSender);
        eez.postAndVerifyBatch(batch);
    }

    function testECDSAProofCannotBeReusedForDifferentCallData() external {
        uint256 signerKey = 0xA11CE;
        address signer = vm.addr(signerKey);
        bytes32 vkey = bytes32(uint256(uint160(signer)));

        EEZ eez = new EEZ(address(0xDEAD));
        ECDSAProofSystem proofSystem = new ECDSAProofSystem(signer);
        address[] memory proofSystems = new address[](1);
        proofSystems[0] = address(proofSystem);
        bytes32[] memory vkeys = _singleton(vkey);
        Rollup rollup = new Rollup(address(eez), address(this), 1, proofSystems, vkeys);
        uint64 rollupId = eez.registerRollup(address(rollup), bytes32(uint256(0x1111)));

        ProofSystemBatchPerVerificationEntries memory batch = _emptyBatch();
        batch.proofSystems = proofSystems;
        batch.proofs = new bytes[](1);
        batch.rollupIdsWithProofSystems = _assignments1(rollupId, 0);

        bytes32 shared = _shared(batch, _emptyCustomData(1), address(0));
        bytes32 signedHash = _publicInput(shared, _singleton(rollupId), vkeys);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(signerKey, signedHash);
        bytes memory signature = abi.encodePacked(r, s, v);

        batch.proofs[0] = signature;
        eez.postAndVerifyBatch(batch);

        batch.callData = hex"01";
        vm.expectRevert(EEZ.InvalidProof.selector);
        eez.postAndVerifyBatch(batch);
    }

    /// `immediateEntryCount` is an intentional poster-controlled scheduling
    /// parameter: it changes dispatch while remaining outside the proof preimage.
    function testImmediateEntryCountCanDropADeferredEntryWithoutChangingPublicInput() external {
        MockProofSystem proofSystem = new MockProofSystem();
        bytes32 vkey = bytes32(uint256(0x42));
        bytes32 initialState = bytes32(uint256(0x1111));
        bytes32 anchorState = bytes32(uint256(0x2222));

        EEZ canonicalEez = new EEZ(address(0xDEAD));
        uint64 canonicalRollup =
            _registerRollup(canonicalEez, _singleton(proofSystem), _singleton(vkey), initialState);
        ProofSystemBatchPerVerificationEntries memory canonical =
            _schedulerBatch(proofSystem, canonicalRollup, initialState, anchorState, 1);

        EEZ mutatedEez = new EEZ(address(0xDEAD));
        uint64 mutatedRollup =
            _registerRollup(mutatedEez, _singleton(proofSystem), _singleton(vkey), initialState);
        ProofSystemBatchPerVerificationEntries memory mutated =
            _schedulerBatch(proofSystem, mutatedRollup, initialState, anchorState, 2);

        assertEq(canonicalRollup, mutatedRollup);
        assertNotEq(keccak256(abi.encode(canonical)), keccak256(abi.encode(mutated)));

        bytes32 canonicalShared = _shared(canonical, _emptyCustomData(1), address(0));
        bytes32 mutatedShared = _shared(mutated, _emptyCustomData(1), address(0));
        bytes32 canonicalPublicInput = _publicInput(
            canonicalShared, _singleton(canonicalRollup), _singleton(vkey)
        );
        bytes32 mutatedPublicInput =
            _publicInput(mutatedShared, _singleton(mutatedRollup), _singleton(vkey));
        assertEq(canonicalShared, mutatedShared);
        assertEq(canonicalPublicInput, mutatedPublicInput);

        proofSystem.setExpectedPublicInputsHash(canonicalPublicInput);

        address poster = address(0xBEEF);
        vm.prank(poster);
        canonicalEez.postAndVerifyBatch(canonical);
        assertEq(canonicalEez.queueLength(canonicalRollup), 1);

        vm.prank(poster);
        mutatedEez.postAndVerifyBatch(mutated);
        assertEq(mutatedEez.queueLength(mutatedRollup), 0);

        (, bytes32 canonicalState,) = canonicalEez.rollups(canonicalRollup);
        (, bytes32 mutatedState,) = mutatedEez.rollups(mutatedRollup);
        assertEq(canonicalState, anchorState);
        assertEq(mutatedState, anchorState);
    }

    function _shared(
        ProofSystemBatchPerVerificationEntries memory batch,
        bytes[] memory customData,
        address boundSender
    )
        private
        pure
        returns (bytes32)
    {
        return _shared(batch, new bytes32[](0), customData, boundSender);
    }

    function _shared(
        ProofSystemBatchPerVerificationEntries memory batch,
        bytes32[] memory blobHashes,
        bytes[] memory customData,
        address boundSender
    )
        private
        pure
        returns (bytes32)
    {
        bytes32[] memory entryHashes = new bytes32[](batch.entries.length);
        for (uint256 i = 0; i < batch.entries.length; i++) {
            entryHashes[i] = keccak256(abi.encode(batch.entries[i]));
        }
        bytes32[] memory staticEntryHashes = new bytes32[](batch.staticEntries.length);
        for (uint256 i = 0; i < batch.staticEntries.length; i++) {
            staticEntryHashes[i] = keccak256(abi.encode(batch.staticEntries[i]));
        }
        bytes32[] memory customDataHashes = new bytes32[](batch.rollupIdsWithProofSystems.length);
        for (uint256 i = 0; i < customData.length; i++) {
            customDataHashes[i] = keccak256(abi.encode(batch.rollupIdsWithProofSystems[i].rollupId, customData[i]));
        }
        return keccak256(
            abi.encodePacked(
                abi.encode(entryHashes),
                abi.encode(staticEntryHashes),
                abi.encode(blobHashes),
                keccak256(batch.callData),
                abi.encode(customDataHashes),
                boundSender
            )
        );
    }

    function _publicInput(
        bytes32 shared,
        uint64[] memory rollupIds,
        bytes32[] memory vkeys
    )
        private
        pure
        returns (bytes32)
    {
        bytes32 acc;
        for (uint256 i = 0; i < rollupIds.length; i++) {
            acc = keccak256(abi.encode(acc, rollupIds[i], vkeys[i]));
        }
        return keccak256(abi.encodePacked(shared, acc));
    }

    function _emptyBatch() private pure returns (ProofSystemBatchPerVerificationEntries memory batch) {
        batch.expectedStateRootPerRollup = new ExpectedStateRootPerRollup[](0);
        batch.entries = new ExecutionEntry[](0);
        batch.staticEntries = new StaticExecutionEntry[](0);
        batch.blobIndices = new uint256[](0);
        batch.blockNumber = 0;
        batch.bindMsgSenderInPublicInput = false;
    }

    function _entry(uint64 rollupId, bytes32 currentState) private pure returns (ExecutionEntry memory entry) {
        entry.stateUpdates = new StateUpdate[](1);
        entry.stateUpdates[0] = StateUpdate({
            rollupId: rollupId, currentState: currentState, newState: bytes32(uint256(0x2222)), etherDelta: 0
        });
        entry.proxyEntryHash = bytes32(uint256(0x3333));
        entry.l2ToL1Calls = new L2ToL1Call[](0);
        entry.expectedL1ToL2Calls = new ExpectedL1ToL2Call[](0);
        entry.rollingHash = bytes32(uint256(0x4444));
        entry.destinationRollupId = rollupId;
        entry.success = true;
        entry.returnData = hex"deadbeef";
    }

    function _schedulerBatch(
        MockProofSystem proofSystem,
        uint64 rollupId,
        bytes32 initialState,
        bytes32 anchorState,
        uint256 immediateEntryCount
    )
        private
        pure
        returns (ProofSystemBatchPerVerificationEntries memory batch)
    {
        batch = _emptyBatch();
        batch.entries = new ExecutionEntry[](2);
        batch.entries[0] = _schedulerEntry(rollupId, initialState, anchorState, bytes32(0));
        batch.entries[1] = _schedulerEntry(
            rollupId, anchorState, bytes32(uint256(0x3333)), bytes32(uint256(0x4444))
        );
        batch.immediateEntryCount = immediateEntryCount;
        batch.proofSystems = _addresses(_singleton(proofSystem));
        batch.proofs = _proofs(1);
        batch.rollupIdsWithProofSystems = _assignments1(rollupId, 0);
    }

    function _schedulerEntry(
        uint64 rollupId,
        bytes32 currentState,
        bytes32 newState,
        bytes32 proxyEntryHash
    )
        private
        pure
        returns (ExecutionEntry memory entry)
    {
        entry.stateUpdates = new StateUpdate[](1);
        entry.stateUpdates[0] = StateUpdate({
            rollupId: rollupId,
            currentState: currentState,
            newState: newState,
            etherDelta: 0
        });
        entry.proxyEntryHash = proxyEntryHash;
        entry.l2ToL1Calls = new L2ToL1Call[](0);
        entry.expectedL1ToL2Calls = new ExpectedL1ToL2Call[](0);
        bytes32 statesHash = keccak256(abi.encodePacked(bytes32(0), rollupId, currentState));
        entry.rollingHash = keccak256(abi.encodePacked(statesHash, proxyEntryHash));
        entry.destinationRollupId = rollupId;
        entry.success = true;
        entry.returnData = "";
    }

    function _registerRollup(
        EEZ eez,
        MockProofSystem[] memory systems,
        bytes32[] memory vkeys
    )
        private
        returns (uint64)
    {
        return _registerRollup(eez, systems, vkeys, bytes32(uint256(0x1111)));
    }

    function _registerRollup(
        EEZ eez,
        MockProofSystem[] memory systems,
        bytes32[] memory vkeys,
        bytes32 initialState
    )
        private
        returns (uint64)
    {
        Rollup rollup = new Rollup(address(eez), address(this), 1, _addresses(systems), vkeys);
        return eez.registerRollup(address(rollup), initialState);
    }

    function _sortedProofSystems() private returns (MockProofSystem[] memory systems) {
        systems = new MockProofSystem[](3);
        systems[0] = new MockProofSystem();
        systems[1] = new MockProofSystem();
        systems[2] = new MockProofSystem();
        for (uint256 i = 0; i < systems.length; i++) {
            for (uint256 j = i + 1; j < systems.length; j++) {
                if (address(systems[j]) < address(systems[i])) {
                    (systems[i], systems[j]) = (systems[j], systems[i]);
                }
            }
        }
    }

    function _addresses(MockProofSystem[] memory systems) private pure returns (address[] memory addresses) {
        addresses = new address[](systems.length);
        for (uint256 i = 0; i < systems.length; i++) {
            addresses[i] = address(systems[i]);
        }
    }

    function _proofs(uint256 length) private pure returns (bytes[] memory proofs) {
        proofs = new bytes[](length);
        for (uint256 i = 0; i < length; i++) {
            proofs[i] = "proof";
        }
    }

    function _emptyCustomData(uint256 length) private pure returns (bytes[] memory customData) {
        customData = new bytes[](length);
    }

    function _assignments1(
        uint64 rollupId,
        uint64 psIndex
    )
        private
        pure
        returns (RollupIdWithProofSystems[] memory assignments)
    {
        assignments = new RollupIdWithProofSystems[](1);
        assignments[0] = RollupIdWithProofSystems({rollupId: rollupId, proofSystemIndexes: _singleton(psIndex)});
    }

    function _assignments2(
        uint64 rollup1,
        uint64 rollup2
    )
        private
        pure
        returns (RollupIdWithProofSystems[] memory assignments)
    {
        assignments = new RollupIdWithProofSystems[](2);
        uint64[] memory indexes1 = new uint64[](2);
        indexes1[0] = 0;
        indexes1[1] = 2;
        assignments[0] = RollupIdWithProofSystems({rollupId: rollup1, proofSystemIndexes: indexes1});
        uint64[] memory indexes2 = new uint64[](2);
        indexes2[0] = 1;
        indexes2[1] = 2;
        assignments[1] = RollupIdWithProofSystems({rollupId: rollup2, proofSystemIndexes: indexes2});
    }

    function _singleton(MockProofSystem value) private pure returns (MockProofSystem[] memory values) {
        values = new MockProofSystem[](1);
        values[0] = value;
    }

    function _singleton(bytes32 value) private pure returns (bytes32[] memory values) {
        values = new bytes32[](1);
        values[0] = value;
    }

    function _singleton(uint64 value) private pure returns (uint64[] memory values) {
        values = new uint64[](1);
        values[0] = value;
    }
}
