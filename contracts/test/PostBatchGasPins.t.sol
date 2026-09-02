// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test, console} from "forge-std/Test.sol";

import {EEZ} from "eez-core-protocol/src/EEZ.sol";
import {
    ExecutionEntry,
    ExpectedL1ToL2Call,
    ExpectedStateRootPerRollup,
    L2ToL1Call,
    ProofSystemBatchPerVerificationEntries,
    RollupIdWithProofSystems,
    StateUpdate,
    StaticExecutionEntry
} from "eez-core-protocol/src/interfaces/IEEZ.sol";
import {Rollup} from "eez-core-protocol/src/rollupContract/Rollup.sol";

import {MockECDSAProofSystem} from "../src/MockECDSAProofSystem.sol";

/// Empty body, so the per-entry marginal prices EEZ.sol's own bookkeeping
/// instead of the callee's work.
contract EmptyTarget {
    function ping() external {}
}

/// Measures from a caller frame so intrinsic and calldata gas stay out; the
/// Rust side prices those with exact EIP-7623 arithmetic.
///
/// Raw `call`, not a typed one: re-encoding a 51-entry struct would bill the
/// ABI encoder to the per-entry marginal.
contract PostBatchGasProbe {
    function measure(address eez, bytes calldata postCall) external returns (uint256 gasUsed) {
        uint256 start = gasleft();
        (bool ok, bytes memory revertData) = eez.call(postCall);
        gasUsed = start - gasleft();
        if (!ok) {
            assembly {
                revert(add(revertData, 0x20), mload(revertData))
            }
        }
    }
}

/// @notice Pins the two gas constants the composer's drain projects with, so CI
///         fails as soon as EEZ.sol's real cost drifts past a pin.
///
/// @dev Measures `postAndVerifyBatch` on a composer-shaped batch: one leading
///      anchor entry plus N outbound entries, each with a chained state delta.
///
///      Several rung sizes catch nonlinearity from memory growth; the pins must
///      cover the base and the WORST per-entry marginal on the ladder.
contract PostBatchGasPinsTest is Test {
    /// Mirrors `POSTBATCH_BASE_GAS_PIN` in `crates/eez-composer/src/composer.rs`.
    /// Measured 139_129; pinned at measured x 1.10 rounded up.
    uint256 private constant POSTBATCH_BASE_GAS_PIN = 160_000;

    /// Mirrors `POSTBATCH_ENTRY_GAS_PIN` in `crates/eez-composer/src/composer.rs`.
    /// Measured 333_730 (worst rung); pinned at measured x 1.10 rounded up.
    uint256 private constant POSTBATCH_ENTRY_GAS_PIN = 370_000;

    uint64 private constant ROLLUP_ID = 1;
    uint256 private constant PROVER_KEY = 0xA11CE;
    bytes32 private constant GENESIS_ROOT = keccak256("postbatch gas pin genesis");

    PostBatchGasProbe private probe;

    /// Accounts touched while building a rung; all cooled before the measured
    /// call, because EIP-2929 warm access is far cheaper and a real batch is cold.
    address[] private touched;

    function setUp() external {
        probe = new PostBatchGasProbe();
    }

    function testPostBatchGasPinsCoverTheLadder() external {
        uint256[4] memory ladder = [uint256(1), 2, 10, 50];
        uint256 base = _rung(0, true);
        console.log("base (leading immediate only):", base);

        uint256 worstMarginal;
        // A sender's first outbound settlement also CREATE2-deploys its source
        // proxy in the batch, which costs more than the entry's own bookkeeping.
        //
        // The compose-time probe deploys that proxy in a separate frame, so its
        // gas never reaches `target_gas` and the entry pin must absorb it.
        //
        // Both ladders run, so the pin has to hold for the cheap established
        // sender and for the costly first-ever one.
        for (uint256 proxies = 0; proxies < 2; proxies++) {
            bool preCreated = proxies == 0;
            console.log(preCreated ? "-- established source proxies" : "-- first-ever source proxies");
            for (uint256 i = 0; i < ladder.length; i++) {
                uint256 gasUsed = _rung(ladder[i], preCreated);
                uint256 marginal = (gasUsed - base) / ladder[i];
                console.log("outbound entries:", ladder[i]);
                console.log("  gas:", gasUsed);
                console.log("  marginal per entry:", marginal);
                if (marginal > worstMarginal) worstMarginal = marginal;
            }
        }

        console.log("worst marginal:", worstMarginal);
        // The 10% slack covers what this ladder cannot show: the DA payload's
        // `keccak256` and memory growth (no `callData` here), plus solc drift.
        //
        // The inner target call sits inside the marginal, and at runtime the
        // composer also charges its probed `target_gas`, so it is paid twice.
        assertLe(base, POSTBATCH_BASE_GAS_PIN, "observed base exceeds POSTBATCH_BASE_GAS_PIN");
        assertLe(worstMarginal, POSTBATCH_ENTRY_GAS_PIN, "observed marginal exceeds POSTBATCH_ENTRY_GAS_PIN");
    }

    /// A fresh deployment per rung, so every rung pays the same cold start and
    /// the marginal is a clean difference.
    ///
    /// `preCreateProxies` off measures a sender's first settlement, with the
    /// proxy deployed inside the batch.
    function _rung(uint256 outboundEntries, bool preCreateProxies) private returns (uint256 gasUsed) {
        delete touched;
        (EEZ eez, MockECDSAProofSystem proofSystem) = _deployProtocol();
        EmptyTarget target = new EmptyTarget();
        touched.push(address(target));

        bytes memory postCall = _postCall(eez, proofSystem, address(target), outboundEntries, preCreateProxies);
        for (uint256 i = 0; i < touched.length; i++) {
            vm.cool(touched[i]);
        }
        gasUsed = probe.measure(address(eez), postCall);
    }

    /// Mirrors `scripts/deploy.sh`: registry, proof system, threshold-1 manager
    /// whose vkey is the signer-address membership ticket, then `registerRollup`.
    function _deployProtocol() private returns (EEZ eez, MockECDSAProofSystem proofSystem) {
        address prover = vm.addr(PROVER_KEY);
        eez = new EEZ(address(0xDEAD));
        proofSystem = new MockECDSAProofSystem(prover);

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = address(proofSystem);
        bytes32[] memory vkeys = new bytes32[](1);
        vkeys[0] = bytes32(uint256(uint160(prover)));
        Rollup manager = new Rollup(address(eez), address(this), 1, proofSystems, vkeys);

        assertEq(eez.registerRollup(address(manager), GENESIS_ROOT), ROLLUP_ID, "unexpected rollup id");
        touched.push(address(eez));
        touched.push(address(proofSystem));
        touched.push(address(manager));
    }

    /// A composer-shaped call: the leading anchor plus `outboundEntries` entries,
    /// all immediate, with state deltas chained from the live root.
    function _postCall(
        EEZ eez,
        MockECDSAProofSystem proofSystem,
        address target,
        uint256 outboundEntries,
        bool preCreateProxies
    )
        private
        returns (bytes memory)
    {
        (, bytes32 root,) = eez.rollups(ROLLUP_ID);
        ExecutionEntry[] memory entries = new ExecutionEntry[](1 + outboundEntries);

        bytes32 next = keccak256(abi.encodePacked(root));
        entries[0] = _entry(eez, root, next, new L2ToL1Call[](0));
        root = next;

        for (uint256 i = 0; i < outboundEntries; i++) {
            // A distinct sender per entry: shared senders would warm the proxy
            // account and understate the marginal.
            address source = address(uint160(0x5000 + i));
            L2ToL1Call[] memory calls = new L2ToL1Call[](1);
            calls[0] = L2ToL1Call({
                revertNextNCalls: 0,
                isStatic: false,
                gas: 0,
                sourceAddress: source,
                sourceRollupId: ROLLUP_ID,
                targetAddress: target,
                value: 0,
                data: abi.encodeCall(EmptyTarget.ping, ())
            });
            next = keccak256(abi.encodePacked(root));
            entries[1 + i] = _entry(eez, root, next, calls);
            root = next;
            if (preCreateProxies) {
                touched.push(eez.createCrossChainProxy(source, ROLLUP_ID));
            }
        }

        address[] memory proofSystems = new address[](1);
        proofSystems[0] = address(proofSystem);
        RollupIdWithProofSystems[] memory rollupIds = new RollupIdWithProofSystems[](1);
        uint64[] memory indexes = new uint64[](1);
        rollupIds[0] = RollupIdWithProofSystems({rollupId: ROLLUP_ID, proofSystemIndexes: indexes});
        bytes[] memory proofs = new bytes[](1);
        proofs[0] = _proof(proofSystem);

        ProofSystemBatchPerVerificationEntries memory batch = ProofSystemBatchPerVerificationEntries({
            expectedStateRootPerRollup: new ExpectedStateRootPerRollup[](0),
            entries: entries,
            staticEntries: new StaticExecutionEntry[](0),
            immediateEntryCount: entries.length,
            immediateStaticEntryCount: 0,
            proofSystems: proofSystems,
            rollupIdsWithProofSystems: rollupIds,
            blobIndices: new uint256[](0),
            callData: "",
            proofs: proofs,
            blockNumber: 0,
            bindMsgSenderInPublicInput: false
        });
        return abi.encodeCall(EEZ.postAndVerifyBatch, (batch));
    }

    /// An outbound settlement entry: `proxyEntryHash == 0` so EEZ.sol drains it
    /// inline, one state delta chaining `currentState` to the prior `newState`.
    function _entry(
        EEZ eez,
        bytes32 currentState,
        bytes32 newState,
        L2ToL1Call[] memory calls
    )
        private
        view
        returns (ExecutionEntry memory)
    {
        StateUpdate[] memory updates = new StateUpdate[](1);
        updates[0] = StateUpdate({rollupId: ROLLUP_ID, currentState: currentState, newState: newState, etherDelta: 0});
        return ExecutionEntry({
            stateUpdates: updates,
            proxyEntryHash: bytes32(0),
            l2ToL1Calls: calls,
            expectedL1ToL2Calls: new ExpectedL1ToL2Call[](0),
            rollingHash: _rollingHash(eez, updates, calls),
            destinationRollupId: ROLLUP_ID,
            success: true,
            returnData: ""
        });
    }

    /// EEZBase's tagged fold for this shape: the entry seed, then CALL_BEGIN and
    /// CALL_END per call. Every call succeeds with empty return data.
    function _rollingHash(
        EEZ eez,
        StateUpdate[] memory updates,
        L2ToL1Call[] memory calls
    )
        private
        view
        returns (bytes32 hash)
    {
        for (uint256 i = 0; i < updates.length; i++) {
            hash = keccak256(abi.encodePacked(hash, updates[i].rollupId, updates[i].currentState));
        }
        hash = keccak256(abi.encodePacked(hash, bytes32(0))); // proxyEntryHash
        for (uint256 i = 0; i < calls.length; i++) {
            bytes32 callHash = eez.computeCrossChainCallHash(
                false,
                calls[i].sourceAddress,
                calls[i].sourceRollupId,
                calls[i].targetAddress,
                eez.MAINNET_ROLLUP_ID(),
                calls[i].value,
                0,
                calls[i].data
            );
            hash = keccak256(abi.encodePacked(hash, uint8(1), callHash)); // CALL_BEGIN
            hash = keccak256(abi.encodePacked(hash, uint8(2), true)); // CALL_END(true, "")
        }
    }

    /// `MockECDSAProofSystem` recovers against a fixed digest and ignores the
    /// public input, so one signature attests any batch.
    function _proof(MockECDSAProofSystem proofSystem) private view returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(PROVER_KEY, proofSystem.MOCK_PROVER_DIGEST());
        return abi.encodePacked(r, s, v);
    }
}
