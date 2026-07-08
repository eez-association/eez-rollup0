// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

/// @notice Minimal L1-side user contract: receives a value-bearing call
/// and forwards it to the L2 proxy. Lifted byte-identical from the
/// `bridge` E2E fixture in `sync-rollups-protocol/script/e2e/bridge/E2E.s.sol`
/// so the composer-produced ExecutionEntry shapes match the existing
/// fixture's `_l1Entries` / `_l2Entries`. The fixture is the canonical
/// reference for the L1→L2 deposit flow.
contract BridgeSender {
    address public immutable L2_PROXY;
    address public immutable L2_DESTINATION;

    constructor(address l2Proxy, address l2Destination) {
        L2_PROXY = l2Proxy;
        L2_DESTINATION = l2Destination;
    }

    function bridge() external payable {
        (bool ok,) = L2_PROXY.call{value: msg.value}("");
        require(ok, "bridge failed");
    }
}

/// @notice Minimal L2-side receiver: accepts ETH via `receive()` so the
/// bridged value lands here once the system tx fires on L2.
contract BridgeReceiver {
    receive() external payable {}
}
