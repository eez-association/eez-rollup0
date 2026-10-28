// SPDX-License-Identifier: MIT
pragma solidity 0.8.34;

import {DeployInitializedBridge} from "eez-core-protocol/script/DeployInitializedBridge.s.sol";

/// @notice Deploy the protocol's reference bridge with atomic initialization.
contract DeployBridge is DeployInitializedBridge {}
