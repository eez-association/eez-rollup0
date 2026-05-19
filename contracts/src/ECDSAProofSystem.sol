// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {IProofSystem} from "sync-rollups-protocol/src/IProofSystem.sol";

/// @title ECDSAProofSystem
/// @notice Development-only `IProofSystem` implementation backed by a single
///         ECDSA signer. Used by the kurtosis devnet to drive the multi-prover
///         protocol without standing up a zisk prover. **Not for production.**
///
/// @dev    `proof` is a 65-byte ECDSA signature encoded as
///         `abi.encodePacked(r, s, v)`:
///         - `r`: bytes32 — R component
///         - `s`: bytes32 — S component (canonical, low-half only — see below)
///         - `v`: uint8   — recovery identifier (27 or 28)
///
///         `publicInputsHash` is signed directly as the raw 32-byte digest;
///         no EIP-191 prefix. This matches the
///         `sharedHash`/`publicInputsHash` shape produced by
///         `EEZ._verifyProofSystemBatch` so the composer can hand the same
///         bytes to this verifier and to a real zisk prover interchangeably
///         (per `IProofSystem.verify(bytes proof, bytes32 publicInputsHash)`
///         at `sync-rollups-protocol/src/IProofSystem.sol:15`).
///
///         The contract is registered on a per-rollup `IRollupContract`'s
///         vkey map; the rollup owner sets `vkey = bytes32(uint256(uint160(
///         authorizedSigner)))` so the registry's
///         `checkProofSystemsAndGetVkeys` returns a non-zero vkey
///         (membership-only). The signer address itself is read off this
///         contract's `signer()` view; the vkey is just the registry's
///         membership ticket.
///
///         The signer is set at construction (immutable) so the contract is
///         a pure verifier — no owner, no setters. Re-deploying with a new
///         signer is the upgrade path; on a dev chain this is one redeploy.
contract ECDSAProofSystem is IProofSystem {
    /// @notice Authorized signer address. ECDSA recovery against
    ///         `publicInputsHash` must equal this value for `verify` to
    ///         return `true`.
    address public immutable signer;

    /// @notice secp256k1 curve order divided by 2. Signatures whose `s`
    ///         component exceeds this are non-canonical (EIP-2 malleability)
    ///         and rejected.
    uint256 private constant SECP256K1_HALF_N =
        0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0;

    /// @notice Emitted at deployment so off-chain tooling can capture the
    ///         signer without ABI-decoding the constructor args.
    event SignerSet(address indexed signer);

    /// @notice Reverts if `initialSigner` is the zero address — silent
    ///         success against `address(0)` would happen if `ecrecover`
    ///         returned 0 on a malformed signature.
    error SignerCannotBeZero();

    constructor(address initialSigner) {
        if (initialSigner == address(0)) revert SignerCannotBeZero();
        signer = initialSigner;
        emit SignerSet(initialSigner);
    }

    /// @inheritdoc IProofSystem
    /// @dev Returns `false` (does not revert) on every invalid-signature
    ///      path — including malformed proof length, non-canonical `s`,
    ///      out-of-range `v`, or signer mismatch — so the multi-prover
    ///      quorum logic in `EEZ._verifyProofSystemBatch` surfaces a
    ///      uniform `InvalidProof(proofSystemAddress)` revert rather than
    ///      a verifier-specific one. This function never reverts.
    function verify(bytes calldata proof, bytes32 publicInputsHash)
        external
        view
        returns (bool valid)
    {
        if (proof.length != 65) return false;

        bytes32 r;
        bytes32 s;
        uint8 v;
        // Decode `abi.encodePacked(r, s, v)` — three fixed-width fields
        // concatenated with no padding. `calldataload` reads 32 bytes at the
        // given offset; the byte index is `4 (selector) + 32 (proof offset
        // pointer) + 32 (proof length prefix) + field offset`. Equivalent
        // to a manual `bytes` view-decode without an intermediate alloc.
        assembly {
            // proof.offset already accounts for the bytes-array length prefix
            r := calldataload(proof.offset)
            s := calldataload(add(proof.offset, 32))
            v := byte(0, calldataload(add(proof.offset, 64)))
        }

        // EIP-2 malleability guard — reject the upper half of the curve.
        if (uint256(s) > SECP256K1_HALF_N) return false;
        // `v` MUST be 27 or 28. `ecrecover` returns 0 on out-of-range `v`,
        // but the explicit check makes the failure mode legible.
        if (v != 27 && v != 28) return false;

        address recovered = ecrecover(publicInputsHash, v, r, s);
        if (recovered == address(0)) return false;
        return recovered == signer;
    }
}
