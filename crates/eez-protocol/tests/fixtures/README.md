# Protocol compatibility vectors

These JSON files are small cross-language compatibility oracles for protocol
commit `f6226f569e9b4534d42eecf5d2e3dd6c649bc6aa`. They are not generator output:
the named Solidity tests assert the expected values against the pinned
contracts, and the Rust tests independently consume the same values. Each Rust
loader rejects unexpected schema, protocol-commit, or oracle metadata.

Verify both sides from the repository root:

```bash
forge test --root contracts --match-contract PublicInputsHashVectorsTest
forge test --root contracts --match-contract RollingHashVectorsTest
cargo test --package eez-protocol --test public_inputs_hash_vectors \
  --test rolling_hash_vectors --locked
```

No captured positive signer window is stored here. Such a fixture would bundle
large, deployment-specific block RLP and execution witnesses. Deterministic
signer tests cover canonical target calldata and settlement, while the fresh
Kurtosis gate covers real windows through signing and on-chain verification.
