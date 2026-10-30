# Native system transactions

Rollup0 uses the RollupX native execution design from `b832ad6a4764c9796eb61f86dcd6a29d85f72c77`, adapted to Rollup0's existing calldata settlement and outbound `[load, user]` groups. This is a fresh-genesis change; there is no activation schedule or conversion of existing `0x76` databases.

## Envelope and identity

The canonical envelope is `0x45 || RLP([chainId, sourceHash, gasLimit, to, value, input])`. It has six fields, no version, signature, sender or nonce. The sender is `0xfffffffffffffffffffffffffffffffffffffffe`. Rollup0 retains its EEZL2 predeploy at `0x4200000000000000000000000000000000000007`; other destinations are rejected. Transaction hashes are Keccak-256 of the entire typed envelope, and receipts retain type `0x45`.

Every complete native execution group has:

```text
sourceHash = keccak256(abi.encode(
    "SOURCE_ENTRY_V1", uint256(l2ChainId), bytes32(intendedL1ParentHash), uint64(groupIndex)
))
```

The domain is a dynamic ABI string, not packed bytes or a prehashed string. Indices start at zero in the full original Sync candidate: all outbound `[load, user]` pairs, then inbound deliveries. A pair counts once. Foreign entries that produce no L2 transaction do not consume an index. Derivation constructs this complete mapping before selecting the entries L1 applied; resumption preserves original indices and materializes gas against the retained execution prefix.

A source hash identifies an execution position, not the contents of the transaction and not authorization by itself. Competing candidates for the same position can share a source hash while differing in transaction hash. The same candidate can also be retransmitted. Exactly one accepted state-root chain determines what executed. There is no global source-hash database or native nonce replay guard inside the EVM.

## Authorization and replay

Public pooling, gossip and reorg reinjection use Ethereum's pooled envelope and reject the native type. Ordinary signed calls cannot acquire the reserved sender; the EVM also rejects an ordinary transaction environment claiming that sender. Authenticated Engine imports and signed unsafe heads remain provisional until L1 derivation confirms them.

Before signing, both prover backends validate execution and produce receipt gas and transaction checkpoints. The shared settlement pipeline binds successful native calls to their incoming/outgoing effects, authorized value, DA actions and state transitions, reconstructs every native transaction independently, and compares the complete terminal transaction sequence byte for byte. It does not learn the source index or source hash from the submitted native transaction. Intermediate blocks cannot contain native transactions.

Native settlement uses `batch.blockNumber = uint64.max`. The reference L1 Rollup already implements `getCustomData(uint64.max)` as `abi.encode(uint256(block.timestamp), blockhash(block.number - 1))`. The proof binds that custom data to the intended parent and terminal Sync timestamp. A proof for another parent or inclusion time cannot settle. The L1 scanner obtains the actual inclusion header, verifies the public-input commitment with its parent/time, and supplies that context to derivation. Accepted-root checks prevent applying a consumed transition twice, including a second submission in the same L1 block.

Rollup0's signer produces conditional proofs: it does not query L1 to authenticate the requested parent before signing. The contract authenticates that parent and time at consumption. v1 carries the intended parent in `PostBatch.l1_block_hash`; its time is the validated terminal header's timestamp. v2 carries the same parent and time in `posting_context`, with `chain_id` and `parent_number` zero for this profile. Blobs remain unsupported. Ordinary-only and historical catch-up batches can use the existing timeless `blockNumber = 0` profile with no posting context.

## Execution and accounting

Each native envelope encodes exactly `min(16_777_216, blockGasLimit - precedingCumulativeGasUsed)`. Builders resolve it after executing the preceding prefix; consensus execution and signer reconstruction check it. Only actual gas usage counts against the block. A budget below intrinsic gas invalidates the transaction. Under a nearly full block, composition defers a group that needs more room rather than permanently evicting it.

Native `GASPRICE` is zero, including nested calls. Native calls retain chain-ID validation and execution metering, but neither validate nor increment the system account nonce. The nonce stays unchanged during execution as well as afterward. Ordinary users, including the user transaction in an outbound pair, retain normal nonces and fee rules. RPC exposes zero nonce/signature/fee placeholders; they are not wire fields or authorization.

An inbound call credits exactly its authorized value before execution. An outer revert/halt removes that mint while retaining gas usage; invalid execution commits nothing. Rollup0 still rejects unsupported nested/multicall settlement shapes, so outbound table loads carry zero value. There is no independent unrestricted mint field.

Residual ETH at SYSTEM and EEZL2 is preserved across groups and block finalization. Outgoing value accumulates at SYSTEM; the native envelope does not introduce automatic balance clearing. The ordinary fee-collector configuration is unchanged.

## Deployment and validation

Regenerate genesis with `scripts/update-eezl2-genesis.sh` using the pinned Foundry version, redeploy the L1 commitment and start fresh L2 datadirs. EEZL2's immutable sender must match the reserved address; genesis omits that codeless account. The signer key and L1 posting key retain separate roles; no system-transaction private key exists.

Rollup0 retains its core-protocol and Reth/Stateless pins, EEZL2 address, DA action format, supported single-call profile and fee model. This port does not introduce RollupX's offline-history `0x46` envelope or its broader deployment/treasury profile.

`native_system` runs two Composers, a stateless signer and two keyless followers, including late join, real deposit/withdrawal, identical transaction/receipt roots and public-RPC native rejection. The local relay selects one posting Composer per phase; this is cross-process execution and proof validation, not a production consensus or atomic-builder test. `partial_consumption`, `resumption` and `resumption_append` cover settlement-prefix recovery. Set `EEZ_TEST_LOG_DIR` to retain evidence and leave `EEZ_TEST_DATADIR_DIR` unset for automatic chain-data cleanup.
