# EEZ proof signer specification

Status: normative current profile

This document specifies the behavior of `eez-proof-signer` for the currently
supported single-rollup profile. It is intentionally narrower than the complete
EEZ protocol.

The protocol source used by this profile is the `eez-core-protocol`
submodule at commit
`6fcc90b65063831cb7797e9fa361004064d28f9f`. Stateless execution uses
`eez-association/stateless` at commit
`4fc3806bdd0e6b296c761ef4d4b260938365cf45`.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Solidity
defines on-chain behavior. This document defines the additional conditions the
signer applies before producing an attestation. If the pinned Solidity ABI or
hash construction changes, the signer, shared Rust mirrors, vectors, and this
document MUST be updated atomically.

## 1. Purpose and supported profile

The signer accepts one Composer-supplied L2 block window, validates every block
with witness-backed Stateless/Reth execution, binds one canonical
`postAndVerifyBatch` payload to the validated execution, recomputes the one
supported public-input hash, and signs that hash.

The only supported execution-entry sequence is:

```text
anchor, outbound*, inbound*
```

where:

- the anchor commits the validated L2 state transition before settlement
  effects;
- each outbound effect represents one successful mutable L2-to-L1 call; and
- each inbound effect represents one successful mutable L1-to-L2 delivery.

The profile supports exactly one nonzero rollup ID, one nonzero proof-system
address, and one nonzero proof-system verification key. It does not support
static entries, nested calls, multiple calls per effect, explicit call gas,
force-revert spans, blobs, L1 block binding, sender binding, or multiple proof
systems.

No failed stage may produce a signature. A successful signature authorizes the
locally recomputed public-input hash only under all rules in this document.

### 1.1 What the signature does not establish

The signature does **not** establish:

- canonical L2 ancestry, fork choice, finality, or sequencer authorization;
- that the witness-backed pre-state is an operator-trusted chain checkpoint;
- current L1 registry state or future on-chain applicability of a state update;
- successful future execution of an L1 target call;
- proof-system registration, threshold configuration, or verification-key
  equality in the deployed rollup manager;
- code identity at the fixed EEZL2 address;
- immediate-versus-queued dispatch, eventual execution, or availability of
  every proved entry;
- the contents of `proofs` or the Composer-supplied `public_inputs_hash`;
- a standalone on-chain commitment to the RPC window bounds, block hashes,
  block RLP, witnesses, or transaction checkpoints; or
- caller authentication, transport confidentiality, or replay protection.

Operators MUST supply those guarantees separately where required.

The window data in the previous list is nevertheless security-critical input
to the signer's decision: it is validated and used to authorize the entries
and DA bytes that are committed by the final digest. The distinction is that a
verifier cannot recover or independently inspect that execution evidence from
the signature alone.

### 1.2 Scheduling boundary

`immediateEntryCount` and `immediateStaticEntryCount` are unproven dispatch
parameters in the pinned protocol. They are absent from the public-input hash.

Before signing, this implementation MUST require:

```text
immediateEntryCount == number of leading entries with proxyEntryHash == 0
immediateStaticEntryCount == 0
```

This is an admission policy, not a cryptographic commitment. Changing either
count after signing does not change the signed digest. Verifiers and operators
MUST NOT infer immediate or queued execution from the signature alone.

The pinned contract intentionally lets the poster select the dispatch path for
eligible entries beyond the mandatory complete leading zero-proxy run. A
deployment that requires dispatch integrity MUST add an independent binding or
submission policy.

[`docs/unbound-scheduling-counts-assessment.md`](docs/unbound-scheduling-counts-assessment.md)
records the accepted protocol semantics and the mutation evidence behind this
boundary.

## 2. Authority and data provenance

Values are classified as follows:

| Source | Authority and required treatment |
| --- | --- |
| Operator configuration | Authoritative for execution-chain rules, expected rollup ID, proof-system address and vkey, attester identity, system-transaction key, and resource limits. The deployment MUST use the fixed EEZL2 address and MUST bind its configured system address to the configured system-transaction key. |
| Composer stream | Untrusted, including range, hashes, RLP, witness, settlement calldata, and claimed public-input hash. |
| Stateless/Reth output | Security-critical derived evidence. It MUST be consumed and checked against the corresponding admitted blocks before settlement uses it. |
| Pinned EEZ contracts | Authoritative for on-chain ABI layouts, selectors, hashes, and execution behavior. |
| `eez-protocol` mirrors | Shared local implementations used by the signer; they MUST match the pinned contracts and independent vectors. |
| Live L1/L2 network state | Not queried by the signer. |

Names in code and diagnostics SHOULD preserve this distinction. Composer values
SHOULD be described as `claimed`, `declared`, or `submitted`; replay results as
`computed`, `recomputed`, or `validated`; and values that passed a complete
direction-specific gate as `authorized`.

## 3. Startup configuration

Every option is available as a CLI flag with the listed environment fallback.
The CLI value takes precedence.

| CLI flag | Environment variable | Default | Requirement |
| --- | --- | --- | --- |
| `--listen-addr` | `EEZ_PROOF_SIGNER_ADDR` | `127.0.0.1:50061` | Valid socket address. |
| `--chain-config` | `EEZ_CHAIN_CONFIG` | none | Path to a strict Alloy `ChainConfig` or complete `Genesis` document. |
| `--rollup-id` | `EEZ_ROLLUP_ID` | none | Nonzero L1 registry rollup ID. This is not the L2 EIP-155 chain ID. |
| `--vkey` | `EEZ_VKEY` | none | Nonzero 32-byte proof-system vkey. |
| `--signer-key` | `EEZ_PROOF_SIGNER_KEY` | none | Valid secp256k1 attestation private key. |
| `--attester-address` | `EEZ_ATTESTER_ADDRESS` | none | MUST equal the address derived from `--signer-key`. |
| `--l2-system-key` | `EEZ_L2_SYSTEM_KEY` | none | Valid secp256k1 system-transaction key. |
| `--l2-system-address` | `EEZ_L2_SYSTEM_ADDRESS` | none | Deployment-generated address embedded in EEZL2; MUST equal the address derived from `--l2-system-key`. |
| `--proof-system` | `EEZ_PROOF_SYSTEM` | none | Nonzero expected proof-system contract address. |
| `--max-request-blocks` | `EEZ_PROOF_SIGNER_MAX_REQUEST_BLOCKS` | `512` | Nonzero. |
| `--max-request-bytes` | `EEZ_PROOF_SIGNER_MAX_REQUEST_BYTES` | `536870912` | Nonzero. |
| `--max-request-witness-items` | `EEZ_PROOF_SIGNER_MAX_REQUEST_WITNESS_ITEMS` | `1000000` | Nonzero. |
| `--stream-idle-timeout-secs` | `EEZ_PROOF_SIGNER_STREAM_IDLE_TIMEOUT_SECS` | `120` | Nonzero and representable as an `Instant` deadline. |
| `--request-timeout-secs` | `EEZ_PROOF_SIGNER_REQUEST_TIMEOUT_SECS` | `600` | Nonzero and representable as an `Instant` deadline. |

Private-key syntax errors MUST be reported without echoing the supplied secret.
Debug formatting MUST NOT reveal private keys.

The attestation key MUST NOT derive the deployment-configured `SYSTEM_ADDRESS`;
attestation and system-transaction construction are separate authorities. The
configured system key MUST derive `SYSTEM_ADDRESS` exactly.

The chain document MUST contain an explicit `chainId`. Unknown supported-level
and extension fields MUST be rejected rather than silently ignored. A complete
Genesis document supplies its timestamp and genesis fields; a bare
`ChainConfig` uses the implementation's default Genesis values.

The active deployment bindings are:

```text
SYSTEM_ADDRESS = EEZ_L2_SYSTEM_ADDRESS (generated by deployment)
EEZL2_ADDRESS  = 0x4200000000000000000000000000000000000007
```

The signer verifies that its system key derives `EEZ_L2_SYSTEM_ADDRESS`. It
does not verify deployed code or independently read the EEZL2 immutable;
matching the generated address and constructor configuration remains a
deployment invariant.

## 4. Service and RPC behavior

The service exposes one client-streaming RPC:

```protobuf
rpc Prove(stream ProveChunk) returns (ProveResponse);
```

One stream contains exactly one header followed by one block/witness chunk per
declared block.

### 4.1 Single-flight execution

Exactly one request may be admitted at a time. A second request MUST be
rejected immediately with `Unavailable`; it MUST NOT wait while holding an
open stream.

The active-request slot remains held through validation, settlement, signing,
and response construction. If a blocking worker outlives its RPC future, the
slot remains held until that worker exits. Graceful shutdown stops accepting
new connections and waits for the slot to become idle.

### 4.2 Message and request limits

The maximum decoded request-message size is:

```text
min(max_request_bytes, 256 MiB)
```

The maximum response-message size is 1024 bytes.

The aggregate byte quota is the sum of `prost::Message::encoded_len()` for
decoded known fields of every header and block chunk, including fields later
discarded. Integer overflow MUST reject the request. This quota does not undo
allocations made while decoding the current message; the per-message limit
bounds that stage.

The witness-item quota counts all `state`, `codes`, `keys`, and `headers`
elements across the request. The block quota covers the inclusive declared
range.

### 4.3 Deadlines and cancellation

The stream idle timeout applies independently to every wait for a message or
EOF. The request timeout is one absolute deadline covering ingestion,
validation, settlement, signing, and response construction.

CPU-bound validation and settlement run in one blocking worker. A worker that
has not started MAY be aborted. Running EVM execution is non-interruptible; a
running worker observes cooperative cancellation before block executions and
at selected settlement phase boundaries. A timed-out or disconnected request
MUST NOT release the active slot while its worker is still running.

If the absolute deadline expires during signing, the signature MUST NOT be
returned even if local signing completed.

## 5. Stream admission

Admission proves stream shape and quota compliance only. It does not prove
block, witness, or settlement correctness.

### 5.1 Header

The first chunk MUST be a `ProveHeader`. It MUST contain `post_batch`.

The header is admitted only if:

- `rollup_id` equals the operator-configured expected rollup ID;
- `from_block != 0`;
- `from_block <= to_block`;
- the inclusive span `to_block - from_block + 1` is representable and no larger
  than `max_request_blocks`; and
- `post_batch.l1_block_hash` is empty.

`post_batch.public_inputs_hash` is a non-authoritative Composer claim and MUST
be ignored. It is not length-checked and is never signed. Only
`post_batch.abi_calldata` is retained for settlement.

### 5.2 Blocks

After the header, every chunk MUST be a `BlockWitness`; duplicate headers and
empty chunk kinds MUST be rejected.

For block index `i`, admission MUST require:

- `number == from_block + i`;
- `hash` and `parent_hash` are exactly 32 bytes;
- a witness is present;
- the aggregate quotas remain within limits; and
- for `i > 0`, the claimed parent hash equals the preceding claimed block hash.

EOF is accepted only after exactly the declared number of blocks. Extra,
missing, duplicated, or reordered blocks MUST be rejected.

The first block's parent is not compared with a trusted external anchor during
admission.

## 6. Execution validation

The production backend is always the pinned in-process Stateless/Reth backend.
The operator-configured chain document determines fork rules and the EIP-155
chain ID; the Composer cannot override either.

### 6.1 Decode and identity binding

For every admitted block, the backend MUST:

1. exact-decode the supplied consensus RLP with no trailing bytes;
2. match the decoded number and parent hash to the admitted claims;
3. recompute the block-header hash and match the admitted hash; and
4. recover every transaction signer using the configured fork rules, including
   the Homestead low-`s` rule when active.

Signer recovery alone is not execution validation.

### 6.2 Local checkpoint plan

Only the terminal block may request transaction-state checkpoints. For each
recovered terminal transaction define:

```text
system[i] = signer(tx[i]) == SYSTEM_ADDRESS
            && tx[i].to == EEZL2_ADDRESS
```

The locally selected candidate positions are:

```text
C = { i | !system[i] || system[i + 1] || i is the last transaction }
```

An absent `system[i + 1]` at block end is treated as `true`. Thus an outbound
`[system-load, user]` pair ends at the user transaction, while a standalone
inbound system transaction ends at itself.

The Composer MUST NOT nominate checkpoint positions. The complete plan MUST be
derived before earlier blocks are replayed. The backend MUST return exactly the
requested positions in strict order. Preceding blocks MUST return no
checkpoints.

### 6.3 Stateless replay guarantees

For every block, successful Stateless validation MUST establish against the
supplied witness and configured chain rules:

- consensus/header validity;
- executable witness-backed pre-state;
- transaction execution and receipt results;
- the computed block hash;
- the computed post-state root and its header commitment; and
- any selected post-transaction state roots.

The pre-state root of each block after the first MUST equal the preceding
block's computed post-state root. The resulting window therefore exposes:

- `window_pre_state_root`: validated pre-state of the first block;
- `settling_pre_state_root`: computed post-state of the preceding block, or the
  window pre-state for a one-block window; and
- `window_post_state_root`: computed post-state of the terminal block.

This telescope is self-consistency, not proof that the first pre-state belongs
to the canonical chain.

### 6.4 Settlement evidence

From the validated block and receipts, the backend MUST retain:

- one receipt-success flag per transaction;
- one recovered `SYSTEM_ADDRESS` sender flag per transaction;
- every log emitted by `EEZL2_ADDRESS` whose first topic is the current
  `CrossChainCallExecuted` signature, ordered by transaction and log position;
  and
- the decoded event's `crossChainCallHash` and `callGas` when the complete log
  encoding is canonical.

A signature-matched but malformed or noncanonical event MUST remain an
observation without decoded fields. It MUST NOT be silently discarded.

Before settlement, a shared consuming check MUST bind one backend output to
each admitted block and verify block count, identities, hashes, exact RLP
transaction counts, receipt and sender-flag coverage, event coordinate bounds
and strict ordering, and checkpoint bounds/order. A backend that claims success
with malformed output is an internal failure, not an input rejection.

## 7. Canonical batch and public-input profile

`PostBatch.abi_calldata` MUST exact-decode as the current
`postAndVerifyBatch(ProofSystemBatchPerVerificationEntries)` ABI with selector
`0xcafef125`. Re-encoding the decoded argument MUST reproduce every byte after
the selector. Alternate, partial, or trailing encodings MUST be rejected.

Canonical decoding establishes byte identity only; every decoded claim remains
untrusted until the corresponding gate below succeeds.

The decoded batch MUST satisfy all of these profile rules:

1. `proofSystems` contains exactly the configured proof-system address.
2. `rollupIdsWithProofSystems` contains exactly one item with the expected
   rollup ID and `proofSystemIndexes == [0]`.
3. `expectedStateRootPerRollup` is empty.
4. Every mutable entry has `destinationRollupId == expected_rollup_id`.
5. `staticEntries` is empty.
6. `immediateStaticEntryCount == 0`.
7. `immediateEntryCount` equals the complete leading run of mutable entries
   whose `proxyEntryHash == 0`.
8. `blockNumber == 0`.
9. `blobIndices` is empty.
10. `bindMsgSenderInPublicInput == false`.

`proofs` is not inspected and is absent from the public-input hash. The caller
is responsible for inserting the returned signature into the correct proof
carrier and satisfying the on-chain `proofSystems.length == proofs.length`
requirement.

This profile assumes the deployed rollup manager returns empty `customData` for
`getCustomData(0)`, as the pinned reference `Rollup` does. The configured vkey
and proof-system address MUST match the deployed manager configuration; the
signer does not query L1 to confirm them.

## 8. State-update chain and effect binding

### 8.1 State-update chain

The batch MUST contain at least one mutable entry. Every entry MUST contain
exactly one `StateUpdate` and that update's `rollupId` MUST equal the expected
rollup ID.

For entries `E[0..n)` with updates `U[0..n)`, the signer MUST require:

```text
U[0].currentState == window_pre_state_root
U[i].currentState == U[i - 1].newState       for every i > 0
U[n - 1].newState == window_post_state_root
```

These checks bind the continuous Composer claim to validated endpoints. The
interior `newState` values are additionally bound to execution checkpoints
below.

### 8.2 Terminal-block framing

Every preceding block MUST contain neither:

- a transaction of reserved type `0x7e`;
- a transaction recovered from `SYSTEM_ADDRESS`; nor
- an observed EEZL2 outbound event.

In the terminal block:

- a transaction recovered from `SYSTEM_ADDRESS` MUST target `EEZL2_ADDRESS`;
- every such transaction MUST have a successful receipt; and
- a system-to-EEZL2 transaction beginning with selector `0x8d8461d9` is an
  inbound candidate even when the remainder of its calldata is malformed.

The number of entries after the anchor MUST equal the number of locally derived
candidate positions `C`. Claims and candidates are joined in order; skipping,
duplicating, or reordering either side MUST fail.

A candidate is classified from execution as:

```text
inbound  if its ending transaction is recovered from SYSTEM_ADDRESS
outbound otherwise
```

The claimed entry at the same position MUST have the same direction.

### 8.3 Entry classification

Every supported entry MUST have `success == true` and an empty
`expectedL1ToL2Calls` array.

The leading entry is a canonical anchor only if:

- `proxyEntryHash == 0`;
- `l2ToL1Calls` is empty;
- `destinationRollupId` is the expected rollup;
- `returnData` is empty; and
- `rollingHash` equals the L1 entry seed over its sole state update and a zero
  proxy hash.

Every later entry MUST be one of:

- **outbound:** `proxyEntryHash == 0`, exactly one `l2ToL1Call`,
  `revertNextNCalls == 0`, `isStatic == false`, and `gas == 0`; or
- **inbound:** `proxyEntryHash != 0` and `l2ToL1Calls` is empty.

A second anchor or any other shape MUST be rejected.

The anchor's `etherDelta` MUST be zero. If at least one effect exists, the
anchor's `newState` MUST equal `settling_pre_state_root`. If no effects exist,
the anchor alone covers the complete window transition.

For every effect at candidate position `C[i]`, the backend checkpoint MUST
target `C[i]`, and the effect's `StateUpdate.newState` MUST equal that computed
post-transaction state root.

## 9. Inbound authorization

An inbound candidate is authorized only if all rules in this section pass.

### 9.1 Executed L2 transaction

The system transaction MUST have succeeded and its calldata MUST be the exact
canonical encoding of current `executeIncomingCrossChainCall`, selector
`0x8d8461d9`.

The decoded call MUST satisfy:

- native transaction value equals the outer `value` argument;
- `sourceRollup == 0`;
- `_entries` contains exactly one L2 mutable entry;
- `_staticEntries` is empty;
- that entry contains exactly one `incomingCall`;
- entry `success == true`;
- `expectedOutgoingCalls` is empty;
- outer `destination`, `value`, `data`, `sourceAddress`, and `sourceRollup`
  equal the corresponding inner call fields; and
- the inner call has `revertNextNCalls == 0`, `isStatic == false`, and
  `gas == 0`.

The signer MUST recompute the common mutable call hash with source rollup `0`
and target rollup `expected_rollup_id`. The hash MUST be nonzero and equal the
L2 entry's `proxyEntryHash`.

The L2 entry rolling hash MUST be:

```text
L2 seed(proxyEntryHash)
  -> CALL_BEGIN(proxyEntryHash)
  -> CALL_END(true, returnData)
```

### 9.2 Claimed L1 entry

The corresponding batch entry MUST:

- target the expected rollup;
- contain no `l2ToL1Calls` and no `expectedL1ToL2Calls`;
- have `success == true`;
- carry the recomputed nonzero call hash as `proxyEntryHash`;
- carry exactly the L2-observed `returnData`;
- use the L1 seed over its sole update and `proxyEntryHash` as its complete
  `rollingHash`; and
- have `etherDelta == +value`, with `value` representable as a nonnegative
  `int256`.

There MUST be exactly one canonical inbound candidate for every claimed inbound
effect and no unclaimed inbound candidate.

## 10. Outbound authorization

All outbound effects MUST precede all inbound effects.

An outbound candidate transaction MUST be immediately preceded by a
`SYSTEM_ADDRESS` transaction. The candidate MUST have exactly one matching
current six-field `EEZL2.CrossChainCallExecuted` event. Every matching EEZL2
event in the terminal receipts MUST be claimed exactly once; malformed,
duplicate, missing, or extra observations MUST fail.

The event's `callGas` MUST be zero. The batch entry MUST:

- target the expected rollup;
- contain exactly one `l2ToL1Call`;
- use `proxyEntryHash == 0`;
- contain no `expectedL1ToL2Calls`;
- have `success == true`; and
- carry a call with the expected source rollup,
  `revertNextNCalls == 0`, `isStatic == false`, and `gas == 0`.

The call source address MUST NOT be `SYSTEM_ADDRESS`.

The signer MUST recompute the mutable cross-chain call hash with target rollup
`0` and the event's zero `callGas`; it MUST equal the event hash. The same
zero-`callGas` identity is committed by the entry's L1 rolling hash:

```text
L1 seed(sole update, proxyEntryHash = 0)
  -> CALL_BEGIN(common zero-callGas call hash)
  -> CALL_END(true, entry.returnData)
```

The update MUST have `etherDelta == -value`, with `value` representable as a
nonnegative `int256`.

## 11. Data-availability and Sync-block verification

The batch `callData` MUST have this exact shape:

```text
0x00 || RLP([blockTxCounts, transactions, l2Entries])
```

There MUST be no trailing bytes or fields. `blockTxCounts` contains one RLP
`u16` per validated block. `transactions` and `l2Entries` are RLP lists whose
items are themselves RLP lists of byte values; decoding each item MUST recover
the exact EIP-2718 transaction bytes or ABI sidecar bytes being compared.

For every block before the terminal block, `blockTxCounts` MUST equal the exact
validated transaction count and every transaction byte string MUST match in
order.

For the terminal block, the DA projection omits:

- the system load immediately preceding each outbound user transaction; and
- each inbound delivery system transaction.

Outbound user transactions remain. Counts and retained transaction bytes MUST
match this projection exactly.

There MUST be one `l2Entries` sidecar per effect, in effect order:

- an outbound sidecar is the authorized batch entry with `stateUpdates`
  cleared and `rollingHash` set to zero, with every other field unchanged;
- an inbound sidecar is derived from the executed inbound calldata with empty
  `stateUpdates`, the recomputed proxy hash, one L1-shaped call copied from the
  L2 `incomingCall`, empty `expectedL1ToL2Calls`, the validated L2 rolling hash,
  the expected destination rollup, success flag, and return data.

No missing, extra, reordered, or byte-different sidecar is accepted.

When effects exist, the signer MUST reconstruct the complete terminal Sync
transaction sequence and compare it byte for byte with the validated block.
The reconstruction context is:

```text
signer       = operator-provided key deriving EEZ_L2_SYSTEM_ADDRESS
target       = EEZL2_ADDRESS
chain id     = operator-configured L2 EIP-155 chain ID
gas price    = 1_000_000_000
gas limit    = 2_000_000
rollup id    = expected_rollup_id
first nonce  = nonce of the first validated omitted system transaction
```

The canonical order is:

```text
[outbound load 0, outbound user 0,
 ...,
 outbound load K-1, outbound user K-1,
 inbound delivery 0,
 ...,
 inbound delivery M-1]
```

System nonces increase through that order. Outbound loads carry zero native
value. Inbound deliveries carry their authorized inbound value. The reconstructed
sequence length and every raw transaction byte MUST equal the validated
terminal block.

With no authorized effects, no transactions are omitted and no Sync sequence
is reconstructed.

## 12. Public-input recomputation

Let `H = keccak256`. Solidity ABI rules are normative.

For every mutable entry:

```text
entryHash[i] = H(abi.encode(batch.entries[i]))
```

For the supported profile:

```text
staticEntryHashes = []
blobHashes        = []
customData        = bytes("")
customDataHash    = H(abi.encode(uint64(expected_rollup_id), customData))
customDataHashes  = dynamic bytes32[] containing only customDataHash
boundSender       = address(0)
```

The shared input is:

```text
shared = H(
    abi.encode(entryHashes)
 || abi.encode(staticEntryHashes)
 || abi.encode(blobHashes)
 || H(batch.callData)
 || abi.encode(customDataHashes)
 || packed_address_20_bytes(boundSender)
)
```

The sole proof-system accumulator is:

```text
acc = H(abi.encode(
    bytes32(0),
    uint64(expected_rollup_id),
    bytes32(configured_vkey)
))
```

The signed digest is:

```text
publicInputsHash = H(shared || acc)
```

where `shared || acc` is the 64-byte packed concatenation.

The computation MUST return exactly one hash. The Composer-supplied hash MUST
not participate.

### 12.1 Fields not bound by the digest

The full mutable entries and `callData` are bound as above. The following batch
carriers are not directly included in the digest:

- `expectedStateRootPerRollup`;
- `immediateEntryCount` and `immediateStaticEntryCount`;
- proof-system contract addresses;
- proof bytes; and
- the literal `blockNumber`, `blobIndices`, and
  `bindMsgSenderInPublicInput` carriers.

Some of these fields affect derived hash inputs in broader protocol profiles.
This signer pins them to the values in section 7 before hashing. The configured
proof-system address is also checked before signing. Those checks constrain
what the signer accepts, but they do not add the omitted carriers to the
signature preimage.

## 13. Attestation

Only the complete validation-and-settlement pipeline may construct an
attestable hash. A helper that merely decodes a batch or recomputes a profile
hash MUST NOT be able to invoke the production attester.

The signer signs the raw 32-byte `publicInputsHash` using secp256k1 ECDSA:

- no EIP-191 prefix;
- no EIP-712 domain;
- no additional chain, contract, or request domain separator;
- canonical low-`s`; and
- `v` normalized to `27` or `28`.

The signature is exactly 65 bytes:

```text
r[32] || s[32] || v[1]
```

The response contains the locally recomputed 32-byte hash and that signature.
It MUST NOT return the Composer-supplied hash.

## 14. Public failure behavior

The service MUST fail closed. Internal diagnostics may be detailed, but public
messages SHOULD remain stable and must not expose secrets.

| gRPC code | Principal cases |
| --- | --- |
| `InvalidArgument` | Malformed stream structure; invalid widths or bounds; noncanonical/invalid PostBatch calldata; malformed or trailing DA payload. |
| `FailedPrecondition` | Rollup identity mismatch; Stateless input rejection; unsupported batch profile; state/effect/inbound/outbound/DA semantic rejection. |
| `Unavailable` | Another request is active. The same complete request may succeed after the active request releases the slot. |
| `ResourceExhausted` | A decoding, block, byte, or witness-item limit was exceeded; or block-vector storage could not be reserved. |
| `DeadlineExceeded` | Stream idle timeout or absolute request deadline. |
| `Cancelled` | Cooperative stop after request cancellation. |
| `Internal` | Backend-success output violates its contract; local invariant failure; impossible public-input computation/cardinality; reconstruction failures attributable to already validated internal evidence; signing failure. |

A malformed candidate that could otherwise disappear from consideration MUST
be retained and rejected at its authorization gate.

### 14.1 Actionable failed preconditions

A `FailedPrecondition` response MAY carry one protobuf-encoded `ProveFailure`
in the gRPC status-details field when the validated execution identifies one
cross-chain candidate that the Composer can safely remove. The status code,
not the details payload, remains authoritative for retry classification.

`ProveFailure.actionable_failure` has exactly two supported variants:

- `OutboundFailure` identifies the original signed L2 user transaction by its
  zero-based index in the terminal Sync block and its canonical 32-byte
  transaction hash. When the preceding synthetic load transaction reverted,
  the failure still identifies the paired user transaction; rebuilding the
  Sync block regenerates or removes both halves together.
- `InboundFailure` identifies the claimed effect by its zero-based index in
  `PostBatch.entries` and the 32-byte keccak hash of that entry's canonical ABI
  encoding. The original signed L1 transaction is not present in the proof
  request, so the Composer MUST resolve it through the request-local
  entry-to-held-transaction mapping retained during composition.

The signer MUST attach an outbound detail only when validated terminal-block
execution safely identifies the original user transaction: a reverted
canonical synthetic load/user pair, or a positioned outbound observation
failure attributable to the user transaction. It MUST attach an inbound detail
only for a positioned inbound delivery transaction that reverted. Structural,
ordering, envelope, claim-only, missing-candidate, extra-observation, DA, and
state-chain failures MUST remain non-actionable even when their diagnostic
contains an index. A mismatch between a claimed entry's call hash and an
execution observation is claim-only in both directions and MUST remain
non-actionable.

Before changing pool state, the Composer MUST verify both fields against the
exact rejected request: index and transaction hash for outbound, or index and
canonical entry hash for inbound. Empty, malformed, unknown, wrong-width, or
mismatched details MUST be handled as an ordinary non-actionable rejection.
The Composer MUST NOT retry an unchanged request after an actionable failure.
It MAY remove the resolved held transaction and its same-sender,
same-direction nonce suffix, then rebuild and submit a smaller batch within the
remaining slot budget. This recovery does not authorize bisection or eviction
for failures that carry no valid typed detail.

The index-and-hash checks bind an actionable detail to the rejected request;
they do not independently prove that the reported execution failure occurred.
The Composer therefore trusts its configured prover not to falsely attribute a
failure. A buggy or compromised prover can cause valid held transactions and
their nonce suffixes to be evicted, requiring users to resubmit. Authenticating
the Composer-prover transport prevents response injection but does not remove
this configured-prover trust.

## 15. Conformance and change control

Changes to the ABI, selectors, call-hash formulas, rolling-hash formulas,
public-input formula, system-transaction bytes, checkpoint semantics, or
supported profile are security-sensitive.

A compatible implementation MUST test at least:

- every stream ordering, identity, and quota boundary;
- strict chain-document parsing and secret redaction;
- exact RLP identity/hash binding and backend-output association;
- checkpoint selection, returned positions, and roots;
- canonical PostBatch decoding and every profile pin;
- state-chain endpoints, continuity, effect count/order/kind, and checkpoints;
- inbound outer/inner equality, call hash, rolling hash, value, and canonical
  calldata;
- outbound event provenance, canonical encoding, zero-`callGas` hash, L1
  rolling hash, ordering, source, and value;
- exact DA projection, sidecars, and mixed Sync-block reconstruction;
- actionable outbound/inbound failure attribution, reference validation, and
  non-actionable fallback;
- public-input vectors against the pinned Solidity formula; and
- raw-digest ECDSA recovery, low-`s`, and `v` encoding.

The root Kurtosis gate MUST also observe at least one successful signer
attestation and one node-side acceptance of a remote attestation while running
the inbound, outbound, and mixed settlement modes. A wave-only success is not
sufficient end-to-end proof evidence.

The current captured `fresh-chain-inbound-2175` PostBatch uses a legacy selector
and is a rejection regression, not a positive current-protocol fixture. The
captured `nonzero-outbound-630` receipts use the former five-field outbound
event and likewise test rejection by the current six-field decoder. The
`stateless-block-13` and `stateless-checkpoint-2175` fixtures validate execution
and checkpoint behavior only; they do not by themselves establish a positive
current-protocol settlement attestation.

Updating either pinned external revision MUST include:

1. review of its source diff and security invariants;
2. updates to the shared Rust ABI/hash mirrors;
3. independent vectors or contract differential tests;
4. signer unit, integration, clippy, documentation, and end-to-end tests; and
5. an explicit review of this document's attestation and non-claim boundaries.

## Annex A. Current ABI surface

Field order and integer widths are part of the ABI.

```solidity
struct StateUpdate {
    uint64 rollupId;
    bytes32 currentState;
    bytes32 newState;
    int256 etherDelta;
}

struct ExpectedStateRootPerRollup {
    uint64 rollupId;
    bytes32 stateRoot;
}

struct L2ToL1Call {
    uint16 revertNextNCalls;
    bool isStatic;
    uint64 gas;
    address sourceAddress;
    uint64 sourceRollupId;
    address targetAddress;
    uint256 value;
    bytes data;
}

struct ExpectedL1ToL2Call {
    bytes32 expectedL1toL2Hash;
    L2ToL1Call[] l2ToL1Calls;
    bytes32 revertedOrStaticRollingHash;
    bool success;
    bytes returnData;
}

struct ExecutionEntry {
    StateUpdate[] stateUpdates;
    bytes32 proxyEntryHash;
    L2ToL1Call[] l2ToL1Calls;
    ExpectedL1ToL2Call[] expectedL1ToL2Calls;
    bytes32 rollingHash;
    uint64 destinationRollupId;
    bool success;
    bytes returnData;
}

struct StaticExecutionEntry {
    ExpectedStateRootPerRollup[] expectedStateRoots;
    bytes32 proxyEntryHash;
    L2ToL1Call[] l2ToL1Calls;
    bytes32 rollingHash;
    uint64 destinationRollupId;
    bool success;
    bytes returnData;
}

struct RollupIdWithProofSystems {
    uint64 rollupId;
    uint64[] proofSystemIndexes;
}

struct ProofSystemBatchPerVerificationEntries {
    ExpectedStateRootPerRollup[] expectedStateRootPerRollup;
    ExecutionEntry[] entries;
    StaticExecutionEntry[] staticEntries;
    uint256 immediateEntryCount;
    uint256 immediateStaticEntryCount;
    address[] proofSystems;
    RollupIdWithProofSystems[] rollupIdsWithProofSystems;
    uint256[] blobIndices;
    bytes callData;
    bytes[] proofs;
    uint64 blockNumber;
    bool bindMsgSenderInPublicInput;
}
```

The L2 inbound envelope uses the separate L2 ABI family:

```solidity
struct CrossChainCall {
    uint16 revertNextNCalls;
    bool isStatic;
    uint64 gas;
    address sourceAddress;
    uint64 sourceRollupId;
    address targetAddress;
    uint256 value;
    bytes data;
}

struct ExpectedOutgoingCrossChainCall {
    bytes32 expectedOutgoingHash;
    CrossChainCall[] incomingCalls;
    bytes32 revertedOrStaticRollingHash;
    bool success;
    bytes returnData;
}

struct L2ExecutionEntry {
    bytes32 proxyEntryHash;
    CrossChainCall[] incomingCalls;
    ExpectedOutgoingCrossChainCall[] expectedOutgoingCalls;
    bytes32 rollingHash;
    bool success;
    bytes returnData;
}

struct L2StaticExecutionEntry {
    bytes32 proxyEntryHash;
    CrossChainCall[] incomingCalls;
    bytes32 rollingHash;
    bool success;
    bytes returnData;
}

function loadExecutionTable(
    L2ExecutionEntry[] _entries,
    L2StaticExecutionEntry[] _staticEntries
);

function executeIncomingCrossChainCall(
    address destination,
    uint256 value,
    bytes data,
    address sourceAddress,
    uint64 sourceRollup,
    L2ExecutionEntry[] _entries,
    L2StaticExecutionEntry[] _staticEntries
) payable returns (bytes);

event CrossChainCallExecuted(
    bytes32 indexed crossChainCallHash,
    address indexed proxy,
    address sourceAddress,
    bytes callData,
    uint256 value,
    uint64 callGas
);
```

Current selector locks are:

| Function | Selector |
| --- | --- |
| `postAndVerifyBatch` | `0xcafef125` |
| `executeL2Txs` | `0xdc6d11fa` |
| `staticCrossChainCall` | `0x31344ade` |
| `loadExecutionTable` | `0xb301bc80` |
| `executeIncomingCrossChainCall` | `0x8d8461d9` |

## Annex B. Hash formulas used by the supported profile

Let `H = keccak256`. `abi.encode` below is standard Solidity ABI encoding;
`packed` means the exact byte concatenation shown.

### B.1 Cross-chain call hash

```text
H(abi.encode(
    bool isStatic,
    address sourceAddress,
    uint64 sourceRollupId,
    address targetAddress,
    uint64 targetRollupId,
    uint256 value,
    uint64 callGas,
    bytes data
))
```

All supported L1, inbound, and static paths use `callGas == 0`.

### B.2 Mutable L2 outbound event hash

```text
H(abi.encode(
    false,
    address sourceAddress,
    uint64 sourceRollupId,
    address targetAddress,
    uint64 targetRollupId,
    uint256 value,
    uint64 callGas,
    bytes data
))
```

This is B.1 with `isStatic == false` and the `callGas` observed in the event.
The supported deployment requires `callGas == 0`, so the event hash is also
the identity folded into the corresponding L1 entry rolling hash.

### B.3 Entry rolling hashes

For L1:

```text
states[0]   = bytes32(0)
states[i+1] = H(states[i] || uint64_be(update[i].rollupId)
                          || update[i].currentState)
l1_seed     = H(states[n] || proxyEntryHash)
```

For L2:

```text
l2_seed = H(bytes32(0) || proxyEntryHash)
```

Supported event folds are:

```text
CALL_BEGIN(prev, callHash) = H(prev || 0x01 || callHash)
CALL_END(prev, success, returnData)
                           = H(prev || 0x02 || bool_byte(success)
                                    || raw_return_data)
```

`uint64_be` is exactly eight big-endian bytes, and the success flag is exactly
one byte (`0x00` or `0x01`).

## Annex C. Source anchors

The principal sources for this specification are:

- `../eez-prover-stateless/src/config.rs`, `src/service.rs`, and `src/service/`
  for configuration, request lifetime, deadlines, and error mapping;
- `src/window.rs` and `../eez-control-rpc/proto/prove.proto` for wire admission;
- `src/validate.rs`, `src/validate/support.rs`,
  `../eez-prover-stateless/src/backend.rs`, and
  `../eez-prover-stateless/src/backend/chain_config.rs` for replay evidence;
- `src/settlement/` for canonical decoding, profile, state, effect, inbound,
  outbound, DA, and system-transaction gates;
- `src/attest.rs` and `../eez-protocol/src/signer.rs` for attestation;
- `../eez-protocol/src/abi.rs`, `action.rs`, `rolling_hash.rs`,
  `public_inputs.rs`, and `system_tx.rs` for shared protocol mirrors; and
- `../../eez-core-protocol/src/interfaces/IEEZ.sol`, `EEZ.sol`,
  `src/interfaces/IEEZL2.sol`, `src/L2/EEZL2.sol`, and
  `src/rollupContract/Rollup.sol` for pinned protocol behavior; and
- `../../contracts/src/ECDSAProofSystem.sol` for the deployed ECDSA verifier.
