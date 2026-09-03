# EEZ Composer-to-prover gRPC specification

Status: normative current Composer profile

This document tells Composer implementers how to construct a proving request,
send it to an EEZ prover, validate the response, and use the returned proof in
`EEZ.postAndVerifyBatch`.

It does not specify how a prover validates or proves a request. See the
[`eez-proof-signer` documentation](../eez-proof-signer/docs/README.md) for the
reference implementation's architecture and validation pipeline.
[`prove.proto`](proto/prove.proto) is the canonical wire schema.

## 1. Composer flow

For each settlement interval, the Composer MUST:

1. assemble one canonical L1 settlement batch from its selected effects;
2. collect the exact L2 block RLP and augmented execution witness for every
   block in the settlement window;
3. ABI-encode `EEZ.postAndVerifyBatch(batch)` while `batch.proofs` is empty;
4. stream one `Prove` request containing that calldata and block window;
5. validate the returned hash and signature;
6. set `batch.proofs` to the returned signature without changing any other
   proven batch fields; and
7. ABI-encode the final call, sign the L1 transaction with the poster key, and
   submit it to the configured EEZ contract.

If proving cannot complete before the settlement cutoff, the
[failure-handling fallback](#63-retry-procedure-and-settlement-cutoff) replaces
steps 5 through 7 for that interval: the Composer commits an empty Sync block
without submitting an unproved batch and retains the selected effects for a
later interval.

```text
batch = assemble_settlement_batch(selected_effects)
batch.proofs = []

response = Prove(
    header(abi_encode(EEZ.postAndVerifyBatch(batch))),
    ordered_block_witnesses,
)
validate_response(response, registered_attester, batch)

batch.proofs = [response.signature]
submit_l1(EEZ_ADDRESS, abi_encode(EEZ.postAndVerifyBatch(batch)))
```

The prover receives the resulting settlement batch and L2 execution data. It
does not receive the Composer's internal planning state, database, L2 provider,
L1 poster key, or an L1 transaction to submit.

## 2. Service definition

The Composer calls one standard gRPC client-streaming method from
[`prove.proto`](proto/prove.proto):

```protobuf
service Prover {
  rpc Prove(stream ProveChunk) returns (ProveResponse);
}
```

The Composer SHOULD generate its client and message types directly from the
canonical protobuf rather than maintaining a second schema. One stream
represents a single complete proving request.

## 3. Constructing the request

### 3.1 Settlement batch

The request carries the batch that the Composer intends to submit to L1, with
only its proof bytes omitted. The deployed Solidity ABI and its
`ProofSystemBatchPerVerificationEntries` field order and integer widths are
authoritative.

The current profile requires:

| Batch field | Composer requirement |
| --- | --- |
| `expectedStateRootPerRollup` | Empty. |
| `entries` | One anchor, followed by zero or more outbound entries, then zero or more inbound entries. |
| `staticEntries` | Empty. |
| `immediateEntryCount` | Complete leading run with `proxyEntryHash == 0`: the anchor plus all outbound entries. |
| `immediateStaticEntryCount` | Zero. |
| `proofSystems` | Exactly the configured `ECDSAProofSystem` address. |
| `rollupIdsWithProofSystems` | Exactly the expected rollup ID assigned to proof-system index `0`. |
| `blobIndices` | Empty. |
| `callData` | Exact tagged DA payload described below. |
| `proofs` | Empty while constructing and sending the request. |
| `blockNumber` | Zero. |
| `bindMsgSenderInPublicInput` | `false`. |

Every entry MUST contain exactly one state update for the expected rollup. The
updates MUST form one continuous chain:

```text
entries[0].stateUpdates[0].currentState = state root at posted
entries[i].stateUpdates[0].currentState = entries[i - 1].stateUpdates[0].newState
entries[last].stateUpdates[0].newState = terminal Sync-block state root
```

The anchor is `entries[0]` and is not counted as a cross-chain effect. Effect
`i` is `entries[i + 1]`. Let `P` be the terminal Sync block's parent state root,
and let `R[i]` be the state root immediately after effect `i`'s effect-ending
transaction:

- for an outbound effect, the effect-ending transaction is the user
  transaction in its `[system load, user]` pair; the system load alone is not a
  checkpoint; and
- for an inbound effect, the effect-ending transaction is its system delivery
  transaction.

Each `R[i]` MUST be derived from execution of the exact terminal-block
transaction prefix through that transaction from state `P`, using the same
block execution environment as the complete terminal block. A Composer MAY
capture these checkpoints during one complete execution or execute the prefixes
separately.

Let `U[j] = entries[j].stateUpdates[0]`. For a batch with `E > 0` effects, the
state updates MUST be:

```text
U[0].currentState = state root at posted       // anchor
U[0].newState = P

U[1].currentState = P                          // effect 0
U[i + 1].currentState = R[i - 1]               for every 0 < i < E
U[i + 1].newState = R[i]                       for every 0 <= i < E

R[E - 1] = terminal Sync-block final state root
```

For an anchor-only batch (`E = 0`), there are no effect checkpoints:
`U[0].currentState` is the state root at `posted` and `U[0].newState` is the
terminal Sync block's final state root. The Composer MUST finalize every entry's
L1 rolling hash only after all state updates have been assigned, because the
rolling-hash seed commits to those updates.

The exact accepted anchor, outbound, and inbound entry shapes are defined in
the [proof-signer profile](../eez-proof-signer/SPEC.md#8-state-update-chain-and-effect-binding).
Those are request-construction constraints: sending unsupported entry shapes
will return no proof.

`callData` MUST be:

```text
0x00 || RLP([blockTxCounts, transactions, l2Entries])
```

It MUST describe every block in the request window. For blocks before the
terminal block it contains every transaction byte-for-byte. For the terminal
Sync block it omits outbound system loads and inbound system deliveries while
retaining outbound user transactions. `l2Entries` contains one derivation
sidecar per effect, ordered outbound first and then inbound. The exact encoding
and sidecar projections are specified in the
[DA profile](../eez-proof-signer/SPEC.md#11-data-availability-and-sync-block-verification).

After assembling the batch, the Composer MUST exact-encode the complete
`postAndVerifyBatch(ProofSystemBatchPerVerificationEntries)` call, including
selector `0xcafef125`, into `post_batch.abi_calldata`.

### 3.2 Header and window bounds

The first streamed chunk MUST contain exactly one `ProveHeader`:

- `rollup_id` is the nonzero L1 rollup-registry ID, not the L2 EIP-155 chain
  ID.
- `from_block` is `posted + 1`, where `posted` is the last L2 block already
  settled on L1. It MUST be nonzero.
- `to_block` is the terminal Sync block and MUST be greater than or equal to
  `from_block`.
- The range MUST cover every L2 block after the last block settled on L1,
  through the terminal Sync block. In the ordinary case this is one settlement
  interval. After a deferred or failed settlement it MAY span multiple
  intervals, and `[from_block, to_block)` MAY contain a previously committed
  empty Sync block. Such a block is sent and validated like every other
  intermediate block; the Composer MUST NOT skip it. For an anchor-only batch,
  the terminal Sync block MUST contain zero transactions; protocol-level
  system writes can still change its state root.
- `post_batch` MUST be present and contain the calldata constructed in section
  3.1.
- `post_batch.public_inputs_hash` is non-authoritative. The Composer SHOULD
  send it empty; the prover response supplies the recomputed hash.
- The current timeless profile requires `post_batch.l1_block_hash` to be empty.

### 3.3 Block chunks

After the header, the Composer MUST send exactly
`to_block - from_block + 1` `BlockWitness` chunks in ascending order. For
zero-based chunk index `i`:

```text
block.number == from_block + i
```

For every block:

- `hash` and `parent_hash` MUST each be exactly 32 bytes;
- for `i > 0`, `parent_hash` MUST equal the preceding chunk's `hash`;
- `rlp` MUST be the exact consensus block RLP, including header and body, with
  no trailing bytes; and
- `witness` MUST be present and contain the complete execution witness for
  that exact block.

The witness fields mirror an Ethereum `ExecutionWitness`:

- `state`: endpoint witness nodes plus the account/state-trie and per-account
  storage-trie removal closures needed for selected intermediate transaction
  roots;
- `codes`: contract bytecodes;
- `keys`: hashed-key preimages; and
- `headers`: ancestor headers required by `BLOCKHASH`.

The account trie and state trie are the same global trie; storage tries are
separate per account. The Composer MUST send the augmented witness regardless
of the prover's internal state backend because the gRPC API has no capability
negotiation.

How the Composer gathers and deduplicates these nodes is implementation-defined.
The wire requirement is that the witness contain enough account-trie and
storage-trie information to recompute every selected intermediate root,
including deletions that may be masked in the block's final state.

### 3.4 Stream completion

The first `ProveChunk` MUST contain exactly one `ProveHeader`. Every subsequent
`ProveChunk` MUST contain exactly one `BlockWitness`, beginning with
`from_block` and ending with `to_block`, inclusive, in strictly increasing block
number order. No later chunk may contain a header or have an empty kind, and the
block sequence MUST contain no missing, extra, duplicated, or reordered blocks.
After sending the `to_block` witness, the Composer MUST close the request
stream. A partial stream is not resumable; retrying requires a new complete
`Prove` call.

### 3.5 Prover acceptance checks

The Composer should expect the prover to return a proof only when all of the
following checks pass:

- the stream has the required header-first shape, declared range, block order,
  rollup identity, field widths, and resource bounds;
- every block exact-decodes, matches its submitted number, hash, parent hash,
  and configured chain rules, and re-executes to its committed state root;
- the blocks form one state-continuous window and contain settlement activity
  only in the terminal Sync block;
- `post_batch.abi_calldata` is the canonical encoding of the supported batch
  profile and matches the configured rollup and proof system;
- the entry state updates form the required chain, and every effect entry's
  `newState` matches the corresponding post-transaction root described in
  section 3.1;
- every inbound and outbound entry matches the applicable executed transaction,
  receipt, event, call hash, value, and ether-delta evidence for that effect;
- `callData` exactly describes the validated block transactions and effect
  sidecars, including byte-exact reconstruction of omitted terminal system
  transactions; and
- the prover can independently recompute the batch's sole `publicInputsHash`.

Any failed check returns no proof. The complete accepted profile and rejection
rules are defined in the
[`eez-proof-signer` specification](../eez-proof-signer/SPEC.md#5-stream-admission).

## 4. Transport behavior

A block witness may exceed gRPC's usual 4 MiB default. Composer implementations
MUST configure encoding and decoding limits large enough for their generated
chunks and the deployed prover's advertised limits. They MUST handle
`ResourceExhausted` when either a per-message or aggregate request limit is
exceeded.

The Composer SHOULD apply an end-to-end request deadline that allows time for
the whole stream and proving operation. A timeout or disconnect does not create
a resumable job; retry with a complete fresh stream.

## 5. Validating and using the response

The Composer MUST reject the response unless:

- `public_inputs_hash` is exactly 32 bytes;
- `signature` is exactly 65 bytes encoded as `r[32] || s[32] || v[1]`;
- the signature is valid secp256k1 ECDSA over the raw 32-byte
  `public_inputs_hash`, without an EIP-191 prefix or EIP-712 domain;
- `s` is canonical low-`s` and `v` is `27` or `28`; and
- recovering the signature over `public_inputs_hash` yields the attester
  registered for the configured `ECDSAProofSystem`.

The Composer MUST also recompute the current profile's sole
`publicInputsHash` from the exact request batch and registered vkey, then require
it to equal `response.public_inputs_hash`. The normative hash construction is
in [proof-signer section 12](../eez-proof-signer/SPEC.md#12-public-input-recomputation),
and cross-language vectors are in
[`eez-protocol/tests/fixtures`](../eez-protocol/tests/fixtures/README.md). This
preflight avoids submitting a response that recovered to the correct attester
but was signed over the wrong batch hash.

After validation, the Composer MUST change only the proof carrier:

```text
assert batch.proofSystems == [ecdsa_proof_system_address]
assert batch.rollupIdsWithProofSystems == [{ rollupId, proofSystemIndexes: [0] }]
batch.proofs = [response.signature]
```

The Composer MUST NOT modify entries, state updates, rolling hashes,
`callData`, proof-system assignments, scheduling counts, or any other proved
batch field after the request. `response.public_inputs_hash` is not inserted
into the batch; EEZ recomputes it on-chain.

Finally, the Composer ABI-encodes `EEZ.postAndVerifyBatch(batch)`, signs the L1
transaction with its poster key, and sends it to the deployment's EEZ contract.
The poster key and proof attester are separate authorities. EEZ recomputes the
public-input hash and calls
`ECDSAProofSystem.verify(response.signature, publicInputsHash)`.

## 6. Failure handling

No gRPC error yields a usable proof. Composer implementations SHOULD handle the
canonical status codes as follows:

### 6.1 Status classification

Only the following statuses are retryable:

| Code | Composer action |
| --- | --- |
| `UNAVAILABLE` | The prover or transport is temporarily unavailable, including a prover that is busy or has not yet reached the required state. Retry the complete request. |
| `DEADLINE_EXCEEDED` | The proving attempt did not complete before its RPC deadline. Retry the complete request if the Composer's settlement cutoff still permits it. |
| `ABORTED` | The prover could not complete the attempt against a stable snapshot or concurrent state. Revalidate or rebuild the request from fresh Composer state, then restart the complete proving operation. |

Every other non-`OK` status is non-retryable for the unchanged request. In
particular:

| Code | Composer action |
| --- | --- |
| `INVALID_ARGUMENT` | Fix malformed stream structure, bounds, widths, calldata, or DA encoding; do not retry unchanged input. |
| `FAILED_PRECONDITION` | Treat the batch, window, deployment identity, or execution evidence as rejected; do not retry unchanged input. |
| `RESOURCE_EXHAUSTED` | Treat the request as exceeding a fixed message, window, witness, or other deployed limit. Change the request or coordinate a limit change before retrying. |
| `CANCELLED` | Respect the cancellation. It normally reflects caller cancellation and MUST NOT trigger an automatic retry. |
| `INTERNAL` | Treat the failure as a prover or operator fault; do not retry automatically. |

This default also covers codes not listed in the second table. Here,
"non-retryable" means that the Composer MUST NOT automatically resend the same
complete request. It does not require the Composer process to stop: the
implementation may discard or recompose the request, correct its configuration,
or alert an operator. The Composer MUST NOT infer retryability from a status
message or implementation-specific error text.

A Composer SHOULD define a bounded recovery policy for repeated non-retryable
prover rejections so that one composition cannot indefinitely prevent
settlement progress. The policy is implementation-defined and MUST NOT rely on
unvalidated failure details.

### 6.2 Actionable failed preconditions

A `FAILED_PRECONDITION` status MAY carry a protobuf-encoded
`prove.v1.ProveFailure` in its gRPC binary status-details field. The Composer
MUST decode the details bytes directly as `ProveFailure`, without a
`google.rpc.Status` wrapper. The gRPC status code remains authoritative:
details attached to any other status MUST NOT authorize candidate removal or
change that status's retry classification.

`ProveFailure.actionable_failure` has two defined variants:

- `OutboundFailure.transaction_index` is the zero-based position of the
  original signed L2 user transaction in the terminal Sync block, not the
  position of its preceding system-load transaction.
  `OutboundFailure.transaction_hash` is that transaction's canonical 32-byte
  signed transaction hash.
- `InboundFailure.entry_index` indexes the complete `PostBatch.entries` array:
  the anchor occupies index zero, so the first effect is at index one.
  `InboundFailure.entry_hash` is the 32-byte
  `keccak256(abi.encode(PostBatch.entries[entry_index]))`.

The original signed L1 transaction for an inbound effect is not present in the
`Prove` request. A Composer that supports actionable recovery MUST therefore
retain its request-local candidate-to-entry mapping until the RPC completes.

Before changing any candidate or pool state, the Composer MUST bind both fields
of the selected variant to the exact rejected request:

1. require the hash field to be exactly 32 bytes and the index to be in range;
2. recompute the transaction or entry hash at that index and require an exact
   match; and
3. resolve that exact transaction or entry through request-local state retained
   during composition to the original selected candidate.

An empty, malformed, unknown, wrong-width, out-of-range, mismatched, stale, or
unresolvable detail MUST be handled as an ordinary non-actionable
`FAILED_PRECONDITION`. The Composer MUST NOT infer a candidate identity from
the status message, diagnostic text, an index alone, or a hash alone.

After a valid detail is resolved, the Composer MUST NOT resend the rejected
request unchanged. It MAY remove the identified candidate and its same-sender,
same-direction nonce suffix, then rebuild the batch, terminal Sync block,
witnesses, and complete `Prove` stream from the remaining candidates. The
rebuilt attempt is subject to the same settlement cutoff as any other proving
attempt.

Index-and-hash validation binds the detail to the rejected request; it does not
independently prove that the reported execution failure occurred. Candidate
removal therefore trusts the configured prover, even when the transport is
authenticated.

### 6.3 Retry procedure and settlement cutoff

For a retryable status, each retry MUST open a new `Prove` call and resend the
complete header and block stream. The Composer SHOULD use exponential backoff
with random jitter, a capped delay, and a bound on total attempts or elapsed
time, consistent with the [gRPC retry guidance](https://grpc.io/docs/guides/retry/).
It MUST NOT sleep or begin another attempt if the remaining settlement window
is too short for a complete stream, proving operation, response validation, L1
transaction construction, and relay or submitter delivery.

The Composer therefore MUST determine its latest safe settlement cutoff before
starting an attempt and re-check it before every retry. Under the current EEZ
slot profile, the proven bundle must reach the relay before the terminal Sync
block's timestamp minus the configured submission slack. A third-party
Composer MAY organize its scheduler differently, but MUST enforce an equivalent
cutoff and reserve time for all post-proof work.

If another complete attempt would miss that cutoff, the Composer MUST stop
retrying, leave `batch.proofs` empty, and MUST NOT submit the unproved batch. It
instead produces and commits an empty terminal Sync block to L2, without the
selected effect transactions or a proof-dependent L1 submission. It preserves
or re-queues the selected effects and tries them again in the next Sync
interval. Because the L1 settlement cursor did not advance, the next successful
proof request begins at the same `posted + 1` and includes the empty Sync block
as an intermediate block.

A malformed response, wrong signer, invalid signature, or mismatch between the
locally recomputed hash and `response.public_inputs_hash` MUST be treated like a
non-retryable failure for that request. The Composer MUST NOT populate
`batch.proofs` or submit the batch.

## 7. Composer conformance

A Composer implementation SHOULD test:

- exact header-first stream ordering and complete block-range emission;
- block hash widths, parent adjacency, exact RLP, and augmented witness
  generation;
- accepted anchor-only, inbound, outbound, and mixed batch construction;
- response length, signature encoding, hash, and registered-attester checks,
  including rejection of a valid registered-attester signature over a hash for
  a different batch;
- proof insertion without mutation of any other batch field;
- the closed retryable-status allowlist and default non-retryable handling;
- accepted outbound and inbound actionable details, plus fail-closed handling
  of details on the wrong status and empty, malformed, unknown, wrong-width,
  out-of-range, mismatched, stale, or unresolvable details;
- request-local actionable-failure resolution, dependent-candidate handling,
  complete recomposition, exponential backoff, and settlement-cutoff fallback
  to an empty Sync block; and
- an end-to-end request whose response is accepted by the Composer and whose
  final `postAndVerifyBatch` succeeds against the deployed EEZ and
  `ECDSAProofSystem` contracts.

The captured successful request in
[`captured-anchor-40155`](../eez-prover-stateless/tests/fixtures/captured-anchor-40155/README.md)
provides a complete positive window and expected public-input hash.
