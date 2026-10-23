# EEZ Composer-to-prover streaming specification

This document specifies how a Composer connects to the streaming prover,
validates blocks ahead of settlement, recovers retained progress, and obtains
a proof for `EEZ.postAndVerifyBatch`. MUST, MUST NOT, SHOULD, and MAY describe
the caller's interoperability requirements.

[`prove_stream.proto`](proto/prove_stream.proto) is the canonical RPC schema.
It imports `BlockWitness`, `ExecutionWitness`, and `ProveFailure`
from [`prove.proto`](proto/prove.proto). Generate clients from these schemas;
use their service, package, field numbers, and types directly. The service is
`prove.v2.Prover/ProveStream`; finalization uses `SettlementInput` from the
streaming schema.

The Rollup0 behavior described here follows [PR #206](https://github.com/eez-association/eez-rollup0/pull/206)
at [4209f9e](https://github.com/eez-association/eez-rollup0/commit/4209f9ee4698830e314b01446dc925bc0dc09d07).
It implements the calldata-only, timeless conditional-proof profile. A proof
binds the transition to its selected execution parent; it does not attest that
parent's L1 acceptance. The L1 contract checks applicability when consuming
entries. The [shared streaming contract](STREAMING_SPEC.md) also describes
other settlement profiles; Rollup0 rejects blobs and posting context.

The required wire baseline is sequential:
`Begin -> Block* -> Finalize -> Proof -> EOF`.
Resume, Rewind, Cancel, subrange selection, pipelining, and shared caches are
optional protocol features. Both Rollup0 prover backends implement the
session controls, subranges, and bounded pipelining, and Rollup0's Composer
requires a pipelining-capable server. A baseline-only client can instead wait
for each ACK and start a fresh session for each proof or recovery.

## 1. Composer flow

Keep block validation ahead of settlement so that finalization normally needs
only the remaining blocks and the batch-specific checks:

1. Persist each committed block's exact RLP and augmented execution witness.
2. Open `ProveStream`, send `Begin` at the last L1-settled block, and wait for
   `Ready`.
3. Send contiguous `Block` frames as witnesses become available. Pipeline
   blocks and consume `Validated` responses without waiting for a settlement
   batch to be ready.
4. Assemble the settlement batch with `proofs` empty. Backfill any missing
   blocks and send the candidate terminal Sync block.
5. Send `Finalize` with the exact range, terminal hash, and encoded batch.
6. Check the correlated `Proof`, successful stream completion, public-input
   hash, and signature. Insert only the returned proof bytes into the batch
   and submit the final L1 call before the settlement cutoff.
7. If supported, resume the retained session to continue validating and
   finalizing later ranges without resending its matching validated prefix.
   Otherwise begin a fresh stream at the requested range's predecessor.

Prevalidation MUST NOT block L2 commitment or durable witness capture. If
sending falls behind, keep the witnesses locally and backfill before
finalization. A validation cursor records prover progress; it does not advance
the Composer's L1 settlement cursor. Neither `Ready` nor `Validated` is a proof.
The reference Composer persists witnesses before attempting best-effort
prevalidation, allows at most one capture-triggered prevalidation task, and
prioritizes complete proof requests. Skipped blocks remain on disk for backfill.
A delayed capture at or below the acknowledged cursor, or a capture beyond a
gap, does not rewind or discard newer progress. A complete proof request
reconciles any changed fork and supplies the missing contiguous range.

For example, with block 100 last settled on L1:

```text
Composer                                      Prover
Begin(anchor = block 100)                  -> Ready(cursor = block 100)
Block(101), Block(102)                      -> Validated(101), Validated(102)
... construct the settlement batch ...
Block(103 = terminal Sync block)            -> Validated(103)
Finalize(101, 103, hash(103), settlement)    -> Proof(hash, proof), then EOF
... verify the proof and submit the batch to L1 ...
Resume(session_id)                         -> Ready(cursor = block 103,
                                                    new epoch)
Block(104), ...                            -> Validated(104), ...
```

The last exchange can resume before L1 settlement finishes, but a later proof
range starts after block 103 only once L1 has actually settled that block. A
dropped L1 submission may instead require another range beginning at 101.

## 2. Connection and frame contract

Connect to the configured prover endpoint using standard bidirectional gRPC:

```protobuf
service Prover {
  rpc ProveStream(stream ClientFrame) returns (stream ServerFrame);
}
```

The Composer needs the deployment's rollup registry ID, L2 chain rules, EEZ
contract address, proof-system address, registered attester/vkey, and prover
limits. `rollup_id` is the nonzero L1 registry ID, not the L2 EIP-155 chain ID.
The L1 poster key remains with the Composer; the prover does not submit L1
transactions.

The reference `RemoteProver` derives its vkey by left-zero-padding the configured
20-byte attester address to 32 bytes. The signer and deployed manager MUST use
that same vkey for this client; a separately configured signer vkey is not
independently configurable in the reference client.

Every `ClientFrame` MUST contain exactly one operation and a client-chosen
`request_id` that is unambiguous among outstanding operations. Responses echo
that ID. A session ID is an opaque server-issued 32-byte value. The epoch
identifies the current attachment and prefix, fencing earlier work.

| Operation | `session_id` / `epoch` | Successful response |
| --- | --- | --- |
| `Begin` | Empty / `0`; first frame on an unbound stream. | `Ready` with a new session ID, nonzero epoch, and anchor cursor. |
| `Resume` (optional) | Retained ID / `0`; first frame on an unbound stream. | `Ready` with the same ID, a new nonzero epoch, and retained cursor. |
| `Rewind` (optional) | Bound ID / current epoch. | `Ready` with a new epoch and ancestor cursor. |
| `Block` | Bound ID / current epoch. | `Validated` for that exact block. |
| `Finalize` | Bound ID / current epoch. | `Proof`, then successful stream closure. |
| `Cancel` (optional) | Bound ID / current epoch. | `Cancelled` echoing the retired epoch, then successful stream closure. |

Optional operations are attempted without a capability handshake. Unsupported
operations return a correlated `Rejected` with `UNIMPLEMENTED`, leaving bound
progress unchanged. Use a fresh `Begin` and backfill for unsupported Resume or
Rewind, and transport cancellation for unsupported Cancel. A baseline server
may keep epoch `1`; changing epochs and fencing apply to supported Resume/Rewind.

The Composer MUST wait for `Ready` after `Begin`, `Resume`, or `Rewind` before
sending dependent frames. Check every response's request ID, expected kind,
session ID, and epoch. Only a correlated `Ready` may establish a new binding;
responses from another session or an obsolete epoch MUST NOT update the active
cursor or authorize submission. A fenced stream cannot regain authorization by
copying a newer epoch: reconnect with `Resume`.

An unbound `Rejected` carries an empty session ID, zero epoch, and zero/empty
cursor fields. Treat it as a rejection, never as progress. Session IDs are not
authentication credentials. The RPC has no built-in peer authentication;
independent Composers sharing a deployment require authentication and session
ownership controls outside this interface.

## 3. Streaming and recovering blocks

### 3.1 Begin

`Begin` MUST contain the configured `rollup_id` and the exact last settled
block's `anchor_number`, 32-byte `anchor_hash`, and 32-byte
`anchor_state_root`. Genesis (`anchor_number = 0`) is permitted; the number
MUST allow computing `anchor_number + 1`. No calldata or final range is needed
yet. The initial `Ready.validated_through` and `Ready.validated_hash` MUST match
that anchor, and the session ID and epoch MUST have the shape in section 2.
This is the execution predecessor of the intended range. The Composer selects
it from its reconciled settlement cursor; the wire anchor does not itself
establish L1 acceptance.

A stateful prover checks this anchor against its follower's L1-derived safe
block. Follower lag or an incompatible snapshot can prevent admission. A
successful `Ready` establishes session progress, not proof of L1 applicability.

### 3.2 Block frames and acknowledgements

Send one `Block` frame per block, with `data` containing a `BlockWitness`:

| Field | Requirement |
| --- | --- |
| `number` | The next height after the session's validated or preceding pipelined block. |
| `hash` | Exactly 32 bytes; the submitted block's consensus hash. |
| `parent_hash` | Exactly 32 bytes; the exact preceding block or anchor hash. |
| `rlp` | Exact consensus block RLP, including header and body, without trailing bytes. |
| `witness` | Present; the complete augmented execution witness for this block. |

The witness fields mirror an Ethereum `ExecutionWitness`:

- `state`: endpoint witness nodes plus the account/state-trie and per-account
  storage-trie removal closures needed for selected intermediate transaction
  roots;
- `codes`: contract bytecodes;
- `keys`: hashed-key preimages; and
- `headers`: ancestor headers required by `BLOCKHASH`.

The account trie and state trie are the same global trie; storage tries are
separate per account. The witness MUST support recomputing all selected
intermediate roots, including deletions masked in the final state. Gathering
and deduplicating nodes is implementation-defined. Send the augmented witness
regardless of the prover's internal state backend; the interface has no
backend-capability negotiation.

A baseline client waits for each block's ACK. A client using a pipelining-capable
server MAY pipeline contiguous blocks without waiting for individual ACKs.
It SHOULD bound outstanding work and drain responses while sending to avoid
backpressure stalls. For each `Validated`, match `number` and the 32-byte
`hash` to the submitted block and require a 32-byte `post_state_root`. The
`reused` flag is diagnostic and MUST NOT change proof or response validation.
A `Validated` response means per-block execution and evidence checks completed;
it does not bind a settlement batch or contain a signature.

Resending a fully validated block with identical consensus bytes and identity
is idempotent. A different block at the same height cannot overwrite the
prefix; replacing it requires `Rewind`. A sent but unacknowledged block MUST
NOT be assumed validated.

### 3.3 Resume and backfill

With Resume support, a transport stream is one attachment to a retained
session. After a disconnect, timeout, or successful proof, open a new stream
and send `Resume` as its first frame. Wait for `Ready` with a new epoch.
Its cursor is the last **fully validated** block, not the last block received,
and may include work whose previous ACK was lost.

The Composer MUST compare both cursor number and hash with its intended chain.
Skip only a matching validated prefix, then backfill from the next block. A
number alone cannot establish a match. If the cursor belongs to another
suffix, rewind to a retained common ancestor before sending replacements.

`NOT_FOUND` means the session is no longer retained. Start a fresh `Begin` at
the current last settled block and replay the required contiguous range. Use
a fresh session also when the needed ancestor is no longer retained, or a
reorg replaces the settlement anchor outside the session's active prefix.
Session retention is bounded and is not guaranteed across restart or eviction;
the Composer MUST retain enough exact block and witness data for recovery.
Retain witnesses through L1 finality, not merely posting, so an L1 reorg can
replay unsettled history. The reference witness store checks the current
canonical hash before reusing an entry keyed by height.

If Resume is unsupported (`UNIMPLEMENTED`), begin at the exact requested
predecessor and backfill. Recovery of a lost or pruned prefix MUST preserve the
requested range and settlement input. It must not widen the range or change
calldata to fit the retained session.

### 3.4 Rewind

`Rewind.ancestor_hash` MUST be exactly 32 bytes and identify the session anchor
or a fully validated block in its current active prefix. Merely sent blocks
and blocks cached for another session do not qualify. Rewind cannot move below
or replace the session anchor.

A successful `Ready` moves the cursor to that ancestor and establishes a new
epoch. Check that cursor against the requested ancestor before sending the
replacement suffix. Abandoned validation/finalization work and its old-epoch
responses MUST NOT be treated as progress or a usable proof.

A rejected rewind establishes neither a new epoch nor a new prefix. The
Composer MUST NOT assume queued requests will still be acknowledged and
SHOULD reconnect to reconcile progress. Bound response waits: a control frame
may supersede pending work without an ACK for each superseded block.

### 3.5 Cancel and close

`Cancel` retires only the bound session. Match its correlated `Cancelled`
acknowledgement before treating cleanup as confirmed, then require successful
stream completion. No later ACK or proof is valid for that retired session.
Queueing a cancellation frame alone does not confirm that it was processed.

Closing the request direction without `Cancel` allows accepted work to drain.
With Resume support, half-close, transport cancellation, and disconnect leave
the session resumable subject to eviction. A baseline server may discard it
when the stream ends. Unsupported Cancel can be replaced by transport
cancellation, which does not confirm release of retained session quotas.
These operations do not delete another Composer's session or shared immutable
validation evidence.

## 4. Finalizing a settlement window

### 4.1 Range and SettlementInput

`Finalize` binds the exact batch to a selected inclusive block range:

| Field | Requirement |
| --- | --- |
| `from_block` | `posted + 1`, where `posted` is the Composer's current last L1-settled block; nonzero. |
| `to_block` | Terminal Sync-block number; at least `from_block`. |
| `terminal_hash` | Exactly 32 bytes; hash of the validated block at `to_block`. |
| `settlement` | Present; contains the complete `SettlementInput` for the configured profile. |

The range MUST cover every block after `posted` through the terminal Sync
block. Blocks in `[from_block, to_block)` are intermediate blocks; after a
failed/deferred settlement they may include earlier empty Sync blocks and MUST
NOT be skipped. Settlement activity belongs only in the terminal block. For an
anchor-only batch, the terminal block contains zero transactions, although
protocol-level system writes can still change its state root.

The baseline requires `from_block == session_anchor_number + 1`. Starting
later selects a subrange and requires that optional capability. If rejected
with `UNIMPLEMENTED`, begin a fresh session at `from_block - 1` and replay the
same range with unchanged settlement input.

The entire range MUST be in the session's validated active prefix when
`Finalize` is processed. With pipelining support, the Composer MAY enqueue it
after the missing `Block` frames on the same stream, without waiting for their
ACKs first, but MUST consume and check those responses. Queuing finalization does not bypass a
validation gap. The range may end before the session tip.

The predecessor at `from_block - 1` MUST be the session anchor or an earlier
fully validated block. Its hash MUST be the batch's initial commitment. On
successful finalization with Resume support, the retained anchor moves to this
**predecessor**, not to `to_block`, and earlier prefix data may be released.
`Proof` closes the transport; with Resume support it retains the selected blocks
and any later validated suffix. A proof alone
does not advance L1 settlement; Composer/Deriver cursor checks and the on-chain
commitment gate remain necessary before submission.

Populate `settlement` as follows for current Rollup0:

| Field | Requirement |
| --- | --- |
| `abi_calldata` | Exact proofless `EEZ.postAndVerifyBatch` call, as specified below. |
| `blobs` | Empty. Nonempty blobs return `UNIMPLEMENTED`. |
| `posting_context` | Absent. Even an empty-but-present context returns `UNIMPLEMENTED`. |

An absent settlement message is `INVALID_ARGUMENT`. The streaming envelope
contains no Composer-claimed public-input hash, L1 block hash, or custom data.
The prover recomputes the digest. Unsupported proof-relevant inputs MUST NOT
be stripped to obtain a weaker proof; use a prover implementing that profile.
The [common settlement contract](STREAMING_SPEC.md#3-common-finalization-input)
defines physical blobs and `L1PostingContext` for profiles that support them.

### 4.2 Settlement batch

The request carries the batch that the Composer intends to submit to L1, with
only its proof bytes omitted. The deployed Solidity ABI and its
`ProofSystemBatchPerVerificationEntries` field order and integer widths are
authoritative.

The current profile requires:

| Batch field | Composer requirement |
| --- | --- |
| `expectedRootPerRollup` | Empty. |
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
entries[0].rollupUpdates[0].currentRoot = hash of block posted
entries[i].rollupUpdates[0].currentRoot = entries[i - 1].rollupUpdates[0].newRoot
entries[last].rollupUpdates[0].newRoot = terminal Sync-block hash
```

The anchor is `entries[0]` and is not counted as a cross-chain effect. Effect
`i` is `entries[i + 1]`. Let `E` be the hash of the candidate block holding no
transactions: the terminal Sync block sealed after its configured pre-block
system changes. Rollup0 keeps EIP-2935 history storage and disables EIP-4788
beacon-root updates. This candidate is not the parent's hash: it has its own
header and may include system state changes even with no transactions.
Let `R[i]` be the hash of the candidate block holding the transaction prefix
through effect `i`'s effect-ending transaction:

- for an outbound effect, the effect-ending transaction is the user
  transaction in its `[system load, user]` pair; the system load alone is not a
  checkpoint; and
- for an inbound effect, the effect-ending transaction is its system delivery
  transaction.

Each `R[i]` MUST be derived from execution of the exact terminal-block
transaction prefix through that transaction from the terminal block's parent
state, using the same block execution environment as the complete terminal
block. Every candidate shares the terminal block's fixed header fields —
parent, number and timestamp — while the commitments derived from execution
are rebuilt from the prefix it carries, so `R` is a commitment chain rather
than a parent-child chain. A Composer MAY capture these checkpoints during
one complete execution or execute the prefixes separately. The prover derives
its own checkpoint plan from the decoded transactions: ordinary nonempty
blocks without system transactions need regular execution only; potential Sync
blocks retain the empty-prefix and effect-ending checkpoints, and empty
terminals retain their pre-execution checkpoint. The Composer does not select
checkpoint positions over the RPC.

Let `U[j] = entries[j].rollupUpdates[0]`. For a batch with `N > 0` effects, the
state updates MUST be:

```text
U[0].currentRoot = hash of block posted       // anchor
U[0].newRoot = E

U[1].currentRoot = E                          // effect 0
U[i + 1].currentRoot = R[i - 1]               for every 0 < i < N
U[i + 1].newRoot = R[i]                       for every 0 <= i < N

R[N - 1] = terminal Sync-block hash
```

For an anchor-only batch (`N = 0`), the sole pre-execution checkpoint seals the
complete empty terminal; there are no post-transaction effect checkpoints:
`U[0].currentRoot` is the hash of block `posted` and `U[0].newRoot` is the
terminal Sync block's hash. The Composer MUST finalize every entry's
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
selector `0xe4a480e4`, into `settlement.abi_calldata`.

### 4.3 Acceptance requirements

Per-block validation does not replace finalization. The Composer should expect
a proof only when:

- the selected blocks form one execution-consistent chain, with settlement
  activity only in the terminal Sync block;
- the calldata exact-decodes as the supported batch profile for the configured
  rollup and proof system;
- state updates form the required chain of candidate block hashes;
- inbound and outbound effects match their executed transactions, receipts,
  events, call hashes, values, and ether deltas;
- DA matches the validated transactions and effect sidecars, including exact
  reconstruction of omitted terminal system transactions; and
- the prover independently recomputes the batch's sole `publicInputsHash`.

These requirements apply on every finalization, including when all blocks
were already validated. Reusing execution evidence does not authorize changed
calldata without these checks. The stateless prover checks the
transition from the selected parent without querying L1 acceptance history.
The stateful prover additionally selects the already validated terminal in
Reth, enforces safe/finalized ancestry, and checks the terminal's canonical
identity. It does not require the selected subrange predecessor to equal the
follower's current safe block at every Finalize. Retained progress does not
guarantee that a later finalization succeeds. The detailed settlement profile is in the
[proof-signer specification](../eez-proof-signer/SPEC.md).

## 5. Proof verification and L1 submission

A `Proof` MUST be correlated to the exact `Finalize`, session, and epoch. The
Composer MUST observe successful stream completion and reject unsolicited
subsequent frames. EOF before the proof is not success.

Reject the response unless:

- `public_inputs_hash` is exactly 32 bytes;
- `proof` is exactly 65 bytes encoded as `r[32] || s[32] || v[1]`;
- it is a valid secp256k1 ECDSA signature over the raw `public_inputs_hash`,
  without an EIP-191 prefix or EIP-712 domain;
- `s` is canonical low-`s` and `v` is `27` or `28`; and
- recovering the signer yields the attester registered for the configured
  `ECDSAProofSystem`.

The Composer MUST independently recompute the batch's sole `publicInputsHash`
from the exact request batch and registered vkey, and require equality with
`response.public_inputs_hash`. See the
[hash construction](../eez-proof-signer/SPEC.md#12-public-input-recomputation)
and [cross-language vectors](../eez-protocol/tests/fixtures/README.md).
A valid registered-attester signature over another batch's hash is insufficient.
The reference client performs both checks in its common proof-verification path.
The proof remains conditional on the selected parent; it does not establish L1
acceptance, future inclusion, or successful L1 target-call execution.

After validation, change only the proof carrier:

```text
assert batch.proofSystems == [ecdsa_proof_system_address]
assert batch.rollupIdsWithProofSystems == [{ rollupId, proofSystemIndexes: [0] }]
batch.proofs = [response.proof]
submit_l1(EEZ_ADDRESS, abi_encode(EEZ.postAndVerifyBatch(batch)))
```

The Composer MUST NOT alter entries, state updates, rolling hashes, DA,
proof-system assignments, scheduling counts, or any other submitted batch field.
In particular, the current digest does not bind the scheduling counts; keeping
them unchanged is a Composer submission policy, not a property of the signature.
See the [signer's scheduling boundary](../eez-proof-signer/SPEC.md#12-scheduling-boundary).
Do not insert `response.public_inputs_hash` into the batch; EEZ recomputes it
on-chain and calls `ECDSAProofSystem.verify(response.proof, publicInputsHash)`.
The Composer signs the L1 transaction with its poster key, which is distinct
from the proof attester.

## 6. Rejections and recovery

### 6.1 Status and cursor handling

Failures appear as either an RPC status or a correlated `Rejected` frame.
`Rejected.code` is the numeric gRPC status, `message` is diagnostic text, and
`details` may carry an actionable failure. Neither form yields a usable proof.

| Code | Composer action |
| --- | --- |
| `UNAVAILABLE` | Temporary transport/prover unavailability or missing follower state. Recover progress and retry within the settlement cutoff. |
| `DEADLINE_EXCEEDED` | Recover progress and retry if time permits. A deadline or idle timeout does not itself retire the session. |
| `ABORTED` | Recheck the anchor, range, and epoch against fresh state, then recover or rebuild the operation. |
| `NOT_FOUND` | A lost session requires a fresh `Begin` and backfill; do not resend frames under its missing ID. |
| `UNIMPLEMENTED` | Replace unsupported session optimizations with baseline Begin/backfill or transport cancellation. Unsupported settlement inputs require a compatible prover; never omit them. |
| `INVALID_ARGUMENT` | Correct malformed frames, fields, bounds, calldata, or DA. Do not retry unchanged input. |
| `FAILED_PRECONDITION` | The batch, execution evidence, identity, or requested prefix was rejected. Recompose/correct it rather than retrying unchanged. |
| `RESOURCE_EXHAUSTED` | Reduce resource use or arrange a limit change before retrying. Reconnecting alone may not free retained session quotas. |
| `CANCELLED` | Respect cancellation; do not automatically retry. |
| `INTERNAL` | Treat as a prover/operator fault; do not automatically retry. |

Only `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and `ABORTED` permit automatically
retrying a proving attempt. Session loss and unsupported optional operations
permit the distinct recovery actions above. The reference client can also rebuild once at the exact requested
anchor when retained history exhausts a quota or Finalize rejects the retained
range with `INVALID_ARGUMENT` or `UNIMPLEMENTED`; this uses the same settlement
input and deadline. This does not make malformed input or global capacity
failure generally retryable. Other non-`OK` codes, including unknown codes,
are non-retryable for unchanged input. Never infer retryability or candidate
identity from diagnostic text.

A session-bound `Rejected` reports `validated_through` and the 32-byte
`validated_hash` at rejection time. Reconcile it with the newest matching
progress observed in the current epoch; do not regress on an older response.
For a retryable rejection leaving the prefix intact, the Composer MAY resend
from the first missing block on the same stream. It MUST NOT assume already
pipelined later blocks crossed the rejected gap. If the stream ended or its
binding is uncertain, reconnect with `Resume` before choosing what to resend.

A finalization timeout does not by itself discard validated blocks. Recover
any missing suffix and retry the exact finalization only while its range and
batch remain applicable. Use `Rewind` or a new session for a changed suffix.
A Composer SHOULD bound recovery so one rejected composition cannot
indefinitely prevent settlement progress.

### 6.2 Actionable settlement failures

A `Rejected` with code `FAILED_PRECONDITION` MAY carry a protobuf-encoded
`ProveFailure` in `details`. Decode those bytes directly, without a
`google.rpc.Status` wrapper. Details on another code MUST NOT authorize
candidate removal or change retry classification.

`ProveFailure.actionable_failure` identifies one candidate:

- `OutboundFailure.transaction_index` is the zero-based position of the
  original signed L2 user transaction in the terminal Sync block, not its
  preceding system load. `transaction_hash` is that transaction's canonical
  32-byte signed transaction hash.
- `InboundFailure.entry_index` indexes the complete settlement `batch.entries` array,
  with the anchor at zero. `entry_hash` is the 32-byte
  `keccak256(abi.encode(batch.entries[entry_index]))`.

The original signed L1 transaction for an inbound effect is not transmitted.
The Composer MUST retain its finalization-local mapping from candidates to
entries until the attempt ends. Before changing candidate or pool state:

1. Correlate the rejection to the exact `Finalize`, session, and epoch.
2. Require a 32-byte hash and an in-range index; recompute the transaction or
   entry hash at that index and require an exact match.
3. Resolve that identity through the retained request-local mapping to the
   original selected candidate.

Empty, malformed, unknown, wrong-width, out-of-range, mismatched, stale, or
unresolvable details are ordinary non-actionable `FAILED_PRECONDITION`s.
Do not apply details to another finalization or infer a candidate from a
block/control rejection, diagnostic text, an index alone, or a hash alone.

After a valid resolution, the Composer MAY remove the candidate and dependent
same-sender, same-direction nonce suffix, then rebuild the batch, terminal
Sync block, and witnesses. Rewind before the changed suffix or begin a new
session, send the replacement blocks, and finalize the rebuilt calldata.
Unchanged validated blocks may be reused. The same settlement cutoff applies.
Index/hash checks bind the report to the request; candidate removal still
trusts the configured prover's report of failure.

### 6.3 Capacity, deadlines, and settlement cutoff

Configure gRPC encoding/decoding limits for the deployment: a block witness
may exceed the usual 4 MiB default. Bound outstanding frames and handle both
operation-level quota rejections and RPC-level failures. Limits cover retained
sessions, blocks, submitted bytes, witness items, and queued requests. Cache
reuse does not exempt a session from admission limits. Rewind, successful
anchor promotion, or cancellation can release session-owned prefix data;
reconnecting alone does not.

Bound connection, write, read, and completion waits. Idle-stream timeouts and
individual operation deadlines are distinct from session lifetime. The final
proof-attempt budget MUST cover any reconnect/backfill, terminal-block
validation, finalization, and response transit/validation. Streaming moves
execution ahead of that deadline; it guarantees no fixed latency.

Use bounded retries with backoff and jitter, and reserve time for L1
transaction construction and relay/submitter delivery. Under the EEZ slot
profile, the proven bundle must reach the relay before the terminal Sync
block's timestamp minus configured submission slack. Recheck that cutoff
before every attempt; do not start another if the remaining time is inadequate.

If proving cannot finish before the cutoff, the Composer MUST leave
`batch.proofs` empty and MUST NOT submit the unproved batch. The normal Composer
path proves the candidate before committing it: discard the uncommitted
candidate, rebuild and commit an empty terminal Sync block without the selected
effects, retain/re-queue those effects, and try again in the next interval.

If an implementation has already committed a speculative candidate, first
reconcile L1 settlement. Never roll back an L1-confirmed block or re-queue an
already consumed effect. If the candidate is still unratified, roll it back to
its parent, rebuild the empty replacement and its witnesses, and commit that
replacement. Rewind the prover to the same parent or begin a new session before
sending the replacement branch. When L1 settlement has not advanced, the next
range starts at the same `posted + 1` and includes the empty Sync block.
A late proof for the abandoned candidate MUST NOT authorize submission.

Malformed responses, wrong signers, invalid signatures, or public-input hash
mismatches are non-retryable for that request and MUST NOT populate
`batch.proofs` or authorize submission.

### 6.4 Reference runtime limits

These are current implementation limits, not wire guarantees:

- The Composer allows at most eight outstanding frames while draining replies.
  A five-second exchange timeout includes acquiring the client lock, connecting,
  sending, receiving, recovery, and EOF; the Composer cutoff may be shorter.
  Queued/active proof requests take priority over best-effort prevalidation.
- The server admits at most 128 active streams and retains at most 64 sessions.
  Disconnected sessions may be evicted at capacity. Reconnecting does not
  guarantee retention or immediately release abandoned session quotas.
- Per-session defaults are 512 blocks, 512 MiB of encoded block payloads, and
  1,000,000 witness items. Pending work is separately bounded by at most the
  block quota plus one frame, twice the payload-byte quota, and the witness
  quota. Session quotas charge each newly admitted block even on a cache hit;
  Finalize calldata is bounded by the message and pending-frame limits.
- The default stream idle timeout is 120 seconds, applied while waiting for
  input with no active work and to output backpressure. Active validation uses
  the request timeout. Defaults are 600 seconds for stateless operations and
  240 seconds for stateful operations; Finalize has its own operation budget.
- Stateless execution has four worker slots and a cache bounded by 512 MiB and
  32,768 blocks. Stateful execution serializes Reth branch selection with
  follower reconciliation; fresh branch availability must still be checked.

See the [signer configuration](../eez-proof-signer/SPEC.md#3-startup-configuration)
for deployment knobs. A retained transport may already have hit its idle
limit: the next exchange can fail before recovery on a later attempt. Treat
that as a bounded transport failure and reconcile the retained session within
the remaining cutoff; do not assume cached progress guarantees the next proof.

## 7. Composer conformance

A Composer implementation SHOULD test:

- sequential baseline behavior and unsupported-control/subrange recovery without
  changing the requested settlement input;
- Begin/Resume/Rewind bindings, response correlation, epoch fencing, duplicate
  blocks, fork replacement, and pipelined ACK handling;
- dropped ACKs, disconnects, session loss, exact-hash cursor reconciliation,
  backfill, rejected-gap recovery, delayed captures, and skipped prevalidation;
- block identity, exact RLP, augmented witnesses, and accepted anchor-only,
  inbound, outbound, and mixed batches;
- finalization over a retained subrange, terminal-hash mismatch rejection,
  calldata binding, proof verification, and proof insertion without mutation
  of any other batch field;
- resumption after a proof, subsequent anchor advancement based on actual L1
  settlement, and cancellation acknowledgement/completion;
- bounded pending work, quotas, deadlines, independent sessions, and both
  RPC-status and `Rejected` recovery, including fresh-anchor quota recovery;
- actionable details with exact finalization-local binding and fail-closed
  handling of malformed or mismatched details; and
- recomposition, empty-Sync cutoff handling, and a full streaming exchange whose
  final `postAndVerifyBatch` succeeds against the configured EEZ and
  `ECDSAProofSystem` contracts.

The [conformance guide](conformance/README.md) documents 17 normal and four
configured-limit wire cases, their shared standalone/integration replayer, and
coverage limits. Those captured empty-block transcripts exercise this Rollup0
profile; they do not establish nonempty Sync-effect behavior, live L1 recovery,
blob/RollupX support, or deadline/stress guarantees.
