# Composer-to-prover streaming RPC

Status: proposed wire contract, following the design-only scope of
[PR #168](https://github.com/eez-association/eez-rollup0/pull/168).
This change does not implement a Composer or prover runtime. The existing
[window RPC specification](SPEC.md) and endpoint remain unchanged.
[prove_stream.proto](proto/prove_stream.proto) is the canonical schema.

The contract separates three things: required streaming behavior, optional
session controls, and settlement inputs. A prover may support blob settlement
without supporting resumable sessions, or resumable sessions without blobs.
Neither requires shared caches or a particular execution backend.

## 1. Required interface and capability discovery

Every implementation supports `ProveStream` with this baseline flow:

```text
Begin(anchor) -> Ready(anchor, supported_features)
Block(anchor + 1) -> Validated
Block(anchor + 2) -> Validated
...
Finalize(range, settlement) -> Proof -> EOF
```

A baseline prover needs only one stream-local prefix and sequential execution.
It MAY limit capacity to one active stream/session, keep no global block cache,
and discard the prefix when the stream ends. Its epoch can be the constant 1.
It need not implement Resume, Rewind, application-level Cancel, pipelining,
durable recovery, parallel execution, deduplication or cross-session sharing.

`Begin.required_features` lists features the client cannot operate without.
Empty means no optional requirement. An unsupported or unknown required feature
(including UNSPECIFIED) MUST reject Begin with UNIMPLEMENTED and allocate no
session. The first Ready advertises the complete optional feature set enabled
for this session and rollup. Before sending blocks the client MUST verify that
this list contains every required feature; this also detects older servers
that silently ignored the new Begin field.

| Feature | Additional contract |
| --- | --- |
| RESUME | Retain a session across disconnect/Proof and reattach with a new epoch. |
| REWIND | Replace a validated suffix on the same stream, with a new epoch. |
| CANCEL | Explicitly retire a session and acknowledge cancellation. |
| PIPELINING | Accept bounded dependent Block/Finalize frames without per-block ACK waits. |
| BLOBS | Accept and validate EIP-4844 sidecars in the common settlement input. |
| POSTING_CONTEXT | Accept and verify the posting-environment/custom-data inputs below. |

Capabilities are stable for a session and MUST be repeated unchanged after
Resume/Rewind. Unknown advertised features may be ignored; clients MUST NOT
use unadvertised features. There are no implicit dependencies between these
features. A resumed session that cannot still honor its capabilities must be
rejected, not silently downgraded.

Unsupported optional controls return a correlated Rejected with UNIMPLEMENTED
without changing progress or ending a bound stream. An unsupported Resume on
an unbound stream returns an unbound rejection. Server capacity, retention,
deadlines, configured chain/rollup rules, ABI and proof system are deployment
parameters, not inferred from support for one optional feature.

### Baseline client behavior

Keep the stream open from Begin through Finalize and wait for each Block ACK.
On disconnect, open a fresh stream, Begin at the current settlement anchor and
backfill from durable block/witness storage. On a fork change, cancel the RPC,
Begin again and resend the selected branch. After Proof, start a new stream
for the next range. Do not attempt Resume/Rewind unless advertised.

Streaming remains useful without these optimizations: ordinary block execution
happens before finalization. It does not guarantee a fixed proof latency.
Prevalidation and backpressure MUST NOT block L2 commitment or durable witness
capture. Bound the final proof attempt including any reconnect/backfill and
leave time for proof verification and L1 submission.

## 2. Frames and required validation

A ClientFrame contains exactly one operation and an unambiguous outstanding
request_id. Every response echoes that ID. Begin is the first frame on an
unbound stream, with an empty session_id and epoch zero. Its Ready returns a
new opaque 32-byte session ID, a nonzero epoch and the exact anchor cursor.
Thereafter frames carry that stream's binding; the baseline simply echoes the
same epoch throughout. A session ID is not a block/proof identity or an
authentication credential. Shared deployments must authenticate and isolate
Composers independently of these fields.

A response without an authorized binding has empty session_id, zero epoch,
zero validated_through and empty validated_hash. Otherwise Ready/Rejected
report the authoritative fully validated cursor and its 32-byte hash, not
merely the last received block. Clients correlate ID, epoch, request ID and
response kind before changing local progress. Ready and Validated are not
attestations.

### Begin and Block

Begin supplies the nonzero rollup registry ID and exact anchor number/hash/
state root. Both hashes are 32 bytes; anchor_number must permit adding one.
The anchor is the last block settled on L1. Admission checks configured chain
identity and the anchor evidence required by that rollup's validation rules.

Each Block contains one present BlockWitness: declared number, 32-byte hash
and parent hash, exact consensus header/body RLP, and a present execution
witness sufficient for the configured validation rules. Witness acquisition
is backend-independent at the wire boundary; omitting witnesses is not one of
this proposal's capabilities.

New blocks must extend the validated prefix by one with exact hash and
execution-state continuity. Bind wire claims to decoded consensus bytes and
complete execution/evidence validation before emitting Validated. Its number,
hash and 32-byte post_state_root identify that block. Ordinary blocks require
ordinary execution; settlement checkpoints belong to blocks that need them.

An exact duplicate of an already validated block is idempotent. A contradictory
block or gap is rejected without advancing or replacing the prefix. Rejected
reports the current cursor so the client can supply missing blocks on the same
stream. No cache is necessary to implement this rule. `reused` is diagnostic;
a conforming implementation may always return false.

### Finalize and completion

Finalize selects a nonempty contiguous inclusive [from_block, to_block] range
and its 32-byte terminal_hash. Every selected block and its predecessor must
belong to the validated prefix. The hash must match the selected terminal.
Range selection may begin after the original anchor or end before the tip.
The selected predecessor is the settlement anchor; L1 applicability and the
rollup's settlement rules are checked at finalization, not established by ACKs.

Finalize binds the complete settlement input in section 3 to that exact range.
Only successful execution, settlement, DA, proof-system and public-input checks
may yield Proof. A failed finalization yields no proof and does not consume
validated progress. The client independently verifies the returned public-input
hash and proof against the exact request and configured verifier.

Proof closes the stream successfully. Without RESUME the session ends there;
with RESUME it retains the selected range and any later validated suffix,
promotes the selected predecessor to anchor and may release earlier data.
Proof never advances the Composer's L1 settlement cursor by itself.

Client half-close drains accepted work; EOF without a Proof is not success.
Transport cancellation stops replies and releases stream-local work. Only
RESUME grants retention after stream teardown. No proof may be used after the
Composer abandons its corresponding batch or settlement cutoff.

## 3. Common finalization input

New requests use `Finalize.settlement`, a SettlementInput containing exact
proofless EEZ.postAndVerifyBatch calldata, zero or more blob sidecars and an
optional PostingContext. This is not Rollup0's PostBatch: blobs and context are
first-class wire inputs rather than an overloaded legacy L1 hash field.
No Composer-claimed public-input hash is authoritative.

The same envelope covers Rollup0's calldata-only/timeless case (no blobs or
posting context) and blob/context-dependent rollups such as RollupX.
Advertising BLOBS/POSTING_CONTEXT means the server supports those inputs for
the configured rollup rules; it is not a claim to support arbitrary managers,
DA codecs or verifier versions. Those rules are deployment configuration.

A sender must request the capabilities it requires in Begin and check Ready.
The server MUST reject unadvertised blob/context use with UNIMPLEMENTED,
including when those inputs would otherwise be ignored. Empty inputs never
make calldata requiring missing blobs or context acceptable. Malformed or
inconsistent input is rejected; a server MUST NOT drop fields and sign a
weaker statement. An absent/unrecognized finalization input is INVALID_ARGUMENT.
Future proof-relevant input extensions must require a new advertised feature
or a new input variant; unknown protobuf fields alone do not negotiate support.

### Blob sidecars

The blob encodings and KZG/versioned-hash rules follow
[EIP-4844](https://eips.ethereum.org/EIPS/eip-4844). For BLOBS, the request
supplies exactly one BlobSidecar for every distinct
index referenced by batch.blobIndices, sorted by index, with no extras:

- index is the position in the **intended L1 transaction's blob array**, not
  the sidecar's position in this request; it must fit uint32 and the configured
  chain's transaction blob-index bound;
- blob is exactly 131072 bytes of canonical EIP-4844 field elements;
- commitment and proof are each 48-byte canonical KZG encodings; and
- the prover verifies the blob KZG proof against that commitment using the
  configured chain's trusted setup and rejects invalid data/proofs.

Derive the versioned hash from the checked commitment, not a client hash claim.
Build the public-input blob-hash list by looking up indices in the exact
batch.blobIndices order (including repeated references). Check the rollup's
blob DA decoding against the selected validated blocks and settlement effects.
The Composer must post the same selected blobs/commitments at those transaction
indices; EEZ's BLOBHASH-based public inputs enforce the commitment binding.
The transaction may contain other, unreferenced blobs. Its complete envelope
is not supplied here; ensuring these indices exist in the actual posting
transaction remains the Composer's responsibility.

### Posting context

PostingContext carries the intended EEZ caller (20-byte sender), L1 posting
block number/timestamp, 32-byte parent hash, and per-rollup custom data. The
posting block number is **not** batch.blockNumber: the latter selects the
manager's getCustomData behavior. A supplied context has a nonzero posting
block number, and rollup_data has one entry per batch participating rollup in
batch assignment order, with matching IDs and no duplicates.

When historical context is needed, ancestor_headers may carry canonical L1
header RLP newest-first from parent_hash. Check exact header decoding, hashes,
consecutive numbers and parent links. A header chain supplied by the Composer
is not by itself authenticated L1 evidence: anchor it to the prover's trusted
L1 view, or obtain the needed headers independently.

The prover must independently derive/check each custom_data value using the
configured manager/rollup rules and authenticated context, not merely hash a
claimed custom-data blob. For a manager whose implementation matches the
reference timeless/past/current modes, this means deriving its empty data,
historical block hash, or posting timestamp/parent hash as appropriate.
Unknown rules, unavailable evidence or inconsistent claims yield no proof.

EEZ public inputs bind the selected versioned blob hashes, calldata, manager
custom-data hashes and sender when batch.bindMsgSenderInPublicInput requests
it. Use zero for the bound sender otherwise. The context envelope itself is
**not** automatically hashed by EEZ: a proof must not claim an execution fact
depends on a context field the configured verifier does not bind. If additional
context must be bound, the rollup/verifier contract must provide that binding;
adding a protobuf field cannot provide it.

Context for a not-yet-mined posting block is a proposed environment, not proof
of L1 inclusion. Verify its derivable constraints and bind the applicable
public inputs. The Composer must ensure the actual posting produces those
same inputs; if the applicable context changes, recompute/reprove rather than
submitting the old proof or stripping context. An already-mined L1 block hash
is not required merely to propose a future posting environment.

### Compatibility and unsupported inputs

The original field 4, Finalize.post_batch, remains a deprecated oneof variant
for the existing calldata-only Rollup0 encoding. Its l1_block_hash must be
empty; its claimed public-input hash is ignored. New input uses field 5, so
older decoders see a missing PostBatch instead of silently ignoring additional
fields inside an accepted PostBatch. Do not send both variants or automatically
downgrade a failed blob/context request.

The generated accessor changes to Finalize.input. Runtime adapters for this
proposal must dispatch it and perform capability checks explicitly. This
design-only change does not alter the existing window runtime or assert that
RollupX settlement execution is already implemented in this repository.

## 4. Optional advanced session controls

RESUME retains progress subject to published limits. Resume is the first frame
on a new stream, with the retained ID and epoch zero. Ready returns the same
ID, a fresh nonzero epoch, the retained cursor and unchanged capabilities.
This atomically fences the old attachment; copying the new epoch on an old
stream cannot regain authorization. Missing/evicted sessions return NOT_FOUND,
requiring Begin and backfill. A lost ACK can mean more progress was validated
than the client saw; reconcile both cursor number and hash.

REWIND accepts only the anchor or a fully validated ancestor in this session's
active prefix, not a block merely cached elsewhere. It truncates the suffix,
fences pending old-prefix work, advances the epoch and returns Ready at the
ancestor. It cannot replace the anchor. A rejected rewind changes neither
epoch nor prefix. Without REWIND, replace the stream and backfill instead.

CANCEL atomically retires the bound session, fences pending work, returns the
correlated Cancelled at the accepted epoch and closes the stream. No later ACK
or Proof follows. Without CANCEL, use transport cancellation; if RESUME is
enabled, retained data remains subject to normal eviction. Cancellation never
authorizes deleting another session's data.

PIPELINING allows dependent Block/Finalize frames to queue in order. Bound
outstanding data and drain responses while sending. Accepted Resume/Rewind/
Cancel fences earlier work before its control response; stale work cannot
mutate the active prefix or emit a usable Proof afterward. Results superseded
by such controls need not receive individual replies. A rejected gap cannot
be skipped by later queued blocks.

Shared caches, singleflight, multiple sessions and parallel execution are
implementation choices, not required capabilities. If used, bind reused
evidence to exact consensus bytes, execution parent and validation rules.
Isolate session progress and results; cancellation of one waiter must not
cancel work still needed by another. Nothing in this protocol mandates Reth
forkchoice, a database layout, or a second execution store.

## 5. Errors, limits and conformance

Rejected.code is the standard numeric gRPC status code; message is diagnostic,
not machine-readable control flow. details may contain the existing ProveFailure
encoding when an actionable settlement candidate is known. Clients must bind
such details to the exact request's index/hash before changing candidates.
RPC transport errors and operation rejections never constitute proofs.

UNAVAILABLE, DEADLINE_EXCEEDED and ABORTED may permit bounded retries after
reconciliation. NOT_FOUND permits session-loss recovery. UNIMPLEMENTED means
a required feature is absent: use the baseline only if it still expresses the
same complete proving request, otherwise select a suitable prover. Unsupported
settlement data must never be converted into a weaker request. Capacity
rejections use RESOURCE_EXHAUSTED; malformed input is not retried unchanged.

Publish finite message/queue/session/byte/witness limits and bound connect,
send, receive and operation deadlines. The minimal server can reject a second
session at capacity without disturbing the first. No 500 ms or other fixed
deadline is guaranteed by the wire contract.

Conformance should cover baseline and optional exchanges separately. Codec
vectors pin field numbers and capability negotiation; they are not substitutes
for executed-block and on-chain proof oracles. Recorded live request/response
transcripts belong with each runtime implementation. Keep vector fixtures and
their test harness separate from this protocol proposal.
