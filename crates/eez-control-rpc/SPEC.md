# Composer-to-prover streaming RPC

The Rollup0 runtime implements this contract for its current calldata-only,
timeless settlement profile. [prove.proto](proto/prove.proto) is the canonical schema, served as
`prove.v1.Prover/ProveStream`. This is the only supported prover RPC.

The former streaming `prove.v2` namespace has been promoted to `prove.v1`;
the former client-streaming `prove.v1.Prover/Prove` endpoint has been removed.
This is a breaking wire change: upgrade clients and servers together and
regenerate bindings from this schema. There is no legacy endpoint or downgrade.
Streaming message field numbers and semantics are preserved.

The contract separates three things: required streaming behavior, optional
session controls, and settlement inputs. A prover may support blob settlement
without supporting resumable sessions, or resumable sessions without blobs.
Neither requires shared caches or a particular execution backend.

## 1. Required interface

Every implementation supports `ProveStream` with this baseline flow:

```text
Begin(anchor) -> Ready(anchor)
Block(anchor + 1) -> Validated
Block(anchor + 2) -> Validated
...
Finalize(anchor + 1 .. terminal, settlement) -> Proof -> EOF
```

A baseline prover needs only one stream-local prefix and sequential execution.
It MAY limit capacity to one active stream/session, keep no global block cache,
and discard the prefix when the stream ends. Its epoch can be the constant 1.
It need not implement subrange finalization, Resume, Rewind, application-level
Cancel, durable recovery, parallel execution, deduplication or cross-session sharing.

Optional operations and settlement inputs are attempted directly, without a
capability handshake. Unsupported operations, including a
frame with no recognized operation, return a correlated Rejected with
UNIMPLEMENTED. A rejection must not change progress or end a bound stream.
Unsupported Resume on an unbound stream returns an unbound rejection.

Unsupported blobs or posting context also return UNIMPLEMENTED; they must never
be silently discarded. A client may fall back to the baseline
session flow, but must not remove proof-relevant inputs to obtain a proof.

Server capacity, retention, deadlines, chain/rollup rules, ABI and proof system
are deployment parameters, not negotiated by this RPC. Clients and provers must
be configured for the intended rollup and settlement rules. A server must not
reuse retained validation under different rules.

### Baseline client behavior

Keep the stream open from Begin through Finalize and wait for each Block ACK.
On disconnect, open a fresh stream, Begin at the selected execution predecessor and
backfill from durable block/witness storage. On a fork change, cancel the RPC,
Begin again and resend the selected branch. After Proof, start a new stream
for the next range. Clients using optional controls fall back to this flow on
UNIMPLEMENTED: Begin and backfill after unsupported Resume, replace the stream
after unsupported Rewind, and use transport cancellation after unsupported Cancel.

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
The anchor is an execution predecessor, not a claim of L1 acceptance. Admission
checks the configured rollup and the profile's execution-parent rules. For
RollupX, Finalize separately authenticates the selected predecessor; a current
registry root or a caller-supplied state root alone cannot authorize it.

Each Block contains one present BlockWitness: declared number, 32-byte hash
and parent hash, exact consensus header/body RLP, and a present execution
witness sufficient for the configured validation rules. Witness acquisition
is backend-independent at the wire boundary; witnesses cannot be omitted.

New blocks must extend the validated prefix by one with exact hash and
execution-state continuity. Bind wire claims to decoded consensus bytes and
complete execution/evidence validation before emitting Validated. Its number,
hash and 32-byte post_state_root identify that block. Ordinary blocks require
ordinary execution; settlement checkpoints belong to blocks that need them.
These ACKs establish execution facts, not L1 authorization or correctness for
an as-yet-unknown posting target. Finalize checks target-dependent constraints
against those facts, including when the execution came from a cache.

An exact duplicate of an already validated block is idempotent. A contradictory
block or gap is rejected without advancing or replacing the prefix. Rejected
reports the current cursor so the client can supply missing blocks on the same
stream. No cache is necessary to implement this rule. `reused` is diagnostic;
a conforming implementation may always return false.

### Finalize and completion

Finalize selects a nonempty contiguous inclusive [from_block, to_block] range
and its 32-byte terminal_hash. Every selected block and its predecessor must
belong to the validated prefix. The hash must match the selected terminal.

The baseline starts immediately after the session anchor, initially
`from_block == Begin.anchor_number + 1`. That anchor is its settlement parent.
The terminal may be any validated block, including one before the tip.

Starting at a later from_block is optional subrange finalization. Its predecessor
must already be a validated block in the session. A prover that does not support
this selection returns a correlated Rejected with UNIMPLEMENTED, leaving the
anchor, epoch and validated prefix unchanged. The client can open a new stream,
Begin at the desired predecessor and backfill the same requested range; it must
not silently widen the range or change the settlement input.

Parent authorization follows the configured settlement profile, as described in
[section 3](#parent-authorization-by-profile). Current Rollup0 proofs are
conditional on the selected parent; they do not attest its L1 acceptance.
Profiles requiring prover-side L1 authentication obtain the selected parent's
evidence from the prover's own L1/L2 sources, including for an intermediate parent.
Missing required evidence is UNAVAILABLE, not permission to sign. Validated ACKs
alone never establish L1 acceptance. No parent-provenance payload is sent by the
Composer.

Finalize binds the complete settlement input in section 3 to that exact range.
Only successful execution, settlement, DA, proof-system and public-input checks
may yield Proof. A failed finalization yields no proof and does not consume
validated progress. The client independently verifies the returned public-input
hash and proof against the exact request and configured verifier.

Proof closes the stream successfully. Without Resume support the session ends
there; with Resume support it retains the selected range and any later validated
suffix, promotes the selected predecessor to anchor and may release earlier data.
After resuming, the promoted anchor is the baseline parent for that session;
Resume support alone does not require support for selecting a later parent.
Proof never advances the Composer's L1 settlement cursor by itself.

Client half-close drains accepted work; EOF without a Proof is not success.
Transport cancellation stops replies and releases stream-local work. Only
Resume support grants retention after stream teardown. No proof may be used
after the Composer abandons its corresponding batch or settlement cutoff.

## 3. Common finalization input

Every request uses `Finalize.settlement`, containing exact proofless
EEZ.postAndVerifyBatch calldata, physical blobs and an L1PostingContext.
Blobs and posting context are optional at the common wire layer; the configured
profile decides which are required. Today's calldata-only, timeless Rollup0
profile omits both. The RollupX blob profile requires both. Blob-based Rollup0
uses these same fields, with its own payload and settlement rules.
Where the profile requires prover-side parent authorization, evidence retrieval
is the prover's responsibility, not an RPC input.
Supporting the wire fields does not imply that a prover implements every profile.
There is no authoritative Composer-claimed public-input hash, commitment or
custom-data value in this envelope.

The settlement inputs follow RollupX main at
[`e15bb41`](https://github.com/eduadiez/rollupX/blob/e15bb412420b36bfa40a0e2a73233ac4f213441d/crates/eez-control-rpc/proto/prove.proto)
and were checked against Rollup0's
[draft Appendix D](https://github.com/eez-association/specs-draft/blob/2c6d346bea0b3736f7453c35174291495694d1fc/docs/rollup0-spec/D-wire-formats.md)
and [DA/batch rules](https://github.com/eez-association/specs-draft/blob/2c6d346bea0b3736f7453c35174291495694d1fc/docs/rollup0-spec/07-da-batches-bundles.md).
L1PostingContext preserves its field names, types and tags. Unlike RollupX's
existing RPC, this interface leaves provenance retrieval to the prover. It does
not copy RollupX's preparation, lease or cache APIs. The streaming envelope
is not byte-compatible with RollupX's older ProveHeader/PostBatch interface.

A server MUST reject unsupported blobs or posting context
with UNIMPLEMENTED, without producing a proof or changing validated progress.
Unsupported rollup profiles also return UNIMPLEMENTED. Missing profile-required
inputs, malformed encodings or an absent settlement message are INVALID_ARGUMENT;
inconsistent execution/settlement claims yield no proof. Future proof-relevant
inputs that existing decoders would silently discard require a new protocol
version; unknown protobuf fields are not a support check.

### Physical blobs

`blobs` contains the complete intended transaction blob array, in transaction
order. Each element is exactly 131072 bytes of canonical
[EIP-4844](https://eips.ethereum.org/EIPS/eip-4844) field elements. The prover
derives KZG commitments and versioned hashes from these bytes using its trusted
chain configuration, then selects hashes in batch.blobIndices order. No separate
index, claimed commitment or blob proof is sent; network sidecar construction
remains the posting client's responsibility. The actual transaction must carry
the same physical blobs in the same order.

Both RollupX and draft blob-based Rollup0 require at least one blob, empty
batch.callData and batch.blobIndices exactly `[0, 1, ..., blobs.len() - 1]`. They reject gaps,
duplicates, reordered indices, extra/unselected blobs and calldata-only DA.
Resolve the active per-transaction/per-block allowance from the independently
configured L1 chain profile and target timestamp, not a client-supplied limit.

The draft delegates physical packing and framing to
[EEZ Core's blob format](https://github.com/eez-association/eez-core-protocol/blob/9a1f815336c20d28e5ec5cecbf195c41fbd5ab0f/docs/blobs/BLOB_FORMAT_SPEC.md),
shown here at RollupX's pinned Core revision. Each field element carries 31
logical bytes: serialized offset 0 is zero and offsets 31 down to 1 recover
the logical bytes in order. Concatenate elements and blobs in order; messages
may cross blob boundaries. The version-00 stream ends its blob portion with
CloseBlobStream followed by zero padding. The RPC carries the original 131072-byte
blobs, not this unpacked stream or separately encoded per-block fragments.
Core also defines a calldata tail; both blob profiles here require it to be empty.
If another profile permits a tail, it is already in the ABI batch.callData field,
not a second RPC payload.

The rollup-specific payload version and codec live inside the opaque operations
bytes. Draft Rollup0 uses its Appendix D columnar V0 span and restricted action
brackets; RollupX uses its configured native codec. The same RPC fields carry
either without claiming their payloads or execution rules are interchangeable.
Validate the stream, then bind decoded operations and semantic effects to the
selected execution; hashing blobs alone is insufficient. Do not enforce shortest
varints on the outer EEZ stream, which permits non-minimal encodings. Rollup0's
inner V0 uvarints have their own shortest-form rule.

Rollup0's draft explicitly leaves the production Core revision and complete
end-to-end vectors to be pinned. Its companion-docs/blob-da-spec.md is a retired
placeholder, not the normative format. The common RPC can carry the required
data, but wire compatibility does not resolve those deployment/spec blockers.

### L1 posting context

L1PostingContext contains exactly the RollupX target fields:

- chain_id: nonzero and equal to the operator-trusted L1 network;
- target_timestamp: nonzero, the intended child's exact timestamp;
- parent_number: canonical L1 parent height, less than uint64.max; and
- parent_hash: a nonzero 32-byte hash, independently authenticated at that height.

The intended posting block is the child of this parent; neither parent_number
nor that child height is batch.blockNumber. Both blob profiles require
`batch.blockNumber == uint64.max`; they do not use the older timeless mode.
The manager's custom-data derivation is profile-specific:

- RollupX uses `abi.encode(uint256(target_timestamp), parent_hash)` and rejects
  sender-bound batches, as specified by its
  [hash implementation](https://github.com/eduadiez/rollupX/blob/e15bb412420b36bfa40a0e2a73233ac4f213441d/crates/eez-protocol/src/public_inputs.rs).
- Draft Rollup0 uses the richer candidate domain in Appendix D.2: the protocol
  tag, L1 chain ID, EEZ/manager/active-wrapper addresses, EEZ rollup ID and L2 chain
  ID, followed by the target timestamp and L1 parent hash. Its wrapper calls EEZ
  with the unchanged batch and blob context; abi_calldata remains the inner
  proofless EEZ call, not a second wrapper-call encoding.

The existing target fields supply the changing L1 values for both. Derive all
other domain values from trusted deployment configuration and authenticated L1
state, including the active wrapper; do not add caller-selected custom data or
manager/wrapper overrides. The configured profile determines the applicable
rules. Sharing fields does not mean sharing public-input hashes or accepting a
signature under another deployment's domain.

Authenticate the named parent through the prover's configured L1 source and
check the target timestamp against its chain/slot rules. The target is a
proposed environment, not proof of future inclusion. A changed parent hash or
timestamp needs a new Finalize and proof; retained execution may still be reused
only after the new target's constraints are checked. Do not require the named
parent to remain the tip: the on-chain latest-context binding determines whether
the proof can be used in the actual posting environment.

### Parent authorization by profile

**Current calldata-only, timeless Rollup0 keeps conditional proofs.** A proof
for blocks 101–110 establishes the valid transition starting from the selected
block 100; it does not establish that block 100 was already accepted on L1.
The signer binds execution and settlement claims to that exact parent, while the
L1 contract checks each update's `currentRoot` against the live rollup root when
consuming the entry. No independent L1-acceptance query is required of the
stateless signer. This applies equally to baseline and subrange finalization,
retries and cache hits. A valid proof does not guarantee future inclusion or
that the batch will apply when submitted.

The stateful backend still applies its existing admission/fork-choice policy:
Begin checks the local L1-derived safe anchor, and Finalize selects a terminal
consistent with Reth's safe/finalized ancestry. Those checks do not change the
conditional meaning of the common attestation or require the stateless backend
to fetch L1 history.

**RollupX and draft blob-based Rollup0 require prover-side parent authentication.**
At every Finalize, including retries and cache hits, the prover authenticates
the selected settlement parent. For the baseline this is the session anchor,
initially supplied in Begin; with optional subrange finalization it is the
retained block immediately before from_block. Fetch any missing header from the
prover's L2 data source and bind its number, hash and state root to that session
parent. Obtain acceptance history from the configured L1 source; neither a
Composer-supplied anchor nor successful block execution proves L1 acceptance.
This is a check of historical acceptance, not a prediction of future inclusion.

For the RollupX profile, authenticate the configured genesis or the selected
parent's accepted endpoint on canonical L1. Obtain the complete ordered live-slot
endpoint set and its L2 headers, verify them against canonical settlement, and
derive the descendant seed. Every selected block must extend the parent and
satisfy the authenticated seed and timing rules. Registry state at the named L1
target must equal the accepted predecessor or a terminal prefix independently
justified by execution and settlement checks. A matching registry root alone
does not establish parent authorization. These are the obligations enforced by
RollupX's [authorization implementation](https://github.com/eduadiez/rollupX/blob/e15bb412420b36bfa40a0e2a73233ac4f213441d/crates/eez-l1/src/derivation/proof.rs);
its caller-supplied evidence is replaced here by prover-owned retrieval.

For these L1-authenticated profiles, the prover must use a coherent canonical L1
snapshot. Unavailable evidence is UNAVAILABLE; a replaced target is ABORTED;
invalid parent authorization is FAILED_PRECONDITION. These failures have no
actionable candidate-eviction details
and must not consume validated progress. The prover needs access to the required
history, but this contract does not mandate an index or storage layout.

Draft Rollup0 instead requires the selected parent to match its Ethereum-confirmed
settled cursor. Do not apply RollupX's terminal-prefix allowance merely because
both use the same blob transport. Supporting subrange selection never bypasses
the configured rollup's parent-acceptance rules.

This profile aligns the inputs and verification obligations with the pinned
RollupX source. It does not mandate that implementation's worker/cache layout or
assert live streaming interoperability. Its ABI, native codec and chain rules must be
configured together; this proposal does not upgrade this repository's protocol
submodule or runtime to RollupX's deployment.

### Separate delivery-validation gap

Rollup0 draft Chapters 7 and 8 require the exact signed L1 trigger transactions
for simulation of any proposed nonzero synchronous prefix. They are deliberately
absent from blob action identity: the bracket's tx_data is empty. Neither this
settlement-only RPC nor RollupX's current RPC carries those delivery inputs.
That remains a separate interface extension, not implicit support from carrying
blobs or posting context. A service claiming the draft's full validation behavior
must obtain and simulate them through an explicit delivery interface before signing;
without that interface it must reject such requests, not silently skip the check.
No trigger-transaction or bundle-simulation fields are added by this proposal.

### Required settlement input

Every Finalize MUST supply settlement; absence is INVALID_ARGUMENT. The prover
checks the complete input against the configured profile and rejects unsupported
inputs. Never strip required blobs or context to retry as a weaker request.

Calldata-only Rollup0 can use the same settlement message with its optional inputs
absent, then populate the existing blob/context fields when its profile migrates.

The current Rollup0 implementation does not claim full Rollup0-draft or live
RollupX conformance; those settlement profiles need separate implementations.

## 4. Optional advanced session controls

Resume retains progress subject to published limits. It is the first frame
on a new stream, with the retained ID and epoch zero. Ready returns the same
ID, a fresh nonzero epoch and the retained cursor. This atomically fences the old
attachment; copying the new epoch on an old stream cannot regain authorization.
Missing/evicted sessions return NOT_FOUND, requiring Begin and backfill.
A lost ACK can mean more progress was validated
than the client saw; reconcile both cursor number and hash.

Rewind accepts only the anchor or a fully validated ancestor in this session's
active prefix, not a block merely cached elsewhere. It truncates the suffix,
fences pending old-prefix work, advances the epoch and returns Ready at the
ancestor. It cannot replace the anchor. A rejected rewind changes neither
epoch nor prefix. Without Rewind support, replace the stream and backfill instead.

Cancel atomically retires the bound session, fences pending work, returns the
correlated Cancelled at the accepted epoch and closes the stream. No later ACK
or Proof follows. Without Cancel support, use transport cancellation; if Resume
is supported, retained data remains subject to normal eviction. Cancellation never
authorizes deleting another session's data.

Clients may pipeline dependent Block/Finalize frames in order, bounding
outstanding data and draining responses while sending. Servers may read and
execute sequentially, apply backpressure, or reject excess work with
RESOURCE_EXHAUSTED; no unbounded queue or parallel execution is required.
Accepted Resume/Rewind/Cancel fences earlier work before its control response;
stale work cannot mutate the active prefix or emit a usable Proof afterward. Results superseded
by such controls need not receive individual replies. A rejected gap cannot
be skipped by later queued blocks.

Shared caches, singleflight, multiple sessions and parallel execution are
implementation choices, not protocol requirements. If used, bind reused
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
the operation or input is unsupported: use the baseline only if it still expresses
the same complete proving request, otherwise select a suitable prover. Unsupported
settlement data must never be converted into a weaker request. Capacity
rejections use RESOURCE_EXHAUSTED; malformed input is not retried unchanged.

Publish finite message/queue/session/byte/witness limits and bound connect,
send, receive and operation deadlines. The minimal server can reject a second
session at capacity without disturbing the first. No 500 ms or other fixed
deadline is guaranteed by the wire contract.

Conformance should cover baseline and optional exchanges separately. Codec
vectors pin field numbers and request/response encoding; they are not substitutes
for executed-block and on-chain proof oracles. Recorded live request/response
transcripts belong with each runtime implementation.

### Rollup0 runtime

The stateless signer and the stateful follower signer implement Resume, Rewind,
Cancel, subrange selection and bounded pipelining. Both reject nonempty blobs
or any posting context with UNIMPLEMENTED; this is not a blob-profile implementation.
Ordinary execution ACKs do not claim L1 acceptance. Final settlement retains
the current Rollup0 [conditional-proof rules](#parent-authorization-by-profile);
stateful fork selection additionally obeys Reth's safe/finalized ancestry.

Rollup0's Composer requires a pipelining-capable server and retains a live stream,
limiting outstanding requests to eight while draining responses concurrently.
Unsupported session controls and subrange selection fall back to fresh Begin/backfill without
changing the requested settlement. An exchange, including reconnect and EOF,
has a five-second transport deadline; the Composer's proof cutoff can be shorter.

The server admits at most 128 active streams and retains at most 64 sessions.
Pending frames are bounded by the configured window block/witness quotas and
twice its payload-byte quota (window plus Finalize). Session admission applies
the configured window quotas independently, including on cache hits. Output
backpressure uses the stream idle deadline. Stateless execution has four worker
slots and a 512 MiB / 32,768-block cache; stateful execution serializes Reth fork
selection with its follower's reconciliation. These are implementation limits,
not protocol guarantees.

### Executed Rollup0 transcripts

See the [conformance guide](conformance/README.md) for the recorded normal and
boundary cases, deployment profiles, replay instructions and coverage limits.
The standalone client and the stateless signer integration suite share the same
wire replayer and read-only expectations. The integration suite starts a fresh
signer for each case, including cold-cache and configured-limit cases.
