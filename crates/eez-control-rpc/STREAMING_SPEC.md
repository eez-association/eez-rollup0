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
Begin(anchor, expected_validation_identity) -> Ready(anchor, validation_identity, supported_features)
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
this list contains every required feature.

| Feature | Additional contract |
| --- | --- |
| RESUME | Retain a session across disconnect/Proof and reattach with a new epoch. |
| REWIND | Replace a validated suffix on the same stream, with a new epoch. |
| CANCEL | Explicitly retire a session and acknowledge cancellation. |
| PIPELINING | Accept bounded dependent Block/Finalize frames without per-block ACK waits. |
| BLOBS | Accept physical EIP-4844 blobs and independently derive their commitments. |
| POSTING_CONTEXT | Authenticate the named L1 target and supplied L2 parent provenance. |

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

### Validation identity

Begin MUST carry the client's independently configured 32-byte
expected_validation_identity. The server compares it with its locally derived
validation namespace before allocating a session or accepting blocks. Missing
or wrong-width identities are INVALID_ARGUMENT; a mismatch is FAILED_PRECONDITION
with no actionable failure details. These are unbound Rejected responses.

Every Ready MUST carry that local validation_identity, not an unchecked echo.
The client checks it before using the session, including after Resume/Rewind.
The identity and capabilities remain fixed for the session; a server whose rules
changed must reject a resumed session. Missing or mismatching identities are invalid.
Do not learn the expected identity by blindly trusting the first server reply.

The digest algorithm belongs to the configured validation profile, not to the
transport. For RollupX, use its existing
[identity implementation](https://github.com/eduadiez/rollupX/blob/1eab3955775aec75612a7144d70f8245771ba97d/crates/eez-control-rpc/src/identity.rs):
schema 4, domain-separated RLP/Keccak over the rollup/chain IDs, system/EEZL2
addresses, canonical loaded chain-document digest/version, validator revision
and dependency digest. Resource limits are not identity components. This
build-specific namespace is not a universal cross-implementation hash; other
backends need an explicitly agreed profile/identity, not an assumed match from
the chain ID alone. Equality checks agreement on validation rules, not signing authority.

### Baseline client behavior

Keep the stream open from Begin through Finalize and wait for each Block ACK.
On disconnect, open a fresh stream, Begin at the selected execution predecessor and
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
The anchor is an execution predecessor, not a claim of L1 acceptance. Admission
checks the validation identity and the profile's execution-parent rules. For
RollupX, Finalize separately authenticates the selected predecessor; a current
registry root or a caller-supplied state root alone cannot authorize it.

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

Every request uses `Finalize.settlement`, containing exact proofless
EEZ.postAndVerifyBatch calldata, physical blobs, an L1PostingContext and
ParentProvenance. The last three inputs are optional at the common wire layer;
the configured profile decides which are required. Today's calldata-only,
timeless Rollup0 profile omits all three. RollupX requires all three. Blob-based
Rollup0 uses the same blob and L1 target fields; its parent must be authenticated
under its own settled-cursor rules. Supporting the wire fields does not imply
that a prover implements every profile.
There is no authoritative Composer-claimed public-input hash, commitment or
custom-data value in this envelope.

The settlement inputs follow RollupX main at
[`e15bb41`](https://github.com/eduadiez/rollupX/blob/e15bb412420b36bfa40a0e2a73233ac4f213441d/crates/eez-control-rpc/proto/prove.proto)
and were checked against Rollup0's
[draft Appendix D](https://github.com/eez-association/specs-draft/blob/2c6d346bea0b3736f7453c35174291495694d1fc/docs/rollup0-spec/D-wire-formats.md)
and [DA/batch rules](https://github.com/eez-association/specs-draft/blob/2c6d346bea0b3736f7453c35174291495694d1fc/docs/rollup0-spec/07-da-batches-bundles.md).
L1PostingContext and ParentProvenance preserve its field names, types and tags.
They are new `prove.v2` messages here: this does not replace either project's
v1 RPC or copy RollupX's preparation, lease or cache APIs.
The outer v2 envelope is not byte-compatible with RollupX's v1 ProveHeader/PostBatch.

A client requests its required input capabilities in Begin and checks Ready.
BLOBS covers physical blobs; POSTING_CONTEXT covers the L1 target and L2 parent
provenance. A server MUST reject unadvertised use of either with UNIMPLEMENTED,
not discard those fields. Unsupported validation profiles are never enabled
merely by accepting a feature flag. Missing profile-required inputs, malformed
encodings or an absent settlement message are INVALID_ARGUMENT;
inconsistent execution/settlement claims yield no proof. Future proof-relevant
extensions must require explicit feature or protocol negotiation: unknown protobuf fields
alone do not negotiate support.

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
manager/wrapper overrides. The profile and its validation identity must identify
the applicable rules. Sharing fields does not mean sharing public-input hashes
or accepting a signature under another deployment's domain.

Authenticate the named parent through the prover's configured L1 source and
check the target timestamp against its chain/slot rules. The target is a
proposed environment, not proof of future inclusion. A changed parent hash or
timestamp needs a new Finalize and proof; retained execution may still be reused
only after the new target's constraints are checked. Do not require the named
parent to remain the tip: the on-chain latest-context binding determines whether
the proof can be used in the actual posting environment.

### Selected L2 parent provenance

RollupX requires ParentProvenance on every Finalize, including retries and
cache-hit windows. It contains the canonical RLP of the selected L2 predecessor,
its accepted_l1_number (zero only for the configured genesis), and the complete
ordered live_slot_endpoint_headers_rlp set. These are **L2 headers**, not an L1
ancestor-header chain. Bind the parent header's number/hash/state root to the
predecessor of from_block; do not assume it is still the original Begin anchor.

The prover independently authenticates the configured genesis or that parent's
L1 acceptance, verifies the complete live-slot endpoint set against canonical
L1 observations, and derives the descendant seed. Every selected block must
extend that parent and use the authenticated seed and allowed timing. Registry
state at the named target parent must equal the accepted predecessor or a
terminal prefix independently justified by execution and settlement checks.
A matching registry root or a manually assigned root alone is not provenance.

RollupX limits each supplied header to 1024 bytes, exact-decodes and round-trips
its RLP, and charges all headers to byte/witness quotas. Its
[authorization implementation](https://github.com/eduadiez/rollupX/blob/1eab3955775aec75612a7144d70f8245771ba97d/crates/eez-l1/src/derivation/proof.rs)
checks the named L1 parent's hash before and after the reads for a coherent
snapshot. Unavailable evidence is UNAVAILABLE; a replaced target is ABORTED;
invalid provenance is FAILED_PRECONDITION. These failures have no actionable
candidate-eviction details. No proof may be issued until all authorization,
execution and settlement gates pass, regardless of earlier Validated ACKs.

The preceding parent-provenance rules are RollupX-specific. Draft Rollup0 instead
requires the candidate's named parent to match its Ethereum-confirmed settled
cursor; do not apply RollupX's terminal-prefix allowance merely because both
use the same blob transport. Any supplied ParentProvenance must be bound to the
selected predecessor and checked under the configured profile, never trusted.

This profile aligns the inputs and verification obligations with the pinned
RollupX source. It does not mandate that implementation's worker/cache layout or
assert live v2 interoperability. Its ABI, native codec, chain rules and validation
identity must be configured together; this proposal does not upgrade this
repository's protocol submodule or runtime to RollupX's deployment.

### Separate delivery-validation gap

Rollup0 draft Chapters 7 and 8 require the exact signed L1 trigger transactions
for simulation of any proposed nonzero synchronous prefix. They are deliberately
absent from blob action identity: the bracket's tx_data is empty. Neither this
settlement-only RPC nor RollupX's current RPC carries those delivery inputs.
That remains a separate interface extension, not implicit support from BLOBS or
POSTING_CONTEXT. A service claiming the draft's full validation behavior must
obtain and simulate them through an explicit delivery interface before signing;
without that interface it must reject such requests, not silently skip the check.
No trigger-transaction or bundle-simulation fields are added by this proposal.

### Required settlement input

Every Finalize MUST supply settlement; absence is INVALID_ARGUMENT. The prover
checks the input against the configured profile and negotiated capabilities.
Never strip required blobs or context to retry as a weaker request.

Calldata-only Rollup0 can use the same settlement message with its optional inputs
absent, then populate the existing blob/context fields when its profile migrates.

This design-only change neither changes the existing runtime nor claims full
Rollup0-draft or live RollupX conformance.

## 4. Optional advanced session controls

RESUME retains progress subject to published limits. Resume is the first frame
on a new stream, with the retained ID and epoch zero. Ready returns the same
ID, a fresh nonzero epoch, the retained cursor, validation identity and unchanged
capabilities. This atomically fences the old attachment; copying the new epoch on an old
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
