# Incremental Composer-to-prover RPC (v2)

Status: protocol proposal for staged implementation. `prove.v1.Prover/Prove`
remains the only active runtime endpoint in this PR. The wire schema is
[`proto/prove_stream.proto`](proto/prove_stream.proto).

## Identity and sharing

One `ProveStream` belongs to one Composer session. `Begin` creates an opaque
server-issued session ID; `Resume` can reconnect to that session. Every response
echoes the session and client request IDs. A server MUST NOT use either ID as a
block, validation, or proof identity. An authorized Composer's session may refer
to immutable validated-block artifacts already computed for another session,
but cannot mutate another session's cursor, pending finalization, or result.
Where multiple independently operated Composers share a prover, the transport
MUST authenticate them and bind the session ID to that identity. The opaque ID
is not a substitute for authentication.

The reusable block identity includes the operator-configured rollup/chain and
validation profile, exact consensus RLP, computed block hash, parent hash, and
execution/proof version. A successful cached artifact contains locally checked
pre/post roots, receipts, and settlement evidence; for a possible terminal Sync
block it also contains all required transaction checkpoints. The server MUST
exact-decode an offered block and compare its canonical bytes and header/body
commitments before satisfying it from cache. A witness is proving auxiliary
data, not part of the consensus block identity. A failed or malformed witness
MUST NOT populate a negative cache entry for the block identity.

Identical in-flight validation may be single-flighted, but cancellation of one
waiter MUST NOT cancel work still required by another. A completed artifact may
be shared only after backend replay and the shared output cross-check succeed.
For the stateful backend, Reth's block tree is the shared continuation store:
candidate blocks are imported as unsafe blocks and sessions retain only exact
anchor/prefix identities plus immutable validation artifacts. Process-global
forkchoice selection and unsafe-head state reads are serialized; one Composer's
session never owns or mutates another Composer's cursor. Merely sharing a
post-state root without the Reth block and its exact ancestry is insufficient.

## Session state machine

`Begin` declares the nonzero rollup ID and the exact number, hash, and state root
of the last settled block. The first expected streamed block is derived as
`anchor_number + 1`. There is no proof range or settlement payload at this
stage. The server checks identity and quotas and returns `Ready`. A resumed
stream receives the server's authoritative **validated**, not merely received,
cursor and hash. If no retained session or trusted checkpoint exists, it must
fail or report the earlier recoverable cursor; the Composer backfills from its
persistent witness store before finalizing.

The Composer sends a contiguous run as back-to-back `Block` frames on the same
long-lived RPC as each block's RLP and witness become available. It may pipeline
those frames without waiting for a `Validated` response between blocks. Keeping
one block per frame lets the prover decode, execute, acknowledge, retry, and
apply backpressure at block granularity rather than buffering a whole run first.
For each session the declared numbers must rise by one, hashes must link, and
the first block must extend the declared anchor. A duplicate of exactly the
same validated block is idempotent; a different block at the same height is a
new fork/session generation, not an overwrite. `Validated` is emitted only
after complete per-block execution and shared input/output checks. Its `reused`
bit is diagnostic. An ACK is never an attestation.

`Finalize` specifies the exact contiguous inclusive `[from_block, to_block]`
range, the terminal Sync-block hash, and canonical proofless
`postAndVerifyBatch` calldata. The range must start at the session anchor + 1
and end at a fully validated block. The server takes an immutable snapshot of
the ordered artifacts and reruns the existing shared state-chain, intermediate
block, terminal effect/checkpoint, exact DA, proof-system, and public-input
gates against that calldata. Only this complete path may invoke the attester.
Each Composer receives its own `Proof` or `Rejected` frame, even when another
Composer used the same blocks. A successful final result may be cached only
under the complete range, ordered block identities, terminal hash, exact
calldata, proof system/vkey, and validation profile. A public-input hash or
state root alone is not a sufficient cache key.

`Proof` closes the current transport stream, but it does not retire the
session. The Composer retains the opaque session ID and resumes it when later
unsafe blocks extend the same safe anchor. This matters when a valid proof was
produced but its L1 submission was dropped or remains unresolved: subsequent
blocks still arrive above the old anchor and must not force the whole prefix to
be replayed. The session is retired only by `Abort`, anchor replacement, or
bounded eviction.

`Abort` ends only its named session. A disconnect need not abort a retained
session. A reorg or contradictory block invalidates the affected branch and
all descendant artifacts and finalization candidates. Stateful validation must
also recheck its L1-derived follower's canonical anchor and every exact block
identity before signing. Setting FCU with `head = terminal` and `safe = posted`
is necessary to select the branch and make its unsafe-head state readable, but is
not itself an ancestry proof: Reth permits a known safe block and a conflicting
known head. The session's checked parent links from the exact safe hash are the
ancestry proof. The prover does not assume that a transport ACK proves the
Composer's current L1 cursor; the on-chain state-root gate and the existing
Composer/Deriver cursor rules remain independent.

## Capacity and timing

The present single-active-request gate is incompatible with parallel Composer
sessions. Admission needs bounded per-Composer sessions, in-flight blocks,
bytes, witnesses, and finalizations, plus a global execution cap and fair
scheduling. No gRPC or prover backpressure may block L2 block commitment;
Composer retains witnesses for retry. Failed capacity admission is explicit and
does not silently downgrade validation. Artifact storage is bounded and may be
evicted only when no active session needs it; a cache miss causes replay, never
an unchecked proof.

The 500 ms Composer proof budget begins at `Finalize`, not `Begin`; it includes
the terminal-block work, settlement validation, signing, and response transit.
An old validated prefix is not replayed within that budget. Measure witness
capture, send, validation ACK, finalization, and submission separately. The
stateful backend must take fresh Reth providers at each block and finalization;
it must not hold one read snapshot across an arbitrarily long stream. It adds no
parallel execution database: Reth stores imported blocks and execution state,
while the streaming layer stores only bounded session identities and immutable
checked artifacts. A terminal block needing transaction checkpoints may be
re-executed once on its Reth parent state when it arrives.
