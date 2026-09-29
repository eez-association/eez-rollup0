# Incremental Composer-to-prover RPC (v2)

Status: protocol proposal for staged implementation. `prove.v1.Prover/Prove`
remains the only active runtime endpoint in this PR. The wire schema is
[`proto/prove_stream.proto`](proto/prove_stream.proto).

## Identity and sharing

One `ProveStream` belongs to one Composer session. `Begin` creates an opaque
server-issued session ID; `Resume` can reconnect to that session. `Begin` MUST
carry an empty session ID and epoch zero. A successful `Ready` echoes its request
ID and carries the newly issued nonempty session ID and nonzero fencing epoch. A
`Rejected` response to `Begin` carries an empty session ID and epoch zero because
no session was created.

`Resume` is the first frame on a new unbound stream and carries the retained
session ID and epoch zero. Success atomically transfers that session to the new
stream, fences any previous stream by issuing a new nonzero epoch, and returns
the authoritative validated cursor in `Ready`. The server binds that epoch to
the stream; subsequent frames and pending responses from a fenced stream are
rejected, and that stream cannot reclaim the session by copying or guessing a
newer epoch. All other client frames carry the current epoch. Responses echo
their client request ID and carry the session's authoritative epoch;
`Cancelled` echoes the accepted epoch that it retired. A response that cannot
refer to an authorized session carries epoch zero. A server MUST NOT use the
session ID, request ID, or epoch as a block, validation, or proof identity. An
authorized Composer's session may refer to immutable validated-block artifacts
already computed for another session, but cannot mutate another session's
cursor, pending finalization, or result. Where multiple independently operated
Composers share a prover, the transport MUST authenticate them and bind the
session ID to that identity. The opaque ID is not a substitute for
authentication.

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
candidate blocks are downloaded directly from the Composer and imported as
unsafe blocks. Import, or selecting a branch with forkchoice, does not make a
block trusted. The exact predecessor selected by `Finalize` must match the
L1-derived follower's last safe number and hash, and every imported block in the
range must pass execution and consensus validation and link by exact parent hash
from that safe block. The follower's safe and finalized labels remain
L1-derived.

Sessions retain exact anchor/prefix identities plus bounded immutable
validation artifacts, including the consensus bytes already needed by
settlement. Process-global forkchoice selection and unsafe-head state reads are
serialized; one Composer's session never owns or mutates another Composer's
cursor. Merely sharing a post-state root, importing a block, or setting
`head = terminal` is not an ancestry proof. Reth may stop exposing an unsafe
fork after another Composer selects a competing head, so finalization
re-submits the session's checked ordered blocks under the same forkchoice lock
before the attestation gates run.

## Session state machine

`Begin` declares the nonzero rollup ID and the exact number, hash, and state root
of the last settled block. The first expected streamed block is derived as
`anchor_number + 1`. There is no proof range or settlement payload at this
stage. The server checks identity and quotas and returns `Ready`. After `Begin`,
`Resume`, or `Rewind`, the Composer waits for `Ready` before sending more frames
because that response establishes the stream's fencing epoch. A resumed stream
receives the server's authoritative **validated**, not merely received, cursor
and hash. If no retained session or trusted checkpoint exists, it must fail or
report the earlier recoverable cursor; the Composer backfills from its persistent
witness store before finalizing.

The Composer sends a contiguous run as back-to-back `Block` frames on the same
long-lived RPC as each block's RLP and witness become available. It may pipeline
those frames without waiting for a `Validated` response between blocks. Keeping
one block per frame lets the prover decode, execute, acknowledge, retry, and
apply backpressure at block granularity rather than buffering a whole run first.
For each session the declared numbers must rise by one, hashes must link, and
the first block must extend the declared anchor. A duplicate of exactly the
same validated block is idempotent. A different block at the same height is
rejected and MUST NOT implicitly overwrite the active prefix; replacing an
unsafe suffix requires `Rewind`. `Validated` is emitted only after complete
per-block execution and shared input/output checks. Its `reused` bit is
diagnostic. An ACK is never an attestation.

A session-bound `Rejected` reports the session's fully validated cursor and
hash at the time of rejection. A rejection that cannot refer to an authorized
session has `validated_through = 0` and an empty `validated_hash`. For a
retryable rejection that leaves the active prefix unchanged, the Composer may
continue on the same stream by resending from the block after the newest cursor
it has observed for that epoch; it does not need `Resume` or `Rewind`. The server
MUST NOT advance past a rejected gap using already-pipelined later frames. A
contradictory block that replaces a validated suffix still requires `Rewind`.

`Rewind` carries the current epoch and the hash of the common ancestor. That
hash must identify the anchor or a fully validated block in this session's
current active prefix; received or globally cached blocks are insufficient. On
success the server atomically increments the epoch, moves the session cursor to
the ancestor, drops that session's later prefix and pending finalization, and
returns `Ready` with the new epoch and cursor. The Composer waits for this
`Ready`, then pipelines replacement `Block` frames under the new epoch. A
rewind cannot replace or move below the session anchor. `Finalize` may advance
the anchor within the validated prefix; any other anchor replacement requires a
new `Begin`.

Every queued validation and finalization captures its session epoch. Once an
epoch changes, work from an older epoch cannot mutate the session cursor or
emit a proof. Independently valid old-fork work may still populate the immutable
shared artifact cache, and another Composer session using that fork is
unaffected. The server MUST NOT emit old-epoch responses after the `Ready` that
establishes a new epoch.

`Finalize` specifies the exact contiguous inclusive `[from_block, to_block]`
range, the terminal Sync-block hash, and canonical proofless
`postAndVerifyBatch` calldata. The range may start at any fully validated block
in the session's active prefix and must end at a fully validated block. Its
predecessor must be either the current session anchor or an earlier fully
validated block in that prefix. `Finalize` promotes that exact predecessor to
the range's settlement anchor: the submitted batch must name its hash as the
initial commitment, and a stateful prover must also match its number and hash
to the L1-derived follower's current safe block. The session's previously
validated parent links prove that the selected range descends from it.

The server takes an immutable snapshot of only the selected ordered range and
reruns the existing shared state-chain, intermediate block, terminal
effect/checkpoint, exact DA, proof-system, and public-input gates against that
calldata. Only this complete path may invoke the attester. Before emitting
`Proof`, the server rechecks that the snapshot's epoch is still current;
otherwise it discards the stale result and rejects that finalization. On
success, the retained session advances its anchor to the selected predecessor
and may release its earlier prefix. Each Composer receives its own `Proof` or
`Rejected` frame, even when another Composer used the same blocks. A successful
final result may be cached only under the complete range, ordered block
identities, selected anchor, terminal hash, exact calldata, proof system/vkey,
and validation profile. A public-input hash or state root alone is not a
sufficient cache key.

`Proof` closes the current transport stream, but it does not retire the session.
The Composer retains the opaque session ID and may resume it to validate later
blocks or finalize a later range after its predecessor becomes the new safe
anchor. This avoids resending blocks and witnesses already validated in that
session. If a valid proof's L1 submission is dropped or remains unresolved, the
anchor does not advance past that proof's predecessor and the retained prefix
remains available. `Resume` issues a new epoch before work continues. An anchor
replacement that is not a validated forward move within this session requires a
new `Begin`; the session is otherwise retired only by `Cancel` or bounded
eviction.

`Cancel` carries the current session ID and epoch in its frame. When the server
accepts it, the server atomically retires only that session, drops its pending
finalization, fences its queued work and responses, releases its quotas and
session-owned artifact references, emits the correlated `Cancelled` response,
and closes the stream successfully. No `Validated`, `Proof`, or other response
from that session may follow `Cancelled`. The explicit acknowledgement tells the
Composer that the cleanup signal was processed; merely enqueueing a frame is not
enough. A transport cancellation or disconnect is not an application-level
`Cancel` and leaves the session resumable.

Cancellation is a release signal, not an instruction to delete shared data.
The server MAY eagerly remove session-local state and evict shared cache entries
that have become unreferenced, subject to its normal retention policy. It MUST
NOT cancel single-flighted work still needed by another session or delete
immutable artifacts or Reth blocks that another session may still use. A
successful `Rewind` likewise invalidates only that session's abandoned suffix
and finalization candidates. Stateful validation must recheck the follower's
last safe number and hash and every exact block identity before signing. An FCU
update may select the branch and make its unsafe-head state readable, but the
checked safe anchor plus exact parent links are the ancestry proof. The prover
does not assume that a transport ACK proves the Composer's current L1 cursor;
the on-chain commitment gate and the existing Composer/Deriver cursor rules
remain independent.

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
An old validated prefix is not re-executed in the normal retained-branch path.
If Reth has evicted a competing unsafe fork, correctness takes precedence: the
stateful backend re-imports the exact checked blocks, and that work counts
against the finalization deadline. Measure witness capture, send, validation
ACK, finalization, and submission separately. The stateful backend must take
fresh Reth providers at each block and finalization; it must not hold one read
snapshot across an arbitrarily long stream. It adds no parallel execution
database: Reth stores imported blocks and execution state, while the streaming
layer stores only bounded session identities and immutable checked artifacts. A
terminal block needing transaction checkpoints may be re-executed once on its
Reth parent state when it arrives.
