# Request lifecycle

[`service/sessions.rs`](../src/service/sessions.rs) owns the sole streaming
runtime. The [wire contract](../../eez-control-rpc/SPEC.md) defines protocol
semantics; the [signer specification](../SPEC.md) defines settlement gates.

1. `Begin` checks the rollup and exact anchor, then returns an opaque session
   ID and fencing epoch. `Resume` binds a retained session to a new stream and
   advances its epoch; accepted progress belongs to the session.
2. `Block` is charged against session quotas. The backend validates or reuses
   execution against the selected parent. The runtime appends only after
   checking that the epoch and parent are unchanged, then sends `Validated`.
3. `Rewind` selects an ancestor in the active prefix and fences outstanding work.
   `Cancel` retires the session. Neither operation authorizes settlement.
4. `Finalize` selects an exact retained range. The backend prepares attestation;
   stateful mode selects its terminal through Reth forkchoice. A blocking
   settlement worker then binds calldata, effects and DA to checked evidence.
5. Immediately before signing, the runtime rechecks the epoch and cancellation.
   Only the complete settlement job can construct the attester's input. Success returns `Proof` and EOF while retaining recoverable progress.

Idle and operation deadlines are independent. A failed operation reports its
correlated status and accepted cursor. Cache entries are immutable execution
facts; session IDs and epochs authorize work and cannot be recovered from a
cache hit. A disconnected caller may resume acknowledged or newly accepted
progress, or begin again and backfill from its durable witnesses.

Shutdown stops new listener acceptance and drains active streams. Backend work
that outlives a timeout retains its own execution permit or reconciliation
guard; cancellation never turns partial execution into reusable evidence.
