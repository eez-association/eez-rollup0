# Fix 3 Soundness Review

Date: 2026-06-27

## Scope

This reviews fix 3 only: the driven prover change in
`crates/eez-proverd/src/main.rs` that strips non-target `PostBatch` sidecars
while replaying a composer-directed verification range.

The question is not "did the live run recover?" It did. The question is whether
the fix is sound, what it relies on, and where it remains fragile.

## Short Verdict

My conclusion: the fix is sound for the failure mode it was written for, but it
is not a self-contained proof of safety.

It is safe under the current composer-driven model because:

- The directive names exactly one target `to_block`.
- The prover still re-executes every L2 block in the range.
- The prover still runs settlement gates on the target sidecar.
- The attestation is content-keyed by recomputed `publicInputsHash`.
- The composer ledger advances only after the `ProofSink` verifies a signature.

The weak point is that the helper strips non-target sidecars based only on
height. It does not locally prove that the stripped sidecar is harmless. Today
that harmlessness comes from composer invariants: one deferred post in flight,
rich failed blocks are rolled back, and settled blocks move below the next
directive cursor.

So I would accept this fix as correct for the current system, but I would not
call it future-proof until the prover adds a fail-closed shape check before
stripping non-target sidecars.

## The Bug

In driven mode, the composer sends a `VerifyRange`:

```text
from_block = posted + 1
to_block   = sync block that owns the posted batch
```

The prover subscribes to the control feed from `from_block` and rebuilds a
window. The control feed may replay older `ControlEvent`s inside that range
which still carry historical `composition.post_batch` sidecars.

Before the fix, the prover treated the first replayed sidecar as the settling
sidecar for the current directive. If that sidecar belonged to an older Sync
block, the prover validated the wrong batch and the directed frontier stopped
advancing.

## The Fix

The new helper is:

```rust
fn driven_effective_settlement(
    event: &mut ControlEvent,
    driven: bool,
    target_to_block: Option<u64>,
) -> bool {
    if event.composition.is_none() {
        return false;
    }
    if driven && Some(event.block_number) != target_to_block {
        event.composition = None;
        return false;
    }
    true
}
```

In driven mode:

- A sidecar at `event.block_number == directive.to_block` is the settling
  sidecar.
- A sidecar at any other height is stripped and the block is treated as an
  ordinary non-settling block.

This changes sidecar selection only. It does not skip native execution of the
block.

## Why This Does Not Create A State-Forgery Hole

The prover still validates the full block range through `native-validate`.
Stripping `event.composition` from an interior block does not remove that block
from execution. It only prevents settlement-specific gates from being run
against the wrong sidecar.

At the directive target, the usual gates still run:

- `publicInputsHash` is recomputed from the target `PostBatch`.
- The settlement delta chain is checked against re-executed roots.
- Inbound outcomes are paired against sealed inbound deliveries.
- Outbound entries are checked against signed L2 user transactions.
- OD-5 anchor checking still binds `currentState` to the re-executed batch
  anchor.

The `ProofSink` verifies the prover signature over the recomputed hash before
marking any posted window attested. A composer-provided directive cannot by
itself advance the frontier.

## The Real Assumption

The safety assumption is:

> Any non-target sidecar inside a driven directive range is not an independent
> rich settlement that still needs to be proven as its own batch.

In the current code, that assumption appears to hold because of these composer
rules:

- `compose_sync_slot` has a one-in-flight gate. While an unresolved post exists,
  the composer commits empty Sync blocks without emitting a new rich batch.
- Failed rich Sync blocks are rolled back in `recover_failed_batch` when they
  carry user transactions.
- Empty/minimal Sync blocks can remain canonical because they do not carry
  user cross-chain effects.
- A successfully settled rich block is below the next directive cursor, so it
  should not appear as an interior block in a later directed range.
- Same-anchor unresolved prefixes are superseded by the widest window in
  `PostedWindows::record_posted`.

That makes the observed stale sidecars metadata artifacts, not independent
settlement obligations.

## What The Fix Does Not Prove Locally

`driven_effective_settlement` does not decode the sidecar it strips. It does not
ask whether the sidecar is minimal, rich, outbound-bearing, inbound-bearing, or
malformed. It trusts the composer/control-feed invariant that a non-target
sidecar is safe to ignore.

That is acceptable for the present bug because the invariant is enforced
elsewhere today. But it is not robust against future changes. If a future
composer path allowed a rich, canonical, non-target sidecar inside a later
directive range, this helper would silently erase the settlement metadata before
the prover's settlement gates could see it.

That would probably not forge an L2 state root, because native execution still
runs. The more realistic failure would be semantic: a cross-chain settlement
obligation could fail to get prover-side settlement checks at the height where
it originally appeared.

## Important Correction To A Naive Guard

A guard that checks only `l2ToL1Calls.is_empty()` is not enough.

Outbound entries have non-empty `l2ToL1Calls`, but inbound/deferred entries may
not. A sidecar can be cross-chain-relevant even when every `l2ToL1Calls` field
is empty.

A useful local guard must prove the whole stripped sidecar is minimal. At a
minimum it should decode the `PostBatch` and require the exact empty/minimal
shape expected for non-target sidecars, for example:

- exactly the leading immediate/root-advance entry,
- no deferred inbound entries,
- no outbound settlement entries,
- no sidecar derivation entries in `callData`,
- no user transactions associated with that Sync block.

If the prover cannot prove that shape, it should fail closed instead of
stripping.

## Separate Concern: Signing On Directive Hash Mismatch

There is another behavior near this fix that I would tighten.

In driven mode, the prover logs an error if its recomputed
`publicInputsHash` differs from the directive hint, but it still signs and
submits the recomputed hash.

That is not a direct state-forgery issue: the hash is still recomputed by the
prover, and `ProofSink` verifies the signature. But it is an avoidable accounting
risk because the composer ledger is keyed by `publicInputsHash`, and multiple
windows can sometimes share a hash. A proof for a mismatched directive should not
be allowed to accidentally resolve some other matching window.

Recommendation: in driven mode, if `recomputed_hash != directive.publicInputsHash`,
refuse to submit the proof and re-request the directive. This is stricter and
easier to reason about.

## Test Coverage Assessment

The added unit tests cover the helper behavior:

- non-target sidecar is stripped in driven mode,
- target sidecar is retained in driven mode.

Those tests are necessary but not sufficient. They do not prove the system-level
invariant that a stripped sidecar is harmless.

Tests I would add:

- A driven replay range with an interior minimal sidecar and a target rich
  sidecar; assert the prover validates the target and ignores only the minimal
  sidecar.
- A negative test with an interior rich/cross-chain sidecar; once the local guard
  exists, assert the prover hard-rejects instead of stripping.
- A driven target block with no sidecar; assert the prover refuses after
  streaming past `to_block`.
- A directive hash mismatch test; once tightened, assert no proof is submitted.

## Operational Evidence From The Live Run

The live two-composer run supports the fix:

- The previous settlement freeze cleared after the driven prover change was
  deployed.
- Verified frontiers advanced again and stayed in lockstep.
- Same-block competition produced harmless no-op losers.
- Mixed inbound/outbound Sync blocks were rederived by both composers.
- Recent identity scans after the fix showed no L2 block mismatches.

This is useful evidence that the fix addresses the actual wedge. It is not a
substitute for the local fail-closed guard described above.

## Final Conclusion

I would keep fix 3. The previous behavior was wrong: in driven mode the prover
must validate the directive target, not whichever historical sidecar appears
first in replay.

I would also harden it before considering the area fully closed:

1. Add a local minimal-sidecar check before stripping any non-target sidecar.
2. Refuse to submit a driven proof when the recomputed hash differs from the
   directive hash.
3. Add integration-style tests for stale-minimal, stale-rich, missing-target,
   and hash-mismatch cases.

With those changes, the fix becomes not only correct for today's composer
invariants, but locally self-defending against future composer/control-feed
changes.
