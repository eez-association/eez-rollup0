# Bundle send after commit

A Sync block's `[postBatch, user tx…]` bundle goes to the relay only after the block commits on
L2, and L1 alone decides what happened to it.

## Order

1. The composer builds the rich Sync block, signs the postBatch, records the batch in the
   optimistic ledger and spawns the bundle observer. It logs `eez.composer.bundle.staged`. The
   observer waits.
2. The Sequencer commits the block (`commit_one_prebuilt`). On success it releases the bundle
   (`SyncSlotBlock.release`, a `BundleRelease`).
3. The observer sends the bundle, logs `eez.composer.bundle.dispatched` and watches L1.

A minimal postBatch takes the same steps and logs `eez.composer.phase1.bundle.staged` and
`eez.composer.phase1.bundle.dispatched`.

If the commit fails, or another block commits at that height, the release is dropped and the
bundle is never sent: it logs `eez.composer.bundle.withheld`, never `dispatched`. So is a bundle
whose `Exact` target L1 block was built before the commit, which the relay would drop. Recovery
re-queues its txs without charging an attempt, unless reth rejected the block as invalid
(`BundleRelease::rejected`): its contents caused that, so its txs are charged. No bundle is in
flight for a block that is not in the chain.

Historical chunks have no block to wait for and send at once. Their one event is
`eez.composer.emission.historical_chunk`.

## Outcome

`Submitter::send_bundle` returns a `SentBundle { reply, outcome }`. It returns an error only when
nothing was sent.

- `reply` is the relay's answer: `Accepted`, `Rejected` or `Unanswered`.
- `outcome` is what L1 shows: the post landed (`Included`), landed but its settlement could not be
  read (`IncludedUnread`), or the target passed without it (`Dropped`).

`bundle_verdict` maps the sent bundle to a verdict from `outcome` alone:

| L1 shows | Verdict |
|---|---|
| post landed, final state is the claimed endpoint | settled |
| post landed, some entries applied | settled partially |
| post landed, nothing applied | failed |
| post landed, settlement unread | unresolved: the batch stays pending until the cursor decides; failed without a charge if an L1 reorg drops the post first |
| target passed without the post | failed |

The relay's answer only affects the charge. A failed batch charges its txs an attempt only if the
relay reply was `Accepted`, the pinned slot was not skipped and the target block's timestamp
matches the pin. A `Rejected` reply says nothing about the txs.

## Cost

The send starts later by the time the commit takes. Both `dispatched` events carry
`commit_wait_ms`.

## Tests

- `relay_reply_lost_bundle_still_settles` (`eez-node-e2e`, `cross_chain`): the builder stub takes
  the bundle and drops the reply (`ack-lost`); the batch still settles.
- `relay_reply_only_waives_the_charge` and `withheld_bundle_is_never_sent` (`eez-composer`).
- `stale_parent_withholds_the_bundle` (`eez-driver`).
