# Rolling back a failed bundle keeps the pure L2 transactions above it

## The bug

When a rich Sync block's bundle fails, `recover_failed_batch` rolls the L2 head
back to the block's parent with `reorg_to`, then commits an empty substitute at
the same height (`crates/eez-composer/src/composer.rs`).

`reorg_to` is a forkchoice update to a canonical ancestor. Every L2 node runs
with `--engine.allow-unwind-canonical-header`, so reth moves its canonical head
straight to the ancestor (reth `fa9e76a`,
`crates/engine/tree/src/tree/mod.rs:985-1084`). It sends no canon-state
notification. The substitute then lands as a plain extension, so the
transaction pool never sees a reorg and never re-adds the transactions mined in
the dropped blocks.

The dropped blocks are the rich Sync block and every Live block built on it
while its bundle was in flight. Their pure L2 transactions are lost:
- the user already had a receipt, and reth keeps serving it, pointing at the
  orphaned block;
- the transaction is never included again;
- the sender's later nonces wait behind the gap.

Cross-chain transactions are not affected: the held pool re-queues them.

The e2e test `failed_rich_bundle_keeps_live_l2_txs` (`cross_chain.rs`)
reproduces it with a relay that acks bundles 5 s late:
- three transfers mined in Live block 92 above rich block 91 are never
  re-included;
- the sender's nonce stays 0 while safe passes 191.

## The fix

Before rolling back, the composer reads the blocks it is about to drop:
everything from the failed Sync height up to the current head. It keeps every
transaction except:
- system transactions;
- the failed batch's cross-chain transactions, which the held pool already
  re-queues or releases.

After the rollback and the substitute commit, it adds the kept transactions back
to reth's pool in block order. The pool re-validates them. One that is no longer
valid is dropped, and logged with its reason.

## Not covered here

- **The Deriver's L1-reorg retreat** (`retreat_l2_to_cursor` → `reorg_to`)
  drops blocks the same way. L1 reorgs past an L2 settlement are rare; it is a
  follow-up.
- **Partial and anchor-only settlements** go the opposite way. The Deriver
  commits L1's variant straight onto its parent, so reth sees a real reorg and
  re-adds every dropped transaction. That includes outbound cross-chain ones
  that L1 didn't apply, which then land as plain L2 calls and burn their nonces
  (seen in the two-composer test on 2026-10-19). It needs its own reproduction
  on a single composer, and is a separate change.
- **Stale receipts:** reth serves receipts for transactions in orphaned blocks
  until they are re-included. That is reth behaviour under
  `--engine.allow-unwind-canonical-header`.
