# Two-Composer Competition Fixes

Date: 2026-06-27

## Context

This run tested two independent composers against the same Chiado rollup:

- Same registry and rollup id.
- Separate L2 nodes, provers, and L1 poster keys.
- No out-of-band coordination between composers.
- L1 transaction ordering arbitrates competing `postBatch` submissions.

The target behavior is that both L2 chains remain byte-identical, both composers
continue proving and settling, both posters land batches, and same-block
competition is harmless.

## Problems Found

### 1. Same-block competition loser was replayed as if it were actionable

When both composers landed `postBatch` transactions in the same L1 block, the
contract correctly applied the lower `transactionIndex` and made the other
transaction a no-op. The deriver did not have a clean path for this case. It
could still try to interpret the non-applied loser as replayable work, even
though local state already matched the winning batch.

Evidence:

- Same L1 block contained two competing `postBatch` transactions.
- One transaction had `state_applied=true`; the other was a harmless no-op.
- The local L2 state matched the winner, but the loser still appeared in the
  derivation stream.

Root cause:

- The deriver did not explicitly recognize a non-applied batch whose claimed
  current root was stale relative to the already-advanced local root.

Fix:

- Added same-block loser detection in
  `crates/eez-deriver/src/deriver.rs`.
- If a non-applied batch has a stale claimed current root and local state already
  reflects the winning batch, the deriver skips it as a harmless competition
  loser.

Tests added:

- `stale_non_applied_batch_is_same_block_loser`
- `deferred_path_with_matching_anchor_is_not_loser`
- `applied_batch_with_stale_anchor_remains_hard_misalignment`

### 2. Posted-window ledger kept stale straddling windows alive

The composer tracks posted windows that are waiting for L1 settlement/proof
progress. In some cases, the L1 cursor had advanced far enough to make old
windows stale, but the verified frontier did not numerically advance. The old
logic only pruned on frontier movement, so stale straddlers could remain in the
ledger and interfere with forward progress.

Evidence:

- The on-chain settlement cursor advanced.
- The already-attested frontier could remain unchanged.
- Old windows crossing the cursor were still retained.

Root cause:

- `mark_settled_on_l1` only pruned stale windows when the frontier advanced.
- A cursor update with no frontier increase could leave obsolete windows alive.

Fix:

- Changed `crates/eez-composer/src/posted_windows.rs` so L1 settlement updates
  prune stale straddlers even when the verified frontier does not advance.
- Added a stale-cursor guard in `crates/eez-composer/src/composer.rs` before
  spawning deferred posts.
- If the fresh L1 cursor has already passed a prepared posted window, the
  composer marks that optimistic post as failed instead of spawning stale work.

Test added:

- `settled_on_l1_prunes_straddler_even_when_cursor_equals_attested_frontier`

### 3. Driven prover accepted the wrong `PostBatch` sidecar

The composer drives the out-of-process prover with target ranges. In driven mode,
the prover could replay a range and encounter an older `PostBatch` sidecar before
the directive target block. It then treated that non-target sidecar as the
settlement sidecar for the current directive.

Evidence:

- The prover replay range contained more than one possible `PostBatch` sidecar.
- The prover latched onto a sidecar whose block number did not match the
  directive target.
- Settlement frontier stopped advancing until the prover was fixed and
  redeployed.

Root cause:

- Driven proving did not require the sidecar block number to equal the directive
  target block.

Fix:

- Updated `crates/eez-proverd/src/main.rs`.
- In driven mode, the prover keeps only the sidecar whose block number equals the
  directive target.
- Non-target sidecars are stripped from the control event and logged.

Tests added:

- `driven_settlement_ignores_non_target_postbatch_sidecar`
- `driven_settlement_accepts_target_postbatch_sidecar`

## Verification

Local verification passed:

```text
cargo build --profile release-fast -p eez-node
cargo test -p eez-deriver -p eez-composer -p eez-proverd
```

Live two-composer verification passed:

- Both nodes produced L2 blocks and tips advanced.
- L2 byte identity held across checked windows.
- Verified frontiers recovered and stayed in lockstep.
- Settlement gap stayed bounded and returned to zero or near-zero.
- Both L1 posters landed `postBatch` transactions.
- Same-block competition resolved through contract ordering without permanent
  divergence or freeze.

Representative live samples:

```text
checked_heights=5915..6215 mismatches=0
c1_tip=6277 c2_tip=6277 c1_frontier=6275 c2_frontier=6275 c1_gap=2 c2_gap=2
```

Later cross-chain checks also passed:

```text
Inbound tx:
0xe3d8d5fa10612c4ceb7720c0b619f5d9c3fc9bc66b0ee129e3416ae75c7f9536
result: delivered on both composers, receiver balance = 123456789

Outbound tx:
0x83f63bd7565ea6c87bf818493c29a8bdfd91c8189ccfa65cf3d1b327ece24dec
result: L1 Value changed from 816 to 566304 on both L1 views

post-outbound:
c1_tip=9756 c2_tip=9756 c1_frontier=9755 c2_frontier=9755 c1_gap=1 c2_gap=1
checked=161 mismatches=0
```

## Files Changed

- `crates/eez-deriver/src/deriver.rs`
- `crates/eez-composer/src/posted_windows.rs`
- `crates/eez-composer/src/composer.rs`
- `crates/eez-proverd/src/main.rs`

## Result

The two-composer competition now runs cleanly under sustained load. The L2 chains
remain byte-identical, both composers keep settling, both posters participate,
same-block competition is harmless, and the settlement gap remains bounded.
