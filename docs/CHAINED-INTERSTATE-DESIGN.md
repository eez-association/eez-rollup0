# Chained-interstate slot composition — design

This document explains the fix for issue #88. Read it top to bottom; each
section builds on the previous one. The hunk-by-hunk code walkthrough lives
in `docs/CHAINED-INTERSTATE-REVIEW.md`.

Two words used throughout:

- A **claim** is a promise baked into a batch: "this cross-chain call will
  return these bytes". Claims are embedded in the system transactions and
  checked by the contracts and the proof signer.
- The **drain** is the moment, once per Sync slot, when the composer takes
  the held cross-chain transactions out of the pool and composes them.

## 1. The bug

Take one stateful contract on L2:

```solidity
contract Counter {
    uint256 public count;
    function increment() external returns (uint256) { count += 1; return count; }
}
```

Three users each send an `increment()` call from L1. All three end up in
the same drain.

The old composer simulated each held tx **in isolation**, against the same
pre-slot state. Each simulation saw `count = 0`, so each recorded the claim
"returns 1".

Then the real Sync block executed all three in sequence. The first call
really returns 1. The second returns 2 — but its claim says 1. The claim
is checked on-chain: `EEZL2._executeEntry` folds the actual return data
into a rolling hash and compares it to the claimed one (`EEZL2.sol:466`).
Mismatch → the delivery transaction reverts.

The proof signer re-executes the block, sees a reverted system
transaction, and refuses to sign the window
(`eez-proof-signer/src/settlement/blocks.rs:207`). The composer's degrade
path puts the transactions back at the front of the queue. Next slot, the
same thing happens. Forever. That is the freeze in issue #88 (and the
blind spot issue #76 describes).

Two details that shape the fix:

1. **It is not only return data.** The identity hash of a call includes
   its `data` and `value` (`eez-protocol/src/action.rs:79`). If a contract
   computes its outbound call's *arguments* from state, isolated
   simulation gets the arguments wrong too. So no post-hoc patching of
   return data can fix this — the simulation itself must see the right
   state.
2. **Half the problem was already solved.** The per-entry *state roots*
   were already computed by re-executing real block prefixes
   (`sync_block_pair_roots`). Only the claims came from isolated
   simulation. That is why the prover kept signing in most tests: as long
   as transactions did not interact, the wrong method happened to give
   the right answer.

## 2. The fix, in one idea

**Stop simulating next to the block. Build the block, and use the block
itself as the simulation state.**

Concretely, the drain keeps two pieces of state for the whole slot:

**On L2 — `SyncBlockState` (`local/build.rs`).** This is the Sync block
under construction. It holds a live EVM state built exactly the way
`build_sync_block` builds the real block: same parent, same block
attributes, same pre-execution changes. Every accepted transaction is
appended to it and executed for real, and its receipt is checked on the
spot. "What is the L2 state right now?" always means "the state of the
block so far". There is no separate, approximate L2 simulation to drift
away from reality.

**On L1 — `L1SlotState` (`local/slot.rs`).** There is no L1 block to
build (the L1 effects happen inside a future `postAndVerifyBatch`), so the
closest equivalent is used: pin the L1 head at the start of the drain (the
*anchor*), and keep one cache of the effects of every transaction accepted
so far. The cache only ever advances at accept time — a rejected
transaction leaves it untouched ("commit-or-drop").

**Claims are read off real executions, not computed by a lookalike.**

- For an **inbound** call (L1 → L2), the return data comes from a *probe*:
  build the real delivery system transaction with the correct call
  identity but placeholder return data, run it on a throwaway fork of the
  block-so-far, and capture the result of the actual `EEZL2 → proxy`
  call frame — the exact value the contract folds into its rolling hash
  (`EEZL2.sol:547-552`). The probe is expected to revert at its own
  hash check; by then the inner call has already run and been captured.
  Then rebuild the delivery with the captured result and run it for real.
  Two runs per call is inherent, not waste: the hash lives *inside* the
  transaction that produces it, so you cannot learn the result and commit
  it in one pass.
- For an **outbound** call (L2 → L1), the L1 result comes from executing
  the *same frames the contract will execute* inside `postAndVerifyBatch`
  (`EEZ.sol:1126-1205`): if the proxy does not exist yet, run the real,
  permissionless `createCrossChainProxy` (`EEZBase.sol:156`); then run
  `caller = EEZ, to = proxy, data = executeOnBehalf(target, gas, data)`
  with the value drawn from EEZ's real balance (`CrossChainProxy.sol:50`).
  No forged `msg.sender`, no nonce hacks on the claim path.

## 3. Canonical order

The block's transaction order is fixed by the canonical builder
(`build_cross_chain_sync_pairs`, `eez-protocol/src/system_tx.rs`): first
every outbound `[load, user]` pair, then every inbound delivery. L1
executes in the same order: the outbound calls run inside `postBatch`,
which comes before the inbound user transactions in the bundle.

So the drain processes held transactions in two passes — all outbound
first, then all inbound — each pass in FIFO order. Simulation order equals
execution order, which is what makes the claims come out right. The
partition cannot reorder any sender's nonce chain, because a sender's
inbound and outbound transactions live on different chains.

## 4. How one transaction flows through the drain

For each held transaction, in canonical order:

1. Reset the overlay channels (bookkeeping for nested calls inside one
   transaction — no execution state is touched).
2. Build this transaction's executors and seed them into the composition:
   - `L1TargetSession`: a fork of `L1SlotState` (clone of the cache over
     the pinned anchor).
   - `InboundL2TargetSession`: a fork of `SyncBlockState`, plus the
     delivery-nonce cursor.
   Both accumulate across calls *within* the transaction, so a source
   transaction that makes several cross-chain calls sees its own earlier
   effects.
3. Run the source simulation on the right fork: an outbound user
   transaction runs on the L2 block fork; an inbound one runs on the L1
   fork. The inspector intercepts the proxy call and dispatches it to the
   target executor, which produces the real result.
4. Finalize the composition. The entry builders are unchanged — they just
   receive real results now.
5. **Shape gate**: entries the block cannot represent (nested calls,
   multi-call, static, revert spans) evict this one transaction, loudly.
   Under the old code these failed after the drain and degraded the whole
   slot forever — a freeze vector.
6. **Accept** — the only point where shared state moves:
   - Outbound: append the canonical `[load, user]` pair to the block and
     execute it. Both receipts must succeed, or the transaction is
     evicted and the block is rebuilt from the accepted list. Then commit
     the L1 fork's cache into `L1SlotState`, and only then debit the
     escrow budget.
   - Inbound: append the canonical delivery and execute it. Its receipt
     re-runs the same on-chain claim check that used to explode at the
     signer — a mismatch now costs one eviction instead of a frozen slot.
     Then commit the source fork's cache into `L1SlotState`.
7. Any failure before accept: drop the forks. Nothing shared changed, so
   there is nothing to roll back.

After the two passes, three checks close the slot:

- **Keystone**: rebuild the transaction list through the canonical builder
  (the same code the deriver and the signer use) and require it to be
  byte-identical to what the drain appended. A mismatch is a composer bug
  and degrades the slot loudly.
- **Belt**: build the final block once with `build_sync_block` and require
  every receipt to be a success.
- Everything after that — per-effect roots, `prepare_post_batch_raw`, the
  bundle, the optimistic observer — is unchanged.

## 5. What is exact, and what is not

Exact by construction:

- Everything on L2. The block is the simulation, executed by the real
  state-transition function, receipt-checked at every append, with the
  real block environment (timestamp, base fee). Return data, call
  identities, rolling hashes, value minting, proxy creation — all real.
- The L1 frames. They are the same code paths `postAndVerifyBatch` runs,
  including proxy auto-creation and real escrow balances.

Approximate, deliberately, and contained by the protocol:

- **The L1 anchor is a guess about the future.** The bundle lands one or
  more L1 blocks after the anchor. This is physics: the composer cannot
  know the future L1 state. The protocol already contains it: every
  entry's pre-state is re-checked at consumption, immediates skip rather
  than abort, deferred consumption stops at a prefix, and the optimistic
  observer rolls the L2 block back if the batch settles short.
- **Inbound source simulation intercepts the proxy call** and answers it
  with the recorded claim instead of running EEZ's consumption machinery.
  That is the protocol's own model — the deferred entry *is* the recorded
  answer — but it means the intercepted frame moves no value and writes
  no EEZ-internal state. User contracts read neither. Divergence surfaces
  on L1 as `EntryNotFound` and partial consumption, never as a signer
  freeze.
- Synthetic L1 frames run at gas price zero and the manager's account
  nonce is restored afterwards (contract nonces only matter for CREATE,
  and proxies are CREATE2).

## 6. Future work, and the seams left for it

- **Multicall** (several calls in one entry): the probe already captures
  frame results in order, and the hash fold takes any number of them. The
  only gate to lift is the entry-shape check.
- **Nested calls** (a target calling back across chains): recorded today
  and evicted by the shape gate. Two prerequisites are documented in the
  code: unify the two L2 clients' overlay channels
  (`LocalComposeClients` doc), and let the probe dispatch through the
  composition builder (the `execute(req, dispatcher)` seam already passes
  the dispatcher).
- **Static calls**: same probe pattern with `staticcall` frames; the
  separate static hash fold already exists in `rolling_hash.rs`.
- **N rollups**: all slot state is keyed by `RollupId`. The missing piece
  is seeding a session for *every* registered rollup rather than the
  single counterpart; until then `compose_chained` refuses unseeded
  dispatch loudly.

## 7. Findings from live chiado validation

The change was validated on a fresh deployment against real chiado with
the real rbuilder relay. The dev suite was green before this; each finding
below is something only a real chain exposed.

1. **Clamp frame gas to the block limit** (`clamp_frame_gas`,
   `local/slot.rs`). Chiado's block gas limit is about 17M; the manager
   frames asked for 30M, and revm rejects a transaction whose gas limit
   exceeds the block's. Dev chains have bigger blocks and never see this.
   Clamping matches what the real chain enforces.
2. **The postBatch gas reserve must scale with the batch**
   (`EEZ_POSTBATCH_GAS_RESERVE`). Queueing one deferred entry on L1 costs
   about 240k gas (measured: a 3-entry batch used 841k, a minimal one
   126k). A 24-effect batch therefore needs ~6M of execution on top of
   the calldata floor. The old fixed 4M reserve made the postBatch revert
   out of gas *inside the block builder's simulation*, and the builder
   drops such bundles silently. The relay has no bundle-size limit — it
   happily included a 25-transaction bundle once the request was honest.
   The reserve is now an env knob; the durable fix is deriving it from
   the entry count.
3. **The signer's checkpoint quota must match the bundle cap**
   (`EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS`, default 8). A
   window with more effects than the quota fails `prepare_post_batch_raw`
   deterministically, and the degrade path retries it forever — the same
   shape as issue #76, one layer up. Size the quota with the cap (both
   are parameterized together in `docker-compose.chiado-node.yml`); the
   durable fix is enforcing the quota at compose time.
4. **Inbound user transactions need gas for queue depth.** The k-th
   consumer in a bundle scans past k−1 stored entries, so it costs more
   than the first (~106k at the head, more deeper in). A gas limit sized
   for the head transaction makes deep ones revert in the builder's
   atomic simulation, which drops the whole bundle. Budget ~300k, and
   keep the sum of user limits plus the postBatch limit under the block
   gas limit.

End state on chiado: the full matrix (both directions × direct and
wrapper × setter, deposit, withdrawal, plus reverts) semantically exact;
120/120 paced transactions settled; the issue-#88 repro (three
same-sender, nonce-ordered increments) settled in one Sync block with
results 1, 2, 3; **24 inbound cross-chain transactions settled in one
Sync block** (results 1..24, verified at the safe head) and 24 outbound
likewise; zero divergence throughout; none of the keystone/belt events
ever fired.

One operating rule, learned twice: **verify L2 effects at the `safe`
block tag.** The unsafe head is optimistic and rolls back when a bundle
fails.

## 8. Implementation map

| Piece | Where |
|---|---|
| `SyncBlockState` / `SyncBlockFork` (the block as simulation state) + receipts | `eez-composer/src/local/build.rs` |
| `L1SlotState`, `L1TargetSession`, `InboundL2TargetSession`, `ProbeInspector`, `SkipTopFrame` | `eez-composer/src/local/slot.rs` |
| `simulate_source_tx_on` (source sim over a caller-provided fork) | `eez-composer/src/local/client.rs` |
| `compose_chained` + the two-pass drain, accept/evict, keystone, belt | `eez-composer/src/composer.rs` |
| `build_outbound_pair`, `check_entry_shape` (single canonical source) | `eez-protocol/src/system_tx.rs` |
| Concrete client handles (`LocalComposeClients`) | wired in `eez-node/src/main.rs` |
| Deterministic e2e (repro, mixed direction, poison, same-sender chain) | `eez-node/tests/chained_interstate.rs` |

Unchanged: entry builders, `prepare_post_batch_raw`,
`sync_block_pair_roots`, the deriver, the proof signer, the optimistic
observer, held-pool semantics, and the overlay machinery for nested calls
within one transaction.

## 9. How it was verified

1. The issue-#88 repro as a deterministic e2e: three `increment()` calls
   forced into one drain must produce claims 1, 2, 3 and a signed window.
2. Mixed direction in one slot, with the block order asserted from
   receipts.
3. Poison mid-bundle: survivors claim 1 and 2 (not 1 and 3), and the next
   slot still settles — no freeze.
4. Same-sender outbound chain (nonces n, n+1) in one slot.
5. Full workspace green (`fmt`, `clippy -D warnings`, `cargo test`,
   nextest e2e), plus the live chiado runs in §7.
