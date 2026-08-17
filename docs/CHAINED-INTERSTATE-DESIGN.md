# Chained-interstate slot composition — design

> Fixes issue #88 (composer simulates held txs in isolation → co-bundled
> order-dependent txs carry claims that sequential execution contradicts →
> proof signer rejects every window containing them) and closes the
> compose-time half of the #76 freeze loop for this failure class.
> Branch: `feat/chained-interstate`.

## 1. Problem

At Sync-slot composition each held cross-chain tx is simulated in a fresh
`CompositionBuilder` over the same pre-slot state:

- source sim opens `provider.latest()` and discards its post-state
  (`eez-composer/src/local/client.rs:251-257`);
- every target session opens `provider.latest()` per composition and dies at
  `finalize` (`local/session.rs:107`);
- `reset_composition_state()` wipes overlay state per tx
  (`composer.rs:191-194`).

The simulated `returnData` is folded into the entry's rolling hash
(`eez-protocol/src/rolling_hash.rs:98-105`) and embedded in the delivery
system tx (`system_tx.rs:113-131`). On-chain, `EEZL2._executeEntry` re-runs
the call for real, sequentially, folds the actual outcome
(`EEZL2.sol:552`) and reverts `RollingHashMismatch` (`EEZL2.sol:466`) on any
divergence. The proof signer sees the reverted system tx
(`eez-proof-signer/src/settlement/blocks.rs:207-211`) and rejects the window;
the drain re-queues at the FIFO front (`composer.rs:1974-1996`) and the same
set deterministically re-fails next slot — a livelock.

It is not only return data: the call-hash preimage includes `data` and
`value` (`eez-protocol/src/action.rs:79-93`, `EEZBase.sol:198-215`), so a
state-dependent *argument* diverges the entry identity itself
(`EntryHashMismatch` / `EntryNotFound`). Claims must therefore come from a
simulation whose *inputs* (source-side state) and *outputs* (target-side
execution) both reflect the sequential reality of the bundle — on both
chains.

The per-entry state-root half is already sequential-correct:
`sync_block_pair_roots` (`local/build.rs:170-212`) re-executes real prefixes
of the real block. Only the claim half lies.

## 2. Core idea — the Sync block is the session

The claims are inputs to the very block whose execution checks them, so any
composer computes them before the block exists. This design collapses that
"prediction" into plain sequential execution of the real thing:

**L2 truth = the Sync block under construction.** The drain maintains
`sync_txs: Vec<Bytes>` — the real system/user txs appended so far, in
canonical order. "L2 state at this point" is always
*execute-the-prefix-on-parent* with the exact next-block env
`build_sync_block` uses (`local/build.rs:74-155`). There is no parallel
approximate L2 world: simulations fork the block-in-progress, and accepted
effects are appended to it as the canonical txs themselves, receipt-checked
immediately. Byte-exact by construction; self-verifying (the appended
delivery re-runs the on-chain compare that used to fire at the signer).

**L1 truth = an anchored world advanced only by real frames.** One
`CacheState` over the L1 head pinned at drain start, advanced commit-or-drop
per surviving tx by executing the *actual contract paths*:

- outbound target execution replays exactly `EEZ._processNCalls`
  (`EEZ.sol:1126-1205`): if `authorizedProxies[proxy]` is unset, run the
  permissionless `createCrossChainProxy(src, srcRid)` (`EEZBase.sol:156`,
  CREATE2 from EEZ's frame — the same deployment `_createCrossChainProxyInternal`
  performs at `EEZ.sol:1150-1152`); then a frame
  `caller = EEZ, to = proxy, data = executeOnBehalf(target, gas, data), value = v`
  — the proxy sees `msg.sender == EEZ` and forwards
  (`CrossChainProxy.sol:50-64`). Value is drawn from EEZ's balance — real
  escrow semantics. No proxy-as-msg.sender shortcut, no
  `restore_caller_nonce` on the target path.
- inbound source txs run as full txs over the world (nonce-check relaxed as
  today); their post-state commits so later inbound sims see predecessors.

**Claims are read off real executions.** For an inbound call the target-side
result comes from a *probe*: build the delivery system tx with the correct
`proxyEntryHash` (computable a priori) and placeholder
`rollingHash`/`returnData`, execute it on a fork of the block prefix, and
capture the outcome of the `EEZL2 → proxy` `executeOnBehalf` frame — the
exact value `_processNCalls` folds at `EEZL2.sol:547-552`. Recompute
`rollingHash` from the captured `(success, retData)` with the shared fold
(`rolling_hash.rs`), rebuild the entry, run the real delivery on the fork
(must succeed — same state, same path), return the captured data to the
suspended source sim. The probe reverting at its own rolling-hash compare is
expected and harmless: the inner call has already executed by then and the
fork is derived state.

## 3. Canonical order

`build_cross_chain_sync_pairs` (`eez-protocol/src/system_tx.rs:278-367`)
fixes the block order: all outbound `[load_k, user_k]` pairs, then all
inbound deliveries; SYSTEM_ADDRESS nonces `N..N+K-1` then `N+K..`. On L1 the
same order holds physically: outbound calls execute inside
`postAndVerifyBatch` (leading immediate entries, `EEZ.sol:397-421`), which
precedes the inbound user txs in the bundle (`composer.rs:2006-2008`).

The drain therefore processes the drained set in two passes — outbound
survivors first, then inbound — preserving per-direction FIFO order.
Partitioning cannot reorder a sender's nonce chain (directions live on
different chains) and poison-gap bookkeeping is keyed per
`(sender, direction)`.

## 4. Per-tx flow

For each held tx (canonical order):

1. `reset_composition_state()` on all clients (overlay channels are
   intra-tx; keep the `a5411b4` semantics).
2. Construct per-composition executors, injected via the existing
   `CompositionBuilder::with_sessions` (`eez-protocol/src/composition.rs:211`):
   - `L1ManagerExec` — fork = clone of the L1 world cache over the pinned
     anchor.
   - `L2BlockProbeExec` — fork = open prefix state of `sync_txs`; tracks the
     delivery nonce cursor.
   Both accumulate across `execute()` calls *within* the composition (a
   source tx dispatching several calls sees its own earlier effects) and
   implement real `checkpoint`/`rollback` (cache clone/restore) so the
   builder's revert-span rollback works.
3. Source sim with the session inspector, over the direction's context:
   outbound → user tx on the L2 prefix fork; inbound → user tx on the L1
   world fork. Post-state cache is returned to the drain (not committed
   yet).
4. `finalize()` → `Composition`. Entry builders are unchanged — their
   `RecordedCall` outcomes are now real-path results.
5. Shape gate at accept time: an entry with nested (`expectedL1ToL2Calls`),
   multi-call, static, or revert-span structure is **poison** (evict, keep
   composing) — not a slot-degrade. This removes the freeze vector where
   `reject_multicall` fired post-drain and degraded the whole slot forever.
6. Survivor-accept (the only point where shared state advances):
   - outbound: append canonical `[load_k, user_k]`
     (`build_l2_outbound_entry` + `build_outbound_load_table_txs` at nonce
     `N+k`) to `sync_txs`; execute the extended prefix; **both receipts must
     succeed**, else evict the tx, truncate, continue. Commit the
     `L1ManagerExec` fork into the L1 world.
   - inbound: append the canonical delivery txs
     (`build_inbound_system_txs` at the phase-2 nonce cursor); execute;
     receipts must succeed (they re-run the on-chain claim compare — this is
     the verifier), else evict + truncate. Commit the source-sim post-cache
     into the L1 world.
   - stage `pending_out` / `pending_in` / `outbound_entries` /
     `survivor_comps` exactly as today.
7. Poison/eviction at any step: drop the forks — the world and the block are
   untouched by non-survivors, so rollback is structural, not mechanical.

End of drain (unchanged tail, plus two asserts):

- `build_cross_chain_sync_pairs(pending_out, pending_in, cfg, N)` →
  `interleave_sync_block_txs` **must equal `sync_txs` byte-for-byte** — the
  keystone invariant tying the composer's incremental block to the
  deriver/signer reconstruction. Mismatch = loud slot-degrade (a bug, not an
  input condition).
- `build_sync_block` on the final list; **every system-tx receipt and every
  outbound user-tx receipt must be success** (surfaced receipts) before
  anything is posted. Nothing in this failure class can reach the signer
  again.
- `sync_block_pair_roots`, `prepare_post_batch_raw`, dispatch: unchanged.

## 5. What is exact, what remains approximate

Exact by construction:

- All L2-side claims and state: the block itself is the simulation. Return
  data, call identities, rolling hashes, value minting/burning, proxy
  creation, EEZL2 table bookkeeping, block env (timestamp/basefee) — real
  STF, real txs, receipt-verified at append time.
- L1-side target execution frames: identical code path to
  `postAndVerifyBatch`'s `_processNCalls`, including proxy auto-creation and
  escrow value flow from EEZ's balance.

Approximate, bounded, and contained on-chain:

- **L1 base drift**: the world is pinned to the L1 head at drain start; the
  bundle lands 1+ blocks later. Physically unavoidable (the future L1 state
  is unknowable). Contained by protocol design: `StateUpdate.currentState`
  gates at consumption (`EEZ.sol:1077-1082`), immediates skip-not-abort
  (`L2TxSkipped`, `EEZ.sol:406-421`), deferred consumption is
  partial-prefix, and the optimistic observer reorgs L2 on any
  less-than-claimed settlement. Fail-closed; `MAX_BUNDLE_ATTEMPTS` evicts
  repeat offenders post-dispatch.
- **Inbound source-sim interception**: the L1 user tx's proxy call is
  answered by the recorded claim instead of executing EEZ's consumption
  machinery (that machinery is what the deferred entry *replaces* — this is
  the protocol's own model, not a shortcut). The intercepted frame does not
  move `msg.value` and EEZ-internal consumption state is not written; user
  contracts don't read either. Divergence surfaces on L1 as `EntryNotFound`
  → partial consumption, never as a signer freeze.
- Synthetic-frame bookkeeping on L1: manager frames run at gas price 0 and
  EEZ's account nonce is restored post-frame (contract nonce only matters
  for CREATE, and proxies are CREATE2).

Gas-dependent target behavior is out of scope on both chains
(`USE_GAS_LEFT = false` everywhere; hashes fold `callGas = 0`).

## 6. Future-proofing (multicall / nested / static)

- **Multicall** (N calls per entry): the probe already captures *per-frame*
  outcomes in order; the rolling-hash recompute folds them in sequence.
  Materialization (`reject_multicall`) is the only gate to lift; execution
  contexts need no rework because they run whatever `_processNCalls` does.
- **Nested / reentrant** (`expectedOutgoingCalls` / `expectedL1ToL2Calls`):
  today recorded (source-sim inspector), shape-gated to poison at accept.
  A nested call reached inside a probe executes the real
  `_consumeNestedCall` path; with no table rows it folds `CALL_NOT_FOUND`,
  the appended delivery reverts, and the tx is evicted — loud, never
  corrupting. Enabling nesting later = materialize the tables and let the
  probe inspector dispatch through the builder (the seam —
  `TargetExecutionSession::execute(req, dispatcher)` — already threads the
  dispatcher).
- **Static entries**: same probe pattern with `staticcall` frames; the
  untagged static fold is already in `rolling_hash.rs`.
- **N rollups**: contexts are keyed by `RollupId`; nothing names two chains.

## 7. Implementation map

| Piece | Where |
|---|---|
| `open_prefix_state` (execute prefix txs, return live `State` + env), receipts surfaced from `build_sync_block` | `eez-composer/src/local/build.rs` |
| `L1ManagerExec`, `L2BlockProbeExec`, `ProbeInspector`, L1 world | `eez-composer/src/local/slot.rs` (new) |
| `simulate_source_tx_with` (explicit base header/state, returns post `CacheState`) | `eez-composer/src/local/client.rs` |
| Concrete local handles on the wiring | `eez-composer/src/composer.rs` (`CrossChainWiring`), populated by `eez-node/src/main.rs` |
| Two-phase drain, accept/evict protocol, asserts | `eez-composer/src/composer.rs::compose_cross_chain_batch` |
| Deterministic repro + mixed-direction + poison e2e | `crates/eez-node/tests/` |

Unchanged: entry builders, `build_cross_chain_sync_pairs`,
`prepare_post_batch_raw`, `sync_block_pair_roots`, deriver, signer, optimistic
observer, held-pool semantics, overlay/nested re-entry machinery (still used
inside a single source sim).

## 8. Live-chain findings (chiado, 2026-08-12)

Validated end to end on chiado (fresh deploy, real rbuilder relay): full
matrix (both directions × direct/wrapper × setter/deposit/withdraw +
reverts) semantically exact, then 120/120 paced cross-chain txs settled
with 6-12 co-bundled txs per Sync slot, zero divergence, L1 root
byte-identical to the L2 settled boundary throughout. Three findings:

1. **Manager frames must clamp to the block gas limit** — chiado's 17M
   block limit rejected `DIRECT_CALL_GAS_LIMIT` frames outright
   (`frame_gas` in `local/slot.rs`; dev chains mask this).
2. **The composer does not bound effects-per-window to the prover's
   `max_transaction_state_checkpoints` (default 8)** — a window with more
   effects fails `prepare_post_batch_raw` deterministically and the
   degrade-and-requeue loops on it (the #76 blind-spot shape, one layer
   up). Mitigated by raising the signer quota alongside
   `EEZ_MAX_USER_TXS_PER_BUNDLE` (both parameterized in
   `docker-compose.chiado-node.yml`); the durable fix is compose-time
   enforcement: cap the drain at the prover's quota, or chunk windows.
3. **The postBatch execution reserve must scale with effect count.** Each
   deferred entry costs ~240k gas to queue on L1 (measured), so a
   24-effect batch needs ~6M execution over the calldata floor — the
   fixed 4M reserve made the postBatch revert OUT OF GAS **inside the
   builder's bundle simulation**, and rbuilder drops such bundles
   silently ("pinned slot built without inclusion"). The relay itself has
   NO bundle tx-count limit — it happily includes a 25-tx bundle once the
   request is valid. `EEZ_POSTBATCH_GAS_RESERVE` now overrides the
   default; size it (and the signer quota) to the bundle cap. Durable
   fix: derive the reserve from the batch's entry count.
4. **Inbound user-tx gas must cover queue depth.** `executeCrossChainCall`
   forward-scans the deferred queue, so the k-th co-bundled consumer costs
   materially more than the first (~106k at head, rising with k). A limit
   that fits the head tx makes deep txs revert in the builder's atomic
   simulation → whole bundle dropped. Budget ~300k for deep entries and
   keep Σ(user limits) + postBatch limit under the L1 block gas limit.

With reserve 8M, signer quota 64, and 300k user limits: **24 inbound
cross-chain txs settled in one Sync block** (`count()==24` at the SAFE
head; results 1..24) and **24 outbound in one Sync block** (L1 counter
24 via a single postBatch). When reading L2 effects for verification,
use the `safe` block tag — the unsafe head is optimistic and can roll
back with a failed bundle.

None of the drain's invariant events (`canonical_mismatch`,
`final_receipt_failed`, `l1_session_lost`) fired at any point, including
under the quota livelock, OOG-drop loops, and 3-strike evictions.

## 9. Verification

1. Issue #88 repro: 3× `increment()` against one stateful L2 target, forced
   into one drain — claims must be `1, 2, 3` and the window must be signed.
2. Mixed direction in one slot: outbound write then inbound read of the same
   target (and the reverse pairing on L1).
3. Poison mid-bundle: tx1 evicted, tx0 + tx2 settle with correct chained
   claims.
4. Value chains: deposit then spend within one slot.
5. Full workspace green (`fmt`, `clippy -D warnings`, `cargo test`,
   nextest e2e), `smoke-setter.sh` + `smoke-deposit.sh` on embedded dev L1.
