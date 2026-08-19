# Chained-interstate change review

This document walks through every change in the chained-interstate
change-set, file by file, hunk by hunk. It explains what each change does,
why it exists, and — where behavior changed — what happened before and
what happens now.

Read `docs/CHAINED-INTERSTATE-DESIGN.md` first. It explains the problem
and the design in plain language, and defines the two words this document
uses constantly: a **claim** (the promised result baked into a batch) and
the **drain** (the once-per-slot moment the composer composes the held
cross-chain transactions).

## The change in one paragraph

Issue #88: the composer simulated each held cross-chain transaction in
isolation, against the same pre-slot state. Three co-bundled `increment()`
calls each claimed "returns 1". The real block returned 1, 2, 3. The
second delivery failed its on-chain check, the proof signer refused to
sign the window, and the transactions re-queued forever. The fix: build
the Sync block during composition and use it as the simulation state
(`SyncBlockState`), keep one accumulated L1 state per slot
(`L1SlotState`), read every claim off an execution of the real contract
code, and append each accepted transaction to the real block with an
immediate receipt check. A wrong claim now costs one transaction an
eviction instead of freezing the composer.

## Reading map

| Part | Files | What it covers |
|---|---|---|
| 1 | `local/build.rs`; `eez-protocol` (`system_tx.rs`, `abi.rs`); `Cargo.toml` | The block-as-simulation machinery and its equivalence tests; the canonical-builder helpers; new ABI entries |
| 2 | `local/slot.rs` (new), `local/client.rs`, `local/mod.rs`, `lib.rs` | The two real-path execution sessions, the probe, the source-sim seams |
| 3 | `composer.rs` | The drain rework: two passes in canonical order, the accept/evict protocol, the keystone and belt checks |
| 4 | tests, harness, `main.rs`, CI, the compose file, `Counter.sol` | How it is verified and operated |
| A | — | Why three hunks exist only because of live chiado testing |
| B | — | The headline behavior changes (each part ends with its full table) |

The parts are in dependency order. The drain (Part 3) uses everything
built in Parts 1 and 2.

## What deliberately did NOT change

- Entry building, `prepare_post_batch_raw`, `sync_block_pair_roots`, the
  deriver, the proof signer, the optimistic observer, the held pool, and
  the overlay machinery for nested calls inside one transaction.
- The old direct-call session (`local/session.rs`) still exists. It is
  simply no longer on the claim path.
- All pre-existing tests pass unmodified.

---

# Part 1 — Execution foundations and protocol helpers


Recall the three `increment()` calls. They must claim 1, 2, 3 — not 1, 1, 1.
So "the L2 state right now" during composition must be the exact state the real
block will hold at that position. The design doc calls this "the Sync block is
the session". Part 1 is where it becomes true.

Four files do the groundwork:

- `build.rs` grows the live block prefix, and hands out forks of it.
- `system_tx.rs` makes the composer's incremental block and the canonical
  rebuild share one lowering.
- `abi.rs` adds the manager and proxy signatures the new L1 executor calls.
- `Cargo.toml` pays for a mock provider, so the equivalence is unit-testable.

Verified while reviewing: `cargo test -p eez-composer --lib local::build` —
3 passed.

## `crates/eez-composer/src/local/build.rs`

619 lines added, 35 removed. Roughly a third is the new live-state type, a
third is helper extraction, and a third is tests.

### Hunk 1 — module doc

Three new lines. They say the file still builds a Sync block, and now also
exposes that same construction as a *live* state that grows one tx at a time.
"A claim read off it is the value the committed block produces." Accurate
framing; the rest of the file delivers it.

### Hunk 2 — imports and the `DraftDb` alias

The import growth is mechanical: `Evm as _`, `EvmEnvFor`, `InspectorFor`,
`Recovered`, `StateProviderBox`, `CacheState`, `NoOpInspector`,
`DatabaseCommit`, `ExecutionResult`, `Arc`.

One new line is worth reading:

```rust
pub type DraftDb = State<StateProviderDatabase<StateProviderBox>>;
```

Note the `StateProviderBox`. `build_sync_block` builds its `State` over a
*borrowed* `&dyn StateProvider`, because it must keep the provider alive
separately to hand to `finish`. The live path owns a boxed provider instead, so
the state can outlive the call that opened it. Same database, different
ownership. That is the reason for the alias, and the reason `open_draft_db`
exists alongside the inline builder inside `build_sync_block`.

### Hunk 3a — `BuiltSyncBlock.tx_successes`

Before: the final build returned payload, header, and block, and a reverted
tx inside it was invisible to the caller. After: a `Vec<bool>` of receipt
statuses in block order.

This is the belt half of belt-and-braces. Every tx was already receipt-checked
when the drain appended it to the live prefix. `tx_successes` lets the composer
re-check the same thing on the final, independently built block
(`composer.rs:2452`). If the two ever disagree, the composer bails to a minimal
postBatch. The whole design claims "appended state == final-block state", and
this field is the assertion that says so out loud instead of trusting it.

### Hunk 3b — `next_block_attributes` extracted

The `NextBlockEnvAttributes` literal moves out of `build_sync_block` into a
free function, comment and all. It carries timestamp, fee recipient,
`prev_randao = 0`, `BUILDER_GAS_LIMIT`, cancun-gated
`parent_beacon_block_root`, shanghai-gated `withdrawals`, and `extra_data`.
No field changed.

Pure extraction, and the single most important extraction in the diff.
`SyncBlockState::open` and `build_sync_block` now *cannot* disagree about the
block env. A disagreement would be silent and nasty. Say `SyncBlockState`
computed a claim under one timestamp while the committed block executed under
another: a contract reading `block.timestamp` returns a different value, and
the entry hash commits to the wrong return data. One shared constructor makes
that class of bug unrepresentable rather than merely unlikely.

### Hunk 3c — `recover_tx` extracted

Decode-2718 plus signer recovery. It was inline in `build_sync_block`'s tx loop;
it is now a helper taking the block position for error labelling. Mechanical.

### Hunk 3d — `open_draft_db`

New helper. It calls `state_by_block_hash(parent)`, then `State::builder()`
with bundle updates, plus an optional `with_cached_prestate(cache)`. That
`Option<CacheState>` is the fork seam: `None` for a fresh prefix,
`Some(clone)` for a fork. `with_cached_prestate` is the same mechanism the
overlay/nested path already relies on in production, so it is not a new trust
assumption.

### Hunk 4 — `build_sync_block` uses the helpers

Three small edits. `attributes` now comes from `next_block_attributes(...)`.
The tx loop becomes `builder.execute_transaction(recover_tx(tx_bytes, idx)?)`.
After `finish`, receipts map into `tx_successes`. No behavior change beyond
the new field being populated.

### Hunk 5 — `TxOutcome` and `exec_outcome`

```rust
pub struct TxOutcome { pub success: bool, pub gas_used: u64, pub output: Bytes }
```

One distinction here drives the drain's whole evict-vs-abort policy, so state
it precisely. **A revert is a successful execution**:
`Ok(TxOutcome { success: false, output: <revert data> })`. Only a tx revm
refuses outright comes back as `Err(BuildError)` — bad nonce, insufficient
funds, undecodable bytes.

The drain then splits on the error class. A revert becomes
`BuildError::ExecuteTx`, and the drain evicts that one tx and keeps composing
(`composer.rs:576`). A database read failure becomes `BuildError::Provider`,
and the drain aborts the slot rather than blame a valid tx
(`composer.rs:1917`). Collapsing the two would either drop the revert data or
turn a single bad tx into a whole-slot failure.

`gas_used` is `result.tx_gas_used()` — gas after refunds, exactly what the
receipt reports and what the header's `gas_used` accumulates. That identity is
what makes the tests below work. `output` is empty when a tx halts without
output, as documented.

### Hunk 6 — `execute_and_commit_inspected`

The per-tx engine for the live paths. It calls `recover_tx`, builds an EVM
over the state with the block's env, calls `transact(&recovered)`, snapshots
the outcome, then calls `db_mut().commit(result.state)`.

The load-bearing question is whether this does the same thing to *state* as
`BlockBuilder::execute_transaction`. I checked the pinned `alloy-evm` 0.34.0
(`src/eth/block.rs`):

- `execute_transaction_without_commit` does a block-gas-availability precheck,
  then `self.evm.transact(tx_env)`. Same call.
- `commit_transaction` calls `system_caller.on_state(...)`, updates gas
  counters, pushes a receipt, then calls `self.evm.db_mut().commit(state)`.
  Same commit. `on_state` is a notification hook and a no-op unless a state
  hook is installed, and the block builder installs none here.

So the claim holds. Everything the builder does beyond this is receipts and
block-gas bookkeeping, and neither touches state. The doc comment says
exactly that, including that cumulative block gas is deliberately *not*
tracked here and that the final `build_sync_block` is what enforces it.

Two honest footnotes:

1. The comment says the uninspected path passes `NoOpInspector`, "the same
   inspector reth uses when a caller supplies none". True at the API level, not
   literally the same construction. reth's `evm_with_env` leaves
   `inspect: false`. `evm_with_env_and_inspector` calls `activate_inspector`,
   which sets `inspect: true` and takes revm's inspector-enabled loop. With
   `NoOpInspector` the hooks do nothing, so outcomes are identical. The tests
   below cross precisely this boundary and compare gas, so it is pinned rather
   than assumed. Just do not read the comment as "identical construction".
2. Skipping the block-gas precheck is deliberate and documented. It does move
   *where* an over-budget window fails: not per tx at append time, but at the
   final `build_sync_block`. The composer treats that as systemic, so it
   re-queues survivors and degrades to minimal. A drain that overshoots
   `BUILDER_GAS_LIMIT` (30M) therefore re-drains the same set next slot and
   fails the same way — the shape of design §7 finding 2, one layer down. It is
   remote at today's caps of roughly 24 txs at 300k each. A cumulative-gas
   counter on `SyncBlockState` that evicts the offending tx would close it.
   Worth a follow-up line, not a blocker.

### Hunk 7 — `SyncBlockState`

The centerpiece. It holds the provider, the evm config, the parent hash, the
block's `evm_env`, the live `State`, and `applied`. `applied` counts the txs
applied so far, which is also the next tx's block position; it labels errors
and seeds forks. `Debug` is hand-written because `State` is not `Debug`.

`open()` is where the design's §2 guarantee is actually established, and the
sequencing matters:

```rust
let mut builder = evm_config.builder_for_next_block(&mut state, parent, attributes)?;
builder.apply_pre_execution_changes()?;
for (idx, raw) in prefix_txs.iter().enumerate() {
    builder.execute_transaction(recover_tx(raw, idx)?)?;
}
// scope ends — builder dropped, `finish` NEVER called
```

Three things to notice.

*The prefix runs through the real `BlockBuilder`*, not the cheaper `transact`
path. So the pre-execution changes (EIP-2935 block-hash write, beacon root)
are applied exactly once, by the same code that applies them in the committed
block.

*The builder is dropped without `finish()`.* This is the subtle, correct call.
`finish` applies post-execution changes (withdrawals, balance increments) and
computes the state root. Mid-block state must not include those. Tx k+1 in a
real block runs after tx k, **not** after block close. Calling `finish` here
would produce a state that no position in the real block ever has. The
builder holds `&mut state`, so the commits it made stay behind when it drops.
That is what leaves `state` sitting at the mid-block point.

*The stored `evm_env` is the builder's env.* I checked reth's
`builder_for_next_block` (fd59fd2, `crates/evm/evm/src/lib.rs:410`). It does
`let evm_env = self.next_evm_env(parent, &attributes)?` and builds its EVM
from that. `open` computes `next_evm_env(parent, &attributes)` from the same
attributes. So the env `execute_tx` uses later is byte-for-byte the env the
prefix ran under. Good.

`execute_tx()` appends one tx through `execute_and_commit_inspected` with
`NoOpInspector`, then bumps `applied`. `fork()` opens a fresh `State` over the
same parent provider, preloaded with a *clone* of the live cache, carrying the
same env and the same `applied` cursor.

Two small notes. `fork(&mut self)` only needs `&self`, since it clones
`self.state.cache`. Harmless, but a `&self` signature would document "forking
cannot disturb the block" in the type. And `open` always re-executes the whole
prefix from the parent, so an evict-and-reopen cycle is O(n²) across a drain.
That is a conscious trade, and the code says why (`composer.rs:1655`).
Rebuilding from the accepted list, rather than restoring a cache, is what keeps
the prefix *provably* equal to the block the canonical rebuild produces. At a
24-tx cap that is the right side of the trade.

### Hunk 8 — `SyncBlockFork`

A throwaway copy. Same fields minus the provider, so it cannot re-open, by
construction. `execute_tx` mirrors `SyncBlockState`'s.
`execute_tx_inspected(raw, inspector)` is the probe path, where the inspector
captures the inner `EEZL2 → proxy` frame outcome that becomes the claim.
`snapshot()` and `restore()` are the restore point the composition builder's
revert-span rollback needs. `ForkSnapshot` carries the cache plus `applied`, and
the comment correctly justifies why the cache is enough: forks never merge
transitions, so there is no `transition_state` to unwind. `state_and_env()`
hands out the raw state and env for callers that drive their own EVM, which is
what the source sim does (`composer.rs:1804`).

The invariant this type carries is one sentence: nothing executed on a fork
touches the block. Probes and source sims live here. Only accepted effects get
appended to the real `SyncBlockState`.

### Hunk 9 — `sync_block_pair_roots` untouched

Worth calling out that leaving it alone is *correct*. It needs state **roots**,
and roots only exist after block close. `SyncBlockState` deliberately never
closes a block, so its build-per-prefix loop cannot be swapped for the cheaper
primitive. The two answer different questions: "what is the state mid-block"
versus "what is the root at this pair-end".

### Hunk 10 — tests

Three tests plus a fixture. They matter more than usual, because the whole
design rests on "the live path executes like the builder does", and that
equivalence is otherwise invisible.

The mechanism is worth understanding. `MockEthProvider` is the new dev-dep, a
flat in-memory account store. I confirmed its `state_by_block_hash` ignores the
hash and returns the same provider, so its state *roots* are meaningless. The
tests use **per-tx gas as the witness** instead:

```rust
fn builder_gas(&self) -> Vec<u64> {
    let cumulative = (0..=self.txs.len()).map(|k| self.build(&self.txs[..k]).header.gas_used());
    cumulative.windows(2).map(|w| w[1] - w[0]).collect()
}
```

It builds the block on every prefix and differences the header's `gas_used`.
That yields exactly what the *builder* charged each tx. Gas is a
state-dependent signal. The fixture's `STORE` contract does `SSTORE 1 → slot
0`, so a cold first write costs about 20k more than the warm repeat. Equal
per-tx gas across the two paths therefore implies equal intermediate state, not
just equal final answers. The fixture's five txs are transfer, store, reverter,
store again, transfer — plain transfer, state write, revert-with-data, warm
rewrite, and a post-revert tx.

- `prefix_state_execution_matches_build_sync_block` appends all five to a live
  `SyncBlockState` and compares status and gas against the built block, tx by
  tx. It also asserts the revert carries `0xdeadbeef` and the successful store
  returns 42. Anti-vacuity: `outcomes[1].gas_used > outcomes[3].gas_used +
  15_000`. The second store is cheap *only* if the first one's write is
  visible, so the test fails if `open` ever restarts from the parent. A
  `builder_gas[0] == 21_000` ground-truth assert stops the comparisons passing
  on empty numbers.
- `prefix_open_matches_the_same_position_in_the_block` opens the prefix
  `[0..k]` for every k and runs tx k through `execute_tx`. The prefix is
  builder-executed and tx k is `transact`-executed, and tx k must reproduce its
  in-block outcome. This is the test that pins the *seam* between the two
  execution mechanisms, including the `inspect: true` versus `inspect: false`
  difference noted above.
- `fork_is_isolated_and_snapshot_restore_rewinds` checks two things. A tx run
  on a fork produces the same outcome when it is then run on the block, so the
  fork wrote nothing back. And `snapshot` / `restore` is a genuine restore
  point: run, restore, re-run gives an identical outcome. Its anti-vacuity is
  the sharpest of the three. A third replay *without* a rewind must `Err`,
  because the nonce is spent. That is precisely the property the probe's
  snapshot/restore in `slot.rs` depends on. The replay is legal only because
  the restore really rewound.

Honest limitation: the mock returns the same state for any block hash, so these
tests verify execution equivalence, not parent-hash routing. That is the right
scope. Hash routing is covered by the e2e tests that run real nodes.

## `crates/eez-protocol/src/system_tx.rs`

Two factorings. Neither changes what gets built on the happy path. Both remove
a place where two copies of one rule could drift apart.

### Hunk 1 — module doc

`simulate_and_resolve` becomes "chained simulation". One word. The old name no
longer describes the path.

### Hunk 2 — `build_inbound_system_txs` calls the shared predicate

Before: an inline `if !entry.success || entry.l2ToL1Calls.len() != 1 || …`
block, a *third* hand-rolled copy of the same protocol rule. The closure in
`build_cross_chain_sync_pairs` was the second. After:
`check_entry_shape(entry, "inbound")?`.

Three copies of one predicate is a real drift hazard. Change the rule in one
copy, and the composer accepts a shape the deriver rejects, or the reverse.
That divergence only shows up as a signer rejection. Now there is one copy.

The two `continue` guards immediately above it stay, and correctly so. A
foreign `destinationRollupId` and an empty `l2ToL1Calls` are *filters*, not
errors: "not ours" and "nothing to deliver". Only what survives the filters
gets shape-checked.

### Hunk 3a — `check_entry_shape` (the ex-closure, now `pub`)

Same four rejections in the same order: multi-call, nested, unsuccessful, and
static/revert-span/explicit-gas. Same messages. The doc comment carries over
the original reasoning that extra calls "would be SILENTLY TRUNCATED to
call[0]". The added paragraph is the important one. The composer calls this per
entry at accept time, and `build_cross_chain_sync_pairs` calls it over the
whole set. Both gate on the exact same predicate.

### Hunk 3b — `build_outbound_pair` (new)

The ex-PHASE-1 loop body, lifted verbatim into a per-entry function. It
shape-checks, takes `l2ToL1Calls.first()`, calls `build_l2_outbound_entry`,
calls `build_outbound_load_table_txs(slice::from_ref(&entry), cfg,
starting_nonce)`, then wraps each load into a `SyncPair` with the user tx.
`build_outbound_load_table_txs` emits one `loadExecutionTable` per entry today,
so this returns a single pair per call. The `Vec` shape is preserved so the
nonce arithmetic stays honest if that ever changes.

Why it exists: the drain appends pairs incrementally as each survivor is
accepted (`composer.rs:1892`), and the post-drain canonical rebuild calls the
same function over the same entries in the same order. So the composer's
incrementally built block and the deriver/signer's canonical rebuild are
byte-identical **by construction**. That is what gives the drain's keystone
assert its teeth. If the drain had kept its own copy of this lowering, the
assert would compare two copies of the same bug and pass.

### Hunk 4 — the pre-gate in `build_cross_chain_sync_pairs`

Before: two pre-gate loops, all outbound entries checked then all inbound,
before anything was emitted. After: only the inbound loop remains, and outbound
is gated inside PHASE 1 by `build_outbound_pair`, which checks its entry before
building anything.

The inbound half must stay, and the comment gives the real reason.
`build_inbound_system_txs` `continue`s past foreign-destination entries
*before* checking them. Without this loop, an ill-shaped entry addressed to
another rollup would never be shape-checked at all. The pre-gate is
deliberately stricter than the emit path.

One accepted behavior change. With **both** a bad outbound and a bad inbound
entry, the reported error flips from the outbound one to the inbound one, since
the inbound loop now runs first. Error text only. The call fails either way,
and nothing escapes, because `pairs` is a local vec discarded on `Err`.

Small note on the comment "a bad shape must fail the call, not half-build it".
For outbound that was never a risk, since the function returns all-or-nothing.
The sentence is true of the inbound gate's *purpose*, which is check before
emit. But the operative justification is the foreign-entry one on the next
line.

### Hunk 5 — PHASE 1 body replaced by the call

```rust
let built = build_outbound_pair(entry, user_tx, cfg, nonce)?;
nonce = nonce.checked_add(built.len() as u64)…;
pairs.extend(built);
```

`built.len() == loads.len()`, one pair per load. So the nonce advance is
identical to before, overflow check included. Pure move.

## `crates/eez-protocol/src/abi.rs`

### Hunk 1 — four new `sol!` declarations

`authorizedProxies(address)`, `createCrossChainProxy(address,uint64)`,
`computeCrossChainProxyAddress(address,uint64)`, and
`executeOnBehalf(address,uint64,bytes)`.

Pure addition. The new L1 executor replays the real `_processNCalls` path
rather than shortcutting it (`local/slot.rs`). It looks up the proxy, deploys
it permissionlessly if absent, then calls through `executeOnBehalf`. That needs
these four signatures. `abi.rs` is the workspace's single ABI source, so they
belong here rather than in file-local `sol!` macros next to each call site.

I checked all four against the pinned submodule, `eez-core-protocol` at
`6fcc90b6`, which matches the module's "ABI pins from commit 6fcc90b".
`executeOnBehalf` matches including `payable` (`CrossChainProxy.sol:50`). The
two proxy functions match (`EEZBase.sol:156` and `EEZBase.sol:176`). The
`authorizedProxies` getter's flattened return `(bool, address, uint64)` matches
`struct ProxyInfo` (`IEEZ.sol:157`). The doc comment noting that Solidity
flattens the struct into its three members is a helpful line to keep.

### Hunk 2 — `manager_and_proxy_selectors_match_upstream`

Four selector asserts, each with an explicit drift message. I recomputed them
with `cast sig` and all four match the pinned bytes: `0x8205f3e1`,
`0xa7587c62`, `0xeb20c0aa`, `0x360d95b6`.

This is the workspace's established guard, and the right one to reach for here.
Bytecode-coupled constants have bitten this project before, in the
`authorizedProxies` slot-constant episode. A silently drifted selector on the
manager path would surface as an unexplained `EmptyCalls`-shaped failure rather
than a compile error.

## `crates/eez-composer/Cargo.toml` + `Cargo.lock`

One new dev-dependency: `reth-provider` with the `test-utils` feature, for
`MockEthProvider` in the prefix-state tests, plus the one-line lock entry.

The cost is real and was flagged consciously. `reth-provider` is already a
workspace dependency, so under resolver 2 the extra feature unifies across the
graph for any invocation that builds tests. `cargo test` and
`clippy --all-targets` therefore pull a second feature set over the reth crates
and pay the compile time. Production builds are untouched, since dev-dependency
features never apply to `cargo build -p eez-node`. The alternative was no unit
coverage at all for the one equivalence the whole design rests on, since every
other test here spawns real nodes. Right call; revisit only if CI wall time
becomes the constraint.

## Behavior-change inventory

| Change | Before | After |
|---|---|---|
| `BuiltSyncBlock.tx_successes` | the built block carried no receipt statuses; a reverted tx in it was invisible | per-tx receipt status surfaced; the composer gates dispatch on all-success and degrades to a minimal postBatch otherwise |
| `next_block_attributes` / `recover_tx` / `open_draft_db` extraction | attributes plus decode/recover inline in `build_sync_block` | shared helpers; `build_sync_block` and `SyncBlockState::open` cannot disagree on the block env |
| `SyncBlockState` / `SyncBlockFork` / `TxOutcome` | none; no live mid-block state existed, and composition simulated over `provider.latest()` | new public API: live prefix state over the block under construction, forks for probes and source sims, per-tx outcome including revert data |
| `build_sync_block` core path | builder → pre-exec → per-tx → `finish` | unchanged: same order, same env, same `finish`; only the receipt mapping is new |
| `sync_block_pair_roots` | rebuilds the block per pair-end for roots | untouched; roots need block close, which `SyncBlockState` deliberately never does |
| `build_inbound_system_txs` shape rejection | third inline copy of the predicate | calls the shared `check_entry_shape`; same rule, same messages; the foreign/empty `continue` filters are unchanged |
| `check_entry_shape` | private closure inside `build_cross_chain_sync_pairs` | public fn, same four rejections in the same order (pure lift) |
| `build_outbound_pair` | inline PHASE-1 loop body | public per-entry fn; the drain and the canonical rebuild share one lowering, which is what makes the "appended == rebuilt" assert meaningful |
| Pre-gate ordering in `build_cross_chain_sync_pairs` | outbound entries gated first, then inbound, before any emission | inbound-only pre-gate, because foreign entries would otherwise never be checked; outbound gated per entry inside PHASE 1. With one bad entry of each kind the reported error flips outbound → inbound. Error text only |
| Nonce arithmetic in PHASE 1 | `nonce += loads.len()`, checked | `nonce += built.len()`, checked; same count, one load per entry |
| Four `sol!` declarations plus selector test in `abi.rs` | none (pure addition) | manager/proxy ABI lives in the single ABI source; all four selectors pinned and verified against `eez-core-protocol@6fcc90b` |
| `reth-provider` `test-utils` dev-dependency | none (pure addition) | test and clippy builds compile a second reth feature set; production builds unaffected |

### Follow-ups worth a line somewhere (neither blocking)

1. **Block-gas overflow fails the whole window.** The live path deliberately
   skips the builder's block-gas precheck. So a drain that overshoots
   `BUILDER_GAS_LIMIT` fails at the final `build_sync_block`, degrades, and
   re-queues the same set, which will fail identically next slot. A
   cumulative-gas counter on `SyncBlockState` that evicts the offending tx
   would turn it into a per-tx eviction. That is the shape of design §7
   finding 2, one layer down.
2. **`SyncBlockState::fork` could take `&self`.** It only clones the cache, and
   the immutable signature would state "forking cannot disturb the block" in
   the type rather than in a comment.

---

# Part 2 — The slot execution contexts

Reading order for this part:

1. `crates/eez-composer/src/local/slot.rs` — new file, 773 lines. Reviewed whole, block by block.
2. `crates/eez-composer/src/local/client.rs` — diff vs the pre-change file, hunk by hunk.
3. `crates/eez-composer/src/local/mod.rs` and `crates/eez-composer/src/lib.rs` — visibility and export diff.

One sentence frames everything below: **no approximations on the claim path.**

The old target session computed a claim by calling the target contract directly. It forged a
computed proxy address into `msg.sender`, disabled every EVM check, and then undid the nonce
damage with a hack. That file is still in the tree, and the drain no longer uses its session type
(`local/session.rs`). The two executors in `slot.rs` run the code the chain will actually run:
`EEZ._processNCalls`' frames on L1, and the canonical delivery system tx on L2. So the numbers
that land in an entry's rolling hash are read off real executions, not modelled.

---

## 1. `crates/eez-composer/src/local/slot.rs`

### 1.1 Module doc (`slot.rs:1-13`)

Thirteen lines that pay for themselves. Both types implement `TargetExecutionSession`, and the
doc says neither approximates the protocol. Each one names the contract lines it mirrors
(`EEZ.sol:1149-1178`, `EEZL2.sol:547-552`). Those citations are load-bearing for anyone
verifying the port, so I checked them. They land on the
`sourceProxy.call{value:…}(abi.encodeCall(CrossChainProxy.executeOnBehalf, …))` sites, plus the
`_rollingHashCallEnd(success, retData)` fold on both chains.

### 1.2 Imports (`slot.rs:15-46`)

Mechanical. Two things worth noting:

- The ABI surface comes from `eez_protocol::abi`, the single ABI source with selector-pin tests,
  not from ad-hoc `sol!` blocks here (`slot.rs:32-35`).
- `DIRECT_CALL_GAS_LIMIT`, `evm_err`, and `provider_err` are reused from `session.rs`
  (`slot.rs:46`). The old file stays the home of those shared bits, even though its session type
  is dormant on this path.

### 1.3 `LocalComposeClients` (`slot.rs:48-68`)

Two `Arc<LocalChainClient>` handles, L1 entry and L2 entry. They ride on `CrossChainWiring`
next to the existing type-erased `ChainClient` map (`composer.rs:125-144`), and are populated
when the node wires the composer (`eez-node/src/main.rs:697`).

**Why it exists.** The drain needs surfaces the `ChainClient` trait deliberately lacks:
`L1SlotState::open`, `simulate_source_tx_on`, and `chain_provider()`. The doc comment says so
outright. Both handles point at the same instances the erased map holds. This is not a second
registry. It is a concrete-typed view onto the same two clients.

**Why not the obvious alternative.** Widening `ChainClient` with an `open_world` method would
force every impl to answer a question only the local reth-backed client can answer. A future
non-local client would have to stub or lie. That is the "stub that lies" anti-pattern, avoided
by keeping the concrete handle beside the erased one.

The doc also records a known seam. L2's two instances have separate overlay channels, so a nested
call back into L2 re-enters through the follower client and opens unseeded (`slot.rs:56-61`).

### 1.4 Constants and the error-kind helpers (`slot.rs:70-101`)

`VIEW_CALL_GAS_LIMIT = 1_000_000` caps the two view frames. `ZERO_CALL_GAS = 0` is
`executeOnBehalf`'s `callGas` argument, and the reason sits inline: zero means "forward all
remaining gas", and it is the only shape the protocol emits (`CrossChainProxy.sol:60`).

The three error constructors look like formatting helpers. They are **policy** helpers. The
drain sorts failures by `ExecutorErrorKind` (`composer.rs:408-427`). `Unavailable`, `Provider`,
and `Missing` are TRANSIENT: re-queue the tx and abort the slot. Everything else is POISON:
evict this tx and keep composing. So the kind chosen at a failure site *is* the eviction
decision.

- `L1SlotState::open` uses `provider_err` and `Missing`, so a provider hiccup at anchor time
  re-queues instead of evicting user transactions (`slot.rs:127-134`). Correct.
- `transact_err` and `fork_err` split one revm failure two ways: a database read failure is the
  store being unreachable and stays transient, everything else is a property of the tx and turns
  poison (`slot.rs:82-101`). Correct, and the reason is in the doc comment.
- Every failure caused by the transaction itself goes through `encoding_err`, so it turns poison
  (`slot.rs:78-80`). That covers a reverted manager frame, a reverted target, a probe that never
  reached the proxy frame, and a reverted delivery.

The policy is right, the name slightly misleading. `Encoding` reads like "ABI problem", but it
now also means "this tx is structurally undeliverable". Not worth churn today, though a new
`ExecutorErrorKind::Rejected` would document itself.

### 1.5 `L1SlotState` (`slot.rs:105-192`)

```rust
pub struct L1SlotState {
    pub anchor: SealedHeader<Header>,
    pub cache: CacheState,
}
```

One per drain, created at the top of the drain (`composer.rs:1668`). The anchor is the L1 head at
drain start, and it never moves.

**Why pin it.** The bundle lands at least one L1 block later no matter what, so a fresher base
buys nothing real. A moving base would be worse than useless. A transaction's claims would then
depend on when it happened to arrive relative to L1 block production, and the same held set would
compose differently on two runs. Pinning makes the drain a pure function of the held set and the
anchor. Design §5 owns the residual approximation and lists the on-chain containment.

`cache` is the commit-or-drop ledger. It holds only the effects of transactions that survived,
because the drain writes it at accept points and nowhere else. That is why eviction needs no
unwind machinery. A poisoned tx's fork is simply dropped.

- `open` (`slot.rs:127-145`) — best block number, then header, then `seal_slow`, then an empty
  cache and one debug line naming the pinned block and hash. Errors are transient. Fine.
- `open_state` (`slot.rs:149-172`) — the single door every fork goes through. It opens state at
  the anchor **hash** rather than at "latest", so it cannot drift mid-drain. It uses the anchor's
  EVM env and preloads accumulated effects with `with_cached_prestate(seed)`.
  `with_bundle_update()` is on, but nothing reads the bundle. See the checkpoint note in §1.9.
- `fork_state` (`slot.rs:183-191`) — the inbound source-sim fork: anchor state, plus the world
  cache, plus the **plain** anchor env. The comment explains the split precisely.
  `simulate_source_tx_on` applies its own source-sim tweak, which turns the nonce check off. The
  manager-frame tweaks deliberately stay out of a path that executes a real signed user
  transaction. Collapsing those two envs "for tidiness" would quietly weaken the inbound sim.

**Observation, not a bug.** `open_state` builds the env from the anchor block itself, not from
`next_evm_env` for anchor+1 (`slot.rs:162-165`). So a target reading `block.number` or
`block.timestamp` inside a manager frame sees the anchor's values, while real execution will see
anchor+1 or later. `frame_gas` likewise clamps to the anchor's gas limit rather than the landing
block's. This is the same bounded L1 base drift design §5 already accepts, and `next_evm_env`
would be a *different* guess rather than a truer one. Worth one comment line at `open_state` so
the next reader need not derive it.

### 1.6 `L1TargetSession` — struct, `Debug`, `new` (`slot.rs:196-255`)

This is the outbound target session. For an L2→L1 call it replays exactly what
`EEZ._processNCalls` will do inside the future `postAndVerifyBatch`.

`new` forks the world by seeding `world.cache.clone()`, then makes exactly four env edits
(`slot.rs:240-243`):

```rust
evm_env.cfg_env.disable_base_fee = true;
evm_env.cfg_env.disable_eip3607 = true;
evm_env.cfg_env.disable_nonce_check = true;
evm_env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);
```

Take an L2→L1 withdrawal of 1 ETH. Before: `msg.sender` was a forged proxy address with balance
checks off, so the value was conjured and the sim always succeeded. After: the value is drawn
from the manager's actual balance at the anchor, exactly as `sourceProxy.call{value: …}` will
draw it on-chain. A short escrow now fails here at compose time, costing one eviction, instead
of failing at the builder's bundle simulation or after settlement.

The mechanism behind that is what the four edits leave out. The old path called
`session::disable_checks`, which also set `disable_balance_check` and `disable_block_gas_limit`
(`session.rs:368-375`). Both are gone on purpose. The comment says why for the first: the frames
are synthetic, with no fee market and no EOA sender, so base-fee, EIP-3607, and nonce checks are
noise. The balance check stays on so escrow is real.

`Debug` is hand-written and prints `manager` and `chain_id` only, because the revm `State` is not
`Debug` and would be unreadable anyway (`slot.rs:215-222`).

### 1.7 `frame_gas` (`slot.rs:257-262`)

```rust
fn frame_gas(&self, requested: u64) -> u64 {
    requested.min(self.evm_env.block_env.gas_limit)
}
```

Three lines with a live-chain story behind them, recorded as design §7 finding 1. This clamp was
added last, after chiado testing. chiado's L1 block gas limit is about 17M and
`DIRECT_CALL_GAS_LIMIT` is 30M. revm refuses any transaction whose gas limit exceeds the block's,
so **every** manager frame failed with "caller gas limit exceeds the block gas limit". That
failure is poison, so every outbound composition evicted. Dev chains mask it completely, because
their block limit is at or above 30M. That is why only a real chain surfaced it.

**Why not the obvious alternative.** Setting `disable_block_gas_limit = true` is what the old
session did. Clamping is truer to the chain. On-chain a call genuinely cannot be given more gas
than the block allows, so a 25M-gas target that "passes" under a disabled limit would be a claim
L1 can never honor. Clamping fails the same transactions L1 would fail. The one-line doc comment
on the function says this in the same number of words.

### 1.8 `manager_frame`, `view_call`, `proxy_address`, `is_authorized_proxy`, `create_proxy` (`slot.rs:264-348`)

`manager_frame` is the shared primitive (`slot.rs:266-300`). It runs a synthetic tx from
`Address::ZERO` to the manager, with supplied calldata and clamped gas, on the fork. A revert
becomes a poison error carrying the raw output. On the `commit` path the caller's nonce bump is
undone before `self.state.commit(changes)`. See §1.11.

The three callers mirror the contract one-for-one, and their doc comments cite the lines:

| Rust | Solidity |
|---|---|
| `proxy_address` | `computeCrossChainProxyAddress(l2ToL1Call.sourceAddress, l2ToL1Call.sourceRollupId)` |
| `is_authorized_proxy` | `if (!authorizedProxies[sourceProxy].isProxy)` |
| `create_proxy` | `_createCrossChainProxyInternal(...)` via the permissionless `createCrossChainProxy` |

Two details separate "mirrors the contract" from "looks like it mirrors the contract".

- The proxy address is obtained by **asking the manager**, not by recomputing CREATE2 in Rust
  (`slot.rs:308-322`). Ported slot constants that disagreed with our contracts are the scar
  tissue here. A view frame cannot drift from the deployed bytecode.
- `create_proxy` runs the **real** `createCrossChainProxy` (`slot.rs:334-348`). The CREATE2
  deployment happens from the manager's own frame, with the manager's salt and the manager's
  `CrossChainProxy` creation code (`EEZBase.sol:160-171`). The `authorizedProxies[proxy]`
  registration lands in the fork as a side effect. Deploying a proxy by hand into the cache
  would be exactly the bytecode-coupled guess that broke before.

Note that `create_proxy` uses the clamped `DIRECT_CALL_GAS_LIMIT` rather than
`VIEW_CALL_GAS_LIMIT`. A CREATE2 deployment is not a view. Correct.

### 1.9 `impl TargetExecutionSession for L1TargetSession` (`slot.rs:351-454`)

**`execute` (`slot.rs:352-439`)** is the heart of the outbound path, in the contract's own order:

1. Refuse `CallMode::Static` with `Encoding`, which classifies POISON. A call mode is fixed by
   the tx, so retrying re-fails forever. Static entries are parked, per design §6.
2. Compute the proxy. If it is not authorized, create it.
3. Build the frame: `caller = manager`, `to = proxy`, `data = executeOnBehalf(target, 0, data)`,
   `value = req.value`.

That shape is the whole point. `executeOnBehalf` is transparent-proxy-style. It forwards only
when `msg.sender == EEZ`, and otherwise falls through to the cross-chain path
(`CrossChainProxy.sol:50-64`). So entering through the manager is the *only* way to get a
faithful target execution. The old direct call with a forged proxy `msg.sender` bypassed the
proxy contract entirely. A target that inspects `msg.sender`, or the proxy's own accounting, saw
a different world than the chain will show it.

4. Run the frame under `SkipTopFrame::new(self.client.inspector_factory().build(dispatcher))`.
   See §1.10.
5. Surface any error the session inspector raised, before looking at state (`slot.rs:411-413`).
   A nested dispatch's failure must not be swallowed by a "successful" outer frame.
6. On success, reset the manager's nonce and commit (`slot.rs:418-421`).
7. Return the outcome **raw**:

```rust
Ok(ExecutionOutcome::Resolved { return_data: return_data.to_vec(), gas_used, success })
```

The comment says the frame's raw output IS what `_processNCalls` folds into CALL_END, revert data
included on failure (`EEZ.sol:1181`). Verified against the contract: it does
`(success, retData) = sourceProxy.call{value: …}(…)` and then `_rollingHashCallEnd(success,
retData)`. `executeOnBehalf` re-reverts with the target's own revert bytes, so a failing target
yields byte-identical revert data here and on-chain. No post-processing and no normalization.
Anything else would be a divergence source.

The commit is guarded by `if success`, and that guard is deliberate (`slot.rs:415-421`). A
reverted frame's only state today is the caller's nonce bump. Committing it anyway would tie the
slot-shared world to whatever a future revm decides to return in `result.state` for a revert.
Meanwhile the proxy created in step 2 stays committed, because `create_proxy` commits its own
frame. That matches the chain: `_processNCalls` creates the proxy first, then catches the call
failure as `(false, retData)`, so the deployment survives the failed call.

**`checkpoint` and `rollback` (`slot.rs:441-453`)** implement the payload contract documented on
the impl (`slot.rs:196-205`):

```rust
fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot> {
    Ok(Box::new(self.state.cache.clone()))
}
```

`SessionSnapshot` is `Box<dyn Any + Send>`, so the type is checked at runtime only. The drain
reclaims the boxed session, calls `checkpoint()`, downcasts to `CacheState`, and commits it into
`L1SlotState::cache` on accept. `take_l1_cache` repeats the constraint in its own doc, calling
the payload a boxed `CacheState` and nothing else (`composer.rs:596-614`). Documenting it on
both ends is right, because `Any` gives no compiler help.

The double duty is neat rather than clever. The same method serves the builder's intra-tx
revert-span rollback and the drain's end-of-tx harvest, because "the accumulated effects" is the
same object in both cases.

Cache-only restore is sound because simulation reads go exclusively through the cache, and the
bundle and transition state are never consulted. The comment says that. Before: the old session's
`checkpoint` returned `Box::new(())` and its `rollback` was a no-op type check, with a comment
admitting that annulled-call safety rested entirely on batch materialization rejecting revert
spans (`session.rs:349-365`). So a reverted span inside a composition left its writes in the
session. After: a revert span actually rewinds.

### 1.10 `SkipTopFrame` (`slot.rs:456-492`)

Found in author review of the first implementation. It is the subtlest thing in the file.

The session inspector fires on **every** call frame. Its job is to intercept a frame whose callee
is an authorized proxy and re-dispatch it through the composition builder. But the manager
frame's own callee *is* an authorized proxy. Without the wrapper, the inspector would intercept
the very frame that IS the dispatch and re-dispatch it as a nested call. In practice every
outbound transaction would have poison-evicted.

The wrapper is 37 lines and hides exactly one frame:

```rust
fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
    let top = self.depth == 0;
    self.depth += 1;
    if top { return None; }
    self.inner.call(context, inputs)
}
```

Nested proxy calls made *inside* the target still forward to the inner inspector, and are
recorded as `expectedL1ToL2Calls`. The accept-time shape gate turns those into a precise per-tx
eviction, because nested composition is parked per design §6. So the wrapper narrows the
blindness to one frame rather than turning inspection off.

The documented consequence is exact: a revert of the top frame itself is not span-annotated.
`SessionInspector::call` pushes a dispatch-count marker, and `call_end` pops it to emit
`annotate_revert_span` (`eez-evm-inspector/src/inspector.rs:242-247, 362-374`). Skipping both
halves for the top frame means no span annotation for it. That is acceptable precisely because
any composition with nested dispatches is shape-gated to eviction anyway.

Three things I checked rather than assumed.

- **Stack balance under interception.** If the inner inspector returns `Some(outcome)` from
  `call`, does revm still deliver `call_end`? Yes. `revm-inspector 19.0.0` calls `frame_end`
  immediately after a short-circuiting `frame_start` (`traits.rs:104-107`). So both
  `SkipTopFrame::depth` and `SessionInspector::frame_starts` stay balanced. If that changed
  upstream, both counters would skew. The depth field is worth a regression test if the revm pin
  moves.
- **Skipping both halves, not one.** Forwarding `call_end` while skipping `call` would underflow
  the inner inspector's `frame_starts` and mis-attribute the next frame's revert span. The
  `saturating_sub` plus the `> 0` check pair them correctly (`slot.rs:486-491`).
- **Hook coverage.** `SkipTopFrame` overrides only `call` and `call_end`. Every other `Inspector`
  hook falls back to the no-op trait default, so it never reaches `inner`. `SessionInspector`
  today implements exactly `call` and `call_end`, so nothing is dropped. Worth a one-line comment
  on the wrapper: adding `create_end` or `log` to `SessionInspector` would be silently ignored
  here.

### 1.11 `reset_frame_caller_nonce` (`local/mod.rs:40-56`)

revm bumps a transaction sender's nonce. Here the senders are the manager contract and the
synthetic `Address::ZERO` proxy-creation caller. Neither is an EOA. A contract account's nonce
only governs CREATE, and every proxy is CREATE2. So the bump would drift the fork from the real
chain for no reason, and the function resets `info.nonce` to `original_info.nonce`.

Before: the old `restore_caller_nonce` did the same mechanical thing for a much worse reason. The
old session forged `msg.sender` as the computed proxy address, and bumping that address's nonce
made a later real CREATE2 at the same address fail EIP-684's "code or nonce non-zero" check. That
burned about 28M gas and reverted the whole session with empty data. The twelve-line comment
documenting that failure mode marks how much the shortcut cost. After: the shortcut is gone, so
the remaining nonce restore is plain hygiene on synthetic senders, and the new comment is short.
It lives in `local/mod.rs` rather than in either executor, because `slot.rs` and `session.rs` both
call it. One definition, one rationale.

### 1.12 `ProbeSnapshot` (`slot.rs:496-502`)

The fork's restore point plus `delivery_nonce`. Both halves must rewind together. If a revert
span rolls back a delivery, the SYSTEM_ADDRESS nonce cursor must go back with it. Otherwise every
later delivery in the composition is built at the wrong nonce and fails at execution. Small
struct, real invariant. The fork half is a `ForkSnapshot`, which carries the cache and the
applied-tx counter together, so restoring a fork also restores its tx position
(`build.rs:409-416`).

### 1.13 `InboundL2TargetSession` — struct, `new`, `l1_entry_for_call`, `delivery_tx` (`slot.rs:504-597`)

The inbound target session. Its state is a `SyncBlockFork`, a throwaway copy of the Sync block
under construction (`build.rs:417-500`), plus the `SystemTxContext` and the delivery nonce cursor.

`l1_entry_for_call` builds the **L1-shape** `ExecutionEntrySol` for one inbound call, which is
the same shape a postBatch carries (`slot.rs:546-582`). It does that because
`build_inbound_system_txs` is the canonical batch-to-delivery lowering the deriver and the signer
both use. Reusing it makes the probe's delivery transaction byte-identical to what a follower
will reconstruct from L1, by construction rather than by review. That is the same single-source
STF discipline the composer and deriver split already relies on.

`delivery_tx` lowers exactly one entry at the current cursor, and asserts the one-in, one-out
shape (`slot.rs:585-596`):

```rust
let [tx] = <[Bytes; 1]>::try_from(txs).map_err(|txs| encoding_err(format!(
    "one inbound entry must lower to exactly one delivery tx; got {}", txs.len())))?;
```

That is not paranoia. `build_inbound_system_txs` silently skips entries whose
`destinationRollupId` does not match `cfg.this_rollup_id` (`eez-protocol/src/system_tx.rs:89-91`).
A rollup-id mix-up would otherwise yield an empty vec and a confusing downstream failure. Loud,
per invariant 7.

**Small observation.** `l1_entry_for_call` computes the lean L2 entry to obtain `proxyEntryHash`
and `rollingHash`. But `build_inbound_system_txs` rebuilds that same lean entry internally from
the L1-shape fields, so those two fields on the returned struct are inert for the lowering
(`eez-protocol/src/system_tx.rs:103-113`). Same builder and same inputs, so this is
correctness-neutral. A reader can still hunt a while for where the hashes are consumed, and one
clause on the doc comment would save that trip.

### 1.14 `impl TargetExecutionSession for InboundL2TargetSession` (`slot.rs:599-704`)

**Why two runs.** This is inherent, not an optimization choice, and the code says so. The rolling
hash must be computed *inside* the transaction that produces it. `EEZL2._executeEntry` seeds it
from `proxyEntryHash`, folds `(success, retData)` at `EEZL2.sol:551`, and compares against the
claimed `entry.rollingHash` at `EEZL2.sol:466`. You cannot learn the true return data and land
the final transaction in one pass, because the final transaction's own input depends on its own
output.

**Run 1, the probe (`slot.rs:612-654`).** The entry is built with the *correct* `proxyEntryHash`
and a placeholder `returnData`. The call hash is computable a priori: it folds identity, meaning
`isStatic`, source, source rollup, target, target rollup, value, `callGas = 0`, and data, but not
return data. That matters. With the right entry hash the delivery passes the
`EntryHashMismatch` check and reaches the real `EEZL2 → proxy → target` call
(`EEZL2.sol:308-311`). The transaction is then **expected** to revert at `RollingHashMismatch`.
By that point the frame has already run, and `ProbeInspector` has captured its
`(success, retData)`. That pair is precisely what `_processNCalls` folds
(`EEZL2.sol:547-552`).

The snapshot and restore around the probe are load-bearing, not hygiene:

```rust
let snapshot = self.fork.snapshot();
… execute probe …
self.fork.restore(snapshot);
```

A reverted transaction still burns the SYSTEM_ADDRESS nonce and gas. Leaving the probe's effects
in place would make the real run at the same nonce fail, so the mechanism would eat itself. The
inline comment frames it as "the probe leaves no trace: its state effects are re-applied by the
real run below". True, but it understates the necessity. The sharper reason is that the probe
must not consume the nonce the real run needs.

The three-way match on `inspector.captures` is good failure design (`slot.rs:629-646`). Zero
captures means the delivery never reached the proxy frame, from an entry-hash or table mismatch,
and the message carries the probe's own success and output for diagnosis. More than one means a
nested or multi-call shape, which is parked, and the message says so. Both are poison-kind, so
each costs one eviction rather than a slot degrade.

A reverting target is also poison (`slot.rs:648-654`). That is a policy statement about today's
entry builders, not a protocol limit. `build_l2_incoming_entry` rejects `success == false`
(`eez-protocol/src/entries/mod.rs:279-281`), while the contract does have a reverting-entry path
(`EEZL2.sol:471-478`).

**Run 2, the real run (`slot.rs:656-669`).** Rebuild the entry with the captured output, so the
canonical builder recomputes the rolling hash with the shared fold, lower it, and execute on the
fork. It **must** succeed:

```rust
if !real.success { return Err(encoding_err(format!(
    "canonical delivery for {} reverted on the block prefix at SYSTEM nonce {}: {}", …)));
}
```

This is the line that closes issue #88. It is the same on-chain claim verifier that used to fire
at the proof signer, where it froze the whole window and the drain re-queued the same set to fail
identically next slot. It now fires at compose time, where the cost is one eviction. The
verification moved. It did not get weaker.

**Before and after, on the #88 repro.** Three `increment()` calls hit one stateful L2 target in
one drain. Before: each was simulated against the same pre-slot state, so all three claimed a
return of 1. On-chain they execute in sequence and really return 1, 2, 3, so the second delivery
reverts `RollingHashMismatch`, the signer rejects the window, the drain re-queues, and the same
failure repeats forever. After: each probe runs on a fork of the block prefix that already
contains the previous accepted delivery, so the claims are 1, 2, 3 and the window signs.

Nonce advance is `checked_add` with a loud error (`slot.rs:671-673`). `checkpoint` and `rollback`
carry both halves of the state, as described in §1.12.

### 1.15 `ProbeCapture` and `ProbeInspector` (`slot.rs:706-773`)

`ProbeCapture` is `(success, output)`, with no gas field. An earlier draft had `gas_used` and
audit removed it, rightly: `USE_GAS_LEFT` is off and hashes fold `callGas = 0`, so gas is not
consensus-relevant on this path, and an unused field invites someone to start trusting it.

The match predicate is the contract's own call shape (`slot.rs:753-759`):

```rust
let matched = !inputs.is_static
    && matches!(inputs.scheme, CallScheme::Call)
    && inputs.caller == self.eezl2_address
    && inputs.input.as_bytes(context).starts_with(&executeOnBehalfCall::SELECTOR);
```

That is exactly the non-static `sourceProxy.call{value:…}(abi.encodeCall(executeOnBehalf, …))`
at `EEZL2.sol:547-550`. The `caller == EEZL2` clause keeps a target that happens to call some
other proxy from being mistaken for the delivery frame.

Capture happens in `call_end`, not `call`, because the outcome is only known at frame end
(`slot.rs:764-772`). A `frames: Vec<bool>` acts as a depth-and-match stack, so nested frames pop
their own marker. `call` always returns `None`, since this inspector observes and never
intercepts, so the pairing is unconditional.

Post-audit it is a plain `Vec` read through a `&mut` borrow. revm auto-implements `Inspector` for
`&mut I`, so `execute_tx_inspected(&probe_tx, &mut inspector)` type-checks and the caller keeps
ownership. That replaced an `Arc<Mutex<…>>`: same guarantees, no lock, no poison branch, and the
captures are readable straight after the run. The inspector and its reader are always one thread.

`ProbeInspector::new` takes the EEZL2 address rather than reading it from the context. The
hand-written `Debug` prints the address and the open-frame depth. Mechanical.

### 1.16 Absent relative to the earlier draft

Noted so a reviewer who saw the first version does not go looking. `final_cache`, `into_fork`, the
`delivery_nonce()` accessor, two vacuous tests, and `ProbeCapture.gas_used` were all removed in
audit. They were dead API surface no caller reached.
There is no `#[cfg(test)]` block in the file either. Coverage lives in
`crates/eez-node/tests/chained_interstate.rs` plus the chiado runs of design §7, which is the
right level for something whose whole contract is "the real chain agrees".

---

## 2. `crates/eez-composer/src/local/client.rs`

The theme: one source-simulation body, one entry point, and everything `slot.rs` needs exposed as
small accessors.

### Hunk 1 — imports (`client.rs:15-18`)

`State` and `StateProviderBox` type the caller-provided state in the new signature.
`revm::DatabaseCommit` backs the new commit. Mechanical.

### Hunk 2 — three accessors (`client.rs:168-191`)

`chain_provider()`, `manager_address()`, and `inspector_factory()` are pure getters over data the
client already held. They exist because `slot.rs` needs them. `L1SlotState::open` reads headers
and state, `L1TargetSession::new` reads the manager address and the EVM config, and `execute`
builds the session inspector.

`inspector_factory()` also removes a duplicate. `begin_execution_session` and the source-sim path
each hand-built the same three-argument `SessionInspectorFactory::new`. Both now call the
accessor, so the proxy-lookup config, rollup id, and overlay channel triple has one definition.

Audit deleted a fourth accessor, `rollup_id()`, which had no callers. Correct instinct. An
accessor with no consumer is API surface that has to be maintained and will eventually be used
for the wrong thing.

### Hunk 3 — `simulate_source_tx_on` (`client.rs:193-214`)

The new public entry point. The caller supplies the state and env, and **the result commits into
that state**.

That commit is what makes the source side chained. Take two inbound L1 transactions from the same
sender, at nonces 0 and 1, in one drain. Before, with open-latest-and-discard: tx 1's simulation
ran against a state where tx 0 never happened, so it saw a stale balance, stale target storage,
and a stale nonce. After, with the fork carried across: tx 1 sees tx 0's writes, so the call
arguments and return data it claims match the order the bundle will really execute in. The same
mechanism carries phase-1 outbound manager effects into phase-2 inbound sims, because both draw
from `L1SlotState::cache`.

Nonce *validation* is a separate matter. `source_sim` sets `disable_nonce_check = true`, which is
inherited behavior needed because a system-signed source tx can legitimately sit at N+1 behind its
`loadExecutionTable` (`client.rs:249-253`). So the chaining benefit here is the visible **state**,
including the sender's nonce as a target or a CREATE would observe it, and not nonce validation.

The doc comment names the constraint the caller owns: the env must already be derived from the
fork's own header. `L1SlotState::fork_state` and `SyncBlockFork::state_and_env` are the two places
that hold up that end.

### Hunk 4 — `source_sim`, the shared body (`client.rs:216-290`)

Everything from the entry-role gate through decode, tx env, inspected run, inspector-error check,
and commit lives here once. Three things inside it are worth a line each.

- The entry-role gate stays in the shared body, so a follower client cannot be smuggled onto the
  source-sim path (`client.rs:230-234`).
- The result is destructured to `(gas_used, success, changes)` with `changes: Option<_>`, and the
  two failure classes split (`client.rs:265-275`). A database error returns a transient provider
  error, because an unreachable store is the slot's problem and must not degrade into an empty
  composition the drain would read as poison. Any other transact error logs `source sim reverted`
  and yields `None`, so nothing is committed on that branch.
- The commit is unconditional, and a comment explains why: the writes are what the fork caller
  needs, and committing into a function-local `State` would be unobservable anyway
  (`client.rs:282-286`). This answers the obvious alternative. An earlier draft threaded a
  `CommitPostState` enum through the body, and audit removed it because the flag could only ever
  have one observable value. One less parameter and one less thing to get wrong at a call site.
  The comment carries the reasoning the enum used to carry.

The timing instrumentation is gone, and the reason matters more than the deletion. After the
split, `state_us` and `env_us` measured work the function no longer does, because on the
`simulate_source_tx_on` path the state and env arrive already open. The fields would have reported
near-zero and lied about where time goes. Dropping a metric that has become structurally wrong
beats keeping a familiar-looking number.

### Hunk 5 — `begin_execution_session` (`client.rs:298-320`)

Body unchanged except for `Some(self.inspector_factory())` in place of the inline three-line
construction. Mechanical. It also moved below the inherent-impl block, so the diff looks larger
than the change.

### Hunk 6 — `ChainClient` loses `simulate_source_tx` (`eez-protocol/src/executor.rs:95-109`)

Before: entry-role clients overrode a `simulate_source_tx` trait method, which opened
`provider.latest()`, ran the sim, and threw the post-state away. After: the trait method is gone,
and source simulation lives only on the concrete entry client as `simulate_source_tx_on`.

The trait's own doc gives the reason: source simulation runs over the caller's slot-scoped state,
so it cannot be a uniform trait method. `ChainClient` now declares only
`reset_composition_state` and `begin_execution_session`. The drain is the single caller of the
remaining source-sim path (`composer.rs:222`), so nothing can reach the old open-latest behavior
by accident.

---

## 3. `crates/eez-composer/src/local/mod.rs` and `crates/eez-composer/src/lib.rs`

Small, and worth being deliberate about. In `mod.rs`:

- The module doc gains one line for `slot`, describing it as slot-scoped execution contexts
  driving the real contract paths. It sits alongside the existing `LocalChainClient` and
  `LocalExecutionSession` entries.
- `pub(crate) mod slot;` matches the visibility of `build`, `client`, `provider`, and `session`.
  Only `gnosis_adapter` stays `pub`. Modules stay crate-private and only chosen items are
  re-exported.
- `pub use slot::LocalComposeClients;` with `#[doc(inline)]` is the single type that must cross
  the crate boundary, because the node constructs it when wiring `CrossChainWiring`
  (`eez-node/src/main.rs:697`).
- `pub(crate) use slot::{InboundL2TargetSession, L1SlotState, L1TargetSession};` carries the
  comment "Slot execution contexts are driven only by `composer.rs`."

That last line is the interesting choice. The three executors are `pub` **within their module**,
because they need doc comments, `#[must_use]`, and public methods for `composer.rs`. The
re-export is `pub(crate)`, so the crate's external surface gains exactly one name. Exporting all
four would publish types whose contracts are only meaningful inside the drain: the
checkpoint-payload hand-off, the anchor pinning, and the accept-time commit protocol. Keeping
them crate-internal keeps those invariants enforceable by reading one file.

In `lib.rs`, `LocalComposeClients` joins the existing `#[doc(inline)] pub use local::{…}` list
(`lib.rs:53-57`). One name added, nothing else touched.

---

## 4. Behavior-change inventory

| Change | Before | After |
|---|---|---|
| L1 target execution path | direct call to the target with a computed proxy address forged as `msg.sender` (`session.rs` `build_tx_env`) | real frames: manager → `proxy.executeOnBehalf(target, 0, data)` → target, mirroring `EEZ._processNCalls` (`slot.rs:366-387`) |
| Proxy existence | assumed; address computed, never deployed | `authorizedProxies` checked via the manager's own getter; missing → real permissionless `createCrossChainProxy` CREATE2 from the manager's frame (`slot.rs:307-348`) |
| Escrow / value | `disable_balance_check = true` — value conjured, sim always paid | balance check ON; value drawn from the manager's real balance, short escrow fails at compose time (`slot.rs:238-243`) |
| Frame gas | `disable_block_gas_limit = true`, 30M frames | clamped to the anchor block's gas limit (`frame_gas`, `slot.rs:257-262`); fixes the chiado 17M-limit poison-evict of every outbound tx |
| L1 state lifetime | fresh `provider.latest()` per composition, post-state dropped at `finalize` | one `L1SlotState` pinned at drain start, advanced commit-or-drop per surviving tx (`slot.rs:105-192`) |
| Target-session checkpoint | `Box::new(())`, rollback a no-op type check | real `CacheState` clone/restore; doubles as the drain's accept-time harvest payload (`slot.rs:441-453`) |
| L1 frame commit on revert | n/a | committed only on success, so a revert cannot leak revm's `result.state` into the slot-shared world; the step-2 proxy creation still survives (`slot.rs:415-421`) |
| Inbound claim resolution | direct call to the target produced the claim; the delivery tx was verified only on-chain / at the proof signer | probe the canonical delivery on a fork of the Sync block, capture the real `EEZL2 → proxy` outcome, then run the canonical delivery for real — must succeed (`slot.rs:612-669`) |
| Inbound nonce cursor | n/a | `delivery_nonce` advances per accepted delivery and rewinds with rollback via `ProbeSnapshot` (`slot.rs:496-502`, `689-703`) |
| Reverting inbound target | claim recorded, failure surfaced later on-chain | poison at compose time, one eviction, drain continues (`slot.rs:648-654`) |
| Static call mode | refused as `Unavailable`, i.e. transient, so the tx re-queued forever | refused as `Encoding`, i.e. poison, so it evicts once (`slot.rs:359-364`) |
| Top-frame inspection | n/a (no manager frame existed) | `SkipTopFrame` hides the outermost frame so the session inspector cannot re-dispatch the dispatch; nested proxy calls still recorded → shape-gated eviction (`slot.rs:456-492`) |
| Nonce restore rationale | undo a bump on the forged proxy `msg.sender` to keep the CREATE2 slot fresh (EIP-684) | undo a bump on synthetic/contract senders only; the forged-sender hazard no longer exists (`local/mod.rs:40-56`) |
| `simulate_source_tx` (trait) | on `ChainClient`; opened latest, discarded post-state | removed from the trait; source sim lives on the concrete entry client (`eez-protocol/src/executor.rs:95-109`) |
| Source sim over a caller fork | did not exist | `simulate_source_tx_on` runs over caller-provided state + env and commits into it, so tx N+1 sees tx N (`client.rs:193-214`) |
| Source-sim timing telemetry | five `timing.*_us` fields per call | removed — the field names described work the split function no longer does |
| Client accessors | none | `chain_provider()`, `manager_address()`, `inspector_factory()`; `rollup_id()` proposed and audit-deleted (no callers) |
| Crate surface | `local::{BuildError, BuiltSyncBlock, GnosisL1Adapter, LocalChainClient, build_sync_block}` | plus `LocalComposeClients`; the three executors stay `pub(crate)` |

---

# Part 3 — The drain rework

The old drain simulated every held cross-chain tx on its own, against the same
pre-slot state. So three `increment()` calls drained into one slot each recorded
the claim "returns 1". The chain executes them as 1, 2, 3. Delivery #2 reverts
`RollingHashMismatch` on-chain, the proof signer refuses the window, and the
drain puts the same three transactions back at the front of the pool. The next
slot fails identically. That is the freeze in issue #88.

This file holds half the fix. The drain now keeps two slot-scoped worlds: the
local `l1_state` is the L1 world pinned at the anchor, and the local `draft` is
the Sync block under construction. The drain composes each tx on a fork of both,
and only on accept does it append the canonical txs to `draft` and commit the L1
effects into `l1_state`. So each composition sees its predecessors exactly as
sequential execution will. Architecture: `docs/CHAINED-INTERSTATE-DESIGN.md`.
Walkthrough in file order.

---

## Hunk 1 (~L43) — imports

Pulls in the slot machinery: `L1SlotState`, `L1TargetSession`,
`InboundL2TargetSession`, `build::SyncBlockState`. Mechanical.

## Hunk 2 (~L125) — `CrossChainWiring`: erased clients out, concrete handles in

The two `Arc<dyn ChainClient>` fields `entry_client` and `l2_entry_client` go
away, and one field replaces them.

```rust
pub local: crate::local::LocalComposeClients,   // { l1_entry, l2_entry: Arc<LocalChainClient> }
```

**Why:** the drain needs two surfaces the erased `ChainClient` trait does not
expose, `L1SlotState::open` and `simulate_source_tx_on`. These are the same
instances registered in `rollups`. They share one overlay channel, so this is a
second *view*, not a second client. The field is required, not an `Option`:
exactly one construction site exists, and it always has both
(`eez-node/src/main.rs:697`).

## Hunk 3 (~L137) — `simulate_and_resolve` / `simulate_and_resolve_recorded_for` deleted, `compose_chained` added

Both `CrossChainWiring` methods are gone. A free function replaces them
(`crates/eez-composer/src/composer.rs:175`):

```rust
compose_chained(cc, entry_rollup_id, entry_client, raw_tx, sessions, source_state, source_env)
```

The pipeline is the same: reset the overlays, build the `Rollup` map, source-sim
through the `CompositionBuilder`, then `finalize`. Three things differ.

1. **The caller pre-seeds the sessions**, via the existing
   `CompositionBuilder::with_sessions`. So tx N's target-side execution runs on a
   context that already holds tx N-1's accepted effects. Before: every dispatch
   lazily opened a session at `provider.latest()`. After: the caller owns the
   session and hands it in.
2. **Source simulation runs on a caller-provided fork state and env**, through
   `simulate_source_tx_on`, so the *inputs* to the claim are sequential too. A
   state-dependent call *argument* is as fatal as a state-dependent return value,
   because it changes the entry hash. Hence both halves had to move.
3. **Sessions are taken back and returned to the caller**, via
   `let sessions = builder.take_sessions();` before `finalize` consumes the
   builder. This is the commit-or-drop seam. The accepted effects live in the
   sessions, not in the `Composition`. The caller commits them on accept and
   simply *drops* them on eviction. No rollback machinery is needed, because a
   non-survivor never touched shared state.

There is a new loud check between `take_sessions` and `finalize`. A session that
comes back for a rollup nobody seeded is an error (`composer.rs:228`). Entry-chain
sessions are exempt, because overlay re-entry legitimately opens one there. Any
other unseeded session means the dispatch opened a lazy one and ran **unchained**,
off this slot's worlds — the bug being fixed, silently. It is unreachable today,
and can only fire once a third rollup is wired without a slot session. The error
is `ExecutorErrorKind::Unavailable`, which `sim_error_is_poison` classifies
**transient** (`composer.rs:408`). A wiring gap is not the transaction's fault, so
the slot aborts and retries rather than evicting a user's tx.

Two helpers land alongside: `type SlotSessions` and `seed_session`
(`composer.rs:147`). Every drain composition seeds exactly one target chain. The
`#[tracing::instrument]` span carries over onto `compose_chained`, with
`skip_all` and fields `tx_len` + `entry_rollup_id`. The log line loses its
`simulate_and_resolve:` prefix. *Nit, since resolved:* the two stale doc
references to `simulate_and_resolve` in `held_pool.rs` and `optimistic.rs` are
gone from the current tree.

## Hunk 4 (~L267) — `MAX_BUNDLE_ATTEMPTS` doc rewrite, and the gas reserve becomes configurable

Two unrelated things in one hunk.

**(a) `MAX_BUNDLE_ATTEMPTS` doc.** The old text documented drain-time isolation
as a KNOWN LIMITATION: a state-dependent tx whose prerequisite is co-bundled in
the same slot diverges from real execution and drops after the retry budget.
This branch removes that limitation. The new text says what the bound actually
backstops now: L1 state that moves between compose time and the bundle's
inclusion block. Doc-only. The value is still 3.

**(b) `POST_BATCH_EXECUTION_GAS_RESERVE` → `DEFAULT_…` plus an accessor**
(`composer.rs:295`):

```rust
fn post_batch_execution_gas_reserve() -> u64 {   // OnceLock, reads EEZ_POSTBATCH_GAS_RESERVE
```

**Why:** queueing ONE deferred entry inside `postAndVerifyBatch` costs ~240k gas
on chiado. A 3-entry batch bills 841k against ~126k for a minimal one. So a
24-effect postBatch needs ~6M of execution *above* the calldata floor. The fixed
4M reserve made the postBatch revert out-of-gas inside the block builder's bundle
simulation, and rbuilder drops such bundles silently. The only symptom is "pinned
slot built without inclusion".

**Before:** 24 effects at a 4M reserve undersize the postBatch, so the whole
bundle is dropped with no on-chain trace, forever. **After:**
`EEZ_POSTBATCH_GAS_RESERVE=8000000` lets the same 24-effect batch settle. The
default is still 4M, so nothing moves unless the operator sets the env var, and
the value is read once via `OnceLock`. The durable fix derives the reserve from
the batch's entry count; design doc §8.3 notes it, and it is not done here.

## Hunk 5 (~L384) — `clamp_max_postbatch_gas`: `MIN_VIABLE` const → `let`

The previous hunk forces this. The reserve is no longer a `const`, so
`const MIN_VIABLE = RESERVE + 21_000` becomes `let min_viable`
(`composer.rs:387`). Same arithmetic, same clamping. The floor now moves with the
env var, which is the point: an 8M reserve must not let a 5M
`EEZ_MAX_POSTBATCH_GAS` through.

## Hunk 6 (~L393) — reserve in the clamp-failure event + doc reference rename

The `reserve = …` field on the out-of-range ERROR event reads the accessor.
`sim_error_is_poison`'s doc comment now names `compose_chained`. Mechanical.

## Hunk 7 (~L547) — five new drain helpers

All private, all small (`composer.rs:553-624`):

- **`restore_pool_order(Vec<(usize, HeldTx)>) -> Vec<HeldTx>`** — sorts by drain
  index, then drops it. The drain now partitions into two direction phases.
  Without this helper, any re-queue would hand the pool back a permutation of
  what it dealt out, all outbound before all inbound, silently reordering the
  FIFO.
- **`abort_rest(failing, rest_of_phase, other_phase)`** — assembles everything a
  transient abort still owes the pool: the failing tx, the remainder of the
  current phase, and the whole untouched other phase. The failing tx is absent
  when it was already evicted as poison. This replaces the old inline
  `let mut rest = vec![held]; rest.extend(iter.by_ref()…)` idiom, which no longer
  covers the second phase.
- **`append_and_execute(&mut SyncBlockState, &[Bytes]) -> Option<(usize, BuildError)>`** —
  appends txs to the block-in-progress via `execute_tx`, stopping at the first
  `TxOutcome` that is not a success. `Some((position, why))` means the block is
  **half-extended** and must be reopened. `why` is a `BuildError`, not a string,
  so the caller can tell a rejected tx from an unreachable backing store.
- **`take_l1_cache(&mut SlotSessions, rollup_id)`** — removes the L1 session,
  calls `checkpoint()`, then downcasts to `revm::database::CacheState`. The
  payload shape is pinned by contract in `L1TargetSession::checkpoint`
  (`local/slot.rs:441`). This is the one place depending on it, and it errors
  rather than assumes.
- **`truncated_hex`** — first 32 bytes of a revert payload, for event messages.

## Hunk 8 (~L1156) — `Box::pin` at the single call site

`compose_cross_chain_batch(...)` is now boxed before `.await`
(`composer.rs:1168`). The drain holds two live execution contexts, the L1 world
and the block prefix. That pushed the future past clippy's `large_futures` 16KB
bound. No behavior change.

## Hunk 9 (~L1535) — `compose_cross_chain_batch` doc comment

Reworded. "Simulate each drained transaction" becomes "compose the drained
transactions in canonical order over the slot's chained execution contexts". The
cadence note now says each per-tx `finalize` is seeded with the slot's live
sessions. Doc-only.

## Hunk 10 (~L1586) — the drain's header comment

The old comment described the two-path optimistic split plus "simulate each held
tx independently". It now describes the two-phase chained drain and the per-tx
accept/evict protocol. Doc-only, but it is the map for everything below, so read
it in the file rather than the diff.

## Hunk 11 (~L1638) — slot execution contexts, and the drain's new bookkeeping

This is the substantive setup, and its placement is deliberate. It sits *after*
the empty-drain early return and *after* the stale-nonce partition
(`composer.rs:1648`). So a common empty-drain slot opens no state at all, and the
degrade path below re-queues exactly the post-stale set, because stale txs are
already released.

```rust
let reopen = |txs: &[Bytes]| SyncBlockState::open(l2_dyn.clone(), …, txs);
let slot_ctx = L1SlotState::open(&local.l1_entry).and_then(|world| reopen(&[]).map(…));
```

- `reopen` **rebuilds from the accepted tx list**; it does not restore a cached
  state. That is what keeps the prefix provably equal to the block the canonical
  rebuild produces. Otherwise the keystone check (hunk 18) would compare the
  block against a cache that drifted.
- Failing to open either context is transient. Nothing has been consumed, so the
  whole post-stale drain goes back via `push_front_batch` and the slot degrades
  to a minimal postBatch. New event: `eez.composer.phase2.slot_setup_failed`.
- Success emits `eez.composer.phase2.slot_anchored` with the pinned L1 anchor
  number and hash. That is the one line telling you which L1 base every claim in
  this slot was computed against.

Three new pieces of state:

- `sync_txs: Vec<Bytes>` — the Sync block's txs in canonical order, exactly as
  accepted. This is both what the prefix is rebuilt from and what the keystone
  check compares against.
- `system_txs_appended: u64` — a **single** SYSTEM_ADDRESS nonce cursor. It
  replaces two counters that were only ever summed at use sites, a forgot-one-term
  bug waiting to happen. It reproduces the canonical builder's two-phase
  allocation by construction: outbound loads take `N..N+K-1`, then inbound
  deliveries take `N+K..`. That holds because phase 1 runs to completion before
  phase 2 starts. Note it counts *system* txs, not block txs. An accepted
  outbound contributes 2 block txs, `load` and `user`, but only 1 nonce. Hence
  the increment uses `pairs_k.len()` and not `pair_txs.len()`.
- `survivors: Vec<(usize, HeldTx)>` — drain indices ride along, so a re-queue
  can restore pool order across the two phases.

## Hunk 12 (~L1733) — transient payload type, and the two-phase partition

`transient` becomes `Option<(String, Vec<(usize, HeldTx)>)>`, carrying indices.
Then:

```rust
let (outbounds, mut inbounds) = drained.into_iter().enumerate()
    .partition(|(_, held)| held.direction == Direction::Outbound);
```

**Why this order, and not drain order:** it is the real execution order on both
chains. On L2 the Sync block is `[load_0, user_0, …, deliver_0, …]`, per
`build_cross_chain_sync_pairs`. On L1 the postBatch's inline outbound executions
run inside `postAndVerifyBatch`, which precedes the bundle's inbound user txs.
Composing in that order is what makes the chained state real rather than plausible.

The partition is safe for nonces. A sender's nonce chain is never reordered,
because the two directions live on different chains, and poison-gap bookkeeping is
keyed per `(sender, direction)`.

## Hunk 13 (~L1770) — PHASE 1, outbound (L2→L1)

The old outbound arm was an `if held.direction == Outbound { … } continue;`
inside one loop. It is now its own `while let Some((idx, held)) = out_iter.next()`
loop (`composer.rs:1764`). The poison-gap pre-check at the top is unchanged, and
now appears once per phase.

**Contexts.** Per tx the drain opens an `L1TargetSession` over a clone of the L1
world cache. That replays real `EEZ._processNCalls` frames, including proxy
auto-creation and escrow value drawn from EEZ's balance. Alongside it sits the
`SyncBlockFork` from `draft.fork()`. The source sim runs the user tx on that L2
fork through `local.l2_entry`, because the L2 follower client errors `Unavailable`
for source sim. Failing to build either context is transient, so the drain aborts
with `abort_rest(Some(this), &mut out_iter, mem::take(&mut inbounds))`.

**Unchanged gates**, in the same order, still evicting: zero L1 entries
(`outbound_no_entries`), more than one entry per tx
(`outbound_multicall_unsupported`), missing ether-out, and over-escrow. Two edits.
A comment now names `check_entry_shape` instead of `reject_multicall`. And the
escrow *debit* moved to accept, while the *check* stays here as the cheap early
rejection: a tx evicted in between never draws the escrow down on L1, so a budget
reduced for it would wrongly evict later legitimate withdrawals
(`composer.rs:1955`).

**New: the ACCEPT block.** This is the heart of the hunk.

```rust
// Block first, world second: a pair evicted at append must leave the L1 world untouched.
let pairs_k = build_outbound_pair(&l1_entries[0], &held.raw_tx, &stf_cfg, nonce + system_txs_appended)?;
let pair_txs = interleave_sync_block_txs(&pairs_k);
if let Some((at, why)) = append_and_execute(&mut draft, &pair_txs) { … evict … draft = reopen(&sync_txs)? … }
sync_txs.extend(pair_txs);
system_txs_appended += pairs_k.len() as u64;
match take_l1_cache(&mut sessions, cc.entry_rollup_id) { Ok(cache) => l1_state.cache = cache, … }
```

Four things to notice.

1. **`build_outbound_pair` runs `check_entry_shape` itself.** So an entry the
   Sync-block lowering cannot represent comes back as its `Err` and evicts
   **this tx**, via `cc_compose.shape_evicted`. Unsupported shapes are multi-call,
   nested, unsuccessful, static, revert-span, and explicit-gas.
   **Before:** the same bad shape sailed through the drain and blew up post-drain
   inside `build_cross_chain_sync_pairs`, which re-queued *all* survivors and
   degraded the slot with `phase2.sync_pairs_failed`. The next slot drained the
   same set and failed the same way, which is a freeze vector. **After:** one tx
   is evicted and the other survivors still settle.
2. **The append is a real receipt check.** If `[load, user]` does not execute on
   the block prefix, the tx is evicted via `cc_compose.append_reverted`; it could
   never have landed on-chain either. One carve-out: if `why.is_provider()` the
   failure says nothing about the tx, so the slot aborts transiently instead of
   evicting a valid pair (`composer.rs:1912`).
3. **The prefix is reopened from `sync_txs` on append failure.** A failed
   `append_and_execute` may be half-applied, with the load succeeding and the user
   tx reverting. Rebuilding from the accepted list is the only truth. If the
   rebuild itself fails, that is transient and the drain aborts.
4. **Block first, world second.** `l1_state.cache` is only overwritten once the
   pair is safely in the block. A tx evicted at append leaves the L1 world
   byte-identical to before it was tried. If the session hand-off is what fails,
   the drain logs `cc_compose.l1_session_lost` at ERROR and aborts transiently,
   because the slot cannot chain L1 any more. The minimal path then discards the
   block built so far wholesale, so the pair sitting in `sync_txs` but not in
   `pending_out` at that instant is inert.

Staging afterwards is unchanged. It fills `pending_out` and `outbound_entries`,
and deliberately NOT `survivor_comps`. Two edits: `pending_out.push` is now a
single push of `l1_entries[0]` rather than a loop, equivalent because `len() > 1`
was evicted above; and `survivors.push((idx, held))` records the index. The
poison and transient arms below keep their classification, and the transient one
just uses `abort_rest` and renames the error prefix to `compose_chained outbound
tx#…`.

## Hunk 13 cont. (~L2035) — PHASE 2, inbound (L1→L2)

This is the mirror image (`composer.rs:2033`). The source sim runs the L1 user tx
on a fork of the world, via `l1_state.fork_state`. The L2 side is an
`InboundL2TargetSession` over `draft.fork()`, seeded with the current delivery
nonce cursor. It builds the canonical delivery, executes it on the fork, and
reads the claim off the real `EEZL2 → proxy` frame.

```rust
let inbounds = if transient.is_some() { Vec::new() } else { inbounds };
```

This guard is **provably redundant today**. All four phase-1 abort paths call
`std::mem::take(&mut inbounds)`, so the vector is already empty. It is kept
deliberately as a 5-line belt: a future phase-1 abort path that forgets its
`mem::take` would otherwise double-queue every inbound tx, once via `abort_rest`
and once by falling into phase 2.

**New shape gate at accept, both halves:**

```rust
target_entries.iter().try_for_each(|e| check_entry_shape(e, "inbound"))
    .and_then(|()| /* source entries with non-empty expectedL1ToL2Calls → Err */)
```

Target-side shape runs first. Then the gate scans the *source* composition's
entries for nested `expectedL1ToL2Calls` recordings. Nested composition is parked,
so a nested recording is this tx's problem, not the slot's, and the tx is evicted
via `cc_compose.shape_evicted`. Because of the `and_then`, a tx malformed on both
halves reports the target-side error.

**Delivery construction is now per-tx.** It calls `build_inbound_system_txs` at
`nonce + system_txs_appended`, with two eviction arms: an `Err`, meaning the shape
was rejected, and the odd case where every entry was skipped as foreign. The latter
is impossible for own-rollup targets, so the drain evicts loudly rather than
appending nothing.

**The append is the verifier.** The appended delivery re-runs the exact
`RollingHashMismatch` compare that used to explode at the proof signer. The same
`why.is_provider()` carve-out applies here: a backing-store failure aborts the
slot instead of evicting a valid delivery (`composer.rs:2160`).

**Before → After, on the #88 example.** Before: three co-bundled `increment()`
calls all record `returnData = 1`, all three deliveries enter the block
unchecked, the signer sees delivery #2 reverted and refuses the window, the drain
re-queues all three, and the next slot repeats forever. After: tx#1's probe runs
on a fork of the block that already contains tx#0's delivery, so it records `2`,
tx#2 records `3`, and the block is signed. And if a claim ever *were* wrong, the
append reverts and costs that ONE tx an eviction instead of freezing the slot.

Then, in this order: `system_txs_appended += deliveries.len()`,
`sync_txs.extend(deliveries)`, and finally `l1_state.cache = l1_fork.cache`. The
source fork's committed writes become the world, so later inbound sims see their
predecessors. Same block-first, world-second discipline as phase 1. The returned
sessions are dropped as `_sessions`, because the probe's fork is throwaway; only
the canonical delivery, appended to the real prefix, counts.

## Hunk 14 (~L2171) — event message wording + `survivors.push((idx, held))`

`"simulate_and_resolve produced {target_count} target(s)"` becomes
`"composition produced …"`. Mechanical.

## Hunk 15 (~L2193) — inbound transient arm

It uses `abort_rest(Some((idx, held)), &mut in_iter, Vec::new())`. Phase 2 is
last, so there is no other phase to hand back. The error prefix is renamed.
Classification is unchanged.

## Hunk 16 (~L2235) — restore pool order in the transient re-queue

```rust
let mut requeue = restore_pool_order(requeue);
```

**Before:** a drain of `[in_A, out_B, in_C]` was re-queued in drain order,
trivially. **After:** without this line the new code would re-queue
`[out_B, in_A, in_C]`, permanently reshuffling the pool's FIFO to match an
implementation detail of the drain. With it, the pool gets its own order back, and
the poison-cascade `retain` below is unchanged.

## Hunk 17 (~L2295) — restore pool order for survivors, post-drain

```rust
let survivors: Vec<HeldTx> = restore_pool_order(survivors);
```

Same reason, on the success path. Past the drain, survivors are only ever used for
a re-queue on degrade, and the pool is owed its own order. This is purely about
the *pool*: the block's order is fixed by `pending_out` and `pending_in`, which
are already canonical. The comment above the canonical builder is reworded from
"Build" to "Rebuild".

## Hunk 18 (~L2336) — THE KEYSTONE CHECK

```rust
let canonical = interleave_sync_block_txs(&pairs);
if canonical != sync_txs { … ERROR + degrade … }
```

The block this drain appended tx-by-tx must be byte-equal to what
`build_cross_chain_sync_pairs` plus `interleave_sync_block_txs` reconstructs from
the same entries. That is the same rebuild the deriver and the proof signer
perform. This is the single tie between the incremental construction and the
canonical one, and it holds by construction: both sides go through
`build_outbound_pair` and `build_inbound_system_txs` with the same nonce
allocation.

Inequality is a **composer bug, never bad input**, so it evicts nobody. The drain
logs `phase2.canonical_mismatch` at ERROR with a `first_divergent` index,
re-queues survivors, and degrades to minimal. Posting a block nobody else can
rebuild would be worse than posting nothing. Note `sync_txs` is no longer computed
here; it is the drain's accumulator now, which is exactly what makes the
comparison meaningful.

## Hunk 19 (~L2410) — belt: every final receipt must be a success

```rust
if let Some(first_failed) = built.tx_successes.iter().position(|s| !s) { … ERROR + degrade … }
```

Every tx in the block was already receipt-verified on the very prefix
`build_sync_block` re-executes, so this should be unreachable. If it fires, the
block and the prefix disagree. Nothing in this failure class may reach the proof
signer, so the drain logs `phase2.final_receipt_failed` at ERROR, re-queues, and
degrades.

**Before:** a reverted system tx in the built Sync block was not inspected here at
all, so it went out and the proof signer rejected the window — the #88 tail.
**After:** it never leaves the composer, given `tx_successes` surfaced from
`build_sync_block` (`local/build.rs:90`).

## Hunk 20 (~L3310) and Hunk 21 (~L3589) — reserve accessor at the two sizing sites

Emission-candidate sizing and `sign_post_batch_tx`'s `gas_limit` both call
`post_batch_execution_gas_reserve()` instead of reading the const
(`composer.rs:3349`, `composer.rs:3629`). This is where the env override reaches
the wire.

## Hunk 22–23 (~L3750, ~L3763) — `clamp_max_postbatch_gas` tests

The test's `min_viable` and the "budget at or below the reserve" case now call the
accessor (`composer.rs:3789`, `composer.rs:3802`). Same assertions. The tests are
now coupled to process env: they still pass if `EEZ_POSTBATCH_GAS_RESERVE` is set,
because everything is expressed relative to the accessor, but the `OnceLock` means
the first reader in the process fixes the value for all of them.

---

## Observable behavior changes in this file

| Change | Before | After |
|---|---|---|
| Claim computation for co-bundled txs | each tx simulated in isolation on the same pre-slot state; 3× `increment()` all claim `returnData=1` | each tx simulated on a fork of the L1 world + Sync block already containing its predecessors; claims are `1, 2, 3` |
| A wrong claim's blast radius | delivery reverts on-chain → signer rejects the window → whole set re-queued → permanent freeze | append reverts at compose time → that ONE tx is evicted, the rest of the slot settles (a backing-store failure aborts the slot instead) |
| Drain processing order | pool FIFO order, direction interleaved | two phases: all outbound, then all inbound (canonical block/L1 order) |
| Pool order after a re-queue | drain order preserved trivially | explicitly restored via `restore_pool_order`; without it the pool would be permanently reshuffled by the phase partition |
| Unsupported entry shape (multi-call / nested / static / revert-span / unsuccessful) | surfaced post-drain from `build_cross_chain_sync_pairs` → `phase2.sync_pairs_failed` → whole slot degraded and re-drained the same set (freeze vector) | per-tx gate at accept → `cc_compose.shape_evicted`, tx evicted + nonce cascade, slot proceeds |
| Inbound source entry recording nested `expectedL1ToL2Calls` | not checked in the composer | evicted at accept (`shape_evicted`) |
| Inbound composition where every target entry is foreign | would silently produce zero deliveries | evicted loudly (`shape_evicted`) |
| Session opened on an unseeded rollup | would run unchained off `provider.latest()`, silently | `Unavailable` error → classified transient → slot aborts and retries |
| Sync block vs canonical rebuild | never compared | byte-compared; mismatch → `phase2.canonical_mismatch` (ERROR) + degrade |
| Reverted tx in the final built Sync block | shipped; rejected later at the proof signer | `phase2.final_receipt_failed` (ERROR) + degrade; never leaves the composer |
| Failure to open slot execution contexts | n/a (no contexts existed) | `phase2.slot_setup_failed` (WARN) + whole post-stale drain re-queued + minimal postBatch |
| postBatch execution gas reserve | fixed 4M; a 24-effect batch reverted OOG inside the builder's bundle simulation and was dropped silently | default still 4M, overridable via `EEZ_POSTBATCH_GAS_RESERVE` (read once); at 8M a 24-effect batch settles |
| `clamp_max_postbatch_gas` lower bound | fixed at `4_000_000 + 21_000` | moves with the configured reserve |
| Shape-error message wording | `"inbound system transaction uses an unsupported execution shape"` (one string for all shapes) | direction-tagged, shape-naming strings from `check_entry_shape` (`"N>=2 multi-call inbound entry not yet supported (l2ToL1Calls=2)…"`, `"nested outbound entry materialization is not supported"`, …) |
| Error precedence when both directions are malformed in the post-drain rebuild | outbound shape error reported first (phase 1 built before inbound was checked) | inbound gate runs first in `build_cross_chain_sync_pairs`, so the inbound error wins — reachable only if the per-tx gates are bypassed. *(The ordering change itself lives in `eez-protocol/src/system_tx.rs`; it surfaces here as the `sync_pairs_failed` message.)* |
| Public API | `CrossChainWiring::simulate_and_resolve` / `::simulate_and_resolve_recorded_for` exported | both deleted (zero live callers after the drain rework); with them goes the last unchained composition path anyone could call by accident |
| `CrossChainWiring` construction | `entry_client` + `l2_entry_client` erased `ChainClient`s | one required `local: LocalComposeClients` |
| Tracing | `composer.simulate.start` logged `"simulate_and_resolve: starting…"` | `"starting composition pipeline"`; new events `phase2.slot_anchored`, `phase2.slot_setup_failed`, `phase2.canonical_mismatch`, `phase2.final_receipt_failed`, `cc_compose.shape_evicted`, `cc_compose.append_reverted`, `cc_compose.l1_session_lost`. No existing event was renamed in this file. |

---

# Part 4 — Tests, harness, wiring, and operational config

This part reviews everything around the fix: the new tests, the harness they
lean on, the node wiring, and the operational knobs. The design lives in
`docs/CHAINED-INTERSTATE-DESIGN.md`, and two words from there are used
throughout. A **claim** is a promise baked into a batch about what a cross-chain
call returns. The **drain** is the once-per-Sync-slot moment when the composer
takes held cross-chain transactions out of the pool and composes them.

Review order:

1. `crates/eez-node/tests/chained_interstate.rs` — new file
2. `crates/eez-node/tests/common/mod.rs` — harness changes
3. `contracts/src/Counter.sol` — new fixture contract
4. `crates/eez-node/src/main.rs` and `crates/eez-node/src/ingress.rs`
5. `.github/workflows/ci.yml` and `docker-compose.chiado-node.yml`

I read the design doc end to end, skimmed `crates/eez-node/tests/cross_chain.rs`
for house style, and ran `cargo check -p eez-node --tests`. It is clean, with no
warnings from our crates.

---

## 0. Read this first — the change-set is committed

An earlier draft of this review flagged `contracts/src/Counter.sol` as untracked
and the config files as unstaged. That is no longer true.

`git status --short` now shows no part of the change-set. The only modified file
is `docs/CHAINED-INTERSTATE-DESIGN.md`, plus a set of unrelated untracked docs.

Two commits carry the work. `d1756b1` ("fix: chained inter-state") carries the
composer change, the four tests, the harness diff, `Counter.sol`, the two
`eez-node` source files, and the compose file. `bdb0d71` ("refactor: rename
vars") carries the naming pass plus the one-line `ci.yml` change.

One coupling is worth remembering, because it is easy to break later. All four
tests call `deploy_counter`, which reads `contracts/out/Counter.sol/Counter.json`.
That path is a `forge build` artifact and `contracts/out/` is gitignored
(`.gitignore:5`). CI runs `forge build` in `contracts/` before the e2e job, so the
artifact exists there. But a future fixture added without being committed would
fail every test in setup, with an error that reads like a build problem.

---

## 1. `crates/eez-node/tests/chained_interstate.rs` (new, 644 lines)

The four tests pin four different properties. They are not four flavours of one
assertion, even though at a glance they all look like "send cross-chain
transactions and check a counter".

| Test | Property pinned |
|---|---|
| `three_order_dependent_inbound_calls_in_one_bundle` | claims chain 1, 2, 3 in one drain — the literal issue-#88 repro |
| `mixed_direction_state_chain_in_one_slot` | canonical block order: outbound before inbound |
| `poison_mid_bundle_leaves_survivors_correct` | eviction isolation, claims 1 and 2 not 1 and 3, plus no freeze |
| `same_sender_outbound_chain` | same-sender outbound nonce chain over a real L1 world, results 1 then 6 |

### 1.1 Module doc (lines 1–9)

The doc states the failure precisely. Isolated per-transaction simulation over
one pre-slot state produces claims the chain contradicts. The delivery reverts
with `RollingHashMismatch`, the proof signer rejects the window, and the drain
re-queues the same set forever.

This is the right framing for a test file. It says what a red test *means*, not
what the code does.

The `§9` reference is correct: §9 is the verification plan these tests
implement. Note the design doc has since been renumbered, so the live-chiado
findings this review cites are now **§7**.

### 1.2 The `CallResult` event declaration (lines 27–32)

A local `sol!` declares `CallResult(uint256 indexed, uint256 indexed, bool, bytes)`.

I checked it against both managers. The two declarations differ only in one
parameter *name*, so topic0 is identical (`EEZL2.sol:135`, `EEZ.sol:171`). The
comment's claim holds. The event is emitted right where the manager folds the
result into the rolling hash (`EEZL2.sol:553`, `EEZ.sol:1181`). Filtering by
emitting address is the correct way to tell L1 from L2.

This event is the ground-truth half of every test. The posted calldata is the
other half. The tests compare the two.

### 1.3 `MAX_USER_TXS` / `cap_env()` (lines 36–40)

Every test in the file pins `EEZ_MAX_USER_TXS_PER_BUNDLE=3` rather than
inheriting the composer default, which is also 3 today (`composer.rs:1132`).

The pin is a good call and the comment gives the reason. If someone lowered the
default to 2, the three co-bundled `increment()` calls would split across two
drains. The test would then fail on its "must ride ONE postBatch" assertion
rather than passing vacuously. The pin and that assertion are belt-and-braces in
the right direction.

Small oddity: `MAX_USER_TXS` is a `(&str, &str)` tuple that `cap_env()`
destructures as `.0` and `.1`. Two plain consts would read better. Cosmetic.

### 1.4 `posted_batches` (lines 47–65) — the strongest signal in the file

The helper reads every `BatchPosted` log from the deploy block. For each one it
fetches the posting transaction and decodes its input
(`eez-protocol/src/entries/mod.rs:593`).

This is what makes the tests convincing. The assertions run against the
byte-for-byte calldata L1 accepted. They do not run against composer-internal
state, a log line, or a re-derived batch. If the composer claimed 1, 1, 1, that
is what comes back here.

Cost note: every call re-scans from the deploy block and issues one
`eth_getTransactionByHash` per batch. That is negligible at four-batch scale.
Do not copy the pattern into a soak harness.

### 1.5 `inbound_claims` (lines 75–89)

The helper keeps entries with `proxyEntryHash != 0`, which are the deferred
inbound consumption entries, and decodes each `returnData` as a `uint256`.

The doc comment is honest about the weak spot. The on-chain entry is lean, so
attribution is by *direction* rather than by call identity. That is sound here
only because the fixture generates no cross-chain traffic other than the test's
own. Keep that sentence. It is the kind of assumption that breaks silently the
day someone adds background load to `setup_cross_chain`.

The `unwrap_or_else(|| panic!(...))` on a non-32-byte return is right. A
non-uint claim is a real failure, and printing the hex makes it debuggable.
That is invariant 7 in test form.

### 1.6 `outbound_calls` (lines 94–103)

This is the mirror image. It keeps immediate entries (`proxyEntryHash == 0`),
flattens over `l2ToL1Calls`, and filters to the test's L1 target.

The target filter is load-bearing. `prepare_post_batch_raw` prepends a leading
immediate entry with no `l2ToL1Calls`, and other immediates may carry unrelated
calls. Filtering by `targetAddress` drops both cleanly.

### 1.7 `CallOutcome` / `call_results` (lines 105–137)

The helper decodes `CallResult` logs into block, transaction hash, transaction
index, success flag, and return data. Test 2 uses `tx_index` to assert
intra-block ordering. Test 1 uses `block` to assert "one Sync block".

The comment "a delivery that diverged from its claim reverts, taking its logs
with it" is the key insight. The absence of a log here is itself the issue-#88
symptom. So `delivered.len() == 3` is a real assertion, not bookkeeping.

Same whole-chain caveat as `inbound_claims`: `from_block(0)` collects every
`CallResult` the chain ever emitted. True by construction today, since setup
only deploys contracts and creates proxies. It is still a coupling to the
fixture.

### 1.8 `assert_receipt_ok` / `wait_for_count` / `assert_reconciled` (lines 139–174)

These are three thin wrappers over the house `wait_for` plus a 3-minute
`SETTLE_TIMEOUT`. `assert_reconciled` is verbatim the reconciliation poll from
`cross_chain.rs`. It compares L1's stored `rollups[rid].stateRoot` against the
L2 **safe** block root. Using `safe` is correct, because the unsafe head is
optimistic.

One inconsistency is worth a decision. `wait_for_count` reads the counter at
`latest`, which is the optimistic head. Design §7 says to verify L2 effects at
the `safe` tag, because the unsafe head rolls back when a bundle fails.

The tests are still sound in practice. The load-bearing assertions that follow
only pass on settled state: `inbound_claims` comes from posted calldata, and
`assert_reconciled` compares settled roots. `wait_for_count` is really a
progress gate, not a verification.

Still, this is the one place the file diverges from its own design doc's advice.
If it ever flakes, this is why. Either switch it to `safe` or say in one line
that the gate is deliberate.

### 1.9 `open_drain_window` (lines 186–195) — how co-bundling is made deterministic

The whole file rests on this helper, so it deserves scrutiny.

It snapshots the posted-batch count, waits for that count to rise, and returns
immediately. The argument runs like this. The composer keeps at most one
postBatch in flight. So a fresh `BatchPosted` means the next drain is gated on
the deriver clearing that batch. That is never sooner than the next L1 block,
which is 5s on the embedded testing L1 (`eez-node/src/l1_embedded.rs:29`).
Submitting the transactions takes milliseconds against that.

Two things make this acceptable rather than hopeful. The window is wide: seconds
against milliseconds, so it is not a tight race. And every test then asserts its
claims arrived in exactly one posted batch. A missed window produces
`claimed.len() == 2` and a failure message that says so. The test cannot
silently weaken itself into a no-op.

Nit: the doc comment says admitting three transactions takes ~100ms; measured
submissions are ~11ms. Numbers in comments should be the measured ones.

### 1.10 `batches` / `assert_no_evictions` (lines 197–209)

`batches` is a one-line convenience over `posted_batches`.
`assert_no_evictions` asserts zero log lines containing `"evicting"` or
`"evicted"`.

This is a substring match over the raw log, the same baseline as
`cross_chain.rs:112`. Any line merely containing the word counts. The
composer's real eviction messages do contain it (`composer.rs:2215`,
`composer.rs:2036`), so the check does fire on real evictions. It is
over-broad, not under-broad, which is the safe direction. To tighten it later,
match the structured event names such as
`eez.composer.cc_compose.poison_evicted` instead of English.

### 1.11 Test 1 — `three_order_dependent_inbound_calls_in_one_bundle`

The test deploys `Counter` on L2, creates the L1-side proxy for it, opens a
drain window, then pushes three `increment()` calls from one sender at nonces
n, n+1, n+2 through the L1→L2 ingress front.

It asserts four things, in rising order of strength:

1. All three L1 user transactions land with status 1, and L2 `count() == 3`.
2. The claim chain decoded from the posted `postAndVerifyBatch` calldata is
   `[1, 2, 3]`, and it rides exactly one batch.
3. All three L2 deliveries succeed, share one block, and their
   `CallResult.returnData` values are `[1, 2, 3]`.
4. The L1 root reconciles with the L2 safe root, there are zero evictions, zero
   `"local L2 state root"` divergence lines, and no process death.

Without the redesign, all three entries carry `returnData = 1`. The second
delivery folds 2, reverts `RollingHashMismatch`, the signer rejects the window,
and nothing settles.

Assertion 2 is the one that names the bug. In practice the test would fail
earlier, at `wait_for_count(… 3 …)` timing out after 3 minutes. Assertion 2 is
what turns "it hung" into an unambiguous diagnosis.

Assertion 3 is not redundant with 2. Assertion 2 proves what was claimed;
assertion 3 proves the chain agreed. Both are needed to say "claims are exact".

Small thing: the loop increments a `nonce` seeded once from `pending_nonce`.
That is correct here, because held transactions have not landed on L1 yet.
Re-reading between submissions would hand back the same nonce three times.
Worth knowing when copying the pattern.

### 1.12 Test 2 — `mixed_direction_state_chain_in_one_slot`

The test deploys a `Counter` on *each* chain, wires an L1-side proxy to the L2
counter and an L2-side proxy to the L1 counter, then submits one inbound
`increment()` and one outbound `add(5)` into the same drain window.

It asserts that both user transactions succeed, L2 count is 1 and L1 count is 5.
It asserts both directions ride one postBatch. It asserts the canonical block
order from receipts: the outbound user transaction and the inbound delivery
share a block, and the outbound transaction index is lower. And it asserts
L1's own `CallResult` returned 5, so the outbound call really executed on L1
rather than just being carried.

Without the redesign the two directions are simulated against unrelated
snapshots, so the composed Sync block need not match what the bundle does on L1
and nothing is guaranteed to settle together.

The ordering assertion is the point of this test, and it is pinned the right
way. It reads receipts and checks them against the canonical builder's
documented order: all outbound `[load, user]` pairs, then all inbound
deliveries (design §3). A regression that flips the two passes shows up here
immediately.

The `.filter(|o| o.success)` before mapping L1 return data is fine. A failed
call is dropped from the vec, so the comparison to `vec![5]` still fails. It
reads as if it could mask a failure. It does not.

### 1.13 Test 3 — `poison_mid_bundle_leaves_survivors_correct`

The test pushes three transactions into one window: `increment()`, a poison
transaction, then `increment()`.

Without the redesign the poison degrades the whole slot, and the survivors'
claims come from isolated simulations that both say 1, so nothing settles.

The poison is the harness's established form. It is a cross-chain submission
whose `to` is a plain address rather than a proxy. I traced the composer path:
the source simulation records no cross-chain call, which yields `EmptyCalls`,
which `sim_error_is_poison` recognises, which fires
`eez.composer.cc_compose.poison_evicted` (`composer.rs:2215`). So the eviction
happens in-drain and deterministically. It is not the 3-strike post-dispatch
eviction. That matters: the test does not depend on retry timing.

The sender choice is the subtle part, and the comment explains it. Poison
bookkeeping evicts the rest of that sender's `(sender, direction)` nonce chain
(`composer.rs:2036`). If the poison shared `INBOUND_USER` with the survivors,
the second `increment()` would be evicted as a gapped successor. The test would
then measure the wrong thing. Hence the new `ANVIL_KEY_5`.

It asserts that both survivors succeed and L2 count is 2. It asserts the
survivor claims are `[1, 2]` in one batch, which is the property: `[1, 3]` would
mean the evicted transaction's simulated effect leaked into its successor's
claim. It asserts the poison has no L1 receipt, so it was dropped rather than
bundled. It asserts at least one eviction was logged. And it asserts no freeze,
by sending a fourth `increment()` in a later window and requiring count 3.

That last block is the issue-#76-adjacent property, and it is the one I would
have asked for if it were missing. Without it, "the poison was evicted" is
compatible with "and the window degraded forever after".

Two honest limitations. First, `receipt_ok(poison) == None` is a point-in-time
check. It proves the poison had not landed by then, not that it never can. The
eviction-log assertion covers the gap, so the two are jointly sufficient rather
than individually. Second, this test does not assert the `"local L2 state root"`
divergence count that test 1 asserts. It drives an eviction path that rebuilds
the block prefix, so it is arguably the test that most wants that check.

### 1.14 Test 4 — `same_sender_outbound_chain`

The test sends two outbound calls from one L2 sender at nonces n and n+1:
`increment()` then `add(5)`, against a stateful L1 target, in one slot.

`wait_for_count(l1, 6)` is the whole story in one line. The L1 counter must go
0 → 1 → 6.

Without the redesign both source simulations see the same L1 snapshot at count 0
and claim 1 and 5. Then `postAndVerifyBatch` re-executes them sequentially,
folds 1 and 6, and reverts.

It asserts both L2 user transactions succeed and the L1 count is 6. It asserts
both calls ride one postBatch, in submission order, which pins the
FIFO-per-direction guarantee from design §3. And it asserts L1's `CallResult`
return values are `[1, 6]`, so the L1 world really advanced between the two
source simulations.

This is the test that specifically covers `L1SlotState` advancement. Tests 1 and
3 cover the L2 block-prefix side. Together they cover both halves of "the world
advances only by real frames".

### 1.15 Three cheap additions

Nothing blocking, but three additions would raise the file's value.

First, assert the drain's own bug events never fired. Design §7 treats
`canonical_mismatch`, `final_receipt_failed`, and `l1_session_lost` as bugs
rather than input conditions (`composer.rs:2388`, `composer.rs:2453`,
`composer.rs:1966`). One shared helper asserting all three are absent would
turn silent degradations into failures. The keystone byte-for-byte equality
check is exactly the sort of thing you want a CI signal on.

Second, apply the divergence check uniformly. Only test 1 checks
`"local L2 state root"`. Folding it and the bug-event check into one
`assert_clean(&w)` used by all four keeps them consistent.

Third, `assert_no_evictions` and its inverse in test 3 could share one pattern
list, so they can never drift apart.

---

## 2. `crates/eez-node/tests/common/mod.rs`

### 2.1 `ANVIL_KEY_5`

A new constant, with a comment that explains why it exists rather than what it
is. Eviction cascades along a sender's nonce chain, so poison needs its own
sender. Keys 1 through 4 are already assigned to roles.

I verified the key derives to anvil account #5,
`0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc`. That address is prefunded in
reth's dev genesis, which is what the embedded testing L1 uses. So the poison
transaction is fundable and will be admitted. The test needs that: it must fail
at composition, not at ingress.

### 2.2 The port registry — the one real behavior change in this file

**Before.** `free_port()` bound `127.0.0.1:0`, read the port, dropped the
listener, and returned it — a pure availability probe with no memory. Separately,
`NodeHandle::spawn` kept its own local `HashSet` so one node's ~14 listeners
would not collide with each other.

**After.** One process-wide `HANDED_OUT_PORTS` registry records every port any
probe hands out, and `spawn` takes a guard on that shared set instead of a fresh
one.

The reason is an observed flake. The fourth sequential test in one process failed
*in setup* with "address already in use". The OS happily re-offers an ephemeral
port that an earlier node in the same process already bound. That node's sockets
may still be lingering when the next node tries the real bind. With roughly 14
ports across 4 nodes in one binary, the collision stops being theoretical.
Sharing one set across every probe removes the re-hand entirely.

Four details worth noting.

`handed_out_ports()` uses `PoisonError::into_inner`, so a panicking test does not
poison the registry for the tests that follow. That is the right choice for a
test harness. A poisoned mutex here would turn one real failure into three
cascading setup failures.

The explicit `drop(used_ports)` after the probes in `spawn` is load-bearing, not
tidiness. `std::sync::Mutex` is not reentrant, so anything later in that long
function calling `free_port()` would self-deadlock. Holding the guard across the
probes also serializes concurrent spawns, which is free given the nextest serial
config.

The set only grows, bounded by ports-per-process, which is tens. A test process
is short-lived, so this is fine. In theory `probe_unique_*` could loop forever if
the OS only ever offered already-handed-out ports, but that is not reachable at
these numbers.

### 2.3 `CrossChainConfig::new` port selection

**Before.** The code drew `l1_http_port` with `free_port()`, then drew
`l1_auth_port` in a retry loop that re-rolled while it equalled `http` or
`http + 1`. That was an ad-hoc guard for the L1's implicit WS listener at
`http + 1`. It protected against that one port, for that one draw. Any later
probe in the process could still hand out `http + 1`.

**After.** `probe_unique_http_port` draws the HTTP port. That pre-existing
helper verifies `http + 1` is bindable and inserts both into the shared registry
(`mod.rs:147`). The auth port is then a plain `free_port()`, which cannot
collide because both are already recorded.

Strictly stronger and shorter. The reservation is global instead of local, and
the retry loop disappears. `NodeHandle::spawn` already used the same helper for
its own L1 HTTP port, so the two paths now agree.

### 2.4 `ICounter` in the shared `sol!` block

Three functions: `count()` as a view, `increment()` returning `uint256`, and
`add(uint256)` returning `uint256`. It carries `#[sol(rpc)]` so `counter_count`
can call it through a provider. It sits alongside `IValue` and `ISetterWrapper`,
consistent with the house pattern.

### 2.5 `deploy_counter` / `counter_count`

`deploy_counter` is a three-line wrapper over `deploy_raw` with no constructor
arguments. These helpers live in `common/` rather than in the test file for a
mechanical reason: `deploy_raw` is private to the harness. `counter_count` has
the same shape as the existing `value_no_ret` and `l2_value` helpers.

Note `deploy_counter` takes `rpc_url` and `chain_id` separately, so one helper
deploys on L1 and L2. Tests 2 and 4 use both.

---

## 3. `contracts/src/Counter.sol` (new, 19 lines)

```solidity
contract Counter {
    uint256 public count;
    function increment() external returns (uint256 newCount) { count += 1; return count; }
    function add(uint256 x) external returns (uint256 newCount) { count += x; return count; }
}
```

No events, no access control, no constructor.

It exists because it is the minimal *state-dependent* target. Each call's return
value depends on its predecessors within the same block, which is exactly the
shape issue #88 describes.

The existing `Value.sol` setter is also order-dependent, so it could have been
used. But `setValue` returns a `(bool, uint256)` tuple, so the expected claim
sequence would be a list of tuples a reader has to simulate mentally. With a
counter, "the claims must be 1, 2, 3" reads at a glance, and a broken chain
reads as "1, 1, 1" rather than a tuple diff. That is a real reviewability win
for the file's headline test, and it is worth 19 lines.

Wiring is identical to `Value.sol`. No artifacts are checked in,
`contracts/out/` is gitignored, the harness reads
`contracts/out/Counter.sol/Counter.json`, and CI runs `forge build` before both
e2e jobs. I checked for an artifact-name collision, since foundry keys artifacts
by *file* name. The only other counter in scope is the submodule's
`CounterContracts.sol`, so there is no clash. The `pragma ^0.8.28` is compatible
with the pinned `solc = "0.8.34"`.

Style nit: `Value.sol` uses natspec tags while `Counter.sol` uses a free-form
`///` block. Cosmetic, but the neighbouring file sets a convention.

---

## 4. `crates/eez-node/src/main.rs` and `crates/eez-node/src/ingress.rs`

### 4.1 L1 entry client: bind concrete, erase after (main.rs:519–558)

**Before.** Each `match l1_variant` arm built a `LocalChainClient`, cloned it
into an `Arc<dyn ChainClient>`, and the match evaluated to that erased view. The
concrete handle went out of scope inside the arm.

**After.** The match evaluates to `Arc<LocalChainClient>`, and the erasure
happens once after the match.

The reason is that one instance now serves two consumers. The wiring's `rollups`
map wants it erased. The new `LocalComposeClients` wants it concrete, so the
drain can reach `L1SlotState` and `simulate_source_tx_on`. They must be the same
instance, because they share a single overlay channel. Two separate clients would
mean two overlay worlds, and the chained drain would silently lose effects. The
new comment says exactly this.

A bonus: each arm loses four lines of turbofish ceremony. The two arms now differ
only in how they build the provider and EvmConfig, which is the actual difference
between them.

### 4.2 L2 entry client (main.rs:576–585)

**Before.** The code built the client, erased it, and stored the erased view on
the wiring as `l2_entry_client`.

**After.** The binding is concrete, and the erased binding is gone along with the
wiring field it fed. The drain rework orphaned that field. Nothing reads
`CrossChainWiring.l2_entry_client` any more; I grepped, and the only remaining
hits were the two lines in `main.rs` itself.

A trailing comment was reworded from the old function name to "the outbound
source simulation", because the named function no longer exists.

One accuracy point on the new field's doc. `CrossChainWiring.local` is
documented as "the same instances registered in `rollups`; they share one
overlay channel" (`composer.rs:140-142`). That holds for L1, because the erased
view is a clone of the concrete client. It does not hold for L2. The `rollups`
map registers the `Role::Follower` client, while `local.l2_entry` is a separately
constructed `Role::Entry` client over the same provider.

This is functionally fine. L2 target execution goes through
`InboundL2TargetSession`, not through the erased client, and the follower client
deliberately errors `Unavailable` for source simulations. But the comment
overclaims. A five-word correction stops the next reader hunting for a shared
identity that is not there.

### 4.3 `wired_rollups` insert (main.rs:604–605)

An `Arc::clone` becomes a plain move. The extra clone is no longer needed, since
nothing else consumes the erased view. Mechanical.

### 4.4 `CrossChainWiring` construction (main.rs:693–701)

The `entry_client` and `l2_entry_client` fields drop out. The new
`local: LocalComposeClients { l1_entry, l2_entry }` replaces them
(`main.rs:697`).

Note `local` is a required field, not an `Option`. Cross-chain composer mode
always has an embedded L1, since this whole block sits inside the embedded-L1
arm. There is no "wired but no local handles" state to represent. That is the
right call. An `Option` here would create an unreachable `None` branch the drain
would have to handle.

### 4.5 `ingress.rs` (comment only)

One doc line changes. It referenced the deleted `simulate_and_resolve` and now
says "the composer's chained simulation". Same rewording as `main.rs`.

The rewrap leaves a ragged line: "…effect). One front / per source chain
(invariant 8)." Reflow it next time you touch the file.

---

## 5. CI and compose

### 5.1 `.github/workflows/ci.yml`

One line changed in the existing `cross-chain-e2e` job. It ran
`--test cross_chain` and now runs `--test cross_chain --test chained_interstate`
(`ci.yml:138`). Without that, the new binary compiles in CI but never runs.

Two things I checked so you do not have to. The job already runs `forge build` in
`contracts/` before the tests, so the `Counter.json` artifact is produced. And
`.config/nextest.toml` serializes by `filter = "kind(test)"`, so the new binary
joins the existing integration test-group automatically. No config change is
needed, and the explicit `--test-threads=1` is belt-and-braces.

One side note, not introduced here. `cargo test --workspace` is the pre-commit
gate in `CLAUDE.md`, and it now compiles and runs two heavy node-spawning
binaries in parallel. That is precisely what the nextest config exists to
prevent. The problem pre-dates this change with `cross_chain` alone, but a
second binary makes it likelier to bite. Worth a contributor-docs line.

### 5.2 `docker-compose.chiado-node.yml`

Three parameterizations, all overridable from the host env, all with unchanged
defaults. A node started with no extra env behaves exactly as before. All three
come straight out of the live chiado runs in design §7. All three are committed.

**a. `EEZ_SIGNER_PORT`, three sites.** Before, `50061` was hardcoded three
times: the signer's `EEZ_PROOF_SIGNER_ADDR`, its healthcheck `nc -z`, and the
node's `EEZ_PROVER_URL`. After, all three read `${EEZ_SIGNER_PORT:-50061}`
(lines 39, 48, 76).

The reason: on this host both 50061 and 50062 were squatted by leftover
signer and proverd processes from other worktrees. With the port hardcoded in
three places there was no way to move the stack without editing the file. The
three sites must move together, which is the argument for one variable rather
than three overrides.

**b. `EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS`.** A new passthrough,
written as `${EEZ_SIGNER_MAX_CHECKPOINTS:-8}` (line 40). I verified the signer's
own default is also 8, so the compose default is a no-op
(`eez-proof-signer/src/config.rs:133-140`).

It needs a knob because the signer's stateless validator caps per-window
transaction-state checkpoints. A window with more effects than the cap fails
`prepare_post_batch_raw` deterministically. The drain then degrades and requeues
the same window forever, which is the issue-#76 shape one layer up. Size the
quota at or above the effect count implied by `EEZ_MAX_USER_TXS_PER_BUNDLE`. The
chiado run used 64 with a 24-transaction bundle.

**c. `EEZ_POSTBATCH_GAS_RESERVE`.** A passthrough for the composer knob, written
as `${EEZ_POSTBATCH_GAS_RESERVE:-4000000}` (line 110). The composer default is
also 4M, so this too is a no-op at default (`composer.rs:291`).

It needs a knob because each deferred entry costs about 240k gas to queue inside
`postAndVerifyBatch`. A many-effect batch blows past a fixed 4M reserve and
reverts out of gas *inside the builder's bundle simulation*. rbuilder then drops
the bundle silently. That is the worst failure shape there is: no error anywhere,
just non-inclusion. The chiado run used 8M.

Two review points on this file.

The comment-placement problem an earlier draft flagged is fixed. The three-line
comment about the bundle cap now sits directly above
`EEZ_MAX_USER_TXS_PER_BUNDLE`, and `EEZ_POSTBATCH_GAS_RESERVE` has its own
one-liner below it (lines 103–110).

A naming asymmetry remains. `EEZ_POSTBATCH_GAS_RESERVE` uses the same name on
host and container. The checkpoint knob uses the short `EEZ_SIGNER_MAX_CHECKPOINTS`
on the host for the long container name. That is defensible, since the real name
is a mouthful, but it is inconsistent. An operator who exports the long name will
silently get the default.

Nothing enforces the couplings these knobs have with
`EEZ_MAX_USER_TXS_PER_BUNDLE`: checkpoints at or above the effect count, and the
reserve above roughly 240k per entry. Design §7 names the durable fixes, which
are capping the drain at the prover's quota and deriving the reserve from the
entry count. A pointer to §7 in the compose comment would save the next operator
the two-hour debug both findings cost.

---

## 6. Verification status

- `cargo check -p eez-node --tests` is clean.
- The author ran the four tests: 4 of 4 pass in about 350s wall, with the
  pre-existing suites re-verified.
- One honest limitation, unchanged from the existing baseline. The eviction
  assertions are English-substring matches over the node log, using the same
  technique `cross_chain.rs` already uses.

---

## 7. Behavior-change inventory

| Change | Before | After |
|---|---|---|
| `tests/chained_interstate.rs` | no coverage of chained-interstate composition | 4 e2e tests pinning claim chaining, two-direction block order, poison isolation plus no-freeze, and same-sender outbound nonce chains |
| Claim verification technique | effects checked via chain state and logs | claims decoded from the posted `postAndVerifyBatch` calldata, compared against the manager's own `CallResult` events |
| Co-bundling determinism | not addressed | `open_drain_window` aligns on a fresh `BatchPosted`, giving at least 5s of headroom; every test asserts its claims rode one batch, so a missed window fails loudly |
| `ANVIL_KEY_5` | keys 1 through 4, all assigned to roles | a 5th prefunded sender, so poison gets its own `(sender, direction)` nonce chain |
| `ICounter` / `deploy_counter` / `counter_count` | no stateful counter helper | shared harness helpers, needed here because `deploy_raw` is harness-private |
| Port handling — `free_port()` | a stateless availability probe; `spawn` kept a separate local used-set, so the 4th node in a process could be re-handed a port an earlier node's lingering sockets still held, failing setup with "address already in use" | one process-wide `HANDED_OUT_PORTS` registry shared by every probe, so no port is handed out twice in a process; the lock tolerates poisoning, so one panicking test does not cascade |
| Port handling — `CrossChainConfig::new` | `free_port()` for HTTP, then a retry loop re-rolling auth until it differed from `http` and `http + 1`; the implicit WS port was never reserved against later probes | `probe_unique_http_port` verifies and reserves both `http` and `http + 1` in the shared registry; auth is a plain `free_port()` that cannot collide; the retry loop is deleted |
| `contracts/src/Counter.sol` | no order-dependent fixture target with a scalar return | a 19-line counter whose `increment()` and `add()` return the new count, making the expected claim chain 1, 2, 3 readable at a glance |
| `main.rs` L1 and L2 entry clients | erased to `Arc<dyn ChainClient>` inside the match; the concrete handle was discarded | the concrete `Arc<LocalChainClient>` is bound first and erased once afterwards, so the wiring map and `LocalComposeClients` share one instance and one overlay channel |
| `CrossChainWiring` fields, from main.rs's side | erased `entry_client` and `l2_entry_client`, plus an `l2_entry_view` binding | all gone; a required `local: LocalComposeClients { l1_entry, l2_entry }` replaces them |
| `main.rs` and `ingress.rs` comments | referenced the deleted `simulate_and_resolve` | reworded to "the composer's chained simulation"; comment-only |
| `.github/workflows/ci.yml` | the cross-chain job ran `--test cross_chain` | it also runs `--test chained_interstate`, in the same serial job with the same `forge build` prerequisite |
| compose: signer port | `50061` hardcoded in 3 places, so the stack could not move when another worktree squatted the port | `${EEZ_SIGNER_PORT:-50061}` in all 3; the default is identical and one export relocates the whole stack |
| compose: signer checkpoint quota | unset, so the signer's built-in default of 8 applied; a window exceeding it failed `prepare_post_batch_raw` deterministically and the drain requeued forever | passed through as `${EEZ_SIGNER_MAX_CHECKPOINTS:-8}`; the default is identical and it can be raised to match the bundle cap |
| compose: postBatch gas reserve | unset, so the composer's 4M default applied; a many-effect batch reverted out of gas inside the builder's bundle simulation and was dropped silently | passed through as `${EEZ_POSTBATCH_GAS_RESERVE:-4000000}`; the default is identical and it can be sized to the bundle cap |

### Must-fix items — all resolved

An earlier draft listed three blockers. All three are now done.

1. `contracts/src/Counter.sol` is committed, in `d1756b1` alongside the tests.
2. `.github/workflows/ci.yml` is committed, in `bdb0d71`.
   `docker-compose.chiado-node.yml` is committed, in `d1756b1`.
3. The module doc cites design `§9`, which is the verification plan. Correct.

### Worth doing

Item 4 below is also done. Items 5 through 7 remain open.

4. ~~Move the `EEZ_POSTBATCH_GAS_RESERVE` line out from between the
   `EEZ_MAX_USER_TXS_PER_BUNDLE` comment and its variable.~~ Fixed; the gas
   reserve now has its own comment.
5. Add `canonical_mismatch`, `final_receipt_failed`, and `l1_session_lost` to a
   shared "clean run" assertion used by all four tests, and apply the
   `"local L2 state root"` check uniformly.
6. Correct the `CrossChainWiring.local` doc. The shared-instance claim holds for
   L1 but not for L2, which is a distinct `Role::Entry` client.
7. `wait_for_count` reads at `latest`, while design §7 says to read effects at
   `safe`. Either switch it or note that it is a progress gate rather than a
   verification.

---

## Appendix A — the three hunks that came from live chiado testing

Three changes exist only because the change-set was validated on live
chiado after the dev test suite was already green. Each one is a
dev-environment blind spot. The design doc (§7) tells the full story;
here is the short version.

1. **`clamp_frame_gas` (`local/slot.rs`).** Chiado's block gas limit is
   about 17M. The manager frames asked for 30M, and revm rejects any
   transaction whose gas limit exceeds the block's. So every outbound
   composition failed and evicted. Dev chains have bigger blocks and
   never see this. The fix clamps each frame to the anchor block's limit
   — which is also what the real chain enforces.
2. **`EEZ_POSTBATCH_GAS_RESERVE` (`composer.rs` + the compose file).**
   Storing one deferred entry on L1 costs about 240k gas. A 24-effect
   postBatch therefore needs about 6M of execution gas, but the old fixed
   reserve was 4M. The under-gassed postBatch reverted out of gas inside
   the block builder's simulation, and the builder dropped the bundle
   silently. The relay never had a bundle-size limit — it included a
   25-transaction bundle happily once the gas was honest. The reserve is
   now an env knob. The durable fix is deriving it from the entry count.
3. **Signer knobs (the compose file).** The proof signer allows at most
   `max_transaction_state_checkpoints` effects per window (default 8). A
   bigger window fails deterministically and the composer retries it
   forever. The knob is now parameterized next to the bundle cap so the
   two are sized together. The signer port also became a parameter,
   because leftover processes on the host had taken both default ports.

One operating rule that came out of this: **check L2 effects at the
`safe` block tag.** The unsafe head is optimistic. It rolls back when a
bundle fails, and it fooled two test scripts during validation.

## Appendix B — headline behavior changes

Each part ends with its full change/before/after table. These are the
changes that matter most when reviewing or operating the system:

| Change | Before | After |
|---|---|---|
| Co-bundled order-dependent claims | Recorded against the same pre-slot state; the signer rejected the window; the transactions re-queued forever | Recorded against the chained state; windows sign; verified up to 24 claims (1..24) in one Sync block on chiado |
| Cost of a wrong claim | The whole slot degraded and retried forever (a freeze) | The one offending transaction is evicted at append time; the rest of the slot proceeds |
| Unsupported entry shapes (nested, multicall, static) | Failed after the drain, degrading the whole slot forever | Evicted per transaction at accept time, with a loud event |
| Target-side execution | A direct call with a forged proxy `msg.sender` plus a nonce-restore hack | The real contract paths: the canonical delivery on the block fork; real `EEZ → proxy.executeOnBehalf` frames with real escrow balances |
| L2 simulation base | A fresh `latest()` state per transaction, then discarded | The Sync block under construction — byte-exact, receipt-checked |
| L1 simulation base | A fresh `latest()` state per transaction, then discarded | One pinned anchor per drain plus a commit-or-drop effect cache |
| Composer/deriver block agreement | Implicit (same builders, called separately) | Asserted byte-for-byte every slot (the keystone check) |
| The old `simulate_and_resolve` API | Existed, with the isolated semantics | Deleted; no unchained composition path remains |
| postBatch gas sizing | A fixed 4M execution reserve | The `EEZ_POSTBATCH_GAS_RESERVE` knob; a batch needs ~240k per deferred entry |
| Error precedence when both directions are malformed | The outbound shape error reported first | The inbound one reports first (accepted, trivial) |
